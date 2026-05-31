from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_core.diagnostics import dependencies, file_contracts, pipewire_canary, start_safety  # type: ignore
from stream_core.diagnostics.model import payload_from_results  # type: ignore
from stream_core.engine import audio_boot, encoder_profile, ffmpeg_args, preflight, rendering_boot, target_runtime  # type: ignore


def cp(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["cmd"], returncode=returncode, stdout=stdout, stderr=stderr)


class RenderingBootstrapContractTests(unittest.TestCase):
    def test_build_browser_url_encodes_stream1090_base_and_map_contract(self) -> None:
        cfg = SimpleNamespace(
            use_overlay_wrapper=True,
            overlay_view_host="127.0.0.1",
            overlay_port=18080,
            map_lat="36.35",
            map_lon="140.75",
            map_zoom="7.6",
            map_scale="0.82",
            map_icon_scale="1.4",
            map_label_scale="0.82",
            map_large_mode="1",
        )

        url = rendering_boot.build_browser_url(cfg)

        self.assertTrue(url.startswith("http://127.0.0.1:18080/index.html?"))
        self.assertIn("map_base=http://127.0.0.1:18080/stream1090/", url)
        self.assertIn("&lat=36.35&lon=140.75&zoom=7.6", url)
        self.assertIn("&scale=0.82&iconScale=1.4&labelScale=0.82", url)
        self.assertIn("&largeMode=1", url)

    def test_build_browser_url_uses_plain_browser_url_when_overlay_disabled(self) -> None:
        cfg = SimpleNamespace(use_overlay_wrapper=False, browser_url="http://example.test/stream1090/")

        self.assertEqual(rendering_boot.build_browser_url(cfg), "http://example.test/stream1090/")

    def test_overlay_http_ready_probe_requires_index_markers_and_stream1090_body(self) -> None:
        cfg = SimpleNamespace(use_overlay_wrapper=True, overlay_port=18080, overlay_view_host="127.0.0.1")
        responses = {
            "http://127.0.0.1:18080/index.html": '<main id="map">Local ADS-B Receiver Evaluated with ARENA</main>',
            "http://127.0.0.1:18080/stream1090/": "x" * 128,
        }

        with mock.patch.object(rendering_boot, "is_port_listening", return_value=True):
            with mock.patch.object(rendering_boot, "http_get_text", side_effect=lambda url, timeout_sec=2.0: responses[url]):
                ok, summary = rendering_boot.overlay_http_ready_probe(cfg)

        self.assertTrue(ok)
        self.assertEqual(summary, "overlay and stream1090 reachable")

    def test_overlay_http_ready_probe_rejects_missing_wrapper_markers(self) -> None:
        cfg = SimpleNamespace(use_overlay_wrapper=True, overlay_port=18080, overlay_view_host="127.0.0.1")

        with mock.patch.object(rendering_boot, "is_port_listening", return_value=True):
            with mock.patch.object(rendering_boot, "http_get_text", return_value="<html></html>"):
                ok, summary = rendering_boot.overlay_http_ready_probe(cfg)

        self.assertFalse(ok)
        self.assertEqual(summary, "overlay index missing expected markers")

    def test_resolve_browser_bin_respects_explicit_binary_and_fallback_order(self) -> None:
        def fake_which(name: str) -> str | None:
            return f"/usr/bin/{name}" if name in {"chromium", "firefox"} else None

        with mock.patch.object(rendering_boot.shutil, "which", side_effect=fake_which):
            self.assertEqual(rendering_boot.resolve_browser_bin("chromium"), "chromium")
            self.assertIsNone(rendering_boot.resolve_browser_bin("missing-browser"))
            self.assertEqual(rendering_boot.resolve_browser_bin(""), "chromium")


class AudioBootstrapContractTests(unittest.TestCase):
    def test_ensure_virtual_sink_skips_existing_sink(self) -> None:
        calls: list[list[str]] = []

        def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
            calls.append(cmd)
            return cp(0, stdout="1\tstream_sink\tmodule-null-sink.c\n")

        audio_boot.ensure_virtual_sink(pulse_sink="stream_sink", run_cmd=run)

        self.assertEqual(calls, [["pactl", "list", "short", "sinks"]])

    def test_ensure_virtual_sink_loads_missing_sink(self) -> None:
        calls: list[list[str]] = []

        def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
            calls.append(cmd)
            if cmd[:4] == ["pactl", "list", "short", "sinks"]:
                return cp(0, stdout="1\talsa_output.pci\tmodule-alsa-card.c\n")
            return cp(0, stdout="42\n")

        audio_boot.ensure_virtual_sink(pulse_sink="stream_sink", run_cmd=run)

        self.assertEqual(calls[0], ["pactl", "list", "short", "sinks"])
        self.assertEqual(calls[1][0:3], ["pactl", "load-module", "module-null-sink"])
        self.assertIn("sink_name=stream_sink", calls[1])

    def test_detect_pulse_monitor_prefers_explicit_source_then_sink_then_default_sink(self) -> None:
        self.assertEqual(
            audio_boot.detect_pulse_monitor(pulse_source="explicit.monitor", pulse_sink="stream_sink", run_cmd=lambda _cmd: cp(0)),
            "explicit.monitor",
        )
        self.assertEqual(
            audio_boot.detect_pulse_monitor(pulse_source="", pulse_sink="stream_sink", run_cmd=lambda _cmd: cp(0)),
            "stream_sink.monitor",
        )

        def run(_cmd: list[str]) -> subprocess.CompletedProcess[str]:
            return cp(0, stdout="Server Name: PulseAudio\nDefault Sink: alsa_output.usb\n")

        self.assertEqual(audio_boot.detect_pulse_monitor(pulse_source="", pulse_sink="", run_cmd=run), "alsa_output.usb.monitor")

    def test_local_audio_monitor_selects_non_stream_sink_and_returns_module_id(self) -> None:
        calls: list[list[str]] = []
        logs: list[str] = []

        def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
            calls.append(cmd)
            if cmd[:4] == ["pactl", "list", "short", "sinks"]:
                return cp(0, stdout="1\tstream_sink\tmodule-null-sink.c\n2\tspeakers\tmodule-alsa-card.c\n")
            return cp(0, stdout="77\n")

        module_id = audio_boot.ensure_local_audio_monitor(
            enabled=True,
            monitor_sink="",
            pulse_sink="stream_sink",
            latency_msec=60,
            run_cmd=run,
            log=logs.append,
        )

        self.assertEqual(module_id, "77")
        self.assertFalse(logs)
        self.assertEqual(calls[1][0:3], ["pactl", "load-module", "module-loopback"])
        self.assertIn("source=stream_sink.monitor", calls[1])
        self.assertIn("sink=speakers", calls[1])


class FfmpegArgumentContractTests(unittest.TestCase):
    def cfg(self, **overrides):
        defaults = {
            "test_mode": False,
            "test_output": "null",
            "test_output_file": Path("/tmp/capture.mkv"),
            "use_fifo_recovery": False,
            "fifo_recovery_wait_sec": 1,
            "fifo_max_recovery_attempts": 0,
            "fifo_queue_size": 600,
            "fifo_drop_pkts_on_overflow": False,
            "fifo_restart_with_keyframe": True,
            "rtmp_url": "rtmps://a.rtmps.youtube.com:443/live2/key",
            "video_bitrate": "3400k",
            "video_maxrate": "3400k",
            "video_bufsize": "6800k",
            "audio_bitrate": "192k",
            "draw_mouse": 0,
            "frame_rate": 4,
            "video_size": "1920x1080",
            "audio_queue_size": 8192,
            "output_size": "1920x1080",
            "audio_filter": "aresample=async=1:first_pts=0,volume=0.25",
            "video_encoder": "libx264",
            "video_preset": "ultrafast",
            "video_nvenc_preset": "p4",
            "video_nvenc_rc": "cbr",
            "video_nvenc_cq": "",
            "video_nvenc_multipass": "",
            "video_nvenc_rc_lookahead": 0,
            "video_nvenc_spatial_aq": False,
            "video_nvenc_temporal_aq": False,
            "audio_sample_rate": 48000,
        }
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def test_output_args_separate_test_null_file_fifo_and_direct_flv(self) -> None:
        self.assertEqual(ffmpeg_args.build_output_args(self.cfg(test_mode=True)), ["-f", "null", "-"])
        self.assertEqual(
            ffmpeg_args.build_output_args(self.cfg(test_mode=True, test_output="file", test_output_file=Path("/tmp/out.mkv"))),
            ["-f", "matroska", "/tmp/out.mkv"],
        )
        self.assertEqual(ffmpeg_args.build_output_args(self.cfg()), ["-f", "flv", "rtmps://a.rtmps.youtube.com:443/live2/key"])

        fifo = ffmpeg_args.build_output_args(
            self.cfg(use_fifo_recovery=True, fifo_drop_pkts_on_overflow=True, fifo_restart_with_keyframe=False)
        )
        self.assertEqual(fifo[:4], ["-f", "fifo", "-fifo_format", "flv"])
        self.assertIn("-attempt_recovery", fifo)
        self.assertIn("-drop_pkts_on_overflow", fifo)
        self.assertEqual(fifo[-3:], ["-restart_with_keyframe", "0", "rtmps://a.rtmps.youtube.com:443/live2/key"])

    def test_build_ffmpeg_args_preserves_stream_contract_and_profile_overrides(self) -> None:
        args = ffmpeg_args.build_ffmpeg_args(
            self.cfg(),
            x11_input=":99+0,0",
            pulse_source="stream_sink.monitor",
            encoder_profile={"video_bitrate": "2500k", "video_maxrate": "2500k", "video_bufsize": "5000k", "audio_bitrate": "128k"},
        )

        self.assertEqual(args[0], "ffmpeg")
        self.assertEqual(args[args.index("-loglevel") + 1], "error")
        self.assertIn("-f", args)
        self.assertIn("x11grab", args)
        self.assertIn("stream_sink.monitor", args)
        self.assertEqual(args[args.index("-c:v") + 1], "libx264")
        self.assertEqual(args[args.index("-preset") + 1], "ultrafast")
        self.assertEqual(args[args.index("-r") + 1], "4")
        self.assertEqual(args[args.index("-g") + 1], "8")
        self.assertEqual(args[args.index("-b:v") + 1], "2500k")
        self.assertEqual(args[args.index("-maxrate") + 1], "2500k")
        self.assertEqual(args[args.index("-bufsize") + 1], "5000k")
        self.assertEqual(args[args.index("-b:a") + 1], "128k")
        self.assertEqual(args[-3:-1], ["-f", "flv"])
        self.assertEqual(args[-1], "rtmps://a.rtmps.youtube.com:443/live2/key")

    def test_build_ffmpeg_args_uses_nvenc_only_when_configured(self) -> None:
        args = ffmpeg_args.build_ffmpeg_args(
            self.cfg(video_encoder="h264_nvenc", video_nvenc_preset="p4"),
            x11_input=":99+0,0",
            pulse_source="stream_sink.monitor",
            encoder_profile={},
        )

        self.assertEqual(args[args.index("-c:v") + 1], "h264_nvenc")
        self.assertEqual(args[args.index("-preset") + 1], "p4")
        self.assertEqual(args[args.index("-rc") + 1], "cbr")
        self.assertNotIn("ultrafast", args)
        self.assertEqual(args[args.index("-b:v") + 1], "3400k")
        self.assertEqual(args[args.index("-maxrate") + 1], "3400k")
        self.assertEqual(args[args.index("-bufsize") + 1], "6800k")

    def test_build_ffmpeg_args_can_use_quality_bounded_nvenc_vbr(self) -> None:
        args = ffmpeg_args.build_ffmpeg_args(
            self.cfg(
                video_encoder="h264_nvenc",
                video_nvenc_preset="p4",
                video_nvenc_rc="vbr",
                video_nvenc_cq="17",
                video_nvenc_multipass="fullres",
                video_nvenc_rc_lookahead=8,
                video_nvenc_spatial_aq=True,
                video_nvenc_temporal_aq=True,
            ),
            x11_input=":99+0,0",
            pulse_source="stream_sink.monitor",
            encoder_profile={},
        )

        self.assertEqual(args[args.index("-c:v") + 1], "h264_nvenc")
        self.assertEqual(args[args.index("-rc") + 1], "vbr")
        self.assertEqual(args[args.index("-cq") + 1], "17")
        self.assertEqual(args[args.index("-multipass") + 1], "fullres")
        self.assertEqual(args[args.index("-rc-lookahead") + 1], "8")
        self.assertEqual(args[args.index("-spatial-aq") + 1], "1")
        self.assertEqual(args[args.index("-temporal-aq") + 1], "1")

    def test_encoder_profile_expiration_only_applies_to_emergency_low_upload_mode(self) -> None:
        self.assertFalse(encoder_profile.encoder_profile_expired({"mode": "normal", "until_ts": 10}, now_ts=20))
        self.assertFalse(encoder_profile.encoder_profile_expired({"mode": "emergency_low_upload", "until_ts": "bad"}, now_ts=20))
        self.assertFalse(encoder_profile.encoder_profile_expired({"mode": "emergency_low_upload", "until_ts": 30}, now_ts=20))
        self.assertTrue(encoder_profile.encoder_profile_expired({"mode": "emergency_low_upload", "until_ts": 20}, now_ts=20))


class PreflightAndTargetRuntimeContractTests(unittest.TestCase):
    def test_assert_systemd_launch_blocks_direct_production_launch_but_allows_systemd_or_override(self) -> None:
        cfg = SimpleNamespace(require_systemd_launch=True, allow_direct_stream_sh=False)

        with self.assertRaisesRegex(RuntimeError, "Direct stream launch is disabled"):
            preflight.assert_systemd_launch(cfg, env={})

        preflight.assert_systemd_launch(cfg, env={"INVOCATION_ID": "abc"})
        preflight.assert_systemd_launch(cfg, env={"STREAM_LAUNCH_MODE": "systemd"})
        preflight.assert_systemd_launch(SimpleNamespace(require_systemd_launch=True, allow_direct_stream_sh=True), env={})

    def test_pick_font_file_uses_configured_file_before_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            font = Path(td) / "Configured.ttf"
            font.write_text("font", encoding="utf-8")

            self.assertEqual(preflight.pick_font_file(str(font)), str(font))

    def test_prepare_runtime_paths_creates_runtime_dirs_and_default_now_playing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            cfg = SimpleNamespace(
                base_dir=base,
                overlay_dir=base / "ui" / "overlay",
                now_playing_file=base / "now_playing.txt",
            )

            preflight.prepare_runtime_paths(cfg)

            self.assertTrue((base / "logs").is_dir())
            self.assertTrue((base / "state" / "runtime").is_dir())
            self.assertTrue((base / "runtime").is_dir())
            self.assertTrue(cfg.overlay_dir.is_dir())
            self.assertIn("Preparing audio", cfg.now_playing_file.read_text(encoding="utf-8"))

    def test_target_runtime_hashes_actual_target_without_exposing_stream_key(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = SimpleNamespace(
                test_mode=False,
                test_output="null",
                test_output_file=root / "capture.mkv",
                rtmp_url="rtmps://a.rtmps.youtube.com:443/live2/REDACTED_TEST_STREAM_KEY",
                stream_lock_dir=root / "locks",
                runtime_state_file=root / "state" / "runtime" / "stream_runtime_state.json",
            )

            resolved = target_runtime.resolve_target_runtime(cfg)

            self.assertRegex(resolved.stream_key_hash, r"^[0-9a-f]{64}$")
            self.assertNotIn("REDACTED_TEST_STREAM_KEY", resolved.stream_lock_file.name)
            self.assertEqual(resolved.stream_lock_file.name, f"adsb-stream-new-stream-{resolved.stream_key_hash}.lock")
            self.assertEqual(resolved.takeover_coord_file.name, f"adsb-stream-new-stream-{resolved.stream_key_hash}.takeover.lock")
            self.assertEqual(resolved.runtime_state_file.name, f"stream_runtime_state_{resolved.stream_key_hash}.json")


class DiagnosticsBootstrapContractTests(unittest.TestCase):
    def test_start_safety_placeholder_key_is_fatal_in_production_but_info_in_test_mode(self) -> None:
        env_path = Path("/tmp/adsb-streamnew")
        ingest = {
            "judgment": "placeholder_stream_key",
            "path": str(env_path),
            "scheme": "rtmps",
            "host": "a.rtmps.youtube.com",
            "port": 443,
            "live2_path": True,
            "reason": "STREAM_KEY is not configured",
        }

        prod = start_safety.start_safety_results(
            read_env_file=lambda _path: {"STREAM_KEY": "YOUR_STREAM_KEY", "TEST_MODE": "0", "DISPLAY_NAME": ":99"},
            is_active=lambda _unit: False,
            legacy_stream_service="adsb-stream.service",
            stream_ingest_status=lambda _path: ingest,
            env_path=env_path,
        )
        test = start_safety.start_safety_results(
            read_env_file=lambda _path: {"STREAM_KEY": "YOUR_STREAM_KEY", "TEST_MODE": "1", "DISPLAY_NAME": ":101"},
            is_active=lambda _unit: False,
            legacy_stream_service="adsb-stream.service",
            stream_ingest_status=lambda _path: ingest,
            env_path=env_path,
        )

        prod_payload = payload_from_results(prod)
        test_payload = payload_from_results(test)
        self.assertEqual(prod_payload["fatal_count"], 2)
        self.assertEqual(test_payload["fatal_count"], 0)
        self.assertIn("ignored because TEST_MODE=1", test[0].summary)

    def test_start_safety_detects_legacy_display_conflict_only_for_production_colon99(self) -> None:
        ingest = {
            "judgment": "rtmps_preferred",
            "path": "/tmp/env",
            "scheme": "rtmps",
            "host": "a.rtmps.youtube.com",
            "port": 443,
            "live2_path": True,
            "reason": "RTMPS ingest endpoint uses explicit port 443",
        }

        conflict = start_safety.start_safety_results(
            read_env_file=lambda _path: {"STREAM_KEY": "real", "TEST_MODE": "0", "DISPLAY_NAME": ":99"},
            is_active=lambda unit: unit == "legacy.service",
            legacy_stream_service="legacy.service",
            stream_ingest_status=lambda _path: ingest,
        )
        no_conflict = start_safety.start_safety_results(
            read_env_file=lambda _path: {"STREAM_KEY": "real", "TEST_MODE": "0", "DISPLAY_NAME": ":101"},
            is_active=lambda unit: unit == "legacy.service",
            legacy_stream_service="legacy.service",
            stream_ingest_status=lambda _path: ingest,
        )

        self.assertTrue(next(item for item in conflict if item.name == "start_safety:legacy_display_conflict").fatal)
        self.assertFalse(next(item for item in no_conflict if item.name == "start_safety:legacy_display_conflict").fatal)

    def test_pipewire_canary_distinguishes_configured_and_runtime_pipewire(self) -> None:
        responses = {
            "pipewire.service": cp(0, stdout="active\n"),
            "pipewire-pulse.service": cp(0, stdout="active\n"),
            "pulseaudio.service": cp(3, stdout="inactive\n"),
        }

        def run(cmd: list[str], check: bool = False):
            if cmd[:3] == ["systemctl", "--user", "is-active"]:
                return responses[cmd[-1]]
            if cmd == ["pactl", "info"]:
                return cp(0, stdout="Server Name: PulseAudio (on PipeWire 1.0.0)\n")
            raise AssertionError(cmd)

        with mock.patch.object(pipewire_canary.shutil, "which", return_value="/usr/bin/pactl"):
            status = pipewire_canary.pipewire_canary_status(
                read_env_file=lambda _path: {"PREFER_PIPEWIRE_PULSE": "1"},
                parse_bool=lambda value: str(value).strip() == "1",
                run=run,
            )

        self.assertTrue(status["prefer_pipewire_pulse"])
        self.assertTrue(status["pipewire_active"])
        self.assertTrue(status["server_is_pipewire"])
        self.assertEqual(status["recommendation"], "canary_active_observe")

    def test_required_file_results_and_command_results_report_fatal_contract_breaks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            present = "src/stream_core/stream_engine.py"
            (base / present).parent.mkdir(parents=True, exist_ok=True)
            (base / present).write_text("# ok\n", encoding="utf-8")

            file_results = file_contracts.required_file_results(base, (present, "missing.py"))

        with mock.patch.object(dependencies.shutil, "which", side_effect=lambda name: f"/usr/bin/{name}" if name == "ffmpeg" else None):
            command_results = dependencies.command_results(("ffmpeg", "missing-command"))

        self.assertTrue(file_results[0].ok)
        self.assertFalse(file_results[1].ok)
        self.assertTrue(file_results[1].fatal)
        self.assertTrue(command_results[0].ok)
        self.assertFalse(command_results[1].ok)
        self.assertTrue(command_results[1].fatal)


if __name__ == "__main__":
    unittest.main()
