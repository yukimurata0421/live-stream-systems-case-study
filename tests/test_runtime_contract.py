from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "stream_core"))

import stream_engine  # type: ignore


ENV_EXAMPLE = ROOT / "ops" / "systemd" / "adsb-streamnew.env.example"
STREAM_SERVICE = ROOT / "ops" / "systemd" / "adsb-streamnew-youtube-stream.service"
PROD_ENV = Path("/etc/default/adsb-streamnew")


def parse_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def parse_unit_seconds(path: Path, key: str) -> float:
    prefix = key + "="
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text.startswith(prefix):
            continue
        raw = text[len(prefix) :].strip()
        if raw.endswith("s"):
            raw = raw[:-1]
        return float(raw)
    raise AssertionError(f"{key} not found in {path}")


class RuntimeContractTests(unittest.TestCase):
    def test_stop_ffmpeg_grace_has_room_before_systemd_timeout(self) -> None:
        env = parse_env_file(ENV_EXAMPLE)
        stop_grace = float(env["STOP_FFMPEG_TERM_GRACE_SEC"])
        timeout_stop = parse_unit_seconds(STREAM_SERVICE, "TimeoutStopSec")

        self.assertGreaterEqual(stop_grace, 0.5)
        self.assertLessEqual(stop_grace + 5.0, timeout_stop)

    def test_audio_filter_from_env_example_is_used_in_ffmpeg_args(self) -> None:
        env = parse_env_file(ENV_EXAMPLE)
        with tempfile.TemporaryDirectory() as td:
            patched_env = dict(env)
            patched_env.update(
                {
                    "BASE_DIR": td,
                    "TEST_MODE": "1",
                    "TEST_OUTPUT": "null",
                    "TEST_OUTPUT_FILE": str(Path(td) / "capture.mkv"),
                }
            )
            with mock.patch.dict(os.environ, patched_env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                args = engine.ffmpeg_args(":99", "stream_sink.monitor")

        self.assertEqual(cfg.audio_filter, env["AUDIO_FILTER"])
        self.assertIn("-af", args)
        self.assertEqual(args[args.index("-af") + 1], env["AUDIO_FILTER"])
        self.assertIn("volume=0.2375", env["AUDIO_FILTER"])

    def test_rtmps_443_ingest_from_env_example_is_used_in_ffmpeg_args(self) -> None:
        env = parse_env_file(ENV_EXAMPLE)
        with tempfile.TemporaryDirectory() as td:
            patched_env = dict(env)
            patched_env.update(
                {
                    "BASE_DIR": td,
                    "STREAM_KEY": "REPLACE_WITH_TEST_STREAM_KEY",
                    "TEST_MODE": "0",
                    "REQUIRE_SYSTEMD_LAUNCH": "0",
                }
            )
            with mock.patch.dict(os.environ, patched_env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                engine.resolve_rtmp_url()
                engine.validate_rtmp_url()
                args = engine.ffmpeg_args(":99", "stream_sink.monitor")

        self.assertEqual(cfg.rtmp_url, "rtmps://a.rtmps.youtube.com:443/live2/REPLACE_WITH_TEST_STREAM_KEY")
        self.assertEqual(args[-1], cfg.rtmp_url)

    def test_env_example_encoding_contract_uses_low_bandwidth_5fps_nvenc_profile(self) -> None:
        env = parse_env_file(ENV_EXAMPLE)
        with tempfile.TemporaryDirectory() as td:
            patched_env = dict(env)
            patched_env.update(
                {
                    "BASE_DIR": td,
                    "TEST_MODE": "1",
                    "TEST_OUTPUT": "null",
                    "TEST_OUTPUT_FILE": str(Path(td) / "capture.mkv"),
                }
            )
            with mock.patch.dict(os.environ, patched_env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                args = engine.ffmpeg_args(":99", "stream_sink.monitor")

        self.assertEqual(cfg.frame_rate, 5)
        self.assertEqual(cfg.video_encoder, "h264_nvenc")
        self.assertEqual(cfg.video_nvenc_preset, "p4")
        self.assertEqual(cfg.video_bitrate, "3400k")
        self.assertEqual(cfg.video_maxrate, "3400k")
        self.assertEqual(cfg.video_bufsize, "6800k")
        self.assertEqual(args[args.index("-c:v") + 1], "h264_nvenc")
        self.assertEqual(args[args.index("-preset") + 1], "p4")
        self.assertEqual(args[args.index("-rc") + 1], "cbr")
        self.assertEqual(args[args.index("-r") + 1], "5")
        self.assertEqual(args[args.index("-g") + 1], "10")
        self.assertEqual(args[args.index("-keyint_min") + 1], "10")
        self.assertEqual(args[args.index("-b:v") + 1], "3400k")
        self.assertEqual(args[args.index("-maxrate") + 1], "3400k")
        self.assertEqual(args[args.index("-bufsize") + 1], "6800k")

    def test_nvenc_encoder_uses_separate_preset_without_changing_bitrate_contract(self) -> None:
        env = parse_env_file(ENV_EXAMPLE)
        with tempfile.TemporaryDirectory() as td:
            patched_env = dict(env)
            patched_env.update(
                {
                    "BASE_DIR": td,
                    "TEST_MODE": "1",
                    "TEST_OUTPUT": "null",
                    "TEST_OUTPUT_FILE": str(Path(td) / "capture.mkv"),
                    "VIDEO_ENCODER": "h264_nvenc",
                    "VIDEO_PRESET": "ultrafast",
                    "VIDEO_NVENC_PRESET": "p4",
                }
            )
            with mock.patch.dict(os.environ, patched_env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                args = engine.ffmpeg_args(":99", "stream_sink.monitor")

        self.assertEqual(cfg.video_encoder, "h264_nvenc")
        self.assertEqual(args[args.index("-c:v") + 1], "h264_nvenc")
        self.assertEqual(args[args.index("-preset") + 1], "p4")
        self.assertEqual(args[args.index("-rc") + 1], "cbr")
        self.assertNotIn("-cq", args)
        self.assertNotIn("ultrafast", args)
        self.assertEqual(args[args.index("-b:v") + 1], "3400k")
        self.assertEqual(args[args.index("-maxrate") + 1], "3400k")
        self.assertEqual(args[args.index("-bufsize") + 1], "6800k")

    def test_nvenc_vbr_quality_knobs_are_configurable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            patched_env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "TEST_OUTPUT": "null",
                "TEST_OUTPUT_FILE": str(Path(td) / "capture.mkv"),
                "VIDEO_ENCODER": "h264_nvenc",
                "VIDEO_NVENC_PRESET": "p4",
                "VIDEO_NVENC_RC": "vbr",
                "VIDEO_NVENC_CQ": "17",
                "VIDEO_NVENC_MULTIPASS": "fullres",
                "VIDEO_NVENC_RC_LOOKAHEAD": "8",
                "VIDEO_NVENC_SPATIAL_AQ": "1",
                "VIDEO_NVENC_TEMPORAL_AQ": "1",
                "VIDEO_NVENC_BFRAMES": "2",
                "VIDEO_NVENC_B_REF_MODE": "middle",
            }
            with mock.patch.dict(os.environ, patched_env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                args = engine.ffmpeg_args(":99", "stream_sink.monitor")

        self.assertEqual(cfg.video_nvenc_rc, "vbr")
        self.assertEqual(args[args.index("-rc") + 1], "vbr")
        self.assertEqual(args[args.index("-cq") + 1], "17")
        self.assertEqual(args[args.index("-multipass") + 1], "fullres")
        self.assertEqual(args[args.index("-rc-lookahead") + 1], "8")
        self.assertIn("-spatial-aq", args)
        self.assertIn("-temporal-aq", args)
        self.assertEqual(args[args.index("-bf") + 1], "2")
        self.assertEqual(args[args.index("-b_ref_mode") + 1], "middle")

    def test_network_down_restart_context_uses_emergency_low_upload_profile(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restart_reason = Path(td) / "restart_reason.json"
            restart_reason.write_text(
                json.dumps(
                    {
                        "ts_utc": "1970-01-01T00:16:30Z",
                        "source": "fast_recovery",
                        "trigger": "network_down",
                        "reason": "network down: dns_ok=False tcp_probe_ok=False",
                    }
                ),
                encoding="utf-8",
            )
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "TEST_OUTPUT": "null",
                "RESTART_REASON_FILE": str(restart_reason),
                "EMERGENCY_LOW_UPLOAD_ENABLED": "1",
                "EMERGENCY_LOW_UPLOAD_DURATION_SEC": "900",
                "EMERGENCY_LOW_UPLOAD_VIDEO_BITRATE": "2500k",
                "EMERGENCY_LOW_UPLOAD_VIDEO_MAXRATE": "2500k",
                "EMERGENCY_LOW_UPLOAD_VIDEO_BUFSIZE": "5000k",
                "AUDIO_BITRATE": "192k",
            }
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(stream_engine.time, "time", return_value=1000):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                profile = engine.effective_encoder_profile()
                args = engine.ffmpeg_args(":99", "stream_sink.monitor", profile)

        self.assertEqual(profile["mode"], "emergency_low_upload")
        self.assertEqual(profile["trigger"], "network_down")
        self.assertEqual(args[args.index("-b:v") + 1], "2500k")
        self.assertEqual(args[args.index("-maxrate") + 1], "2500k")
        self.assertEqual(args[args.index("-bufsize") + 1], "5000k")
        self.assertEqual(args[args.index("-b:a") + 1], "192k")

    def test_low_upload_pressure_restart_context_uses_emergency_low_upload_profile(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restart_reason = Path(td) / "restart_reason.json"
            restart_reason.write_text(
                json.dumps(
                    {
                        "ts_utc": "1970-01-01T00:16:30Z",
                        "source": "fast_recovery",
                        "trigger": "low_upload_pressure",
                        "reason": "low upload pressure: send_mbps=2.0<=3.2",
                        "emergency_low_upload_profile": {
                            "name": "low_upload_pressure_low_upload",
                            "duration_sec": 900,
                            "video_bitrate": "2500k",
                            "video_maxrate": "2500k",
                            "video_bufsize": "5000k",
                        },
                    }
                ),
                encoding="utf-8",
            )
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "TEST_OUTPUT": "null",
                "RESTART_REASON_FILE": str(restart_reason),
                "EMERGENCY_LOW_UPLOAD_ENABLED": "1",
                "EMERGENCY_LOW_UPLOAD_TRIGGERS": "network_down,low_upload_pressure",
                "AUDIO_BITRATE": "192k",
            }
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(stream_engine.time, "time", return_value=1000):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                profile = engine.effective_encoder_profile()
                args = engine.ffmpeg_args(":99", "stream_sink.monitor", profile)

        self.assertEqual(profile["mode"], "emergency_low_upload")
        self.assertEqual(profile["trigger"], "low_upload_pressure")
        self.assertEqual(profile["name"], "low_upload_pressure_low_upload")
        self.assertEqual(args[args.index("-b:v") + 1], "2500k")
        self.assertEqual(args[args.index("-maxrate") + 1], "2500k")

    def test_expired_network_down_restart_context_uses_normal_encoder_profile(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restart_reason = Path(td) / "restart_reason.json"
            restart_reason.write_text(
                '{"ts_utc":"1970-01-01T00:00:00Z","source":"fast_recovery","trigger":"network_down"}',
                encoding="utf-8",
            )
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "TEST_OUTPUT": "null",
                "RESTART_REASON_FILE": str(restart_reason),
                "EMERGENCY_LOW_UPLOAD_ENABLED": "1",
                "EMERGENCY_LOW_UPLOAD_DURATION_SEC": "900",
                "VIDEO_BITRATE": "3500k",
                "VIDEO_MAXRATE": "3500k",
                "VIDEO_BUFSIZE": "7000k",
            }
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(stream_engine.time, "time", return_value=1000):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                profile = engine.effective_encoder_profile()
                args = engine.ffmpeg_args(":99", "stream_sink.monitor", profile)

        self.assertEqual(profile["mode"], "normal")
        self.assertEqual(args[args.index("-b:v") + 1], "3500k")
        self.assertEqual(args[args.index("-maxrate") + 1], "3500k")
        self.assertEqual(args[args.index("-bufsize") + 1], "7000k")

    @unittest.skipUnless(PROD_ENV.exists(), "/etc/default/adsb-streamnew is not present")
    def test_production_env_matches_repo_contract_for_runtime_sensitive_knobs(self) -> None:
        example = parse_env_file(ENV_EXAMPLE)
        production = parse_env_file(PROD_ENV)

        for key in (
            "FRAME_RATE",
            "VIDEO_BITRATE",
            "VIDEO_MAXRATE",
            "VIDEO_BUFSIZE",
            "AUDIO_FILTER",
            "STOP_FFMPEG_TERM_GRACE_SEC",
        ):
            self.assertEqual(production.get(key), example.get(key), key)


if __name__ == "__main__":
    unittest.main()
