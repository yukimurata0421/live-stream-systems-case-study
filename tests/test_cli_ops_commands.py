from __future__ import annotations

import json
import contextlib
import io
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "stream_core"))

import cli  # type: ignore


def cp(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["cmd"], returncode=returncode, stdout=stdout, stderr=stderr)


class CliOpsCommandsTests(unittest.TestCase):
    def test_ops_history_filters_by_day_and_body_text(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "2026-05-08_01_stream.md").write_text("# Stream\nremote_sample_id\n", encoding="utf-8")
            (root / "2026-05-07_01_api.md").write_text("# API\nquota\n", encoding="utf-8")

            entries = cli._ops_history_entries(root, day="2026-05-08", grep_text="remote_sample")

        self.assertEqual([p.name for p in entries], ["2026-05-08_01_stream.md"])

    def test_history_command_reads_ops_logs_and_routine_checks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ops_dir = root / "docs" / "50_ops_logs"
            routine_dir = root / "docs" / "45_routine_checks"
            ops_dir.mkdir(parents=True)
            routine_dir.mkdir(parents=True)
            (ops_dir / "2026-05-24_06_ops_change.md").write_text("# Ops Change\n", encoding="utf-8")
            (routine_dir / "2026-05-24_03_routine_check_health.md").write_text(
                "# Routine Check\nhealth\n",
                encoding="utf-8",
            )

            out = io.StringIO()
            with mock.patch.object(cli, "BASE_DIR", root):
                with mock.patch.object(cli, "OPS_LOG_DIR", ops_dir):
                    with mock.patch.object(cli, "ROUTINE_CHECK_DIR", routine_dir):
                        with contextlib.redirect_stdout(out):
                            rc = cli.history(limit=10, grep_text="health")

        self.assertEqual(rc, 0)
        output = out.getvalue()
        self.assertIn("docs/45_routine_checks/2026-05-24_03_routine_check_health.md", output)
        self.assertNotIn("docs/50_ops_logs/2026-05-24_06_ops_change.md", output)

    def test_history_command_reads_v3_and_v2_split_docs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            v3_ops_dir = root / "docs" / "v3" / "50_ops_logs"
            v2_ops_dir = root / "docs" / "v2" / "50_ops_logs"
            routine_dir = root / "docs" / "v2" / "45_routine_checks"
            v3_ops_dir.mkdir(parents=True)
            v2_ops_dir.mkdir(parents=True)
            routine_dir.mkdir(parents=True)
            (v3_ops_dir / "2026-05-29_01_v3.md").write_text("# V3\nneedle\n", encoding="utf-8")
            (v2_ops_dir / "2026-05-25_01_v2.md").write_text("# V2\nneedle\n", encoding="utf-8")
            (routine_dir / "2026-05-24_03_routine.md").write_text("# Routine\nneedle\n", encoding="utf-8")

            out = io.StringIO()
            with mock.patch.object(cli, "BASE_DIR", root):
                with mock.patch.object(cli, "OPS_LOG_DIR", v3_ops_dir):
                    with mock.patch.object(cli, "ROUTINE_CHECK_DIR", routine_dir):
                        with contextlib.redirect_stdout(out):
                            rc = cli.history(limit=10, grep_text="needle")

        self.assertEqual(rc, 0)
        output = out.getvalue()
        self.assertIn("docs/v3/50_ops_logs/2026-05-29_01_v3.md", output)
        self.assertIn("docs/v2/50_ops_logs/2026-05-25_01_v2.md", output)
        self.assertIn("docs/v2/45_routine_checks/2026-05-24_03_routine.md", output)

    def test_api_usage_open_day_uses_pt_report_defaults(self) -> None:
        payload = {
            "status": "ok",
            "target_day": "2026-05-08",
            "window": {"tz": "America/Los_Angeles", "open_day": True},
            "totals": {"calls": 2, "units": 101, "quota_exceeded_events": 0},
            "by_method": {"search.list": {"calls": 1, "units": 100}},
            "by_source": {"youtube_api": 2},
            "ingest": {
                "coverage_ok": True,
                "coverage_observed_ratio": 0.5,
                "coverage_gap_start_ratio": 0.0,
                "coverage_gap_end_ratio": 0.1,
            },
        }
        calls: list[list[str]] = []

        def fake_run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
            calls.append(cmd)
            return cp(0, stdout=json.dumps(payload))

        with mock.patch("cli.run", side_effect=fake_run):
            with mock.patch(
                "cli.read_json_file",
                side_effect=[
                    {"quota_exhausted": False},
                    {
                        "api_cost_burn_rate_active": False,
                        "api_cost_projected_units_per_day": 1200,
                        "api_cost_threshold_units_per_day": 9000,
                    },
                ],
            ):
                with mock.patch("builtins.print") as printed:
                    rc = cli.api_usage()

        self.assertEqual(rc, 0)
        self.assertIn("--include-open-day", calls[0])
        self.assertIn("America/Los_Angeles", calls[0])
        output = "\n".join(str(call.args[0]) for call in printed.call_args_list)
        self.assertIn("target_day=2026-05-08", output)
        self.assertIn("units=101", output)
        self.assertIn("projected_units_per_day=1200", output)

    def test_api_usage_json_combines_report_quota_and_burn_guard(self) -> None:
        payload = {
            "status": "ok",
            "target_day": "2026-05-08",
            "window": {"tz": "America/Los_Angeles", "open_day": True},
            "totals": {"calls": 1, "units": 1, "quota_exceeded_events": 0},
            "ingest": {},
        }
        with mock.patch("cli.run", return_value=cp(0, stdout=json.dumps(payload))):
            with mock.patch(
                "cli.read_json_file",
                side_effect=[
                    {"quota_exhausted": False},
                    {"api_cost_burn_rate_active": False, "judgment": "ok"},
                ],
            ):
                with mock.patch("builtins.print") as printed:
                    rc = cli.api_usage(json_output=True)

        self.assertEqual(rc, 0)
        combined = json.loads(str(printed.call_args.args[0]))
        self.assertEqual(combined["api_cost_report"]["target_day"], "2026-05-08")
        self.assertFalse(combined["quota_state"]["quota_exhausted"])
        self.assertEqual(combined["watchdog_stats"]["judgment"], "ok")

    def test_remote_warning_comparison_payload_includes_watchdog_context(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            log_dir = Path(td)
            (log_dir / "fast_recovery_events.jsonl").write_text(
                json.dumps(
                    {
                        "ts_utc": "2026-05-09T09:00:00Z",
                        "kind": "restart",
                        "trigger": "remote_warning",
                        "message": "youtube pre-loss warning while broadcast live: streamStatus=inactive",
                        "ffmpeg_pid": 123,
                        "metrics": {
                            "bytes_sent_delta": 3292760,
                            "lastsnd_ms": 12,
                            "notsent": 0,
                            "unacked": 33,
                            "network_down": False,
                            "remote_warning": True,
                        },
                        "youtube_hint": {
                            "api_live_state": "live",
                            "oauth_stream_status": "inactive",
                            "oauth_stream_health_status": "noData",
                            "remote_source": "data_api_oauth",
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (log_dir / "youtube_watchdog.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts_utc": "2026-05-09T08:55:00Z",
                                "status": "ok",
                                "oauth_stream_status": "active",
                                "oauth_stream_health_status": "good",
                            }
                        ),
                        json.dumps(
                            {
                                "ts_utc": "2026-05-09T09:05:00Z",
                                "status": "ok",
                                "oauth_stream_status": "active",
                                "oauth_stream_health_status": "good",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            payload = cli._remote_warning_comparison_payload(
                log_dir=log_dir,
                hours=24,
                limit=5,
                now_ts=cli.parse_utc_ts("2026-05-09T10:00:00Z"),
            )

        self.assertEqual(payload["remote_warning_restart_count"], 1)
        event = payload["events"][0]
        self.assertEqual(event["metrics"]["bytes_sent_delta"], 3292760)
        self.assertEqual(event["youtube_hint"]["oauth_stream_status"], "inactive")
        self.assertEqual(event["youtube_watchdog_before"]["status"], "ok")
        self.assertEqual(event["youtube_watchdog_after"]["oauth_stream_health_status"], "good")

    def test_health_summary_runs_observe_for_multiple_windows(self) -> None:
        payload = {
            "pass": True,
            "checks": {"current_fail": False, "historical_degraded": False},
            "remote_warning_restart_count_1h": 0,
            "remote_warning_restart_count_24h": 1,
            "remote_warning_restart_judgment": "ok_single_or_none",
            "public_probe_degraded_count_1h": 1,
            "public_probe_degraded_count_24h": 2,
            "public_probe_authoritative_live_ok_count_24h": 2,
            "public_probe_judgment": "observe_public_probe_noise_authoritative_live_ok",
            "stream_engine_ffmpeg_exit_224_count_1h": 0,
            "stream_engine_ffmpeg_exit_224_count_24h": 1,
            "stream_engine_ffmpeg_exit_224_judgment": "ok_single_or_none",
        }
        calls: list[list[str]] = []

        def fake_run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
            calls.append(cmd)
            return cp(0, stdout=json.dumps(payload))

        with mock.patch("cli.run", side_effect=fake_run):
            with mock.patch("builtins.print") as printed:
                rc = cli.health_summary(windows="1,8", json_output=False)

        self.assertEqual(rc, 0)
        self.assertEqual([cmd[-1] for cmd in calls], ["1", "8"])
        output = "\n".join(str(call.args[0]) for call in printed.call_args_list)
        self.assertIn("window=1h", output)
        self.assertIn("remote_warning_24h=1", output)
        self.assertIn("public_probe_24h=2", output)
        self.assertIn("public_probe_live_ok_24h=2", output)
        self.assertIn("exit224_24h=1", output)

    def test_maintenance_on_stops_monitors_but_not_stream_or_dj(self) -> None:
        active = set(cli.MAINTENANCE_TIMERS) | set(cli.MAINTENANCE_SERVICES) | {
            cli.STREAM_SERVICE,
            cli.DJ_SERVICE,
            cli.NOTIFY_TIMER,
        }
        stopped: list[str] = []

        def fake_is_active(unit: str) -> bool:
            return unit in active

        def fake_stop(unit: str) -> bool:
            stopped.append(unit)
            active.discard(unit)
            return True

        with tempfile.TemporaryDirectory() as td:
            state_file = Path(td) / "maintenance_mode.json"
            with mock.patch.object(cli, "MAINTENANCE_STATE_FILE", state_file):
                with mock.patch("cli.unit_installed", return_value=True):
                    with mock.patch("cli.is_active", side_effect=fake_is_active):
                        with mock.patch("cli.stop_unit", side_effect=fake_stop):
                            with mock.patch("builtins.print"):
                                rc = cli.maintenance_on()

            state = json.loads(state_file.read_text(encoding="utf-8"))

        self.assertEqual(rc, 0)
        self.assertTrue(state["active"])
        self.assertEqual(stopped, list(cli.MAINTENANCE_TIMERS) + list(cli.MAINTENANCE_SERVICES))
        self.assertIn(cli.STREAM_SERVICE, active)
        self.assertIn(cli.DJ_SERVICE, active)
        self.assertIn(cli.NOTIFY_TIMER, active)

    def test_maintenance_off_starts_only_timers(self) -> None:
        active: set[str] = set()
        started: list[str] = []

        def fake_is_active(unit: str) -> bool:
            return unit in active

        def fake_start(unit: str) -> bool:
            started.append(unit)
            active.add(unit)
            return True

        with tempfile.TemporaryDirectory() as td:
            state_file = Path(td) / "maintenance_mode.json"
            state_file.write_text(json.dumps({"active": True, "last_action": "on"}), encoding="utf-8")
            with mock.patch.object(cli, "MAINTENANCE_STATE_FILE", state_file):
                with mock.patch("cli.unit_installed", return_value=True):
                    with mock.patch("cli.is_active", side_effect=fake_is_active):
                        with mock.patch("cli.start_unit", side_effect=fake_start):
                            with mock.patch("builtins.print"):
                                rc = cli.maintenance_off()

            state = json.loads(state_file.read_text(encoding="utf-8"))

        self.assertEqual(rc, 0)
        self.assertFalse(state["active"])
        self.assertEqual(started, list(cli.MAINTENANCE_TIMERS))
        self.assertNotIn(cli.STREAM_SERVICE, started)
        self.assertNotIn(cli.DJ_SERVICE, started)

    def test_maintenance_short_alias_guard_classification(self) -> None:
        self.assertTrue(cli.command_requires_mutating_systemd("m", "on"))
        self.assertTrue(cli.command_requires_mutating_systemd("maint", "off"))
        self.assertTrue(cli.command_requires_mutating_systemd("pause"))
        self.assertTrue(cli.command_requires_mutating_systemd("resume"))
        self.assertFalse(cli.command_requires_mutating_systemd("m", "status"))
        self.assertFalse(cli.command_requires_mutating_systemd("m", "s"))

    def test_maintenance_short_alias_dispatches_to_status(self) -> None:
        argv = ["stream-new", "m", "s"]
        with mock.patch.object(sys, "argv", argv):
            with mock.patch("cli.maintenance_status", return_value=0) as status:
                rc = cli.main()

        self.assertEqual(rc, 0)
        status.assert_called_once_with(json_output=False)

    def test_maintenance_top_level_action_rejects_extra_argument(self) -> None:
        argv = ["stream-new", "pause", "off"]
        with mock.patch.object(sys, "argv", argv):
            with mock.patch("builtins.print") as printed:
                rc = cli.main()

        self.assertEqual(rc, 2)
        self.assertEqual(str(printed.call_args.args[0]), "[error] usage: stream pause")

    def test_stream1090_report_payload_is_report_only_and_detects_movement(self) -> None:
        aircraft_calls = 0

        def fake_text(url: str, timeout: float) -> str:
            self.assertIn("/stream1090/", url)
            return "<html><script>Leaflet tar1090 map</script></html>"

        def fake_json(url: str, timeout: float) -> object:
            nonlocal aircraft_calls
            if url.endswith("/outline.json"):
                return {"actualRange": {"last24h": {"points": [[1.0, 2.0, 30000]]}}}
            if url.endswith("/aircraft.json"):
                aircraft_calls += 1
                lon = 2.0 if aircraft_calls == 1 else 2.1
                messages = 100 if aircraft_calls == 1 else 110
                return {"messages": messages, "aircraft": [{"hex": "abc123", "lat": 1.0, "lon": lon}]}
            raise AssertionError(url)

        payload = cli._stream1090_report_payload(
            base_url="http://example.test",
            sample_sec=0,
            timeout=1,
            sleep_func=lambda _sec: None,
            fetch_text_func=fake_text,
            fetch_json_func=fake_json,
        )

        self.assertEqual(payload["mode"], "report_only")
        self.assertFalse(payload["affects_restart"])
        self.assertEqual(payload["judgment"], "report_only_ok")
        self.assertEqual(payload["checks"]["actual_range_points"], 1)
        self.assertEqual(payload["checks"]["position_change_count"], 1)
        self.assertEqual(payload["checks"]["messages_delta"], 10)

    def test_stream1090_report_payload_retries_single_static_aircraft_sample(self) -> None:
        aircraft_calls = 0
        sleeps: list[float] = []

        def fake_text(url: str, timeout: float) -> str:
            return "<html><script>Leaflet tar1090 map</script></html>"

        def fake_json(url: str, timeout: float) -> object:
            nonlocal aircraft_calls
            if url.endswith("/outline.json"):
                return {"actualRange": {"last24h": {"points": [[1.0, 2.0, 30000]]}}}
            if url.endswith("/aircraft.json"):
                aircraft_calls += 1
                messages = 100 if aircraft_calls < 3 else 108
                return {"messages": messages, "aircraft": [{"hex": "abc123", "lat": 1.0, "lon": 2.0}]}
            raise AssertionError(url)

        payload = cli._stream1090_report_payload(
            base_url="http://example.test",
            sample_sec=5,
            timeout=1,
            sleep_func=lambda sec: sleeps.append(sec),
            fetch_text_func=fake_text,
            fetch_json_func=fake_json,
        )

        self.assertEqual(payload["judgment"], "report_only_ok")
        self.assertEqual(payload["checks"]["messages_delta"], 8)
        self.assertEqual(payload["checks"]["movement_retry_count"], 1)
        self.assertEqual(payload["checks"]["movement_sample_elapsed_sec"], 10)
        self.assertNotIn("aircraft_messages_and_positions_not_moving_in_sample", payload["warnings"])
        self.assertEqual(sleeps, [5, 5])

    def test_upstream_report_payload_accepts_direct_stream1090_url_path(self) -> None:
        fetched: list[str] = []
        aircraft_calls = 0

        def fake_text(url: str, timeout: float) -> str:
            fetched.append(url)
            return "<html><script>Leaflet tar1090 map</script></html>"

        def fake_json(url: str, timeout: float) -> object:
            nonlocal aircraft_calls
            fetched.append(url)
            if url.endswith("/outline.json"):
                return {"actualRange": {"last24h": {"points": [[1.0, 2.0, 30000]]}}}
            if url.endswith("/aircraft.json"):
                aircraft_calls += 1
                return {"messages": 100 + aircraft_calls, "aircraft": [{"hex": "abc123", "lat": 1.0, "lon": 2.0}]}
            raise AssertionError(url)

        base_url, map_path = cli.split_url_root_and_path("http://upstream.test/stream1090/", "/stream1090/")
        payload = cli._stream1090_report_payload(
            base_url=base_url,
            map_path=map_path,
            target="upstream_readsb_tar1090_stream1090",
            sample_sec=0,
            timeout=1,
            sleep_func=lambda _sec: None,
            fetch_text_func=fake_text,
            fetch_json_func=fake_json,
        )

        self.assertEqual(base_url, "http://upstream.test")
        self.assertEqual(map_path, "/stream1090/")
        self.assertEqual(payload["target"], "upstream_readsb_tar1090_stream1090")
        self.assertFalse(payload["affects_stream_restart"])
        self.assertIn("http://upstream.test/stream1090/", fetched)
        self.assertIn("http://upstream.test/stream1090/data/aircraft.json", fetched)

    def test_report_history_summary_counts_warn_rate_and_alert(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            log_file = Path(td) / "upstream_stream1090_report.jsonl"
            now = cli.time.strftime("%Y-%m-%dT%H:%M:%SZ", cli.time.gmtime())
            rows = [
                {"ts_utc": now, "target": "upstream_readsb_tar1090_stream1090", "judgment": "report_only_ok"},
                {
                    "ts_utc": now,
                    "target": "upstream_readsb_tar1090_stream1090",
                    "judgment": "report_only_warn",
                    "warnings": ["aircraft_messages_and_positions_not_moving_in_sample"],
                },
            ]
            log_file.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

            summary = cli._report_history_summary(
                log_file,
                target="upstream_readsb_tar1090_stream1090",
                include_payload={
                    "ts_utc": now,
                    "target": "upstream_readsb_tar1090_stream1090",
                    "judgment": "report_only_warn",
                    "warnings": ["visual_probe_warn"],
                },
            )

        self.assertEqual(summary["sample_count"], 3)
        self.assertEqual(summary["warn_count"], 2)
        self.assertTrue(summary["alert"])
        self.assertEqual(summary["warning_counts"]["visual_probe_warn"], 1)

    def test_stream1090_visual_probe_warning_is_report_only(self) -> None:
        def fake_text(url: str, timeout: float) -> str:
            return "<html><script>Leaflet tar1090 map</script></html>"

        def fake_json(url: str, timeout: float) -> object:
            if url.endswith("/outline.json"):
                return {"actualRange": {"last24h": {"points": [[1.0, 2.0, 30000]]}}}
            if url.endswith("/aircraft.json"):
                return {"messages": 100, "aircraft": [{"hex": "abc123", "lat": 1.0, "lon": 2.0}]}
            raise AssertionError(url)

        payload = cli._stream1090_report_payload(
            base_url="http://example.test",
            sample_sec=0,
            timeout=1,
            sleep_func=lambda _sec: None,
            fetch_text_func=fake_text,
            fetch_json_func=fake_json,
            visual=True,
            visual_fetch_func=lambda **_kwargs: {
                "enabled": True,
                "available": True,
                "judgment": "visual_probe_warn",
                "warnings": ["tile_dom_markers_missing"],
            },
        )

        self.assertEqual(payload["mode"], "report_only")
        self.assertFalse(payload["affects_stream_restart"])
        self.assertEqual(payload["judgment"], "report_only_warn")
        self.assertIn("visual_probe_warn", payload["warnings"])

    def test_visual_probe_writes_screenshot_via_temp_then_state_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            screenshot_dir = Path(td) / ".state" / "stream1090_visual"

            def fake_run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
                screenshot_args = [part for part in cmd if part.startswith("--screenshot=")]
                if screenshot_args:
                    Path(screenshot_args[0].split("=", 1)[1]).write_bytes(b"png-bytes")
                    return cp(0, stdout="10 bytes written")
                if "--dump-dom" in cmd:
                    return cp(0, stdout='<div class="ol-viewport"><div class="ol-layer"><canvas></canvas></div></div>')
                return cp(1, stderr="unexpected command")

            with mock.patch("cli._chromium_binary", return_value="/bin/chromium"):
                with mock.patch("cli.run", side_effect=fake_run):
                    with mock.patch("cli._screenshot_mean_luma", return_value=42):
                        payload = cli._visual_probe_payload(
                            page_url="http://example.test/stream1090/",
                            target="overlay_stream1090",
                            timeout=1,
                            screenshot_dir=screenshot_dir,
                        )

            screenshot_path = Path(payload["screenshot_path"])
            self.assertEqual(payload["judgment"], "visual_probe_ok")
            self.assertTrue(screenshot_path.exists())
            self.assertEqual(payload["screenshot_bytes"], len(b"png-bytes"))
            self.assertGreater(payload["tile_dom_count"], 0)

    def test_needrestart_contract_status_requires_stream_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conf = Path(td) / "stream-24x7.conf"
            conf.write_text(
                "$nrconf{override_rc}{qr(^adsb-streamnew-(youtube-stream|auto-dj)\\.service$)} = 0;\n",
                encoding="utf-8",
            )
            ok = cli.needrestart_contract_status(conf)
            conf.write_text("# missing override\n", encoding="utf-8")
            bad = cli.needrestart_contract_status(conf)

        self.assertTrue(ok["ok"])
        self.assertFalse(bad["ok"])

    def test_stream_ingest_endpoint_status_prefers_rtmps_443(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = Path(td) / "adsb-streamnew"
            env.write_text(
                "STREAM_KEY=REPLACE_WITH_TEST_STREAM_KEY\n"
                "RTMP_URL=rtmps://a.rtmps.youtube.com:443/live2\n",
                encoding="utf-8",
            )

            status = cli.stream_ingest_endpoint_status(env)

        self.assertTrue(status["ok"])
        self.assertEqual(status["judgment"], "rtmps_preferred")
        self.assertEqual(status["scheme"], "rtmps")
        self.assertEqual(status["host"], "a.rtmps.youtube.com")
        self.assertEqual(status["port"], 443)

    def test_stream_ingest_endpoint_status_flags_legacy_rtmp_without_failing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = Path(td) / "adsb-streamnew"
            env.write_text(
                "STREAM_KEY=REPLACE_WITH_TEST_STREAM_KEY\n"
                "RTMP_URL=rtmp://a.rtmp.youtube.com/live2\n",
                encoding="utf-8",
            )

            status = cli.stream_ingest_endpoint_status(env)

        self.assertTrue(status["ok"])
        self.assertEqual(status["judgment"], "rtmp_legacy")
        self.assertEqual(status["preferred_url"], "rtmps://a.rtmps.youtube.com:443/live2")

    def test_guard_start_safety_blocks_invalid_ingest_endpoint(self) -> None:
        def fake_read_env_file(_path: Path) -> dict[str, str]:
            return {
                "STREAM_KEY": "REPLACE_WITH_TEST_STREAM_KEY",
                "RTMP_URL": "rtmps://example.invalid/live2",
                "TEST_MODE": "0",
                "DISPLAY_NAME": ":101",
            }

        ctx = cli.runtime_safety_command.RuntimeSafetyContext(
            base_dir=Path("/tmp/stream-v2-test"),
            stream_service=cli.STREAM_SERVICE,
            legacy_stream_service=cli.LEGACY_STREAM_SERVICE,
            read_env_file=fake_read_env_file,
            run=cli.run,
            run_systemctl=cli.run_systemctl,
            is_active=lambda _unit: False,
        )
        with mock.patch("builtins.print") as printed:
            rc = cli.runtime_safety_command.guard_start_safety(ctx)

        self.assertEqual(rc, 1)
        lines = [" ".join(str(part) for part in call.args) for call in printed.call_args_list]
        self.assertTrue(any("RTMP_URL host is not a known YouTube ingest host" in line for line in lines))

    def test_guard_start_safety_allows_placeholder_stream_key_in_test_mode(self) -> None:
        def fake_read_env_file(_path: Path) -> dict[str, str]:
            return {
                "STREAM_KEY": "YOUR_STREAM_KEY",
                "RTMP_URL": "rtmps://a.rtmps.youtube.com:443/live2",
                "TEST_MODE": "1",
                "DISPLAY_NAME": ":101",
            }

        ctx = cli.runtime_safety_command.RuntimeSafetyContext(
            base_dir=Path("/tmp/stream-v2-test"),
            stream_service=cli.STREAM_SERVICE,
            legacy_stream_service=cli.LEGACY_STREAM_SERVICE,
            read_env_file=fake_read_env_file,
            run=cli.run,
            run_systemctl=cli.run_systemctl,
            is_active=lambda _unit: False,
        )

        self.assertEqual(cli.runtime_safety_command.guard_start_safety(ctx), 0)

    def test_contract_check_json_uses_diagnostics_payload(self) -> None:
        result = cli.doctor_command.suite.CheckResult(
            name="ingest:youtube_endpoint",
            category="ingest_contract",
            severity="ok",
            ok=True,
            fatal=False,
            summary="ingest endpoint: RTMPS ingest endpoint uses explicit port 443",
        )
        buf = io.StringIO()
        with mock.patch.object(cli.doctor_command.suite, "collect_contract_results", return_value=[result]):
            with contextlib.redirect_stdout(buf):
                rc = cli.contract_check(json_output=True)

        payload = json.loads(buf.getvalue())
        self.assertEqual(rc, 0)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["mode"], "contract_check")
        self.assertEqual(payload["checks"][0]["name"], "ingest:youtube_endpoint")

    def test_collect_notification_incidents_includes_api_and_report_axes(self) -> None:
        now_ts = cli.parse_utc_ts("2026-05-10T00:00:00Z")
        observe_payload = {
            "pass": True,
            "checks": {"current_fail": False, "youtube_current_degraded": False, "fast_mode_current_active": False},
            "api_report_judgment": "api_open_day_report_stale",
            "api_report_judgment_reason": "open_day_latest.json is stale",
            "fast_mode_judgment": "ok_none",
            "encoder_gap_enable_auto_stop_false_judgment": "ok_none",
            "remote_warning_restart_judgment": "ok_single_or_none",
            "stream_engine_ffmpeg_exit_224_judgment": "ok_single_or_none",
            "public_probe_judgment": "ok_none",
            "watchdog_restart_reasons": {},
            "fast_recovery_restart_triggers": {},
            "stream_engine_ffmpeg_exit_224_count_1h": 0,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stream_log = root / "stream1090_report.jsonl"
            upstream_log = root / "upstream_stream1090_report.jsonl"
            stream_log.write_text(
                json.dumps(
                    {
                        "ts_utc": "2026-05-09T23:59:30Z",
                        "target": "overlay_stream1090",
                        "judgment": "report_only_warn",
                        "warnings": ["actual_range_points_missing"],
                        "checks": {"position_change_count": 0, "messages_delta": 0},
                        "baseline": {"warn_rate": 1.0, "alert": True},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            upstream_log.write_text(
                json.dumps(
                    {
                        "ts_utc": "2026-05-09T23:59:30Z",
                        "target": "upstream_readsb_tar1090_stream1090",
                        "judgment": "report_only_ok",
                        "warnings": [],
                        "checks": {"position_change_count": 1, "messages_delta": 10},
                        "baseline": {"warn_rate": 0.0, "alert": False},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            with mock.patch("cli._observe_payload", return_value=(0, observe_payload, "")):
                with mock.patch.object(cli, "STREAM1090_REPORT_EVENTS_FILE", stream_log):
                    with mock.patch.object(cli, "UPSTREAM_REPORT_EVENTS_FILE", upstream_log):
                        incidents = cli.collect_notification_incidents(now_ts=now_ts, report_stale_sec=1800)

        incident_ids = {item["id"] for item in incidents}
        self.assertIn("api_report:freshness_or_timer", incident_ids)
        self.assertIn("stream1090:overlay_report", incident_ids)
        self.assertNotIn("stream1090:upstream_report", incident_ids)

    def test_collect_notification_incidents_suppresses_single_report_only_movement_noise(self) -> None:
        now_ts = cli.parse_utc_ts("2026-05-10T00:00:00Z")
        observe_payload = {
            "pass": True,
            "checks": {"current_fail": False, "youtube_current_degraded": False, "fast_mode_current_active": False},
            "api_report_judgment": "ok",
            "fast_mode_judgment": "ok_none",
            "encoder_gap_enable_auto_stop_false_judgment": "ok_none",
            "remote_warning_restart_judgment": "ok_single_or_none",
            "stream_engine_ffmpeg_exit_224_judgment": "ok_single_or_none",
            "public_probe_judgment": "ok_none",
            "watchdog_restart_reasons": {},
            "fast_recovery_restart_triggers": {},
            "stream_engine_ffmpeg_exit_224_count_1h": 0,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stream_log = root / "stream1090_report.jsonl"
            upstream_log = root / "upstream_stream1090_report.jsonl"
            movement_warn = {
                "ts_utc": "2026-05-09T23:59:30Z",
                "judgment": "report_only_warn",
                "warnings": ["aircraft_messages_and_positions_not_moving_in_sample"],
                "checks": {"position_change_count": 0, "messages_delta": 0},
                "baseline": {"warn_rate": 0.01, "alert": False},
            }
            stream_log.write_text(
                json.dumps({**movement_warn, "target": "overlay_stream1090"}) + "\n",
                encoding="utf-8",
            )
            upstream_log.write_text(
                json.dumps({**movement_warn, "target": "upstream_readsb_tar1090_stream1090"}) + "\n",
                encoding="utf-8",
            )
            with mock.patch("cli._observe_payload", return_value=(0, observe_payload, "")):
                with mock.patch.object(cli, "STREAM1090_REPORT_EVENTS_FILE", stream_log):
                    with mock.patch.object(cli, "UPSTREAM_REPORT_EVENTS_FILE", upstream_log):
                        incidents = cli.collect_notification_incidents(now_ts=now_ts, report_stale_sec=1800)

        incident_ids = {item["id"] for item in incidents}
        self.assertNotIn("stream1090:overlay_report", incident_ids)
        self.assertNotIn("stream1090:upstream_report", incident_ids)

    def test_collect_notification_incidents_suppresses_bootstrap_missing_state_gaps(self) -> None:
        now_ts = cli.parse_utc_ts("2026-05-16T13:48:46Z")
        observe_payload = {
            "pass": False,
            "checks": {
                "current_fail": True,
                "youtube_current_degraded": False,
                "youtube_observability_current_fail": False,
                "fast_mode_current_active": False,
                "youtube_current_status": "",
                "youtube_current_judgment": "",
                "youtube_stats_stale": True,
                "pulse_pass": True,
            },
            "api_report_judgment": "api_open_day_report_stale",
            "api_report_judgment_reason": "open_day_latest.json is stale or missing",
            "fast_mode_judgment": "ok_none",
            "encoder_gap_enable_auto_stop_false_judgment": "ok_none",
            "remote_warning_restart_judgment": "ok_single_or_none",
            "stream_engine_ffmpeg_exit_224_judgment": "ok_single_or_none",
            "public_probe_judgment": "ok_none",
            "watchdog_restart_reasons": {},
            "fast_recovery_restart_triggers": {},
            "stream_engine_ffmpeg_exit_224_count_1h": 0,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with mock.patch("cli._observe_payload", return_value=(0, observe_payload, "")):
                with mock.patch.object(cli, "STREAM1090_REPORT_EVENTS_FILE", root / "missing_stream1090.jsonl"):
                    with mock.patch.object(cli, "UPSTREAM_REPORT_EVENTS_FILE", root / "missing_upstream.jsonl"):
                        with mock.patch("cli.notify_bootstrap_grace_active", return_value=True):
                            incidents = cli.collect_notification_incidents(
                                now_ts=now_ts,
                                report_stale_sec=1800,
                                startup_grace_sec=300,
                            )

        self.assertEqual(incidents, [])

    def test_collect_notification_incidents_skips_recovered_rtmps_and_public_probe_history(self) -> None:
        now_ts = cli.parse_utc_ts("2026-05-11T13:46:12Z")
        observe_payload = {
            "pass": True,
            "checks": {
                "current_fail": False,
                "youtube_current_degraded": False,
                "youtube_observability_current_fail": False,
                "fast_mode_current_active": False,
            },
            "api_report_judgment": "ok",
            "fast_mode_current_active": False,
            "fast_mode_judgment": "ok_short_fast_mode_episode",
            "encoder_gap_enable_auto_stop_false_judgment": "ok_none",
            "remote_warning_restart_judgment": "ok_single_or_none",
            "stream_engine_ffmpeg_exit_224_judgment": "ok_single_or_none",
            "rtmps_ssl_tls_judgment": "investigate_rtmps_ssl_tls_immediate",
            "rtmps_ssl_tls_count_1h": 17,
            "rtmps_ssl_tls_count_24h": 24,
            "journal_ssl_tls": {"count_24h": 24},
            "stream_engine_ffmpeg_ssl_tls_count_24h": 0,
            "fast_recovery_ssl_tls_count_24h": 0,
            "public_probe_judgment": "observe_public_probe_noise_clustered",
            "public_probe_degraded_count_1h": 5,
            "public_probe_degraded_count_24h": 12,
            "public_probe_authoritative_live_ok_count_24h": 0,
            "watchdog_restart_reasons": {},
            "fast_recovery_restart_triggers": {},
            "stream_engine_ffmpeg_exit_224_count_1h": 0,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stream_log = root / "stream1090_report.jsonl"
            upstream_log = root / "upstream_stream1090_report.jsonl"
            stream_log.write_text("", encoding="utf-8")
            upstream_log.write_text("", encoding="utf-8")
            with mock.patch("cli._observe_payload", return_value=(0, observe_payload, "")):
                with mock.patch.object(cli, "STREAM1090_REPORT_EVENTS_FILE", stream_log):
                    with mock.patch.object(cli, "UPSTREAM_REPORT_EVENTS_FILE", upstream_log):
                        incidents = cli.collect_notification_incidents(now_ts=now_ts, report_stale_sec=1800)

        incident_ids = {item["id"] for item in incidents}
        self.assertNotIn("rtmps:ssl_tls_specific_event", incident_ids)
        self.assertNotIn("public_probe:429_or_bot_confirmation_repeated", incident_ids)

    def test_collect_notification_incidents_skips_recovered_encoder_gap_history(self) -> None:
        now_ts = cli.parse_utc_ts("2026-05-18T09:54:00Z")
        observe_payload = {
            "pass": True,
            "checks": {
                "current_fail": False,
                "youtube_current_degraded": False,
                "youtube_observability_current_fail": False,
                "fast_mode_current_active": False,
            },
            "api_report_judgment": "ok",
            "fast_mode_current_active": False,
            "fast_mode_judgment": "ok_short_fast_mode_episode",
            "encoder_gap_enable_auto_stop_false_judgment": "observe_encoder_gap_viewer_state",
            "encoder_gap_enable_auto_stop_false_sample_count_24h": 3,
            "encoder_gap_enable_auto_stop_false_duration_sec_24h": 345,
            "remote_warning_restart_judgment": "ok_single_or_none",
            "stream_engine_ffmpeg_exit_224_judgment": "ok_single_or_none",
            "public_probe_judgment": "ok_none",
            "watchdog_restart_reasons": {},
            "fast_recovery_restart_triggers": {},
            "stream_engine_ffmpeg_exit_224_count_1h": 0,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stats_file = root / "youtube_watchdog_stats.json"
            stats_file.write_text(
                json.dumps(
                    {
                        "stats_file_updated_at_utc": "2026-05-18T09:53:52Z",
                        "oauth_enable_auto_stop": False,
                        "api_live_state": "live",
                        "oauth_life_cycle_status": "live",
                        "stream_active": True,
                        "ingest_connected": True,
                        "local_ok": True,
                        "ffmpeg_pid": 2670753,
                    }
                ),
                encoding="utf-8",
            )
            stream_log = root / "stream1090_report.jsonl"
            upstream_log = root / "upstream_stream1090_report.jsonl"
            stream_log.write_text("", encoding="utf-8")
            upstream_log.write_text("", encoding="utf-8")
            with mock.patch("cli._observe_payload", return_value=(0, observe_payload, "")):
                with mock.patch.object(cli, "STREAM1090_REPORT_EVENTS_FILE", stream_log):
                    with mock.patch.object(cli, "UPSTREAM_REPORT_EVENTS_FILE", upstream_log):
                        with mock.patch.object(cli, "YOUTUBE_WATCHDOG_STATS_FILE", stats_file):
                            incidents = cli.collect_notification_incidents(now_ts=now_ts, report_stale_sec=1800)

        incident_ids = {item["id"] for item in incidents}
        self.assertNotIn("youtube:enable_auto_stop_false_encoder_gap", incident_ids)

    def test_collect_notification_incidents_keeps_current_encoder_gap(self) -> None:
        now_ts = cli.parse_utc_ts("2026-05-18T09:54:00Z")
        observe_payload = {
            "pass": False,
            "checks": {
                "current_fail": True,
                "youtube_current_status": "warn",
                "youtube_current_judgment": "ng",
                "youtube_stats_stale": False,
                "pulse_pass": True,
                "fast_mode_current_active": False,
            },
            "api_report_judgment": "ok",
            "fast_mode_current_active": False,
            "fast_mode_judgment": "ok_none",
            "encoder_gap_enable_auto_stop_false_judgment": "observe_encoder_gap_viewer_state",
            "encoder_gap_enable_auto_stop_false_sample_count_24h": 1,
            "encoder_gap_enable_auto_stop_false_duration_sec_24h": 180,
            "remote_warning_restart_judgment": "ok_single_or_none",
            "stream_engine_ffmpeg_exit_224_judgment": "ok_single_or_none",
            "public_probe_judgment": "ok_none",
            "watchdog_restart_reasons": {},
            "fast_recovery_restart_triggers": {},
            "stream_engine_ffmpeg_exit_224_count_1h": 0,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stats_file = root / "youtube_watchdog_stats.json"
            stats_file.write_text(
                json.dumps(
                    {
                        "stats_file_updated_at_utc": "2026-05-18T09:53:52Z",
                        "oauth_enable_auto_stop": False,
                        "api_live_state": "live",
                        "oauth_life_cycle_status": "live",
                        "stream_active": False,
                        "ingest_connected": False,
                        "local_ok": False,
                        "ffmpeg_pid": 0,
                    }
                ),
                encoding="utf-8",
            )
            stream_log = root / "stream1090_report.jsonl"
            upstream_log = root / "upstream_stream1090_report.jsonl"
            stream_log.write_text("", encoding="utf-8")
            upstream_log.write_text("", encoding="utf-8")
            with mock.patch("cli._observe_payload", return_value=(0, observe_payload, "")):
                with mock.patch.object(cli, "STREAM1090_REPORT_EVENTS_FILE", stream_log):
                    with mock.patch.object(cli, "UPSTREAM_REPORT_EVENTS_FILE", upstream_log):
                        with mock.patch.object(cli, "YOUTUBE_WATCHDOG_STATS_FILE", stats_file):
                            incidents = cli.collect_notification_incidents(now_ts=now_ts, report_stale_sec=1800)

        incident_ids = {item["id"] for item in incidents}
        self.assertIn("youtube:enable_auto_stop_false_encoder_gap", incident_ids)

    def test_collect_notification_incidents_keeps_rtmps_context_during_current_failure(self) -> None:
        now_ts = cli.parse_utc_ts("2026-05-11T13:46:12Z")
        observe_payload = {
            "pass": False,
            "checks": {
                "current_fail": True,
                "youtube_current_status": "fail",
                "youtube_current_judgment": "current_fail",
                "youtube_stats_stale": False,
                "pulse_pass": True,
                "fast_mode_current_active": False,
            },
            "api_report_judgment": "ok",
            "fast_mode_current_active": False,
            "fast_mode_judgment": "ok_none",
            "encoder_gap_enable_auto_stop_false_judgment": "ok_none",
            "remote_warning_restart_judgment": "ok_single_or_none",
            "stream_engine_ffmpeg_exit_224_judgment": "ok_single_or_none",
            "rtmps_ssl_tls_judgment": "investigate_rtmps_ssl_tls_immediate",
            "rtmps_ssl_tls_count_1h": 2,
            "rtmps_ssl_tls_count_24h": 2,
            "journal_ssl_tls": {"count_24h": 2},
            "stream_engine_ffmpeg_ssl_tls_count_24h": 0,
            "fast_recovery_ssl_tls_count_24h": 0,
            "public_probe_judgment": "observe_public_probe_noise_clustered",
            "public_probe_degraded_count_1h": 2,
            "public_probe_degraded_count_24h": 2,
            "public_probe_authoritative_live_ok_count_24h": 0,
            "watchdog_restart_reasons": {},
            "fast_recovery_restart_triggers": {},
            "stream_engine_ffmpeg_exit_224_count_1h": 0,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stream_log = root / "stream1090_report.jsonl"
            upstream_log = root / "upstream_stream1090_report.jsonl"
            stream_log.write_text("", encoding="utf-8")
            upstream_log.write_text("", encoding="utf-8")
            with mock.patch("cli._observe_payload", return_value=(0, observe_payload, "")):
                with mock.patch.object(cli, "STREAM1090_REPORT_EVENTS_FILE", stream_log):
                    with mock.patch.object(cli, "UPSTREAM_REPORT_EVENTS_FILE", upstream_log):
                        incidents = cli.collect_notification_incidents(now_ts=now_ts, report_stale_sec=1800)

        incident_ids = {item["id"] for item in incidents}
        self.assertIn("stream:current_fail", incident_ids)
        self.assertIn("rtmps:ssl_tls_specific_event", incident_ids)
        self.assertIn("public_probe:429_or_bot_confirmation_repeated", incident_ids)

    def test_notify_status_sends_detected_repeat_and_recovery_followup(self) -> None:
        config = {
            "enabled": True,
            "webhook_url": "https://discord.example/webhook",
            "repeat_sec": 60,
            "report_stale_sec": 1800,
            "username": "test",
            "outbox_ttl_sec": 86400,
            "outbox_max_pending": 50,
            "outbox_flush_limit": 10,
        }
        incident_payload = [
            {
                "id": "stream1090:overlay_report",
                "severity": "warning",
                "component": "overlay_stream1090",
                "summary": "overlay report is warn",
                "evidence": "judgment=report_only_warn",
                "recovery_type": "report_only_observation_no_stream_restart",
                "follow_up": "next sample",
                "observed_ts": 940,
            }
        ]
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_file = root / "notify_state.json"
            events_file = root / "notify_events.jsonl"
            outbox_file = root / "notify_outbox.jsonl"
            stream_log = root / "stream1090_report.jsonl"
            stream_log.write_text(
                json.dumps(
                    {
                        "ts_utc": "1970-01-01T00:18:40Z",
                        "target": "overlay_stream1090",
                        "judgment": "report_only_ok",
                        "warnings": [],
                        "checks": {"position_change_count": 1, "messages_delta": 10},
                        "baseline": {"warn_rate": 0.0, "alert": False},
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            sent: list[str] = []

            def fake_send(_url: str, content: str, **_kwargs) -> tuple[bool, str]:
                sent.append(content)
                return True, "ok"

            with mock.patch.object(cli, "NOTIFY_STATE_FILE", state_file):
                with mock.patch.object(cli, "NOTIFY_EVENTS_FILE", events_file):
                    with mock.patch.object(cli, "NOTIFY_OUTBOX_FILE", outbox_file):
                        with mock.patch.object(cli, "STREAM1090_REPORT_EVENTS_FILE", stream_log):
                            with mock.patch("cli.load_stream_notify_config", return_value=config):
                                with mock.patch("cli.send_discord_webhook", side_effect=fake_send):
                                    with mock.patch("cli.collect_notification_incidents", return_value=incident_payload):
                                        self.assertEqual(cli.notify_status(now_ts=1000), 0)
                                        self.assertEqual(cli.notify_status(now_ts=1030), 0)
                                        self.assertEqual(cli.notify_status(now_ts=1061), 0)
                                    with mock.patch("cli.collect_notification_incidents", return_value=[]):
                                        self.assertEqual(cli.notify_status(now_ts=1120), 0)

        self.assertEqual(len(sent), 3)
        self.assertIn("障害検知", sent[0])
        self.assertIn("障害継続ステータス", sent[1])
        self.assertIn("復旧フォローアップ", sent[2])
        self.assertIn("window=1970-01-01 09:15:40 JST -> ongoing", sent[0])
        self.assertIn("detected_at=1970-01-01 09:16:40 JST", sent[0])
        self.assertIn("active_incidents=0", sent[2])
        self.assertIn("resolved_incidents=1", sent[2])
        self.assertIn("window=1970-01-01 09:15:40 JST -> 1970-01-01 09:18:40 JST", sent[2])

    def test_notify_status_sends_maintenance_reminder_without_collecting_incidents(self) -> None:
        config = {
            "enabled": True,
            "webhook_url": "https://discord.example/webhook",
            "repeat_sec": 60,
            "maintenance_repeat_sec": 600,
            "report_stale_sec": 1800,
            "username": "test",
            "outbox_ttl_sec": 86400,
            "outbox_max_pending": 50,
            "outbox_flush_limit": 10,
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_file = root / "notify_state.json"
            events_file = root / "notify_events.jsonl"
            outbox_file = root / "notify_outbox.jsonl"
            maintenance_file = root / "maintenance_mode.json"
            maintenance_file.write_text(
                json.dumps(
                    {
                        "active": True,
                        "started_at_utc": "1970-01-01T00:10:00Z",
                        "last_action": "on",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            state_file.write_text(
                json.dumps(
                    {
                        "active": {
                            "stream:current_fail": {
                                "first_seen_ts": 500,
                                "first_notified_ts": 500,
                                "last_bad_ts": 590,
                                "last_incident": {
                                    "id": "stream:current_fail",
                                    "summary": "current_fail=true",
                                },
                            }
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            sent: list[str] = []

            def fake_send(_url: str, content: str, **_kwargs) -> tuple[bool, str]:
                sent.append(content)
                return True, "ok"

            with mock.patch.object(cli, "NOTIFY_STATE_FILE", state_file):
                with mock.patch.object(cli, "NOTIFY_EVENTS_FILE", events_file):
                    with mock.patch.object(cli, "NOTIFY_OUTBOX_FILE", outbox_file):
                        with mock.patch.object(cli, "MAINTENANCE_STATE_FILE", maintenance_file):
                            with mock.patch("cli.load_stream_notify_config", return_value=config):
                                with mock.patch("cli.send_discord_webhook", side_effect=fake_send):
                                    with mock.patch("cli.collect_notification_incidents") as collect:
                                        self.assertEqual(cli.notify_status(now_ts=610), 0)
                                        self.assertEqual(cli.notify_status(now_ts=900), 0)
                                        self.assertEqual(cli.notify_status(now_ts=1211), 0)
                                        collect.assert_not_called()

            state = json.loads(state_file.read_text(encoding="utf-8"))

        self.assertEqual(len(sent), 2)
        self.assertIn("メンテナンス継続中", sent[0])
        self.assertIn("maintenance=on", sent[0])
        self.assertIn("作業完了後は stream m off", sent[0])
        self.assertIn("stream:current_fail", state["active"])
        self.assertTrue(state["maintenance_active"])
        self.assertEqual(state["last_maintenance_status_sent_ts"], 1211)

    def test_notify_status_sends_fast_recovery_auto_recovered_event_once(self) -> None:
        config = {
            "enabled": True,
            "webhook_url": "https://discord.example/webhook",
            "repeat_sec": 60,
            "maintenance_repeat_sec": 600,
            "report_stale_sec": 1800,
            "username": "test",
            "outbox_ttl_sec": 86400,
            "outbox_max_pending": 50,
            "outbox_flush_limit": 10,
            "fast_recovery_event_recent_sec": 1800,
            "fast_recovery_event_triggers": ["tcp_stall"],
        }
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_file = root / "notify_state.json"
            events_file = root / "notify_events.jsonl"
            outbox_file = root / "notify_outbox.jsonl"
            fast_events_file = root / "fast_recovery_events.jsonl"
            fast_events_file.write_text(
                json.dumps(
                    {
                        "ts_utc": "1970-01-01T00:20:00Z",
                        "kind": "restart",
                        "trigger": "tcp_stall",
                        "message": "tcp stall: bytes_delta=0 lastsnd_ms=12332 notsent=1649460 unacked=390",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            sent: list[str] = []

            def fake_send(_url: str, content: str, **_kwargs) -> tuple[bool, str]:
                sent.append(content)
                return True, "ok"

            with mock.patch.object(cli, "NOTIFY_STATE_FILE", state_file):
                with mock.patch.object(cli, "NOTIFY_EVENTS_FILE", events_file):
                    with mock.patch.object(cli, "NOTIFY_OUTBOX_FILE", outbox_file):
                        with mock.patch.object(cli, "FAST_RECOVERY_EVENTS_FILE", fast_events_file):
                            with mock.patch("cli.load_stream_notify_config", return_value=config):
                                with mock.patch("cli.collect_notification_incidents", return_value=[]):
                                    with mock.patch("cli.send_discord_webhook", side_effect=fake_send):
                                        self.assertEqual(cli.notify_status(now_ts=1230), 0)
                                        self.assertEqual(cli.notify_status(now_ts=1290), 0)

            state = json.loads(state_file.read_text(encoding="utf-8"))

        self.assertEqual(len(sent), 1)
        self.assertIn("自動復旧イベント", sent[0])
        self.assertIn("active_incidents=0", sent[0])
        self.assertIn("trigger=tcp_stall", sent[0])
        self.assertIn("stream service restart completed", sent[0])
        self.assertIn("1970-01-01T00:20:00Z|tcp_stall", state["fast_recovery_auto_recovered_notified"])

    def test_notify_status_dry_run_does_not_mutate_state_or_events(self) -> None:
        config = {
            "enabled": True,
            "webhook_url": "https://discord.example/webhook",
            "repeat_sec": 60,
            "report_stale_sec": 1800,
            "username": "test",
            "outbox_ttl_sec": 86400,
            "outbox_max_pending": 50,
            "outbox_flush_limit": 10,
        }
        incident_payload = [
            {
                "id": "rtmps:ssl_tls_specific_event",
                "severity": "warning",
                "component": "rtmps_ingest_tls",
                "summary": "investigate_rtmps_ssl_tls_immediate",
                "evidence": "count_1h=2",
                "recovery_type": "observe_rtmps_ssl_tls_before_transport_reclassification",
                "follow_up": "check logs",
                "observed_ts": 1000,
            }
        ]
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_file = root / "notify_state.json"
            events_file = root / "notify_events.jsonl"
            outbox_file = root / "notify_outbox.jsonl"
            state_file.write_text(
                json.dumps({"active": {}, "last_status_sent_ts": 0, "updated_ts_utc": "old"}) + "\n",
                encoding="utf-8",
            )
            before = state_file.read_text(encoding="utf-8")
            with mock.patch.object(cli, "NOTIFY_STATE_FILE", state_file):
                with mock.patch.object(cli, "NOTIFY_EVENTS_FILE", events_file):
                    with mock.patch.object(cli, "NOTIFY_OUTBOX_FILE", outbox_file):
                        with mock.patch("cli.load_stream_notify_config", return_value=config):
                            with mock.patch("cli.collect_notification_incidents", return_value=incident_payload):
                                self.assertEqual(cli.notify_status(dry_run=True, now_ts=1060), 0)

            self.assertEqual(state_file.read_text(encoding="utf-8"), before)
            self.assertFalse(events_file.exists())
            self.assertFalse(outbox_file.exists())

    def test_notify_status_retries_pending_outbox_after_send_failure(self) -> None:
        config = {
            "enabled": True,
            "webhook_url": "https://discord.example/webhook",
            "repeat_sec": 60,
            "report_stale_sec": 1800,
            "username": "test",
            "outbox_ttl_sec": 86400,
            "outbox_max_pending": 50,
            "outbox_flush_limit": 10,
        }
        incident_payload = [
            {
                "id": "stream:current_fail",
                "severity": "critical",
                "component": "stream",
                "summary": "current_fail",
                "evidence": "current_fail=true",
                "recovery_type": "service_restart_or_manual_check",
                "follow_up": "check stream",
                "observed_ts": 1000,
            }
        ]
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            state_file = root / "notify_state.json"
            events_file = root / "notify_events.jsonl"
            outbox_file = root / "notify_outbox.jsonl"
            calls: list[str] = []

            def fake_send(_url: str, content: str, **_kwargs) -> tuple[bool, str]:
                calls.append(content)
                if len(calls) == 1:
                    return False, "network_down"
                return True, "ok"

            with mock.patch.object(cli, "NOTIFY_STATE_FILE", state_file):
                with mock.patch.object(cli, "NOTIFY_EVENTS_FILE", events_file):
                    with mock.patch.object(cli, "NOTIFY_OUTBOX_FILE", outbox_file):
                        with mock.patch("cli.load_stream_notify_config", return_value=config):
                            with mock.patch("cli.send_discord_webhook", side_effect=fake_send):
                                with mock.patch("cli.collect_notification_incidents", return_value=incident_payload):
                                    self.assertEqual(cli.notify_status(now_ts=1000), 1)
                                    pending = [json.loads(line) for line in outbox_file.read_text(encoding="utf-8").splitlines()]
                                    self.assertEqual(len(pending), 1)
                                    self.assertEqual(pending[0]["attempts"], 1)
                                    self.assertEqual(pending[0]["last_error"], "network_down")
                                    self.assertEqual(cli.notify_status(now_ts=1030), 0)
                                    self.assertEqual(outbox_file.read_text(encoding="utf-8"), "")
                                    events = [
                                        json.loads(line)
                                        for line in events_file.read_text(encoding="utf-8").splitlines()
                                    ]
                                    self.assertEqual([item["send_ok"] for item in events], [False, True])
                                    self.assertTrue(all(item.get("outbox") for item in events))

        self.assertEqual(len(calls), 2)

    def test_oauth_status_classifies_invalid_grant_without_exposing_secret(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            stats_file = root / "youtube_watchdog_stats.json"
            token_file = root / "youtube_oauth_token_state.json"
            stats_file.write_text(
                json.dumps(
                    {
                        "oauth_ok": False,
                        "oauth_probe_ok": False,
                        "oauth_healthy": False,
                        "oauth_mode": "shadow",
                        "oauth_reason": "oauth token refresh http 400: invalid_grant (refresh token invalid/expired or app test-user restriction)",
                        "api_ok": True,
                        "status": "ok",
                        "judgment": "ok",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            token_file.write_text(
                json.dumps({"access_token": "secret-access-token", "expires_at": 1000}) + "\n",
                encoding="utf-8",
            )

            def fake_env(path: Path) -> dict[str, str]:
                if path == cli.YOUTUBE_MONITOR_ENV_FILE:
                    return {
                        "YTW_OAUTH_ENABLE": "1",
                        "YTW_OAUTH_SHADOW_MODE": "1",
                        "YTW_OAUTH_CLIENT_ID": "client-id.example",
                        "YTW_OAUTH_CLIENT_SECRET": "client-secret",
                        "YTW_OAUTH_REFRESH_TOKEN": "refresh-secret",
                        "YTW_OAUTH_TOKEN_STATE_FILE": str(token_file),
                    }
                return {}

            with mock.patch.object(cli, "YOUTUBE_WATCHDOG_STATS_FILE", stats_file):
                with mock.patch("cli.read_env_file", side_effect=fake_env):
                    payload = cli.oauth_status_payload(now_ts=2000)

        self.assertEqual(payload["judgment"], "oauth_refresh_token_invalid")
        self.assertTrue(payload["invalid_grant"])
        self.assertTrue(payload["configured"])
        self.assertIn("client-id.example", payload["authorization_url"])
        self.assertNotIn("refresh-secret", json.dumps(payload))
        self.assertNotIn("secret-access-token", json.dumps(payload))
        self.assertTrue(payload["actions"])

    def test_objective_sli_payload_separates_cumulative_and_regime_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            log_dir = root / "logs"
            log_dir.mkdir()

            youtube_log = log_dir / "youtube_watchdog.jsonl"
            youtube_log.write_text(
                "\n".join(
                    [
                        json.dumps({"ts_utc": "2026-05-06T23:00:00Z", "status": "degraded_public"}),
                        json.dumps({"ts_utc": "2026-05-07T01:00:00Z", "status": "ok"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            fast_log = log_dir / "fast_recovery_events.jsonl"
            fast_log.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts_utc": "2026-05-10T03:34:00Z",
                                "kind": "tcp_send_sample",
                                "send_mbps": 4.8,
                                "sample_interval_sec": 30,
                            }
                        ),
                        json.dumps(
                            {
                                "ts_utc": "2026-05-10T03:35:00Z",
                                "kind": "restart",
                                "trigger": "tcp_stall",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            stream_engine_log = log_dir / "stream_engine_events.jsonl"
            stream_engine_log.write_text(
                json.dumps(
                    {
                        "ts_utc": "2026-05-10T04:00:00Z",
                        "kind": "ffmpeg_restart_scheduled",
                        "exit_code": 224,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            overlay_log = log_dir / "stream1090_report.jsonl"
            overlay_log.write_text(
                "\n".join(
                    [
                        json.dumps({"ts_utc": "2026-05-10T20:00:00Z", "target": "overlay_stream1090", "judgment": "report_only_ok"}),
                        json.dumps({"ts_utc": "2026-05-10T20:01:00Z", "target": "overlay_stream1090", "judgment": "report_only_warn"}),
                        json.dumps({"ts_utc": "2026-05-10T20:02:00Z", "target": "overlay_stream1090", "judgment": "report_only_ok"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            upstream_log = log_dir / "upstream_stream1090_report.jsonl"
            upstream_log.write_text(overlay_log.read_text(encoding="utf-8"), encoding="utf-8")
            notify_log = log_dir / "stream_notify_events.jsonl"
            notify_log.write_text(
                json.dumps({"ts_utc": "2026-05-10T20:03:00Z", "kind": "send_ok"}) + "\n",
                encoding="utf-8",
            )
            (log_dir / "youtube_api_calls.jsonl").write_text(
                json.dumps({"ts_utc": "2026-05-10T20:04:00Z", "cost_units": 1}) + "\n",
                encoding="utf-8",
            )
            (log_dir / "stream_watchdog_events.jsonl").write_text(
                json.dumps({"ts_utc": "2026-05-10T20:05:00Z", "kind": "watchdog_ok"}) + "\n",
                encoding="utf-8",
            )
            snapshot = root / "objective_sli.json"
            history = log_dir / "objective_sli.jsonl"

            with mock.patch.object(cli, "LOG_BASE_DIR", log_dir):
                with mock.patch.object(cli, "YOUTUBE_WATCHDOG_EVENTS_FILE", youtube_log):
                    with mock.patch.object(cli, "FAST_RECOVERY_EVENTS_FILE", fast_log):
                        with mock.patch.object(cli, "STREAM_ENGINE_EVENTS_FILE", stream_engine_log):
                            with mock.patch.object(cli, "STREAM1090_REPORT_EVENTS_FILE", overlay_log):
                                with mock.patch.object(cli, "UPSTREAM_REPORT_EVENTS_FILE", upstream_log):
                                    with mock.patch.object(cli, "NOTIFY_EVENTS_FILE", notify_log):
                                            with mock.patch.object(cli, "OBJECTIVE_SLI_FILE", snapshot):
                                                with mock.patch.object(cli, "OBJECTIVE_SLI_EVENTS_FILE", history):
                                                    payload = cli.objective_sli_payload(
                                                        now_ts=cli.parse_utc_ts("2026-05-10T20:10:00Z")
                                                    )
                                                    cli.save_objective_sli(payload)
                                                    snapshot_exists = snapshot.exists()
                                                    history_exists = history.exists()

        self.assertEqual(payload["schema_version"], 1)
        youtube = payload["metrics"]["youtube_live"]
        self.assertEqual(youtube["cumulative"]["ok_ratio_pct"], 50.0)
        self.assertEqual(youtube["since_post_stabilization"]["ok_ratio_pct"], 100.0)
        upload = payload["metrics"]["upload_budget"]["since_samples_started"]
        self.assertEqual(upload["over_5mbps_sec"], 0.0)
        self.assertEqual(upload["within_5mbps_ratio_pct"], 100.0)
        stream_engine = payload["metrics"]["stream_engine"]["cumulative"]
        self.assertEqual(stream_engine["ffmpeg_restart_attempt_count"], 1)
        self.assertEqual(stream_engine["ffmpeg_restart_episode_count"], 1)
        self.assertEqual(stream_engine["ffmpeg_restart_retry_episode_count"], 1)
        self.assertEqual(stream_engine["ffmpeg_restart_incident_cluster_count"], 1)
        self.assertEqual(
            stream_engine["ffmpeg_restart_episode_root_causes"].get("rtmp_broken_pipe_self_recovery"),
            1,
        )
        self.assertEqual(payload["metrics"]["stream_engine"]["rolling_24h"]["ffmpeg_restart_attempt_count"], 1)
        self.assertEqual(payload["metrics"]["stream_engine"]["rolling_1h"]["ffmpeg_restart_attempt_count"], 0)
        visual = payload["metrics"]["visual_upstream"]
        self.assertEqual(visual["overlay_stream1090"]["incident_count"], 1)
        self.assertEqual(visual["ab_interpretation"]["status"], "pending_next_incident_or_deep_log_review")
        self.assertTrue(snapshot_exists)
        self.assertTrue(history_exists)

    def fake_shadow_result(self) -> SimpleNamespace:
        snapshot = {
            "overall": {
                "state": "healthy",
                "stream_public_state": "same_url_live",
                "expected_video_id": "VID",
                "recommended_action": "none",
                "action_scope": "none",
                "degraded_subsystems": [],
            }
        }
        event = {
            "selected_action": {"action": "none", "scope": "none", "execute": False},
            "gates": {"monitoring_safety": {"passed": True}},
        }
        return SimpleNamespace(
            snapshot=snapshot,
            orchestrator_event=event,
            recovery_action_plan={"action": "none", "execute": False},
            objective_sli={"windows": {}},
            stream_components={"components": {}},
        )

    def test_subsystem_shadow_commands_delegate_to_stream_v2_pipeline(self) -> None:
        fake = self.fake_shadow_result()
        with mock.patch.object(cli, "_stream_v2_subsystems_status_result", return_value=fake) as run_shadow:
            with mock.patch("builtins.print") as printed:
                rc = cli.subsystems_status(record=False)

        self.assertEqual(rc, 0)
        run_shadow.assert_called_once_with(record=False)
        output = "\n".join(str(call.args[0]) for call in printed.call_args_list)
        self.assertIn("[subsystems-status]", output)
        self.assertIn("public=same_url_live", output)

    def test_recovery_orchestrator_command_delegates_to_stream_v2_pipeline(self) -> None:
        fake = self.fake_shadow_result()
        with mock.patch.object(cli, "_stream_v2_recovery_orchestrator_result", return_value=fake) as run_shadow:
            with mock.patch("builtins.print") as printed:
                rc = cli.recovery_orchestrator(record=False)

        self.assertEqual(rc, 0)
        run_shadow.assert_called_once_with(record=False)
        output = "\n".join(str(call.args[0]) for call in printed.call_args_list)
        self.assertIn("[recovery-orchestrator]", output)
        self.assertIn("execute=False", output)

    def test_shadow_once_json_keeps_program_map_entrypoint_compatible(self) -> None:
        fake = self.fake_shadow_result()
        with mock.patch.object(cli, "_stream_v2_shadow_result", return_value=fake):
            with mock.patch("builtins.print") as printed:
                rc = cli.shadow_once(json_output=True, record=False)

        self.assertEqual(rc, 0)
        payload = json.loads(str(printed.call_args.args[0]))
        self.assertEqual(payload["subsystems_status"]["overall"]["state"], "healthy")
        self.assertEqual(payload["recovery_orchestrator"]["selected_action"]["action"], "none")


if __name__ == "__main__":
    unittest.main()
