from __future__ import annotations

import importlib.util
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "ops" / "scripts" / "observe_stream_health.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("observe_stream_health", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load observe_stream_health module")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class ObserveStreamHealthTests(unittest.TestCase):
    def test_rotated_youtube_watchdog_log_is_included(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl.1").write_text(
                json.dumps({"ts_utc": ts, "status": "warn"}) + "\n",
                encoding="utf-8",
            )
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts, "status": "ok"}) + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()
            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertEqual(payload.get("youtube_watchdog_status_counts", {}).get("warn", 0), 1)
            self.assertEqual(payload.get("youtube_watchdog_status_counts", {}).get("ok", 0), 1)
            self.assertGreaterEqual(payload.get("log_files_read", {}).get("youtube_watchdog", 0), 2)
            self.assertFalse(bool(payload.get("checks", {}).get("current_fail", True)))
            self.assertTrue(bool(payload.get("checks", {}).get("historical_degraded", False)))

    def test_quota_guard_only_fails_observability_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts, "status": "quota_guard"}) + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()
            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertFalse(bool(payload.get("checks", {}).get("youtube_observability_history_pass", True)))
            self.assertTrue(bool(payload.get("checks", {}).get("youtube_observability_pass", False)))
            self.assertTrue(bool(payload.get("checks", {}).get("historical_degraded", False)))
            self.assertEqual(int(payload.get("checks", {}).get("youtube_quota_guard_count", 0)), 1)

    def test_event_only_rows_are_not_counted_as_unknown_status(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"ts_utc": ts, "event": "youtube_quota_guard_activated"}),
                        json.dumps({"ts_utc": ts, "status": "ok"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()
            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertEqual(payload.get("youtube_watchdog_status_counts", {}).get("unknown", 0), 0)
            self.assertEqual(
                payload.get("youtube_watchdog_event_only_counts", {}).get("youtube_quota_guard_activated", 0),
                1,
            )

    def test_historical_degraded_when_youtube_unknown_exceeds_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"ts_utc": ts, "status": "unknown"}),
                        json.dumps({"ts_utc": ts, "status": "unknown"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()
            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertTrue(bool(payload.get("pass", False)))
            self.assertFalse(bool(payload.get("checks", {}).get("youtube_history_pass", True)))
            self.assertTrue(bool(payload.get("checks", {}).get("historical_degraded", False)))

    def test_current_fail_when_latest_stats_status_warn(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts, "status": "ok"}) + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts, "status": "warn", "judgment": "ng"}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()
            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 2)
            self.assertFalse(bool(payload.get("pass", True)))
            self.assertTrue(bool(payload.get("checks", {}).get("current_fail", False)))
            self.assertTrue(bool(payload.get("checks", {}).get("youtube_current_fail", False)))

    def test_public_probe_429_is_historical_degraded_when_current_oauth_stats_are_ok(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps(
                    {
                        "ts_utc": ts,
                        "status": "warn",
                        "health_source": "public_probe",
                        "watch_reason": "watch page fetch failed: HTTP Error 429: Too Many Requests; public live probe failed: yt-dlp bot confirmation",
                        "public_probe_status": 429,
                        "api_live_state": "live",
                        "oauth_probe_ok": True,
                        "oauth_healthy": True,
                        "local_ok": True,
                        "ingest_connected": True,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps(
                    {
                        "ts_utc": ts,
                        "status": "ok",
                        "judgment": "ok",
                        "remote_status": "ok",
                        "oauth_probe_ok": True,
                        "oauth_healthy": True,
                    }
                ),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()

            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertTrue(bool(payload.get("pass", False)))
            self.assertFalse(bool(payload.get("checks", {}).get("current_fail", True)))
            self.assertFalse(bool(payload.get("checks", {}).get("youtube_current_fail", True)))
            self.assertTrue(bool(payload.get("checks", {}).get("historical_degraded", False)))
            self.assertEqual(payload.get("youtube_health_source_counts", {}).get("public_probe"), 1)
            self.assertEqual(payload.get("youtube_oauth_healthy_counts", {}).get("true"), 1)
            self.assertEqual(payload.get("public_probe_degraded_count_1h"), 1)
            self.assertEqual(payload.get("public_probe_degraded_count_24h"), 1)
            self.assertEqual(payload.get("public_probe_authoritative_live_ok_count_1h"), 1)
            self.assertEqual(payload.get("public_probe_authoritative_live_ok_count_24h"), 1)
            self.assertEqual(payload.get("public_probe_degraded_reasons", {}).get("public_probe_429"), 1)
            self.assertEqual(payload.get("public_probe_judgment"), "observe_public_probe_noise_authoritative_live_ok")

    def test_fast_recovery_restart_is_reported_without_failing_current_health(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts, "status": "ok"}) + "\n",
                encoding="utf-8",
            )
            (log_dir / "fast_recovery_events.jsonl").write_text(
                json.dumps(
                    {
                        "ts_utc": ts,
                        "kind": "restart",
                        "trigger": "remote_warning",
                        "message": "youtube pre-loss warning while broadcast live: streamStatus=inactive",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts, "status": "ok", "judgment": "ok"}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()

            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertTrue(bool(payload.get("pass", False)))
            self.assertFalse(bool(payload.get("checks", {}).get("current_fail", True)))
            self.assertFalse(bool(payload.get("checks", {}).get("historical_degraded", True)))
            self.assertEqual(payload.get("fast_recovery_event_counts", {}).get("restart"), 1)
            self.assertEqual(payload.get("fast_recovery_restart_count"), 1)
            self.assertEqual(payload.get("fast_recovery_restart_triggers", {}).get("remote_warning"), 1)
            self.assertGreaterEqual(payload.get("log_files_read", {}).get("fast_recovery_events", 0), 1)

    def test_remote_warning_restart_sli_uses_1h_and_24h_windows_without_failing_health(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts_recent = (now - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_older = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts_now, "status": "ok"}) + "\n",
                encoding="utf-8",
            )
            (log_dir / "fast_recovery_events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"ts_utc": ts_older, "kind": "restart", "trigger": "remote_warning"}),
                        json.dumps({"ts_utc": ts_recent, "kind": "restart", "trigger": "remote_warning"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts_now, "status": "ok", "judgment": "ok"}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts_now}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()

            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertTrue(bool(payload.get("pass", False)))
            self.assertFalse(bool(payload.get("checks", {}).get("current_fail", True)))
            self.assertEqual(payload.get("fast_recovery_restart_count"), 1)
            self.assertEqual(payload.get("fast_recovery_restart_count_1h"), 1)
            self.assertEqual(payload.get("fast_recovery_restart_count_24h"), 2)
            self.assertEqual(payload.get("remote_warning_restart_count_1h"), 1)
            self.assertEqual(payload.get("remote_warning_restart_count_24h"), 2)
            self.assertEqual(payload.get("remote_warning_restart_judgment"), "observe")

    def test_stream_engine_ffmpeg_self_recovery_is_reported_without_failing_current_health(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts, "status": "ok"}) + "\n",
                encoding="utf-8",
            )
            (log_dir / "stream_engine_events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"ts_utc": ts, "event_type": "ffmpeg_exited", "exit_code": 224}),
                        json.dumps({"ts_utc": ts, "event_type": "ffmpeg_restart_scheduled", "exit_code": 224}),
                        json.dumps({"ts_utc": ts, "event_type": "ffmpeg_started", "ffmpeg_pid": 1234}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts, "status": "ok", "judgment": "ok"}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()

            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertTrue(bool(payload.get("pass", False)))
            self.assertFalse(bool(payload.get("checks", {}).get("current_fail", True)))
            self.assertFalse(bool(payload.get("checks", {}).get("historical_degraded", True)))
            self.assertEqual(payload.get("stream_engine_event_counts", {}).get("ffmpeg_exited"), 1)
            self.assertEqual(payload.get("stream_engine_event_counts", {}).get("ffmpeg_restart_scheduled"), 1)
            self.assertEqual(payload.get("stream_engine_ffmpeg_restart_count"), 1)
            self.assertEqual(payload.get("stream_engine_ffmpeg_exit_codes", {}).get("224"), 1)
            self.assertEqual(payload.get("stream_engine_ffmpeg_exit_224_count"), 1)
            self.assertGreaterEqual(payload.get("log_files_read", {}).get("stream_engine_events", 0), 1)

    def test_ffmpeg_exit_224_sli_uses_1h_and_24h_windows_without_failing_health(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts_recent = (now - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_older = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts_now, "status": "ok"}) + "\n",
                encoding="utf-8",
            )
            (log_dir / "stream_engine_events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"ts_utc": ts_older, "event_type": "ffmpeg_exited", "exit_code": 224}),
                        json.dumps(
                            {"ts_utc": ts_older, "event_type": "ffmpeg_restart_scheduled", "exit_code": 224}
                        ),
                        json.dumps({"ts_utc": ts_recent, "event_type": "ffmpeg_exited", "exit_code": 224}),
                        json.dumps(
                            {"ts_utc": ts_recent, "event_type": "ffmpeg_restart_scheduled", "exit_code": 224}
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts_now, "status": "ok", "judgment": "ok"}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts_now}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()

            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertTrue(bool(payload.get("pass", False)))
            self.assertFalse(bool(payload.get("checks", {}).get("current_fail", True)))
            self.assertEqual(payload.get("stream_engine_ffmpeg_exit_224_count"), 1)
            self.assertEqual(payload.get("stream_engine_ffmpeg_exit_224_count_1h"), 1)
            self.assertEqual(payload.get("stream_engine_ffmpeg_exit_224_count_24h"), 2)
            self.assertEqual(payload.get("stream_engine_ffmpeg_restart_count_1h"), 1)
            self.assertEqual(payload.get("stream_engine_ffmpeg_restart_count_24h"), 2)
            self.assertEqual(payload.get("stream_engine_ffmpeg_exit_224_judgment"), "observe_rtmp_path")

    def test_ffmpeg_restart_attempts_are_grouped_into_retry_episodes_and_incident_clusters(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc).replace(microsecond=0)
            ts_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            first_episode = now - timedelta(minutes=30)
            second_episode = first_episode + timedelta(minutes=3)
            # First episode: 7 attempts in 43s. Second episode starts 137s
            # later, so it is a separate retry episode but the same incident
            # cluster under the 600s incident gap.
            attempt_times = [
                *(first_episode + timedelta(seconds=offset) for offset in (0, 5, 11, 16, 22, 32, 43)),
                *(second_episode + timedelta(seconds=offset) for offset in (0, 6, 11, 16, 27)),
            ]
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts_now, "status": "ok"}) + "\n",
                encoding="utf-8",
            )
            (log_dir / "stream_engine_events.jsonl").write_text(
                "\n".join(
                    json.dumps(
                        {
                            "ts_utc": item.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "event_type": "ffmpeg_restart_scheduled",
                            "exit_code": 251 if idx < 7 else 146,
                            "reason": "Cannot open connection tls://a.rtmps.youtube.com:443",
                        }
                    )
                    for idx, item in enumerate(attempt_times)
                )
                + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts_now, "status": "ok", "judgment": "ok"}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts_now}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()

            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            # The first key is a legacy compatibility alias. The shorter
            # ffmpeg_restart_* names are the preferred summary keys.
            for key in (
                "stream_engine_ffmpeg_restart_count_24h",
                "stream_engine_ffmpeg_restart_attempts_24h",
                "ffmpeg_restart_attempts_24h",
            ):
                self.assertEqual(payload.get(key), 12, key)
            for key in (
                "stream_engine_ffmpeg_restart_retry_episodes_24h",
                "ffmpeg_restart_episodes_24h",
                "ffmpeg_restart_retry_episodes_24h",
            ):
                self.assertEqual(payload.get(key), 2, key)
            for key in (
                "stream_engine_ffmpeg_restart_incident_clusters_24h",
                "ffmpeg_restart_incident_clusters_24h",
            ):
                self.assertEqual(payload.get(key), 1, key)
            self.assertEqual(
                payload.get("stream_engine_ffmpeg_restart_episode_root_causes_24h", {}).get(
                    "rtmps_tls_connect_cluster"
                ),
                2,
            )
            self.assertEqual(
                payload.get("stream_engine_ffmpeg_restart_incident_root_causes_24h", {}).get(
                    "rtmps_tls_connect_cluster"
                ),
                1,
            )
            self.assertEqual(
                payload.get("ffmpeg_restart_incident_root_causes_24h", {}).get("rtmps_tls_connect_cluster"),
                1,
            )
            self.assertEqual(
                payload.get("ffmpeg_restart_episodes_root_cause_24h", {}).get("rtmps_tls_connect_cluster"),
                2,
            )
            self.assertEqual(payload.get("stream_engine_ffmpeg_restart_max_episode_duration_sec_24h"), 43)
            self.assertEqual(payload.get("ffmpeg_restart_max_episode_duration_sec_24h"), 43)
            self.assertEqual(payload.get("stream_engine_ffmpeg_restart_max_attempts_per_episode_24h"), 7)

    def test_rtmps_ssl_tls_events_are_reported_as_observability_axis(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts_recent = (now - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts_now, "status": "ok"}) + "\n",
                encoding="utf-8",
            )
            (log_dir / "stream_engine_events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts_utc": ts_recent,
                                "event_type": "ffmpeg_exited",
                                "exit_code": 1,
                                "message": "OpenSSL SSL_connect: SSL_ERROR_SYSCALL in connection to a.rtmps.youtube.com:443",
                            }
                        ),
                        json.dumps(
                            {
                                "ts_utc": ts_recent,
                                "event_type": "ffmpeg_restart_scheduled",
                                "exit_code": 1,
                                "reason": "ffmpeg exited unexpectedly after TLS handshake failed",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (log_dir / "fast_recovery_events.jsonl").write_text(
                json.dumps(
                    {
                        "ts_utc": ts_recent,
                        "kind": "restart_failed",
                        "trigger": "network_down",
                        "detail": "RTMPS TLS handshake failed while probing a.rtmps.youtube.com:443",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts_now, "status": "ok", "judgment": "ok"}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts_now}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()

            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertTrue(bool(payload.get("pass", False)))
            self.assertFalse(bool(payload.get("checks", {}).get("current_fail", True)))
            self.assertTrue(bool(payload.get("checks", {}).get("historical_degraded", False)))
            self.assertEqual(payload.get("stream_engine_ffmpeg_ssl_tls_count_1h"), 2)
            self.assertEqual(payload.get("fast_recovery_ssl_tls_count_1h"), 1)
            self.assertEqual(payload.get("rtmps_ssl_tls_count_1h"), 3)
            self.assertEqual(payload.get("rtmps_ssl_tls_judgment"), "investigate_rtmps_ssl_tls_immediate")

    def test_journal_ssl_tls_events_detect_ssl_error_patterns(self) -> None:
        mod = _load_module()
        now = int(datetime.now(timezone.utc).timestamp())
        journal_line = json.dumps(
            {
                "__REALTIME_TIMESTAMP": str(now * 1_000_000),
                "MESSAGE": "TLS handshake failed: SSL_ERROR_SSL while connecting to a.rtmps.youtube.com:443",
            }
        )
        completed = mock.Mock(returncode=0, stdout=journal_line + "\n", stderr="")
        with mock.patch.object(mod.subprocess, "run", return_value=completed):
            with mock.patch.dict(os.environ, {"OBSERVE_ENABLE_JOURNAL": "1"}, clear=False):
                payload = mod.journal_ssl_tls_events(
                    since_ts=now - 3600,
                    now_ts=now,
                    cutoff_1h=now - 3600,
                    cutoff_24h=now - 86400,
                )
        self.assertTrue(payload.get("enabled"))
        self.assertEqual(payload.get("count_1h"), 1)
        self.assertEqual(payload.get("count_24h"), 1)
        self.assertEqual(payload.get("reasons", {}).get("ssl_tls_handshake_failed"), 1)
        self.assertIn("TLS handshake failed", payload.get("samples", [{}])[0].get("message", ""))

    def test_real_log_fragment_observability_axes_do_not_merge_control_decisions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_public = (now - timedelta(minutes=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_remote = (now - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_exit = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts_utc": ts_public,
                                "status": "warn",
                                "judgment": "degraded_public",
                                "health_source": "public_probe",
                                "watch_reason": "watch page fetch failed: HTTP Error 429: Too Many Requests; public live probe failed: yt-dlp bot confirmation",
                                "api_live_state": "live",
                                "stream_active": True,
                                "ingest_connected": True,
                                "local_ok": True,
                                "oauth_probe_ok": True,
                                "oauth_healthy": True,
                                "oauth_stream_status": "active",
                                "oauth_stream_health_status": "good",
                            }
                        ),
                        json.dumps(
                            {
                                "ts_utc": ts_now,
                                "status": "ok",
                                "judgment": "ok",
                                "api_live_state": "live",
                                "oauth_probe_ok": True,
                                "oauth_healthy": True,
                                "oauth_stream_status": "active",
                                "oauth_stream_health_status": "good",
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (log_dir / "fast_recovery_events.jsonl").write_text(
                json.dumps(
                    {
                        "ts_utc": ts_remote,
                        "kind": "restart",
                        "trigger": "remote_warning",
                        "message": "youtube pre-loss warning while broadcast live: streamStatus=inactive healthStatus=noData",
                        "ffmpeg_pid": 4242,
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
            (log_dir / "stream_engine_events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts_utc": ts_exit,
                                "event_type": "ffmpeg_exited",
                                "exit_code": 224,
                                "returncode": 224,
                            }
                        ),
                        json.dumps(
                            {
                                "ts_utc": ts_exit,
                                "event_type": "ffmpeg_restart_scheduled",
                                "reason": "ffmpeg exited unexpectedly",
                                "exit_code": 224,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts_now, "status": "ok", "judgment": "ok", "remote_status": "ok"}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts_now}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()

            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertTrue(bool(payload.get("pass", False)))
            self.assertFalse(bool(payload.get("checks", {}).get("current_fail", True)))
            self.assertTrue(bool(payload.get("checks", {}).get("historical_degraded", False)))
            self.assertEqual(payload.get("public_probe_degraded_count_1h"), 1)
            self.assertEqual(payload.get("public_probe_authoritative_live_ok_count_1h"), 1)
            self.assertEqual(payload.get("remote_warning_restart_count_1h"), 1)
            self.assertEqual(payload.get("stream_engine_ffmpeg_exit_224_count_1h"), 1)

    def test_strict_history_returns_nonzero_for_historical_degraded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts, "status": "warn"}) + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts, "status": "ok"}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1", "--strict-history"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()
            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 2)
            self.assertTrue(bool(payload.get("pass", False)))
            self.assertFalse(bool(payload.get("checks", {}).get("strict_pass", True)))

    def test_fast_mode_api_report_and_encoder_gap_observability_axes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            report_dir = state_dir / "reports" / "youtube_api_cost"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)
            report_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts_enter = (now - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_exit = (now - timedelta(minutes=8)).strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_enter_current = (now - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_gap = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            now_epoch = int(now.timestamp())

            (log_dir / "youtube_video_id_resolver_events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"ts_utc": ts_enter, "event": "fast_mode_enter"}),
                        json.dumps({"ts_utc": ts_exit, "event": "fast_mode_exit"}),
                        json.dumps({"ts_utc": ts_enter_current, "event": "fast_mode_enter"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_video_id_resolver_state.json").write_text(
                json.dumps(
                    {
                        "ts_utc": ts_now,
                        "fast_mode_active": True,
                        "fast_mode_reason": "runtime tcp disconnected",
                        "fast_search_window_start_ts": now_epoch - 120,
                    }
                ),
                encoding="utf-8",
            )
            (log_dir / "youtube_watchdog.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts_utc": ts_gap,
                                "status": "ok",
                                "api_live_state": "live",
                                "oauth_life_cycle_status": "live",
                                "oauth_enable_auto_stop": False,
                                "stream_active": False,
                                "ingest_connected": False,
                                "local_ok": False,
                                "ffmpeg_pid": 0,
                            }
                        ),
                        json.dumps(
                            {
                                "ts_utc": ts_now,
                                "status": "ok",
                                "api_live_state": "live",
                                "oauth_life_cycle_status": "live",
                                "oauth_enable_auto_stop": False,
                                "stream_active": True,
                                "ingest_connected": True,
                                "local_ok": True,
                                "ffmpeg_pid": 123,
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            open_report = {
                "status": "ok",
                "target_day": "2026-05-09",
                "window": {"open_day": True, "effective_end_utc": ts_now},
                "totals": {"calls": 1, "units": 1, "quota_exceeded_events": 0},
            }
            closed_report = {
                "status": "ok",
                "target_day": "2026-05-08",
                "window": {"open_day": False, "effective_end_utc": ts_now},
                "totals": {"calls": 1, "units": 1, "quota_exceeded_events": 0},
            }
            (report_dir / "open_day_latest.json").write_text(json.dumps(open_report), encoding="utf-8")
            (report_dir / "latest.json").write_text(json.dumps(closed_report), encoding="utf-8")
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts_now, "status": "ok", "judgment": "ok"}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts_now}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                    "OBSERVE_FAST_MODE_ESTIMATED_INTERVAL_SEC": "5",
                    "OBSERVE_FAST_MODE_ESTIMATED_UNITS_PER_PROBE": "3",
                },
                clear=False,
            ):
                mod = _load_module()
                mod.systemd_timer_status = lambda unit: {"unit": unit, "active": True}  # type: ignore[assignment]
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()

            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertEqual(payload.get("fast_mode_episode_count_24h"), 2)
            self.assertGreaterEqual(payload.get("fast_mode_active_duration_sec_24h"), 230)
            self.assertGreaterEqual(payload.get("fast_mode_api_units_estimated_24h"), 138)
            self.assertTrue(payload.get("api_report_open_day_fresh"))
            self.assertTrue(payload.get("api_report_closed_day_fresh"))
            self.assertTrue(payload.get("api_report_timers_active"))
            self.assertEqual(payload.get("encoder_gap_enable_auto_stop_false_sample_count_24h"), 1)
            self.assertGreaterEqual(payload.get("encoder_gap_enable_auto_stop_false_duration_sec_24h"), 290)

    def test_tcp_send_budget_and_shared_line_hourly_axes_are_reported(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            sample_rows = []
            for idx, mbps in enumerate([4.0, 4.8, 5.5, 6.0], start=1):
                ts = (now - timedelta(minutes=idx)).strftime("%Y-%m-%dT%H:%M:%SZ")
                sample_rows.append(
                    json.dumps(
                        {
                            "ts_utc": ts,
                            "kind": "tcp_send_sample",
                            "sample_interval_sec": 60,
                            "mbps": mbps,
                            "bytes_sent_delta": int(mbps * 60 * 1_000_000 / 8),
                        }
                    )
                )
            ts_stall_dt = now - timedelta(minutes=10)
            ts_stall = ts_stall_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_stall_hour_jst = f"{(ts_stall_dt.hour + 9) % 24:02d}"
            (log_dir / "fast_recovery_events.jsonl").write_text(
                "\n".join(
                    [
                        *sample_rows,
                        json.dumps({"ts_utc": ts_stall, "kind": "restart", "trigger": "tcp_stall"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            ts_exit_dt = now - timedelta(minutes=20)
            ts_exit = ts_exit_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            ts_exit_hour_jst = f"{(ts_exit_dt.hour + 9) % 24:02d}"
            (log_dir / "stream_engine_events.jsonl").write_text(
                json.dumps({"ts_utc": ts_exit, "event_type": "ffmpeg_exited", "exit_code": 224}) + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts_now, "status": "ok", "judgment": "ok"}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts_now}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = ["observe_stream_health.py", "--hours", "1", "--tcp-send-budget-mbps", "5.0"]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()

            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertEqual(payload.get("ffmpeg_tcp_send_mbps_24h_sample_count"), 4)
            self.assertEqual(payload.get("ffmpeg_tcp_send_mbps_24h_p50"), 4.8)
            self.assertEqual(payload.get("ffmpeg_tcp_send_mbps_24h_p95"), 6.0)
            self.assertEqual(payload.get("ffmpeg_tcp_send_mbps_24h_max"), 6.0)
            self.assertEqual(payload.get("ffmpeg_tcp_send_mbps_24h_over_5mbps_duration_sec"), 120)
            self.assertEqual(payload.get("ffmpeg_tcp_send_budget_judgment"), "ok_within_budget")
            self.assertEqual(payload.get("tcp_stall_count_by_hour", {}).get(ts_stall_hour_jst), 1)
            self.assertEqual(payload.get("exit_224_count_by_hour", {}).get(ts_exit_hour_jst), 1)

    def test_pass_true_when_all_thresholds_satisfied(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts, "status": "ok"}) + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = [
                    "observe_stream_health.py",
                    "--hours",
                    "1",
                    "--max-youtube-unknown",
                    "0",
                    "--max-youtube-warn",
                    "0",
                    "--max-youtube-restart",
                    "0",
                    "--max-youtube-stats-stale-sec",
                    "300",
                ]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()
            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertTrue(bool(payload.get("pass", False)))
            self.assertTrue(bool(payload.get("checks", {}).get("youtube_pass", False)))

    def test_quota_guard_allowed_by_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state_dir = Path(td) / "state"
            log_dir = Path(td) / "logs"
            state_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            now = datetime.now(timezone.utc)
            ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            (log_dir / "youtube_watchdog.jsonl").write_text(
                json.dumps({"ts_utc": ts, "status": "quota_guard"}) + "\n",
                encoding="utf-8",
            )
            (state_dir / "youtube_watchdog_stats.json").write_text(
                json.dumps({"ts_utc": ts}),
                encoding="utf-8",
            )
            (state_dir / "slo_snapshot.json").write_text(
                json.dumps({"pulse_unavailable_count": 0, "slo_pulse_unavailable_24h_max": 1, "ts_utc": ts}),
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": str(state_dir),
                    "STREAM_RUNTIME_LOG_DIR": str(log_dir),
                },
                clear=False,
            ):
                mod = _load_module()
                buf = io.StringIO()
                argv = [
                    "observe_stream_health.py",
                    "--hours",
                    "1",
                    "--max-youtube-quota-guard",
                    "2",
                ]
                with mock.patch("sys.argv", argv):
                    with redirect_stdout(buf):
                        rc = mod.main()
            payload = json.loads(buf.getvalue())
            self.assertEqual(rc, 0)
            self.assertTrue(bool(payload.get("checks", {}).get("youtube_observability_pass", False)))


if __name__ == "__main__":
    unittest.main()
