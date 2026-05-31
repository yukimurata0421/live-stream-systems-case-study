from __future__ import annotations

import contextlib
import os
import json
import sys
import subprocess
import tempfile
import time
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "stream_core"))

import stream_engine  # type: ignore


class StreamEngineWaitModeTests(unittest.TestCase):
    def test_test_mode_uses_test_wait_value(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "PRE_FFMPEG_MIN_WAIT_SEC": "20",
                "PRE_FFMPEG_MIN_WAIT_SEC_TEST": "0",
                "PRE_FFMPEG_MIN_WAIT_SEC_RESTART": "5",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                wait_sec, mode = engine.effective_pre_ffmpeg_min_wait_sec()
                self.assertEqual(wait_sec, 0.0)
                self.assertEqual(mode, "test")

    def test_recent_runtime_snapshot_switches_to_restart_wait(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            runtime_state = Path(td) / "runtime" / "stream_runtime_state.json"
            runtime_state.parent.mkdir(parents=True, exist_ok=True)
            runtime_state.write_text("{}", encoding="utf-8")
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "0",
                "RUNTIME_STATE_FILE": str(runtime_state),
                "PRE_FFMPEG_MIN_WAIT_SEC": "20",
                "PRE_FFMPEG_MIN_WAIT_SEC_RESTART": "4",
                "PRE_FFMPEG_RESTART_CONTEXT_MAX_AGE_SEC": "300",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                wait_sec, mode = engine.effective_pre_ffmpeg_min_wait_sec()
                self.assertEqual(wait_sec, 4.0)
                self.assertEqual(mode, "restart")

    def test_stale_restart_markers_do_not_trigger_restart_wait(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            runtime_state = Path(td) / "runtime" / "stream_runtime_state.json"
            restart_reason = Path(td) / "runtime" / "restart_reason.json"
            runtime_state.parent.mkdir(parents=True, exist_ok=True)
            runtime_state.write_text("{}", encoding="utf-8")
            restart_reason.write_text("{}", encoding="utf-8")
            old = time.time() - 3600
            os.utime(runtime_state, (old, old))
            os.utime(restart_reason, (old, old))
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "0",
                "RUNTIME_STATE_FILE": str(runtime_state),
                "RESTART_REASON_FILE": str(restart_reason),
                "PRE_FFMPEG_MIN_WAIT_SEC": "12",
                "PRE_FFMPEG_MIN_WAIT_SEC_RESTART": "4",
                "PRE_FFMPEG_RESTART_CONTEXT_MAX_AGE_SEC": "300",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                wait_sec, mode = engine.effective_pre_ffmpeg_min_wait_sec()
                self.assertEqual(wait_sec, 12.0)
                self.assertEqual(mode, "normal")

    def test_consumed_restart_reason_does_not_trigger_restart_wait(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restart_reason = Path(td) / "runtime" / "restart_reason.json"
            restart_reason.parent.mkdir(parents=True, exist_ok=True)
            restart_reason.write_text(
                '{"ts_utc":"1970-01-01T00:16:30Z","consumed_at_utc":"1970-01-01T00:16:35Z"}',
                encoding="utf-8",
            )
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "0",
                "RESTART_REASON_FILE": str(restart_reason),
                "PRE_FFMPEG_MIN_WAIT_SEC": "12",
                "PRE_FFMPEG_MIN_WAIT_SEC_RESTART": "4",
                "PRE_FFMPEG_RESTART_CONTEXT_MAX_AGE_SEC": "300",
            }
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(stream_engine.time, "time", return_value=1000):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                wait_sec, mode = engine.effective_pre_ffmpeg_min_wait_sec()
                self.assertEqual(wait_sec, 12.0)
                self.assertEqual(mode, "normal")

    def test_startup_restart_context_marks_reason_consumed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            restart_reason = Path(td) / "runtime" / "restart_reason.json"
            event_log = Path(td) / "logs" / "stream_engine_events.jsonl"
            restart_reason.parent.mkdir(parents=True, exist_ok=True)
            restart_reason.write_text('{"ts_utc":"1970-01-01T00:16:30Z","reason":"tcp_stall"}', encoding="utf-8")
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "0",
                "RESTART_REASON_FILE": str(restart_reason),
                "EVENT_LOG_FILE": str(event_log),
                "PRE_FFMPEG_RESTART_CONTEXT_MAX_AGE_SEC": "300",
            }
            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(stream_engine.time, "time", return_value=1000):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                engine.emit_startup_restart_context()

            payload = json.loads(restart_reason.read_text(encoding="utf-8"))
            self.assertEqual(payload["consumed_by"], "stream_engine")
            self.assertIn("consumed_at_utc", payload)
            event = json.loads(event_log.read_text(encoding="utf-8").splitlines()[-1])
            self.assertEqual(event["event_type"], "startup_restart_context")
            self.assertFalse(event["restart_context_stale"])
            self.assertFalse(event["restart_context_consumed_before"])
            self.assertEqual(event["restart_context_age_sec"], 10)

    def test_runtime_state_hash_path_keeps_configured_parent_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            runtime_root = Path(td) / "external-state"
            runtime_state = runtime_root / "stream_runtime_state.json"
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "RUNTIME_STATE_FILE": str(runtime_state),
                "TEST_OUTPUT": "null",
                "TEST_OUTPUT_FILE": str(Path(td) / "capture.mkv"),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                engine.configure_target_runtime_paths()
                self.assertEqual(engine.cfg.runtime_state_file.parent, runtime_root)
                self.assertNotEqual(engine.cfg.runtime_state_file.name, "stream_runtime_state.json")
                self.assertRegex(engine.cfg.runtime_state_file.name, r"^stream_runtime_state_[0-9a-f]{64}\.json$")

    def test_runtime_state_custom_filename_is_not_rewritten(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            runtime_root = Path(td) / "external-state"
            runtime_state = runtime_root / "custom_runtime_state.json"
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "RUNTIME_STATE_FILE": str(runtime_state),
                "TEST_OUTPUT": "null",
                "TEST_OUTPUT_FILE": str(Path(td) / "capture.mkv"),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                engine.configure_target_runtime_paths()
                self.assertEqual(engine.cfg.runtime_state_file, runtime_state)

    def test_stale_capture_helpers_are_detected_by_owned_resources(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            overlay_dir = Path(td) / "ui" / "overlay"
            profile_dir = Path(td) / "runtime" / "chromium_profile"
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "DISPLAY_NAME": ":99",
                "OVERLAY_PORT": "18080",
                "OVERLAY_DIR": str(overlay_dir),
                "BROWSER_PROFILE_DIR": str(profile_dir),
            }
            outputs = [
                "111 Xvfb :99 -screen 0 1920x1080x24 -ac -nolisten tcp\n"
                "112 Xvfb :98 -screen 0 1920x1080x24 -ac -nolisten tcp\n",
                f"221 {sys.executable} {Path(td) / 'src' / 'stream_core' / 'overlay_server.py'} "
                f"--port 18080 --host 0.0.0.0 --directory {overlay_dir} --stream1090-url http://example/\n"
                f"222 {sys.executable} {Path(td) / 'src' / 'stream_core' / 'overlay_server.py'} "
                f"--port 18081 --host 0.0.0.0 --directory {overlay_dir}\n",
                f"331 chromium --app=http://127.0.0.1:18080/index.html --user-data-dir={profile_dir}\n"
                f"332 chromium --type=renderer --user-data-dir={profile_dir}\n"
                "333 chromium --app=http://127.0.0.1:18080/index.html --user-data-dir=/tmp/other\n",
            ]

            def fake_run(_cmd, check=True, timeout=None):
                return mock.Mock(stdout=outputs.pop(0), returncode=0)

            with mock.patch.dict(os.environ, env, clear=False), mock.patch.object(stream_engine, "run", side_effect=fake_run):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
                stale = engine.stale_capture_helper_pids()

            self.assertEqual(stale["xvfb"], [111])
            self.assertEqual(stale["overlay"], [221])
            self.assertEqual(stale["browser"], [331, 332])

    def test_runtime_heartbeat_interval_is_configurable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "RUNTIME_HEARTBEAT_SEC": "17",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
            self.assertEqual(cfg.runtime_heartbeat_sec, 17)

    def test_stop_ffmpeg_term_grace_is_configurable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "STOP_FFMPEG_TERM_GRACE_SEC": "2.5",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
            self.assertEqual(cfg.stop_ffmpeg_term_grace_sec, 2.5)

    def test_capture_helper_memory_guard_config_is_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "CAPTURE_HELPER_MEMORY_GUARD_ENABLED": "1",
                "XVFB_MEMORY_GUARD_RSS_MIB": "1234",
                "XVFB_MEMORY_GUARD_SHMEM_MIB": "567",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
            self.assertTrue(cfg.capture_helper_memory_guard_enabled)
            self.assertEqual(cfg.xvfb_memory_guard_rss_mib, 1234)
            self.assertEqual(cfg.xvfb_memory_guard_shmem_mib, 567)

    def test_capture_helper_memory_guard_requests_ordered_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            event_log = Path(td) / "logs" / "stream_engine_events.jsonl"
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "EVENT_LOG_FILE": str(event_log),
                "XVFB_MEMORY_GUARD_RSS_MIB": "2048",
                "XVFB_MEMORY_GUARD_SHMEM_MIB": "1536",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)

            proc = mock.Mock()
            proc.pid = 4321
            proc.poll.return_value = None
            engine.xvfb_proc = proc

            with mock.patch.object(
                engine,
                "proc_status_memory_mib",
                return_value={"VmRSS": 2300.0, "RssShmem": 1800.0},
            ):
                action = engine.helper_memory_guard_action()

            self.assertTrue(action.should_stop)
            self.assertEqual(action.stop_reason, "xvfb memory guard")
            self.assertEqual(engine.capture_helpers_force_restart_reason, "xvfb memory guard")
            event = json.loads(event_log.read_text(encoding="utf-8").splitlines()[-1])
            self.assertEqual(event["event_type"], "capture_helper_memory_guard_triggered")
            self.assertEqual(event["helper"], "xvfb")
            self.assertEqual(event["pid"], 4321)

    def test_pending_capture_stack_restart_runs_before_next_ffmpeg_start(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = {"BASE_DIR": td, "TEST_MODE": "1"}
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)

            engine.capture_helpers_force_restart_reason = "xvfb memory guard"
            with (
                mock.patch.object(engine, "restart_capture_stack_after_ffmpeg_stop", return_value=True) as restart_mock,
                mock.patch.object(engine, "ensure_x_display_running", return_value=False),
                mock.patch.object(engine, "ensure_overlay_server_running", return_value=False),
                mock.patch.object(engine, "ensure_browser_running", return_value=False),
                mock.patch.object(engine, "append_event"),
            ):
                restarted = engine.ensure_capture_helpers_running()

            restart_mock.assert_called_once_with("xvfb memory guard")
            self.assertEqual(engine.capture_helpers_force_restart_reason, "")
            self.assertEqual(restarted, ["xvfb", "browser"])

    def test_signal_handler_kills_ffmpeg_after_stop_grace_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            event_log = Path(td) / "logs" / "stream_engine_events.jsonl"
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "EVENT_LOG_FILE": str(event_log),
                "STOP_FFMPEG_TERM_GRACE_SEC": "1",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)

            proc = mock.Mock()
            proc.pid = 12345
            proc.poll.return_value = None
            proc.wait.side_effect = [
                subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=1.0),
                -9,
            ]
            engine.ffmpeg_proc = proc

            engine.signal_handler(stream_engine.signal.SIGTERM, None)

            self.assertTrue(engine.stop_requested)
            proc.terminate.assert_called_once()
            proc.kill.assert_called_once()
            events = [json.loads(line) for line in event_log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(
                [event["event_type"] for event in events],
                ["signal", "ffmpeg_stop_requested", "ffmpeg_stop_timeout_kill", "ffmpeg_stop_killed"],
            )
            self.assertEqual(events[2]["ffmpeg_pid"], 12345)
            self.assertEqual(events[2]["grace_sec"], 1.0)

    def test_signal_handler_does_not_kill_ffmpeg_when_it_exits_within_stop_grace(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            event_log = Path(td) / "logs" / "stream_engine_events.jsonl"
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "EVENT_LOG_FILE": str(event_log),
                "STOP_FFMPEG_TERM_GRACE_SEC": "1",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)

            proc = mock.Mock()
            proc.pid = 12345
            proc.poll.return_value = None
            proc.wait.return_value = -15
            engine.ffmpeg_proc = proc

            engine.signal_handler(stream_engine.signal.SIGTERM, None)

            proc.terminate.assert_called_once()
            proc.kill.assert_not_called()
            events = [json.loads(line) for line in event_log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(
                [event["event_type"] for event in events],
                ["signal", "ffmpeg_stop_requested", "ffmpeg_stop_exited"],
            )
            self.assertEqual(events[-1]["exit_code"], -15)

    def test_takeover_coord_wait_aborts_when_stop_requested(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock_dir = Path(td) / "locks"
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "STREAM_LOCK_DIR": str(lock_dir),
                "TAKEOVER_ENABLED": "1",
                "TAKEOVER_GRACE_SEC": "1",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)
            engine.stream_lock_file = lock_dir / "stream.lock"
            engine.takeover_coord_file = lock_dir / "stream.takeover.lock"
            stream_holder = stream_engine.engine_locks.try_acquire_lock(engine.stream_lock_file)
            coord_holder = stream_engine.engine_locks.try_acquire_lock(engine.takeover_coord_file)
            self.assertIsNotNone(stream_holder)
            self.assertIsNotNone(coord_holder)
            engine.stop_requested = True
            try:
                with self.assertRaisesRegex(RuntimeError, "Stop requested while waiting"):
                    engine.acquire_single_instance_lock()
            finally:
                for fp in (stream_holder, coord_holder):
                    if fp:
                        fp.close()

    def test_ffmpeg_exit_224_schedules_child_recovery_not_engine_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            event_log = Path(td) / "logs" / "stream_engine_events.jsonl"
            env = {
                "BASE_DIR": td,
                "TEST_MODE": "1",
                "TEST_OUTPUT": "null",
                "TEST_OUTPUT_FILE": str(Path(td) / "capture.mkv"),
                "EVENT_LOG_FILE": str(event_log),
                "RESTART_DELAY_SEC": "5",
                "RUNTIME_HEARTBEAT_SEC": "5",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)

            proc = mock.Mock()
            proc.pid = 12345
            proc.wait.return_value = 224

            def stop_after_restart_sleep(_seconds: float) -> None:
                engine.stop_requested = True

            with contextlib.ExitStack() as stack:
                for method_name in (
                    "ensure_commands",
                    "assert_systemd_launch",
                    "configure_target_runtime_paths",
                    "ensure_pulse_server",
                    "acquire_single_instance_lock",
                    "acquire_capture_lock",
                    "cleanup_stale_rtmp_ffmpeg",
                    "cleanup_stale_capture_helpers",
                    "assert_rtmp_health_gate",
                    "ensure_x_display",
                    "start_overlay_server",
                    "ensure_virtual_sink",
                    "ensure_local_audio_monitor",
                    "start_browser",
                    "wait_for_render_ready",
                    "emit_startup_restart_context",
                ):
                    stack.enter_context(mock.patch.object(engine, method_name))
                stack.enter_context(
                    mock.patch.object(engine, "detect_pulse_monitor", return_value="stream_sink.monitor")
                )
                stack.enter_context(mock.patch.object(engine, "ensure_capture_helpers_running", return_value=[]))
                stack.enter_context(mock.patch.object(stream_engine.subprocess, "Popen", return_value=proc))
                stack.enter_context(
                    mock.patch.object(stream_engine.time, "sleep", side_effect=stop_after_restart_sleep)
                )
                rc = engine.run()

            self.assertEqual(rc, 0)
            self.assertEqual(engine.restart_count, 1)
            events = [json.loads(line) for line in event_log.read_text(encoding="utf-8").splitlines()]
            event_types = [event["event_type"] for event in events]
            self.assertIn("ffmpeg_exited", event_types)
            self.assertIn("ffmpeg_restart_scheduled", event_types)
            exited = next(event for event in events if event["event_type"] == "ffmpeg_exited")
            scheduled = next(event for event in events if event["event_type"] == "ffmpeg_restart_scheduled")
            self.assertEqual(exited["exit_code"], 224)
            self.assertEqual(scheduled["exit_code"], 224)
            self.assertEqual(scheduled["delay_sec"], 5)

    def test_stale_capture_cleanup_skips_when_foreign_rtmp_is_alive(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = {"BASE_DIR": td, "TEST_MODE": "1"}
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)

            with (
                mock.patch.object(engine, "foreign_rtmp_pids", return_value=[1234]),
                mock.patch.object(engine, "stale_capture_helper_pids") as stale_mock,
                mock.patch.object(engine, "append_event"),
            ):
                engine.cleanup_stale_capture_helpers()

            stale_mock.assert_not_called()

    def test_stale_capture_cleanup_terminates_helpers_before_start(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env = {"BASE_DIR": td, "TEST_MODE": "1"}
            with mock.patch.dict(os.environ, env, clear=False):
                cfg = stream_engine.load_config()
                engine = stream_engine.StreamEngine(cfg)

            killed: list[tuple[int, int]] = []
            with (
                mock.patch.object(engine, "foreign_rtmp_pids", return_value=[]),
                mock.patch.object(
                    engine,
                    "stale_capture_helper_pids",
                    return_value={"browser": [331], "overlay": [221], "xvfb": [111]},
                ),
                mock.patch.object(stream_engine.os, "kill", side_effect=lambda pid, sig: killed.append((pid, sig))),
                mock.patch.object(engine, "pid_alive", return_value=False),
                mock.patch.object(engine, "append_event"),
                mock.patch.object(stream_engine.time, "sleep"),
            ):
                engine.cleanup_stale_capture_helpers()

            self.assertEqual(
                killed,
                [
                    (331, stream_engine.signal.SIGTERM),
                    (221, stream_engine.signal.SIGTERM),
                    (111, stream_engine.signal.SIGTERM),
                ],
            )


if __name__ == "__main__":
    unittest.main()
