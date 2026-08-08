from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_v3 import control_loop


class StreamV3ControlLoopTests(unittest.TestCase):
    def test_default_tasks_are_shadow_and_non_mutating(self) -> None:
        tasks = control_loop.default_tasks(
            {
                "STREAM_RUNTIME_STATE_DIR": "/state",
                "STREAM_V2_SOURCE_STATE_ROOT": "/source-v2-readonly",
                "PYTHON_BIN": "python3",
                "STREAM_V3_STREAM_CLI_BIN": "/app/bin/stream-prod",
                "V3_SHADOW_INTERVAL_SEC": "60",
                "STREAM_RUNTIME_SUPERVISOR": "k8s",
            }
        )

        commands = [" ".join(task.command) for task in tasks]
        names = [task.name for task in tasks]

        self.assertIn("python3 -m stream_v2 shadow-once --source-state-root /source-v2-readonly --state-root /state --mode shadow --supervisor-mode k8s", commands)
        self.assertIn("/app/bin/stream-prod subsystems-status --json", commands)
        self.assertIn("/app/bin/stream-prod recovery-orchestrator --json", commands)
        self.assertIn("python3 -m stream_v2 ops-summary --state-root /state --text", commands)
        self.assertIn("subsystems_status", names)
        self.assertIn("recovery_orchestrator", names)
        self.assertNotIn("shadow_sli", names)
        for command in commands:
            self.assertNotIn("restart", command)
            self.assertNotIn("systemctl", command)
            self.assertNotIn("notify-status", command)

    def test_notify_dry_run_task_is_opt_in(self) -> None:
        tasks = control_loop.default_tasks(
            {
                "STREAM_RUNTIME_STATE_DIR": "/state",
                "STREAM_V2_SOURCE_STATE_ROOT": "/source-v2-readonly",
                "STREAM_V3_STREAM_CLI_BIN": "/app/bin/stream-prod",
                "V3_ENABLE_NOTIFY_DRY_RUN": "1",
            }
        )

        commands = [" ".join(task.command) for task in tasks]

        self.assertIn("/app/bin/stream-prod notify-status --dry-run", commands)

    def test_cutover_tasks_map_watchdogs_into_single_control_loop(self) -> None:
        tasks = control_loop.default_tasks(
            {
                "PYTHON_BIN": "python3",
                "STREAM_V3_STREAM_CLI_BIN": "/app/bin/stream-prod",
                "V3_FAST_RECOVERY_INTERVAL_SEC": "10",
                "V3_VIDEO_RESOLVER_INTERVAL_SEC": "5",
            },
            mode="cutover",
        )

        commands = [" ".join(task.command) for task in tasks]
        intervals = {task.name: task.interval_sec for task in tasks}

        self.assertIn("python3 " + str(ROOT / "src" / "watchers" / "fast_recovery.py"), commands)
        self.assertIn("python3 " + str(ROOT / "src" / "watchers" / "stream_watchdog.py"), commands)
        self.assertIn("python3 " + str(ROOT / "src" / "watchers" / "youtube_watchdog.py"), commands)
        self.assertIn("python3 " + str(ROOT / "src" / "watchers" / "youtube_video_id_resolver.py"), commands)
        self.assertIn("/app/bin/stream-prod notify-status", commands)
        self.assertEqual(intervals["fast_recovery"], 10.0)
        self.assertEqual(intervals["youtube_video_resolver"], 5.0)

    def test_streaming_tasks_only_run_fast_recovery(self) -> None:
        tasks = control_loop.default_tasks(
            {
                "PYTHON_BIN": "python3",
                "V3_FAST_RECOVERY_INTERVAL_SEC": "10",
            },
            mode="streaming",
        )

        self.assertEqual([task.name for task in tasks], ["fast_recovery"])
        self.assertEqual(tasks[0].interval_sec, 10.0)
        self.assertIn("fast_recovery.py", " ".join(tasks[0].command))

    def test_monitor_tasks_exclude_fast_recovery(self) -> None:
        tasks = control_loop.default_tasks(
            {
                "PYTHON_BIN": "python3",
                "STREAM_V3_STREAM_CLI_BIN": "/app/bin/stream-prod",
            },
            mode="monitor",
        )

        names = [task.name for task in tasks]
        self.assertNotIn("fast_recovery", names)
        self.assertIn("youtube_video_resolver", names)
        self.assertIn("youtube_monitor", names)
        self.assertIn("map_runtime_probe", names)
        self.assertIn("viewer_synthetic_probe", names)
        self.assertIn("stream_watchdog", names)
        self.assertIn("notify_status", names)
        self.assertIn("subsystems_status", names)
        self.assertIn("recovery_orchestrator", names)
        self.assertNotIn("shadow_sli", names)

    def test_monitor_map_runtime_probe_has_independent_cadence_and_timeout(self) -> None:
        tasks = control_loop.default_tasks(
            {
                "PYTHON_BIN": "python3",
                "V3_CONTROL_TASK_TIMEOUT_SEC": "45",
                "V3_MAP_RUNTIME_PROBE_INTERVAL_SEC": "90",
                "V3_MAP_RUNTIME_PROBE_TIMEOUT_SEC": "20",
            },
            mode="monitor",
        )

        probe = next(task for task in tasks if task.name == "map_runtime_probe")
        self.assertEqual(probe.interval_sec, 90.0)
        self.assertEqual(probe.timeout_sec, 20.0)
        self.assertEqual(
            probe.command,
            ("python3", str(ROOT / "ops" / "scripts" / "stream_v3_map_runtime_probe.py")),
        )

    def test_monitor_viewer_probe_has_five_minute_cadence(self) -> None:
        tasks = control_loop.default_tasks(
            {
                "PYTHON_BIN": "python3",
                "V3_VIEWER_SYNTHETIC_INTERVAL_SEC": "300",
                "V3_VIEWER_SYNTHETIC_TIMEOUT_SEC": "75",
            },
            mode="monitor",
        )
        probe = next(task for task in tasks if task.name == "viewer_synthetic_probe")
        self.assertEqual(probe.interval_sec, 300.0)
        self.assertEqual(probe.timeout_sec, 75.0)

    def test_inline_shadow_sli_is_opt_in_with_an_independent_timeout(self) -> None:
        tasks = control_loop.default_tasks(
            {
                "STREAM_V3_STREAM_CLI_BIN": "/app/bin/stream-prod",
                "V3_CONTROL_TASK_TIMEOUT_SEC": "45",
                "V3_ENABLE_INLINE_SHADOW_SLI": "1",
                "V3_SHADOW_SLI_TIMEOUT_SEC": "120",
            },
            mode="monitor",
        )

        shadow_sli = next(task for task in tasks if task.name == "shadow_sli")
        self.assertEqual(shadow_sli.command, ("/app/bin/stream-prod", "shadow-sli", "--json"))
        self.assertEqual(shadow_sli.timeout_sec, 120.0)

    def test_main_rejects_cutover_mode_without_cutover_flag(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with mock.patch.dict(
                "os.environ",
                {"STREAM_RUNTIME_STATE_DIR": str(root), "STREAM_V3_CUTOVER_ENABLE": "0"},
                clear=False,
            ):
                rc = control_loop.main(["--once", "--mode", "cutover"])

        self.assertEqual(rc, 2)

    def test_main_rejects_streaming_mode_without_cutover_flag(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with mock.patch.dict(
                "os.environ",
                {"STREAM_RUNTIME_STATE_DIR": str(root), "STREAM_V3_CUTOVER_ENABLE": "0"},
                clear=False,
            ):
                rc = control_loop.main(["--once", "--mode", "streaming"])

        self.assertEqual(rc, 2)

    def test_run_once_writes_state_and_event_log(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            task = control_loop.ControlTask("ok", 60, ("true",))
            state_file = root / "state.json"
            event_log = root / "events.jsonl"
            with mock.patch.object(
                control_loop.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(args=["true"], returncode=0, stdout="done\n", stderr=""),
            ):
                results = control_loop.run_once([task], state_file=state_file, event_log=event_log, env={})

            self.assertEqual(len(results), 1)
            self.assertTrue(results[0].ok)
            state = json.loads(state_file.read_text(encoding="utf-8"))
            event = json.loads(event_log.read_text(encoding="utf-8").splitlines()[0])

        self.assertTrue(state["ok"])
        self.assertEqual(event["results"][0]["name"], "ok")

    def test_timeout_bytes_are_normalized_and_persisted_without_crashing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            task = control_loop.ControlTask("slow", 60, ("slow-command",), timeout_sec=45)
            state_file = root / "state.json"
            event_log = root / "events.jsonl"
            timeout = subprocess.TimeoutExpired(
                cmd=["slow-command"],
                timeout=45,
                output=b"partial stdout \xff",
                stderr=b"partial stderr \xfe",
            )
            with mock.patch.object(control_loop.subprocess, "run", side_effect=timeout):
                results = control_loop.run_once([task], state_file=state_file, event_log=event_log, env={})

            state = json.loads(state_file.read_text(encoding="utf-8"))
            event = json.loads(event_log.read_text(encoding="utf-8").splitlines()[0])

        self.assertEqual(results[0].returncode, 124)
        self.assertIsInstance(results[0].stdout_tail, str)
        self.assertIsInstance(results[0].stderr_tail, str)
        self.assertIn("timeout after 45s", results[0].stderr_tail)
        self.assertFalse(state["ok"])
        self.assertEqual(event["results"][0]["returncode"], 124)

    def test_task_runner_exception_is_recorded_as_task_failure(self) -> None:
        task = control_loop.ControlTask("missing", 60, ("missing-command",))
        with mock.patch.object(control_loop.subprocess, "run", side_effect=FileNotFoundError("missing-command")):
            result = control_loop.run_task(task, env={})

        self.assertEqual(result.returncode, 125)
        self.assertIn("FileNotFoundError", result.stderr_tail)

    def test_main_rejects_empty_task_selection(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            with mock.patch.dict("os.environ", {"STREAM_RUNTIME_STATE_DIR": str(root)}, clear=False):
                rc = control_loop.main(["--once", "--only", "missing"])

        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
