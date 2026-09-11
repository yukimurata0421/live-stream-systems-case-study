from __future__ import annotations

import io
import json
import os
import signal
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from stream_monitoring_v4.commands import core_runner, input_projector, wall_clock_loop
from stream_monitoring_v4.runtime.health_files import validate_distinct_health_files


BASE_TS = 1_775_000_000


class _Repository:
    def __init__(self) -> None:
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1


class _Projection:
    def __init__(self, ready: bool) -> None:
        self.ready = ready

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "monitoring_v4.safe_input_projection.v4",
            "ready": self.ready,
        }


class RuntimeEntrypointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_health_files_reject_path_symlink_and_hardlink_aliases(self) -> None:
        ready = self.root / "ready"
        ready.write_text("1\n", encoding="ascii")
        hardlink = self.root / "heartbeat-hardlink"
        os.link(ready, hardlink)
        symlink = self.root / "heartbeat-symlink"
        symlink.symlink_to(ready)

        with self.assertRaisesRegex(ValueError, "must be distinct"):
            validate_distinct_health_files(ready, ready)
        with self.assertRaisesRegex(ValueError, "must be distinct"):
            validate_distinct_health_files(ready, symlink)
        with self.assertRaisesRegex(ValueError, "must not alias"):
            validate_distinct_health_files(ready, hardlink)

        validate_distinct_health_files(ready, self.root / "heartbeat-new")
        validate_distinct_health_files(None, ready)

    def test_all_health_entrypoints_reject_alias_before_work(self) -> None:
        shared = self.root / "shared-health"
        core_arguments = [
            "--db",
            str(self.root / "core.sqlite3"),
            "--state-root",
            str(self.root / "source"),
            "--ready-file",
            str(shared),
            "--heartbeat-file",
            str(shared),
        ]
        with patch.object(core_runner.shadow_once, "initialize_runtime") as initialize:
            with self.assertRaisesRegex(ValueError, "must be distinct"):
                core_runner.main(core_arguments)
        initialize.assert_not_called()

        projector_arguments = [
            "--source-root",
            str(self.root / "raw"),
            "--target-root",
            str(self.root / "safe"),
            "--ready-file",
            str(shared),
            "--heartbeat-file",
            str(shared),
            "--once",
        ]
        with patch.object(input_projector, "project_safe_inputs") as project:
            with self.assertRaisesRegex(ValueError, "must be distinct"):
                input_projector.main(projector_arguments)
        project.assert_not_called()

        with patch.object(wall_clock_loop.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "must be distinct"):
                wall_clock_loop.main(
                    [
                        "--second",
                        "20",
                        "--ready-file",
                        str(shared),
                        "--heartbeat-file",
                        str(shared),
                        "--",
                        "/bin/true",
                    ]
                )
        run.assert_not_called()

    def test_input_projector_rejects_nonfinite_interval_before_work(self) -> None:
        arguments = [
            "--source-root",
            str(self.root / "raw"),
            "--target-root",
            str(self.root / "safe"),
            "--interval-sec",
            "nan",
            "--once",
        ]
        with patch.object(input_projector, "project_safe_inputs") as project:
            with self.assertRaisesRegex(ValueError, "positive finite"):
                input_projector.main(arguments)
        project.assert_not_called()

    def test_input_projector_once_withdraws_ready_on_rejection(self) -> None:
        ready = self.root / "projector-ready"
        heartbeat = self.root / "projector-heartbeat"
        ready.write_text("stale\n", encoding="ascii")
        arguments = [
            "--source-root",
            str(self.root / "raw"),
            "--target-root",
            str(self.root / "safe"),
            "--ready-file",
            str(ready),
            "--heartbeat-file",
            str(heartbeat),
            "--once",
        ]
        with patch.object(
            input_projector,
            "project_safe_inputs",
            return_value=_Projection(False),
        ), patch.object(input_projector.time, "time", return_value=BASE_TS), redirect_stdout(
            io.StringIO()
        ):
            self.assertEqual(input_projector.main(arguments), 1)
        self.assertFalse(ready.exists())
        self.assertEqual(heartbeat.read_text(encoding="ascii"), f"{BASE_TS}\n")

    def test_core_runner_survives_one_failed_cycle_then_recovers(self) -> None:
        ready = self.root / "core-ready"
        heartbeat = self.root / "core-heartbeat"
        repository = _Repository()
        handlers: dict[int, object] = {}
        calls = 0

        def install(signum: int, handler: object) -> None:
            handlers[signum] = handler

        def run_once(*_args: object, **_kwargs: object) -> int:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("injected cycle failure")
            handler = handlers[signal.SIGTERM]
            assert callable(handler)
            handler(signal.SIGTERM, None)
            return 0

        output = io.StringIO()
        arguments = [
            "--db",
            str(self.root / "core.sqlite3"),
            "--state-root",
            str(self.root / "source"),
            "--ready-file",
            str(ready),
            "--heartbeat-file",
            str(heartbeat),
        ]
        with patch.object(core_runner.signal, "signal", side_effect=install), patch.object(
            core_runner.shadow_once,
            "initialize_runtime",
            return_value=(repository, self.root / "core.sqlite3"),
        ), patch.object(core_runner.shadow_once, "run_parsed_once", side_effect=run_once), patch.object(
            core_runner, "next_run_epoch", return_value=BASE_TS
        ), patch.object(core_runner.time, "time", return_value=float(BASE_TS)), patch.object(
            core_runner.time, "monotonic", return_value=10.0
        ), redirect_stdout(output):
            self.assertEqual(core_runner.main(arguments), 0)

        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([event["status"] for event in events], ["failed", "good"])
        self.assertEqual(events[0]["error_type"], "RuntimeError")
        self.assertEqual(calls, 2)
        self.assertEqual(repository.close_count, 1)
        self.assertEqual(ready.read_text(encoding="ascii"), f"{BASE_TS}\n")
        self.assertEqual(heartbeat.read_text(encoding="ascii"), f"{BASE_TS}\n")

    def test_wall_clock_requires_a_command_after_separator(self) -> None:
        with self.assertRaisesRegex(ValueError, "command is required"):
            wall_clock_loop.main(["--second", "20", "--"])

    def test_wall_clock_records_success_failure_and_timeout_without_false_ready(self) -> None:
        for outcome, response, expected_code, expect_ready in (
            ("ok", SimpleNamespace(returncode=0), 0, True),
            ("failed", SimpleNamespace(returncode=7), 7, False),
            ("timeout", subprocess.TimeoutExpired(["probe"], 1), 124, False),
        ):
            with self.subTest(outcome=outcome):
                ready = self.root / f"ready-{outcome}"
                heartbeat = self.root / f"heartbeat-{outcome}"

                def execute(*_args: object, **_kwargs: object) -> object:
                    wall_clock_loop._stop(signal.SIGTERM, None)
                    if isinstance(response, BaseException):
                        raise response
                    return response

                output = io.StringIO()
                with patch.object(wall_clock_loop.signal, "signal"), patch.object(
                    wall_clock_loop, "next_run_epoch", return_value=BASE_TS
                ), patch.object(wall_clock_loop.time, "time", return_value=float(BASE_TS)), patch.object(
                    wall_clock_loop.time, "monotonic", return_value=10.0
                ), patch.object(wall_clock_loop.subprocess, "run", side_effect=execute), redirect_stdout(
                    output
                ):
                    self.assertEqual(
                        wall_clock_loop.main(
                            [
                                "--second",
                                "20",
                                "--ready-file",
                                str(ready),
                                "--heartbeat-file",
                                str(heartbeat),
                                "--",
                                "probe",
                            ]
                        ),
                        0,
                    )
                event = json.loads(output.getvalue())
                self.assertEqual(event["outcome"], outcome)
                self.assertEqual(event["returncode"], expected_code)
                self.assertEqual(ready.exists(), expect_ready)
                self.assertTrue(heartbeat.is_file())


if __name__ == "__main__":
    unittest.main()
