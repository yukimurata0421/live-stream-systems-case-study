from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stream_v2.cli import main
from stream_v2.stream_app import (
    ALLOW_MUTATING_ENV,
    app_cli_command,
    is_mutating_app_command,
    run_stream_cli,
)


class TestStreamAppCliGuard(unittest.TestCase):
    def test_detects_mutating_stream_commands_after_options(self) -> None:
        self.assertEqual(app_cli_command(["--lines", "20", "restart"]), "restart")
        self.assertTrue(is_mutating_app_command(["start"]))
        self.assertTrue(is_mutating_app_command(["--", "enable"]))
        self.assertTrue(is_mutating_app_command(["--lines=20", "stop"]))
        self.assertTrue(is_mutating_app_command(["watch"]))
        self.assertTrue(is_mutating_app_command(["maintenance", "on"]))
        self.assertTrue(is_mutating_app_command(["maint", "on"]))
        self.assertTrue(is_mutating_app_command(["m", "on"]))
        self.assertTrue(is_mutating_app_command(["pause"]))
        self.assertTrue(is_mutating_app_command(["resume"]))
        self.assertFalse(is_mutating_app_command(["maintenance", "status"]))
        self.assertFalse(is_mutating_app_command(["m", "s"]))
        self.assertFalse(is_mutating_app_command(["status"]))
        self.assertFalse(is_mutating_app_command(["--help"]))

    def test_mutating_stream_command_is_refused_by_default(self) -> None:
        with patch("stream_v2.stream_app.subprocess.run") as subprocess_run:
            rc = run_stream_cli(["restart"])
        self.assertEqual(rc, 2)
        subprocess_run.assert_not_called()

        with patch("stream_v2.stream_app.subprocess.run") as subprocess_run:
            rc = run_stream_cli(["pause"])
        self.assertEqual(rc, 2)
        subprocess_run.assert_not_called()

    def test_mutating_stream_command_can_be_explicitly_allowed(self) -> None:
        completed = Mock(returncode=0)
        with patch("stream_v2.stream_app.subprocess.run", return_value=completed) as subprocess_run:
            rc = run_stream_cli(["start"], allow_mutating=True)
        self.assertEqual(rc, 0)
        subprocess_run.assert_called_once()
        argv = subprocess_run.call_args.args[0]
        self.assertEqual(argv[-1], "start")
        self.assertEqual(subprocess_run.call_args.kwargs["env"][ALLOW_MUTATING_ENV], "1")

    def test_mutating_stream_command_can_be_allowed_by_env(self) -> None:
        completed = Mock(returncode=0)
        old = os.environ.get(ALLOW_MUTATING_ENV)
        os.environ[ALLOW_MUTATING_ENV] = "1"
        try:
            with patch("stream_v2.stream_app.subprocess.run", return_value=completed) as subprocess_run:
                rc = run_stream_cli(["stop"])
        finally:
            if old is None:
                os.environ.pop(ALLOW_MUTATING_ENV, None)
            else:
                os.environ[ALLOW_MUTATING_ENV] = old
        self.assertEqual(rc, 0)
        subprocess_run.assert_called_once()

    def test_stream_cli_main_blocks_start_without_cutover_flag(self) -> None:
        with patch("stream_v2.stream_app.subprocess.run") as subprocess_run:
            rc = main(["stream-cli", "--", "start"])
        self.assertEqual(rc, 2)
        subprocess_run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
