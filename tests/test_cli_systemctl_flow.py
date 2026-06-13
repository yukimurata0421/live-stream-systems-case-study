from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "stream_core"))

import cli  # type: ignore


def cp(returncode: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["systemctl"], returncode=returncode, stdout=stdout, stderr=stderr)


class CliSystemctlFlowTests(unittest.TestCase):
    def test_start_unit_fails_when_systemctl_start_fails(self) -> None:
        with mock.patch("cli.run_systemctl", return_value=cp(1, stderr="boom")):
            with mock.patch("cli.is_active", return_value=False):
                with mock.patch("builtins.print"):
                    self.assertFalse(cli.start_unit("dummy.service"))

    def test_start_unit_fails_when_unit_not_active_after_start(self) -> None:
        with mock.patch("cli.run_systemctl", return_value=cp(0)):
            with mock.patch("cli.is_active", return_value=False):
                with mock.patch("builtins.print"):
                    self.assertFalse(cli.start_unit("dummy.service"))

    def test_start_unit_succeeds_when_start_and_active_ok(self) -> None:
        def run_systemctl(args: list[str], check: bool = True):
            if args[0] == "is-active":
                return cp(0, stdout="active\n")
            return cp(0)

        with mock.patch("cli.run_systemctl", side_effect=run_systemctl):
            with mock.patch("cli.is_active", return_value=True):
                with mock.patch("builtins.print"):
                    self.assertTrue(cli.start_unit("dummy.service"))

    def test_restart_unit_fails_when_systemctl_restart_fails(self) -> None:
        with mock.patch("cli.run_systemctl", return_value=cp(1, stderr="boom")):
            with mock.patch("cli.is_active", return_value=False):
                with mock.patch("builtins.print"):
                    self.assertFalse(cli.restart_unit("dummy.service"))

    def test_restart_unit_succeeds_when_restart_and_active_ok(self) -> None:
        def run_systemctl(args: list[str], check: bool = True):
            if args[0] == "is-active":
                return cp(0, stdout="active\n")
            return cp(0)

        with mock.patch("cli.run_systemctl", side_effect=run_systemctl):
            with mock.patch("cli.is_active", return_value=True):
                with mock.patch("builtins.print"):
                    self.assertTrue(cli.restart_unit("dummy.service", reason="test"))

    def test_stream_v3_refuses_mutating_systemd_commands_by_default(self) -> None:
        with mock.patch("builtins.print"):
            self.assertEqual(cli.guard_stream_v3_mutating_command("start"), 1)

    def test_stream_v3_refuses_mutating_systemd_commands_in_release_archive_tree(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "live-stream-systems-case-study-3.4.0"
            (root / "ops" / "systemd").mkdir(parents=True)
            (root / "src" / "stream_core").mkdir(parents=True)
            (root / "src" / "stream_core" / "cli.py").write_text("", encoding="utf-8")
            (root / "pyproject.toml").write_text('[project]\nname = "stream-v3"\n', encoding="utf-8")

            with mock.patch.object(cli, "BASE_DIR", root):
                with mock.patch("builtins.print"):
                    self.assertEqual(cli.guard_stream_v3_mutating_command("start"), 1)

    def test_stream_v3_guard_does_not_block_outside_stream_repo_tree(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "scratch"
            root.mkdir()

            with mock.patch.object(cli, "BASE_DIR", root):
                self.assertEqual(cli.guard_stream_v3_mutating_command("start"), 0)

    def test_stream_v3_allows_mutating_systemd_commands_with_explicit_env(self) -> None:
        with mock.patch.dict("os.environ", {"STREAM_V2_ALLOW_MUTATING_SYSTEMD": "1"}):
            self.assertEqual(cli.guard_stream_v3_mutating_command("start"), 0)

    def test_stream_v3_guard_does_not_block_read_only_commands(self) -> None:
        self.assertEqual(cli.guard_stream_v3_mutating_command("doctor"), 0)
        self.assertEqual(cli.guard_stream_v3_mutating_command("contract-check"), 0)


if __name__ == "__main__":
    unittest.main()
