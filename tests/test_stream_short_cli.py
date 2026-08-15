from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = ROOT / "bin" / "stream-short"


class StreamShortCliTests(unittest.TestCase):
    def _fake_ssh(self, root: Path) -> Path:
        ssh = root / "ssh"
        ssh.write_text("#!/usr/bin/env bash\nprintf '[%s]\\n' \"$@\"\n", encoding="utf-8")
        ssh.chmod(0o755)
        return ssh

    def test_sli_report_alias_hides_ssh_and_forwards_safe_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._fake_ssh(root)
            env = os.environ.copy()
            env["PATH"] = f"{root}:{env['PATH']}"
            env["STREAM_V3_ARENA_SSH_HOST"] = "test-arena"
            env["STREAM_V3_ARENA_REPO"] = "/srv/stream v3"
            completed = subprocess.run(
                [str(LAUNCHER), "-sli-report", "--windows", "7d", "--end-time", "value;not-a-command"],
                cwd=ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(
            completed.stdout.splitlines(),
            [
                "[test-arena]",
                "[cd /srv/stream\\ v3 && bin/stream-prod sli-report --windows 7d --end-time value\\;not-a-command]",
            ],
        )

    def test_documented_aliases_route_to_the_same_read_only_remote_command(self) -> None:
        for alias in ("-sli-report", "--sli-report", "sli-report"):
            with self.subTest(alias=alias), tempfile.TemporaryDirectory() as td:
                root = Path(td)
                self._fake_ssh(root)
                env = os.environ.copy()
                env["PATH"] = f"{root}:{env['PATH']}"
                completed = subprocess.run(
                    [str(LAUNCHER), alias],
                    cwd=ROOT,
                    env=env,
                    text=True,
                    capture_output=True,
                    check=False,
                )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("[stream-monitor]", completed.stdout)
            self.assertIn("[cd /opt/stream_v3 && bin/stream-prod sli-report]", completed.stdout)

    def test_unknown_command_is_rejected_without_ssh(self) -> None:
        completed = subprocess.run(
            [str(LAUNCHER), "restart"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 2)
        self.assertIn("unsupported command", completed.stderr)


if __name__ == "__main__":
    unittest.main()
