from __future__ import annotations

import sys
from pathlib import Path
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ops" / "scripts"))

import ensure_pulse  # type: ignore


class EnsurePulseTests(unittest.TestCase):
    def test_write_with_backup_updates_file_and_creates_backup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "client.conf"
            p.write_text("old\n", encoding="utf-8")
            ensure_pulse.write_with_backup(p, "new\n", dry_run=False)
            self.assertEqual(p.read_text(encoding="utf-8"), "new\n")
            backups = sorted(p.parent.glob("client.conf.bak.*"))
            self.assertTrue(backups)

    def test_restore_pulse_configs_uses_latest_backup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            fake_home = Path(td)
            pulse_dir = fake_home / ".config" / "pulse"
            pulse_dir.mkdir(parents=True, exist_ok=True)
            target = pulse_dir / "client.conf"
            target.write_text("current\n", encoding="utf-8")
            older = pulse_dir / "client.conf.bak.20000101_000000"
            newer = pulse_dir / "client.conf.bak.21000101_000000"
            older.write_text("older\n", encoding="utf-8")
            newer.write_text("newer\n", encoding="utf-8")
            daemon_target = pulse_dir / "daemon.conf"
            daemon_target.write_text("daemon-current\n", encoding="utf-8")
            daemon_bak = pulse_dir / "daemon.conf.bak.21000101_000000"
            daemon_bak.write_text("daemon-newer\n", encoding="utf-8")

            with mock.patch("ensure_pulse.Path.home", return_value=fake_home):
                rc = ensure_pulse.restore_pulse_configs(dry_run=False)

            self.assertEqual(rc, 0)
            self.assertEqual(target.read_text(encoding="utf-8"), "newer\n")
            self.assertEqual(daemon_target.read_text(encoding="utf-8"), "daemon-newer\n")


if __name__ == "__main__":
    unittest.main()

