from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from stream_monitoring_v4.commands import restore_select
from stream_monitoring_v4.runtime import backup_integrity

from tests.helpers import BASE_TS


def _backup(root: Path, content: bytes, *, modified_ts: int) -> Path:
    path = root / "stream-v4-20260814T000000Z.dump"
    path.write_bytes(content)
    path.chmod(0o600)
    checksum = path.with_name(f"{path.name}.sha256")
    checksum.write_text(
        f"{hashlib.sha256(content).hexdigest()}  {path.name}\n", encoding="ascii"
    )
    checksum.chmod(0o600)
    os.utime(path, (modified_ts, modified_ts))
    return path


class RestoreSelectTests(unittest.TestCase):
    def test_selects_matching_stable_copy_and_writes_bounded_work_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            primary = root / "primary"
            independent = root / "independent"
            work = root / "work"
            primary.mkdir()
            independent.mkdir()
            _backup(primary, b"restore-data", modified_ts=BASE_TS - 60)
            _backup(independent, b"restore-data", modified_ts=BASE_TS - 60)
            args = Namespace(
                backup_dir=primary,
                independent_backup_dir=independent,
                work_dir=work,
                backup_max_age_sec=3600,
            )
            with patch.object(
                restore_select, "backup_directory_device", side_effect=(1, 2)
            ):
                report = restore_select.run(args, now_ts=BASE_TS)
            self.assertEqual((work / "selected.dump").read_bytes(), b"restore-data")
            self.assertEqual(report["backup_sha256"], (work / "backup-sha256").read_text().strip())
            self.assertEqual((work / "selected.dump").stat().st_mode & 0o777, 0o640)

    def test_same_device_and_mismatch_fail_before_output(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            primary = root / "primary"
            independent = root / "independent"
            work = root / "work"
            primary.mkdir()
            independent.mkdir()
            _backup(primary, b"one", modified_ts=BASE_TS - 60)
            _backup(independent, b"two", modified_ts=BASE_TS - 60)
            args = Namespace(
                backup_dir=primary,
                independent_backup_dir=independent,
                work_dir=work,
                backup_max_age_sec=3600,
            )
            with patch.object(restore_select, "backup_directory_device", return_value=1):
                with self.assertRaisesRegex(RuntimeError, "physically distinct"):
                    restore_select.run(args, now_ts=BASE_TS)
            self.assertFalse(work.exists())
            with patch.object(
                restore_select, "backup_directory_device", side_effect=(1, 2)
            ):
                with self.assertRaisesRegex(RuntimeError, "matching fresh"):
                    restore_select.run(args, now_ts=BASE_TS)
            self.assertFalse(work.exists())

    def test_copy_closes_source_when_temporary_creation_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source = _backup(root, b"fd-data", modified_ts=BASE_TS - 60)
            descriptor, metadata = backup_integrity._open_regular(source)
            status = {
                "fresh": True,
                "path": str(source),
                "sha256": hashlib.sha256(b"fd-data").hexdigest(),
            }
            with patch.object(
                backup_integrity, "_open_regular", return_value=(descriptor, metadata)
            ), patch.object(
                backup_integrity.tempfile, "mkstemp", side_effect=OSError("injected")
            ):
                with self.assertRaisesRegex(OSError, "injected"):
                    backup_integrity.copy_verified_backup(status, root / "work" / "dump")
            with self.assertRaises(OSError):
                os.fstat(descriptor)


if __name__ == "__main__":
    unittest.main()
