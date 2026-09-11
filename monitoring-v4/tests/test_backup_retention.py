from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.commands import backup_retention

from tests.helpers import BASE_TS


def _pair(root: Path, *, created_ts: int, content: bytes) -> Path:
    stamp = datetime.fromtimestamp(created_ts, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dump = root / f"stream-v4-{stamp}.dump"
    dump.write_bytes(content)
    dump.chmod(0o600)
    checksum = dump.with_name(f"{dump.name}.sha256")
    checksum.write_text(
        f"{hashlib.sha256(content).hexdigest()}  {dump.name}\n",
        encoding="ascii",
    )
    checksum.chmod(0o600)
    os.utime(dump, (created_ts, created_ts))
    return dump


def _attestation(root: Path, backup: Path, *, verified_ts: int) -> Path:
    path = root / f"{backup.name}.restore-verified.json"
    path.write_text(
        json.dumps(
            {
                "schema": "monitoring_v4.postgresql_restore_verification.v1",
                "backup_name": backup.name,
                "backup_sha256": hashlib.sha256(backup.read_bytes()).hexdigest(),
                "verified_at": utc_text(verified_ts),
                "schema_versions": [1, 2, 3, 4, 5, 6],
                "observations": 1,
                "shadow_cycles": 1,
                "public_artifact_publications": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    path.chmod(0o640)
    return path


class BackupRetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.primary = root / "primary"
        self.independent = root / "independent"
        self.verification = root / "verification"
        self.primary.mkdir()
        self.independent.mkdir()
        self.verification.mkdir()
        self.arguments = Namespace(
            backup_dir=self.primary,
            independent_backup_dir=self.independent,
            restore_verification_dir=self.verification,
            backup_max_age_sec=27 * 3600,
            primary_retention_days=35,
            independent_retention_days=90,
            minimum_preserved_sets=2,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _device(self, root: Path) -> int:
        return 1 if Path(root) == self.primary else 2

    def _seed(self) -> Path:
        latest: Path | None = None
        for age_days in (120, 100, 40, 2, 1):
            created_ts = BASE_TS - age_days * backup_retention.DAY
            content = f"backup-{age_days}".encode("ascii")
            primary = _pair(self.primary, created_ts=created_ts, content=content)
            _pair(self.independent, created_ts=created_ts, content=content)
            if age_days == 1:
                latest = primary
        assert latest is not None
        return latest

    def test_deletion_requires_current_dual_copy_and_exact_restore_anchor(self) -> None:
        latest = self._seed()
        _attestation(self.verification, latest, verified_ts=BASE_TS - 30)
        with patch.object(
            backup_retention,
            "_directory_device",
            side_effect=self._device,
        ):
            report = backup_retention.run_retention(self.arguments, now_ts=BASE_TS)

        self.assertEqual(report["restore_anchor_backup"], latest.name)
        self.assertEqual(len(report["primary_deleted"]), 3)
        self.assertEqual(len(report["independent_deleted"]), 2)
        self.assertEqual(len(list(self.primary.glob("*.dump"))), 2)
        self.assertEqual(len(list(self.independent.glob("*.dump"))), 3)
        self.assertTrue(latest.exists())

    def test_missing_restore_anchor_deletes_nothing(self) -> None:
        self._seed()
        before = sorted(path.name for path in self.primary.iterdir())
        with patch.object(
            backup_retention,
            "_directory_device",
            side_effect=self._device,
        ):
            with self.assertRaisesRegex(RuntimeError, "full restore verification"):
                backup_retention.run_retention(self.arguments, now_ts=BASE_TS)
        self.assertEqual(sorted(path.name for path in self.primary.iterdir()), before)

    def test_changed_pair_is_not_deleted_after_inventory(self) -> None:
        dump = _pair(
            self.primary,
            created_ts=BASE_TS - 120 * backup_retention.DAY,
            content=b"before",
        )
        pair = backup_retention._pairs(self.primary)[0]
        dump.write_bytes(b"changed-after-inventory")
        with self.assertRaisesRegex(RuntimeError, "changed before deletion"):
            backup_retention._delete_pair(self.primary, pair)
        self.assertTrue(dump.exists())
        self.assertTrue(dump.with_name(f"{dump.name}.sha256").exists())

    def test_second_unlink_failure_preserves_data_bearing_dump(self) -> None:
        dump = _pair(
            self.primary,
            created_ts=BASE_TS - 120 * backup_retention.DAY,
            content=b"recoverable-data",
        )
        pair = backup_retention._pairs(self.primary)[0]
        real_unlink = os.unlink
        calls = 0

        def fail_dump_unlink(path: str, *, dir_fd: int) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("injected dump unlink failure")
            real_unlink(path, dir_fd=dir_fd)

        with patch.object(backup_retention.os, "unlink", side_effect=fail_dump_unlink):
            with self.assertRaisesRegex(OSError, "injected dump unlink failure"):
                backup_retention._delete_pair(self.primary, pair)
        self.assertTrue(dump.exists())
        self.assertEqual(dump.read_bytes(), b"recoverable-data")
        self.assertFalse(dump.with_name(f"{dump.name}.sha256").exists())

    def test_same_device_is_rejected_before_any_deletion(self) -> None:
        latest = self._seed()
        _attestation(self.verification, latest, verified_ts=BASE_TS - 30)
        with patch.object(backup_retention, "_directory_device", return_value=1):
            with self.assertRaisesRegex(RuntimeError, "physically distinct"):
                backup_retention.run_retention(self.arguments, now_ts=BASE_TS)

    def test_symlinked_backup_root_is_rejected(self) -> None:
        alias = self.primary.parent / "primary-alias"
        alias.symlink_to(self.primary, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, "not a directory"):
            backup_retention._directory_device(alias)

    def test_unsafe_retention_arguments_are_rejected_before_filesystem_access(self) -> None:
        for field, value, message in (
            ("primary_retention_days", 0, "primary-retention-days"),
            ("independent_retention_days", -1, "independent-retention-days"),
            ("minimum_preserved_sets", 1, "minimum-preserved-sets"),
            ("backup_max_age_sec", 3599, "backup-max-age-sec"),
        ):
            with self.subTest(field=field), patch.object(
                backup_retention,
                "_directory_device",
            ) as device:
                arguments = Namespace(**vars(self.arguments))
                setattr(arguments, field, value)
                with self.assertRaisesRegex(ValueError, message):
                    backup_retention.run_retention(arguments, now_ts=BASE_TS)
            device.assert_not_called()

    def test_candidate_limit_fails_closed_before_deletion(self) -> None:
        for age_days in (120, 100, 80):
            _pair(
                self.primary,
                created_ts=BASE_TS - age_days * backup_retention.DAY,
                content=f"backup-{age_days}".encode("ascii"),
            )
        before = sorted(path.name for path in self.primary.iterdir())
        with patch.object(backup_retention, "_MAX_BACKUP_MEMBERS", 4):
            with self.assertRaisesRegex(RuntimeError, "candidate limit exceeded"):
                backup_retention._delete_expired(
                    self.primary,
                    now_ts=BASE_TS,
                    retention_days=35,
                    minimum_preserved_sets=2,
                    protected_name="stream-v4-20990101T000000Z.dump",
                )
        self.assertEqual(sorted(path.name for path in self.primary.iterdir()), before)

    def test_real_distinct_filesystems_pass_without_device_mock(self) -> None:
        shared_memory = Path("/dev/shm")
        if not shared_memory.is_dir():
            self.skipTest("/dev/shm is unavailable")
        with tempfile.TemporaryDirectory() as primary_td, tempfile.TemporaryDirectory(
            dir=shared_memory
        ) as independent_td:
            primary = Path(primary_td) / "primary"
            independent = Path(independent_td) / "independent"
            verification = Path(primary_td) / "verification"
            primary.mkdir()
            independent.mkdir()
            verification.mkdir()
            if primary.stat().st_dev == independent.stat().st_dev:
                self.skipTest("test filesystems are not physically distinct")
            latest: Path | None = None
            for age_days in (120, 100, 40, 2, 1):
                created_ts = BASE_TS - age_days * backup_retention.DAY
                content = f"real-device-{age_days}".encode("ascii")
                candidate = _pair(
                    primary,
                    created_ts=created_ts,
                    content=content,
                )
                _pair(
                    independent,
                    created_ts=created_ts,
                    content=content,
                )
                if age_days == 1:
                    latest = candidate
            assert latest is not None
            _attestation(verification, latest, verified_ts=BASE_TS - 30)
            arguments = Namespace(
                backup_dir=primary,
                independent_backup_dir=independent,
                restore_verification_dir=verification,
                backup_max_age_sec=27 * 3600,
                primary_retention_days=35,
                independent_retention_days=90,
                minimum_preserved_sets=2,
            )
            report = backup_retention.run_retention(arguments, now_ts=BASE_TS)
            self.assertEqual(len(report["primary_deleted"]), 3)
            self.assertEqual(len(report["independent_deleted"]), 2)


if __name__ == "__main__":
    unittest.main()
