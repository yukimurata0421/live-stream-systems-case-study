from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.commands.retention import (
    RETENTION_SPECS,
    _run_retention,
    _validated_backups,
)

from tests.helpers import BASE_TS


def _backup(root: Path, content: bytes, *, mtime: int = BASE_TS - 60) -> Path:
    path = root / "stream-v4-20260814T000000Z.dump"
    path.write_bytes(content)
    path.chmod(0o600)
    digest = hashlib.sha256(content).hexdigest()
    checksum = path.with_name(f"{path.name}.sha256")
    checksum.write_text(
        f"{digest}  {path.name}\n",
        encoding="ascii",
    )
    checksum.chmod(0o600)
    import os

    os.utime(path, (mtime, mtime))
    return path


def _restore_verification(root: Path, backup: Path, *, verified_ts: int) -> Path:
    digest = hashlib.sha256(backup.read_bytes()).hexdigest()
    path = root / f"{backup.name}.restore-verified.json"
    path.write_text(
        json.dumps(
            {
                "schema": "monitoring_v4.postgresql_restore_verification.v1",
                "backup_name": backup.name,
                "backup_sha256": digest,
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


class _Cursor:
    def __init__(self, *, row=None, rowcount: int = -1) -> None:
        self._row = row
        self.rowcount = rowcount

    def fetchone(self):
        return self._row


class _Connection:
    def __init__(self, *, released: bool = True) -> None:
        self.closed = False
        self.discarded = False
        self.released = released
        self.delete_sql: list[str] = []

    def execute(self, sql, params=()):
        del params
        if "pg_try_advisory_lock" in sql:
            return _Cursor(row={"acquired": True})
        if sql.lstrip().startswith("WITH doomed"):
            self.delete_sql.append(sql)
            return _Cursor(rowcount=2)
        if "pg_advisory_unlock" in sql:
            return _Cursor(row={"released": self.released})
        return _Cursor()

    def close(self) -> None:
        self.closed = True

    def discard(self) -> None:
        self.discarded = True
        self.closed = True


class _Repository:
    def __init__(self, *, released: bool = True) -> None:
        self.connection = _Connection(released=released)

    def connect(self):
        return self.connection


class RetentionTests(unittest.TestCase):
    def test_retention_requires_matching_recent_verified_copies(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            primary = root / "primary"
            independent = root / "independent"
            primary.mkdir()
            independent.mkdir()
            verification = root / "verification"
            verification.mkdir()
            primary_backup = _backup(primary, b"same")
            _backup(independent, b"same")
            _restore_verification(
                verification,
                primary_backup,
                verified_ts=BASE_TS - 30,
            )
            args = Namespace(
                backup_dir=primary,
                independent_backup_dir=independent,
                restore_verification_dir=verification,
                backup_max_age_sec=3600,
            )
            with patch(
                "stream_monitoring_v4.commands.retention.backup_directory_device",
                side_effect=(1, 2),
            ):
                result = _validated_backups(args, now_ts=BASE_TS)
            self.assertEqual(result["basename"], "stream-v4-20260814T000000Z.dump")

            _backup(independent, b"different")
            with patch(
                "stream_monitoring_v4.commands.retention.backup_directory_device",
                side_effect=(1, 2),
            ):
                with self.assertRaisesRegex(RuntimeError, "matching fresh verified"):
                    _validated_backups(args, now_ts=BASE_TS)

    def test_retention_refuses_backup_without_full_restore_verification(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            primary = root / "primary"
            independent = root / "independent"
            verification = root / "verification"
            primary.mkdir()
            independent.mkdir()
            verification.mkdir()
            _backup(primary, b"same")
            _backup(independent, b"same")
            args = Namespace(
                backup_dir=primary,
                independent_backup_dir=independent,
                restore_verification_dir=verification,
                backup_max_age_sec=3600,
            )
            with patch(
                "stream_monitoring_v4.commands.retention.backup_directory_device",
                side_effect=(1, 2),
            ):
                with self.assertRaisesRegex(RuntimeError, "full isolated restore"):
                    _validated_backups(args, now_ts=BASE_TS)

    def test_retention_rejects_same_filesystem_and_unsafe_age_before_backup_read(self) -> None:
        args = Namespace(
            backup_dir=Path("/not-read-primary"),
            independent_backup_dir=Path("/not-read-independent"),
            restore_verification_dir=Path("/not-read-verification"),
            backup_max_age_sec=3600,
        )
        with patch(
            "stream_monitoring_v4.commands.retention.backup_directory_device",
            return_value=1,
        ), patch(
            "stream_monitoring_v4.commands.retention.verified_backup_status"
        ) as status:
            with self.assertRaisesRegex(RuntimeError, "physically distinct"):
                _validated_backups(args, now_ts=BASE_TS)
        status.assert_not_called()

        args.backup_max_age_sec = 3599
        with patch(
            "stream_monitoring_v4.commands.retention.backup_directory_device"
        ) as device:
            with self.assertRaisesRegex(ValueError, "backup-max-age-sec"):
                _validated_backups(args, now_ts=BASE_TS)
        device.assert_not_called()

    def test_retention_plan_is_bounded_and_preserves_current_audit_references(self) -> None:
        by_name = {spec.name: spec for spec in RETENTION_SPECS}
        self.assertGreaterEqual(by_name["shadow_cycles"].retention_days, 45)
        self.assertGreaterEqual(by_name["observations"].retention_days, 120)
        self.assertIn(
            "retention_protected_observations",
            by_name["observations"].sql,
        )
        current_sql = by_name["current_snapshots_unreferenced"].sql
        self.assertIn("domain_current", current_sql)
        self.assertIn("incident_candidates", current_sql)
        self.assertIn("incident_transitions", current_sql)
        self.assertNotIn("incident_episodes", by_name)
        self.assertNotIn("notification_intents", by_name)
        self.assertTrue(all("LIMIT ?" in spec.sql for spec in RETENTION_SPECS))

    def test_each_retention_statement_runs_in_a_bounded_batch(self) -> None:
        repository = _Repository()
        acquired, deleted = _run_retention(
            repository,  # type: ignore[arg-type]
            now_ts=BASE_TS,
            batch_size=100,
        )
        self.assertTrue(acquired)
        self.assertEqual(set(deleted), {spec.name for spec in RETENTION_SPECS})
        self.assertTrue(all(count == 2 for count in deleted.values()))
        self.assertEqual(len(repository.connection.delete_sql), len(RETENTION_SPECS))
        self.assertTrue(repository.connection.closed)
        self.assertFalse(repository.connection.discarded)

    def test_ambiguous_retention_unlock_discards_physical_session(self) -> None:
        repository = _Repository(released=False)
        acquired, _deleted = _run_retention(
            repository,  # type: ignore[arg-type]
            now_ts=BASE_TS,
            batch_size=100,
        )
        self.assertTrue(acquired)
        self.assertTrue(repository.connection.discarded)


if __name__ == "__main__":
    unittest.main()
