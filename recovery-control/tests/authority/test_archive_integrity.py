from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from cra_authority.archive_integrity import ArchiveIntegrityConfig, run_check


def _database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.executescript(
            """
            CREATE TABLE archive_records (record_id TEXT PRIMARY KEY, payload TEXT NOT NULL) STRICT;
            CREATE TABLE daily_checkpoints (utc_day TEXT PRIMARY KEY) STRICT;
            INSERT INTO archive_records VALUES ('record-a', '{}');
            INSERT INTO daily_checkpoints VALUES ('2026-09-25');
            """
        )
        connection.commit()
    finally:
        connection.close()
    os.chmod(path, 0o600)


def _config(tmp_path: Path, archive: Path) -> ArchiveIntegrityConfig:
    release = "cra-no-action-test-release"
    status = tmp_path / "releases" / release / "archive-integrity-status.json"
    value = {
        "schema": "cra.archive_integrity.v1",
        "runtime_release_id": release,
        "archive_database": str(archive),
        "status_file": str(status),
        "busy_timeout_seconds": 5,
    }
    path = tmp_path / "archive-integrity.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    os.chmod(path, 0o600)
    return ArchiveIntegrityConfig.load(path)


def test_external_archive_integrity_is_read_only_release_and_inode_bound(tmp_path: Path) -> None:
    archive = tmp_path / "archive.sqlite3"
    _database(archive)
    before = archive.stat()
    config = _config(tmp_path, archive)

    result = run_check(config)

    after = archive.stat()
    assert result["status"] == "PASS"
    assert result["archive_integrity_check"] == "ok"
    assert result["archive_quick_check"] == "ok"
    assert result["archive_journal_mode"] == "wal"
    assert result["archive_record_count"] == 1
    assert result["archive_checkpoint_count"] == 1
    assert result["archive_device"] == after.st_dev == before.st_dev
    assert result["archive_inode"] == after.st_ino == before.st_ino
    assert json.loads(Path(config.value["status_file"]).read_text(encoding="utf-8")) == result


def test_external_archive_integrity_publishes_failure_without_reusing_old_pass(tmp_path: Path) -> None:
    archive = tmp_path / "archive.sqlite3"
    archive.write_bytes(b"not sqlite")
    os.chmod(archive, 0o600)
    config = _config(tmp_path, archive)

    result = run_check(config)

    assert result["status"] == "FAIL"
    assert "archive_inode" not in result
    assert result["reason_code"] != "ARCHIVE_INTEGRITY_VALID"
    assert json.loads(Path(config.value["status_file"]).read_text(encoding="utf-8")) == result


def test_external_archive_integrity_rejects_symlink_database(tmp_path: Path) -> None:
    actual = tmp_path / "actual.sqlite3"
    _database(actual)
    archive = tmp_path / "archive.sqlite3"
    archive.symlink_to(actual)
    config = _config(tmp_path, archive)

    result = run_check(config)

    assert result["status"] == "FAIL"
    assert "archive_device" not in result
