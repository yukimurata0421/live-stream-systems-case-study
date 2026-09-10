from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

from cra_dell_recovery.errors import LedgerUnavailable
from cra_dell_recovery.sqlite import SQLiteLedger


def test_existing_empty_database_receives_baseline_and_ordered_sibling_migration(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    baseline = migrations / "001_initial.sql"
    second = migrations / "002_second.sql"
    baseline.write_text("CREATE TABLE baseline_value(id INTEGER PRIMARY KEY) STRICT;", encoding="utf-8")
    second.write_text("CREATE TABLE second_value(id INTEGER PRIMARY KEY) STRICT;", encoding="utf-8")
    database = tmp_path / "existing-empty.db"
    database.touch()

    ledger = SQLiteLedger(database, baseline)
    try:
        tables = {
            str(row[0])
            for row in ledger.connection.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        }
        assert {"baseline_value", "second_value", "schema_migrations"} <= tables
        assert ledger.connection.execute("SELECT count(*) FROM schema_migrations").fetchone()[0] == 2
    finally:
        ledger.close()


def test_applied_migration_checksum_drift_fails_closed(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    baseline = migrations / "001_initial.sql"
    second = migrations / "002_second.sql"
    baseline.write_text("CREATE TABLE baseline_value(id INTEGER PRIMARY KEY) STRICT;", encoding="utf-8")
    second.write_text("CREATE TABLE second_value(id INTEGER PRIMARY KEY) STRICT;", encoding="utf-8")
    database = tmp_path / "ledger.db"
    ledger = SQLiteLedger(database, baseline)
    ledger.close()
    second.write_text("CREATE TABLE changed_value(id INTEGER PRIMARY KEY) STRICT;", encoding="utf-8")

    with pytest.raises(LedgerUnavailable, match="MIGRATION_CHECKSUM_MISMATCH:002_second.sql"):
        SQLiteLedger(database, baseline)


def test_unknown_migration_record_fails_closed(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    baseline = migrations / "001_initial.sql"
    baseline.write_text("CREATE TABLE baseline_value(id INTEGER PRIMARY KEY) STRICT;", encoding="utf-8")
    database = tmp_path / "ledger.db"
    ledger = SQLiteLedger(database, baseline)
    ledger.close()
    connection = sqlite3.connect(database)
    try:
        connection.execute("INSERT INTO schema_migrations VALUES ('999_unknown.sql','deadbeef','2026-09-01T00:00:00Z')")
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(LedgerUnavailable, match="MIGRATION_SET_MISMATCH"):
        SQLiteLedger(database, baseline)


def test_database_migration_and_sidecar_symlinks_fail_closed(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    baseline = migrations / "001_initial.sql"
    baseline.write_text("CREATE TABLE baseline_value(id INTEGER PRIMARY KEY) STRICT;", encoding="utf-8")
    database = tmp_path / "ledger.db"
    ledger = SQLiteLedger(database, baseline)
    ledger.close()

    database_link = tmp_path / "database-link.db"
    database_link.symlink_to(database)
    with pytest.raises(LedgerUnavailable, match="SQLITE_DATABASE_NOT_REGULAR"):
        SQLiteLedger(database_link, baseline)

    migration_link = tmp_path / "migration-link.sql"
    migration_link.symlink_to(baseline)
    with pytest.raises(LedgerUnavailable, match="SQLITE_MIGRATION_NOT_REGULAR"):
        SQLiteLedger(tmp_path / "migration-link.db", migration_link)

    sidecar_target = tmp_path / "sidecar-target"
    sidecar_target.touch()
    sidecar = database.with_name(f"{database.name}-wal")
    sidecar.symlink_to(sidecar_target)
    with pytest.raises(LedgerUnavailable, match="SQLITE_SIDECAR_NOT_REGULAR:-wal"):
        SQLiteLedger(database, baseline)
    sidecar.unlink()


def test_backup_destination_is_exclusive_and_does_not_follow_symlink(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    baseline = migrations / "001_initial.sql"
    baseline.write_text("CREATE TABLE baseline_value(id INTEGER PRIMARY KEY) STRICT;", encoding="utf-8")
    ledger = SQLiteLedger(tmp_path / "ledger.db", baseline)
    try:
        existing = tmp_path / "existing.db"
        existing.write_bytes(b"preserve")
        with pytest.raises(LedgerUnavailable, match="SQLITE_BACKUP_DESTINATION_EXISTS"):
            ledger.backup_to(existing)
        assert existing.read_bytes() == b"preserve"

        symlink = tmp_path / "backup-link.db"
        symlink.symlink_to(existing)
        with pytest.raises(LedgerUnavailable, match="SQLITE_BACKUP_DESTINATION_EXISTS"):
            ledger.backup_to(symlink)
        assert existing.read_bytes() == b"preserve"

        destination = tmp_path / "backup.db"
        ledger.backup_to(destination)
        assert os.stat(destination).st_mode & 0o777 == 0o600
    finally:
        ledger.close()
