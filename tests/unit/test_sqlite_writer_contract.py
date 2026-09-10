from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from cra_dell_recovery.errors import LedgerUnavailable
from cra_dell_recovery.sqlite import SQLiteLedger


def test_nested_write_is_rejected_immediately(tmp_path: Path) -> None:
    migration = tmp_path / "001_test.sql"
    migration.write_text("CREATE TABLE value(id INTEGER PRIMARY KEY) STRICT;", encoding="utf-8")
    ledger = SQLiteLedger(tmp_path / "ledger.db", migration)
    try:
        with ledger.write(), pytest.raises(LedgerUnavailable, match="SQLITE_NESTED_WRITE_FORBIDDEN"), ledger.write():
            pass
    finally:
        ledger.close()


def test_failed_write_rolls_back_and_releases_writer_ownership(tmp_path: Path) -> None:
    migration = tmp_path / "001_test.sql"
    migration.write_text("CREATE TABLE value(id INTEGER PRIMARY KEY) STRICT;", encoding="utf-8")
    ledger = SQLiteLedger(tmp_path / "ledger.db", migration)
    try:
        with pytest.raises(RuntimeError, match="injected failure"), ledger.write() as database:
            database.execute("INSERT INTO value VALUES (1)")
            raise RuntimeError("injected failure")
        with ledger.write() as database:
            database.execute("INSERT INTO value VALUES (2)")
        assert ledger.read_one("SELECT group_concat(id) FROM value")[0] == "2"
    finally:
        ledger.close()


def test_constructor_failure_closes_database_connection(tmp_path: Path) -> None:
    migration = tmp_path / "001_invalid.sql"
    migration.write_text("CREATE TABLE broken(", encoding="utf-8")
    database_path = tmp_path / "ledger.db"
    with pytest.raises(sqlite3.OperationalError, match="incomplete input"):
        SQLiteLedger(database_path, migration)

    # A failed initialization must not retain a hidden connection or lock.
    connection = sqlite3.connect(database_path)
    try:
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        connection.close()


def test_disk_full_write_is_normalized_rolled_back_and_recoverable(tmp_path: Path) -> None:
    migration = tmp_path / "001_test.sql"
    migration.write_text("CREATE TABLE value(id INTEGER PRIMARY KEY,payload BLOB NOT NULL) STRICT;", encoding="utf-8")
    ledger = SQLiteLedger(tmp_path / "ledger.db", migration)
    try:
        page_count = int(ledger.connection.execute("PRAGMA page_count").fetchone()[0])
        ledger.connection.execute(f"PRAGMA max_page_count={page_count}")
        with pytest.raises(LedgerUnavailable, match="SQLITE_FULL"), ledger.write() as database:
            database.execute("INSERT INTO value VALUES (1,zeroblob(1048576))")
        assert ledger.read_one("SELECT count(*) FROM value")[0] == 0

        ledger.connection.execute(f"PRAGMA max_page_count={page_count + 1024}")
        with ledger.write() as database:
            database.execute("INSERT INTO value VALUES (2,x'00')")
        assert ledger.read_one("SELECT group_concat(id) FROM value")[0] == "2"
        assert ledger.integrity_check() == "ok"
    finally:
        ledger.close()
