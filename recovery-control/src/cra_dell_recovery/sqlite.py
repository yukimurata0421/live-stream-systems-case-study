from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from cra_dell_recovery.errors import LedgerUnavailable

MINIMUM_PRODUCTION_SQLITE = (3, 51, 3)


def _require_regular_file(path: Path, *, reason: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as error:
        raise LedgerUnavailable(f"{reason}_MISSING") from error
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise LedgerUnavailable(f"{reason}_NOT_REGULAR")


def _reject_unsafe_database_entry(path: Path) -> None:
    if not os.path.lexists(path):
        return
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise LedgerUnavailable("SQLITE_DATABASE_NOT_REGULAR")


def _reject_unsafe_sidecars(path: Path) -> None:
    for suffix in ("-journal", "-shm", "-wal"):
        sidecar = path.with_name(f"{path.name}{suffix}")
        if not os.path.lexists(sidecar):
            continue
        metadata = sidecar.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise LedgerUnavailable(f"SQLITE_SIDECAR_NOT_REGULAR:{suffix}")


def production_sqlite_gate(version: str | None = None, *, backport_proven: bool = False) -> bool:
    parsed = tuple(int(part) for part in (version or sqlite3.sqlite_version).split(".")[:3])
    return parsed >= MINIMUM_PRODUCTION_SQLITE or backport_proven


@dataclass(frozen=True)
class SQLiteRuntimeStatus:
    version: str
    production_gate: str
    journal_mode: str
    synchronous: int
    foreign_keys: int
    busy_timeout: int
    wal_autocheckpoint: int
    trusted_schema: int
    quick_check: str
    foreign_key_check: str


class SQLiteLedger:
    """One serialized writer plus short-lived role-owned SQLite connections."""

    def __init__(self, path: Path, migration: Path, *, restored: bool = False) -> None:
        self.path = path
        self.migration = migration
        self._writer_lock = threading.Lock()
        self._write_owner_thread: int | None = None
        self._lock_timeout_seconds = 5.0
        _require_regular_file(migration, reason="SQLITE_MIGRATION")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        _reject_unsafe_database_entry(path)
        _reject_unsafe_sidecars(path)
        existed = path.exists()
        self.connection = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        try:
            self._configure()
            if not existed:
                self.connection.executescript(migration.read_text(encoding="utf-8"))
            self._apply_migrations(migration)
            os.chmod(path, 0o600)
            self.status = self._verify()
            if restored:
                self.mark_restored()
        except BaseException:
            self.connection.close()
            raise

    def _apply_migrations(self, baseline: Path) -> None:
        """Apply immutable sibling migrations and reject checksum drift.

        Older databases predate ``schema_migrations``.  The caller-provided
        baseline is their already-applied schema and is registered without
        executing it again; later numbered files are then applied in order.
        """

        user_table_count = int(
            self.connection.execute(
                """SELECT count(*) FROM sqlite_schema
                   WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name!='schema_migrations'"""
            ).fetchone()[0]
        )
        if user_table_count == 0:
            self.connection.executescript(baseline.read_text(encoding="utf-8"))
        self.connection.execute(
            """CREATE TABLE IF NOT EXISTS schema_migrations (
                   migration_name TEXT PRIMARY KEY,
                   sha256 TEXT NOT NULL,
                   applied_at TEXT NOT NULL
               ) STRICT"""
        )
        candidates = sorted(baseline.parent.glob("[0-9][0-9][0-9]_*.sql"))
        if baseline not in candidates:
            candidates.insert(0, baseline)
        baseline_digest = hashlib.sha256(baseline.read_bytes()).hexdigest()
        registered = self.connection.execute(
            "SELECT sha256 FROM schema_migrations WHERE migration_name=?",
            (baseline.name,),
        ).fetchone()
        if registered is None:
            self.connection.execute(
                "INSERT INTO schema_migrations VALUES (?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
                (baseline.name, baseline_digest),
            )
        elif str(registered[0]) != baseline_digest:
            raise LedgerUnavailable(f"MIGRATION_CHECKSUM_MISMATCH:{baseline.name}")
        for migration in candidates:
            digest = hashlib.sha256(migration.read_bytes()).hexdigest()
            row = self.connection.execute(
                "SELECT sha256 FROM schema_migrations WHERE migration_name=?",
                (migration.name,),
            ).fetchone()
            if row is not None:
                if str(row[0]) != digest:
                    raise LedgerUnavailable(f"MIGRATION_CHECKSUM_MISMATCH:{migration.name}")
                continue
            name_literal = migration.name.replace("'", "''")
            digest_literal = digest.replace("'", "''")
            script = (
                "BEGIN IMMEDIATE;\n"
                + migration.read_text(encoding="utf-8")
                + "\nINSERT INTO schema_migrations VALUES ('"
                + name_literal
                + "','"
                + digest_literal
                + "',strftime('%Y-%m-%dT%H:%M:%fZ','now'));\nCOMMIT;"
            )
            try:
                self.connection.executescript(script)
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
                raise
        expected = {migration.name for migration in candidates}
        recorded = {str(row[0]) for row in self.connection.execute("SELECT migration_name FROM schema_migrations ORDER BY migration_name")}
        if recorded != expected:
            raise LedgerUnavailable("MIGRATION_SET_MISMATCH")

    def _configure(self) -> None:
        mode = str(self.connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]).lower()
        if mode != "wal":
            raise LedgerUnavailable(f"journal_mode is {mode}, not wal")
        self.connection.execute("PRAGMA synchronous = FULL")
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.execute("PRAGMA wal_autocheckpoint = 1000")
        self.connection.execute("PRAGMA trusted_schema = OFF")

    def _verify(self) -> SQLiteRuntimeStatus:
        foreign_key_violation = self.connection.execute("PRAGMA foreign_key_check").fetchone()
        values = {
            "journal_mode": str(self.connection.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
            "synchronous": int(self.connection.execute("PRAGMA synchronous").fetchone()[0]),
            "foreign_keys": int(self.connection.execute("PRAGMA foreign_keys").fetchone()[0]),
            "busy_timeout": int(self.connection.execute("PRAGMA busy_timeout").fetchone()[0]),
            "wal_autocheckpoint": int(self.connection.execute("PRAGMA wal_autocheckpoint").fetchone()[0]),
            "trusted_schema": int(self.connection.execute("PRAGMA trusted_schema").fetchone()[0]),
            "quick_check": str(self.connection.execute("PRAGMA quick_check").fetchone()[0]),
            "foreign_key_check": "ok" if foreign_key_violation is None else "violation",
        }
        if values != {
            "journal_mode": "wal",
            "synchronous": 2,
            "foreign_keys": 1,
            "busy_timeout": 5000,
            "wal_autocheckpoint": 1000,
            "trusted_schema": 0,
            "quick_check": "ok",
            "foreign_key_check": "ok",
        }:
            raise LedgerUnavailable(f"SQLite readiness failed: {values}")
        return SQLiteRuntimeStatus(
            version=sqlite3.sqlite_version,
            production_gate="PASS" if production_sqlite_gate() else "FAIL",
            journal_mode=str(values["journal_mode"]),
            synchronous=cast(int, values["synchronous"]),
            foreign_keys=cast(int, values["foreign_keys"]),
            busy_timeout=cast(int, values["busy_timeout"]),
            wal_autocheckpoint=cast(int, values["wal_autocheckpoint"]),
            trusted_schema=cast(int, values["trusted_schema"]),
            quick_check=str(values["quick_check"]),
            foreign_key_check=str(values["foreign_key_check"]),
        )

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        caller = threading.get_ident()
        if self._write_owner_thread == caller:
            raise LedgerUnavailable("SQLITE_NESTED_WRITE_FORBIDDEN")
        acquired = self._writer_lock.acquire(timeout=self._lock_timeout_seconds)
        if not acquired:
            raise LedgerUnavailable("SQLITE_WRITER_LOCK_TIMEOUT")
        self._write_owner_thread = caller
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            yield self.connection
            self.connection.execute("COMMIT")
        except BaseException as error:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            if isinstance(error, sqlite3.Error):
                raise map_sqlite_failure(error) from error
            raise
        finally:
            self._write_owner_thread = None
            self._writer_lock.release()

    def _role_connection(self, *, readonly: bool) -> sqlite3.Connection:
        target = f"{self.path.absolute().as_uri()}?mode=ro" if readonly else str(self.path)
        connection = sqlite3.connect(
            target,
            isolation_level=None,
            uri=readonly,
            timeout=self._lock_timeout_seconds,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA trusted_schema = OFF")
        if readonly:
            connection.execute("PRAGMA query_only = ON")
        return connection

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        """Own a consistent read snapshot on the calling thread."""

        connection = self._role_connection(readonly=True)
        try:
            connection.execute("BEGIN")
            yield connection
        except sqlite3.Error as error:
            raise map_sqlite_failure(error) from error
        finally:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            connection.close()

    def read_one(self, sql: str, parameters: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        with self.read() as connection:
            return cast(sqlite3.Row | None, connection.execute(sql, parameters).fetchone())

    def read_all(self, sql: str, parameters: tuple[Any, ...] = ()) -> tuple[sqlite3.Row, ...]:
        with self.read() as connection:
            return tuple(cast(list[sqlite3.Row], connection.execute(sql, parameters).fetchall()))

    def checkpoint(self, mode: str = "PASSIVE") -> tuple[int, int, int]:
        """Run a checkpoint from a connection that is never used for reads or writes."""

        normalized = mode.upper()
        if normalized not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise ValueError(f"unsupported checkpoint mode: {mode}")
        connection = self._role_connection(readonly=False)
        try:
            try:
                row = connection.execute(f"PRAGMA wal_checkpoint({normalized})").fetchone()
            except sqlite3.Error as error:
                raise map_sqlite_failure(error) from error
            if row is None:
                raise LedgerUnavailable("SQLITE_CHECKPOINT_NO_RESULT")
            return int(row[0]), int(row[1]), int(row[2])
        finally:
            connection.close()

    def writable_probe(self) -> bool:
        try:
            with self.write():
                pass
        except sqlite3.Error:
            return False
        return True

    def integrity_check(self) -> str:
        row = self.read_one("PRAGMA integrity_check")
        if row is None:
            raise LedgerUnavailable("SQLITE_INTEGRITY_CHECK_NO_RESULT")
        return str(row[0])

    def backup_to(self, destination: Path) -> str:
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if os.path.lexists(destination):
            raise LedgerUnavailable("SQLITE_BACKUP_DESTINATION_EXISTS")
        try:
            descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except FileExistsError as error:
            raise LedgerUnavailable("SQLITE_BACKUP_DESTINATION_EXISTS") from error
        os.close(descriptor)
        source: sqlite3.Connection | None = None
        try:
            source = self._role_connection(readonly=True)
            backup = sqlite3.connect(destination, timeout=self._lock_timeout_seconds)
            try:
                source.backup(backup)
            finally:
                backup.close()
            os.chmod(destination, 0o600)
            return hashlib.sha256(destination.read_bytes()).hexdigest()
        except BaseException:
            destination.unlink(missing_ok=True)
            raise
        finally:
            if source is not None:
                source.close()

    def mark_restored(self) -> None:
        """Only CentralLedger supports this; subclasses may override."""

    def close(self) -> None:
        if self._write_owner_thread == threading.get_ident():
            raise LedgerUnavailable("SQLITE_CLOSE_DURING_WRITE_FORBIDDEN")
        acquired = self._writer_lock.acquire(timeout=self._lock_timeout_seconds)
        if not acquired:
            raise LedgerUnavailable("SQLITE_WRITER_LOCK_TIMEOUT")
        try:
            self.connection.close()
        finally:
            self._writer_lock.release()


def map_sqlite_failure(error: sqlite3.Error) -> LedgerUnavailable:
    code = getattr(error, "sqlite_errorname", "SQLITE_ERROR")
    return LedgerUnavailable(f"{code}: {error}")
