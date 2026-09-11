from __future__ import annotations

import hashlib
import re
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from stream_contracts.monitoring_v4.time import parse_utc

from .postgres_schema import SCHEMA_VERSION, migrate
from .repository import RepositoryComposition


_INSERT_OR_IGNORE = re.compile(r"^(\s*)INSERT\s+OR\s+IGNORE\s+INTO\b", re.IGNORECASE)


def postgres_advisory_lock_key(namespace: str) -> int:
    digest = hashlib.sha256(
        f"stream-monitoring-v4-cycle:{namespace}".encode("utf-8")
    ).digest()[:8]
    return int.from_bytes(digest, byteorder="big", signed=True)


def postgres_sql(sql: str) -> str:
    """Translate the deliberately small SQLite-compatible repository SQL subset."""

    translated, replacements = _INSERT_OR_IGNORE.subn(r"\1INSERT INTO", sql, count=1)
    translated = translated.replace("?", "%s")
    if replacements:
        stripped = translated.rstrip()
        suffix = ";" if stripped.endswith(";") else ""
        if suffix:
            stripped = stripped[:-1].rstrip()
        translated = f"{stripped} ON CONFLICT DO NOTHING{suffix}"
    return translated


class PostgresConnection:
    def __init__(self, raw: Any, *, release: Any | None = None) -> None:
        self.raw = raw
        self._release = release
        self._closed = False

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> Any:
        return self.raw.execute(postgres_sql(sql), params)

    def commit(self) -> None:
        self.raw.commit()

    def rollback(self) -> None:
        self.raw.rollback()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._release is None:
            self.raw.close()
        else:
            self._release(self.raw)

    def discard(self) -> None:
        """Close the physical session before returning it to the pool."""

        if self._closed:
            return
        try:
            self.raw.close()
        finally:
            self.close()


class PostgresMonitoringRepository(RepositoryComposition):
    """PostgreSQL composition root with no inherited SQLite maintenance behavior."""

    backend = "postgresql"
    path: Path | None = None

    def __init__(
        self,
        conninfo: str = "",
        *,
        connect_timeout_sec: int = 5,
        application_name: str = "stream-monitoring-v4",
        pool_min_size: int = 1,
        pool_max_size: int = 4,
        statement_timeout_ms: int = 30_000,
        lock_timeout_ms: int = 5_000,
    ) -> None:
        self.conninfo = conninfo
        self.connect_timeout_sec = max(1, int(connect_timeout_sec))
        self.application_name = application_name[:63]
        self.pool_min_size = max(0, int(pool_min_size))
        self.pool_max_size = max(self.pool_min_size or 1, int(pool_max_size))
        self.statement_timeout_ms = max(1_000, min(int(statement_timeout_ms), 900_000))
        self.lock_timeout_ms = max(100, min(int(lock_timeout_ms), 300_000))
        self._pool: Any | None = None
        self._pool_lock = threading.Lock()

    @staticmethod
    def _driver() -> tuple[Any, Any]:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - exercised in the container integration gate
            raise RuntimeError(
                "PostgreSQL backend requires the optional psycopg[binary] dependency"
            ) from exc
        return psycopg, dict_row

    def _raw_connect(self) -> Any:
        psycopg, dict_row = self._driver()
        return psycopg.connect(
            self.conninfo,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=self.connect_timeout_sec,
            application_name=self.application_name,
        )

    def _connection_pool(self) -> Any:
        if self._pool is not None:
            return self._pool
        with self._pool_lock:
            if self._pool is not None:
                return self._pool
            psycopg, dict_row = self._driver()
            del psycopg
            try:
                from psycopg_pool import ConnectionPool
            except ImportError as exc:  # pragma: no cover - container integration path
                raise RuntimeError(
                    "PostgreSQL backend requires the optional psycopg_pool dependency"
                ) from exc
            self._pool = ConnectionPool(
                conninfo=self.conninfo,
                min_size=self.pool_min_size,
                max_size=self.pool_max_size,
                open=True,
                kwargs={
                    "autocommit": True,
                    "row_factory": dict_row,
                    "connect_timeout": self.connect_timeout_sec,
                    "application_name": self.application_name,
                },
                name=self.application_name,
            )
            return self._pool

    def connect(self, *, read_only: bool = False) -> PostgresConnection:
        pool = self._connection_pool()
        raw = pool.getconn(timeout=self.connect_timeout_sec)
        try:
            raw.execute(
                "SELECT set_config('statement_timeout', %s, false), "
                "set_config('lock_timeout', %s, false)",
                (
                    f"{self.statement_timeout_ms}ms",
                    f"{self.lock_timeout_ms}ms",
                ),
            )
            raw.execute(
                "SET default_transaction_read_only = on"
                if read_only
                else "SET default_transaction_read_only = off"
            )
        except BaseException:
            # A session that failed while applying its mandatory safety
            # settings must never be handed to another role/caller as a normal
            # pooled connection. Returning a physically closed connection lets
            # psycopg_pool discard and replace it deterministically.
            try:
                raw.close()
            finally:
                pool.putconn(raw)
            raise
        return PostgresConnection(raw, release=pool.putconn)

    @contextmanager
    def connection(self, *, read_only: bool = False) -> Iterator[PostgresConnection]:
        connection = self.connect(read_only=read_only)
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self, *, applied_at: str) -> None:
        parse_utc(applied_at, field="applied_at")
        connection = self.connect()
        try:
            with connection.raw.transaction():
                migrate(connection, applied_at=applied_at)
        finally:
            connection.close()
        self.ensure_schema()

    def ensure_schema(self, *, connection: PostgresConnection | None = None) -> None:
        owned = connection is None
        if owned:
            connection = self.connect(read_only=True)
        assert connection is not None
        try:
            rows = connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall()
        finally:
            if owned:
                connection.close()
        versions = [int(row["version"]) for row in rows]
        expected = list(range(1, SCHEMA_VERSION + 1))
        if versions != expected:
            raise RuntimeError(f"PostgreSQL schema migrations are {versions}, expected {expected}")

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[PostgresConnection]:
        del immediate
        connection = self.connect()
        try:
            with connection.raw.transaction():
                yield connection
        finally:
            connection.close()

    @contextmanager
    def cycle_guard(self, name: str) -> Iterator[bool]:
        """Hold a PostgreSQL session lock for the complete live writer cycle."""

        connection = self.connect()
        acquired = False
        discard = False
        lock_key = postgres_advisory_lock_key(name)
        try:
            row = connection.execute(
                "SELECT pg_try_advisory_lock(?) AS acquired",
                (lock_key,),
            ).fetchone()
            acquired = bool(row and row["acquired"])
            yield acquired
        finally:
            if acquired:
                try:
                    row = connection.execute(
                        "SELECT pg_advisory_unlock(?) AS released",
                        (lock_key,),
                    ).fetchone()
                    if not row or row["released"] is not True:
                        discard = True
                except Exception:
                    # Closing the session releases the lock even if the explicit
                    # unlock cannot be confirmed after a connection failure.
                    discard = True
            if discard:
                connection.discard()
            else:
                connection.close()

    def integrity_check(self) -> str:
        try:
            with self.connection(read_only=True) as connection:
                self.ensure_schema(connection=connection)
                row = connection.execute("SELECT 1 AS healthy").fetchone()
            return "ok" if row and int(row["healthy"]) == 1 else "failed"
        except Exception:
            return "failed"

    def ping(self) -> bool:
        try:
            with self.connection(read_only=True) as connection:
                row = connection.execute("SELECT 1 AS healthy").fetchone()
            return bool(row and int(row["healthy"]) == 1)
        except Exception:
            return False

    def close(self) -> None:
        with self._pool_lock:
            pool = self._pool
            self._pool = None
        if pool is not None:
            pool.close()

    def backup(self, target: Path) -> None:
        del target
        raise NotImplementedError("PostgreSQL backups use pg_dump from the scoped backup workload")

    @classmethod
    def restore(
        cls,
        backup: Path,
        target: Path,
        *,
        applied_at: str,
    ) -> "PostgresMonitoringRepository":
        del cls, backup, target, applied_at
        raise NotImplementedError(
            "PostgreSQL restore is an explicit pg_restore maintenance workflow"
        )

    def copy_database_files_for_forensics(self, target_dir: Path) -> list[Path]:
        del target_dir
        raise NotImplementedError("PostgreSQL physical files are owned by the StatefulSet PVC")
