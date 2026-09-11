from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from stream_contracts.monitoring_v4.time import parse_utc

from .component_health import ComponentHealthRepositoryMixin
from .current_store import CurrentRepositoryMixin
from .incident_store import IncidentRepositoryMixin
from .metrics_read import MonitoringMetricsRepositoryMixin
from .observation_store import ObservationRepositoryMixin
from .outbox import OutboxRepositoryMixin
from .projections import ProjectionRepositoryMixin
from .publications import ArtifactPublicationRepositoryMixin
from .schema import SCHEMA_VERSION, configure, migrate
from .sqlite_maintenance import SQLiteMaintenanceRepositoryMixin


class RepositoryComposition(
    ObservationRepositoryMixin,
    CurrentRepositoryMixin,
    IncidentRepositoryMixin,
    ProjectionRepositoryMixin,
    OutboxRepositoryMixin,
    ArtifactPublicationRepositoryMixin,
    MonitoringMetricsRepositoryMixin,
    ComponentHealthRepositoryMixin,
):
    """Backend-neutral domain stores; connection ownership stays in subclasses."""


class MonitoringRepository(
    RepositoryComposition,
    SQLiteMaintenanceRepositoryMixin,
):
    """SQLite composition root; consumers depend on role-specific ports."""

    def __init__(self, path: Path, *, busy_timeout_ms: int = 5000) -> None:
        self.path = Path(path)
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))

    def connect(self, *, read_only: bool = False) -> sqlite3.Connection:
        if read_only:
            uri = f"{self.path.resolve().as_uri()}?mode=ro"
            connection = sqlite3.connect(
                uri,
                uri=True,
                timeout=self.busy_timeout_ms / 1000,
                isolation_level=None,
            )
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(
                self.path,
                timeout=self.busy_timeout_ms / 1000,
                isolation_level=None,
            )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        if not read_only:
            configure(connection, busy_timeout_ms=self.busy_timeout_ms)
        return connection

    @contextmanager
    def connection(
        self,
        *,
        read_only: bool = False,
    ) -> Iterator[sqlite3.Connection]:
        connection = self.connect(read_only=read_only)
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self, *, applied_at: str) -> None:
        parse_utc(applied_at, field="applied_at")
        with self.connection() as connection:
            migrate(connection, applied_at=applied_at)
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version != SCHEMA_VERSION:
                raise RuntimeError(
                    f"schema migration stopped at {version}, expected {SCHEMA_VERSION}"
                )

    @contextmanager
    def transaction(
        self,
        *,
        immediate: bool = True,
    ) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
