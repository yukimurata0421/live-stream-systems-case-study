from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

from .postgres import PostgresMonitoringRepository
from .repository import MonitoringRepository


Repository = MonitoringRepository | PostgresMonitoringRepository


def wait_for_integrity(
    repository: Repository,
    *,
    timeout_sec: int,
    retry_interval_sec: float = 1.0,
) -> None:
    """Bound startup DB unavailability without turning it into a restart loop."""

    deadline = time.monotonic() + max(0, int(timeout_sec))
    while True:
        if repository.integrity_check() == "ok":
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                f"Monitoring v4 database did not become ready within {max(0, int(timeout_sec))}s"
            )
        time.sleep(min(max(0.05, float(retry_interval_sec)), remaining))


def add_repository_arguments(parser: argparse.ArgumentParser) -> None:
    database = parser.add_mutually_exclusive_group(required=True)
    database.add_argument("--db", type=Path, help="explicit isolated SQLite path")
    database.add_argument(
        "--postgres",
        action="store_true",
        help="use libpq PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD environment",
    )


def repository_from_args(args: Any, *, application_name: str) -> Repository:
    if bool(getattr(args, "postgres", False)):
        return PostgresMonitoringRepository(application_name=application_name)
    path = getattr(args, "db", None)
    if path is None:
        raise ValueError("SQLite backend requires --db")
    return MonitoringRepository(Path(path))
