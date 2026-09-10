"""CRA-local query-only safety facts. Never packaged on observation hosts."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from cra_dell_recovery.time import isoformat_utc
from cra_no_action_soak.recovery_facts import ACTION_TABLES


@contextmanager
def query_only(path: Path) -> Iterator[sqlite3.Connection]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("OBSERVATION_DATABASE_PATH_INVALID")
    deadline = time.monotonic() + 2.0
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0.25)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        connection.execute("BEGIN")
        yield connection
    finally:
        connection.close()


def central_safety(path: Path, runtime: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    try:
        with query_only(path) as db:
            integrity = db.execute("PRAGMA quick_check").fetchone()[0]
            counts = {name: db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0] for name in ACTION_TABLES}
    except (OSError, ValueError, sqlite3.Error) as error:
        raise ValueError("CENTRAL_SAFETY_READ_FAILED") from error
    return {
        "observed_at": isoformat_utc(now),
        "integrity": integrity,
        "action_table_counts": counts,
        "command_delivery_enabled": runtime.get("command_delivery_enabled"),
        "control_capability_count": runtime.get("control_capability_count"),
        "runtime_readiness": runtime.get("readiness"),
        "runtime_operating_mode": runtime.get("operating_mode"),
        "runtime_observed_at": runtime.get("observed_at"),
    }
