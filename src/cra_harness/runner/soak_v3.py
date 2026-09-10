from __future__ import annotations

import math
import resource
import sqlite3
import statistics
import time
import tracemalloc
from pathlib import Path
from typing import Any


def _percentiles(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    return {
        "sample_count": len(ordered),
        "median": None if not ordered else round(statistics.median(ordered), 6),
        "p95": None if not ordered else ordered[math.ceil(0.95 * len(ordered)) - 1],
        "p99": None if not ordered else ordered[math.ceil(0.99 * len(ordered)) - 1],
        "maximum": None if not ordered else ordered[-1],
        "unit": "ms",
    }


def _fd_count() -> int:
    return len(list(Path("/proc/self/fd").iterdir()))


def run_simulated_soak(workspace: Path, *, cycles: int = 30_000, step_seconds: int = 30) -> dict[str, Any]:
    workspace.mkdir(parents=True, exist_ok=True)
    database = workspace / "soak.sqlite3"
    fd_before = _fd_count()
    rss_before_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    tracemalloc.start()
    started = time.perf_counter_ns()
    connection = sqlite3.connect(database, isolation_level=None)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA wal_autocheckpoint=1000")
    connection.execute(
        "CREATE TABLE lifecycle_events (seq INTEGER PRIMARY KEY, simulated_second INTEGER NOT NULL, event_type TEXT NOT NULL) STRICT"
    )
    connection.execute(
        "CREATE TABLE current_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), simulated_second INTEGER NOT NULL, "
        "credential_state TEXT NOT NULL, authority_state TEXT NOT NULL, snapshot_seq INTEGER NOT NULL, "
        "reconciliation_count INTEGER NOT NULL) "
        "STRICT"
    )
    connection.execute("INSERT INTO current_state VALUES (1,0,'VALID','CENTRAL_ACTIVE',0,0)")
    checkpoint_ms: list[float] = []
    transaction_ms: list[float] = []
    wal_sizes: list[int] = []
    rotation_count = 0
    reconciliation_count = 0
    transaction_failures = 0
    for cycle in range(1, cycles + 1):
        simulated_second = cycle * step_seconds
        credential_state = "VALID"
        event = "SNAPSHOT"
        if simulated_second % 86_400 == 0:
            credential_state = "ROTATING"
            rotation_count += 1
            event = "CREDENTIAL_ROTATION"
        if simulated_second % 21_600 == 0:
            reconciliation_count += 1
            event = "RECONCILIATION"
        transaction_started = time.perf_counter_ns()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("INSERT INTO lifecycle_events VALUES (?,?,?)", (cycle, simulated_second, event))
            connection.execute(
                "UPDATE current_state SET simulated_second=?,credential_state=?,snapshot_seq=?,reconciliation_count=? WHERE singleton=1",
                (simulated_second, credential_state, cycle, reconciliation_count),
            )
            connection.execute("COMMIT")
        except sqlite3.Error:
            transaction_failures += 1
            if connection.in_transaction:
                connection.execute("ROLLBACK")
        transaction_ms.append(round((time.perf_counter_ns() - transaction_started) / 1_000_000, 6))
        if cycle % 1_000 == 0:
            wal_path = database.with_name(f"{database.name}-wal")
            wal_sizes.append(wal_path.stat().st_size if wal_path.exists() else 0)
            checkpoint_started = time.perf_counter_ns()
            connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            checkpoint_ms.append(round((time.perf_counter_ns() - checkpoint_started) / 1_000_000, 6))
    integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    rows = int(connection.execute("SELECT count(*) FROM lifecycle_events").fetchone()[0])
    final_state = tuple(connection.execute("SELECT * FROM current_state").fetchone())
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    connection.close()
    current_memory, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    duration_ms = round((time.perf_counter_ns() - started) / 1_000_000, 6)
    fd_after = _fd_count()
    rss_after_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    simulated_seconds = cycles * step_seconds
    database_bytes = database.stat().st_size
    return {
        "profile": "SOAK_SIMULATED",
        "cycles": cycles,
        "step_seconds": step_seconds,
        "simulated_seconds": simulated_seconds,
        "simulated_days": round(simulated_seconds / 86_400, 6),
        "wall_duration_ms": duration_ms,
        "accelerated_lifecycle": {
            "snapshot_count": cycles,
            "credential_rotation_count": rotation_count,
            "reconciliation_count": reconciliation_count,
            "timer_drift_ms": 0,
        },
        "sqlite": {
            "runtime_version": sqlite3.sqlite_version,
            "journal_mode": "wal",
            "integrity_check": integrity,
            "event_rows": rows,
            "transaction_failures": transaction_failures,
            "transaction_latency": _percentiles(transaction_ms),
            "checkpoint_latency": _percentiles(checkpoint_ms),
            "wal_sample_count": len(wal_sizes),
            "wal_max_bytes_before_checkpoint": max(wal_sizes, default=0),
            "database_bytes_after_truncate": database_bytes,
            "projected_database_bytes_per_simulated_day": round(database_bytes / max(simulated_seconds / 86_400, 1), 3),
        },
        "process": {
            "fd_before": fd_before,
            "fd_after": fd_after,
            "fd_growth": fd_after - fd_before,
            "rss_high_water_before_kib": rss_before_kib,
            "rss_high_water_after_kib": rss_after_kib,
            "tracemalloc_current_bytes": current_memory,
            "tracemalloc_peak_bytes": peak_memory,
        },
        "final_state": list(final_state),
        "physical_attempt_count": 0,
        "production_mutation_count": 0,
        "result": (
            "PASS"
            if integrity == "ok" and rows == cycles and transaction_failures == 0 and fd_after == fd_before and final_state[4] == cycles
            else "HARNESS_FAILURE"
        ),
        "interpretation": ("accelerated local lifecycle evidence; it does not replace a wall-clock soak across real credential expiration"),
    }
