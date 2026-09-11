from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import os
import sqlite3
import stat
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable

from stream_contracts.monitoring_v4.ids import canonical_json
from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.runtime.atomic_file import atomic_write_text
from stream_monitoring_v4.storage.postgres import PostgresMonitoringRepository
from stream_monitoring_v4.storage.schema import SCHEMA_VERSION as SQLITE_SCHEMA_VERSION


IMMUTABLE_TABLES = (
    "observations",
    "rejections",
    "current_snapshots",
    "incident_transitions",
    "incident_processed_currents",
    "notification_delivery_epochs",
    "notification_intents",
    "notification_intent_delivery",
    "delivery_attempts",
    "delivery_results",
    "sli_projections",
    "shadow_cycles",
)

MUTABLE_POLICIES = {
    "domain_current": ("domain", "reduced_ts"),
    "incident_candidates": ("domain", "last_seen_ts"),
    "incident_episodes": ("episode_id", "last_transition_ts"),
    "component_health": ("component", "checked_ts"),
    "delivery_attempt_states": ("attempt_id", "completed_ts"),
    "public_artifact_publications": ("publication_id", "last_attempt_ts"),
}

TABLE_ORDER = (
    "observations",
    "rejections",
    "current_snapshots",
    "domain_current",
    "incident_candidates",
    "incident_episodes",
    "incident_transitions",
    "incident_processed_currents",
    "notification_delivery_epochs",
    "notification_intents",
    "notification_intent_delivery",
    "delivery_attempts",
    "delivery_results",
    "delivery_attempt_states",
    "component_health",
    "sli_projections",
    "shadow_cycles",
    "public_artifact_publications",
)

IMPORT_BATCH_SIZE = 1_000

PRIMARY_KEYS = {
    "observations": "observation_id",
    "rejections": "rejection_id",
    "current_snapshots": "snapshot_id",
    "incident_transitions": "transition_id",
    "incident_processed_currents": "snapshot_id",
    "notification_delivery_epochs": "epoch_id",
    "notification_intents": "intent_id",
    "notification_intent_delivery": "intent_id",
    "delivery_attempts": "attempt_id",
    "delivery_results": "result_id",
    "sli_projections": "projection_id",
    "shadow_cycles": "cycle_id",
}

TABLE_KEYS = {
    **PRIMARY_KEYS,
    "domain_current": "domain",
    "incident_candidates": "domain",
    "incident_episodes": "episode_id",
    "delivery_attempt_states": "attempt_id",
    "component_health": "component",
    "public_artifact_publications": "publication_id",
}

TEMPORAL_IDENTITY_POLICIES = {
    "observations": ("received_at", "received_ts"),
    "current_snapshots": ("reduced_at", "reduced_ts"),
    "incident_processed_currents": ("processed_at", "processed_ts"),
    "sli_projections": ("evaluated_at", "evaluated_ts"),
}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Merge a consistent Monitoring v4 SQLite snapshot into PostgreSQL")
    result.add_argument("--sqlite", type=Path, required=True)
    result.add_argument("--output", type=Path, default=None)
    return result


def _snapshot(source: Path, target: Path) -> None:
    sidecars = (Path(f"{source}-wal"), Path(f"{source}-shm"))
    if any(path.exists() for path in sidecars):
        raise RuntimeError(
            "SQLite import source must be a prepared DELETE-journal snapshot without sidecars"
        )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(source, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeError("SQLite import source must be a regular file")
        uri = f"{Path(f'/proc/self/fd/{descriptor}').as_uri()}?mode=ro&immutable=1"
        with closing(sqlite3.connect(uri, uri=True)) as input_db:
            journal_mode = str(input_db.execute("PRAGMA journal_mode").fetchone()[0])
            if journal_mode.lower() != "delete":
                raise RuntimeError(
                    "SQLite import source must use DELETE journal mode; "
                    "run monitoring_v4_prepare_sqlite_import.py first"
                )
            with closing(sqlite3.connect(target)) as output_db, output_db:
                input_db.backup(output_db)
        after_path = os.stat(source, follow_symlinks=False)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        )
        identity_after = (
            after_path.st_dev,
            after_path.st_ino,
            after_path.st_size,
            after_path.st_mtime_ns,
        )
        if identity_after != identity_before or any(path.exists() for path in sidecars):
            raise RuntimeError("SQLite import source changed while it was being copied")
    finally:
        os.close(descriptor)
    with closing(sqlite3.connect(target)) as check:
        result = str(check.execute("PRAGMA integrity_check").fetchone()[0])
    if result != "ok":
        raise RuntimeError(f"SQLite snapshot integrity check failed: {result}")


def _columns(connection: sqlite3.Connection, table: str) -> tuple[str, ...]:
    return tuple(str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")'))


def _iter_rows(
    connection: sqlite3.Connection,
    table: str,
    columns: tuple[str, ...],
    *,
    batch_size: int = IMPORT_BATCH_SIZE,
) -> Iterable[tuple[Any, ...]]:
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    names = ",".join(f'"{name}"' for name in columns)
    key = TABLE_KEYS[table]
    cursor = connection.execute(
        f'SELECT {names} FROM "{table}" ORDER BY "{key}"'
    )
    while rows := cursor.fetchmany(batch_size):
        yield from (tuple(row) for row in rows)


def _digest(columns: tuple[str, ...], rows: Iterable[tuple[Any, ...]]) -> str:
    hasher = hashlib.sha256()
    hasher.update(canonical_json(columns).encode("utf-8"))
    for row in rows:
        hasher.update(b"\n")
        hasher.update(canonical_json(row).encode("utf-8"))
    return hasher.hexdigest()


def _immutable_insert(table: str, columns: tuple[str, ...]) -> str:
    names = ",".join(f'"{name}"' for name in columns)
    placeholders = ",".join("?" for _ in columns)
    return f'INSERT INTO "{table}" ({names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'


def _earliest_temporal_insert(
    table: str,
    columns: tuple[str, ...],
    *,
    clock_text: str,
    clock_ts: str,
) -> str:
    names = ",".join(f'"{name}"' for name in columns)
    placeholders = ",".join("?" for _ in columns)
    semantic_columns = tuple(
        name
        for name in columns
        if name not in {PRIMARY_KEYS[table], clock_text, clock_ts}
    )
    semantic_match = " AND ".join(
        f'"{table}"."{name}" IS NOT DISTINCT FROM excluded."{name}"'
        for name in semantic_columns
    )
    return (
        f'INSERT INTO "{table}" ({names}) VALUES ({placeholders}) '
        f'ON CONFLICT("{PRIMARY_KEYS[table]}") DO UPDATE SET '
        f'"{clock_text}"=CASE WHEN excluded."{clock_ts}" < "{table}"."{clock_ts}" '
        f'THEN excluded."{clock_text}" ELSE "{table}"."{clock_text}" END,'
        f'"{clock_ts}"=LEAST(excluded."{clock_ts}", "{table}"."{clock_ts}") '
        f'WHERE ({semantic_match}) '
        f'AND excluded."{clock_ts}" < "{table}"."{clock_ts}"'
    )


def _observation_insert(columns: tuple[str, ...]) -> str:
    return _earliest_temporal_insert(
        "observations",
        columns,
        clock_text="received_at",
        clock_ts="received_ts",
    )


def _mutable_insert(table: str, columns: tuple[str, ...], key: str, clock: str) -> str:
    names = ",".join(f'"{name}"' for name in columns)
    placeholders = ",".join("?" for _ in columns)
    updates = ",".join(
        f'"{name}"=excluded."{name}"' for name in columns if name != key
    )
    return (
        f'INSERT INTO "{table}" ({names}) VALUES ({placeholders}) '
        f'ON CONFLICT("{key}") DO UPDATE SET {updates} '
        f'WHERE excluded."{clock}" > "{table}"."{clock}"'
    )


def _active_episodes_sqlite(connection: sqlite3.Connection) -> dict[str, str]:
    return {
        str(row[0]): str(row[1])
        for row in connection.execute(
            "SELECT domain, episode_id FROM incident_episodes WHERE status='active'"
        )
    }


def _active_episodes_postgres(repository: PostgresMonitoringRepository) -> dict[str, str]:
    with repository.connection(read_only=True) as connection:
        rows = connection.execute(
            "SELECT domain, episode_id FROM incident_episodes WHERE status='active'"
        ).fetchall()
    return {str(row["domain"]): str(row["episode_id"]) for row in rows}


def _verify_immutable(
    target: Any,
    source: sqlite3.Connection,
    metadata: dict[str, dict[str, Any]],
) -> dict[str, int]:
    accepted_temporal_differences = {table: 0 for table in TEMPORAL_IDENTITY_POLICIES}
    for table in IMMUTABLE_TABLES:
        columns = tuple(metadata[table]["columns"])
        key = PRIMARY_KEYS[table]
        key_index = columns.index(key)
        names = ",".join(f'"{name}"' for name in columns)
        for row in _iter_rows(source, table, columns):
            target_row = target.execute(
                f'SELECT {names} FROM "{table}" WHERE "{key}"=?',
                (row[key_index],),
            ).fetchone()
            if target_row is None:
                raise RuntimeError(f"PostgreSQL import verification missing {table}.{row[key_index]}")
            target_values = tuple(target_row[name] for name in columns)
            if target_values == row:
                continue
            if table in TEMPORAL_IDENTITY_POLICIES:
                clock_text, clock_ts = TEMPORAL_IDENTITY_POLICIES[table]
                differences = {
                    name
                    for name, source_value, target_value in zip(columns, row, target_values)
                    if source_value != target_value
                }
                if differences <= {clock_text, clock_ts}:
                    source_clock_ts = int(row[columns.index(clock_ts)])
                    target_clock_ts = int(target_values[columns.index(clock_ts)])
                    if target_clock_ts <= source_clock_ts:
                        accepted_temporal_differences[table] += 1
                        continue
            raise RuntimeError(f"PostgreSQL import verification mismatch in {table}.{row[key_index]}")
    return accepted_temporal_differences


def _write_atomic(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(
        path,
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n",
        mode=0o600,
    )


def _run_import(
    args: argparse.Namespace,
    source_path: Path,
    repository: PostgresMonitoringRepository,
) -> int:
    repository.ensure_schema()
    with tempfile.TemporaryDirectory(prefix="stream-v4-sqlite-import-") as td:
        snapshot = Path(td) / "source.sqlite3"
        _snapshot(source_path, snapshot)
        source = sqlite3.connect(
            f"{snapshot.resolve().as_uri()}?mode=ro&immutable=1",
            uri=True,
        )
        source.row_factory = sqlite3.Row
        try:
            source_schema_version = int(
                source.execute("PRAGMA user_version").fetchone()[0]
            )
            if source_schema_version != SQLITE_SCHEMA_VERSION:
                raise RuntimeError(
                    "SQLite import source schema is "
                    f"{source_schema_version}, expected {SQLITE_SCHEMA_VERSION}"
                )
            sqlite_active = _active_episodes_sqlite(source)
            postgres_active = _active_episodes_postgres(repository)
            conflicts = {
                domain: (episode, postgres_active[domain])
                for domain, episode in sqlite_active.items()
                if domain in postgres_active and postgres_active[domain] != episode
            }
            if conflicts:
                raise RuntimeError(
                    "refusing to merge distinct active incident episodes: "
                    + ",".join(sorted(conflicts))
                )
            metadata: dict[str, dict[str, Any]] = {}
            with repository.transaction() as target:
                for table in TABLE_ORDER:
                    columns = _columns(source, table)
                    if table in TEMPORAL_IDENTITY_POLICIES:
                        clock_text, clock_ts = TEMPORAL_IDENTITY_POLICIES[table]
                        statement = _earliest_temporal_insert(
                            table,
                            columns,
                            clock_text=clock_text,
                            clock_ts=clock_ts,
                        )
                    elif table in MUTABLE_POLICIES:
                        key, clock = MUTABLE_POLICIES[table]
                        statement = _mutable_insert(table, columns, key, clock)
                    else:
                        statement = _immutable_insert(table, columns)
                    hasher = hashlib.sha256()
                    hasher.update(canonical_json(columns).encode("utf-8"))
                    source_rows = 0
                    changed = 0
                    for row in _iter_rows(source, table, columns):
                        source_rows += 1
                        hasher.update(b"\n")
                        hasher.update(canonical_json(row).encode("utf-8"))
                        changed += max(0, int(target.execute(statement, row).rowcount))
                    metadata[table] = {
                        "columns": list(columns),
                        "source_rows": source_rows,
                        "source_sha256": hasher.hexdigest(),
                        "changed_rows": changed,
                    }
                target.execute(
                    """INSERT INTO sli_projection_current(
                        objective_id, assessment_scope, window_name, projection_id
                    )
                    SELECT DISTINCT ON (objective_id, assessment_scope, window_name)
                        objective_id, assessment_scope, window_name, projection_id
                    FROM sli_projections
                    ORDER BY objective_id, assessment_scope, window_name, window_end_ts DESC, projection_id DESC
                    ON CONFLICT(objective_id, assessment_scope, window_name)
                    DO UPDATE SET projection_id=excluded.projection_id"""
                )
                accepted_temporal_differences = _verify_immutable(target, source, metadata)
        finally:
            source.close()
    now_at = utc_text(int(time.time()))
    report = {
        "schema": "monitoring_v4.sqlite_to_postgres.v1",
        "completed_at": now_at,
        "source": str(source_path),
        "sqlite_snapshot_integrity": "ok",
        "tables": metadata,
        "immutable_row_verification": "ok",
        "temporal_identity_merge_policy": "earliest_auxiliary_timestamp_for_semantically_identical_event",
        "accepted_temporal_identity_differences": accepted_temporal_differences,
        "accepted_observation_receipt_differences": accepted_temporal_differences["observations"],
        "leases_imported": False,
        "notification_delivery_enabled": False,
        "runtime_mutation_enabled": False,
    }
    if args.output is not None:
        _write_atomic(args.output, report)
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    raw_source = args.sqlite.expanduser()
    if raw_source.is_symlink():
        raise ValueError("SQLite import source must not be a symlink")
    source_path = raw_source.resolve()
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    repository = PostgresMonitoringRepository(
        application_name="stream-monitoring-v4-sqlite-import"
    )
    try:
        return _run_import(args, source_path, repository)
    finally:
        repository.close()


if __name__ == "__main__":
    raise SystemExit(main())
