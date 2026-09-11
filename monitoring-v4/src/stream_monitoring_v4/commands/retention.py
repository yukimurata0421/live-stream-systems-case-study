from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.runtime.backup_integrity import (
    backup_directory_device,
    matching_backup_copies,
    verified_backup_status,
    verified_restore_status,
)
from stream_monitoring_v4.storage.postgres import PostgresMonitoringRepository


DAY = 86400
ADVISORY_LOCK_KEY = 0x4D56345245544E  # "MV4RETN", below signed BIGINT max.


@dataclass(frozen=True)
class RetentionSpec:
    name: str
    retention_days: int
    sql: str


RETENTION_SPECS = (
    RetentionSpec(
        "shadow_cycles",
        45,
        """WITH doomed AS (
            SELECT ctid FROM shadow_cycles
            WHERE started_ts<? ORDER BY started_ts LIMIT ?
        )
        DELETE FROM shadow_cycles AS target USING doomed
        WHERE target.ctid=doomed.ctid""",
    ),
    RetentionSpec(
        "observations",
        120,
        """WITH doomed AS (
            SELECT ctid FROM observations
            WHERE received_ts<?
              AND NOT EXISTS (
                SELECT 1 FROM retention_protected_observations AS protected
                WHERE protected.observation_id=observations.observation_id
              )
            ORDER BY received_ts LIMIT ?
        )
        DELETE FROM observations AS target USING doomed
        WHERE target.ctid=doomed.ctid""",
    ),
    RetentionSpec(
        "rejections",
        180,
        """WITH doomed AS (
            SELECT ctid FROM rejections
            WHERE received_ts<? ORDER BY received_ts LIMIT ?
        )
        DELETE FROM rejections AS target USING doomed
        WHERE target.ctid=doomed.ctid""",
    ),
    RetentionSpec(
        "sli_projections_non_current",
        120,
        """WITH doomed AS (
            SELECT projection.ctid FROM sli_projections AS projection
            WHERE projection.evaluated_ts<?
              AND NOT EXISTS (
                SELECT 1 FROM sli_projection_current AS current_projection
                WHERE current_projection.projection_id=projection.projection_id
              )
            ORDER BY projection.evaluated_ts LIMIT ?
        )
        DELETE FROM sli_projections AS target USING doomed
        WHERE target.ctid=doomed.ctid""",
    ),
    RetentionSpec(
        "current_snapshots_unreferenced",
        180,
        """WITH doomed AS (
            SELECT snapshot.ctid FROM current_snapshots AS snapshot
            WHERE snapshot.reduced_ts<?
              AND NOT EXISTS (
                SELECT 1 FROM domain_current AS current_domain
                WHERE current_domain.snapshot_id=snapshot.snapshot_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM incident_candidates AS candidate
                WHERE candidate.snapshot_id=snapshot.snapshot_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM incident_transitions AS transition
                WHERE transition.current_snapshot_id=snapshot.snapshot_id
              )
            ORDER BY snapshot.reduced_ts LIMIT ?
        )
        DELETE FROM current_snapshots AS target USING doomed
        WHERE target.ctid=doomed.ctid""",
    ),
    RetentionSpec(
        "incident_processed_currents_orphaned",
        180,
        """WITH doomed AS (
            SELECT processed.ctid FROM incident_processed_currents AS processed
            WHERE processed.processed_ts<?
              AND NOT EXISTS (
                SELECT 1 FROM current_snapshots AS snapshot
                WHERE snapshot.snapshot_id=processed.snapshot_id
              )
            ORDER BY processed.processed_ts LIMIT ?
        )
        DELETE FROM incident_processed_currents AS target USING doomed
        WHERE target.ctid=doomed.ctid""",
    ),
)


def _prepare_protected_observations(connection: Any) -> None:
    connection.execute(
        """CREATE TEMP TABLE IF NOT EXISTS retention_protected_observations(
            observation_id TEXT PRIMARY KEY
        ) ON COMMIT PRESERVE ROWS"""
    )
    connection.execute("TRUNCATE retention_protected_observations")
    connection.execute(
        """INSERT INTO retention_protected_observations(observation_id)
        SELECT DISTINCT evidence.observation_id
        FROM current_snapshots AS snapshot
        CROSS JOIN LATERAL jsonb_array_elements_text(
            snapshot.source_observation_ids_json::jsonb
        ) AS evidence(observation_id)
        WHERE EXISTS (
            SELECT 1 FROM domain_current AS current_domain
            WHERE current_domain.snapshot_id=snapshot.snapshot_id
        ) OR EXISTS (
            SELECT 1 FROM incident_candidates AS candidate
            WHERE candidate.snapshot_id=snapshot.snapshot_id
        ) OR EXISTS (
            SELECT 1 FROM incident_transitions AS transition
            WHERE transition.current_snapshot_id=snapshot.snapshot_id
        )
        ON CONFLICT DO NOTHING"""
    )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Backup-gated, bounded PostgreSQL retention for Monitoring v4"
    )
    result.add_argument("--backup-dir", type=Path, required=True)
    result.add_argument("--independent-backup-dir", type=Path, required=True)
    result.add_argument("--restore-verification-dir", type=Path, required=True)
    result.add_argument("--backup-max-age-sec", type=int, default=3 * 3600)
    result.add_argument("--batch-size", type=int, default=5000)
    return result


def _validated_backups(args: argparse.Namespace, *, now_ts: int) -> dict[str, Any]:
    max_age_sec = int(args.backup_max_age_sec)
    if max_age_sec < 3600:
        raise ValueError("--backup-max-age-sec must be at least 3600")
    if backup_directory_device(args.backup_dir) == backup_directory_device(
        args.independent_backup_dir
    ):
        raise RuntimeError("retention requires physically distinct backup filesystems")
    primary = verified_backup_status(
        args.backup_dir,
        now_ts=now_ts,
        max_age_sec=max_age_sec,
    )
    independent = verified_backup_status(
        args.independent_backup_dir,
        now_ts=now_ts,
        max_age_sec=max_age_sec,
    )
    if not matching_backup_copies(primary, independent):
        raise RuntimeError("retention requires matching fresh verified backup copies")
    restore = verified_restore_status(
        args.restore_verification_dir,
        backup=primary,
        now_ts=now_ts,
        max_age_sec=max_age_sec,
    )
    if restore.get("verified") is not True:
        raise RuntimeError("retention requires a fresh full isolated restore verification")
    return {
        "basename": Path(str(primary["path"])).name,
        "sha256": str(primary["sha256"]),
        "primary_age_sec": int(primary["age_sec"]),
        "independent_age_sec": int(independent["age_sec"]),
        "restore_verified_at": str(restore["verified_at"]),
    }


def _run_retention(
    repository: PostgresMonitoringRepository,
    *,
    now_ts: int,
    batch_size: int,
) -> tuple[bool, dict[str, int]]:
    size = max(100, min(int(batch_size), 20_000))
    connection = repository.connect()
    acquired = False
    deleted: dict[str, int] = {spec.name: 0 for spec in RETENTION_SPECS}
    try:
        row = connection.execute(
            "SELECT pg_try_advisory_lock(?) AS acquired",
            (ADVISORY_LOCK_KEY,),
        ).fetchone()
        acquired = bool(row and row["acquired"])
        if not acquired:
            return False, deleted
        connection.execute("SET lock_timeout='2s'")
        connection.execute("SET statement_timeout='30s'")
        _prepare_protected_observations(connection)
        for spec in RETENTION_SPECS:
            cutoff_ts = int(now_ts) - spec.retention_days * DAY
            while True:
                cursor = connection.execute(spec.sql, (cutoff_ts, size))
                count = max(0, int(cursor.rowcount))
                deleted[spec.name] += count
                if count < size:
                    break
        return True, deleted
    finally:
        discard = False
        if acquired:
            try:
                row = connection.execute(
                    "SELECT pg_advisory_unlock(?) AS released",
                    (ADVISORY_LOCK_KEY,),
                ).fetchone()
                if not row or row["released"] is not True:
                    discard = True
            except Exception:
                discard = True
        if discard:
            connection.discard()
        else:
            connection.close()


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    now_ts = int(time.time())
    backup = _validated_backups(args, now_ts=now_ts)
    repository = PostgresMonitoringRepository(
        application_name="stream-monitoring-v4-retention",
        pool_min_size=0,
        pool_max_size=1,
    )
    try:
        repository.ensure_schema()
        acquired, deleted = _run_retention(
            repository,
            now_ts=now_ts,
            batch_size=args.batch_size,
        )
    finally:
        repository.close()
    payload = {
        "schema": "monitoring_v4.retention_result.v1",
        "completed_at": utc_text(now_ts),
        "lock_acquired": acquired,
        "backup": backup,
        "deleted": deleted,
        "retention_days": {
            spec.name: spec.retention_days for spec in RETENTION_SPECS
        },
        "incident_notification_audit_deleted": False,
        "runtime_mutation_enabled": False,
        "notification_delivery_enabled": False,
    }
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
