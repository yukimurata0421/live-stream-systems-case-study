from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from typing import Any, Mapping, Sequence

from stream_contracts.monitoring_v4.ids import canonical_json
from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.sli import SLIProjection
from stream_contracts.monitoring_v4.time import unix_ts


class ProjectionRepositoryMixin:
    def save_sli_projection(
        self,
        item: SLIProjection,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> bool:
        owned = connection is None
        if owned:
            connection = self.connect()
        assert connection is not None
        try:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO sli_projections(
                    projection_id, schema_name, objective_id, window_name, assessment_scope,
                    is_official_window, observed, eligible, bad, missing, coverage_pct,
                    source_freshness_pct, source_disagreement, compliance_status,
                    measurement_unknown_reasons_json, window_start, window_start_ts,
                    window_end, window_end_ts, evaluated_at, evaluated_ts, policy_revision,
                    evidence_ids_json, no_automatic_recovery, payload_sha256, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    item.projection_id,
                    item.SCHEMA,
                    item.objective_id,
                    item.window,
                    item.assessment_scope,
                    int(item.is_official_window),
                    item.observed,
                    item.eligible,
                    item.bad,
                    item.missing,
                    item.coverage_pct,
                    item.source_freshness_pct,
                    int(item.source_disagreement),
                    item.compliance_status,
                    canonical_json(item.measurement_unknown_reasons),
                    item.window_start,
                    unix_ts(item.window_start),
                    item.window_end,
                    unix_ts(item.window_end),
                    item.evaluated_at,
                    unix_ts(item.evaluated_at),
                    item.policy_revision,
                    canonical_json(item.evidence_ids),
                    1,
                    item.payload_sha256,
                    canonical_json(item.payload),
                ),
            )
            connection.execute(
                """INSERT INTO sli_projection_current(
                    objective_id, assessment_scope, window_name, projection_id
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(objective_id, assessment_scope, window_name)
                DO UPDATE SET projection_id=excluded.projection_id
                WHERE
                    (SELECT window_end_ts FROM sli_projections
                     WHERE projection_id=excluded.projection_id)
                    >
                    (SELECT window_end_ts FROM sli_projections
                     WHERE projection_id=sli_projection_current.projection_id)
                   OR (
                    (SELECT window_end_ts FROM sli_projections
                     WHERE projection_id=excluded.projection_id)
                    =
                    (SELECT window_end_ts FROM sli_projections
                     WHERE projection_id=sli_projection_current.projection_id)
                    AND (
                        (SELECT evaluated_ts FROM sli_projections
                         WHERE projection_id=excluded.projection_id)
                        >
                        (SELECT evaluated_ts FROM sli_projections
                         WHERE projection_id=sli_projection_current.projection_id)
                        OR (
                            (SELECT evaluated_ts FROM sli_projections
                             WHERE projection_id=excluded.projection_id)
                            =
                            (SELECT evaluated_ts FROM sli_projections
                             WHERE projection_id=sli_projection_current.projection_id)
                            AND excluded.projection_id > sli_projection_current.projection_id
                        )
                    )
                   )""",
                (item.objective_id, item.assessment_scope, item.window, item.projection_id),
            )
            return cursor.rowcount == 1
        finally:
            if owned:
                connection.close()

    def current_sli_projections(self) -> list[SLIProjection]:
        with self.connection(read_only=True) as connection:
            rows = connection.execute(
                """SELECT p.* FROM sli_projection_current c
                JOIN sli_projections p ON p.projection_id=c.projection_id
                ORDER BY p.objective_id, p.assessment_scope, p.window_name"""
            ).fetchall()
        return [self._sli_projection(row) for row in rows]

    @staticmethod
    def _sli_projection(row: sqlite3.Row) -> SLIProjection:
        return SLIProjection(
            projection_id=row["projection_id"],
            objective_id=row["objective_id"],
            window=row["window_name"],
            assessment_scope=row["assessment_scope"],
            is_official_window=bool(row["is_official_window"]),
            observed=row["observed"],
            eligible=row["eligible"],
            bad=row["bad"],
            missing=row["missing"],
            coverage_pct=row["coverage_pct"],
            source_freshness_pct=row["source_freshness_pct"],
            source_disagreement=bool(row["source_disagreement"]),
            compliance_status=row["compliance_status"],
            measurement_unknown_reasons=tuple(
                json.loads(row["measurement_unknown_reasons_json"])
            ),
            window_start=row["window_start"],
            window_end=row["window_end"],
            evaluated_at=row["evaluated_at"],
            policy_revision=row["policy_revision"],
            evidence_ids=tuple(json.loads(row["evidence_ids_json"])),
            no_automatic_recovery=bool(row["no_automatic_recovery"]),
            payload_sha256=row["payload_sha256"],
            payload=json.loads(row["payload_json"]),
        )

    def observations_in_window(
        self,
        *,
        source: str,
        window_start_ts: int,
        window_end_ts: int,
    ) -> list[ObservationEnvelope]:
        with self.connection(read_only=True) as connection:
            rows = connection.execute(
                """SELECT * FROM observations
                WHERE source=? AND observed_ts>=? AND observed_ts<?
                ORDER BY observed_ts, observation_id""",
                (source, int(window_start_ts), int(window_end_ts)),
            ).fetchall()
        return [self._observation(row) for row in rows]

    def append_shadow_cycle(
        self,
        *,
        cycle_id: str,
        started_at: str,
        completed_at: str,
        build_revision: str,
        source_revision: str,
        observer: Mapping[str, Any],
        current_states: Mapping[str, str],
        parity: Mapping[str, Any],
        notification_intent_count: int,
        connection: sqlite3.Connection | None = None,
    ) -> bool:
        owned = connection is None
        if owned:
            connection = self.connect()
        assert connection is not None
        try:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO shadow_cycles(
                    cycle_id, started_at, started_ts, completed_at, completed_ts,
                    build_revision, source_revision, observer_json, current_states_json, parity_json,
                    notification_intent_count, real_delivery_enabled, runtime_mutation_enabled
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)""",
                (
                    cycle_id,
                    started_at,
                    unix_ts(started_at),
                    completed_at,
                    unix_ts(completed_at),
                    build_revision[:200],
                    source_revision[:240],
                    canonical_json(observer),
                    canonical_json(current_states),
                    canonical_json(parity),
                    max(0, int(notification_intent_count)),
                ),
            )
            return cursor.rowcount == 1
        finally:
            if owned:
                connection.close()

    def shadow_cycle_rows(self, *, start_ts: int = 0, end_ts: int = 2**63 - 1) -> list[dict[str, Any]]:
        with self.connection(read_only=True) as connection:
            rows = connection.execute(
                """SELECT * FROM shadow_cycles WHERE started_ts>=? AND started_ts<?
                ORDER BY started_ts, cycle_id""",
                (int(start_ts), int(end_ts)),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            value = dict(row)
            for field in ("observer_json", "current_states_json", "parity_json"):
                value[field.removesuffix("_json")] = json.loads(value.pop(field))
            result.append(value)
        return result

    def shadow_cycle_report_rows(
        self,
        *,
        build_revision: str,
        source_revision: str,
        start_ts: int,
        end_ts: int,
    ) -> list[dict[str, Any]]:
        """Return only the reporter read model, already revision-filtered.

        Report calculations are order-independent. Omitting ``ORDER BY`` and
        wide unused columns prevents PostgreSQL from sorting full JSON rows and
        spilling the sort to temporary files as the soak ledger grows.
        """

        return list(
            self.iter_shadow_cycle_report_rows(
                build_revision=build_revision,
                source_revision=source_revision,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        )

    def iter_shadow_cycle_report_rows(
        self,
        *,
        build_revision: str,
        source_revision: str,
        start_ts: int,
        end_ts: int,
        batch_size: int = 256,
    ) -> Iterator[dict[str, Any]]:
        """Stream the revision-pinned reporter read model in bounded batches.

        The reporter runs inside a small, long-lived Pod.  Returning a list here
        retained both the raw JSON rows and their decoded object graphs for the
        complete 14-day window.  Keeping the connection inside the generator
        lets both SQLite and PostgreSQL release each batch after it is reduced.
        """

        size = max(1, min(int(batch_size), 4096))
        with self.connection(read_only=True) as connection:
            cursor = connection.execute(
                """SELECT started_ts, observer_json, parity_json
                FROM shadow_cycles
                WHERE build_revision=? AND source_revision=?
                  AND started_ts>=? AND started_ts<?""",
                (
                    build_revision,
                    source_revision,
                    int(start_ts),
                    int(end_ts),
                ),
            )
            while True:
                rows = cursor.fetchmany(size)
                if not rows:
                    break
                for row in rows:
                    yield {
                        "started_ts": int(row["started_ts"]),
                        "observer": json.loads(row["observer_json"]),
                        "parity": json.loads(row["parity_json"]),
                    }
