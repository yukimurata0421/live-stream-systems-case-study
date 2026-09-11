from __future__ import annotations

import json
from typing import Any

from stream_contracts.monitoring_v4.ids import canonical_json
from stream_contracts.monitoring_v4.observation import (
    ObservationEnvelope,
    ObservationRejection,
)
from stream_contracts.monitoring_v4.time import unix_ts


class ObservationRepositoryMixin:
    """Immutable observation and source-rejection ledger."""

    def append_observation(
        self,
        item: ObservationEnvelope,
        *,
        connection: Any | None = None,
    ) -> bool:
        owned = connection is None
        if owned:
            connection = self.connect()
        assert connection is not None
        try:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO observations(
                    observation_id, schema_name, domain, source, source_event_id,
                    source_generation, evidence_role, status, reason_code,
                    observed_at, observed_ts, received_at, received_ts,
                    freshness_limit_sec, producer_revision, payload_sha256,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    item.observation_id,
                    item.SCHEMA,
                    item.domain,
                    item.source,
                    item.source_event_id,
                    item.source_generation,
                    item.evidence_role,
                    item.status,
                    item.reason_code,
                    item.observed_at,
                    unix_ts(item.observed_at),
                    item.received_at,
                    unix_ts(item.received_at),
                    item.freshness_limit_sec,
                    item.producer_revision,
                    item.payload_sha256,
                    canonical_json(item.payload),
                ),
            )
            return cursor.rowcount == 1
        finally:
            if owned:
                connection.close()

    def append_rejection(
        self,
        item: ObservationRejection,
        *,
        connection: Any | None = None,
    ) -> bool:
        owned = connection is None
        if owned:
            connection = self.connect()
        assert connection is not None
        try:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO rejections(
                    rejection_id, schema_name, source, reason_code, detail,
                    received_at, received_ts, payload_sha256
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    item.rejection_id,
                    item.SCHEMA,
                    item.source,
                    item.reason_code,
                    item.detail,
                    item.received_at,
                    unix_ts(item.received_at),
                    item.payload_sha256,
                ),
            )
            return cursor.rowcount == 1
        finally:
            if owned:
                connection.close()

    def latest_observations(
        self,
        domain: str,
        *,
        limit: int = 200,
    ) -> list[ObservationEnvelope]:
        with self.connection(read_only=True) as connection:
            rows = connection.execute(
                "SELECT * FROM observations WHERE domain=? "
                "ORDER BY observed_ts DESC, observation_id LIMIT ?",
                (domain, max(1, int(limit))),
            ).fetchall()
        return [self._observation(row) for row in rows]

    @staticmethod
    def _observation(row: Any) -> ObservationEnvelope:
        return ObservationEnvelope(
            observation_id=row["observation_id"],
            domain=row["domain"],
            source=row["source"],
            source_event_id=row["source_event_id"],
            source_generation=row["source_generation"],
            evidence_role=row["evidence_role"],
            status=row["status"],
            reason_code=row["reason_code"],
            observed_at=row["observed_at"],
            received_at=row["received_at"],
            freshness_limit_sec=int(row["freshness_limit_sec"]),
            producer_revision=row["producer_revision"],
            payload_sha256=row["payload_sha256"],
            payload=json.loads(row["payload_json"]),
        )
