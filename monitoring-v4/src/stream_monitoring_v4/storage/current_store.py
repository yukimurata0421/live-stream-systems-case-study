from __future__ import annotations

import json
from typing import Any

from stream_contracts.monitoring_v4.current import DomainCurrent
from stream_contracts.monitoring_v4.ids import canonical_json
from stream_contracts.monitoring_v4.time import unix_ts


class CurrentRepositoryMixin:
    """Immutable current snapshots and one monotonic canonical row per domain."""

    @staticmethod
    def _current_values(item: DomainCurrent) -> tuple[Any, ...]:
        return (
            item.snapshot_id,
            item.domain,
            item.state,
            item.observed_at,
            unix_ts(item.observed_at),
            item.reduced_at,
            unix_ts(item.reduced_at),
            item.valid_until,
            unix_ts(item.valid_until),
            item.policy_revision,
            item.reducer_revision,
            canonical_json(item.reason_codes),
            canonical_json(item.source_observation_ids),
            canonical_json(item.payload),
        )

    def append_current_snapshot(
        self,
        item: DomainCurrent,
        *,
        connection: Any | None = None,
    ) -> bool:
        owned = connection is None
        if owned:
            connection = self.connect()
        assert connection is not None
        try:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO current_snapshots(
                    snapshot_id, domain, state, observed_at, observed_ts,
                    reduced_at, reduced_ts, valid_until, valid_until_ts,
                    policy_revision, reducer_revision, reason_codes_json,
                    source_observation_ids_json, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                self._current_values(item),
            )
            return cursor.rowcount == 1
        finally:
            if owned:
                connection.close()

    def save_current(
        self,
        item: DomainCurrent,
        *,
        connection: Any | None = None,
    ) -> bool:
        owned = connection is None
        if owned:
            connection = self.connect()
        assert connection is not None
        values = self._current_values(item)
        try:
            self.append_current_snapshot(item, connection=connection)
            current_cursor = connection.execute(
                """INSERT INTO domain_current(
                    snapshot_id, domain, state, observed_at, observed_ts,
                    reduced_at, reduced_ts, valid_until, valid_until_ts,
                    policy_revision, reducer_revision, reason_codes_json,
                    source_observation_ids_json, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain) DO UPDATE SET
                    snapshot_id=excluded.snapshot_id,
                    state=excluded.state,
                    observed_at=excluded.observed_at,
                    observed_ts=excluded.observed_ts,
                    reduced_at=excluded.reduced_at,
                    reduced_ts=excluded.reduced_ts,
                    valid_until=excluded.valid_until,
                    valid_until_ts=excluded.valid_until_ts,
                    policy_revision=excluded.policy_revision,
                    reducer_revision=excluded.reducer_revision,
                    reason_codes_json=excluded.reason_codes_json,
                    source_observation_ids_json=excluded.source_observation_ids_json,
                    payload_json=excluded.payload_json
                WHERE excluded.observed_ts > domain_current.observed_ts
                   OR (
                       excluded.observed_ts = domain_current.observed_ts
                       AND excluded.reduced_ts >= domain_current.reduced_ts
                   )
                   OR (
                       excluded.reduced_ts > domain_current.valid_until_ts
                   )""",
                values,
            )
            return current_cursor.rowcount == 1
        finally:
            if owned:
                connection.close()

    def current(
        self,
        domain: str,
        *,
        connection: Any | None = None,
    ) -> DomainCurrent | None:
        owned = connection is None
        if owned:
            connection = self.connect(read_only=True)
        assert connection is not None
        try:
            row = connection.execute(
                "SELECT * FROM domain_current WHERE domain=?",
                (domain,),
            ).fetchone()
            return self._current(row) if row else None
        finally:
            if owned:
                connection.close()

    @staticmethod
    def _current(row: Any) -> DomainCurrent:
        return DomainCurrent(
            snapshot_id=row["snapshot_id"],
            domain=row["domain"],
            state=row["state"],
            reason_codes=tuple(json.loads(row["reason_codes_json"])),
            source_observation_ids=tuple(
                json.loads(row["source_observation_ids_json"])
            ),
            observed_at=row["observed_at"],
            reduced_at=row["reduced_at"],
            valid_until=row["valid_until"],
            policy_revision=row["policy_revision"],
            reducer_revision=row["reducer_revision"],
            payload=json.loads(row["payload_json"]),
        )
