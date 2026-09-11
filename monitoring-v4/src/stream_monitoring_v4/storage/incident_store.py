from __future__ import annotations

import json
from typing import Any

from stream_contracts.monitoring_v4.ids import canonical_json
from stream_contracts.monitoring_v4.incident import (
    IncidentEpisode,
    IncidentTransition,
)
from stream_contracts.monitoring_v4.time import unix_ts


class IncidentRepositoryMixin:
    """Incident candidate, episode, transition, and processed-current ledger."""

    def active_episode(
        self,
        domain: str,
        *,
        connection: Any | None = None,
    ) -> IncidentEpisode | None:
        owned = connection is None
        if owned:
            connection = self.connect(read_only=True)
        assert connection is not None
        try:
            row = connection.execute(
                "SELECT * FROM incident_episodes "
                "WHERE domain=? AND status='active'",
                (domain,),
            ).fetchone()
            return self._episode(row) if row else None
        finally:
            if owned:
                connection.close()

    @staticmethod
    def _episode(row: Any) -> IncidentEpisode:
        return IncidentEpisode(
            episode_id=row["episode_id"],
            domain=row["domain"],
            status=row["status"],
            severity=row["severity"],
            opened_at=row["opened_at"],
            last_bad_at=row["last_bad_at"],
            closed_at=row["closed_at"],
            bad_samples=int(row["bad_samples"]),
            unknown_samples=int(row["unknown_samples"]),
            last_transition_at=row["last_transition_at"],
            next_notification_at=row["next_notification_at"],
            policy_revision=row["policy_revision"],
            summary=row["summary"],
            reason_codes=tuple(json.loads(row["reason_codes_json"])),
        )

    def save_episode(self, item: IncidentEpisode, *, connection: Any) -> None:
        connection.execute(
            """INSERT INTO incident_episodes(
                episode_id, domain, status, severity, opened_at, opened_ts,
                last_bad_at, last_bad_ts, closed_at, closed_ts, bad_samples,
                unknown_samples, last_transition_at, last_transition_ts,
                next_notification_at, next_notification_ts, policy_revision,
                summary, reason_codes_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(episode_id) DO UPDATE SET
                status=excluded.status,
                severity=excluded.severity,
                last_bad_at=excluded.last_bad_at,
                last_bad_ts=excluded.last_bad_ts,
                closed_at=excluded.closed_at,
                closed_ts=excluded.closed_ts,
                bad_samples=excluded.bad_samples,
                unknown_samples=excluded.unknown_samples,
                last_transition_at=excluded.last_transition_at,
                last_transition_ts=excluded.last_transition_ts,
                next_notification_at=excluded.next_notification_at,
                next_notification_ts=excluded.next_notification_ts,
                policy_revision=excluded.policy_revision,
                summary=excluded.summary,
                reason_codes_json=excluded.reason_codes_json""",
            (
                item.episode_id,
                item.domain,
                item.status,
                item.severity,
                item.opened_at,
                unix_ts(item.opened_at),
                item.last_bad_at,
                unix_ts(item.last_bad_at),
                item.closed_at,
                unix_ts(item.closed_at) if item.closed_at else 0,
                item.bad_samples,
                item.unknown_samples,
                item.last_transition_at,
                unix_ts(item.last_transition_at),
                item.next_notification_at,
                unix_ts(item.next_notification_at) if item.next_notification_at else 0,
                item.policy_revision,
                item.summary,
                canonical_json(item.reason_codes),
            ),
        )

    def has_recovered_episode_overlapping(
        self,
        domain: str,
        *,
        opened_at: str,
        recovered_at: str,
        excluding_policy_revision: str,
        connection: Any,
    ) -> bool:
        row = connection.execute(
            """SELECT 1 FROM incident_episodes AS episode
            WHERE episode.domain=? AND episode.status='closed'
              AND episode.policy_revision<>? AND episode.opened_ts<=?
              AND episode.closed_ts>=?
              AND EXISTS (
                  SELECT 1 FROM incident_transitions AS transition
                  WHERE transition.episode_id=episode.episode_id
                    AND transition.phase='recovered'
              )
            LIMIT 1""",
            (
                domain,
                excluding_policy_revision,
                unix_ts(recovered_at),
                unix_ts(opened_at),
            ),
        ).fetchone()
        return row is not None

    def episode_has_transition(
        self,
        episode_id: str,
        phase: str,
        *,
        connection: Any,
    ) -> bool:
        row = connection.execute(
            "SELECT 1 FROM incident_transitions "
            "WHERE episode_id=? AND phase=? LIMIT 1",
            (episode_id, phase),
        ).fetchone()
        return row is not None

    def candidate(self, domain: str, *, connection: Any) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT * FROM incident_candidates WHERE domain=?",
            (domain,),
        ).fetchone()
        if row is None:
            return None
        value = dict(row)
        value["reason_codes"] = json.loads(value.pop("reason_codes_json"))
        return value

    def save_candidate(self, value: dict[str, Any], *, connection: Any) -> None:
        connection.execute(
            """INSERT INTO incident_candidates(
                domain, state, first_seen_at, first_seen_ts, last_seen_at,
                last_seen_ts, samples, snapshot_id, reason_codes_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(domain) DO UPDATE SET
                state=excluded.state,
                first_seen_at=excluded.first_seen_at,
                first_seen_ts=excluded.first_seen_ts,
                last_seen_at=excluded.last_seen_at,
                last_seen_ts=excluded.last_seen_ts,
                samples=excluded.samples,
                snapshot_id=excluded.snapshot_id,
                reason_codes_json=excluded.reason_codes_json""",
            (
                value["domain"],
                value["state"],
                value["first_seen_at"],
                unix_ts(value["first_seen_at"]),
                value["last_seen_at"],
                unix_ts(value["last_seen_at"]),
                int(value["samples"]),
                value["snapshot_id"],
                canonical_json(value["reason_codes"]),
            ),
        )

    def delete_candidate(self, domain: str, *, connection: Any) -> None:
        connection.execute(
            "DELETE FROM incident_candidates WHERE domain=?",
            (domain,),
        )

    def append_transition(
        self,
        item: IncidentTransition,
        *,
        connection: Any,
    ) -> bool:
        cursor = connection.execute(
            """INSERT OR IGNORE INTO incident_transitions(
                transition_id, episode_id, domain, phase, severity, occurred_at,
                occurred_ts, current_snapshot_id, summary, reason_codes_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                item.transition_id,
                item.episode_id,
                item.domain,
                item.phase,
                item.severity,
                item.occurred_at,
                unix_ts(item.occurred_at),
                item.current_snapshot_id,
                item.summary,
                canonical_json(item.reason_codes),
            ),
        )
        return cursor.rowcount == 1

    def current_was_processed(self, snapshot_id: str, *, connection: Any) -> bool:
        row = connection.execute(
            "SELECT 1 FROM incident_processed_currents WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone()
        return row is not None

    def mark_current_processed(
        self,
        snapshot_id: str,
        domain: str,
        *,
        processed_at: str,
        connection: Any,
    ) -> None:
        connection.execute(
            """INSERT OR IGNORE INTO incident_processed_currents(
                snapshot_id, domain, processed_at, processed_ts
            ) VALUES (?, ?, ?, ?)""",
            (snapshot_id, domain, processed_at, unix_ts(processed_at)),
        )

    def transitions(self, *, domain: str | None = None) -> list[IncidentTransition]:
        query = "SELECT * FROM incident_transitions"
        params: tuple[Any, ...] = ()
        if domain:
            query += " WHERE domain=?"
            params = (domain,)
        query += " ORDER BY occurred_ts, transition_id"
        with self.connection(read_only=True) as connection:
            rows = connection.execute(query, params).fetchall()
        return [
            IncidentTransition(
                transition_id=row["transition_id"],
                episode_id=row["episode_id"],
                domain=row["domain"],
                phase=row["phase"],
                severity=row["severity"],
                occurred_at=row["occurred_at"],
                current_snapshot_id=row["current_snapshot_id"],
                summary=row["summary"],
                reason_codes=tuple(json.loads(row["reason_codes_json"])),
            )
            for row in rows
        ]
