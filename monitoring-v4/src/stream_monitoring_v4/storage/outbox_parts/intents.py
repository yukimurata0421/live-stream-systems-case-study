from __future__ import annotations

from typing import Any

from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.notification import NotificationIntent
from stream_contracts.monitoring_v4.time import unix_ts


class IntentRepositoryMixin:
    """Notification intent and explicit delivery-eligibility persistence."""

    def activate_delivery_epoch(
        self,
        *,
        cutover_at: str,
        activated_at: str,
        policy_revision: str,
        writer_identity: str,
        note: str,
    ) -> str:
        cutover_ts = unix_ts(cutover_at)
        activated_ts = unix_ts(activated_at)
        if cutover_ts > activated_ts:
            raise ValueError("notification cutover cannot be after activation")
        if not policy_revision.strip() or not writer_identity.strip() or not note.strip():
            raise ValueError("notification delivery epoch audit fields are required")
        epoch_id = stable_id(
            "nep",
            cutover_at,
            activated_at,
            policy_revision,
            writer_identity,
        )
        with self.transaction() as connection:
            connection.execute(
                """INSERT INTO notification_delivery_epochs(
                    epoch_id, status, cutover_at, cutover_ts, created_at, created_ts,
                    activated_at, activated_ts, closed_at, closed_ts,
                    policy_revision, writer_identity, note
                ) VALUES (?, 'active', ?, ?, ?, ?, ?, ?, '', 0, ?, ?, ?)""",
                (
                    epoch_id,
                    cutover_at,
                    cutover_ts,
                    activated_at,
                    activated_ts,
                    activated_at,
                    activated_ts,
                    policy_revision[:200],
                    writer_identity[:200],
                    note[:500],
                ),
            )
        return epoch_id

    def close_delivery_epoch(self, epoch_id: str, *, closed_at: str) -> None:
        closed_ts = unix_ts(closed_at)
        with self.transaction() as connection:
            cursor = connection.execute(
                """UPDATE notification_delivery_epochs
                SET status='closed', closed_at=?, closed_ts=?
                WHERE epoch_id=? AND status='active' AND activated_ts<=?""",
                (closed_at, closed_ts, epoch_id, closed_ts),
            )
            if cursor.rowcount != 1:
                raise ValueError("notification delivery epoch is not active")

    def append_intent(
        self,
        item: NotificationIntent,
        *,
        connection: Any,
        delivery_epoch_id: str | None = None,
    ) -> bool:
        mode = "shadow"
        eligible = 0
        reason = "credential_free_shadow"
        epoch_id: str | None = None
        if delivery_epoch_id is not None:
            row = connection.execute(
                """SELECT epoch_id, cutover_ts FROM notification_delivery_epochs
                WHERE epoch_id=? AND status='active'""",
                (delivery_epoch_id,),
            ).fetchone()
            if row is None:
                raise ValueError("notification delivery epoch is not active")
            if unix_ts(item.created_at) < int(row["cutover_ts"]):
                raise ValueError(
                    "pre-cutover notification intent cannot become deliverable"
                )
            mode = "production"
            eligible = 1
            reason = "created_in_active_delivery_epoch"
            epoch_id = str(row["epoch_id"])
        cursor = connection.execute(
            """INSERT OR IGNORE INTO notification_intents(
                intent_id, transition_id, episode_id, route, phase, severity,
                created_at, created_ts, not_before, not_before_ts, subject,
                content, dedupe_key, route_policy_revision, template_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                item.intent_id,
                item.transition_id,
                item.episode_id,
                item.route,
                item.phase,
                item.severity,
                item.created_at,
                unix_ts(item.created_at),
                item.not_before,
                unix_ts(item.not_before),
                item.subject,
                item.content,
                item.dedupe_key,
                item.route_policy_revision,
                item.template_revision,
            ),
        )
        connection.execute(
            """INSERT OR IGNORE INTO notification_intent_delivery(
                intent_id, mode, epoch_id, eligible, eligibility_reason,
                assigned_at, assigned_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                item.intent_id,
                mode,
                epoch_id,
                eligible,
                reason,
                item.created_at,
                unix_ts(item.created_at),
            ),
        )
        return cursor.rowcount == 1

    def intents(self) -> list[NotificationIntent]:
        with self.connection(read_only=True) as connection:
            rows = connection.execute(
                "SELECT * FROM notification_intents ORDER BY created_ts, intent_id"
            ).fetchall()
        return [self._intent(row) for row in rows]

    @staticmethod
    def _intent(row: Any) -> NotificationIntent:
        return NotificationIntent(
            intent_id=row["intent_id"],
            transition_id=row["transition_id"],
            episode_id=row["episode_id"],
            route=row["route"],
            phase=row["phase"],
            severity=row["severity"],
            created_at=row["created_at"],
            not_before=row["not_before"],
            subject=row["subject"],
            content=row["content"],
            dedupe_key=row["dedupe_key"],
            route_policy_revision=row["route_policy_revision"],
            template_revision=row["template_revision"],
        )

    def intent_delivery_metadata(self, intent_id: str) -> dict[str, Any]:
        with self.connection(read_only=True) as connection:
            row = connection.execute(
                "SELECT * FROM notification_intent_delivery WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
        return dict(row) if row is not None else {}
