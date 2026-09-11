from __future__ import annotations

from typing import Any

from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.notification import NotificationIntent
from stream_contracts.monitoring_v4.time import utc_text

from .constants import ATTEMPT_STATES


class DeliveryAttemptRepositoryMixin:
    """Fenced delivery-attempt state machine and read model."""

    def begin_delivery_attempt(
        self,
        intent_id: str,
        owner: str,
        *,
        lease_name: str,
        fence_token: int,
        now_ts: int,
    ) -> dict[str, Any]:
        with self.transaction() as connection:
            lease = connection.execute(
                """SELECT lease.owner, lease.expires_ts, fence.fence_token
                FROM leases AS lease
                JOIN lease_fences AS fence ON fence.lease_name=lease.lease_name
                WHERE lease.lease_name=?""",
                (lease_name,),
            ).fetchone()
            if (
                lease is None
                or lease["owner"] != owner
                or int(lease["fence_token"]) != int(fence_token)
                or int(lease["expires_ts"]) <= now_ts
            ):
                raise RuntimeError("delivery lease fence is not current")
            delivery = connection.execute(
                """SELECT intent.created_ts, state.eligible, epoch.status, epoch.cutover_ts
                FROM notification_intents AS intent
                JOIN notification_intent_delivery AS state
                  ON state.intent_id=intent.intent_id
                JOIN notification_delivery_epochs AS epoch ON epoch.epoch_id=state.epoch_id
                WHERE intent.intent_id=? AND state.mode='production'""",
                (intent_id,),
            ).fetchone()
            if (
                delivery is None
                or int(delivery["eligible"]) != 1
                or delivery["status"] != "active"
                or int(delivery["created_ts"]) < int(delivery["cutover_ts"])
            ):
                raise RuntimeError("notification intent is not delivery eligible")
            blocker = connection.execute(
                """SELECT state.state FROM delivery_attempts AS attempt
                JOIN delivery_attempt_states AS state
                  ON state.attempt_id=attempt.attempt_id
                WHERE attempt.intent_id=?
                  AND state.state IN (
                    'in_flight', 'succeeded', 'permanent_failed', 'uncertain'
                  )
                LIMIT 1""",
                (intent_id,),
            ).fetchone()
            if blocker is not None:
                raise RuntimeError(
                    f"notification intent is blocked by {blocker['state']}"
                )
            row = connection.execute(
                "SELECT COALESCE(MAX(attempt_no), 0) AS attempt_no "
                "FROM delivery_attempts WHERE intent_id=?",
                (intent_id,),
            ).fetchone()
            attempt_no = int(row["attempt_no"]) + 1
            attempt_id = stable_id("try", intent_id, attempt_no)
            connection.execute(
                """INSERT INTO delivery_attempts(
                    attempt_id, intent_id, attempt_no, owner, started_at, started_ts
                ) VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    attempt_id,
                    intent_id,
                    attempt_no,
                    owner,
                    utc_text(now_ts),
                    now_ts,
                ),
            )
            connection.execute(
                """INSERT INTO delivery_attempt_states(
                    attempt_id, state, lease_name, fence_token,
                    next_retry_ts, completed_ts, detail
                ) VALUES (?, 'in_flight', ?, ?, 0, 0, '')""",
                (attempt_id, lease_name, int(fence_token)),
            )
        return {
            "attempt_id": attempt_id,
            "intent_id": intent_id,
            "attempt_no": attempt_no,
            "started_ts": now_ts,
            "owner": owner,
            "lease_name": lease_name,
            "fence_token": int(fence_token),
        }

    def finish_delivery_attempt(
        self,
        attempt: dict[str, Any],
        *,
        success: bool,
        status_code: int,
        detail: str,
        retry_after_sec: int,
        now_ts: int,
        outcome_state: str,
    ) -> str:
        if outcome_state not in ATTEMPT_STATES - {"in_flight"}:
            raise ValueError("invalid delivery attempt outcome state")
        retry_delay = max(
            0,
            int(retry_after_sec),
            (
                2 ** min(10, int(attempt["attempt_no"]))
                if outcome_state == "retryable_failed"
                else 0
            ),
        )
        result_id = stable_id(
            "res",
            attempt["attempt_id"],
            success,
            status_code,
            now_ts,
            detail[:200],
        )
        fence_lost = False
        with self.transaction() as connection:
            lease = connection.execute(
                """SELECT lease.owner, lease.expires_ts, fence.fence_token
                FROM leases AS lease
                JOIN lease_fences AS fence ON fence.lease_name=lease.lease_name
                WHERE lease.lease_name=?""",
                (attempt["lease_name"],),
            ).fetchone()
            fence_current = (
                lease is not None
                and lease["owner"] == attempt["owner"]
                and int(lease["fence_token"]) == int(attempt["fence_token"])
                and int(lease["expires_ts"]) > now_ts
            )
            if not fence_current:
                connection.execute(
                    """UPDATE delivery_attempt_states
                    SET state='uncertain', completed_ts=?,
                        detail='lease_fence_lost_before_result'
                    WHERE attempt_id=? AND state='in_flight'""",
                    (now_ts, attempt["attempt_id"]),
                )
                fence_lost = True
            else:
                cursor = connection.execute(
                    """UPDATE delivery_attempt_states
                    SET state=?, next_retry_ts=?, completed_ts=?, detail=?
                    WHERE attempt_id=? AND state='in_flight'
                      AND lease_name=? AND fence_token=?""",
                    (
                        outcome_state,
                        (
                            now_ts + retry_delay
                            if outcome_state == "retryable_failed"
                            else 0
                        ),
                        now_ts,
                        detail[:500],
                        attempt["attempt_id"],
                        attempt["lease_name"],
                        int(attempt["fence_token"]),
                    ),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("delivery attempt is no longer in flight")
                connection.execute(
                    """INSERT INTO delivery_results(
                        result_id, attempt_id, intent_id, success, status_code,
                        detail, retry_after_sec, completed_at, completed_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        result_id,
                        attempt["attempt_id"],
                        attempt["intent_id"],
                        1 if success else 0,
                        int(status_code),
                        detail[:500],
                        max(0, int(retry_after_sec)),
                        utc_text(now_ts),
                        now_ts,
                    ),
                )
        if fence_lost:
            raise RuntimeError(
                "delivery lease fence was lost before result persistence"
            )
        return result_id

    def quarantine_stale_delivery_attempts(
        self,
        *,
        now_ts: int,
        attempt_timeout_sec: int,
    ) -> int:
        with self.transaction() as connection:
            cursor = connection.execute(
                """UPDATE delivery_attempt_states
                SET state='uncertain', completed_ts=?,
                    detail='writer_lost_before_result'
                WHERE state='in_flight' AND attempt_id IN (
                    SELECT attempt_id FROM delivery_attempts WHERE started_ts<=?
                )""",
                (now_ts, now_ts - max(1, int(attempt_timeout_sec))),
            )
            return max(0, int(cursor.rowcount))

    def delivery_state(self, intent_id: str) -> dict[str, Any]:
        with self.connection(read_only=True) as connection:
            attempts = connection.execute(
                """SELECT attempt.*, state.state, state.lease_name,
                    state.fence_token, state.next_retry_ts,
                    state.completed_ts AS state_completed_ts,
                    state.detail AS state_detail
                FROM delivery_attempts AS attempt
                JOIN delivery_attempt_states AS state
                  ON state.attempt_id=attempt.attempt_id
                WHERE attempt.intent_id=? ORDER BY attempt.attempt_no""",
                (intent_id,),
            ).fetchall()
            results = connection.execute(
                "SELECT * FROM delivery_results WHERE intent_id=? "
                "ORDER BY completed_ts, result_id",
                (intent_id,),
            ).fetchall()
        return {
            "attempts": [dict(row) for row in attempts],
            "results": [dict(row) for row in results],
        }

    def due_intents(
        self,
        *,
        now_ts: int,
        max_attempts: int,
        attempt_timeout_sec: int,
        limit: int,
    ) -> list[NotificationIntent]:
        del attempt_timeout_sec
        with self.connection(read_only=True) as connection:
            rows = connection.execute(
                """SELECT intent.* FROM notification_intents AS intent
                JOIN notification_intent_delivery AS delivery
                  ON delivery.intent_id=intent.intent_id
                JOIN notification_delivery_epochs AS epoch
                  ON epoch.epoch_id=delivery.epoch_id
                WHERE delivery.mode='production' AND delivery.eligible=1
                  AND epoch.status='active' AND intent.created_ts>=epoch.cutover_ts
                  AND intent.not_before_ts<=?
                  AND (
                      SELECT COUNT(*) FROM delivery_attempts attempt
                      WHERE attempt.intent_id=intent.intent_id
                  ) < ?
                  AND NOT EXISTS (
                      SELECT 1 FROM delivery_attempts attempt
                      JOIN delivery_attempt_states state
                        ON state.attempt_id=attempt.attempt_id
                      WHERE attempt.intent_id=intent.intent_id
                        AND state.state IN (
                          'in_flight', 'succeeded', 'permanent_failed', 'uncertain'
                        )
                  )
                  AND (
                      NOT EXISTS (
                          SELECT 1 FROM delivery_attempts attempt
                          WHERE attempt.intent_id=intent.intent_id
                      )
                      OR EXISTS (
                          SELECT 1 FROM delivery_attempts attempt
                          JOIN delivery_attempt_states state
                            ON state.attempt_id=attempt.attempt_id
                          WHERE attempt.intent_id=intent.intent_id
                            AND attempt.attempt_no=(
                                SELECT MAX(latest.attempt_no)
                                FROM delivery_attempts latest
                                WHERE latest.intent_id=intent.intent_id
                            )
                            AND state.state='retryable_failed'
                            AND state.next_retry_ts<=?
                      )
                  )
                ORDER BY intent.not_before_ts, intent.intent_id
                LIMIT ?""",
                (
                    now_ts,
                    max(1, int(max_attempts)),
                    now_ts,
                    max(1, int(limit)),
                ),
            ).fetchall()
        return [self._intent(row) for row in rows]

    def undelivered_intent_count(
        self,
        *,
        now_ts: int,
        max_attempts: int,
        attempt_timeout_sec: int,
    ) -> int:
        del now_ts, attempt_timeout_sec
        with self.connection(read_only=True) as connection:
            row = connection.execute(
                """SELECT COUNT(*) AS count FROM notification_intents AS intent
                JOIN notification_intent_delivery AS delivery
                  ON delivery.intent_id=intent.intent_id
                JOIN notification_delivery_epochs AS epoch
                  ON epoch.epoch_id=delivery.epoch_id
                WHERE delivery.mode='production' AND delivery.eligible=1
                  AND epoch.status='active' AND intent.created_ts>=epoch.cutover_ts
                  AND (
                      SELECT COUNT(*) FROM delivery_attempts attempt
                      WHERE attempt.intent_id=intent.intent_id
                  ) < ?
                  AND NOT EXISTS (
                      SELECT 1 FROM delivery_attempts attempt
                      JOIN delivery_attempt_states state
                        ON state.attempt_id=attempt.attempt_id
                      WHERE attempt.intent_id=intent.intent_id
                        AND state.state IN (
                          'in_flight', 'succeeded', 'permanent_failed', 'uncertain'
                        )
                  )""",
                (max(1, int(max_attempts)),),
            ).fetchone()
        return int(row["count"])

    def delivery_outbox_counts(self, *, max_attempts: int = 8) -> dict[str, int]:
        with self.connection(read_only=True) as connection:
            shadow = connection.execute(
                """SELECT COUNT(*) AS count FROM notification_intent_delivery
                WHERE mode='shadow' AND eligible=0"""
            ).fetchone()
            states = connection.execute(
                """SELECT state.state, COUNT(*) AS count
                FROM delivery_attempt_states AS state GROUP BY state.state"""
            ).fetchall()
            exhausted = connection.execute(
                """SELECT COUNT(*) AS count
                FROM notification_intent_delivery AS delivery
                WHERE delivery.mode='production' AND delivery.eligible=1
                  AND (
                      SELECT COUNT(*) FROM delivery_attempts attempt
                      WHERE attempt.intent_id=delivery.intent_id
                  ) >= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM delivery_attempts attempt
                      JOIN delivery_attempt_states state
                        ON state.attempt_id=attempt.attempt_id
                      WHERE attempt.intent_id=delivery.intent_id
                        AND state.state IN (
                          'succeeded', 'permanent_failed', 'uncertain'
                        )
                  )""",
                (max(1, int(max_attempts)),),
            ).fetchone()
        result = {
            "shadow_quarantined": int(shadow["count"]),
            "exhausted": int(exhausted["count"]),
        }
        result.update({str(row["state"]): int(row["count"]) for row in states})
        return result
