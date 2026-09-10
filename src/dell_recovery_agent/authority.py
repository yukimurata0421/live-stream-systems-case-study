from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from typing import Any

from cra_dell_recovery.canonical import SignedMessageCodec
from cra_dell_recovery.errors import AuthorityDeadlineExceeded, LedgerUnavailable
from cra_dell_recovery.sqlite import map_sqlite_failure
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now
from dell_recovery_agent.deadline import AuthorityOperationDeadline
from dell_recovery_agent.storage import DellStore
from maintenance_audit import audit_maintenance_decision


class AgentAuthorityLease:
    def __init__(
        self,
        store: DellStore,
        codec: SignedMessageCodec,
        *,
        suspect_after_seconds: float = 4.0,
        lease_ttl_seconds: float = 15.0,
        critical_db_deadline_seconds: float | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not 0 < suspect_after_seconds < lease_ttl_seconds:
            raise ValueError("suspect threshold must be between zero and TTL")
        self.store = store
        self.codec = codec
        self.suspect_after_seconds = suspect_after_seconds
        self.lease_ttl_seconds = lease_ttl_seconds
        self.critical_db_deadline_seconds = (
            suspect_after_seconds - min(1.0, suspect_after_seconds / 4)
            if critical_db_deadline_seconds is None
            else critical_db_deadline_seconds
        )
        if not 0 < self.critical_db_deadline_seconds < suspect_after_seconds:
            raise ValueError("critical DB deadline must be below the authority suspect threshold")
        self.monotonic = monotonic
        self._last_valid_monotonic: dict[str, float] = {}
        self._last_audited_authority_state: dict[str, str] = {}

    def receive(self, payload: dict[str, Any]) -> str:
        deadline = AuthorityOperationDeadline.start(
            deadline_seconds=self.critical_db_deadline_seconds,
            monotonic=self.monotonic,
        )
        heartbeat = self.codec.decode(payload)
        target_id = str(heartbeat["target_id"])
        if parse_utc(str(heartbeat["expires_at"])) <= utc_now():
            return "HEARTBEAT_EXPIRED"
        received_at = isoformat_utc(utc_now())
        try:
            with self.store.write() as db:
                deadline.ensure_valid()
                fence = db.execute("SELECT * FROM authority_fences WHERE target_id=?", (target_id,)).fetchone()
                if fence is None:
                    raise LedgerUnavailable("SQLITE_AUTHORITY_FENCE_MISSING")
                if fence["authority_state"] == "LOCAL_FALLBACK":
                    return "LOCAL_AUTHORITY_ACTIVE"
                if fence["authority_state"] not in ("CENTRAL_ACTIVE", "CENTRAL_SUSPECT"):
                    return "CENTRAL_AUTHORITY_NOT_ACTIVE"
                if int(heartbeat["authority_epoch"]) != int(fence["highest_authority_epoch_seen"]):
                    return "STALE_AUTHORITY_EPOCH"
                if heartbeat["authority_session_id"] != fence["active_authority_session_id"]:
                    return "AUTHORITY_SESSION_MISMATCH"
                if int(heartbeat["heartbeat_seq"]) <= int(fence["heartbeat_seq"]):
                    return "STALE_HEARTBEAT_SEQUENCE"
                db.execute(
                    """UPDATE authority_fences SET authority_state='CENTRAL_ACTIVE',heartbeat_seq=?,action_ready=?,
                       lease_duration_ms=?,last_heartbeat_received_at=?,state_reason='VALID_AUTHORITY_HEARTBEAT',
                       version=version+1 WHERE target_id=?""",
                    (
                        heartbeat["heartbeat_seq"],
                        int(bool(heartbeat["decision_ready"])),
                        int(self.lease_ttl_seconds * 1000),
                        received_at,
                        target_id,
                    ),
                )
                deadline.ensure_valid()
        except AuthorityDeadlineExceeded:
            self.store.set_process_lease_valid(target_id, False)
            self._last_valid_monotonic.pop(target_id, None)
            return "HEARTBEAT_DB_DEADLINE_EXCEEDED"
        except sqlite3.Error as error:
            self.store.set_process_lease_valid(target_id, False)
            raise map_sqlite_failure(error) from error
        self.store.set_process_lease_valid(target_id, True)
        self._last_valid_monotonic[target_id] = self.monotonic()
        return "CENTRAL_ACTIVE"

    def tick(self, target_id: str) -> str:
        try:
            fence = self.store.fence(target_id)
            state = str(fence["authority_state"])
            if self._last_audited_authority_state.get(target_id) != state:
                audit_maintenance_decision(
                    path_id="MP-09",
                    phase="ACTION_PLAN_CREATED",
                    operation="evaluate_local_fallback_eligibility",
                    path_role="LOCAL_FALLBACK",
                    process_service="dell-recovery-agent-shadow.service",
                    resource_identity=f"target/{target_id}/local-authority",
                    correlation_id=f"local-fallback-state-{target_id}-{state}",
                    in_flight_evidence={"status": "MISSING", "source": "authority tick does not read action ledger"},
                    generation_evidence={
                        "status": "CONFIRMED",
                        "authority_epoch": int(fence["highest_authority_epoch_seen"]),
                        "heartbeat_seq": int(fence["heartbeat_seq"]),
                        "source": "Dell authority_fences",
                    },
                )
                self._last_audited_authority_state[target_id] = state
            if state in (
                "AGENT_STARTUP_RECONCILING",
                "LOCAL_FALLBACK",
                "RECONCILING",
                "SAFE_BLOCKED",
                "MAINTENANCE",
            ):
                return state
            last = self._last_valid_monotonic.get(target_id)
            if last is None:
                self.store.set_authority_state(target_id, "CENTRAL_SUSPECT", "NO_MONOTONIC_LEASE")
                return "CENTRAL_SUSPECT"
            elapsed = self.monotonic() - last
            if elapsed >= self.lease_ttl_seconds:
                audit_maintenance_decision(
                    path_id="MP-09",
                    phase="ADMISSION",
                    operation="acquire_local_fallback",
                    path_role="LOCAL_FALLBACK",
                    process_service="dell-recovery-agent-shadow.service",
                    resource_identity=f"target/{target_id}/local-authority",
                    correlation_id=f"lease-expiry-{target_id}-{int(elapsed * 1000)}",
                    in_flight_evidence={"status": "MISSING", "source": "authority tick does not read action ledger"},
                    generation_evidence={
                        "status": "CONFIRMED",
                        "authority_epoch": int(fence["highest_authority_epoch_seen"]),
                        "heartbeat_seq": int(fence["heartbeat_seq"]),
                        "source": "Dell authority_fences",
                    },
                )
                self.store.set_authority_state(target_id, "LOCAL_FALLBACK", "AUTHORITY_LEASE_EXPIRED")
                self.store.begin_local_fallback(target_id)
                return "LOCAL_FALLBACK"
            if elapsed >= self.suspect_after_seconds:
                self.store.set_authority_state(target_id, "CENTRAL_SUSPECT", "AUTHORITY_HEARTBEAT_LATE")
                return "CENTRAL_SUSPECT"
            return state
        except (KeyError, LedgerUnavailable, sqlite3.Error):
            self.store.set_process_lease_valid(target_id, False)
            self._last_valid_monotonic.pop(target_id, None)
            return "SAFE_BLOCKED"

    def arm_reconciliation_grace(self, target_id: str) -> None:
        fence = self.store.fence(target_id)
        if fence["authority_state"] != "CENTRAL_SUSPECT" or fence["state_reason"] != "RECONCILED_AWAITING_VALID_HEARTBEAT":
            raise ValueError("reconciliation grace requires a committed reconciliation")
        self._last_valid_monotonic[target_id] = self.monotonic()

    def reset_after_agent_restart(self, target_id: str) -> None:
        self._last_valid_monotonic.pop(target_id, None)
        self.store.startup_recover(target_id)
