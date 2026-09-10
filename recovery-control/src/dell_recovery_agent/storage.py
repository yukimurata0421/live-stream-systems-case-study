from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from cra_dell_recovery.canonical import payload_digest
from cra_dell_recovery.effect_scope import effect_scope_id, logical_generation_scope_id
from cra_dell_recovery.errors import LedgerUnavailable
from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.sqlite import SQLiteLedger, map_sqlite_failure
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now

if TYPE_CHECKING:
    from dell_recovery_agent.deadline import AuthorityOperationDeadline


class DellStore(SQLiteLedger):
    def __init__(self, path: Path, migration: Path) -> None:
        super().__init__(path, migration)
        self._process_lease_valid_targets: set[str] = set()
        self._process_state_lock = threading.Lock()
        try:
            self._assert_single_target_invariant()
            self._backfill_effect_scopes()
            self._backfill_logical_generation_scopes()
        except BaseException:
            self.close()
            raise

    def _table_exists(self, name: str) -> bool:
        return self.read_one("SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", (name,)) is not None

    def _column_exists(self, table: str, name: str) -> bool:
        return any(str(row[1]) == name for row in self.read_all(f"PRAGMA table_info({table})"))

    def _backfill_logical_generation_scopes(self) -> None:
        if not self._table_exists("effect_scope_fences") or not self._column_exists("effect_scope_fences", "logical_generation_scope_id"):
            return
        rows = self.read_all(
            "SELECT effect_scope_id,target_id,exact_target_json FROM effect_scope_fences ORDER BY created_at,effect_scope_id"
        )
        planned: list[tuple[str, str, str]] = []
        owners: dict[str, tuple[str, str, str]] = {}
        conflict_target: str | None = None
        for row in rows:
            target = TargetIdentity.from_dict(dict(json.loads(str(row["exact_target_json"]))))
            logical_id = logical_generation_scope_id("restart_ffmpeg", target)
            exact = json.dumps(target.to_dict(), separators=(",", ":"), sort_keys=True)
            previous = owners.get(logical_id)
            if previous is not None and previous[1] != exact:
                conflict_target = str(row["target_id"])
                break
            owners[logical_id] = (str(row["effect_scope_id"]), exact, str(row["target_id"]))
            planned.append((logical_id, str(row["effect_scope_id"]), exact))
        if conflict_target is not None:
            with self.write() as db:
                db.execute(
                    """UPDATE authority_fences SET authority_state='SAFE_BLOCKED',action_ready=0,
                       state_reason='GENERATION_PID_INVARIANT_BROKEN',version=version+1 WHERE target_id=?""",
                    (conflict_target,),
                )
            raise LedgerUnavailable("GENERATION_PID_INVARIANT_BROKEN")
        with self.write() as db:
            for logical_id, scope_id, _ in planned:
                db.execute(
                    """UPDATE effect_scope_fences SET logical_generation_scope_id=?
                       WHERE effect_scope_id=? AND logical_generation_scope_id IS NULL""",
                    (logical_id, scope_id),
                )

    def _assert_single_target_invariant(self) -> None:
        """Fail closed if a v1 Dell agent database contains multiple targets."""

        if not self._table_exists("agent_target_binding"):
            return
        rows = self.read_all("SELECT target_id FROM authority_fences ORDER BY target_id")
        binding = self.read_one("SELECT target_id FROM agent_target_binding WHERE singleton_id=1")
        if len(rows) > 1:
            raise LedgerUnavailable("DELL_AGENT_MULTIPLE_TARGETS_UNSUPPORTED")
        if rows and (binding is None or str(binding[0]) != str(rows[0][0])):
            raise LedgerUnavailable("DELL_AGENT_TARGET_BINDING_MISMATCH")
        if not rows and binding is not None:
            raise LedgerUnavailable("DELL_AGENT_TARGET_BINDING_ORPHANED")

    def assert_managed_target(self, target_id: str) -> None:
        if self._table_exists("agent_target_binding"):
            binding = self.read_one("SELECT target_id FROM agent_target_binding WHERE singleton_id=1")
            if binding is None or str(binding[0]) != target_id:
                raise ValueError("DELL_AGENT_CONFIGURED_TARGET_MISMATCH")
            return
        rows = self.read_all("SELECT target_id FROM authority_fences ORDER BY target_id")
        if len(rows) != 1 or str(rows[0][0]) != target_id:
            raise ValueError("DELL_AGENT_CONFIGURED_TARGET_MISMATCH")

    def _backfill_effect_scopes(self) -> None:
        """Fence pre-migration execution history before any new admission."""

        if self.read_one("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='effect_scope_fences'") is None:
            return
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            for row in db.execute(
                """SELECT command_id,target_id,expected_target_json,state FROM agent_commands
                   WHERE state!='REJECTED' ORDER BY received_at"""
            ):
                target = TargetIdentity.from_dict(dict(json.loads(str(row["expected_target_json"]))))
                scope_id = effect_scope_id("restart_ffmpeg", target)
                state = str(row["state"])
                boundary = state in {"EXECUTION_STARTED", "EFFECT_OBSERVED", "EFFECT_FAILED", "OUTCOME_UNKNOWN"}
                db.execute(
                    """INSERT OR IGNORE INTO effect_scope_fences(
                           effect_scope_id,target_id,action,exact_target_json,owner_kind,owner_id,state,
                           effect_boundary_reached,physical_attempt_count,result_json,created_at,updated_at
                       ) VALUES(?,?,'restart_ffmpeg',?,'CENTRAL_COMMAND',?,?,?,?,NULL,?,?)""",
                    (
                        scope_id,
                        row["target_id"],
                        json.dumps(target.to_dict(), separators=(",", ":"), sort_keys=True),
                        row["command_id"],
                        state
                        if state in {"ACCEPTED", "EXECUTION_STARTED", "EFFECT_OBSERVED", "EFFECT_FAILED", "OUTCOME_UNKNOWN"}
                        else "RELEASED_NO_EFFECT",
                        int(boundary),
                        int(boundary),
                        stamp,
                        stamp,
                    ),
                )
                if boundary:
                    db.execute(
                        """UPDATE effect_scope_fences SET effect_boundary_reached=1,physical_attempt_count=1,
                               state=CASE WHEN state='ACCEPTED' THEN 'OUTCOME_UNKNOWN' ELSE state END,
                               updated_at=? WHERE effect_scope_id=?""",
                        (stamp, scope_id),
                    )
            local_rows = list(
                db.execute(
                    """SELECT local_action_id,target_id,target_identity_json,evidence_json,state,updated_at
                       FROM local_actions ORDER BY created_at"""
                )
            )
            for row in local_rows:
                target = TargetIdentity.from_dict(dict(json.loads(str(row["target_identity_json"]))))
                scope_id = effect_scope_id("restart_ffmpeg", target)
                state = str(row["state"])
                boundary = state in {"EXECUTION_STARTED", "EFFECT_OBSERVED", "EFFECT_FAILED", "OUTCOME_UNKNOWN"}
                db.execute(
                    """INSERT OR IGNORE INTO effect_scope_fences(
                           effect_scope_id,target_id,action,exact_target_json,owner_kind,owner_id,state,
                           effect_boundary_reached,physical_attempt_count,result_json,created_at,updated_at
                       ) VALUES(?,?,'restart_ffmpeg',?,'LOCAL_ACTION',?,?,?,?,NULL,?,?)""",
                    (
                        scope_id,
                        row["target_id"],
                        json.dumps(target.to_dict(), separators=(",", ":"), sort_keys=True),
                        row["local_action_id"],
                        state,
                        int(boundary),
                        int(boundary),
                        stamp,
                        stamp,
                    ),
                )
                if boundary:
                    db.execute(
                        """UPDATE effect_scope_fences SET effect_boundary_reached=1,physical_attempt_count=1,
                               state=CASE WHEN state='ACCEPTED' THEN 'OUTCOME_UNKNOWN' ELSE state END,
                               updated_at=? WHERE effect_scope_id=?""",
                        (stamp, scope_id),
                    )
                if state == "ACCEPTED":
                    continue
                journal_state = state
                if (
                    db.execute(
                        "SELECT 1 FROM local_action_journal WHERE local_action_id=? AND record_state=?",
                        (row["local_action_id"], journal_state),
                    ).fetchone()
                    is None
                ):
                    self.append_local_journal(
                        db,
                        local_action_id=str(row["local_action_id"]),
                        record_state=journal_state,
                        before_target=target,
                        after_target=None,
                        evidence=dict(json.loads(str(row["evidence_json"]))),
                        reason_code=f"MIGRATED_LEGACY_{state}",
                        stamp=str(row["updated_at"]),
                    )

    def set_process_lease_valid(self, target_id: str, valid: bool) -> None:
        with self._process_state_lock:
            if valid:
                self._process_lease_valid_targets.add(target_id)
            else:
                self._process_lease_valid_targets.discard(target_id)

    def process_lease_valid(self, target_id: str) -> bool:
        with self._process_state_lock:
            return target_id in self._process_lease_valid_targets

    def has_identity(self) -> bool:
        try:
            row = self.read_one("SELECT count(*) FROM agent_identity")
        except sqlite3.Error as error:
            raise map_sqlite_failure(error) from error
        return row is not None and int(row[0]) > 0

    def bootstrap(
        self,
        *,
        agent_id: str,
        installation_id: str,
        host_id: str,
        host_boot_id: str,
        target_id: str,
        policy_revision: str = "shadow-policy-v1",
    ) -> None:
        stamp = isoformat_utc(utc_now())
        policy = {
            "policy_revision": policy_revision,
            "action": "restart_ffmpeg",
            "minimum_action_interval_sec": 60,
            "hourly_action_cost_limit_sec": 120,
            "daily_action_cost_limit_sec": 600,
            "local_fallback_enabled": True,
            "environment": "NO_ACTION_SHADOW",
        }
        with self.write() as db:
            if self._table_exists("agent_target_binding"):
                db.execute(
                    "INSERT INTO agent_target_binding VALUES (1,?,?)",
                    (target_id, stamp),
                )
            db.execute(
                "INSERT INTO agent_identity VALUES (1,?,?,?,?,?,'HEALTHY',?,?,0)",
                (
                    agent_id,
                    installation_id,
                    host_id,
                    str(uuid.uuid4()),
                    host_boot_id,
                    stamp,
                    stamp,
                ),
            )
            if self._column_exists("authority_fences", "action_ready"):
                db.execute(
                    """INSERT INTO authority_fences(
                           target_id,authority_state,highest_authority_epoch_seen,
                           active_authority_session_id,active_controller_instance_id,
                           highest_command_seq_consumed,heartbeat_seq,lease_duration_ms,
                           last_heartbeat_received_at,state_reason,last_reconciled_at,version,
                           action_ready
                       ) VALUES (?,'AGENT_STARTUP_RECONCILING',0,NULL,NULL,0,0,15000,NULL,?,NULL,0,0)""",
                    (target_id, "AGENT_PROCESS_START_REQUIRES_RECONCILIATION"),
                )
            else:
                db.execute(
                    "INSERT INTO authority_fences VALUES (?,'AGENT_STARTUP_RECONCILING',0,NULL,NULL,0,0,15000,NULL,?,NULL,0)",
                    (target_id, "AGENT_PROCESS_START_REQUIRES_RECONCILIATION"),
                )
            db.execute(
                """INSERT INTO agent_safety_policies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    "shadow-policy",
                    target_id,
                    policy_revision,
                    "restart_ffmpeg",
                    60,
                    5,
                    120,
                    600,
                    1,
                    json.dumps(policy, separators=(",", ":"), sort_keys=True),
                    "test-operator-key-not-a-private-key",
                    "TEST_ONLY_NOT_PRODUCTION_SIGNATURE",
                    stamp,
                    "2999-01-01T00:00:00.000Z",
                    stamp,
                ),
            )

    def fence(self, target_id: str) -> sqlite3.Row:
        try:
            row = self.read_one("SELECT * FROM authority_fences WHERE target_id=?", (target_id,))
        except sqlite3.Error as error:
            raise map_sqlite_failure(error) from error
        if row is None:
            raise KeyError(target_id)
        return row

    def agent_identity(self) -> sqlite3.Row:
        try:
            row = self.read_one("SELECT * FROM agent_identity WHERE singleton_id=1")
        except sqlite3.Error as error:
            raise map_sqlite_failure(error) from error
        if row is None:
            raise KeyError("agent identity")
        return row

    def shadow_status_snapshot(self) -> tuple[sqlite3.Row, int]:
        """Read the authority row and status counter from one WAL snapshot."""

        try:
            with self.read() as db:
                fence = db.execute("SELECT * FROM authority_fences ORDER BY target_id LIMIT 1").fetchone()
                count = db.execute("SELECT count(*) FROM agent_events WHERE event_type='SHADOW_COMMAND_EVALUATED'").fetchone()
        except sqlite3.Error as error:
            raise map_sqlite_failure(error) from error
        if fence is None or count is None:
            raise LedgerUnavailable("SQLITE_CRITICAL_STATUS_SNAPSHOT_INCOMPLETE")
        return fence, int(count[0])

    def set_authority_state(self, target_id: str, state: str, reason: str) -> bool:
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            old = db.execute(
                "SELECT authority_state,state_reason FROM authority_fences WHERE target_id=?",
                (target_id,),
            ).fetchone()
            if old is None:
                raise KeyError(target_id)
            if old[0] == state and old[1] == reason:
                return False
            if self._column_exists("authority_fences", "action_ready"):
                db.execute(
                    """UPDATE authority_fences SET authority_state=?,state_reason=?,
                           action_ready=CASE WHEN ?='CENTRAL_ACTIVE' THEN action_ready ELSE 0 END,
                           version=version+1 WHERE target_id=?""",
                    (state, reason, state, target_id),
                )
            else:
                db.execute(
                    "UPDATE authority_fences SET authority_state=?,state_reason=?,version=version+1 WHERE target_id=?",
                    (state, reason, target_id),
                )
            db.execute(
                "INSERT INTO agent_transitions VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    f"transition-{uuid.uuid4()}",
                    target_id,
                    None,
                    None,
                    old[0],
                    state,
                    reason,
                    None,
                    stamp,
                ),
            )
        if state != "CENTRAL_ACTIVE":
            self.set_process_lease_valid(target_id, False)
        return True

    def install_reconciliation(
        self,
        *,
        target_id: str,
        reconciliation_id: str,
        challenge_id: str,
        nonce: str,
        new_epoch: int,
        session_id: str,
        controller_instance_id: str,
    ) -> None:
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            self._install_reconciliation_in_transaction(
                db,
                target_id=target_id,
                reconciliation_id=reconciliation_id,
                challenge_id=challenge_id,
                nonce=nonce,
                new_epoch=new_epoch,
                session_id=session_id,
                controller_instance_id=controller_instance_id,
                stamp=stamp,
            )
        self.set_process_lease_valid(target_id, False)

    def _install_reconciliation_in_transaction(
        self,
        db: sqlite3.Connection,
        *,
        target_id: str,
        reconciliation_id: str,
        challenge_id: str,
        nonce: str,
        new_epoch: int,
        session_id: str,
        controller_instance_id: str,
        stamp: str,
    ) -> None:
        fence = db.execute(
            "SELECT highest_authority_epoch_seen FROM authority_fences WHERE target_id=?",
            (target_id,),
        ).fetchone()
        if fence is None or new_epoch <= int(fence[0]):
            raise ValueError("new epoch must exceed Dell high-water")
        existing = db.execute(
            "SELECT reconciliation_id FROM reconciliation_sessions WHERE reconciliation_id=?",
            (reconciliation_id,),
        ).fetchone()
        if existing is None:
            db.execute(
                """INSERT INTO reconciliation_sessions VALUES (?,?,?,?,?,?,?,'COMMITTED',?,?,?)""",
                (
                    reconciliation_id,
                    target_id,
                    challenge_id,
                    hashlib.sha256(nonce.encode()).hexdigest(),
                    new_epoch,
                    session_id,
                    controller_instance_id,
                    stamp,
                    "2999-01-01T00:00:00.000Z",
                    stamp,
                ),
            )
        else:
            db.execute(
                """UPDATE reconciliation_sessions SET proposed_authority_epoch=?,
                   proposed_authority_session_id=?,proposed_controller_instance_id=?,
                   state='COMMITTED',completed_at=? WHERE reconciliation_id=?""",
                (new_epoch, session_id, controller_instance_id, stamp, reconciliation_id),
            )
        db.execute(
            """UPDATE authority_fences SET authority_state='CENTRAL_SUSPECT',
               highest_authority_epoch_seen=?,active_authority_session_id=?,
               active_controller_instance_id=?,highest_command_seq_consumed=0,heartbeat_seq=0,
               action_ready=0,last_heartbeat_received_at=NULL,state_reason='RECONCILED_AWAITING_VALID_HEARTBEAT',
               last_reconciled_at=?,version=version+1 WHERE target_id=?""",
            (new_epoch, session_id, controller_instance_id, stamp, target_id),
        )
        db.execute(
            "UPDATE local_fallback_sessions SET state='CLOSED',ended_at=? WHERE target_id=? AND state IN ('ACTIVE','RECONCILING')",
            (stamp, target_id),
        )

    def commit_reconciliation(
        self,
        *,
        target_id: str,
        reconciliation_id: str,
        challenge_id: str,
        nonce: str,
        agent_installation_id: str,
        new_epoch: int,
        session_id: str,
        controller_instance_id: str,
        journal_ack_sequence: int,
        journal_ack_record_digest: str,
        deadline: AuthorityOperationDeadline | None = None,
    ) -> None:
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            if deadline is not None:
                deadline.ensure_valid()
            challenge = db.execute(
                "SELECT * FROM reconciliation_sessions WHERE reconciliation_id=? AND challenge_id=?",
                (reconciliation_id, challenge_id),
            ).fetchone()
            identity = db.execute("SELECT agent_installation_id FROM agent_identity WHERE singleton_id=1").fetchone()
            if challenge is None or challenge["state"] != "CHALLENGE_ISSUED":
                raise ValueError("challenge is not active")
            if hashlib.sha256(nonce.encode()).hexdigest() != challenge["challenge_nonce_hash"]:
                raise ValueError("challenge nonce mismatch")
            if parse_utc(str(challenge["expires_at"])) <= utc_now():
                raise ValueError("reconciliation challenge expired")
            if identity is None or agent_installation_id != identity["agent_installation_id"]:
                raise ValueError("agent installation mismatch")
            unresolved = db.execute(
                """SELECT
                       (SELECT count(*) FROM agent_commands WHERE target_id=? AND state IN
                           ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN')) +
                       (SELECT count(*) FROM local_actions WHERE target_id=? AND state IN
                           ('ACCEPTED','EXECUTION_STARTED','OUTCOME_UNKNOWN'))""",
                (target_id, target_id),
            ).fetchone()
            if unresolved is None or int(unresolved[0]):
                raise ValueError("unresolved action blocks reconciliation")
            journal = db.execute(
                """SELECT journal_sequence,record_digest FROM local_action_journal
                   WHERE target_id=? ORDER BY journal_sequence DESC LIMIT 1""",
                (target_id,),
            ).fetchone()
            expected_sequence = 0 if journal is None else int(journal["journal_sequence"])
            expected_digest = "0" * 64 if journal is None else str(journal["record_digest"])
            if journal_ack_sequence != expected_sequence or journal_ack_record_digest != expected_digest:
                raise ValueError("journal acknowledgement mismatch")
            db.execute(
                """INSERT INTO local_action_journal_acks VALUES (?,?,?,?,?)
                   ON CONFLICT(target_id) DO UPDATE SET
                       reconciliation_id=excluded.reconciliation_id,
                       ack_sequence=excluded.ack_sequence,
                       ack_record_digest=excluded.ack_record_digest,
                       acknowledged_at=excluded.acknowledged_at""",
                (target_id, reconciliation_id, expected_sequence, expected_digest, stamp),
            )
            self._install_reconciliation_in_transaction(
                db,
                target_id=target_id,
                reconciliation_id=reconciliation_id,
                challenge_id=challenge_id,
                nonce=nonce,
                new_epoch=new_epoch,
                session_id=session_id,
                controller_instance_id=controller_instance_id,
                stamp=stamp,
            )
            if deadline is not None:
                deadline.ensure_valid()
        self.set_process_lease_valid(target_id, False)

    def startup_recover(self, target_id: str) -> None:
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            accepted_commands = [
                str(row[0])
                for row in db.execute(
                    "SELECT command_id FROM agent_commands WHERE target_id=? AND state='ACCEPTED'",
                    (target_id,),
                )
            ]
            started_commands = [
                str(row[0])
                for row in db.execute(
                    "SELECT command_id FROM agent_commands WHERE target_id=? AND state='EXECUTION_STARTED'",
                    (target_id,),
                )
            ]
            started_local = list(
                db.execute(
                    """SELECT local_action_id,target_identity_json,evidence_json FROM local_actions
                       WHERE target_id=? AND state IN ('ACCEPTED','EXECUTION_STARTED')""",
                    (target_id,),
                )
            )
            db.execute(
                """UPDATE agent_commands SET state='SUPERSEDED_AFTER_AGENT_RESTART',
                   terminal_reason_code='SUPERSEDED_AFTER_AGENT_RESTART',updated_at=? WHERE state='ACCEPTED'""",
                (stamp,),
            )
            db.execute(
                """UPDATE agent_commands SET state='OUTCOME_UNKNOWN',terminal_reason_code='AGENT_RESTART_AFTER_EXECUTION_STARTED',
                   updated_at=? WHERE state='EXECUTION_STARTED'""",
                (stamp,),
            )
            db.execute(
                """UPDATE execution_attempts SET state='OUTCOME_UNKNOWN',finished_at=?,
                   outcome_reason_code='AGENT_RESTART_AFTER_EXECUTION_STARTED' WHERE state='STARTED'""",
                (stamp,),
            )
            db.execute(
                """UPDATE local_actions SET state='OUTCOME_UNKNOWN',updated_at=?
                   WHERE target_id=? AND state IN ('ACCEPTED','EXECUTION_STARTED')""",
                (stamp, target_id),
            )
            db.execute(
                """UPDATE authority_fences SET authority_state='AGENT_STARTUP_RECONCILING',
                   state_reason='MONOTONIC_LEASE_NOT_RESTORED',action_ready=0,last_heartbeat_received_at=NULL,
                   version=version+1 WHERE target_id=?""",
                (target_id,),
            )
            for command_id in accepted_commands:
                if db.execute("SELECT 1 FROM effect_scope_fences WHERE owner_id=?", (command_id,)).fetchone() is not None:
                    self.transition_effect_scope(
                        db,
                        owner_id=command_id,
                        state="RELEASED_NO_EFFECT",
                        reason="AGENT_RESTART_BEFORE_EXECUTION_STARTED",
                        evidence={"physical_attempt_count": 0},
                        stamp=stamp,
                    )
            for command_id in started_commands:
                if db.execute("SELECT 1 FROM effect_scope_fences WHERE owner_id=?", (command_id,)).fetchone() is not None:
                    self.transition_effect_scope(
                        db,
                        owner_id=command_id,
                        state="OUTCOME_UNKNOWN",
                        reason="AGENT_RESTART_AFTER_EXECUTION_STARTED",
                        evidence={"automatic_retry": False},
                        stamp=stamp,
                    )
            for action in started_local:
                local_action_id = str(action["local_action_id"])
                if db.execute("SELECT 1 FROM effect_scope_fences WHERE owner_id=?", (local_action_id,)).fetchone() is None:
                    continue
                self.transition_effect_scope(
                    db,
                    owner_id=local_action_id,
                    state="OUTCOME_UNKNOWN",
                    reason="AGENT_RESTART_AFTER_LOCAL_EXECUTION_STARTED",
                    evidence={"automatic_retry": False},
                    stamp=stamp,
                )
                if (
                    db.execute(
                        "SELECT 1 FROM local_action_journal WHERE local_action_id=? AND record_state='OUTCOME_UNKNOWN'",
                        (local_action_id,),
                    ).fetchone()
                    is None
                ):
                    self.append_local_journal(
                        db,
                        local_action_id=local_action_id,
                        record_state="OUTCOME_UNKNOWN",
                        before_target=TargetIdentity.from_dict(json.loads(str(action["target_identity_json"]))),
                        after_target=None,
                        evidence=dict(json.loads(str(action["evidence_json"]))),
                        reason_code="AGENT_RESTART_AFTER_LOCAL_EXECUTION_STARTED",
                        stamp=stamp,
                    )
        self.set_process_lease_valid(target_id, False)

    def begin_local_fallback(self, target_id: str) -> str:
        session_id = f"local-{uuid.uuid4()}"
        stamp = isoformat_utc(utc_now())
        with self.write() as db:
            fence = db.execute("SELECT * FROM authority_fences WHERE target_id=?", (target_id,)).fetchone()
            identity = db.execute("SELECT * FROM agent_identity WHERE singleton_id=1").fetchone()
            if fence is None or identity is None:
                raise LedgerUnavailable("SQLITE_LOCAL_FALLBACK_SNAPSHOT_INCOMPLETE")
            existing = db.execute(
                "SELECT local_session_id FROM local_fallback_sessions WHERE target_id=? AND state='ACTIVE'",
                (target_id,),
            ).fetchone()
            if existing is not None:
                return str(existing[0])
            db.execute(
                "INSERT INTO local_fallback_sessions VALUES (?,?,?,?,?,'ACTIVE',?,NULL)",
                (
                    session_id,
                    target_id,
                    identity["last_host_boot_id"],
                    fence["highest_authority_epoch_seen"],
                    "shadow-policy-v1",
                    stamp,
                ),
            )
        return session_id

    def attempt_count(self, command_id: str | None = None) -> int:
        if command_id is None:
            row = self.read_one("SELECT count(*) FROM execution_attempts")
        else:
            row = self.read_one("SELECT count(*) FROM execution_attempts WHERE command_id=?", (command_id,))
        if row is None:
            raise LedgerUnavailable("SQLITE_ATTEMPT_COUNT_NO_RESULT")
        return int(row[0])

    def claim_effect_scope(
        self,
        db: sqlite3.Connection,
        *,
        target_id: str,
        target: TargetIdentity,
        owner_kind: str,
        owner_id: str,
        stamp: str,
    ) -> tuple[bool, str, str]:
        scope_id = effect_scope_id("restart_ffmpeg", target)
        logical_id = logical_generation_scope_id("restart_ffmpeg", target)
        has_logical_scope = self._column_exists("effect_scope_fences", "logical_generation_scope_id")
        if has_logical_scope:
            existing = db.execute(
                """SELECT owner_id,state,effect_scope_id,exact_target_json FROM effect_scope_fences
                   WHERE logical_generation_scope_id=?""",
                (logical_id,),
            ).fetchone()
        else:
            existing = db.execute(
                """SELECT owner_id,state,effect_scope_id,exact_target_json FROM effect_scope_fences
                   WHERE effect_scope_id=?""",
                (scope_id,),
            ).fetchone()
        encoded_target = json.dumps(target.to_dict(), separators=(",", ":"), sort_keys=True)
        if existing is not None and str(existing["exact_target_json"]) != encoded_target:
            return False, str(existing["effect_scope_id"]), "GENERATION_PID_INVARIANT_BROKEN"
        if existing is not None and str(existing["owner_id"]) == owner_id:
            return True, str(existing["effect_scope_id"]), str(existing["state"])
        if existing is not None and str(existing["state"]) != "RELEASED_NO_EFFECT":
            return False, str(existing["effect_scope_id"]), str(existing["state"])
        if existing is None:
            if has_logical_scope:
                db.execute(
                    """INSERT INTO effect_scope_fences(
                           effect_scope_id,target_id,action,exact_target_json,owner_kind,owner_id,
                           state,effect_boundary_reached,physical_attempt_count,result_json,created_at,updated_at,
                           logical_generation_scope_id
                       ) VALUES(?,?, 'restart_ffmpeg',?,?,?,'ACCEPTED',0,0,NULL,?,?,?)""",
                    (scope_id, target_id, encoded_target, owner_kind, owner_id, stamp, stamp, logical_id),
                )
            else:
                db.execute(
                    """INSERT INTO effect_scope_fences(
                           effect_scope_id,target_id,action,exact_target_json,owner_kind,owner_id,
                           state,effect_boundary_reached,physical_attempt_count,result_json,created_at,updated_at
                       ) VALUES(?,?, 'restart_ffmpeg',?,?,?,'ACCEPTED',0,0,NULL,?,?)""",
                    (scope_id, target_id, encoded_target, owner_kind, owner_id, stamp, stamp),
                )
            from_state: str | None = None
        else:
            db.execute(
                """UPDATE effect_scope_fences SET owner_kind=?,owner_id=?,state='ACCEPTED',
                       exact_target_json=?,effect_boundary_reached=0,physical_attempt_count=0,
                       result_json=NULL,updated_at=?
                   WHERE effect_scope_id=? AND state='RELEASED_NO_EFFECT'""",
                (owner_kind, owner_id, encoded_target, stamp, existing["effect_scope_id"]),
            )
            scope_id = str(existing["effect_scope_id"])
            from_state = "RELEASED_NO_EFFECT"
        self._append_scope_event(
            db,
            scope_id=scope_id,
            from_state=from_state,
            to_state="ACCEPTED",
            reason="EFFECT_SCOPE_CLAIMED",
            evidence={"owner_kind": owner_kind, "owner_id": owner_id},
            stamp=stamp,
        )
        return True, scope_id, "ACCEPTED"

    def generation_scope_conflict(self, target: TargetIdentity) -> sqlite3.Row | None:
        """Return a same-generation fence whose exact PID snapshot differs."""

        if not self._column_exists("effect_scope_fences", "logical_generation_scope_id"):
            return None
        row = self.read_one(
            """SELECT * FROM effect_scope_fences WHERE logical_generation_scope_id=?""",
            (logical_generation_scope_id("restart_ffmpeg", target),),
        )
        if row is None:
            return None
        encoded = json.dumps(target.to_dict(), separators=(",", ":"), sort_keys=True)
        return row if str(row["exact_target_json"]) != encoded else None

    def transition_effect_scope(
        self,
        db: sqlite3.Connection,
        *,
        owner_id: str,
        state: str,
        reason: str,
        evidence: dict[str, object] | None,
        stamp: str,
        effect_boundary: bool = False,
    ) -> str:
        row = db.execute(
            "SELECT effect_scope_id,state,effect_boundary_reached,physical_attempt_count FROM effect_scope_fences WHERE owner_id=?",
            (owner_id,),
        ).fetchone()
        if row is None:
            raise LedgerUnavailable("EFFECT_SCOPE_MISSING")
        if effect_boundary and (int(row["effect_boundary_reached"]) or int(row["physical_attempt_count"])):
            raise LedgerUnavailable("EFFECT_SCOPE_PHYSICAL_ATTEMPT_ALREADY_RESERVED")
        encoded = json.dumps(evidence, separators=(",", ":"), sort_keys=True) if evidence is not None else None
        db.execute(
            """UPDATE effect_scope_fences SET state=?,effect_boundary_reached=?,physical_attempt_count=?,
                   result_json=coalesce(?,result_json),updated_at=? WHERE owner_id=?""",
            (
                state,
                1 if effect_boundary else int(row["effect_boundary_reached"]),
                1 if effect_boundary else int(row["physical_attempt_count"]),
                encoded,
                stamp,
                owner_id,
            ),
        )
        self._append_scope_event(
            db,
            scope_id=str(row["effect_scope_id"]),
            from_state=str(row["state"]),
            to_state=state,
            reason=reason,
            evidence=evidence or {},
            stamp=stamp,
        )
        return str(row["effect_scope_id"])

    @staticmethod
    def _append_scope_event(
        db: sqlite3.Connection,
        *,
        scope_id: str,
        from_state: str | None,
        to_state: str,
        reason: str,
        evidence: dict[str, object],
        stamp: str,
    ) -> None:
        db.execute(
            "INSERT INTO effect_scope_events VALUES (?,?,?,?,?,?,?)",
            (
                f"scope-event-{uuid.uuid4()}",
                scope_id,
                from_state,
                to_state,
                reason,
                json.dumps(evidence, separators=(",", ":"), sort_keys=True),
                stamp,
            ),
        )

    def append_local_journal(
        self,
        db: sqlite3.Connection,
        *,
        local_action_id: str,
        record_state: str,
        before_target: TargetIdentity,
        after_target: TargetIdentity | None,
        evidence: dict[str, object],
        reason_code: str,
        stamp: str,
    ) -> int:
        action = db.execute(
            "SELECT local_session_id,target_id FROM local_actions WHERE local_action_id=?",
            (local_action_id,),
        ).fetchone()
        if action is None:
            raise LedgerUnavailable("LOCAL_JOURNAL_BINDING_MISSING")
        target_scope = effect_scope_id("restart_ffmpeg", before_target)
        scope = db.execute(
            "SELECT effect_scope_id,physical_attempt_count FROM effect_scope_fences WHERE effect_scope_id=?",
            (target_scope,),
        ).fetchone()
        if scope is None:
            raise LedgerUnavailable("LOCAL_JOURNAL_EFFECT_SCOPE_MISSING")
        previous = db.execute("SELECT record_digest FROM local_action_journal ORDER BY journal_sequence DESC LIMIT 1").fetchone()
        previous_digest = "0" * 64 if previous is None else str(previous[0])
        payload: dict[str, object] = {
            "local_action_id": local_action_id,
            "local_session_id": str(action["local_session_id"]),
            "target_id": str(action["target_id"]),
            "effect_scope_id": str(scope["effect_scope_id"]),
            "record_state": record_state,
            "physical_attempt_count": int(scope["physical_attempt_count"]),
            "before_target": before_target.to_dict(),
            "after_target": None if after_target is None else after_target.to_dict(),
            "evidence_digest": payload_digest(evidence),
            "reason_code": reason_code,
            "recorded_at": stamp,
            "previous_digest": previous_digest,
        }
        record_digest = payload_digest(payload)
        cursor = db.execute(
            """INSERT INTO local_action_journal(
                   local_action_id,local_session_id,target_id,effect_scope_id,record_state,
                   previous_digest,record_digest,payload_json,recorded_at
               ) VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                local_action_id,
                action["local_session_id"],
                action["target_id"],
                scope["effect_scope_id"],
                record_state,
                previous_digest,
                record_digest,
                json.dumps(payload, separators=(",", ":"), sort_keys=True),
                stamp,
            ),
        )
        if cursor.lastrowid is None:
            raise LedgerUnavailable("LOCAL_JOURNAL_SEQUENCE_MISSING")
        return int(cursor.lastrowid)

    def local_journal_high_water(self, target_id: str) -> tuple[int, str]:
        row = self.read_one(
            """SELECT journal_sequence,record_digest FROM local_action_journal
               WHERE target_id=? ORDER BY journal_sequence DESC LIMIT 1""",
            (target_id,),
        )
        return (0, "0" * 64) if row is None else (int(row[0]), str(row[1]))

    def local_journal_rows(self, target_id: str, *, after_sequence: int, limit: int) -> tuple[sqlite3.Row, ...]:
        if not 1 <= limit <= 200:
            raise ValueError("journal page limit must be between 1 and 200")
        return self.read_all(
            """SELECT * FROM local_action_journal WHERE target_id=? AND journal_sequence>?
               ORDER BY journal_sequence LIMIT ?""",
            (target_id, after_sequence, limit),
        )

    def acknowledge_local_journal(
        self,
        db: sqlite3.Connection,
        *,
        target_id: str,
        reconciliation_id: str,
        sequence: int,
        record_digest: str,
        stamp: str,
    ) -> None:
        high_sequence, high_digest = self.local_journal_high_water(target_id)
        if sequence != high_sequence or record_digest != high_digest:
            raise ValueError("journal acknowledgement does not match Dell high-water")
        db.execute(
            """INSERT INTO local_action_journal_acks VALUES (?,?,?,?,?)
               ON CONFLICT(target_id) DO UPDATE SET
                   reconciliation_id=excluded.reconciliation_id,
                   ack_sequence=excluded.ack_sequence,
                   ack_record_digest=excluded.ack_record_digest,
                   acknowledged_at=excluded.acknowledged_at""",
            (target_id, reconciliation_id, sequence, record_digest, stamp),
        )
