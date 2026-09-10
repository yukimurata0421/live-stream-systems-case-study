from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.time import parse_utc
from cra_harness.maintenance.model import (
    REQUIRED_MUTATORS,
    FenceReleaseAck,
    FenceReleaseState,
    MaintenanceExecutorId,
    MaintenanceIntent,
    MaintenanceMutationAuthorization,
    MaintenanceMutationOperation,
    MaintenanceState,
    MutationAuthorizationState,
    MutatorId,
    QuiesceAck,
)

if TYPE_CHECKING:
    from dell_recovery_agent.deadline import AuthorityOperationDeadline


class MaintenanceStore:
    """Harness-only durable maintenance truth; never opened by production services."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS maintenance_transactions (
                maintenance_id TEXT PRIMARY KEY,
                intent_json TEXT NOT NULL,
                generation INTEGER NOT NULL,
                state TEXT NOT NULL,
                authority_state TEXT NOT NULL,
                old_target_json TEXT NOT NULL,
                new_target_json TEXT,
                authority_epoch INTEGER NOT NULL,
                authority_session_id TEXT NOT NULL,
                fresh_heartbeat INTEGER NOT NULL DEFAULT 0,
                planned_would_mutate_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS maintenance_fences (
                maintenance_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                mutator_id TEXT NOT NULL,
                target_identity_json TEXT NOT NULL,
                active INTEGER NOT NULL,
                release_state TEXT NOT NULL,
                PRIMARY KEY (maintenance_id, mutator_id)
            );
            CREATE TABLE IF NOT EXISTS quiesce_acks (
                maintenance_id TEXT NOT NULL,
                mutator_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                ack_json TEXT NOT NULL,
                accepted INTEGER NOT NULL,
                reject_reason TEXT,
                PRIMARY KEY (maintenance_id, mutator_id)
            );
            CREATE TABLE IF NOT EXISTS maintenance_mutation_authorizations (
                authorization_id TEXT PRIMARY KEY,
                maintenance_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                authorization_json TEXT NOT NULL,
                state TEXT NOT NULL,
                use_count INTEGER NOT NULL DEFAULT 0,
                accepted_at TEXT,
                execution_started_at TEXT,
                terminal_at TEXT,
                reason_code TEXT,
                UNIQUE (maintenance_id, generation, sequence)
            );
            CREATE TABLE IF NOT EXISTS maintenance_mutation_executions (
                execution_id TEXT PRIMARY KEY,
                authorization_id TEXT NOT NULL UNIQUE,
                maintenance_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                state TEXT NOT NULL,
                source_target_json TEXT NOT NULL,
                observed_target_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                reason_code TEXT
            );
            CREATE TABLE IF NOT EXISTS fence_release_acks (
                maintenance_id TEXT NOT NULL,
                mutator_id TEXT NOT NULL,
                generation INTEGER NOT NULL,
                ack_json TEXT NOT NULL,
                accepted INTEGER NOT NULL,
                reject_reason TEXT,
                PRIMARY KEY (maintenance_id, mutator_id)
            );
            CREATE TABLE IF NOT EXISTS maintenance_events (
                event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                maintenance_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            """
        )

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield self.connection
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def create(self, intent: MaintenanceIntent) -> None:
        with self.write() as db:
            db.execute(
                "INSERT INTO maintenance_transactions VALUES (?,?,?,?,?,?,?,?,?,?,0)",
                (
                    intent.maintenance_id,
                    json.dumps(intent.to_dict(), sort_keys=True, separators=(",", ":")),
                    intent.generation,
                    MaintenanceState.REQUESTED.value,
                    "CENTRAL_ACTIVE",
                    json.dumps(intent.target_identity.to_dict(), sort_keys=True, separators=(",", ":")),
                    None,
                    intent.authority_epoch,
                    intent.authority_session_id,
                    0,
                ),
            )
            self._event(intent.maintenance_id, "MAINTENANCE_REQUESTED", {"generation": intent.generation})

    def row(self, maintenance_id: str) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM maintenance_transactions WHERE maintenance_id=?", (maintenance_id,)).fetchone()
        if row is None:
            raise KeyError(maintenance_id)
        return cast(sqlite3.Row, row)

    def intent(self, maintenance_id: str) -> MaintenanceIntent:
        value = json.loads(str(self.row(maintenance_id)["intent_json"]))
        value["target_identity"] = TargetIdentity.from_dict(value["target_identity"])
        return MaintenanceIntent(**value)

    def transition(self, maintenance_id: str, state: MaintenanceState, *, authority_state: str | None = None) -> None:
        with self.write() as db:
            if authority_state is None:
                db.execute("UPDATE maintenance_transactions SET state=? WHERE maintenance_id=?", (state.value, maintenance_id))
            else:
                db.execute(
                    "UPDATE maintenance_transactions SET state=?,authority_state=? WHERE maintenance_id=?",
                    (state.value, authority_state, maintenance_id),
                )
            self._event(
                maintenance_id,
                "STATE_TRANSITION",
                {"state": state.value, "authority_state": authority_state},
            )

    def install_fence(self, maintenance_id: str, generation: int, mutator_id: MutatorId, target: TargetIdentity) -> None:
        with self.write() as db:
            db.execute(
                "INSERT OR REPLACE INTO maintenance_fences VALUES (?,?,?,?,1,?)",
                (
                    maintenance_id,
                    generation,
                    mutator_id.value,
                    json.dumps(target.to_dict(), sort_keys=True, separators=(",", ":")),
                    FenceReleaseState.HELD.value,
                ),
            )
            self._event(maintenance_id, "PERSISTENT_FENCE_INSTALLED", {"mutator_id": mutator_id.value, "generation": generation})

    def fence_active(self, maintenance_id: str, mutator_id: MutatorId, generation: int) -> bool:
        row = self.connection.execute(
            "SELECT active,generation FROM maintenance_fences WHERE maintenance_id=? AND mutator_id=?",
            (maintenance_id, mutator_id.value),
        ).fetchone()
        return bool(row is not None and row["active"] and int(row["generation"]) == generation)

    def record_ack(self, maintenance_id: str, ack: QuiesceAck, *, accepted: bool, reason: str = "") -> None:
        with self.write() as db:
            db.execute(
                "INSERT OR REPLACE INTO quiesce_acks VALUES (?,?,?,?,?,?)",
                (
                    maintenance_id,
                    ack.mutator_id.value,
                    ack.generation,
                    json.dumps(ack.to_dict(), sort_keys=True, separators=(",", ":")),
                    int(accepted),
                    reason or None,
                ),
            )
            self._event(
                maintenance_id,
                "QUIESCE_ACK_ACCEPTED" if accepted else "QUIESCE_ACK_REJECTED",
                {"mutator_id": ack.mutator_id.value, "generation": ack.generation, "reason": reason},
            )

    def accepted_acks(self, maintenance_id: str) -> dict[str, dict[str, Any]]:
        return {
            str(row["mutator_id"]): json.loads(str(row["ack_json"]))
            for row in self.connection.execute("SELECT * FROM quiesce_acks WHERE maintenance_id=? AND accepted=1", (maintenance_id,))
        }

    def all_required_fences_active(self, maintenance_id: str, generation: int) -> bool:
        row = self.connection.execute(
            """SELECT count(*) FROM maintenance_fences
               WHERE maintenance_id=? AND generation=? AND active=1 AND release_state IN (?,?)""",
            (
                maintenance_id,
                generation,
                FenceReleaseState.HELD.value,
                FenceReleaseState.RELEASE_PREPARED.value,
            ),
        ).fetchone()
        return bool(row is not None and int(row[0]) == len(REQUIRED_MUTATORS))

    def issue_authorization(
        self,
        authorization: MaintenanceMutationAuthorization,
        *,
        now: str,
        deadline: AuthorityOperationDeadline | None = None,
    ) -> str:
        with self.write() as db:
            if deadline is not None:
                deadline.ensure_valid()
            transaction = db.execute(
                "SELECT * FROM maintenance_transactions WHERE maintenance_id=?",
                (authorization.maintenance_id,),
            ).fetchone()
            expected_sequence = int(
                db.execute(
                    "SELECT coalesce(max(sequence),0)+1 FROM maintenance_mutation_authorizations WHERE maintenance_id=?",
                    (authorization.maintenance_id,),
                ).fetchone()[0]
            )
            accepted_ack_count = int(
                db.execute(
                    "SELECT count(*) FROM quiesce_acks WHERE maintenance_id=? AND accepted=1",
                    (authorization.maintenance_id,),
                ).fetchone()[0]
            )
            active_fence_count = int(
                db.execute(
                    """SELECT count(*) FROM maintenance_fences
                       WHERE maintenance_id=? AND generation=? AND active=1 AND release_state=?""",
                    (
                        authorization.maintenance_id,
                        authorization.generation,
                        FenceReleaseState.HELD.value,
                    ),
                ).fetchone()[0]
            )
            reason = ""
            if transaction is None:
                reason = "MAINTENANCE_NOT_FOUND"
            elif str(transaction["state"]) != MaintenanceState.ESTABLISHED.value:
                reason = "MAINTENANCE_NOT_ESTABLISHED"
            elif authorization.generation != int(transaction["generation"]):
                reason = "AUTHORIZATION_GENERATION_MISMATCH"
            elif authorization.source_target_identity != TargetIdentity.from_dict(json.loads(str(transaction["old_target_json"]))):
                reason = "AUTHORIZATION_SOURCE_TARGET_MISMATCH"
            elif authorization.issued_authority_epoch != int(transaction["authority_epoch"]):
                reason = "AUTHORIZATION_AUTHORITY_EPOCH_MISMATCH"
            elif authorization.issued_authority_session_id != str(transaction["authority_session_id"]):
                reason = "AUTHORIZATION_AUTHORITY_SESSION_MISMATCH"
            elif authorization.sequence != expected_sequence:
                reason = "AUTHORIZATION_SEQUENCE_MISMATCH"
            elif parse_utc(authorization.issued_at) > parse_utc(now) or parse_utc(authorization.expires_at) <= parse_utc(now):
                reason = "AUTHORIZATION_EXPIRED"
            elif authorization.operation_digest != authorization.expected_operation_digest():
                reason = "AUTHORIZATION_OPERATION_DIGEST_MISMATCH"
            elif not authorization.single_use:
                reason = "AUTHORIZATION_NOT_SINGLE_USE"
            elif accepted_ack_count != len(REQUIRED_MUTATORS) or active_fence_count != len(REQUIRED_MUTATORS):
                reason = "MAINTENANCE_FENCE_NOT_COMPLETE"
            if reason:
                self._event(
                    authorization.maintenance_id,
                    "MUTATION_AUTHORIZATION_ISSUE_REJECTED",
                    {"authorization_id": authorization.authorization_id, "reason": reason},
                )
                return reason
            db.execute(
                """INSERT INTO maintenance_mutation_authorizations
                   VALUES (?,?,?,?,?,?,0,NULL,NULL,NULL,NULL)""",
                (
                    authorization.authorization_id,
                    authorization.maintenance_id,
                    authorization.generation,
                    authorization.sequence,
                    json.dumps(authorization.to_dict(), sort_keys=True, separators=(",", ":")),
                    MutationAuthorizationState.ISSUED.value,
                ),
            )
            if deadline is not None:
                deadline.ensure_valid()
            self._event(
                authorization.maintenance_id,
                "MUTATION_AUTHORIZATION_ISSUED",
                {"authorization_id": authorization.authorization_id, "generation": authorization.generation},
            )
        return "ISSUED"

    def authorization(self, authorization_id: str) -> tuple[MaintenanceMutationAuthorization, sqlite3.Row]:
        row = self.connection.execute(
            "SELECT * FROM maintenance_mutation_authorizations WHERE authorization_id=?",
            (authorization_id,),
        ).fetchone()
        if row is None:
            raise KeyError(authorization_id)
        value = MaintenanceMutationAuthorization.from_dict(json.loads(str(row["authorization_json"])))
        return value, cast(sqlite3.Row, row)

    def _authorization_boundary_reason(
        self,
        db: sqlite3.Connection,
        *,
        authorization: MaintenanceMutationAuthorization,
        authorization_row: sqlite3.Row,
        required_state: MutationAuthorizationState,
        executor_id: MaintenanceExecutorId,
        operation: MaintenanceMutationOperation,
        resource_identity: str,
        requested_generation: int,
        observed_target: TargetIdentity,
        now: str,
    ) -> str:
        transaction = db.execute(
            "SELECT * FROM maintenance_transactions WHERE maintenance_id=?",
            (authorization.maintenance_id,),
        ).fetchone()
        if transaction is None:
            return "MAINTENANCE_NOT_FOUND"
        if str(transaction["state"]) != MaintenanceState.ESTABLISHED.value:
            return "MAINTENANCE_NOT_ESTABLISHED_AT_EFFECT_BOUNDARY"
        if str(authorization_row["state"]) != required_state.value:
            if str(authorization_row["state"]) in {
                MutationAuthorizationState.CONSUMED.value,
                MutationAuthorizationState.EXECUTION_STARTED.value,
                MutationAuthorizationState.OUTCOME_UNKNOWN.value,
            }:
                return "AUTHORIZATION_REPLAY_REJECTED"
            return "AUTHORIZATION_NOT_EXECUTABLE"
        if int(authorization_row["use_count"]) != 0:
            return "AUTHORIZATION_REPLAY_REJECTED"
        if requested_generation != authorization.generation or authorization.generation != int(transaction["generation"]):
            return "AUTHORIZATION_GENERATION_MISMATCH"
        if authorization.executor_id != executor_id:
            return "AUTHORIZATION_EXECUTOR_MISMATCH"
        if authorization.operation != operation or authorization.resource_identity != resource_identity:
            return "AUTHORIZATION_OPERATION_SCOPE_MISMATCH"
        old_target = TargetIdentity.from_dict(json.loads(str(transaction["old_target_json"])))
        if authorization.source_target_identity != old_target or observed_target != old_target:
            return "STALE_TARGET"
        if authorization.issued_authority_epoch != int(transaction["authority_epoch"]):
            return "AUTHORIZATION_AUTHORITY_EPOCH_MISMATCH"
        if authorization.issued_authority_session_id != str(transaction["authority_session_id"]):
            return "AUTHORIZATION_AUTHORITY_SESSION_MISMATCH"
        if parse_utc(authorization.expires_at) <= parse_utc(now):
            return "AUTHORIZATION_EXPIRED"
        if authorization.operation_digest != authorization.expected_operation_digest():
            return "AUTHORIZATION_OPERATION_DIGEST_MISMATCH"
        fence_count = int(
            db.execute(
                """SELECT count(*) FROM maintenance_fences
                   WHERE maintenance_id=? AND generation=? AND active=1 AND release_state=?""",
                (
                    authorization.maintenance_id,
                    authorization.generation,
                    FenceReleaseState.HELD.value,
                ),
            ).fetchone()[0]
        )
        if fence_count != len(REQUIRED_MUTATORS):
            return "EFFECT_BOUNDARY_FENCE_NOT_COMPLETE"
        return ""

    def accept_authorization(
        self,
        authorization_id: str,
        *,
        executor_id: MaintenanceExecutorId,
        operation: MaintenanceMutationOperation,
        resource_identity: str,
        requested_generation: int,
        observed_target: TargetIdentity,
        now: str,
        deadline: AuthorityOperationDeadline | None = None,
    ) -> str:
        with self.write() as db:
            if deadline is not None:
                deadline.ensure_valid()
            row = db.execute(
                "SELECT * FROM maintenance_mutation_authorizations WHERE authorization_id=?",
                (authorization_id,),
            ).fetchone()
            if row is None:
                return "AUTHORIZATION_NOT_FOUND"
            authorization = MaintenanceMutationAuthorization.from_dict(json.loads(str(row["authorization_json"])))
            reason = self._authorization_boundary_reason(
                db,
                authorization=authorization,
                authorization_row=row,
                required_state=MutationAuthorizationState.ISSUED,
                executor_id=executor_id,
                operation=operation,
                resource_identity=resource_identity,
                requested_generation=requested_generation,
                observed_target=observed_target,
                now=now,
            )
            if reason:
                state = MutationAuthorizationState.EXPIRED.value if reason == "AUTHORIZATION_EXPIRED" else str(row["state"])
                db.execute(
                    "UPDATE maintenance_mutation_authorizations SET state=?,reason_code=? WHERE authorization_id=?",
                    (state, reason, authorization_id),
                )
                self._event(
                    authorization.maintenance_id,
                    "MUTATION_AUTHORIZATION_ACCEPT_REJECTED",
                    {"authorization_id": authorization_id, "reason": reason},
                )
                return reason
            db.execute(
                "UPDATE maintenance_mutation_authorizations SET state=?,accepted_at=?,reason_code=NULL WHERE authorization_id=?",
                (MutationAuthorizationState.ACCEPTED.value, now, authorization_id),
            )
            if deadline is not None:
                deadline.ensure_valid()
            self._event(
                authorization.maintenance_id,
                "MUTATION_AUTHORIZATION_ACCEPTED",
                {"authorization_id": authorization_id},
            )
        return "ACCEPTED"

    def start_authorized_execution(
        self,
        authorization_id: str,
        *,
        executor_id: MaintenanceExecutorId,
        operation: MaintenanceMutationOperation,
        resource_identity: str,
        requested_generation: int,
        observed_target: TargetIdentity,
        now: str,
        deadline: AuthorityOperationDeadline | None = None,
    ) -> str:
        with self.write() as db:
            if deadline is not None:
                deadline.ensure_valid()
            row = db.execute(
                "SELECT * FROM maintenance_mutation_authorizations WHERE authorization_id=?",
                (authorization_id,),
            ).fetchone()
            if row is None:
                return "AUTHORIZATION_NOT_FOUND"
            authorization = MaintenanceMutationAuthorization.from_dict(json.loads(str(row["authorization_json"])))
            reason = self._authorization_boundary_reason(
                db,
                authorization=authorization,
                authorization_row=row,
                required_state=MutationAuthorizationState.ACCEPTED,
                executor_id=executor_id,
                operation=operation,
                resource_identity=resource_identity,
                requested_generation=requested_generation,
                observed_target=observed_target,
                now=now,
            )
            if reason:
                self._event(
                    authorization.maintenance_id,
                    "EFFECT_BOUNDARY_REJECTED",
                    {"authorization_id": authorization_id, "reason": reason},
                )
                return reason
            db.execute(
                """UPDATE maintenance_mutation_authorizations
                   SET state=?,use_count=1,execution_started_at=?,reason_code=NULL WHERE authorization_id=?""",
                (MutationAuthorizationState.EXECUTION_STARTED.value, now, authorization_id),
            )
            db.execute(
                "INSERT INTO maintenance_mutation_executions VALUES (?,?,?,?,?,?,?,?,NULL,NULL)",
                (
                    f"execution-{authorization_id}",
                    authorization_id,
                    authorization.maintenance_id,
                    authorization.generation,
                    MutationAuthorizationState.EXECUTION_STARTED.value,
                    json.dumps(authorization.source_target_identity.to_dict(), sort_keys=True, separators=(",", ":")),
                    json.dumps(observed_target.to_dict(), sort_keys=True, separators=(",", ":")),
                    now,
                ),
            )
            if deadline is not None:
                deadline.ensure_valid()
            self._event(
                authorization.maintenance_id,
                "EFFECT_BOUNDARY_VALIDATED",
                {
                    "authorization_id": authorization_id,
                    "fence_revalidated": True,
                    "authorization_revalidated": True,
                    "target_revalidated": True,
                },
            )
        return "EXECUTION_STARTED"

    def consume_authorization(self, authorization_id: str, *, finished_at: str) -> str:
        with self.write() as db:
            row = db.execute(
                "SELECT * FROM maintenance_mutation_authorizations WHERE authorization_id=?",
                (authorization_id,),
            ).fetchone()
            if row is None or str(row["state"]) != MutationAuthorizationState.EXECUTION_STARTED.value:
                return "AUTHORIZATION_NOT_EXECUTION_STARTED"
            maintenance_id = str(row["maintenance_id"])
            db.execute(
                "UPDATE maintenance_mutation_authorizations SET state=?,terminal_at=?,reason_code=? WHERE authorization_id=?",
                (MutationAuthorizationState.CONSUMED.value, finished_at, "WOULD_MUTATE_RECORDED", authorization_id),
            )
            db.execute(
                """UPDATE maintenance_mutation_executions SET state=?,finished_at=?,reason_code=?
                   WHERE authorization_id=?""",
                (MutationAuthorizationState.CONSUMED.value, finished_at, "WOULD_MUTATE_RECORDED", authorization_id),
            )
            db.execute(
                """UPDATE maintenance_transactions
                   SET state=?,authority_state='MAINTENANCE',planned_would_mutate_count=planned_would_mutate_count+1
                   WHERE maintenance_id=?""",
                (MaintenanceState.MUTATING.value, maintenance_id),
            )
            self._event(maintenance_id, "PLANNED_WOULD_MUTATE", {"authorization_id": authorization_id})
        return "CONSUMED"

    def recover_authorization_after_restart(self, maintenance_id: str, *, recovered_at: str) -> int:
        with self.write() as db:
            rows = db.execute(
                """SELECT authorization_id FROM maintenance_mutation_authorizations
                   WHERE maintenance_id=? AND state=?""",
                (maintenance_id, MutationAuthorizationState.EXECUTION_STARTED.value),
            ).fetchall()
            for row in rows:
                authorization_id = str(row["authorization_id"])
                db.execute(
                    """UPDATE maintenance_mutation_authorizations
                       SET state=?,terminal_at=?,reason_code=? WHERE authorization_id=?""",
                    (
                        MutationAuthorizationState.OUTCOME_UNKNOWN.value,
                        recovered_at,
                        "EXECUTOR_RESTART_AFTER_EXECUTION_STARTED",
                        authorization_id,
                    ),
                )
                db.execute(
                    """UPDATE maintenance_mutation_executions SET state=?,finished_at=?,reason_code=?
                       WHERE authorization_id=?""",
                    (
                        MutationAuthorizationState.OUTCOME_UNKNOWN.value,
                        recovered_at,
                        "EXECUTOR_RESTART_AFTER_EXECUTION_STARTED",
                        authorization_id,
                    ),
                )
                self._event(
                    maintenance_id,
                    "MUTATION_OUTCOME_UNKNOWN",
                    {"authorization_id": authorization_id, "automatic_retry": False},
                )
        return len(rows)

    def supersede_authorizations(self, maintenance_id: str, *, superseded_at: str, reason: str) -> int:
        with self.write() as db:
            rows = db.execute(
                """SELECT authorization_id FROM maintenance_mutation_authorizations
                   WHERE maintenance_id=? AND state IN (?,?)""",
                (
                    maintenance_id,
                    MutationAuthorizationState.ISSUED.value,
                    MutationAuthorizationState.ACCEPTED.value,
                ),
            ).fetchall()
            for row in rows:
                authorization_id = str(row["authorization_id"])
                db.execute(
                    """UPDATE maintenance_mutation_authorizations
                       SET state=?,terminal_at=?,reason_code=? WHERE authorization_id=?""",
                    (MutationAuthorizationState.SUPERSEDED.value, superseded_at, reason, authorization_id),
                )
                self._event(
                    maintenance_id,
                    "MUTATION_AUTHORIZATION_SUPERSEDED",
                    {"authorization_id": authorization_id, "reason": reason},
                )
        return len(rows)

    def authorization_summary(self, maintenance_id: str) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT * FROM maintenance_mutation_authorizations WHERE maintenance_id=? ORDER BY sequence",
            (maintenance_id,),
        ).fetchall()
        return {
            "issued_count": len(rows),
            "use_count": sum(int(row["use_count"]) for row in rows),
            "states": {str(row["authorization_id"]): str(row["state"]) for row in rows},
            "outcome_unknown_count": sum(1 for row in rows if str(row["state"]) == MutationAuthorizationState.OUTCOME_UNKNOWN.value),
        }

    def has_consumed_authorization(self, maintenance_id: str) -> bool:
        row = self.connection.execute(
            """SELECT count(*) FROM maintenance_mutation_authorizations
               WHERE maintenance_id=? AND state=? AND use_count=1""",
            (maintenance_id, MutationAuthorizationState.CONSUMED.value),
        ).fetchone()
        return bool(row is not None and int(row[0]) == 1)

    def set_new_target(self, maintenance_id: str, target: TargetIdentity) -> None:
        with self.write() as db:
            db.execute(
                "UPDATE maintenance_transactions SET new_target_json=? WHERE maintenance_id=?",
                (json.dumps(target.to_dict(), sort_keys=True, separators=(",", ":")), maintenance_id),
            )
            self._event(maintenance_id, "NEW_TARGET_OBSERVED", target.to_dict())

    def set_reconciled(self, maintenance_id: str, *, epoch: int, session_id: str, fresh_heartbeat: bool) -> None:
        with self.write() as db:
            db.execute(
                """UPDATE maintenance_transactions SET authority_epoch=?,authority_session_id=?,fresh_heartbeat=?
                   WHERE maintenance_id=?""",
                (epoch, session_id, int(fresh_heartbeat), maintenance_id),
            )
            self._event(
                maintenance_id,
                "RECONCILIATION_OBSERVED",
                {"authority_epoch": epoch, "authority_session_id": session_id, "fresh_heartbeat": fresh_heartbeat},
            )

    def increment_planned(self, maintenance_id: str) -> None:
        with self.write() as db:
            db.execute(
                """UPDATE maintenance_transactions SET planned_would_mutate_count=planned_would_mutate_count+1
                   WHERE maintenance_id=?""",
                (maintenance_id,),
            )
            self._event(maintenance_id, "PLANNED_WOULD_MUTATE", {})

    def record_effect_boundary_rejection(
        self,
        maintenance_id: str,
        *,
        mutator_id: MutatorId,
        generation: int,
        reason: str,
    ) -> None:
        with self.write():
            self._event(
                maintenance_id,
                "EFFECT_BOUNDARY_REJECTED",
                {
                    "mutator_id": mutator_id.value,
                    "generation": generation,
                    "reason": reason,
                    "physical_effect": 0,
                },
            )

    def record_release_ack(self, ack: FenceReleaseAck, *, accepted: bool, reason: str = "") -> None:
        with self.write() as db:
            db.execute(
                "INSERT OR REPLACE INTO fence_release_acks VALUES (?,?,?,?,?,?)",
                (
                    ack.maintenance_id,
                    ack.mutator_id.value,
                    ack.generation,
                    json.dumps(ack.to_dict(), sort_keys=True, separators=(",", ":")),
                    int(accepted),
                    reason or None,
                ),
            )
            if accepted:
                db.execute(
                    """UPDATE maintenance_fences SET release_state=?
                       WHERE maintenance_id=? AND mutator_id=? AND generation=? AND active=1""",
                    (
                        FenceReleaseState.RELEASE_PREPARED.value,
                        ack.maintenance_id,
                        ack.mutator_id.value,
                        ack.generation,
                    ),
                )
            self._event(
                ack.maintenance_id,
                "FENCE_RELEASE_PREPARED" if accepted else "FENCE_RELEASE_REJECTED",
                {"mutator_id": ack.mutator_id.value, "generation": ack.generation, "reason": reason},
            )

    def accepted_release_acks(self, maintenance_id: str) -> dict[str, dict[str, Any]]:
        return {
            str(row["mutator_id"]): json.loads(str(row["ack_json"]))
            for row in self.connection.execute(
                "SELECT * FROM fence_release_acks WHERE maintenance_id=? AND accepted=1",
                (maintenance_id,),
            )
        }

    def release_states(self, maintenance_id: str) -> dict[str, str]:
        return {
            str(row["mutator_id"]): str(row["release_state"])
            for row in self.connection.execute(
                "SELECT mutator_id,release_state FROM maintenance_fences WHERE maintenance_id=?",
                (maintenance_id,),
            )
        }

    def commit_fence_release(self, maintenance_id: str, generation: int) -> str:
        with self.write() as db:
            transaction = db.execute(
                "SELECT * FROM maintenance_transactions WHERE maintenance_id=?",
                (maintenance_id,),
            ).fetchone()
            prepared = int(
                db.execute(
                    """SELECT count(*) FROM maintenance_fences WHERE maintenance_id=? AND generation=?
                       AND active=1 AND release_state=?""",
                    (maintenance_id, generation, FenceReleaseState.RELEASE_PREPARED.value),
                ).fetchone()[0]
            )
            release_acks = int(
                db.execute(
                    """SELECT count(*) FROM fence_release_acks
                       WHERE maintenance_id=? AND generation=? AND accepted=1""",
                    (maintenance_id, generation),
                ).fetchone()[0]
            )
            consumed = int(
                db.execute(
                    """SELECT count(*) FROM maintenance_mutation_authorizations
                       WHERE maintenance_id=? AND generation=? AND state=? AND use_count=1""",
                    (maintenance_id, generation, MutationAuthorizationState.CONSUMED.value),
                ).fetchone()[0]
            )
            if transaction is None:
                return "MAINTENANCE_NOT_FOUND"
            if int(transaction["generation"]) != generation:
                return "STALE_RELEASE_GENERATION"
            if str(transaction["state"]) != MaintenanceState.EXIT_PENDING.value:
                return "MAINTENANCE_NOT_EXIT_PENDING"
            if not int(transaction["fresh_heartbeat"]) or transaction["new_target_json"] is None:
                return "EXIT_RECONCILIATION_INCOMPLETE"
            if prepared != len(REQUIRED_MUTATORS) or release_acks != len(REQUIRED_MUTATORS):
                return "FENCE_RELEASE_NOT_FULLY_PREPARED"
            if consumed != 1:
                return "MUTATION_AUTHORIZATION_NOT_CONSUMED"
            db.execute(
                """UPDATE maintenance_fences SET active=0,release_state=?
                   WHERE maintenance_id=? AND generation=?""",
                (FenceReleaseState.RELEASED.value, maintenance_id, generation),
            )
            db.execute(
                "UPDATE maintenance_transactions SET state=?,authority_state='CENTRAL_ACTIVE' WHERE maintenance_id=?",
                (MaintenanceState.COMPLETED.value, maintenance_id),
            )
            self._event(maintenance_id, "ALL_FENCES_RELEASED", {"generation": generation})
            self._event(
                maintenance_id,
                "STATE_TRANSITION",
                {"state": MaintenanceState.COMPLETED.value, "authority_state": "CENTRAL_ACTIVE"},
            )
        return "COMPLETED"

    def release_all_fences_for_abort(self, maintenance_id: str) -> None:
        with self.write() as db:
            db.execute(
                "UPDATE maintenance_fences SET active=0,release_state=? WHERE maintenance_id=?",
                (FenceReleaseState.RELEASED.value, maintenance_id),
            )
            self._event(maintenance_id, "ALL_FENCES_RELEASED", {})

    def active_fence_count(self, maintenance_id: str) -> int:
        row = self.connection.execute(
            "SELECT count(*) FROM maintenance_fences WHERE maintenance_id=? AND active=1", (maintenance_id,)
        ).fetchone()
        return int(row[0])

    def normal_recovery_eligible(self, maintenance_id: str) -> bool:
        row = self.row(maintenance_id)
        return (
            str(row["state"]) in {MaintenanceState.COMPLETED.value, MaintenanceState.ABORTED.value}
            and self.active_fence_count(maintenance_id) == 0
        )

    def events(self, maintenance_id: str) -> list[dict[str, Any]]:
        return [
            {
                "event_seq": int(row["event_seq"]),
                "event_type": str(row["event_type"]),
                "payload": json.loads(str(row["payload_json"])),
            }
            for row in self.connection.execute(
                "SELECT * FROM maintenance_events WHERE maintenance_id=? ORDER BY event_seq", (maintenance_id,)
            )
        ]

    def _event(self, maintenance_id: str, event_type: str, payload: dict[str, Any]) -> None:
        self.connection.execute(
            "INSERT INTO maintenance_events(maintenance_id,event_type,payload_json) VALUES (?,?,?)",
            (maintenance_id, event_type, json.dumps(payload, sort_keys=True, separators=(",", ":"))),
        )

    def close(self) -> None:
        self.connection.close()
