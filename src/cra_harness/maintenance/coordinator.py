from __future__ import annotations

import json

from cra_dell_recovery.errors import AuthorityDeadlineExceeded
from cra_dell_recovery.models import TargetIdentity
from cra_harness.maintenance.model import (
    REQUIRED_MUTATORS,
    FenceReleaseAck,
    MaintenanceExecutorId,
    MaintenanceIntent,
    MaintenanceMutationAuthorization,
    MaintenanceMutationOperation,
    MaintenanceState,
    MutationDecision,
    MutatorId,
    QuiesceAck,
)
from cra_harness.maintenance.store import MaintenanceStore
from dell_recovery_agent.deadline import AuthorityOperationDeadline


class NoActionPhysicalAdapter:
    def __init__(self) -> None:
        self.physical_effect_count = 0
        self.would_mutate_count = 0

    def record_authorized_would_mutate(self) -> None:
        self.would_mutate_count += 1


class MaintenanceCoordinator:
    """Harness SUT. It records WOULD_MUTATE only and has no production adapter."""

    def __init__(self, store: MaintenanceStore, adapter: NoActionPhysicalAdapter | None = None) -> None:
        self.store = store
        self.adapter = adapter or NoActionPhysicalAdapter()

    def request(self, intent: MaintenanceIntent) -> None:
        self.store.create(intent)

    def begin_quiesce(self, maintenance_id: str, mutators: tuple[MutatorId, ...] = REQUIRED_MUTATORS) -> None:
        intent = self.store.intent(maintenance_id)
        self.store.transition(maintenance_id, MaintenanceState.QUIESCING, authority_state="MAINTENANCE")
        for mutator in mutators:
            self.store.install_fence(maintenance_id, intent.generation, mutator, intent.target_identity)

    def acknowledge(self, maintenance_id: str, ack: QuiesceAck) -> bool:
        intent = self.store.intent(maintenance_id)
        reason = ""
        if ack.maintenance_id != maintenance_id:
            reason = "MAINTENANCE_ID_MISMATCH"
        elif ack.generation != intent.generation:
            reason = "GENERATION_MISMATCH"
        elif ack.target_identity != intent.target_identity:
            reason = "TARGET_IDENTITY_MISMATCH"
        elif not ack.quiesced:
            reason = "MUTATOR_NOT_QUIESCED"
        elif not ack.persistent_fence_installed or not self.store.fence_active(maintenance_id, ack.mutator_id, ack.generation):
            reason = "PERSISTENT_FENCE_NOT_PROVEN"
        elif ack.in_flight_count != 0:
            reason = "IN_FLIGHT_ACTION_REMAINS"
        self.store.record_ack(maintenance_id, ack, accepted=not reason, reason=reason)
        return not reason

    def establish(self, maintenance_id: str) -> bool:
        intent = self.store.intent(maintenance_id)
        accepted = self.store.accepted_acks(maintenance_id)
        valid = all(
            mutator.value in accepted
            and int(accepted[mutator.value]["generation"]) == intent.generation
            and int(accepted[mutator.value]["in_flight_count"]) == 0
            and self.store.fence_active(maintenance_id, mutator, intent.generation)
            for mutator in REQUIRED_MUTATORS
        )
        if not valid:
            self.store.transition(maintenance_id, MaintenanceState.QUIESCE_FAILED, authority_state="MAINTENANCE")
            return False
        self.store.transition(maintenance_id, MaintenanceState.ESTABLISHED, authority_state="MAINTENANCE")
        return True

    def automatic_mutation(self, maintenance_id: str, mutator: MutatorId, generation: int) -> bool:
        row = self.store.row(maintenance_id)
        maintenance_active = str(row["state"]) not in {
            MaintenanceState.COMPLETED.value,
            MaintenanceState.ABORTED.value,
        }
        if maintenance_active:
            reason = (
                "AUTOMATIC_MUTATION_FENCED"
                if self.store.fence_active(maintenance_id, mutator, generation)
                else "AUTOMATIC_MUTATION_FENCE_UNCERTAIN"
            )
            self.store.record_effect_boundary_rejection(
                maintenance_id,
                mutator_id=mutator,
                generation=generation,
                reason=reason,
            )
            return False
        return self.store.normal_recovery_eligible(maintenance_id)

    def planned_mutation(self, maintenance_id: str) -> bool:
        """Unscoped v2 compatibility entry point; deliberately cannot bypass authorization."""
        self.store.record_effect_boundary_rejection(
            maintenance_id,
            mutator_id=MutatorId.KUBERNETES_AUTORECONCILE,
            generation=int(self.store.row(maintenance_id)["generation"]),
            reason="MAINTENANCE_MUTATION_AUTHORIZATION_REQUIRED",
        )
        return False

    def issue_mutation_authorization(
        self,
        authorization: MaintenanceMutationAuthorization,
        *,
        now: str,
        deadline: AuthorityOperationDeadline | None = None,
    ) -> str:
        try:
            return self.store.issue_authorization(authorization, now=now, deadline=deadline)
        except AuthorityDeadlineExceeded:
            self.store.transition(authorization.maintenance_id, MaintenanceState.SAFE_BLOCKED, authority_state="SAFE_BLOCKED")
            return "DB_DEADLINE_AUTHORITY_INVALIDATED"

    def accept_mutation_authorization(
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
        try:
            return self.store.accept_authorization(
                authorization_id,
                executor_id=executor_id,
                operation=operation,
                resource_identity=resource_identity,
                requested_generation=requested_generation,
                observed_target=observed_target,
                now=now,
                deadline=deadline,
            )
        except AuthorityDeadlineExceeded:
            authorization, _ = self.store.authorization(authorization_id)
            self.store.transition(authorization.maintenance_id, MaintenanceState.SAFE_BLOCKED, authority_state="SAFE_BLOCKED")
            return "DB_DEADLINE_AUTHORITY_INVALIDATED"

    def execute_authorized_mutation(
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
        crash_after_execution_started: bool = False,
    ) -> MutationDecision:
        authorization, _ = self.store.authorization(authorization_id)
        try:
            reason = self.store.start_authorized_execution(
                authorization_id,
                executor_id=executor_id,
                operation=operation,
                resource_identity=resource_identity,
                requested_generation=requested_generation,
                observed_target=observed_target,
                now=now,
                deadline=deadline,
            )
        except AuthorityDeadlineExceeded:
            self.store.transition(authorization.maintenance_id, MaintenanceState.SAFE_BLOCKED, authority_state="SAFE_BLOCKED")
            reason = "DB_DEADLINE_AUTHORITY_INVALIDATED"
        if reason != "EXECUTION_STARTED":
            summary = self.store.authorization_summary(authorization.maintenance_id)
            state = summary["states"].get(authorization_id, "UNKNOWN")
            return MutationDecision(False, reason, str(state), int(summary["use_count"]), self._planned_count(authorization.maintenance_id))
        if crash_after_execution_started:
            return MutationDecision(
                False,
                "CRASH_AFTER_EXECUTION_STARTED",
                "EXECUTION_STARTED",
                1,
                self._planned_count(authorization.maintenance_id),
            )
        self.adapter.record_authorized_would_mutate()
        consumed = self.store.consume_authorization(authorization_id, finished_at=now)
        summary = self.store.authorization_summary(authorization.maintenance_id)
        return MutationDecision(
            consumed == "CONSUMED",
            "AUTHORIZED_WOULD_MUTATE" if consumed == "CONSUMED" else consumed,
            str(summary["states"].get(authorization_id, "UNKNOWN")),
            int(summary["use_count"]),
            self._planned_count(authorization.maintenance_id),
        )

    def _planned_count(self, maintenance_id: str) -> int:
        return int(self.store.row(maintenance_id)["planned_would_mutate_count"])

    def normal_recovery_mutation(self, maintenance_id: str, mutator: MutatorId) -> bool:
        del mutator
        return self.store.normal_recovery_eligible(maintenance_id)

    def legacy_effect_boundary(
        self,
        maintenance_id: str,
        *,
        mutator: MutatorId,
        admitted_generation: int,
        observed_target: TargetIdentity,
    ) -> bool:
        row = self.store.row(maintenance_id)
        active = str(row["state"]) not in {MaintenanceState.COMPLETED.value, MaintenanceState.ABORTED.value}
        old_target = TargetIdentity.from_dict(json.loads(str(row["old_target_json"])))
        if active:
            reason = "EFFECT_BOUNDARY_MAINTENANCE_FENCE_ACTIVE"
            if admitted_generation != int(row["generation"]):
                reason = "EFFECT_BOUNDARY_STALE_GENERATION"
            elif observed_target != old_target:
                reason = "EFFECT_BOUNDARY_STALE_TARGET"
            self.store.record_effect_boundary_rejection(
                maintenance_id,
                mutator_id=mutator,
                generation=admitted_generation,
                reason=reason,
            )
            return False
        return True

    def complete_planned_mutation(self, maintenance_id: str, new_target: TargetIdentity) -> bool:
        row = self.store.row(maintenance_id)
        old_target = TargetIdentity.from_dict(json.loads(str(row["old_target_json"])))
        if (
            str(row["state"]) != MaintenanceState.MUTATING.value
            or not self.store.has_consumed_authorization(maintenance_id)
            or new_target == old_target
        ):
            self.store.transition(maintenance_id, MaintenanceState.SAFE_BLOCKED, authority_state="SAFE_BLOCKED")
            return False
        self.store.set_new_target(maintenance_id, new_target)
        self.store.transition(maintenance_id, MaintenanceState.VERIFYING_TARGET, authority_state="MAINTENANCE")
        return True

    def reconcile(
        self,
        maintenance_id: str,
        observed_target: TargetIdentity,
        *,
        new_epoch: int,
        new_session_id: str,
        fresh_heartbeat: bool,
    ) -> bool:
        row = self.store.row(maintenance_id)
        old_target = TargetIdentity.from_dict(json.loads(str(row["old_target_json"])))
        new_target_raw = row["new_target_json"]
        new_target = None if new_target_raw is None else TargetIdentity.from_dict(json.loads(str(new_target_raw)))
        self.store.transition(maintenance_id, MaintenanceState.RECONCILING, authority_state="RECONCILING")
        valid = (
            new_target is not None
            and observed_target == new_target
            and observed_target != old_target
            and new_epoch > int(row["authority_epoch"])
            and new_session_id != str(row["authority_session_id"])
            and bool(fresh_heartbeat)
        )
        if not valid:
            return False
        self.store.set_reconciled(
            maintenance_id,
            epoch=new_epoch,
            session_id=new_session_id,
            fresh_heartbeat=fresh_heartbeat,
        )
        self.store.transition(maintenance_id, MaintenanceState.EXIT_PENDING, authority_state="RECONCILING")
        return True

    def acknowledge_fence_release(self, ack: FenceReleaseAck) -> bool:
        row = self.store.row(ack.maintenance_id)
        reason = ""
        new_target_raw = row["new_target_json"]
        new_target = None if new_target_raw is None else TargetIdentity.from_dict(json.loads(str(new_target_raw)))
        if ack.generation != int(row["generation"]):
            reason = "STALE_RELEASE_GENERATION"
        elif str(row["state"]) != MaintenanceState.EXIT_PENDING.value:
            reason = "MAINTENANCE_NOT_EXIT_PENDING"
        elif new_target is None or ack.target_identity != new_target:
            reason = "RELEASE_TARGET_IDENTITY_MISMATCH"
        elif not ack.prepared or not ack.fence_still_active:
            reason = "RELEASE_PREPARE_FAILED"
        elif not self.store.fence_active(ack.maintenance_id, ack.mutator_id, ack.generation):
            reason = "RELEASE_FENCE_NOT_ACTIVE"
        self.store.record_release_ack(ack, accepted=not reason, reason=reason)
        if reason in {"RELEASE_PREPARE_FAILED", "RELEASE_FENCE_NOT_ACTIVE"}:
            self.store.transition(ack.maintenance_id, MaintenanceState.SAFE_BLOCKED, authority_state="SAFE_BLOCKED")
        return not reason

    def complete_exit(self, maintenance_id: str, generation: int) -> bool:
        return self.store.commit_fence_release(maintenance_id, generation) == "COMPLETED"

    def abort(
        self,
        maintenance_id: str,
        *,
        reconciliation_complete: bool,
        superseded_at: str = "2026-08-23T08:00:03.000Z",
    ) -> bool:
        self.store.transition(maintenance_id, MaintenanceState.ABORTING, authority_state="RECONCILING")
        if not reconciliation_complete:
            self.store.transition(maintenance_id, MaintenanceState.SAFE_BLOCKED, authority_state="SAFE_BLOCKED")
            return False
        self.store.supersede_authorizations(
            maintenance_id,
            superseded_at=superseded_at,
            reason="MAINTENANCE_ABORTED",
        )
        self.store.release_all_fences_for_abort(maintenance_id)
        self.store.transition(maintenance_id, MaintenanceState.ABORTED, authority_state="CENTRAL_ACTIVE")
        return True

    def recover_after_restart(self, maintenance_id: str, *, recovered_at: str = "2026-08-23T08:00:03.000Z") -> MaintenanceState:
        unknown_count = self.store.recover_authorization_after_restart(maintenance_id, recovered_at=recovered_at)
        if unknown_count:
            self.store.transition(maintenance_id, MaintenanceState.SAFE_BLOCKED, authority_state="SAFE_BLOCKED")
            return MaintenanceState.SAFE_BLOCKED
        row = self.store.row(maintenance_id)
        state = MaintenanceState(str(row["state"]))
        if state in {
            MaintenanceState.ESTABLISHED,
            MaintenanceState.MUTATING,
            MaintenanceState.VERIFYING_TARGET,
            MaintenanceState.RECONCILING,
            MaintenanceState.EXIT_PENDING,
        } and self.store.active_fence_count(maintenance_id) != len(REQUIRED_MUTATORS):
            self.store.transition(maintenance_id, MaintenanceState.SAFE_BLOCKED, authority_state="SAFE_BLOCKED")
            return MaintenanceState.SAFE_BLOCKED
        return state
