from __future__ import annotations

from typing import Any


def evaluate_mutation_authorization_invariants(scenario: dict[str, Any], observation: dict[str, Any]) -> list[str]:
    """Independent Oracle. It imports no maintenance SUT, state machine, or authorization model."""

    violations: list[str] = []

    def require(condition: bool, code: str) -> None:
        if not condition:
            violations.append(code)

    planned_count = int(observation.get("planned_would_mutate_count", 0))
    use_count = int(observation.get("authorization_use_count", 0))
    require(use_count <= 1, "AUTHORIZATION_USED_MORE_THAN_ONCE")

    if planned_count > 0:
        require(bool(observation.get("valid_maintenance_authorization")), "PLANNED_MUTATION_WITHOUT_VALID_AUTHORIZATION")
        require(observation.get("authorization_kind") == "MAINTENANCE_MUTATION", "WRONG_AUTHORIZATION_KIND_FOR_PLANNED_MUTATION")
        require(use_count == 1, "PLANNED_MUTATION_WITHOUT_SINGLE_AUTHORIZATION_USE")
        require(bool(observation.get("effect_boundary_fence_revalidated")), "EFFECT_BOUNDARY_FENCE_NOT_REVALIDATED")
        require(
            bool(observation.get("effect_boundary_authorization_revalidated")),
            "EFFECT_BOUNDARY_AUTHORIZATION_NOT_REVALIDATED",
        )
        require(bool(observation.get("effect_boundary_target_revalidated")), "EFFECT_BOUNDARY_TARGET_NOT_REVALIDATED")

    if scenario.get("authorization_happy_path"):
        require(planned_count == 1, "AUTHORIZED_PLANNED_MUTATION_COUNT_NOT_ONE")
        require(observation.get("authorization_state") == "CONSUMED", "AUTHORIZATION_NOT_CONSUMED")
        require(int(observation.get("normal_recovery_mutations", 0)) == 0, "NORMAL_RECOVERY_DURING_MAINTENANCE")
    if scenario.get("authorization_replay"):
        require(not bool(observation.get("second_mutation_allowed")), "AUTHORIZATION_REPLAY_ALLOWED")
        require(planned_count == 1, "AUTHORIZATION_REPLAY_CHANGED_MUTATION_COUNT")
    if scenario.get("authorization_expired"):
        require(planned_count == 0, "EXPIRED_AUTHORIZATION_ALLOWED")
    if scenario.get("authorization_generation_mismatch"):
        require(planned_count == 0, "OLD_GENERATION_AUTHORIZATION_ALLOWED")
    if scenario.get("authorization_target_mismatch"):
        require(planned_count == 0, "WRONG_TARGET_AUTHORIZATION_ALLOWED")
    if scenario.get("normal_recovery_during_maintenance"):
        require(int(observation.get("normal_recovery_mutations", 0)) == 0, "NORMAL_RECOVERY_AUTHORIZATION_ALLOWED")
    if scenario.get("stale_legacy_action"):
        require(int(observation.get("legacy_mutations", 0)) == 0, "STALE_LEGACY_EFFECT_ALLOWED")
        require(int(observation.get("effect_boundary_check_count", 0)) > 0, "STALE_LEGACY_EFFECT_BOUNDARY_NOT_CHECKED")
    if scenario.get("executor_restart"):
        require(bool(observation.get("authorization_persisted")), "AUTHORIZATION_LOST_ON_EXECUTOR_RESTART")
        require(not bool(observation.get("second_mutation_allowed")), "EXECUTOR_RESTART_FORGOT_SINGLE_USE")
        require(planned_count == 1, "EXECUTOR_RESTART_MUTATION_COUNT_NOT_ONE")
    if scenario.get("outcome_unknown"):
        require(observation.get("authorization_state") == "OUTCOME_UNKNOWN", "UNKNOWN_OUTCOME_NOT_PERSISTED")
        require(int(observation.get("automatic_retry_count", 0)) == 0, "UNKNOWN_OUTCOME_AUTOMATICALLY_RETRIED")
        require(planned_count == 0, "UNKNOWN_OUTCOME_REMUTATED")
    if scenario.get("partial_release"):
        require(not bool(observation.get("authority_restored")), "PARTIAL_RELEASE_RESTORED_AUTHORITY")
        require(not bool(observation.get("normal_recovery_eligible")), "PARTIAL_RELEASE_ENABLED_NORMAL_RECOVERY")
        require(int(observation.get("active_fence_count", 0)) > 0, "PARTIAL_RELEASE_DROPPED_ALL_FENCES")
    if scenario.get("stale_release_generation"):
        require(not bool(observation.get("stale_release_accepted")), "STALE_RELEASE_GENERATION_ACCEPTED")
        require(bool(observation.get("current_fence_unaffected")), "STALE_RELEASE_CHANGED_CURRENT_FENCE")
    if scenario.get("complete_exit"):
        require(bool(observation.get("new_target_accepted")), "COMPLETION_WITHOUT_NEW_TARGET")
        require(bool(observation.get("reconciliation_complete")), "COMPLETION_WITHOUT_RECONCILIATION")
        require(bool(observation.get("new_epoch")), "COMPLETION_WITHOUT_NEW_EPOCH")
        require(bool(observation.get("new_session")), "COMPLETION_WITHOUT_NEW_SESSION")
        require(bool(observation.get("fresh_heartbeat")), "COMPLETION_WITHOUT_FRESH_HEARTBEAT")
        require(bool(observation.get("all_release_acks")), "COMPLETION_WITHOUT_ALL_RELEASE_ACKS")
        require(bool(observation.get("authority_restored")), "COMPLETE_EXIT_DID_NOT_RESTORE_AUTHORITY")
        require(bool(observation.get("normal_recovery_eligible")), "COMPLETE_EXIT_DID_NOT_ENABLE_NORMAL_RECOVERY")
    if scenario.get("db_deadline"):
        require(planned_count == 0, "DB_DEADLINE_STALE_AUTHORIZATION_COMMITTED")
        require(int(observation.get("physical_effect_count", 0)) == 0, "DB_DEADLINE_PHYSICAL_EFFECT")

    require(int(observation.get("physical_effect_count", 0)) == 0, "NON_FAKE_PHYSICAL_EFFECT")
    return violations
