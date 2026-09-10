from __future__ import annotations

from typing import Any


def evaluate_maintenance_invariants(scenario: dict[str, Any], observation: dict[str, Any]) -> list[str]:
    """Independent oracle: no import from the maintenance SUT/model package."""

    violations: list[str] = []

    def require(condition: bool, code: str) -> None:
        if not condition:
            violations.append(code)

    if observation.get("maintenance_established"):
        require(bool(observation.get("all_required_acks")), "ESTABLISHED_WITHOUT_ALL_REQUIRED_ACKS")
        require(int(observation.get("in_flight_count", 0)) == 0, "ESTABLISHED_WITH_IN_FLIGHT_ACTION")
    if scenario.get("maintenance_active"):
        require(int(observation.get("automatic_central_mutations", 0)) == 0, "CENTRAL_MUTATION_DURING_MAINTENANCE")
        require(int(observation.get("automatic_local_mutations", 0)) == 0, "LOCAL_MUTATION_DURING_MAINTENANCE")
        require(int(observation.get("legacy_mutations", 0)) == 0, "LEGACY_MUTATION_DURING_MAINTENANCE")
    if scenario.get("missing_ack"):
        require(not bool(observation.get("maintenance_established")), "MISSING_ACK_ACCEPTED")
        require(int(observation.get("planned_would_mutate_count", 0)) == 0, "PLANNED_MUTATION_WITH_MISSING_ACK")
    if scenario.get("generation_mismatch"):
        require(not bool(observation.get("generation_mismatch_accepted")), "GENERATION_MISMATCH_ACCEPTED")
    if scenario.get("lan_loss"):
        require(not bool(observation.get("local_fallback_acquired")), "LOCAL_FALLBACK_GRANTED_DURING_MAINTENANCE")
    if scenario.get("process_restart"):
        require(bool(observation.get("persistent_fence_survived")), "PERSISTENT_FENCE_LOST_ON_RESTART")
    if scenario.get("old_target_replay"):
        require(not bool(observation.get("old_target_accepted")), "OLD_TARGET_IDENTITY_ACCEPTED")
        require(not bool(observation.get("authority_restored")), "AUTHORITY_RESTORED_FROM_OLD_TARGET")
    if scenario.get("exit_attempt") and observation.get("authority_restored"):
        require(bool(observation.get("new_epoch")), "EXIT_WITHOUT_NEW_EPOCH")
        require(bool(observation.get("new_session")), "EXIT_WITHOUT_NEW_SESSION")
        require(bool(observation.get("fresh_heartbeat")), "EXIT_WITHOUT_FRESH_HEARTBEAT")
        require(bool(observation.get("reconciliation_complete")), "EXIT_WITHOUT_RECONCILIATION")
    if scenario.get("abort"):
        require(int(observation.get("planned_would_mutate_count", 0)) == 0, "PLANNED_MUTATION_AFTER_PARTIAL_ABORT")
        require(int(observation.get("authority_ambiguity_count", 0)) == 0, "AUTHORITY_AMBIGUITY_AFTER_ABORT")
    if scenario.get("unknown_maintenance_state"):
        require(int(observation.get("physical_effect_count", 0)) == 0, "PHYSICAL_ACTION_WITH_UNKNOWN_MAINTENANCE_STATE")
    require(int(observation.get("physical_effect_count", 0)) == 0, "NON_FAKE_PHYSICAL_EFFECT")
    return violations
