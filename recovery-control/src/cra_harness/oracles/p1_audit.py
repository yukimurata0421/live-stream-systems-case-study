from __future__ import annotations

from typing import Any

ACTIVE_STATES = {
    "REQUESTED",
    "QUIESCING",
    "ESTABLISHED",
    "MUTATING",
    "VERIFYING_TARGET",
    "RECONCILING",
    "EXIT_PENDING",
    "QUIESCE_FAILED",
    "ABORTING",
    "SAFE_BLOCKED",
}


def expected_verdict(value: dict[str, Any]) -> str:
    if value.get("role") in {"HOST_SAFETY", "BREAK_GLASS", "INACTIVE_LEGACY"}:
        return "NOT_APPLICABLE"
    if value.get("role") == "EXECUTION_SUBSTRATE":
        return "NOT_APPLICABLE" if value.get("correlation_owner_path_id") else "UNKNOWN"
    if not value.get("state_available") or not value.get("state_fresh"):
        return "UNKNOWN"
    active = value.get("maintenance_state") in ACTIVE_STATES
    planned = value.get("role") in {"PLANNED_EXECUTOR", "MANUAL_PLANNED_EXECUTOR"}
    if not active:
        return "WOULD_REJECT" if planned else "WOULD_ALLOW"
    if value.get("role") == "LOCAL_FALLBACK":
        return "WOULD_BLOCK"
    if value.get("target_status") == "UNKNOWN":
        return "UNKNOWN"
    if value.get("target_status") == "MISMATCH":
        return "WOULD_REJECT"
    operation_generation = value.get("operation_generation")
    if operation_generation is not None and operation_generation != value.get("maintenance_generation"):
        return "WOULD_REJECT"
    if not planned:
        return "WOULD_BLOCK"
    authorization = value.get("authorization")
    if not isinstance(authorization, dict):
        return "WOULD_REJECT"
    expected_executor = "planned_rollout_executor" if value.get("role") == "PLANNED_EXECUTOR" else "manual_planned_executor"
    valid = (
        authorization.get("authorization_kind") == "MAINTENANCE_MUTATION"
        and authorization.get("state") == "ACCEPTED"
        and authorization.get("single_use") is True
        and authorization.get("use_count") == 0
        and authorization.get("maintenance_generation") == value.get("maintenance_generation")
        and authorization.get("executor_id") == expected_executor
        and authorization.get("operation") == value.get("operation")
        and authorization.get("resource_identity") == value.get("resource_identity")
        and authorization.get("target_status") == "EXACT"
        and authorization.get("not_expired") is True
    )
    return "WOULD_ALLOW" if valid else "WOULD_REJECT"


def scenario_violations(case: dict[str, Any]) -> list[str]:
    violations: list[str] = []
    observations = case.get("observations")
    if not isinstance(observations, list):
        return ["P1_OBSERVATIONS_MISSING"]
    required_phases = [str(item) for item in case.get("required_phases", [])]
    actual_phases = [str(item.get("phase") or "") for item in observations if isinstance(item, dict)]
    for phase in required_phases:
        if phase not in actual_phases:
            violations.append(f"P1_REQUIRED_PHASE_MISSING:{phase}")
    for observation in observations:
        if not isinstance(observation, dict):
            violations.append("P1_OBSERVATION_INVALID")
            continue
        expected = expected_verdict(dict(observation.get("oracle_input") or {}))
        actual = str(observation.get("audit_verdict") or "")
        if actual != expected:
            violations.append(f"P1_VERDICT_MISMATCH:{observation.get('phase')}:{actual}!={expected}")
        if observation.get("production_behavior_modified") is not False:
            violations.append("P1_AUDIT_MODIFIED_PRODUCTION_BEHAVIOR")
        if int(observation.get("audit_triggered_restart_count") or 0) != 0:
            violations.append("P1_AUDIT_TRIGGERED_RESTART")
    return violations
