from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def expected_mp03_verdict(fixture: Mapping[str, Any]) -> dict[str, Any]:
    """Independent R1 oracle; intentionally imports no production evaluator."""

    if not fixture.get("maintenance_available"):
        audit = "UNKNOWN"
        reason = "MAINTENANCE_STATE_UNAVAILABLE"
    elif not fixture.get("maintenance_fresh"):
        audit = "UNKNOWN"
        reason = "MAINTENANCE_STATE_STALE"
    elif not fixture.get("maintenance_active"):
        audit = "WOULD_ALLOW"
        reason = "MAINTENANCE_INACTIVE"
    elif not fixture.get("target_valid"):
        audit = "UNKNOWN"
        reason = "MAINTENANCE_TARGET_UNKNOWN"
    elif not fixture.get("target_match"):
        audit = "WOULD_REJECT"
        reason = "TARGET_IDENTITY_MISMATCH"
    elif not fixture.get("generation_match"):
        audit = "WOULD_REJECT"
        reason = "STALE_MAINTENANCE_GENERATION"
    else:
        audit = "WOULD_BLOCK"
        reason = "MAINTENANCE_FENCE_ACTIVE"
    p2 = {
        "WOULD_ALLOW": "ALLOW",
        "WOULD_BLOCK": "BLOCK",
        "WOULD_REJECT": "REJECT",
        "UNKNOWN": "BLOCK",
    }[audit]
    return {
        "audit_verdict": audit,
        "audit_reason": reason,
        "p2_disabled_verdict": p2,
        "effect_boundary_revalidated": fixture.get("phase") == "EFFECT_BOUNDARY",
        "production_behavior_modified": False,
        "production_branch_signal": None,
        "physical_effect_count": 0,
    }


def injected_control_violations(control: Mapping[str, Any]) -> list[str]:
    """Detect forbidden R1 semantics from a normalized evidence fixture."""

    violations: list[str] = []
    if control.get("audit_changed_production_decision"):
        violations.append("AUDIT_CHANGED_PRODUCTION_DECISION")
    if control.get("p2_blocked_action"):
        violations.append("P2_DISABLED_BLOCKED_ACTION")
    if control.get("audit_exception_stopped_loop"):
        violations.append("AUDIT_EXCEPTION_PROPAGATED")
    if control.get("snapshot_status") == "MISSING" and control.get("audit_verdict") == "WOULD_ALLOW":
        violations.append("MISSING_SNAPSHOT_ALLOWED")
    if control.get("target_status") == "STALE" and control.get("target_treated_exact"):
        violations.append("STALE_TARGET_TREATED_EXACT")
    if control.get("host_id_source") == "OS_HOSTNAME":
        violations.append("HOSTNAME_USED_AS_PROTOCOL_HOST_ID")
    if control.get("native_id_used_as_maintenance_generation"):
        violations.append("NATIVE_ID_MAPPED_TO_MAINTENANCE_GENERATION")
    if control.get("audit_absence_used_as_in_flight_zero"):
        violations.append("AUDIT_ABSENCE_USED_AS_IN_FLIGHT_ZERO")
    if control.get("phase") == "EFFECT_BOUNDARY" and not control.get("effect_boundary_revalidated", True):
        violations.append("EFFECT_BOUNDARY_REVALIDATION_MISSING")
    if control.get("enforcement_enabled"):
        violations.append("ENFORCEMENT_FLAG_TRUE")
    return violations
