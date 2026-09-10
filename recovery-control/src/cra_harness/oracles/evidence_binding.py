from __future__ import annotations

from collections.abc import Mapping
from typing import Any

EXACT_TARGET_FIELDS = frozenset(
    {
        "host_id",
        "host_boot_id",
        "namespace",
        "pod_uid",
        "container_name",
        "container_id",
        "ffmpeg_generation",
        "ffmpeg_pid",
    }
)


def independent_expected(case: Mapping[str, Any]) -> str:
    """Compute evidence-binding truth without importing production code."""

    domain = str(case["domain"])
    if domain == "HOST":
        left = case.get("left")
        right = case.get("right")
        if not isinstance(left, Mapping) or not isinstance(right, Mapping):
            return "UNKNOWN"
        if not left.get("host_id") or not right.get("host_id"):
            return "UNKNOWN"
        same = left.get("host_id") == right.get("host_id") and left.get("host_boot_id") == right.get("host_boot_id")
        return "SAME" if same else "DIFFERENT"

    if domain == "SNAPSHOT":
        required = (
            bool(case.get("exists")),
            bool(case.get("integrity_ok")),
            bool(case.get("fresh")),
            bool(case.get("startup_reconciled")),
            int(case.get("unresolved_count") or 0) == 0,
            int(case.get("active_fence_count") or 0) == 0,
            not bool(case.get("transaction_uncertain")),
        )
        if not all(required):
            return "UNKNOWN"
        candidate_generation = case.get("candidate_generation")
        current_generation = case.get("current_generation")
        if candidate_generation is not None and current_generation is not None and int(candidate_generation) < int(current_generation):
            return "REJECT"
        return "INACTIVE" if case.get("maintenance_state") == "INACTIVE" else "ACTIVE"

    if domain == "TARGET":
        expected = case.get("expected")
        observed = case.get("observed")
        if not bool(case.get("fresh")):
            return "UNKNOWN"
        if not isinstance(expected, Mapping) or not isinstance(observed, Mapping):
            return "UNKNOWN"
        if set(expected) != EXACT_TARGET_FIELDS or set(observed) != EXACT_TARGET_FIELDS:
            return "UNKNOWN"
        return "MATCH" if dict(expected) == dict(observed) else "MISMATCH"

    if domain == "GENERATION":
        if not bool(case.get("maintenance_mapping_confirmed")):
            return "MISSING"
        operation_generation = case.get("operation_generation")
        if operation_generation is None:
            return "MISSING"
        return "CURRENT" if int(operation_generation) == int(case["current_generation"]) else "STALE"

    raise ValueError(f"unsupported oracle domain: {domain}")


def evidence_binding_violations(case: Mapping[str, Any]) -> list[str]:
    expected = independent_expected(case)
    observed = str(case.get("observed_result") or "")
    return [] if observed == expected else [f"{case['control_id']}:{observed}!={expected}"]
