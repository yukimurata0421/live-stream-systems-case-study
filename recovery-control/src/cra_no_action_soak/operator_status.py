"""Operator-facing classification for recovery-soak results.

The raw gate remains the fail-closed machine contract.  This module projects
that contract into explicit operator vocabulary without turning unavailable
evidence into healthy SUT evidence and without exposing a bare UNKNOWN label.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

OPERATOR_STATUS_SCHEMA = "cra.operator_status.v1"
OPERATOR_REPORT_SCHEMA = "cra.operator_report.v1"

_SUT_BLOCKERS = frozenset(
    {
        "BOUNDED_TERMINATION_NOT_ACTIVE",
        "CRA_NO_ACTION_SAFETY_VIOLATED",
        "DELL_EFFECT_SAFETY_VIOLATED",
        "NO_ACTION_PHYSICAL_EFFECT_DETECTED",
        "RECOVERY_DEADLINE_EXCEEDED",
        "FORMAL_GATE_NOT_HEALTHY",
    }
)

_ENVIRONMENT_BLOCKERS = frozenset(
    {
        "EVIDENCE_CAPACITY_EXHAUSTED",
        "EVIDENCE_CAPACITY_NOT_PROVEN",
        "FILESYSTEM_HEADROOM_LOW",
    }
)

_HARNESS_BLOCKERS = frozenset(
    {
        "COLLECTOR_GAP_OR_CLOCK_REGRESSION",
        "COLLECTOR_STALE",
        "FORMAL_GATE_EVALUATION_UNTRUSTED",
        "FORMAL_GATE_STALE_OR_UNBOUND",
        "FORMAL_GATE_UNAVAILABLE",
        "LIVE_RECOVERY_CHECKPOINT_MISSING",
        "LIVE_STATE_EVIDENCE_NOT_SYNCHRONIZED",
        "SAMPLE_FROM_FUTURE",
        "SAMPLE_INPUT_ROLES_INVALID",
    }
)


def _display_code(code: object) -> str:
    """Return a stable operator code with uncertainty expressed as UNVERIFIED."""
    value = str(code) if isinstance(code, str) and code else "UNCLASSIFIED_EVIDENCE_CONDITION"
    return value.replace("UNKNOWN", "UNVERIFIED")


def _evidence_status(code: str) -> str:
    if "PERMISSION_DENIED" in code or "ACCESS_DENIED" in code:
        return "ACCESS_DENIED"
    if "STALE" in code or code in {"COLLECTOR_GAP_OR_CLOCK_REGRESSION", "SAMPLE_FROM_FUTURE"}:
        return "STALE"
    if "READ_FAILURE" in code or "READ_FAILED" in code or "OS_READ_FAILED" in code:
        return "READ_ERROR"
    if any(token in code for token in ("IDENTITY", "SIGNATURE", "SIGNED_EVIDENCE", "SEQUENCE_REGRESSION", "CONFLICT")):
        return "IDENTITY_CONFLICT"
    if any(token in code for token in ("INVALID", "MISMATCH", "REGRESSION", "VIOLATED")):
        return "INVALID"
    if any(token in code for token in ("MISSING", "UNAVAILABLE", "REQUIRED", "UNVERIFIED", "UNPROVEN", "NOT_SYNCHRONIZED")):
        return "MISSING"
    return "UNVERIFIED"


def _failure_domain(code: str, *, source: str) -> str:
    if source == "oracle":
        return "ORACLE"
    if source == "environment":
        return "ENVIRONMENT"
    if code in _SUT_BLOCKERS:
        return "SUT"
    if code in _ENVIRONMENT_BLOCKERS or "CAPACITY" in code or "FILESYSTEM" in code:
        return "ENVIRONMENT"
    if code in _HARNESS_BLOCKERS or source == "harness":
        return "HARNESS"
    if code.startswith("FORMAL_GATE_") or code.startswith("LIVE_CHECKPOINT_") or code.startswith("SAMPLE_CHAIN_"):
        return "HARNESS"
    if "TRANSPORT" in code and code not in {
        "TRANSPORT_SOURCE_TIME_CONFLICT",
        "TRANSPORT_SOURCE_TIME_REGRESSION",
        "TRANSPORT_TARGET_INVALID",
    }:
        return "TRANSPORT"
    return "OBSERVATION_SOURCE"


def _finding(code: object, *, source: str) -> dict[str, str]:
    display = _display_code(code)
    domain = _failure_domain(display, source=source)
    return {
        "code": display,
        "evidence_status": "COMPLETE" if domain == "SUT" else _evidence_status(display),
        "failure_domain": domain,
        "soak_impact": "FAIL" if domain == "SUT" else "CERTIFICATION_PAUSED",
    }


def _aggregate(values: set[str], *, empty: str) -> str:
    if not values:
        return empty
    if len(values) == 1:
        return next(iter(values))
    return "MULTIPLE"


def classify_gate(result: Mapping[str, Any]) -> dict[str, Any]:
    """Classify a raw gate or watchdog result for human consumption."""
    findings: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add(code: object, *, source: str) -> None:
        item = _finding(code, source=source)
        identity = (item["code"], item["failure_domain"])
        if identity not in seen:
            seen.add(identity)
            findings.append(item)

    for code in result.get("oracle_errors", []) if isinstance(result.get("oracle_errors"), list) else []:
        add(code, source="oracle")
    harness_classification = result.get("harness_classification")
    if harness_classification == "ENVIRONMENT_FAILURE":
        add("EVIDENCE_ENVIRONMENT_NOT_PROVEN", source="environment")
    elif harness_classification == "HARNESS_FAILURE" and not result.get("oracle_errors"):
        add("EVIDENCE_HARNESS_EVALUATION_FAILED", source="harness")
    for code in result.get("blockers", []) if isinstance(result.get("blockers"), list) else []:
        add(code, source="blocker")
    for code in result.get("unknown_reasons", []) if isinstance(result.get("unknown_reasons"), list) else []:
        add(code, source="evidence")

    unresolved = result.get("owner_unverified_diagnostic_code_counts", {})
    if isinstance(unresolved, dict):
        for code, count in unresolved.items():
            if isinstance(count, int) and not isinstance(count, bool) and count > 0:
                add(code, source="evidence")

    notices: list[dict[str, Any]] = []
    resolved = result.get("owner_read_diagnostic_code_counts", {})
    if isinstance(resolved, dict):
        unresolved_codes = set(unresolved) if isinstance(unresolved, dict) else set()
        for code, count in sorted(resolved.items()):
            if (
                code not in unresolved_codes
                and isinstance(code, str)
                and isinstance(count, int)
                and not isinstance(count, bool)
                and count > 0
            ):
                suffix = _display_code(code.removeprefix("RECOVERY_OWNER_"))
                notices.append(
                    {
                        "code": f"OWNER_{suffix}_REVALIDATED",
                        "count": count,
                        "impact": "NONE",
                    }
                )

    lifecycle = result.get("owner_lifecycle_sample_counts", {})
    lifecycle_counts: dict[str, int] = {}
    if isinstance(lifecycle, dict):
        for state, count in lifecycle.items():
            if isinstance(count, int) and not isinstance(count, bool):
                key = "UNVERIFIED" if state == "UNKNOWN" else _display_code(state)
                lifecycle_counts[key] = lifecycle_counts.get(key, 0) + count

    domains = {item["failure_domain"] for item in findings}
    evidence_states = {item["evidence_status"] for item in findings}
    failure_domain = _aggregate(domains, empty="NONE")
    evidence_status = _aggregate(evidence_states, empty="COMPLETE")
    live_health = result.get("live_health")

    if "SUT" in domains:
        current_sut_state = "CONFIRMED_FAILURE"
    elif findings:
        current_sut_state = "RECOVERING" if live_health == "RECOVERING" else "NOT_OBSERVABLE"
    elif live_health == "READY":
        current_sut_state = "VERIFIED_HEALTHY"
    elif live_health == "RECOVERING":
        current_sut_state = "RECOVERING"
    else:
        current_sut_state = "NOT_OBSERVABLE"

    if "SUT" in domains:
        soak_impact = "FAIL"
        summary_code = "SUT_FAILURE_CONFIRMED"
    elif findings:
        soak_impact = "CERTIFICATION_PAUSED"
        summary_code = "EVIDENCE_CLASSIFIED_AND_CERTIFICATION_PAUSED"
    else:
        formal_status = result.get("soak_status", result.get("formal_status"))
        if formal_status == "PASS":
            soak_impact = "NONE"
            summary_code = "SOAK_REQUIREMENTS_SATISFIED"
        else:
            soak_impact = "CERTIFICATION_PENDING"
            summary_code = "HEALTHY_SOAK_PENDING_REQUIREMENTS" if current_sut_state == "VERIFIED_HEALTHY" else "SOAK_PENDING_REQUIREMENTS"

    return {
        "schema": OPERATOR_STATUS_SCHEMA,
        "current_sut_state": current_sut_state,
        "evidence_status": evidence_status,
        "failure_domain": failure_domain,
        "soak_impact": soak_impact,
        "summary_code": summary_code,
        "lifecycle_counts": lifecycle_counts,
        "findings": sorted(findings, key=lambda item: (item["failure_domain"], item["code"])),
        "notices": notices,
    }


def operator_report(result: Mapping[str, Any]) -> dict[str, Any]:
    """Return the compact projection used by systemd/journal operators."""
    status = result.get("operator_status")
    if not isinstance(status, dict):
        status = classify_gate(result)
    return {
        "schema": OPERATOR_REPORT_SCHEMA,
        "epoch_id": result.get("epoch_id"),
        "evaluated_at": result.get("evaluated_at", result.get("observed_at")),
        "sample_count": result.get("sample_count"),
        "operator_status": status,
    }
