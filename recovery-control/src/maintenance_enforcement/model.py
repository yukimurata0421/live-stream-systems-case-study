from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from maintenance_audit import (
    AuditObservation,
    AuditPhase,
    AuditVerdict,
    evaluate_audit_decision,
    target_identity_complete,
)


class EnforcementDisposition(StrEnum):
    ALLOW = "ALLOW"
    BLOCK = "BLOCK"
    ACK = "ACK"
    REJECT = "REJECT"
    NOT_APPLICABLE = "NOT_APPLICABLE"


@dataclass(frozen=True)
class EnforcementShadowResult:
    disposition: EnforcementDisposition
    reason_code: str
    audit_verdict: AuditVerdict
    enforcement_enabled: bool = False
    production_branch_signal: bool | None = None
    production_adapter_connected: bool = False
    production_behavior_modified: bool = False
    physical_effect_count: int = 0
    effect_boundary_revalidated: bool = False


def _require_disabled(enforcement_enabled: bool) -> None:
    if enforcement_enabled:
        raise ValueError("P2 activation is prohibited: enforcement_enabled must remain false")


def evaluate_enforcement_shadow(
    observation: AuditObservation,
    snapshot: Mapping[str, Any] | None,
    *,
    enforcement_enabled: bool = False,
) -> EnforcementShadowResult:
    """Evaluate future enforcement semantics without returning a branch signal."""

    _require_disabled(enforcement_enabled)
    audit = evaluate_audit_decision(observation, snapshot)
    disposition = {
        AuditVerdict.WOULD_ALLOW: EnforcementDisposition.ALLOW,
        AuditVerdict.WOULD_BLOCK: EnforcementDisposition.BLOCK,
        AuditVerdict.WOULD_ACK: EnforcementDisposition.ACK,
        AuditVerdict.WOULD_REJECT: EnforcementDisposition.REJECT,
        AuditVerdict.UNKNOWN: EnforcementDisposition.BLOCK,
        AuditVerdict.NOT_APPLICABLE: EnforcementDisposition.NOT_APPLICABLE,
    }[audit.verdict]
    reason = f"FAIL_CLOSED_{audit.reason_code}" if audit.verdict == AuditVerdict.UNKNOWN else audit.reason_code
    return EnforcementShadowResult(
        disposition,
        reason,
        audit.verdict,
        effect_boundary_revalidated=observation.phase == AuditPhase.EFFECT_BOUNDARY,
    )


def evaluate_establishment_shadow(
    acknowledgements: Mapping[str, Mapping[str, Any]],
    required_mutators: Sequence[str],
    *,
    maintenance_generation: int,
    target_identity: Mapping[str, Any],
    enforcement_enabled: bool = False,
) -> EnforcementShadowResult:
    """Evaluate generation-bound quiesce ACK completeness without changing state."""

    _require_disabled(enforcement_enabled)
    if not target_identity_complete(target_identity):
        return EnforcementShadowResult(
            EnforcementDisposition.BLOCK,
            "FAIL_CLOSED_TARGET_IDENTITY_UNKNOWN",
            AuditVerdict.UNKNOWN,
        )
    for mutator in required_mutators:
        ack = acknowledgements.get(mutator)
        if not isinstance(ack, Mapping):
            return EnforcementShadowResult(
                EnforcementDisposition.BLOCK,
                f"FAIL_CLOSED_ACK_MISSING:{mutator}",
                AuditVerdict.UNKNOWN,
            )
        if int(ack.get("maintenance_generation") or 0) != maintenance_generation:
            return EnforcementShadowResult(
                EnforcementDisposition.REJECT,
                f"ACK_GENERATION_MISMATCH:{mutator}",
                AuditVerdict.WOULD_REJECT,
            )
        if ack.get("target_identity") != target_identity:
            return EnforcementShadowResult(
                EnforcementDisposition.REJECT,
                f"ACK_TARGET_MISMATCH:{mutator}",
                AuditVerdict.WOULD_REJECT,
            )
        if str(ack.get("in_flight_status") or "") != "CONFIRMED":
            return EnforcementShadowResult(
                EnforcementDisposition.BLOCK,
                f"FAIL_CLOSED_IN_FLIGHT_UNKNOWN:{mutator}",
                AuditVerdict.UNKNOWN,
            )
        if int(ack.get("in_flight_count") or 0) != 0:
            return EnforcementShadowResult(
                EnforcementDisposition.REJECT,
                f"IN_FLIGHT_NOT_ZERO:{mutator}",
                AuditVerdict.WOULD_REJECT,
            )
    return EnforcementShadowResult(
        EnforcementDisposition.ACK,
        "ALL_GENERATION_BOUND_QUIESCE_ACKS_ELIGIBLE",
        AuditVerdict.WOULD_ACK,
    )
