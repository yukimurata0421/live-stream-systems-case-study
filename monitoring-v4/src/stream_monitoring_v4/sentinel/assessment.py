from __future__ import annotations

from .checks import evaluate_checks
from .facts import derive_facts
from .models import SentinelAssessment, SentinelEvidence
from .payload import build_payload


def assess(evidence: SentinelEvidence) -> SentinelAssessment:
    """Reduce collected evidence into named checks and a stable status payload."""

    facts = derive_facts(evidence)
    checks = evaluate_checks(evidence, facts)
    failed_checks = tuple(name for name, passed in checks.items() if not passed)
    return SentinelAssessment(
        payload=build_payload(evidence, facts, healthy=not failed_checks),
        failed_checks=failed_checks,
    )


__all__ = ["SentinelAssessment", "SentinelEvidence", "assess"]
