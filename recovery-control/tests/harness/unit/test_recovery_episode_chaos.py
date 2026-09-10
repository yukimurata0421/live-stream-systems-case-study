from __future__ import annotations

from cra_dell_recovery.transport_resilience import FailureClassification, classify_failure
from cra_harness.recovery_episode_chaos import FAULTS, run_recovery_episode_chaos


def test_recovery_episode_chaos_covers_every_fault_family() -> None:
    report = run_recovery_episode_chaos(case_count=len(FAULTS), seed=20260902)
    assert report["pass"] is True
    assert report["failure_count"] == 0
    assert report["control_capability_count"] == 0
    assert report["physical_effect_count"] == 0


def test_recovery_episode_oracle_detects_illegal_certificate_retry() -> None:
    def mutant(error: BaseException) -> FailureClassification:
        result = classify_failure(error)
        if result.reason_code == "TLS_CERTIFICATE_REJECTED":
            return FailureClassification(result.category, result.reason_code, True)
        return result

    report = run_recovery_episode_chaos(case_count=len(FAULTS), seed=20260902, classifier=mutant)
    assert report["pass"] is False
    assert any(item["fault"] == "tls_certificate" for item in report["failures"])
