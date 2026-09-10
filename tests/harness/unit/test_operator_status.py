from __future__ import annotations

import json
from typing import Any

import pytest

from cra_no_action_soak.operator_status import classify_gate, operator_report


def raw_result(**changes: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "epoch_id": "operator-fixture",
        "evaluated_at": "2026-09-09T03:00:00.000Z",
        "sample_count": 10,
        "live_health": "READY",
        "status": "NOT_YET_ELIGIBLE",
        "soak_status": "NOT_YET_ELIGIBLE",
        "formal_status": "NOT_YET_ELIGIBLE",
        "harness_classification": "PASS",
        "oracle_errors": [],
        "blockers": [],
        "unknown_reasons": [],
        "owner_lifecycle_sample_counts": {
            "RUNNING": 10,
            "ABSENT": 0,
            "TRANSITION": 0,
            "UNKNOWN": 0,
            "MISSING": 0,
        },
        "owner_read_diagnostic_code_counts": {},
        "owner_unverified_diagnostic_code_counts": {},
    }
    result.update(changes)
    return result


def assert_operator_vocabulary(value: object) -> None:
    assert "UNKNOWN" not in json.dumps(value, sort_keys=True)


def test_healthy_pending_gate_is_explicit() -> None:
    status = classify_gate(raw_result())
    assert status["current_sut_state"] == "VERIFIED_HEALTHY"
    assert status["evidence_status"] == "COMPLETE"
    assert status["failure_domain"] == "NONE"
    assert status["soak_impact"] == "CERTIFICATION_PENDING"
    assert status["summary_code"] == "HEALTHY_SOAK_PENDING_REQUIREMENTS"
    assert status["lifecycle_counts"]["UNVERIFIED"] == 0
    assert_operator_vocabulary(status)


def test_required_evidence_gap_does_not_become_a_sut_failure() -> None:
    status = classify_gate(
        raw_result(
            status="UNKNOWN",
            soak_status=None,
            harness_classification="MISSING_EVIDENCE",
            unknown_reasons=["DELL_OWNER_LIFECYCLE_UNKNOWN_OUTSIDE_RECOVERY"],
            owner_lifecycle_sample_counts={"RUNNING": 9, "UNKNOWN": 1},
        )
    )
    assert status["current_sut_state"] == "NOT_OBSERVABLE"
    assert status["evidence_status"] == "MISSING"
    assert status["failure_domain"] == "OBSERVATION_SOURCE"
    assert status["soak_impact"] == "CERTIFICATION_PAUSED"
    assert status["findings"][0]["code"] == "DELL_OWNER_LIFECYCLE_UNVERIFIED_OUTSIDE_RECOVERY"
    assert_operator_vocabulary(status)


def test_owner_permission_failure_is_classified_as_access_denied() -> None:
    status = classify_gate(
        raw_result(
            status="UNKNOWN",
            soak_status=None,
            harness_classification="MISSING_EVIDENCE",
            owner_unverified_diagnostic_code_counts={"PERMISSION_DENIED": 1},
        )
    )
    assert status["evidence_status"] == "ACCESS_DENIED"
    assert status["failure_domain"] == "OBSERVATION_SOURCE"
    assert status["current_sut_state"] == "NOT_OBSERVABLE"
    assert_operator_vocabulary(status)


@pytest.mark.parametrize(
    ("changes", "domain", "evidence", "sut", "impact"),
    [
        (
            {
                "status": "UNKNOWN",
                "soak_status": None,
                "harness_classification": "HARNESS_FAILURE",
                "oracle_errors": ["LIVE_CHECKPOINT_REPLAY_MISMATCH"],
            },
            "ORACLE",
            "INVALID",
            "NOT_OBSERVABLE",
            "CERTIFICATION_PAUSED",
        ),
        (
            {"status": "FAIL", "soak_status": "FAIL", "harness_classification": "SUT_FAILURE", "blockers": ["RECOVERY_DEADLINE_EXCEEDED"]},
            "SUT",
            "COMPLETE",
            "CONFIRMED_FAILURE",
            "FAIL",
        ),
        (
            {
                "status": "UNKNOWN",
                "soak_status": None,
                "harness_classification": "ENVIRONMENT_FAILURE",
                "blockers": ["EVIDENCE_CAPACITY_NOT_PROVEN"],
            },
            "ENVIRONMENT",
            "UNVERIFIED",
            "NOT_OBSERVABLE",
            "CERTIFICATION_PAUSED",
        ),
        (
            {
                "status": "AT_RISK",
                "soak_status": None,
                "harness_classification": None,
                "live_health": "UNKNOWN",
                "blockers": ["COLLECTOR_STALE"],
            },
            "HARNESS",
            "STALE",
            "NOT_OBSERVABLE",
            "CERTIFICATION_PAUSED",
        ),
    ],
)
def test_failure_domains_are_not_collapsed_into_sut(changes: dict[str, Any], domain: str, evidence: str, sut: str, impact: str) -> None:
    status = classify_gate(raw_result(**changes))
    assert status["failure_domain"] == domain
    assert status["evidence_status"] == evidence
    assert status["current_sut_state"] == sut
    assert status["soak_impact"] == impact
    assert_operator_vocabulary(status)


def test_resolved_owner_exec_race_is_a_non_blocking_notice() -> None:
    status = classify_gate(raw_result(owner_read_diagnostic_code_counts={"RECOVERY_OWNER_AUXILIARY_EXECUTABLE_CHANGED": 20}))
    assert status["evidence_status"] == "COMPLETE"
    assert status["failure_domain"] == "NONE"
    assert status["notices"] == [
        {
            "code": "OWNER_AUXILIARY_EXECUTABLE_CHANGED_REVALIDATED",
            "count": 20,
            "impact": "NONE",
        }
    ]
    assert_operator_vocabulary(status)


def test_operator_report_excludes_raw_fail_closed_fields() -> None:
    raw = raw_result(
        status="UNKNOWN",
        soak_status=None,
        harness_classification="MISSING_EVIDENCE",
        unknown_reasons=["SOURCE_EVIDENCE_UNKNOWN_OUTSIDE_KNOWN_OUTAGE"],
    )
    raw["operator_status"] = classify_gate(raw)
    report = operator_report(raw)
    assert set(report) == {"schema", "epoch_id", "evaluated_at", "sample_count", "operator_status"}
    assert "unknown_reasons" not in report
    assert_operator_vocabulary(report)
