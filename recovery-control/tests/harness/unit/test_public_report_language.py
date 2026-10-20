"""Public report checks that do not require private operator documents."""

from __future__ import annotations

from cra_harness import task_contract as gate
from cra_harness.evidence_lineage import render_check


def test_owner_report_keeps_unproven_live_claims_unverified() -> None:
    value = {
        "classification": "PASS",
        "source_stable": True,
        "candidate": {"native_cycles": 64},
        "contract_gate": {"classification": "PASS", "invariant_count": 7},
        "real_ffmpeg": True,
        "formal_soak": True,
    }
    text = gate.report(value)
    for key in gate.UNPROVEN:
        assert f"| {key} | Not verified" in text
    del value["contract_gate"]
    assert "Overall assessment: HOLD" in gate.report(value)


def test_evidence_report_preserves_hold_and_error_codes() -> None:
    text = render_check({"status": "HOLD", "errors": ["EVIDENCE_NOT_OBSERVABLE"], "counts": {"nodes": 1}})
    assert "Assessment: HOLD" in text
    assert "Scope: nodes=1" in text
    assert "`EVIDENCE_NOT_OBSERVABLE`" in text
