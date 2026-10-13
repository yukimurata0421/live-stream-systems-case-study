from __future__ import annotations

import pytest

from cra_harness import interserver_resilience_chaos as model
from cra_harness.interserver_resilience_chaos import FAULTS, run_interserver_resilience_chaos


def test_campaign_report_does_not_claim_real_transport_from_model_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(model, "campaign_rows", lambda **_kwargs: ((0, 0, 0, 0, 0, 0, 0, 0, 0),))
    monkeypatch.setattr(model, "coverage_report", lambda *_args, **_kwargs: {"coverage_complete": False})
    report = run_interserver_resilience_chaos(case_count=70_000, seed=1)
    assert report["evidence_profile"] == "MODEL_ONLY_CLASSIFIER_AND_STATE_MACHINE"
    assert report["real_transport_executed"] is False
    assert report["fidelity"]["real_recovery_transport_called"] is False
    assert report["pass"] is False


def test_interserver_resilience_campaign_covers_two_hop_faults_and_detects_negative_controls() -> None:
    report = run_interserver_resilience_chaos(case_count=71_000, seed=20260902)
    assert report["pass"] is True
    assert report["failure_count"] == 0
    assert report["cross_hop_fault_pair_count"] == len(FAULTS) ** 2
    assert report["four_way_coverage"]["coverage_complete"] is True
    assert all(report["negative_control_detection"].values())
    assert report["safety"]["physical_effect_count"] == 0
    assert report["fidelity"]["physical_l2_l3_faults"] is False
    assert report["evidence_profile"] == "MODEL_ONLY_CLASSIFIER_AND_STATE_MACHINE"
    assert report["real_transport_executed"] is False
    assert report["real_transport_qualification"] == "SEPARATE_LOOPBACK_MTLS_TEST_REQUIRED"
