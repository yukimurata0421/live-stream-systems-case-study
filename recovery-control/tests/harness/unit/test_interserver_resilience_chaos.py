from __future__ import annotations

from cra_harness.interserver_resilience_chaos import FAULTS, run_interserver_resilience_chaos


def test_interserver_resilience_campaign_covers_two_hop_faults_and_detects_negative_controls() -> None:
    report = run_interserver_resilience_chaos(case_count=70_000, seed=20260902)
    assert report["pass"] is True
    assert report["failure_count"] == 0
    assert report["cross_hop_fault_pair_count"] == len(FAULTS) ** 2
    assert report["four_way_coverage"]["coverage_complete"] is True
    assert all(report["negative_control_detection"].values())
    assert report["safety"]["physical_effect_count"] == 0
    assert report["fidelity"]["physical_l2_l3_faults"] is False
