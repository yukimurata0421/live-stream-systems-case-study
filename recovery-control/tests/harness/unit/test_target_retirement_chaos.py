from __future__ import annotations

from cra_harness.target_retirement import MUTATIONS, run_target_retirement_chaos


def test_target_retirement_chaos_covers_all_negative_controls() -> None:
    report = run_target_retirement_chaos(case_count=32, seed=20260901)

    assert report["pass"] is True
    assert report["failure_count"] == 0
    assert report["valid_case_count"] > 0
    assert report["idempotent_replay_count"] == report["valid_case_count"]
    assert set(report["negative_control_detected"]) == set(MUTATIONS) - {"VALID"}
    assert all(value > 0 for value in report["negative_control_detected"].values())
    assert report["physical_effect_adapter_success_count"] == 0
    assert report["production_target_touched"] is False
