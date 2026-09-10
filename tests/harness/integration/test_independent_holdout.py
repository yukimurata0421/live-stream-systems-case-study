from __future__ import annotations

from cra_harness.holdout import run_holdout_harness


def test_independent_state_machine_holdout_covers_pre_and_post_boundary_reentry() -> None:
    report = run_holdout_harness(seeds=(20260911, 20260912), cases_per_seed=24)

    assert report["classification"] == "PASS"
    assert report["case_count"] == 48
    assert report["sut_failure_count"] == 0
    assert report["harness_failure_count"] == 0
    assert report["boundary_reached_case_count"] > 0
    assert report["pre_boundary_case_count"] > 0
    assert report["restart_reentry_case_count"] > 0
    assert report["simulated_operation_count"] >= report["case_count"] * 8
    assert report["safety"]["physical_effect_count"] == 0
