from __future__ import annotations

from cra_harness.projection_transport_chaos import SCENARIOS, run_projection_transport_chaos


def test_projection_transport_chaos_exercises_retry_and_fail_closed_paths() -> None:
    report = run_projection_transport_chaos(case_count=256, seed=20260901)

    assert report["pass"] is True
    assert report["failure_count"] == 0
    assert set(report["scenario_detection_count"]) == set(SCENARIOS)
    assert all(value > 0 for value in report["scenario_detection_count"].values())
    assert report["security_or_semantic_retry_count"] == 0
    assert report["production_target_touched"] is False
