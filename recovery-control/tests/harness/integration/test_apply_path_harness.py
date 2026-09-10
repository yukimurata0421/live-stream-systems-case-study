from __future__ import annotations

from pathlib import Path

from cra_harness.apply_path import run_apply_path_harness

ROOT = Path(__file__).resolve().parents[3]


def test_apply_path_reaches_real_policy_command_fence_verification_and_reconciliation(tmp_path: Path) -> None:
    report = run_apply_path_harness(ROOT, tmp_path / "apply-path")

    assert report["classification"] == "PASS"
    assert report["passed_scenario_count"] == report["scenario_count"] == 8
    assert report["synthetic_effect_boundary_count"] == 4
    assert report["physical_effect_count"] == 0
    assert report["ffmpeg_signal_count"] == 0
    assert report["production_database_used"] is False
    scenarios = {item["scenario"]: item for item in report["scenarios"]}
    assert scenarios["outcome_unknown_no_retry"]["automatic_retry_count"] == 0
    assert scenarios["different_ids_same_effect_scope"]["reason"] == "CENTRAL_LOGICAL_GENERATION_ALREADY_FENCED"
    assert scenarios["reconciling_has_no_effect"]["synthetic_effect_boundary_count"] == 0
    assert scenarios["crash_before_effect_boundary_releases_only_uncommitted_fence"]["synthetic_effect_boundary_count"] == 0
    assert scenarios["crash_after_effect_boundary_never_retries"]["automatic_retry_count"] == 0
