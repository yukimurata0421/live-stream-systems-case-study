from __future__ import annotations

from pathlib import Path

from cra_harness.mutation_controls import run_mutation_controls

ROOT = Path(__file__).resolve().parents[3]


def test_all_coverage_closure_mutants_are_detected(tmp_path: Path) -> None:
    report = run_mutation_controls(ROOT, tmp_path / "mutations")

    assert report["classification"] == "PASS"
    assert report["mutation_count"] == 8
    assert report["detected_mutation_count"] == 8
    assert report["detection_rate"] == 1.0
    assert {item["mutation_identity"] for item in report["mutations"]} == {
        "finite_check_deleted",
        "boolean_rejection_deleted",
        "effect_counter_comparison_deleted",
        "epoch_baseline_drift_check_deleted",
        "target_transition_conservation_deleted",
        "unresolved_age_check_deleted",
        "safe_blocked_duration_check_deleted",
        "collector_serialization_deleted",
    }
