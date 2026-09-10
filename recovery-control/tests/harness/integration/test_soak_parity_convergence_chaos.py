from __future__ import annotations

from cra_harness.soak_parity_chaos import run


def test_soak_parity_convergence_chaos_has_no_oracle_escape() -> None:
    result = run(cases=1_000, seed=20260901)

    assert result["failure_count"] == 0
    assert result["production_touched"] is False
    assert set(result["scenario_counts"]) == {
        "converged",
        "pending",
        "timeout",
        "late_proof",
        "wrong_source",
        "policy_drift",
        "normalization_error",
        "cycle_mutation",
        "count_contradiction",
        "self_accepted",
        "input_integrity",
        "readiness_contradiction",
        "identical_cycle_replay",
    }
