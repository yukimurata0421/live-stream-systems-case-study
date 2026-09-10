from __future__ import annotations

from cra_harness.runner.no_action_randomized import run_randomized_no_action_harness


def test_randomized_no_action_harness_is_deterministic_and_has_no_production_effects() -> None:
    first = run_randomized_no_action_harness(cases=128, seed=20_260_901)
    second = run_randomized_no_action_harness(cases=128, seed=20_260_901)

    assert first["result"] == "PASS"
    assert first["case_results_sha256"] == second["case_results_sha256"]
    assert first["scenario_counts"] == second["scenario_counts"]
    assert first["gate_case_count"] > first["adapter_case_count"] > 0
    assert first["coverage_matrix_complete"] is True
    assert first["maximum_scenario_variant_count"] - first["minimum_scenario_variant_count"] <= 1
    assert set(first["scenario_counts"]) >= {
        "cra_no_action_soak.gate.evaluate_no_action_soak:target_change_without_transition",
        "monitoring_projection.live_adapter.MonitoringLiveAdapter._latest_consistent_snapshot:transient_current_newer",
    }
    assert first["boundary"] == {
        "production_state_read_count": 0,
        "credential_read_count": 0,
        "network_call_count": 0,
        "production_mutation_count": 0,
        "physical_effect_count": 0,
        "live_port_bind_count": 0,
    }
    assert first["formal_soak_replacement"] is False
