from __future__ import annotations

from cra_harness.operational.fixtures import negative_controls, target_policy_comparison
from cra_harness.operational.model import HIGH_RISK_CELLS, build_operational_scenarios, mandatory_scenarios, matches_risk
from cra_harness.operational.simulator import simulate_candidate_policy
from cra_harness.oracles.operational_v3 import detect_negative_control, evaluate_operational_invariants
from cra_harness.reporting.coverage_v3 import build_coverage


def test_operational_randomized_profile_has_500_cases_and_mandatory_hits() -> None:
    scenarios = build_operational_scenarios((20260823, 20260824, 20260825, 20260826, 20260827))
    assert len(scenarios) == 500
    for risk in HIGH_RISK_CELLS:
        assert sum(matches_risk(scenario, risk) for scenario in scenarios) >= 20


def test_operational_candidate_satisfies_independent_invariants() -> None:
    scenarios = mandatory_scenarios() + build_operational_scenarios((20260823, 20260824, 20260825, 20260826, 20260827))
    for scenario in scenarios:
        observation = simulate_candidate_policy(scenario)
        assert evaluate_operational_invariants(scenario.to_dict(), observation) == []


def test_coverage_materializes_only_explored_cells_and_no_mandatory_gap() -> None:
    scenarios = build_operational_scenarios((20260823, 20260824, 20260825, 20260826, 20260827))
    outcomes = [
        {"scenario_id": scenario.scenario_id, "classification": "PASS", "observations": simulate_candidate_policy(scenario)}
        for scenario in scenarios
    ]
    matrix, markdown, uncovered = build_coverage(scenarios, outcomes)
    assert matrix["total_possible_modeled_cells"] == 165_888
    assert 0 < matrix["explored_cells"] <= 500
    assert matrix["high_risk_uncovered"] == 0
    assert uncovered["uncovered_high_risk_cells"] == []
    assert "High-risk uncovered: 0" in markdown


def test_all_fifteen_negative_controls_are_detected() -> None:
    controls = negative_controls()
    assert [item["scenario_id"] for item in controls] == [f"NC-{index:02d}" for index in range(1, 16)]
    for control in controls:
        violations = detect_negative_control(control)
        assert len(violations) == 1


def test_target_unavailable_option_b_is_a_proposal_not_accepted_contract() -> None:
    comparison = target_policy_comparison()
    assert comparison["recommendation"] == "B"
    assert comparison["decision_status"] == "PROPOSED_NOT_ACCEPTED"
    assert comparison["required_contract_change"] is True
