from __future__ import annotations

from itertools import product

from cra_harness.covering_array import coverage_report, generate_covering_array
from cra_harness.runner.no_action_covering import FACTOR_SIZES, _mandatory_high_risk_rows, execute_rows


def test_mixed_level_covering_array_is_complete_unique_and_deterministic() -> None:
    sizes = (4, 3, 3, 2, 2)
    mandatory = tuple((*values, 0, 0) for values in product(range(2), range(2), range(2)))
    first = generate_covering_array(sizes, strength=3, target_count=96, mandatory_rows=mandatory, seed=20260902)
    second = generate_covering_array(sizes, strength=3, target_count=96, mandatory_rows=mandatory, seed=20260902)

    assert first == second
    assert len(first) == len(set(first)) == 96
    assert set(mandatory) <= set(first)
    assert coverage_report(first, sizes, strength=3) == {
        "strength": 3,
        "expected_combination_count": 208,
        "observed_combination_count": 208,
        "missing_combination_count": 0,
        "coverage_complete": True,
    }


def test_each_fault_scenario_executes_real_gate_with_bound_factor_values() -> None:
    rows = tuple((index, *(0 for _ in FACTOR_SIZES[1:])) for index in range(FACTOR_SIZES[0]))
    report = execute_rows(rows, seed=20260902)

    assert report["case_count"] == report["passed_case_count"] == 25
    assert report["failure_count"] == 0
    assert len(report["scenario_counts"]) == 25
    assert report["boundary"] == {
        "production_state_read_count": 0,
        "credential_read_count": 0,
        "network_call_count": 0,
        "production_mutation_count": 0,
        "live_port_bind_count": 0,
    }


def test_high_risk_rows_include_compound_faults_and_extreme_boundaries() -> None:
    rows = _mandatory_high_risk_rows()

    assert len(rows) == len(set(rows)) == 972
    assert {row[0] for row in rows} == {22, 23, 24}
    assert {row[2] for row in rows} == {0, 2, 4, 5}
    assert {row[3] for row in rows} == {1, 2, 5}
    assert {row[5] for row in rows} == {0, 3, 4}
    assert {row[6] for row in rows} == {0, 3, 4}
    assert {row[10] for row in rows} == {0, 2, 3}
