from __future__ import annotations

from tools.run_v21_incident_matrix import EXPECTED_SCENARIOS, MINIMUM_SCENARIOS, execute_matrix


def test_v21_closed_incident_matrix_is_exhaustive_and_above_requested_floor() -> None:
    report = execute_matrix()

    assert EXPECTED_SCENARIOS == 53_760
    assert EXPECTED_SCENARIOS >= MINIMUM_SCENARIOS
    assert report["result"] == "PASS"
    assert report["counters"]["scenarios"] == EXPECTED_SCENARIOS
    assert report["counters"]["invariant_checks"] == EXPECTED_SCENARIOS * 6
    assert report["counters"]["physical_effect_calls"] == 0
    assert report["isolation"] == {
        "external_network_calls": 0,
        "production_effect_socket_touched": False,
        "production_sqlite_touched": False,
        "production_target_touched": False,
        "temporary_files_only": True,
    }
