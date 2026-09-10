from __future__ import annotations

import json
from pathlib import Path

from cra_harness.runner.no_action_sqlite_chaos import _runtime_contract_classification, run_suite

ROOT = Path(__file__).resolve().parents[3]


def test_sqlite_version_gate_failure_is_environment_not_sut() -> None:
    assert (
        _runtime_contract_classification(
            production_gate="FAIL",
            quick_check="ok",
            foreign_key_check="ok",
            integrity_check="ok",
        )
        == "ENVIRONMENT_FAILURE"
    )


def test_no_action_sqlite_chaos_covers_storage_and_interpretation_without_effects(tmp_path: Path) -> None:
    output = tmp_path / "sqlite-chaos"
    summary = run_suite(ROOT, output, seeds=(20260901,), operations_per_seed=64)

    assert summary["classification"] == "PASS"
    assert summary["deterministic_pass_count"] == summary["deterministic_case_count"]
    assert summary["config_invalid_case_count"] >= 170
    assert summary["randomized_pass_count"] == summary["randomized_run_count"] == 1
    assert summary["safety"] == {
        "physical_attempt_count": 0,
        "ffmpeg_signal_count": 0,
        "pod_mutation_count": 0,
        "deployment_mutation_count": 0,
        "host_restart_count": 0,
        "production_database_used": False,
        "production_network_used": False,
    }
    cases = json.loads((output / "deterministic_cases.json").read_text(encoding="utf-8"))
    assert {item["scenario_id"] for item in cases} >= {"DB-20", "DB-24", "DB-25", "DB-28", "DB-31", "DB-32", "CFG-01"}
    by_id = {item["scenario_id"]: item for item in cases}
    assert by_id["DB-31"]["evidence"]["reason_class"] == "SQLITE_FULL"
    assert by_id["DB-31"]["evidence"]["rows_after_failure"] == 0
    assert by_id["DB-32"]["evidence"]["worker_count"] == by_id["DB-32"]["evidence"]["ready_count"] == 8
    assert by_id["DB-32"]["evidence"]["command_count"] == 0
