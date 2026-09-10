from __future__ import annotations

from pathlib import Path

from cra_harness.runner.compound import CompoundRegistry, CompoundRunner

ROOT = Path(__file__).resolve().parents[3]
SEEDS = (20260823, 20260824, 20260825, 20260826, 20260827)


def _runner(tmp_path: Path) -> CompoundRunner:
    registry = CompoundRegistry(ROOT / "harness/scenarios/compound_v2.json")
    return CompoundRunner(ROOT, tmp_path, registry)


def test_explicit_compound_faults_pass_all_eight_oracles(tmp_path: Path) -> None:
    outcomes = [_runner(tmp_path / scenario_id).run(scenario_id) for scenario_id in (f"RF-{index:02d}" for index in range(1, 9))]
    assert [item.classification for item in outcomes] == ["PASS"] * 8


def test_randomized_compound_exploration_runs_200_reproducible_cases(tmp_path: Path) -> None:
    outcomes = _runner(tmp_path).run_randomized(SEEDS)
    assert len(outcomes) == 200
    assert {item.seed for item in outcomes} == set(SEEDS)
    assert {axis for item in outcomes for axis in item.state_axes} >= {
        "authority",
        "central_db",
        "command",
        "dell_ledger",
        "heartbeat",
        "lease",
        "local_action",
        "reconciliation",
        "target_identity",
    }
    assert {item.classification for item in outcomes} == {"PASS"}


def test_compound_negative_controls_are_all_detected(tmp_path: Path) -> None:
    outcomes = _runner(tmp_path).run_negative_controls()
    assert len(outcomes) == 4
    assert all(item.classification == "EXPECTED_INJECTED_FAILURE" for item in outcomes)
    assert all(item.oracle_violations for item in outcomes)
