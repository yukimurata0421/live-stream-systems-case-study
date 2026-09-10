from __future__ import annotations

from pathlib import Path

import pytest

from cra_harness.classifiers.result import ResultClassifier
from cra_harness.classifiers.taxonomy import Classification
from cra_harness.controls.manifest import build_manifest
from cra_harness.observers.evidence import EvidenceCompletenessChecker
from cra_harness.oracles.contract import IndependentOracle
from cra_harness.runner.executor import ScenarioExecutor
from cra_harness.scenarios.registry import ScenarioRegistry

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize("scenario_id", ["NC-01", "NC-02", "NC-03", "NC-04", "NC-05", "NC-06"])
def test_negative_control_is_detected(tmp_path: Path, scenario_id: str) -> None:
    scenario = ScenarioRegistry.load(ROOT / "harness/scenarios/protocol_v1.json").by_id(scenario_id)
    manifest = build_manifest(ROOT, "negative", 20260823, ["pytest negative control"])
    execution = ScenarioExecutor(ROOT, tmp_path, manifest).execute("negative", scenario)
    completeness = EvidenceCompletenessChecker().check(execution.evidence, scenario.required_evidence)
    oracle = IndependentOracle().evaluate(scenario, execution.evidence)
    result = ResultClassifier().classify(scenario, completeness, oracle, injector_errors=execution.injector_errors)
    assert result.classification == Classification.EXPECTED_INJECTED_FAILURE
    assert result.oracle_violations
    lifecycle = [event.data["lifecycle"] for event in execution.evidence.named("fault_events")]
    assert lifecycle == ["requested", "armed", "triggered", "completed"]
