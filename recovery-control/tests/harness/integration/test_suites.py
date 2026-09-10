from __future__ import annotations

from pathlib import Path

from cra_harness.classifiers.result import ResultClassifier
from cra_harness.classifiers.taxonomy import Classification
from cra_harness.controls.manifest import build_manifest
from cra_harness.observers.evidence import EvidenceCompletenessChecker
from cra_harness.oracles.contract import IndependentOracle
from cra_harness.runner.executor import ScenarioExecutor
from cra_harness.scenarios.registry import ScenarioRegistry

ROOT = Path(__file__).resolve().parents[3]


def _run_profile(tmp_path: Path, profile: str) -> list[Classification]:
    registry = ScenarioRegistry.load(ROOT / "harness/scenarios/protocol_v1.json")
    manifest = build_manifest(ROOT, "integration", 20260823, ["pytest integration"])
    executor = ScenarioExecutor(ROOT, tmp_path, manifest)
    classifications: list[Classification] = []
    for scenario in registry.profile(profile):
        execution = executor.execute("integration", scenario)
        completeness = EvidenceCompletenessChecker().check(execution.evidence, scenario.required_evidence)
        oracle = IndependentOracle().evaluate(scenario, execution.evidence)
        result = ResultClassifier().classify(
            scenario,
            completeness,
            oracle,
            harness_errors=execution.harness_errors,
            environment_errors=execution.environment_errors,
            safety_errors=execution.safety_errors,
            injector_errors=execution.injector_errors,
        )
        classifications.append(result.classification)
    return classifications


def test_deterministic_suite_has_17_passes(tmp_path: Path) -> None:
    assert _run_profile(tmp_path, "deterministic") == [Classification.PASS] * 17


def test_randomized_exploration_has_32_passes(tmp_path: Path) -> None:
    assert _run_profile(tmp_path, "randomized") == [Classification.PASS] * 32


def test_environment_manifest_is_attached_to_every_event_bundle(tmp_path: Path) -> None:
    registry = ScenarioRegistry.load(ROOT / "harness/scenarios/protocol_v1.json")
    manifest = build_manifest(ROOT, "manifest-test", 20260823, ["pytest integration"])
    execution = ScenarioExecutor(ROOT, tmp_path, manifest).execute("manifest-test", registry.by_id("D-01-normal"))
    assert execution.evidence.latest_data("environment_manifest") == manifest
    assert execution.evidence.latest_data("production_isolation")["safe"] is True
