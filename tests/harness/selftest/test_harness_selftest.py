from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from cra_harness.classifiers.result import ResultClassifier
from cra_harness.classifiers.taxonomy import Classification
from cra_harness.injectors.faults import FaultController
from cra_harness.observers.evidence import EvidenceBundle, EvidenceCompletenessChecker, EvidenceEvent
from cra_harness.oracles.contract import OracleResult
from cra_harness.reporting.artifacts import ArtifactWriter
from cra_harness.scenarios.registry import ScenarioRegistry

ROOT = Path(__file__).resolve().parents[3]


def _scenario() -> object:
    return ScenarioRegistry.load(ROOT / "harness/scenarios/protocol_v1.json").by_id("D-01-normal")


def _empty_bundle() -> EvidenceBundle:
    return EvidenceBundle("run", "D-01-normal", "2026-08-23T00:00:00.000Z", "2026-08-23T00:00:01.000Z", ())


def test_missing_fixture_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="malformed or missing"):
        ScenarioRegistry.load(tmp_path / "missing.json")


def test_malformed_fixture_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "broken.json"
    path.write_text("{", encoding="utf-8")
    with pytest.raises(ValueError, match="malformed or missing"):
        ScenarioRegistry.load(path)


def test_observer_not_run_is_harness_failure() -> None:
    scenario = _scenario()
    completeness = EvidenceCompletenessChecker().check(_empty_bundle(), scenario.required_evidence)
    result = ResultClassifier().classify(
        scenario,
        completeness,
        None,
        harness_errors=("OBSERVER_NOT_RUN",),
    )
    assert result.classification == Classification.HARNESS_FAILURE


def test_oracle_exception_is_not_sut_failure() -> None:
    scenario = _scenario()
    complete = replace(EvidenceCompletenessChecker().check(_empty_bundle(), ()), missing=())
    result = ResultClassifier().classify(scenario, complete, OracleResult(0, (), ("ORACLE_EXCEPTION",)))
    assert result.classification == Classification.HARNESS_FAILURE


def test_untriggered_injector_is_harness_failure() -> None:
    scenario = _scenario()
    complete = EvidenceCompletenessChecker().check(_empty_bundle(), ())
    controller = FaultController(({"kind": "DROP_HEARTBEAT"},))
    errors = tuple(f"FAULT_NOT_COMPLETED:{item}" for item in controller.untriggered)
    result = ResultClassifier().classify(scenario, complete, OracleResult(1, (), ()), injector_errors=errors)
    assert result.classification == Classification.HARNESS_FAILURE


def test_duplicate_artifact_run_is_rejected(tmp_path: Path) -> None:
    ArtifactWriter(tmp_path, "same-run")
    with pytest.raises(FileExistsError):
        ArtifactWriter(tmp_path, "same-run")


def test_run_id_mismatch_is_harness_failure() -> None:
    event = EvidenceEvent("id", 1, "wrong", "D-01-normal", "required", "test", "2026-08-23T00:00:00.500Z", {})
    bundle = EvidenceBundle("run", "D-01-normal", "2026-08-23T00:00:00.000Z", "2026-08-23T00:00:01.000Z", (event,))
    completeness = EvidenceCompletenessChecker().check(bundle, ("required",))
    result = ResultClassifier().classify(_scenario(), completeness, OracleResult(1, (), ()))
    assert result.classification == Classification.HARNESS_FAILURE


def test_required_evidence_missing_is_missing_evidence() -> None:
    scenario = _scenario()
    completeness = EvidenceCompletenessChecker().check(_empty_bundle(), ("required",))
    result = ResultClassifier().classify(scenario, completeness, OracleResult(1, (), ()))
    assert result.classification == Classification.MISSING_EVIDENCE


def test_unsupported_condition_is_not_sut_failure() -> None:
    scenario = _scenario()
    completeness = EvidenceCompletenessChecker().check(_empty_bundle(), ())
    result = ResultClassifier().classify(scenario, completeness, None, unsupported=("UNSUPPORTED_KERNEL",))
    assert result.classification == Classification.UNSUPPORTED_CONDITION


def test_negative_control_requires_actual_oracle_violation() -> None:
    scenario = ScenarioRegistry.load(ROOT / "harness/scenarios/protocol_v1.json").by_id("NC-01")
    completeness = EvidenceCompletenessChecker().check(_empty_bundle(), ())
    result = ResultClassifier().classify(scenario, completeness, OracleResult(1, (), ()))
    assert result.classification == Classification.HARNESS_FAILURE
    assert result.reason_codes == ("NEGATIVE_CONTROL_ESCAPED",)
