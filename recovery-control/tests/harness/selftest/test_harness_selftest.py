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


def test_negative_control_does_not_receipt_unexecuted_extra_fault(tmp_path: Path) -> None:
    from cra_harness.runner.executor import ScenarioExecutor

    scenario = ScenarioRegistry.load(ROOT / "harness/scenarios/protocol_v1.json").by_id("NC-01")
    scenario = replace(
        scenario,
        faults=scenario.faults + ({"fault_id": "unused-write", "kind": "SQLITE_WRITE_FAILURE"},),
    )
    execution = ScenarioExecutor(ROOT, tmp_path, {"run_id": "unused-negative-fault"}).execute("unused-negative-fault", scenario)
    assert execution.injector_errors == ("FAULT_NOT_COMPLETED:unused-write",)
    applications = [item.data for item in execution.evidence.named("fault_application")]
    assert all(item["fault_id"] != "unused-write" for item in applications)


def test_fault_lifecycle_requires_matching_hook_receipt() -> None:
    from cra_harness.injectors.faults import FaultKind

    controller = FaultController(({"fault_id": "expected", "kind": "SQLITE_WRITE_FAILURE"},))
    controller.arm_all()
    controller.receipt(FaultKind.DROP_HEARTBEAT, "wrong.operation")
    controller.complete_all()
    assert controller.untriggered == ("expected",)
    assert [item.lifecycle.value for item in controller.history] == ["requested", "armed"]
    with pytest.raises(ValueError, match="id/kind mismatch"):
        controller.receipt(FaultKind.SQLITE_WRITE_FAILURE, "DellStore.write.injected_sqlite_ioerr", fault_id="wrong")
    controller.receipt(FaultKind.SQLITE_WRITE_FAILURE, "DellStore.write.injected_sqlite_ioerr")
    controller.complete_all()
    assert controller.untriggered == ()
    assert [item.lifecycle.value for item in controller.history][-2:] == ["triggered", "completed"]


def test_noop_crash_hook_does_not_complete_fault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from cra_harness.controls.manifest import build_manifest
    from cra_harness.runner import executor

    monkeypatch.setattr(executor, "crash_at", lambda _point: lambda _actual: None)
    scenario = ScenarioRegistry.load(ROOT / "harness/scenarios/protocol_v1.json").by_id("D-04-central-commit-crash")
    manifest = build_manifest(ROOT, "noop-crash", 1, ["isolated no-op mutation control"])
    execution = executor.ScenarioExecutor(ROOT, tmp_path, manifest).execute("noop-crash", scenario)
    assert execution.injector_errors == ("FAULT_NOT_COMPLETED:fault-1",)
    assert [event.data["lifecycle"] for event in execution.evidence.named("fault_events")] == ["requested", "armed"]


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
