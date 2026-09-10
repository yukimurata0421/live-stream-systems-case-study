from __future__ import annotations

import ast
from pathlib import Path

import pytest

from cra_harness.classifiers.taxonomy import Classification
from cra_harness.controls.manifest import manifest_complete
from cra_harness.observers.evidence import EvidenceCollector, EvidenceCompletenessChecker
from cra_harness.oracles.contract import IndependentOracle
from cra_harness.reporting.artifacts import ArtifactWriter, verify_artifact_consistency
from cra_harness.runner.suite_v2 import _runtime_mode
from cra_harness.scenarios.registry import ScenarioRegistry

ROOT = Path(__file__).resolve().parents[3]


def registry() -> ScenarioRegistry:
    return ScenarioRegistry.load(ROOT / "harness/scenarios/protocol_v1.json")


def test_registry_has_required_profiles_and_stable_schema() -> None:
    value = registry()
    assert len(value.profile("deterministic")) == 17
    assert len(value.profile("negative_control")) == 6
    assert len(value.profile("randomized")) == 32
    assert all(len(item.required_evidence) == 17 and item.expected_invariants for item in value.scenarios)
    assert {"central_state", "dell_state", "heartbeat_events", "lease_events", "target_before", "target_after"} <= set(
        value.scenarios[0].required_evidence
    )


def test_taxonomy_is_complete() -> None:
    assert {item.value for item in Classification} == {
        "PASS",
        "SUT_FAILURE",
        "HARNESS_FAILURE",
        "MISSING_EVIDENCE",
        "UNKNOWN_AMBIGUOUS",
        "EXPECTED_INJECTED_FAILURE",
        "UNSUPPORTED_CONDITION",
        "ENVIRONMENT_FAILURE",
        "SAFETY_GATE_FAILURE",
    }


def test_oracle_source_has_no_sut_decision_imports() -> None:
    source = (ROOT / "src/cra_harness/oracles/contract.py").read_text(encoding="utf-8")
    imported = {node.module for node in ast.walk(ast.parse(source)) if isinstance(node, ast.ImportFrom) and node.module is not None}
    assert not any(name.startswith(("cra_authority", "dell_recovery_agent", "cra_dell_recovery.states")) for name in imported)


def test_oracle_evaluates_fixture_expectations_only() -> None:
    scenario = registry().by_id("D-01-normal")
    collector = EvidenceCollector("run", scenario.scenario_id)
    collector.append("physical_attempts", "fixture", {"count": 1})
    collector.append(
        "scenario_result",
        "fixture",
        {
            "terminal_state": "EFFECT_OBSERVED",
            "central_active_local_attempt_count": 0,
            "central_active_local_result": "LOCAL_AUTHORITY_NOT_ACTIVE",
        },
    )
    collector.append("production_isolation", "fixture", {"safe": True})
    result = IndependentOracle().evaluate(scenario, collector.freeze())
    assert result.passed
    assert result.evaluated == 5


def test_evidence_is_append_only_after_freeze() -> None:
    collector = EvidenceCollector("run", "scenario")
    collector.append("event", "test", {"value": 1})
    collector.freeze()
    with pytest.raises(RuntimeError, match="frozen"):
        collector.append("event", "test", {"value": 2})


def test_duplicate_evidence_event_id_is_rejected() -> None:
    collector = EvidenceCollector("run", "scenario")
    collector.append("event", "test", {}, event_id="same")
    with pytest.raises(ValueError, match="duplicate"):
        collector.append("event", "test", {}, event_id="same")


def test_completeness_accepts_complete_associated_bundle() -> None:
    collector = EvidenceCollector("run", "scenario")
    collector.append("required", "test", {})
    result = EvidenceCompletenessChecker().check(collector.freeze(), ("required",))
    assert result.complete


def test_manifest_rejects_inconsistent_unborn_git_identity() -> None:
    manifest = {
        "run_id": "run",
        "started_at": "2026-08-23T00:00:00.000Z",
        "finished_at": None,
        "git": {"commit": "HEAD", "unborn": False},
        "runtime": {},
        "protocol_revision": "v1",
        "policy_revision": "v1",
        "ddl_sha256": {},
        "source_sha256": {},
        "fixture_sha256": {},
        "random_seed": 1,
        "test_commands": [],
        "fake_adapter_confirmation": True,
        "production_credentials_absent_confirmation": True,
        "secret_values_recorded": False,
    }
    assert manifest_complete(manifest) is False


def test_runtime_mode_requires_mapped_project_local_fixed_library(tmp_path: Path) -> None:
    fixed_library = tmp_path / ".runtime/sqlite-3.51.3/install/lib/libsqlite3.so.3.51.3"
    fixed_manifest = {"runtime": {"sqlite": "3.51.3", "sqlite_library": str(fixed_library)}}
    wrong_library = {"runtime": {"sqlite": "3.51.3", "sqlite_library": "/usr/lib/libsqlite3.so.0"}}
    wrong_version = {"runtime": {"sqlite": "3.46.1", "sqlite_library": str(fixed_library)}}

    assert _runtime_mode(tmp_path, fixed_manifest) == "PROJECT_LOCAL_SQLITE_FIXED_FAKE_NO_ACTION"
    assert _runtime_mode(tmp_path, wrong_library) == "UNFIXED_SQLITE_DIAGNOSTIC_FAKE_NO_ACTION"
    assert _runtime_mode(tmp_path, wrong_version) == "UNFIXED_SQLITE_DIAGNOSTIC_FAKE_NO_ACTION"


def test_summary_matrix_classification_and_profile_counts_are_consistent(tmp_path: Path) -> None:
    counts = {item.value: 0 for item in Classification}
    counts[Classification.PASS.value] = 1
    outcome = {
        "run_id": "summary-run",
        "scenario_id": "scenario-1",
        "profile": "deterministic",
        "deterministic": True,
        "source": "REGRESSION",
        "expected": "PASS",
        "classification": "PASS",
        "reason_codes": ["ALL_INVARIANTS_HOLD"],
        "matches_expectation": True,
        "evidence_started_at": "2026-08-23T00:00:00.000Z",
        "evidence_finished_at": "2026-08-23T00:00:01.000Z",
        "events": [],
    }
    writer = ArtifactWriter(tmp_path, "summary-run")
    writer.write_all(
        manifest={
            "run_id": "summary-run",
            "started_at": "2026-08-23T00:00:00.000Z",
            "finished_at": "2026-08-23T00:00:03.000Z",
        },
        outcomes=[outcome],
        verification={
            "started_at": "2026-08-23T00:00:01.000Z",
            "finished_at": "2026-08-23T00:00:02.000Z",
        },
        summary={
            "run_id": "summary-run",
            "total": 1,
            "scenario_count": 1,
            "profile_counts": {"deterministic": 1},
            "classification_counts": counts,
        },
        report="# test\n",
        registry_hash="fixture-hash",
    )
    assert verify_artifact_consistency(writer.run_dir) == (True, ())
    (writer.run_dir / "verification.json").write_text(
        '{"started_at":"2026-08-22T23:59:59.000Z","finished_at":"2026-08-23T00:00:02.000Z"}\n',
        encoding="utf-8",
    )
    consistent, errors = verify_artifact_consistency(writer.run_dir)
    assert consistent is False
    assert "RUN_TIMELINE_MISMATCH" in errors
