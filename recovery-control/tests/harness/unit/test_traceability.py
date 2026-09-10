from __future__ import annotations

import hashlib
import json
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest

from cra_harness import task_contract as gate
from cra_harness.traceability import build_task_view, check_traceability

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def project(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    catalog = gate.load_catalog(ROOT)
    paths = set(gate.DOCS) | set(gate.SEMANTIC_SOURCES) | set(gate.CI_ADAPTERS) | set(catalog["test_files"])
    paths.update(path for binding in catalog["assurance"]["bindings"] for path in binding["sut"])
    for relative in paths:
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, destination)
    (root / gate.REVIEW).parent.mkdir(parents=True, exist_ok=True)
    gate.dump(
        root / gate.REVIEW,
        {
            "schema": "cra.owner_contract_review.v1",
            "reason": "test-owned traceability review fixture",
            "source_hashes": gate.source_hashes(root),
        },
    )
    return root


def catalog(root: Path) -> dict[str, Any]:
    return json.loads((root / gate.CATALOG).read_text(encoding="utf-8"))


def save_catalog(root: Path, value: dict[str, Any]) -> None:
    gate.dump(root / gate.CATALOG, value)


def mapped_nodes(root: Path) -> set[str]:
    value = catalog(root)
    return {probe["nodeid"] for binding in value["assurance"]["bindings"] for probe in binding["probes"]}


def execution_artifact(
    root: Path,
    directory: Path,
    *,
    classification: str = "PASS",
    contract_classification: str = "PASS",
    probes_passed: bool = True,
) -> Path:
    nodes = sorted(mapped_nodes(root))
    candidate = directory / "candidate"
    candidate.mkdir(parents=True)
    gate.dump(candidate / "nodeids.json", nodes)
    suite = ET.Element("testsuite")
    for node in nodes:
        file, name = node.split("::", 1)
        ET.SubElement(suite, "testcase", file=file, name=name)
    ET.ElementTree(suite).write(candidate / "result.xml")
    source_hashes = gate.source_hashes(root)
    source_hashes["src/unrelated_profile_module.py"] = "a" * 64
    gate.dump(directory / "source_hashes_before.json", source_hashes)
    gate.dump(
        directory / "summary.json",
        {
            "schema": "cra.owner_observation_chaos_result.v1",
            "classification": classification,
            "contract_gate": {
                "classification": contract_classification,
                "probes": [{"nodeid": node, "passed": probes_passed} for node in nodes],
            },
        },
    )
    hashes = {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest() for path in directory.rglob("*") if path.is_file()
    }
    gate.dump(directory / "artifact_hashes.json", hashes)
    return directory


def codes(report: dict[str, Any]) -> set[str]:
    return {issue["code"] for issue in report["issues"]}


def test_valid_mapping_passes_and_environment_is_first_class(project: Path) -> None:
    report = check_traceability(project, collected_nodeids=mapped_nodes(project))
    assert report["status"] == "PASS", report
    diagnostics = next(item for item in report["cases"] if item["id"] == "VC-OWNER-DIAGNOSTICS-001")
    assert diagnostics["environment_coverage"] == {
        "fake": "EXECUTABLE",
        "process": "NOT_VERIFIED",
        "production": "OUT_OF_SCOPE",
    }
    assert diagnostics["canonical_ref"]["source"] == gate.SPEC
    assert diagnostics["oracle"]["implementation"].endswith("IndependentOracle.evaluate_expectations")


@pytest.mark.parametrize(
    ("fault", "expected_code"),
    [
        ("missing-invariant", "CATALOG_INVALID"),
        ("missing-test", "TEST_NODE_MISSING"),
        ("missing-oracle", "ORACLE_SYMBOL_MISSING"),
        ("missing-evidence-collector", "EVIDENCE_COLLECTOR_UNMAPPED"),
        ("unknown-environment", "CATALOG_INVALID"),
        ("unmapped-catalog-test", "CATALOG_TEST_WITHOUT_INVARIANT"),
    ],
)
def test_missing_or_broken_mapping_fails(project: Path, fault: str, expected_code: str) -> None:
    collected = mapped_nodes(project)
    value = catalog(project)
    binding = value["assurance"]["bindings"][0]
    if fault == "missing-invariant":
        binding["invariant_id"] = "I-OWNER-NOT-CANONICAL"
    elif fault == "missing-test":
        binding["probes"][0]["nodeid"] += "-does-not-exist"
    elif fault == "missing-oracle":
        value["assurance"]["traceability"]["oracles"][0]["implementation"] += ".missing"
    elif fault == "missing-evidence-collector":
        value["assurance"]["traceability"]["collectors"][0]["evidence_prefixes"] = ["unrelated."]
    elif fault == "unknown-environment":
        binding["probes"][0]["environment"] = "unknown"
    else:
        value["assurance"]["traceability"]["regression_only_tests"] = []
    save_catalog(project, value)
    report = check_traceability(project, collected_nodeids=collected)
    assert report["status"] != "PASS"
    assert expected_code in codes(report), report
    if fault == "unknown-environment":
        assert not any(status == "VERIFIED" for case in report["cases"] for status in case["environment_coverage"].values())


def test_execution_is_verified_then_stale_after_referenced_code_change(project: Path, tmp_path: Path) -> None:
    result = execution_artifact(project, tmp_path / "result")
    fresh = check_traceability(project, result_directory=result)
    assert fresh["status"] == "PASS", fresh
    diagnostics = next(item for item in fresh["cases"] if item["id"] == "VC-OWNER-DIAGNOSTICS-001")
    assert diagnostics["environment_coverage"]["fake"] == "VERIFIED"
    assert diagnostics["environment_coverage"]["process"] == "NOT_VERIFIED"

    source = project / "src/cra_no_action_soak/recovery_facts.py"
    source.write_text(source.read_text(encoding="utf-8") + "\n# changed after verification\n", encoding="utf-8")
    stale = check_traceability(project, result_directory=result)
    assert stale["status"] == "STALE"
    assert "VERIFICATION_SOURCE_STALE" in codes(stale)
    assert all(status != "VERIFIED" for case in stale["cases"] for status in case["environment_coverage"].values())


@pytest.mark.parametrize(
    ("artifact", "expected_code"),
    [
        ({"probes_passed": False}, "VERIFICATION_FAILED"),
        ({"classification": "HOLD"}, "VERIFICATION_INCONCLUSIVE"),
    ],
)
def test_source_bound_failed_or_inconclusive_case_fails_gate(
    project: Path,
    tmp_path: Path,
    artifact: dict[str, Any],
    expected_code: str,
) -> None:
    result = execution_artifact(project, tmp_path / "result", **artifact)
    report = check_traceability(project, result_directory=result)
    assert report["status"] == "FAIL", report
    assert expected_code in codes(report)


def test_task_view_projects_existing_semantics_for_changed_file(project: Path, tmp_path: Path) -> None:
    result = execution_artifact(project, tmp_path / "result")
    view = build_task_view(
        project,
        files=["src/cra_no_action_soak/recovery_facts.py"],
        result_directory=result,
    )
    assert view["status"] == "PASS", view
    assert {item["id"] for item in view["canonical_invariants"]} == {
        "I-OWNER-DIAGNOSTICS",
        "I-OWNER-RETENTION",
    }
    assert "observed.exception_type" in view["required_evidence"]
    assert any("IndependentOracle.evaluate_expectations" in item["implementation"] for item in view["oracles"])
    process_gaps = [item for item in view["known_gaps"] if item["environment"] == "process" and item["status"] == "NOT_VERIFIED"]
    assert {item["invariant_id"] for item in process_gaps} == {"I-OWNER-DIAGNOSTICS", "I-OWNER-RETENTION"}
    assert {item["reason"] for item in process_gaps} == {"PROCESS_NOT_APPLICABLE"}
    assert all(item["missing_capability"] and item["next_action"] for item in process_gaps)
    assert all(reference.startswith(gate.SPEC + ":") for reference in view["canonical_refs"])


def test_component_task_view_classifies_every_process_gap(project: Path, tmp_path: Path) -> None:
    result = execution_artifact(project, tmp_path / "result")
    view = build_task_view(project, component="OwnerObservationPipeline", result_directory=result)
    process_gaps = [item for item in view["known_gaps"] if item["environment"] == "process"]
    assert {item["invariant_id"]: item["reason"] for item in process_gaps} == {
        "I-OWNER-FINAL": "INJECTOR_MISSING",
        "I-OWNER-DIAGNOSTICS": "PROCESS_NOT_APPLICABLE",
        "I-OWNER-ACTIVATION": "INJECTOR_MISSING",
        "I-OWNER-RETENTION": "PROCESS_NOT_APPLICABLE",
    }
    assert all(item["missing_capability"] and item["next_action"] for item in process_gaps)
