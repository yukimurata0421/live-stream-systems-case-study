from __future__ import annotations

import hashlib
import json
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest

from cra_harness import task_contract as contract
from cra_harness.verification_gate import (
    EXIT_HARNESS_ERROR,
    EXIT_PASS,
    EXIT_VERIFICATION_FAILURE,
    evaluate_owner_gate,
)

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def project(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    catalog = contract.load_catalog(ROOT)
    paths = set(contract.DOCS) | set(contract.SEMANTIC_SOURCES) | set(contract.CI_ADAPTERS) | set(catalog["test_files"])
    paths.update(path for binding in catalog["assurance"]["bindings"] for path in binding["sut"])
    for relative in paths:
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, destination)
    review(root)
    return root


def review(root: Path) -> None:
    (root / contract.REVIEW).parent.mkdir(parents=True, exist_ok=True)
    contract.dump(
        root / contract.REVIEW,
        {
            "schema": "cra.owner_contract_review.v1",
            "reason": "test-owned provider-independent verification review",
            "source_hashes": contract.source_hashes(root),
        },
    )


def mapped_nodes(root: Path) -> list[str]:
    catalog = contract.load_catalog(root)
    return sorted(probe["nodeid"] for binding in catalog["assurance"]["bindings"] for probe in binding["probes"])


def artifact(
    root: Path,
    directory: Path,
    *,
    classification: str = "PASS",
    contract_classification: str = "PASS",
    probes_passed: bool = True,
    nodes: list[str] | None = None,
) -> Path:
    selected = mapped_nodes(root) if nodes is None else nodes
    candidate = directory / "candidate"
    candidate.mkdir(parents=True)
    contract.dump(candidate / "nodeids.json", selected)
    suite = ET.Element("testsuite")
    for node in selected:
        file, name = node.split("::", 1)
        ET.SubElement(suite, "testcase", file=file, name=name)
    ET.ElementTree(suite).write(candidate / "result.xml")
    contract.dump(directory / "source_hashes_before.json", contract.source_hashes(root))
    contract.dump(
        directory / "summary.json",
        {
            "schema": "cra.owner_observation_chaos_result.v1",
            "classification": classification,
            "contract_gate": {
                "classification": contract_classification,
                "probes": [{"nodeid": node, "passed": probes_passed} for node in selected],
            },
        },
    )
    hashes = {
        str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest() for path in directory.rglob("*") if path.is_file()
    }
    contract.dump(directory / "artifact_hashes.json", hashes)
    return directory


def issue_codes(result: dict[str, Any]) -> set[str]:
    return {issue["code"] for issue in result["issues"]}


def test_pass_artifact_has_stable_success_and_preserves_environment_semantics(project: Path, tmp_path: Path) -> None:
    result = evaluate_owner_gate(project, result_directory=artifact(project, tmp_path / "result"))
    assert result["exit_code"] == EXIT_PASS
    assert (result["execution_status"], result["verification_status"], result["failure_owner"]) == ("EXECUTED", "PASS", "NONE")
    assert result["environment_status"] == {
        "fake": {"VERIFIED": 8},
        "process": {"NOT_VERIFIED": 4, "VERIFIED": 4},
        "production": {"OUT_OF_SCOPE": 8},
    }


@pytest.mark.parametrize(
    ("classification", "probes_passed", "expected_status", "expected_owner", "expected_code"),
    [
        ("PASS", False, "FAIL", "SUT", "VERIFICATION_FAILED"),
        ("FAIL", True, "INCONCLUSIVE", "HARNESS", "VERIFICATION_INCONCLUSIVE"),
        ("HOLD", True, "INCONCLUSIVE", "HARNESS", "VERIFICATION_INCONCLUSIVE"),
    ],
)
def test_failed_and_inconclusive_artifacts_fail_without_owner_confusion(
    project: Path,
    tmp_path: Path,
    classification: str,
    probes_passed: bool,
    expected_status: str,
    expected_owner: str,
    expected_code: str,
) -> None:
    result_path = artifact(project, tmp_path / "result", classification=classification, probes_passed=probes_passed)
    result = evaluate_owner_gate(project, result_directory=result_path)
    assert result["exit_code"] == EXIT_VERIFICATION_FAILURE
    assert result["verification_status"] == expected_status
    assert result["failure_owner"] == expected_owner
    assert expected_code in issue_codes(result)


def test_missing_artifact_is_a_failed_not_executed_gate(project: Path, tmp_path: Path) -> None:
    result = evaluate_owner_gate(project, result_directory=tmp_path / "missing")
    assert result["exit_code"] == EXIT_VERIFICATION_FAILURE
    assert (result["execution_status"], result["verification_status"], result["failure_owner"]) == (
        "NOT_EXECUTED",
        "FAIL",
        "HARNESS",
    )
    assert issue_codes(result) == {"EXECUTION_ARTIFACT_REQUIRED"}


def test_malformed_artifact_is_a_harness_error(project: Path, tmp_path: Path) -> None:
    malformed = tmp_path / "malformed"
    malformed.mkdir()
    result = evaluate_owner_gate(project, result_directory=malformed)
    assert result["exit_code"] == EXIT_HARNESS_ERROR
    assert (result["execution_status"], result["verification_status"], result["failure_owner"]) == (
        "ERROR",
        "INCONCLUSIVE",
        "HARNESS",
    )
    assert "EXECUTION_ARTIFACT_INVALID" in issue_codes(result)


@pytest.mark.parametrize(
    ("fault", "expected_code"),
    [
        ("missing-evidence", "EVIDENCE_COLLECTOR_UNMAPPED"),
        ("missing-oracle", "ORACLE_SYMBOL_MISSING"),
        ("missing-test", "TEST_NODE_MISSING"),
    ],
)
def test_broken_canonical_mapping_fails_the_gate(project: Path, tmp_path: Path, fault: str, expected_code: str) -> None:
    path = project / contract.CATALOG
    catalog = json.loads(path.read_text(encoding="utf-8"))
    old_nodes = mapped_nodes(project)
    if fault == "missing-evidence":
        catalog["assurance"]["traceability"]["collectors"][0]["evidence_prefixes"] = ["unrelated."]
    elif fault == "missing-oracle":
        catalog["assurance"]["traceability"]["oracles"][0]["implementation"] += ".missing"
    else:
        catalog["assurance"]["bindings"][0]["probes"][0]["nodeid"] += "-missing"
    contract.dump(path, catalog)
    review(project)
    result_path = artifact(project, tmp_path / "result", nodes=old_nodes if fault == "missing-test" else None)
    result = evaluate_owner_gate(project, result_directory=result_path)
    assert result["exit_code"] == EXIT_VERIFICATION_FAILURE
    assert result["failure_owner"] == "HARNESS"
    assert expected_code in issue_codes(result)


@pytest.mark.parametrize(
    "relative",
    [
        "src/cra_harness/verification_gate.py",
        "src/cra_harness/oracles/contract.py",
        contract.CATALOG,
    ],
)
def test_semantic_source_change_invalidates_existing_artifact(project: Path, tmp_path: Path, relative: str) -> None:
    result_path = artifact(project, tmp_path / "result")
    path = project / relative
    if relative == contract.CATALOG:
        value = json.loads(path.read_text(encoding="utf-8"))
        value["assurance"]["bindings"][0]["injection"] += " after semantic change"
        contract.dump(path, value)
    else:
        path.write_text(path.read_text(encoding="utf-8") + "\n# semantic change\n", encoding="utf-8")
    result = evaluate_owner_gate(project, result_directory=result_path)
    assert result["exit_code"] == EXIT_VERIFICATION_FAILURE
    assert result["verification_status"] == "STALE"
    assert "VERIFICATION_SOURCE_STALE" in issue_codes(result)


@pytest.mark.parametrize("relative", contract.CI_ADAPTERS[:2])
def test_provider_adapter_change_is_integrity_drift_not_semantic_staleness(project: Path, tmp_path: Path, relative: str) -> None:
    result_path = artifact(project, tmp_path / "result")
    semantic_before = contract.source_hashes(project)
    adapter_before = contract.adapter_hashes(project)
    workflow = project / relative
    workflow.write_text(workflow.read_text(encoding="utf-8") + "\n# adapter-only change\n", encoding="utf-8")
    assert contract.source_hashes(project) == semantic_before
    assert contract.adapter_hashes(project) != adapter_before
    result = evaluate_owner_gate(project, result_directory=result_path)
    assert result["exit_code"] == EXIT_PASS
    assert result["verification_status"] == "PASS"
