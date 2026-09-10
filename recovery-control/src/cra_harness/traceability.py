"""Deterministic verification traceability and task views for the owner profile.

The catalog remains the source for faults, evidence expectations, and test node IDs.
This module only connects those existing facts to canonical invariants, code symbols,
oracles, environments, and source-bound execution artifacts.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from cra_harness.task_contract import CATALOG, SPEC, load_catalog, review_status, source_hashes

VERIFICATION_STATUSES = {
    "DEFINED",
    "MAPPED",
    "EXECUTABLE",
    "EXECUTED",
    "VERIFIED",
    "FAILED",
    "INCONCLUSIVE",
    "STALE",
    "NOT_VERIFIED",
    "OUT_OF_SCOPE",
}


@dataclass(frozen=True)
class TraceIssue:
    severity: str
    code: str
    subject: str
    detail: str


@dataclass(frozen=True)
class ExecutionEvidence:
    classification: str
    contract_classification: str
    source_hashes: Mapping[str, str]
    executed_nodeids: frozenset[str]
    probe_results: Mapping[str, bool]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json_file(path: Path) -> Any:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError(f"TRACEABILITY_FILE_INVALID:{path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _implementation_path(reference: str) -> Path:
    module, separator, _ = reference.partition("::")
    if not separator or not module or not reference.split("::", 1)[1]:
        raise ValueError(f"IMPLEMENTATION_REF_INVALID:{reference}")
    prefix = Path("src") if not module.startswith("tools.") else Path()
    return prefix / Path(*module.split(".")).with_suffix(".py")


def _symbol_exists(root: Path, reference: str) -> tuple[bool, str]:
    try:
        relative = _implementation_path(reference)
    except ValueError:
        return False, "invalid implementation reference"
    path = root / relative
    if path.is_symlink() or not path.is_file():
        return False, f"module file does not exist: {relative}"
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(relative))
    except (OSError, SyntaxError, UnicodeError) as error:
        return False, f"module cannot be parsed: {type(error).__name__}"
    parts = reference.split("::", 1)[1].split(".")
    body: list[ast.stmt] = tree.body
    for part in parts:
        match = next(
            (node for node in body if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == part),
            None,
        )
        if match is None:
            return False, f"symbol does not exist: {reference}"
        body = match.body if isinstance(match, ast.ClassDef) else []
    return True, str(relative)


def _canonical_invariants(root: Path) -> dict[str, dict[str, Any]]:
    lines = (root / SPEC).read_text(encoding="utf-8").splitlines()
    result: dict[str, dict[str, Any]] = {}
    for number, line in enumerate(lines, 1):
        if line.startswith("## I-OWNER-"):
            identifier = line.removeprefix("## ").strip()
            result[identifier] = {"id": identifier, "source": SPEC, "line": number}
    return result


def _collect_nodeids(root: Path, test_files: Iterable[str]) -> tuple[set[str], str | None]:
    env = dict(os.environ)
    env.update(
        PYTHONPATH=str(root / "src") + os.pathsep + str(root),
        PYTHONDONTWRITEBYTECODE="1",
        OPENBLAS_NUM_THREADS="1",
        OMP_NUM_THREADS="1",
    )
    command = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "--collect-only",
        "--tb=short",
        "-p",
        "no:cacheprovider",
        *test_files,
    ]
    try:
        run = subprocess.run(command, cwd=root, env=env, text=True, capture_output=True, timeout=120)
    except subprocess.TimeoutExpired:
        return set(), "pytest collection timed out"
    nodes = {line for line in run.stdout.splitlines() if line.startswith("tests/") and "::" in line}
    if run.returncode != 0 or not nodes:
        detail = (run.stdout + run.stderr)[-2000:].strip()
        return nodes, "pytest collection failed or was empty: " + detail
    return nodes, None


def _verify_artifact_hashes(directory: Path, manifest: Mapping[str, Any]) -> None:
    for relative in ("summary.json", "source_hashes_before.json", "candidate/nodeids.json", "candidate/result.xml"):
        expected = manifest.get(relative)
        path = directory / relative
        if not isinstance(expected, str) or expected != _sha256(path):
            raise ValueError(f"EXECUTION_ARTIFACT_HASH_MISMATCH:{relative}")


def _load_execution(directory: Path) -> ExecutionEvidence:
    directory = directory.resolve()
    artifacts = _json_file(directory / "artifact_hashes.json")
    if not isinstance(artifacts, dict):
        raise ValueError("EXECUTION_ARTIFACT_MANIFEST_INVALID")
    _verify_artifact_hashes(directory, artifacts)
    summary = _json_file(directory / "summary.json")
    hashes = _json_file(directory / "source_hashes_before.json")
    nodeids = _json_file(directory / "candidate/nodeids.json")
    if not isinstance(summary, dict) or summary.get("schema") != "cra.owner_observation_chaos_result.v1":
        raise ValueError("EXECUTION_SUMMARY_SCHEMA")
    if not isinstance(hashes, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in hashes.items()):
        raise ValueError("EXECUTION_SOURCE_HASHES_INVALID")
    if not isinstance(nodeids, list) or not nodeids or not all(isinstance(item, str) for item in nodeids):
        raise ValueError("EXECUTION_NODEIDS_INVALID")
    if len(nodeids) != len(set(nodeids)):
        raise ValueError("EXECUTION_NODEIDS_DUPLICATE")
    junit_nodes = {
        case.attrib["file"] + "::" + case.attrib["name"] for case in ET.parse(directory / "candidate/result.xml").getroot().iter("testcase")
    }
    if junit_nodes != set(nodeids):
        raise ValueError("EXECUTION_JUNIT_NODEIDS_MISMATCH")
    gate = summary.get("contract_gate")
    if not isinstance(gate, dict) or not isinstance(gate.get("probes"), list):
        raise ValueError("EXECUTION_CONTRACT_GATE_INVALID")
    probe_results: dict[str, bool] = {}
    for row in gate["probes"]:
        if not isinstance(row, dict) or not isinstance(row.get("nodeid"), str) or row["nodeid"] in probe_results:
            raise ValueError("EXECUTION_PROBE_RESULT_INVALID")
        probe_results[row["nodeid"]] = row.get("passed") is True
    return ExecutionEvidence(
        classification=str(summary.get("classification", "INCONCLUSIVE")),
        contract_classification=str(gate.get("classification", "INCONCLUSIVE")),
        source_hashes=hashes,
        executed_nodeids=frozenset(nodeids),
        probe_results=probe_results,
    )


def _environment_status(
    *,
    environment: Mapping[str, Any],
    probes: list[dict[str, Any]],
    execution: ExecutionEvidence | None,
    source_stale: bool,
) -> str:
    if not probes:
        return str(environment["unmapped_status"])
    if execution is None:
        return "EXECUTABLE"
    if source_stale:
        return "STALE"
    nodes = {str(probe["nodeid"]) for probe in probes}
    if not nodes.issubset(execution.executed_nodeids) or not nodes.issubset(execution.probe_results):
        return "INCONCLUSIVE"
    if any(execution.probe_results[node] is False for node in nodes):
        return "FAILED"
    if execution.classification == execution.contract_classification == "PASS":
        return "VERIFIED"
    return "INCONCLUSIVE"


def check_traceability(
    root: Path,
    *,
    result_directory: Path | None = None,
    collected_nodeids: set[str] | None = None,
) -> dict[str, Any]:
    """Validate the owner catalog and project it into verification cases."""

    root = root.resolve()
    issues: list[TraceIssue] = []
    try:
        catalog = load_catalog(root)
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as error:
        issue = TraceIssue("ERROR", "CATALOG_INVALID", CATALOG, f"{type(error).__name__}:{error}")
        return {"schema": "cra.traceability_report.v1", "status": "FAIL", "issues": [asdict(issue)], "cases": []}

    assurance = catalog["assurance"]
    trace = assurance["traceability"]
    canonical = _canonical_invariants(root)
    components = {item["id"]: item for item in trace["system_ir"]["components"]}
    collectors = {item["id"]: item for item in trace["collectors"]}
    oracles = {item["id"]: item for item in trace["oracles"]}
    environments = {item["id"]: item for item in trace["environments"]}

    implementation_paths: dict[str, str] = {}
    for kind, entries in (("COMPONENT", components.values()), ("COLLECTOR", collectors.values()), ("ORACLE", oracles.values())):
        for entry in entries:
            references = entry.get("symbols", []) if kind == "COMPONENT" else [entry.get("implementation")]
            for reference in references:
                if not isinstance(reference, str):
                    issues.append(TraceIssue("ERROR", f"{kind}_REFERENCE_INVALID", str(entry.get("id")), repr(reference)))
                    continue
                exists, detail = _symbol_exists(root, reference)
                if not exists:
                    issues.append(TraceIssue("ERROR", f"{kind}_SYMBOL_MISSING", str(entry.get("id")), detail))
                else:
                    implementation_paths[reference] = detail

    execution: ExecutionEvidence | None = None
    if result_directory is not None:
        try:
            execution = _load_execution(result_directory)
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, ET.ParseError) as error:
            issues.append(TraceIssue("ERROR", "EXECUTION_ARTIFACT_INVALID", str(result_directory), f"{type(error).__name__}:{error}"))
    current_hashes = source_hashes(root)
    execution_hashes = {} if execution is None else dict(execution.source_hashes)
    source_stale = execution is not None and any(
        execution_hashes.get(path) != current_digest for path, current_digest in current_hashes.items()
    )
    if source_stale:
        changed = sorted(path for path in current_hashes if execution_hashes.get(path) != current_hashes.get(path))
        issues.append(TraceIssue("STALE", "VERIFICATION_SOURCE_STALE", "owner-profile", ", ".join(changed)))

    review = review_status(root)
    if review["classification"] != "PASS":
        issues.append(TraceIssue("STALE", "CANONICAL_REVIEW_STALE", "owner-profile", ", ".join(review["changed"])))

    if collected_nodeids is None:
        if execution is not None:
            collected_nodeids = set(execution.executed_nodeids)
        else:
            collected_nodeids, collection_error = _collect_nodeids(root, catalog["test_files"])
            if collection_error:
                issues.append(TraceIssue("ERROR", "TEST_COLLECTION_FAILED", "owner-profile", collection_error))

    bound_test_files: set[str] = set()
    cases: list[dict[str, Any]] = []
    for binding in assurance["bindings"]:
        identifier = str(binding["invariant_id"])
        case_id = str(binding["verification_case_id"])
        component_id = str(binding["component_id"])
        oracle_id = str(binding["oracle_ref"])
        reference = canonical.get(identifier)
        if reference is None:
            issues.append(TraceIssue("ERROR", "CANONICAL_INVARIANT_MISSING", case_id, identifier))
        component = components.get(component_id)
        if component is None:
            issues.append(TraceIssue("ERROR", "COMPONENT_MISSING", case_id, component_id))
        oracle = oracles.get(oracle_id)
        if oracle is None:
            issues.append(TraceIssue("ERROR", "ORACLE_MISSING", case_id, oracle_id))
            collector = None
        else:
            collector = collectors.get(oracle["collector_ref"])
            if collector is None:
                issues.append(TraceIssue("ERROR", "COLLECTOR_MISSING", case_id, str(oracle["collector_ref"])))

        required_evidence = sorted({str(item) for probe in binding["probes"] for item in probe["required_evidence"]})
        if collector is not None:
            prefixes = tuple(str(item) for item in collector["evidence_prefixes"])
            for evidence in required_evidence:
                if not any(evidence.startswith(prefix) for prefix in prefixes):
                    issues.append(TraceIssue("ERROR", "EVIDENCE_COLLECTOR_UNMAPPED", case_id, evidence))

        for path in binding["sut"]:
            if not (root / path).is_file():
                issues.append(TraceIssue("ERROR", "SUT_PATH_MISSING", case_id, path))
            if component is not None:
                component_paths = {implementation_paths.get(ref) for ref in component["symbols"]}
                if path not in component_paths:
                    issues.append(TraceIssue("ERROR", "SUT_NOT_IN_COMPONENT", case_id, path))

        tests: list[str] = []
        by_environment: dict[str, list[dict[str, Any]]] = {name: [] for name in environments}
        for probe in binding["probes"]:
            nodeid = str(probe["nodeid"])
            tests.append(nodeid)
            bound_test_files.add(nodeid.split("::", 1)[0])
            if collected_nodeids is not None and nodeid not in collected_nodeids:
                issues.append(TraceIssue("ERROR", "TEST_NODE_MISSING", case_id, nodeid))
            environment_id = str(probe["environment"])
            if environment_id not in by_environment:
                issues.append(TraceIssue("ERROR", "ENVIRONMENT_UNKNOWN", case_id, environment_id))
            else:
                by_environment[environment_id].append(probe)

        coverage = {
            name: _environment_status(
                environment=environment,
                probes=by_environment[name],
                execution=execution,
                source_stale=source_stale,
            )
            for name, environment in environments.items()
        }
        for environment_id, environment_status in coverage.items():
            if environment_status in {"FAILED", "INCONCLUSIVE"}:
                issues.append(
                    TraceIssue(
                        "ERROR",
                        f"VERIFICATION_{environment_status}",
                        f"{case_id}:{environment_id}",
                        "source-bound execution did not verify every mapped probe",
                    )
                )
        cases.append(
            {
                "id": case_id,
                "invariant_id": identifier,
                "canonical_ref": reference,
                "component_id": component_id,
                "sut": list(binding["sut"]),
                "fault": {"mechanism": binding["mechanism"], "injection": binding["injection"]},
                "required_evidence": required_evidence,
                "oracle": {
                    "id": oracle_id,
                    "implementation": None if oracle is None else oracle["implementation"],
                },
                "tests": tests,
                "environment_coverage": coverage,
                "coverage_gaps": list(binding.get("coverage_gaps", [])),
            }
        )

    regression_paths = {str(item["path"]) for item in trace["regression_only_tests"]}
    selected_paths = set(catalog["test_files"])
    for path in sorted(selected_paths - bound_test_files - regression_paths):
        issues.append(TraceIssue("ERROR", "CATALOG_TEST_WITHOUT_INVARIANT", path, "not bound and not regression-only"))
    for path in sorted(regression_paths - selected_paths):
        issues.append(TraceIssue("ERROR", "REGRESSION_TEST_NOT_SELECTED", path, "not present in test_files"))
    for path in sorted(regression_paths & bound_test_files):
        issues.append(TraceIssue("ERROR", "REGRESSION_TEST_CONTRADICTS_BINDING", path, "also used as invariant evidence"))

    severities = {issue.severity for issue in issues}
    status = "FAIL" if "ERROR" in severities else ("STALE" if "STALE" in severities else "PASS")
    return {
        "schema": "cra.traceability_report.v1",
        "status": status,
        "catalog": CATALOG,
        "canonical_invariant_source": SPEC,
        "review": review,
        "execution_artifact": None if result_directory is None else str(result_directory),
        "issues": [asdict(issue) for issue in issues],
        "cases": cases,
        "system_ir": trace["system_ir"],
    }


def changed_files(root: Path, diff_ref: str | None = None) -> list[str]:
    """Return deterministic repository-relative changed paths without parsing a call graph."""

    commands = (
        (["git", "diff", "--name-only", diff_ref, "--"] if diff_ref else ["git", "diff", "--name-only", "--"]),
        ["git", "diff", "--cached", "--name-only", "--"],
        ["git", "ls-files", "--others", "--exclude-standard"],
    )
    paths: set[str] = set()
    for command in commands:
        output = subprocess.check_output(command, cwd=root, text=True)
        paths.update(line for line in output.splitlines() if line)
    return sorted(paths)


def build_task_view(
    root: Path,
    *,
    files: Iterable[str] = (),
    symbols: Iterable[str] = (),
    component: str | None = None,
    verification_case: str | None = None,
    diff_ref: str | None = None,
    result_directory: Path | None = None,
) -> dict[str, Any]:
    report = check_traceability(root, result_directory=result_directory)
    requested_files = sorted(set(files))
    requested_symbols = sorted(set(symbols))
    if not requested_files and not requested_symbols and component is None and verification_case is None:
        requested_files = changed_files(root, diff_ref)

    components = {item["id"]: item for item in report.get("system_ir", {}).get("components", [])}
    selected: list[dict[str, Any]] = []
    for case in report["cases"]:
        component_data = components.get(case["component_id"], {})
        component_symbols = set(component_data.get("symbols", []))
        case_files = set(case["sut"]) | {node.split("::", 1)[0] for node in case["tests"]}
        global_file = any(path in {CATALOG, SPEC} for path in requested_files)
        file_match = bool(set(requested_files) & case_files)
        symbol_match = bool(set(requested_symbols) & component_symbols)
        explicit_match = (component is not None and component == case["component_id"]) or (
            verification_case is not None and verification_case == case["id"]
        )
        if global_file or file_match or symbol_match or explicit_match:
            selected.append(case)

    selected_components = sorted({case["component_id"] for case in selected})
    affected_symbols = sorted(
        {
            symbol
            for name in selected_components
            for symbol in components[name]["symbols"]
            if not requested_files or _implementation_path(symbol).as_posix() in requested_files or component is not None
        }
    )
    if selected and not affected_symbols:
        affected_symbols = sorted({symbol for name in selected_components for symbol in components[name]["symbols"]})
    gaps = []
    for case in selected:
        declared = {gap["environment"]: gap for gap in case["coverage_gaps"]}
        for environment, status in case["environment_coverage"].items():
            if status in {"VERIFIED", "OUT_OF_SCOPE"}:
                continue
            metadata = declared.get(environment)
            if metadata is None:
                metadata = {
                    "reason": "EXECUTION_REQUIRED" if status == "EXECUTABLE" else status,
                    "missing_capability": "source-bound execution evidence",
                    "next_action": "run the mapped owner verification probes and evaluate the resulting artifact",
                }
            gaps.append(
                {
                    "case_id": case["id"],
                    "invariant_id": case["invariant_id"],
                    "environment": environment,
                    "status": status,
                    **{key: metadata[key] for key in ("reason", "missing_capability", "next_action")},
                }
            )
    return {
        "schema": "cra.task_view.v1",
        "status": report["status"] if selected else "NO_MATCH",
        "targets": {
            "files": requested_files,
            "symbols": requested_symbols,
            "component": component,
            "verification_case": verification_case,
        },
        "affected_components": selected_components,
        "affected_symbols": affected_symbols,
        "canonical_invariants": [{"id": case["invariant_id"], "ref": case["canonical_ref"]} for case in selected],
        "related_scenarios": [{"case_id": case["id"], **case["fault"]} for case in selected],
        "required_evidence": sorted({item for case in selected for item in case["required_evidence"]}),
        "oracles": sorted(
            {case["oracle"]["id"]: case["oracle"] for case in selected}.values(),
            key=lambda item: item["id"],
        ),
        "tests": sorted({node for case in selected for node in case["tests"]}),
        "environment_coverage": [
            {"case_id": case["id"], "environment": environment, "status": status}
            for case in selected
            for environment, status in case["environment_coverage"].items()
        ],
        "known_gaps": gaps,
        "canonical_refs": sorted(
            {f"{case['canonical_ref']['source']}:{case['canonical_ref']['line']}" for case in selected if case["canonical_ref"] is not None}
        ),
        "traceability_issues": report["issues"],
    }


def render_traceability(report: Mapping[str, Any]) -> str:
    lines = [f"TRACEABILITY_STATUS={report['status']}"]
    for issue in report.get("issues", []):
        lines.append(f"{issue['severity']} {issue['code']} {issue['subject']}: {issue['detail']}")
    for case in report.get("cases", []):
        coverage = ", ".join(f"{name}={status}" for name, status in case["environment_coverage"].items())
        lines.append(f"CASE {case['id']} invariant={case['invariant_id']} component={case['component_id']} {coverage}")
    return "\n".join(lines) + "\n"


def render_task_view(view: Mapping[str, Any]) -> str:
    lines = [
        f"TASK_VIEW_STATUS={view['status']}",
        "TASK TARGET",
        json.dumps(view["targets"], ensure_ascii=False, sort_keys=True),
        "AFFECTED COMPONENTS",
        *view["affected_components"],
        "AFFECTED SYMBOLS",
        *view["affected_symbols"],
        "CANONICAL INVARIANTS",
        *(f"{item['id']} {item['ref']['source']}:{item['ref']['line']}" for item in view["canonical_invariants"]),
        "RELATED SCENARIOS",
        *(f"{item['case_id']} {item['mechanism']} {item['injection']}" for item in view["related_scenarios"]),
        "REQUIRED EVIDENCE",
        *view["required_evidence"],
        "EXISTING ORACLES",
        *(f"{item['id']} {item['implementation']}" for item in view["oracles"]),
        "EXISTING TESTS",
        *view["tests"],
        "ENVIRONMENT COVERAGE",
        *(f"{item['case_id']} {item['environment']}={item['status']}" for item in view["environment_coverage"]),
        "KNOWN GAPS",
        *(
            f"{item['case_id']} {item['environment']}={item['status']} reason={item['reason']} "
            f"missing={item['missing_capability']} next={item['next_action']}"
            for item in view["known_gaps"]
        ),
        "CANONICAL REFERENCES",
        *view["canonical_refs"],
    ]
    return "\n".join(lines) + "\n"
