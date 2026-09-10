"""Source-bound owner profile gates. No production access or SUT decision imports."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import xml.etree.ElementTree as ET
from dataclasses import asdict
from pathlib import Path
from typing import Any

from cra_harness.observers.evidence import EvidenceCollector
from cra_harness.oracles.contract import IndependentOracle
from cra_harness.scenarios.model import InvariantExpectation

CATALOG = "harness/scenarios/owner_observation_chaos_v1.json"
REVIEW = "harness/contracts/owner_observation_review.json"
SPEC = "docs/oracle/04_owner_observation_contract.md"
DOCS = (
    "AGENTS.md",
    ".agents/skills/cra-evidence-workflow/SKILL.md",
    "docs/oracle/README.md",
    "docs/oracle/00_restart_authority_overview.md",
    "docs/oracle/01_restart_authority_detail.md",
    "docs/oracle/02_oracle_specification.md",
    "docs/oracle/03_code_and_doc_drift_map.md",
    SPEC,
    "docs/oracle/05_evidence_lineage_and_restart_domains.md",
    "docs/engineering/harness_trust_model.md",
    "docs/runbooks/cra_evidence_workflow.md",
    "harness/contracts/evidence_lineage.v1.schema.json",
    "harness/scenarios/cra_evidence_lineage_v1.json",
    CATALOG,
)
SEMANTIC_SOURCES = (
    "src/cra_harness/evidence_lineage.py",
    "src/cra_harness/task_contract.py",
    "src/cra_harness/traceability.py",
    "src/cra_harness/verification_gate.py",
    "src/cra_harness/oracles/contract.py",
    "src/cra_harness/observers/evidence.py",
    "src/cra_harness/scenarios/model.py",
    "tools/cra_task.py",
    "tools/run_owner_observation_chaos.py",
    "tools/run_cra_workflow_eval.py",
    "tests/harness/unit/test_task_contract.py",
    "tests/harness/unit/test_traceability.py",
    "tests/harness/unit/test_verification_gate.py",
)
CI_ADAPTERS = (
    ".github/workflows/owner-verification-traceability.yml",
    ".forgejo/workflows/owner-verification-traceability.yml",
    "tests/harness/unit/test_traceability_ci.py",
)
COVERAGE_GAP_REASONS = {"INJECTOR_MISSING", "PROCESS_NOT_APPLICABLE", "EVIDENCE_INSUFFICIENT", "IMPLEMENTATION_INCOMPLETE"}
UNPROVEN = ("real_ffmpeg", "network_recovery", "cra_action", "viewer_recovery", "formal_soak")


def read(root: Path, relative: str) -> bytes:
    path = root / relative
    if Path(relative).is_absolute() or ".." in Path(relative).parts or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("CONTEXT_PATH_ESCAPE:" + relative)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise ValueError("CONTEXT_FILE_INVALID:" + relative)
    return path.read_bytes()


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def dump(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_catalog(root: Path) -> dict[str, Any]:
    value: dict[str, Any] = json.loads(read(root, CATALOG))
    if value.get("schema") != "cra.owner_observation_chaos_catalog.v1":
        raise ValueError("CATALOG_SCHEMA")
    boundary = value["execution_boundary"]
    if any(boundary[k] is not False for k in ("production_mutation", "production_credentials", "external_network")):
        raise ValueError("PROFILE_BOUNDARY")
    assurance = value["assurance"]
    if assurance.get("schema") != "cra.owner_contract_bindings.v3" or assurance.get("spec") != SPEC:
        raise ValueError("ASSURANCE_SCHEMA_OR_SPEC")
    traceability = assurance.get("traceability")
    if not isinstance(traceability, dict) or traceability.get("schema") != "cra.verification_traceability.v1":
        raise ValueError("TRACEABILITY_SCHEMA")
    system_ir = traceability.get("system_ir")
    if not isinstance(system_ir, dict) or system_ir.get("schema") != "cra.system_semantic_ir.v1":
        raise ValueError("SYSTEM_IR_SCHEMA")
    components = system_ir.get("components")
    collectors = traceability.get("collectors")
    oracles = traceability.get("oracles")
    environments = traceability.get("environments")
    regression_only = traceability.get("regression_only_tests")
    if not all(isinstance(items, list) and items for items in (components, collectors, oracles, environments)):
        raise ValueError("TRACEABILITY_REGISTRY_EMPTY")
    if not isinstance(regression_only, list):
        raise ValueError("REGRESSION_ONLY_TESTS_INVALID")
    assert isinstance(components, list)
    assert isinstance(collectors, list)
    assert isinstance(oracles, list)
    assert isinstance(environments, list)

    def keyed(items: list[Any], label: str) -> dict[str, dict[str, Any]]:
        if not all(isinstance(item, dict) and isinstance(item.get("id"), str) and item["id"] for item in items):
            raise ValueError(label + "_ID_INVALID")
        result = {item["id"]: item for item in items}
        if len(result) != len(items):
            raise ValueError(label + "_ID_DUPLICATE")
        return result

    component_by_id = keyed(components, "COMPONENT")
    collector_by_id = keyed(collectors, "COLLECTOR")
    oracle_by_id = keyed(oracles, "ORACLE")
    environment_by_id = keyed(environments, "ENVIRONMENT")
    for component in component_by_id.values():
        if not isinstance(component.get("authority"), str) or not component["authority"].strip() or not component.get("symbols"):
            raise ValueError("COMPONENT_SEMANTICS_INVALID")
    for collector in collector_by_id.values():
        if not collector.get("implementation") or not collector.get("evidence_prefixes"):
            raise ValueError("COLLECTOR_INCOMPLETE")
    for oracle in oracle_by_id.values():
        if not oracle.get("implementation") or oracle.get("collector_ref") not in collector_by_id:
            raise ValueError("ORACLE_INCOMPLETE")
    allowed_unmapped = {"NOT_VERIFIED", "OUT_OF_SCOPE"}
    for environment in environment_by_id.values():
        if environment.get("unmapped_status") not in allowed_unmapped or not environment.get("boundary"):
            raise ValueError("ENVIRONMENT_BOUNDARY_INVALID")
    regression_paths = [item.get("path") for item in regression_only if isinstance(item, dict)]
    if len(regression_paths) != len(regression_only) or len(set(regression_paths)) != len(regression_paths):
        raise ValueError("REGRESSION_ONLY_TESTS_INVALID")
    if any(path not in value["test_files"] for path in regression_paths) or any(
        not isinstance(item.get("reason"), str) or not item["reason"].strip() for item in regression_only
    ):
        raise ValueError("REGRESSION_ONLY_TESTS_INVALID")
    bindings = assurance["bindings"]
    required = re.findall(r"^## (I-OWNER-[A-Z-]+)$", read(root, SPEC).decode(), re.M)
    if not required or len(set(required)) != len(required):
        raise ValueError("SPEC_INVARIANTS_INVALID")
    ids = [b["invariant_id"] for b in bindings]
    if set(ids) != set(required) or len(ids) != len(set(ids)):
        raise ValueError("SPEC_BINDING_MISSING_OR_UNKNOWN")
    case_ids = [b.get("verification_case_id") for b in bindings]
    if not all(isinstance(item, str) and item.startswith("VC-") for item in case_ids) or len(set(case_ids)) != len(case_ids):
        raise ValueError("VERIFICATION_CASE_ID_INVALID")
    mechanisms = {m["id"] for m in value["mechanisms"]}
    for binding in bindings:
        if (
            binding["mechanism"] not in mechanisms
            or not binding["injection"]
            or not binding["sut"]
            or not binding["probes"]
            or binding.get("component_id") not in component_by_id
            or binding.get("oracle_ref") not in oracle_by_id
        ):
            raise ValueError("BINDING_INCOMPLETE")
        for path in binding["sut"]:
            read(root, path)
        for probe in binding["probes"]:
            if probe.get("environment") not in environment_by_id:
                raise ValueError("PROBE_ENVIRONMENT_UNKNOWN")
            if probe["nodeid"].split("::")[0] not in value["test_files"]:
                raise ValueError("TEST_OUTSIDE_SELECTION")
            read(root, probe["nodeid"].split("::")[0])
            expectations = probe["expectations"]
            if not expectations or not probe["required_evidence"]:
                raise ValueError("EXPECTED_EVIDENCE_EMPTY")
            if set(probe["required_evidence"]) != {e["evidence"] for e in expectations}:
                raise ValueError("EXPECTED_EVIDENCE_UNBOUND")
            for expectation in expectations:
                InvariantExpectation.from_dict({**expectation, "invariant_id": binding["invariant_id"]})
        mapped_environments = {probe["environment"] for probe in binding["probes"]}
        required_gap_environments = {
            identifier
            for identifier, environment in environment_by_id.items()
            if environment["unmapped_status"] == "NOT_VERIFIED" and identifier not in mapped_environments
        }
        gaps = binding.get("coverage_gaps", [])
        if not isinstance(gaps, list) or not all(isinstance(gap, dict) for gap in gaps):
            raise ValueError("COVERAGE_GAPS_INVALID")
        gap_environments = [gap.get("environment") for gap in gaps]
        if set(gap_environments) != required_gap_environments or len(set(gap_environments)) != len(gap_environments):
            raise ValueError("COVERAGE_GAP_MAPPING_INVALID")
        for gap in gaps:
            if (
                gap.get("reason") not in COVERAGE_GAP_REASONS
                or not isinstance(gap.get("missing_capability"), str)
                or not gap["missing_capability"].strip()
                or not isinstance(gap.get("next_action"), str)
                or not gap["next_action"].strip()
            ):
                raise ValueError("COVERAGE_GAP_DETAIL_INVALID")
    return value


def source_hashes(root: Path) -> dict[str, str]:
    catalog = load_catalog(root)
    paths = set(DOCS) | set(SEMANTIC_SOURCES) | set(catalog["test_files"])
    paths.update(path for b in catalog["assurance"]["bindings"] for path in b["sut"])
    return {p: digest(read(root, p)) for p in sorted(paths)}


def adapter_hashes(root: Path) -> dict[str, str]:
    """Hash provider adapters separately from verification semantics."""

    return {path: digest(read(root, path)) for path in CI_ADAPTERS}


def review_status(root: Path) -> dict[str, Any]:
    current = source_hashes(root)
    try:
        reviewed = json.loads(read(root, REVIEW))
    except (OSError, ValueError):
        return {"classification": "HOLD", "reason": "REVIEW_MISSING", "changed": sorted(current)}
    old = reviewed.get("source_hashes", {})
    changed = sorted(p for p in set(old) | set(current) if old.get(p) != current.get(p))
    valid = reviewed.get("schema") == "cra.owner_contract_review.v1" and bool(reviewed.get("reason", "").strip())
    return {"classification": "PASS" if valid and not changed else "HOLD", "changed": changed}


def prepare(root: Path, output: Path) -> dict[str, Any]:
    root = root.resolve()
    if output.exists():
        raise FileExistsError("CONTEXT_OUTPUT_EXISTS")
    # Materialize every input before writing; a missing spec cannot silently fall back to history.
    hashes = source_hashes(root)
    contents = [(path, read(root, path).decode()) for path in DOCS]
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    context = "# CRA task context\n\nLocal source only; not live state or mutation authorization.\n"
    context += f"\nRepository: {root}\nGit HEAD: {head}\n"
    for path, content in contents:
        context += f"\n<repository-document path={json.dumps(path)} sha256={json.dumps(hashes[path])}>\n{content}\n</repository-document>\n"
    manifest = {
        "schema": "cra.task_context.v1",
        "root": str(root),
        "git_head": head,
        "source_hashes": hashes,
        "context_sha256": digest(context.encode()),
        "review": review_status(root),
    }
    output.mkdir(parents=True, mode=0o700)
    (output / "context.md").write_text(context, encoding="utf-8")
    dump(output / "context.json", manifest)
    return manifest


def verify_context(root: Path, output: Path) -> list[str]:
    manifest = json.loads(read(output, "context.json"))
    errors = []
    if manifest.get("root") != str(root.resolve()) or manifest.get("source_hashes") != source_hashes(root):
        errors.append("CONTEXT_SOURCE_DRIFT")
    if manifest.get("context_sha256") != digest(read(output, "context.md")):
        errors.append("CONTEXT_BODY_DRIFT")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if manifest.get("git_head") != head:
        errors.append("CONTEXT_REVISION_DRIFT")
    return errors


def junit_cases(path: Path) -> dict[str, ET.Element]:
    cases: dict[str, ET.Element] = {}
    for case in ET.parse(path).getroot().iter("testcase"):
        # The runner pins legacy JUnit so file is explicit; never guess an ambiguous classname.
        node = case.attrib["file"] + "::" + case.attrib["name"]
        if node in cases:
            raise ValueError("DUPLICATE_TEST_RESULT:" + node)
        cases[node] = case
    return cases


def contract_gate(root: Path, candidate: Path, controls: list[dict[str, Any]]) -> dict[str, Any]:
    errors: list[str] = []
    rows: list[dict[str, Any]] = []
    try:
        catalog = load_catalog(root)
        review = review_status(root)
        if review["classification"] != "PASS":
            errors.append("SPEC_REVIEW_STALE")
        nodes = json.loads(read(candidate, "nodeids.json"))
        cases = junit_cases(candidate / "result.xml")
        if len(nodes) != len(set(nodes)) or set(nodes) != set(cases):
            errors.append("COLLECTED_EXECUTED_MISMATCH")
        detected = {c["name"] for c in controls if c.get("detected") is True}
        for binding in catalog["assurance"]["bindings"]:
            identifier = binding["invariant_id"]
            missing_controls = sorted(set(binding["negative_controls"]) - detected)
            if missing_controls:
                errors.append(identifier + ":NEGATIVE_CONTROL_MISSING")
            for probe in binding["probes"]:
                node = probe["nodeid"]
                case = cases.get(node)
                if case is None or any(case.find(tag) is not None for tag in ("failure", "error", "skipped")):
                    errors.append(identifier + ":REQUIRED_TEST_NOT_PASSED:" + node)
                    continue
                properties = case.findall("./properties/property[@name='cra_evidence']")
                if len(properties) != 1:
                    errors.append(identifier + ":EVIDENCE_MISSING_OR_DUPLICATE:" + node)
                    continue
                observation = json.loads(properties[0].attrib["value"])
                if not isinstance(observation, dict):
                    raise ValueError("EVIDENCE_NOT_OBJECT")
                collector = EvidenceCollector("owner-contract", node)
                collector.append("observed", "pytest-property", observation)
                bundle = collector.freeze()
                expectations = tuple(InvariantExpectation.from_dict({**e, "invariant_id": identifier}) for e in probe["expectations"])
                # Do not accept bool as numeric evidence (Python treats True == 1).
                for expectation in expectations:
                    observed = bundle.scalar(expectation.evidence)
                    if type(expectation.expected) in (int, float) and type(observed) not in (int, float):
                        raise ValueError("EVIDENCE_NUMERIC_TYPE")
                result = IndependentOracle().evaluate_expectations(expectations, bundle)
                rows.append(
                    {
                        "invariant_id": identifier,
                        "nodeid": node,
                        "passed": result.passed,
                        "observation": observation,
                        "oracle": asdict(result),
                    }
                )
                if not result.passed:
                    errors.append(identifier + ":ORACLE_REJECTED:" + node)
    except (OSError, ValueError, TypeError, KeyError, ET.ParseError) as error:
        errors.append("CONTRACT_INPUT_INVALID:" + type(error).__name__)
    return {
        "schema": "cra.owner_contract_gate.v1",
        "classification": "PASS" if rows and not errors else "HOLD",
        "errors": errors,
        "probes": rows,
        "invariant_count": len({r["invariant_id"] for r in rows}),
    }


def report(result: dict[str, Any]) -> str:
    """Only this local profile's positive claims are expressible; broader claims stay unproven."""
    gate = result.get("contract_gate", {})
    passed = result.get("classification") == gate.get("classification") == "PASS" and result.get("source_stable") is True
    lines = [
        "# Owner observation 検証結果",
        "",
        f"総合判定: {'PASS' if passed else 'HOLD'}",
        f"仕様対応 gate: {gate.get('classification', 'HOLD')} / invariant {gate.get('invariant_count', 0)} 件",
        "",
        "| 検証対象 | 結果 |",
        "|---|---|",
        f"| owner / signed fixture のローカル契約 | {'確認済み' if passed else '未確認'} |",
    ]
    lines += [f"| {name} | 未検証（この profile の対象外） |" for name in UNPROVEN]
    lines += [
        "",
        "実 kernel child は Python executable（argv[0]=ffmpeg）。実 FFmpeg codec / RTMPS の証拠にはならない。",
        "本番の現在状態・soak の経過はこの試験から判断しない。",
        "ID・実測・source の対応を検査した結果であり、全不具合の不存在や仕様の意味的正しさの保証ではない。",
    ]
    return "\n".join(lines) + "\n"
