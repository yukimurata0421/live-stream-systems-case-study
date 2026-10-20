from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest

from cra_harness import task_contract as gate
from tools.run_cra_workflow_eval import grade, model_name, observed_reads
from tools.verify_owner_trial_read import build_proof

ROOT = Path(__file__).resolve().parents[3]


def test_causal_history_is_a_complete_mandatory_context_set() -> None:
    assert gate.CAUSAL_HISTORY_DOCS == (
        "docs/engineering/causal_history/README.md",
        "docs/engineering/causal_history/SYSTEM_STATE.md",
        "docs/engineering/causal_history/ARCHITECTURE.md",
        "docs/engineering/causal_history/INVARIANTS.md",
        "docs/engineering/causal_history/FAILURE_LEDGER.md",
        "docs/engineering/causal_history/CHANGELOG.md",
        "docs/engineering/causal_history/KNOWN_ASSUMPTIONS.md",
    )
    assert set(gate.CAUSAL_HISTORY_DOCS).issubset(gate.DOCS)
    policy = (ROOT / gate.CAUSAL_HISTORY_DOCS[0]).read_text(encoding="utf-8")
    assert "read every entry in FAILURE_LEDGER.md`" in policy
    assert "at least three existing entries in the same family" in policy
    assert "do not close a local patch alone as the final resolution" in policy
    assert gate.MAX_FOCUSED_CONTEXT_BYTES == 512 * 1024


def test_failure_ledger_keeps_a_complete_cross_document_causal_chain() -> None:
    ledger = (ROOT / "docs/engineering/causal_history/FAILURE_LEDGER.md").read_text(encoding="utf-8")
    invariants = (ROOT / "docs/engineering/causal_history/INVARIANTS.md").read_text(encoding="utf-8")
    entry_matches = list(re.finditer(r"^### (FL-\d{8}-\d{2}) — .+$", ledger, re.MULTILINE))
    assert len(entry_matches) >= 16
    entry_ids = {match.group(1) for match in entry_matches}
    required_fields = {
        "Evidence window / epoch",
        "Symptom",
        "Immediate cause",
        "Root cause family",
        "Affected identity/state",
        "Minimal counterexample",
        "Fix / design change",
        "Invariant / regression",
        "Affected assumptions",
        "Source / status",
    }
    known_invariants = set(re.findall(r"`(INV-[A-Z]+-\d{3})`", invariants))
    assert known_invariants
    for index, match in enumerate(entry_matches):
        end = entry_matches[index + 1].start() if index + 1 < len(entry_matches) else len(ledger)
        entry = ledger[match.end() : end]
        assert required_fields == set(re.findall(r"^- \*\*([^*]+)\*\*:", entry, re.MULTILINE)), match.group(1)
        assert set(re.findall(r"`(INV-[A-Z]+-\d{3})`", entry)).issubset(known_invariants)

    family_rows = re.findall(r"^\| `(FAM-[A-Z-]+)` \|[^\n]+\| ([^\n]+) \|$", ledger, re.MULTILINE)
    assert len(family_rows) == 8
    for family, references in family_rows:
        members = set(re.findall(r"`(FL-\d{8}-\d{2})`", references))
        assert len(members) >= 3, family
        assert members.issubset(entry_ids), family


def test_workflow_eval_uses_a_source_bound_focused_context() -> None:
    source = (ROOT / "tools/run_cra_workflow_eval.py").read_text(encoding="utf-8")
    assert 'prepare(ROOT, output / "context", focus_paths=["src/cra_no_action_soak/recovery_soak.py"])' in source
    assert 'f"python3 tools/verify_owner_trial_read.py --trial' in source
    assert '"Populate missing_invariants with only the exact missing I-OWNER-* IDs, "' in source


@pytest.mark.parametrize("value", ["gpt-5.6-luna", "model_1", "provider.model"])
def test_workflow_eval_accepts_safe_explicit_model_identity(value: str) -> None:
    assert model_name(value) == value


@pytest.mark.parametrize("value", ["", "--model", "model name", "../model", "a" * 129])
def test_workflow_eval_rejects_unsafe_model_identity(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="safe identifier"):
        model_name(value)


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "project"
    catalog = json.loads((ROOT / gate.CATALOG).read_text())
    paths = set(gate.DOCS) | set(gate.SEMANTIC_SOURCES) | set(gate.CI_ADAPTERS) | set(catalog["test_files"])
    paths.update(p for b in catalog["assurance"]["bindings"] for p in b["sut"])
    for path in paths:
        dest = root / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / path, dest)
    (root / gate.REVIEW).parent.mkdir(parents=True, exist_ok=True)
    gate.dump(
        root / gate.REVIEW,
        {"schema": "cra.owner_contract_review.v1", "reason": "test-owned reviewed fixture", "source_hashes": gate.source_hashes(root)},
    )
    monkeypatch.setattr("cra_harness.task_contract.subprocess.check_output", lambda *a, **kw: "a" * 40)
    return root


def artifacts(root: Path, directory: Path) -> tuple[list[dict[str, Any]], ET.Element]:
    """Only gate-input fixtures: the separate real suite supplies actual SUT measurements."""
    catalog = gate.load_catalog(root)
    suite = ET.Element("testsuite")
    nodes = []
    controls = set()
    for binding in catalog["assurance"]["bindings"]:
        controls.update(binding["negative_controls"])
        for probe in binding["probes"]:
            node = probe["nodeid"]
            nodes.append(node)
            file, name = node.split("::")
            case = ET.SubElement(suite, "testcase", file=file, name=name)
            observation: dict[str, Any] = {}
            for e in probe["expectations"]:
                path = e["evidence"].split(".")
                assert path.pop(0) == "observed" and path
                parent = observation
                for part in path[:-1]:
                    parent = parent.setdefault(part, {})
                parent[path[-1]] = e["expected"][0] if e["operator"] == "in" else e["expected"]
            props = ET.SubElement(case, "properties")
            ET.SubElement(props, "property", name="cra_evidence", value=json.dumps(observation))
    directory.mkdir()
    gate.dump(directory / "nodeids.json", nodes)
    ET.ElementTree(suite).write(directory / "result.xml")
    return [{"name": name, "detected": True} for name in controls], suite


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "missing-test",
        "skip",
        "duplicate",
        "no-observation",
        "duplicate-observation",
        "wrong-state",
        "numeric-bool",
        "missing-control",
        "stale-source",
        "stale-spec",
        "bad-xml",
    ],
)
def test_gate_rejects_incomplete_or_unbound_evidence(project: Path, tmp_path: Path, fault: str) -> None:
    candidate = tmp_path / "candidate"
    controls, suite = artifacts(project, candidate)
    first = list(suite)[0]
    if fault == "missing-test":
        suite.remove(first)
    elif fault == "skip":
        ET.SubElement(first, "skipped")
    elif fault == "duplicate":
        suite.append(first)
    elif fault == "no-observation":
        first.remove(first.find("properties"))  # type: ignore[arg-type]
    elif fault == "duplicate-observation":
        props = first.find("properties")
        assert props is not None
        props.append(list(props)[0])
    elif fault in {"wrong-state", "numeric-bool"}:
        prop = first.find("./properties/property")
        assert prop is not None
        value = json.loads(prop.attrib["value"])
        value["state" if fault == "wrong-state" else "physical_attempts"] = "UNKNOWN" if fault == "wrong-state" else False
        prop.set("value", json.dumps(value))
    elif fault == "missing-control":
        controls[0]["detected"] = False
    elif fault in {"stale-source", "stale-spec"}:
        path = project / (gate.SPEC if fault == "stale-spec" else "src/runtime_boundary/recovery_publisher.py")
        path.write_text(path.read_text() + "\n# changed after review\n")
    ET.ElementTree(suite).write(candidate / "result.xml")
    if fault == "bad-xml":
        (candidate / "result.xml").write_text("<broken")
    result = gate.contract_gate(project, candidate, controls)
    assert result["classification"] == ("PASS" if fault == "none" else "HOLD"), result
    if fault == "none":
        assert result["invariant_count"] == 8
        assert len(result["probes"]) == sum(len(b["probes"]) for b in gate.load_catalog(project)["assurance"]["bindings"])


@pytest.mark.parametrize(
    "fault",
    ["missing-binding", "unknown-id", "missing-file", "empty-expectation", "unbound-evidence", "missing-coverage-gap"],
)
def test_catalog_cannot_claim_requirements_without_probes(project: Path, fault: str) -> None:
    path = project / gate.CATALOG
    value = json.loads(path.read_text())
    bindings = value["assurance"]["bindings"]
    if fault == "missing-binding":
        bindings.pop()
    elif fault == "unknown-id":
        bindings[0]["invariant_id"] = "I-OWNER-INVENTED"
    elif fault == "missing-file":
        (project / bindings[0]["sut"][0]).unlink()
    elif fault == "empty-expectation":
        bindings[0]["probes"][0]["expectations"] = []
    elif fault == "unbound-evidence":
        bindings[0]["probes"][0]["required_evidence"].append("observed.unmeasured")
    else:
        diagnostics = next(binding for binding in bindings if binding["invariant_id"] == "I-OWNER-DIAGNOSTICS")
        diagnostics["coverage_gaps"] = []
    gate.dump(path, value)
    with pytest.raises((ValueError, OSError)):
        gate.load_catalog(project)


@pytest.mark.parametrize("fault", ["none", "wrong-root", "body-drift", "source-drift", "missing-doc", "escape", "symlink"])
def test_context_contains_actual_docs_and_detects_wrong_or_stale_inputs(project: Path, tmp_path: Path, fault: str) -> None:
    output = tmp_path / "context"
    if fault in {"missing-doc", "symlink"}:
        path = project / gate.SPEC
        path.unlink()
        if fault == "symlink":
            path.symlink_to(ROOT / gate.SPEC)
        with pytest.raises(ValueError):
            gate.prepare(project, output)
        assert not output.exists()
        return
    if fault == "escape":
        with pytest.raises(ValueError):
            gate.read(project, "../outside")
        return
    manifest = gate.prepare(project, output)
    context = (output / "context.md").read_text()
    assert (project / gate.SPEC).read_text() in context
    for path in gate.CAUSAL_HISTORY_DOCS:
        assert (project / path).read_text() in context
        assert manifest["source_hashes"][path] == gate.digest((project / path).read_bytes())
    if fault == "body-drift":
        (output / "context.md").write_text("I read it")
    elif fault == "source-drift":
        path = project / gate.SPEC
        path.write_text(path.read_text() + "\nchanged\n")
    elif fault == "wrong-root":
        manifest["root"] = str(tmp_path)
        gate.dump(output / "context.json", manifest)
    assert bool(gate.verify_context(project, output)) is (fault != "none")


def test_report_cannot_promote_child_counts_to_live_claims() -> None:
    value = {
        "classification": "PASS",
        "source_stable": True,
        "candidate": {"native_cycles": 64},
        "contract_gate": {"classification": "PASS", "invariant_count": 7},
        "real_ffmpeg": True,
        "formal_soak": True,
    }
    text = gate.report(value)
    for key in gate.UNPROVEN:
        assert f"| {key} | Not verified" in text
    del value["contract_gate"]
    assert "Overall assessment: HOLD" in gate.report(value)


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "annotation",
        "git-head-prefix",
        "git-head-sentence-prefix",
        "git-head-declared-spec",
        "current-spec-prefix",
        "markdown-current-spec",
        "semantic-current-spec",
        "history",
        "history-then-current",
        "overclaim",
        "missing",
        "invented-spec",
    ],
)
def test_agent_eval_grades_decisions_instead_of_read_claims(fault: str) -> None:
    answer = {
        "current_spec": gate.SPEC,
        "owner_schema": "runtime.recovery_evidence.v3",
        "lifecycle_schema": "runtime.child_lifecycle.v2",
        "missing_invariants": ["I-OWNER-RETENTION"],
        "trial_verdict": "HOLD",
        "claims": dict.fromkeys(gate.UNPROVEN, False),
    }
    if fault == "annotation":
        for key in ("current_spec", "owner_schema", "lifecycle_schema"):
            answer[key] += "\uff08\u73fe\u884c\u3002trial \u306e\u6e96\u62e0\u306f\u672a\u78ba\u8a8d\uff09"  # type: ignore[operator]
    elif fault == "git-head-prefix":
        answer["current_spec"] = "Git HEAD 8ac2fbdc3c59142d195647ffc56215db64f6d1cb \u306e " + gate.SPEC
    elif fault == "git-head-sentence-prefix":
        answer["current_spec"] = "Git HEAD 8ac2fbdc3c59142d195647ffc56215db64f6d1cb\u3002\u73fe\u884c owner \u5951\u7d04\u306f `" + gate.SPEC + "`。"
    elif fault == "git-head-declared-spec":
        answer["current_spec"] = "Git HEAD 8ac2fbdc3c59142d195647ffc56215db64f6d1cb\u3002\u6b63\u672c\u306f " + gate.SPEC + "\uff08\u73fe\u884c\u4ed5\u69d8\uff09\u3002"
        answer["evidence_paths"] = [f"/tmp/project/{gate.SPEC}"]
    elif fault == "current-spec-prefix":
        answer["current_spec"] = "\u73fe\u884c\u6b63\u672c\u306f " + gate.SPEC + "（sha256: example）。"
    elif fault == "markdown-current-spec":
        answer["current_spec"] = f"`{gate.SPEC}` \u306e\u73fe\u884c\u5951\u7d04\u3002"
    elif fault == "semantic-current-spec":
        answer["current_spec"] = "Git HEAD 8ac2fbd \u3067\u7167\u5408\u3057\u305f\u73fe\u884cowner\u5951\u7d04\u3068\u5fc5\u9808invariant\u3092\u4f7f\u7528\u3002"
        answer["evidence_paths"] = [f"/tmp/project/{gate.SPEC}:12"]
    elif fault == "history":
        answer["owner_schema"] = "runtime.recovery_evidence.v2"
    elif fault == "history-then-current":
        answer["owner_schema"] = "runtime.recovery_evidence.v2 (see runtime.recovery_evidence.v3)"
    elif fault == "overclaim":
        answer["claims"]["real_ffmpeg"] = True  # type: ignore[index]
    elif fault == "missing":
        answer["trial_verdict"] = "PASS"
    elif fault == "invented-spec":
        answer["current_spec"] = "new-spec-to-create.md"
    assert bool(grade(answer, "I-OWNER-RETENTION")) is (
        fault
        not in {
            "none",
            "annotation",
            "git-head-prefix",
            "git-head-sentence-prefix",
            "git-head-declared-spec",
            "current-spec-prefix",
            "markdown-current-spec",
            "semantic-current-spec",
        }
    )


def test_agent_eval_accepts_only_bounded_case_annotation_on_missing_invariant() -> None:
    answer = {
        "current_spec": gate.SPEC,
        "owner_schema": "runtime.recovery_evidence.v3",
        "lifecycle_schema": "runtime.child_lifecycle.v2",
        "missing_invariants": ["I-OWNER-RETENTION (VC-OWNER-RETENTION-001)"],
        "trial_verdict": "HOLD",
        "claims": dict.fromkeys(gate.UNPROVEN, False),
    }
    assert grade(answer, "I-OWNER-RETENTION") == []
    answer["missing_invariants"] = ["missing I-OWNER-RETENTION (VC-OWNER-RETENTION-001)"]
    assert grade(answer, "I-OWNER-RETENTION") == ["MISSING_REQUIREMENT_ACCEPTED"]


def test_agent_eval_binds_the_final_answer_to_the_current_read_proof() -> None:
    answer = {
        "current_spec": gate.SPEC,
        "owner_schema": "runtime.recovery_evidence.v3",
        "lifecycle_schema": "runtime.child_lifecycle.v2",
        "missing_invariants": ["I-OWNER-RETENTION"],
        "trial_verdict": "HOLD",
        "claims": dict.fromkeys(gate.UNPROVEN, False),
        "read_proof_sha256": "a" * 64,
    }
    assert grade(answer, "I-OWNER-RETENTION", read_proof_sha256="a" * 64) == []
    assert grade(answer, "I-OWNER-RETENTION", read_proof_sha256="b" * 64) == ["READ_PROOF_DIGEST_MISMATCH"]


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "projection",
        "projection-without-count",
        "invariants-projection",
        "invariants-without-count",
        "binding-invariants-projection",
        "binding-ids-projection",
        "bindings-projection",
        "bindings-without-count",
        "nested-bindings-projection",
        "nested-observation-projection",
        "self-report",
        "printed-complete",
        "printed-projection",
        "failed-read",
        "only-hashes",
        "partial",
        "redundant-context",
        "wrong-ids",
        "wrong-invariants",
        "wrong-binding-invariants",
        "wrong-binding-ids",
        "wrong-count",
        "bool-count",
        "wrong-observation",
        "missing-file",
        "markers-only",
        "oversized-output",
    ],
)
def test_read_proof_requires_actual_successful_tool_output(fault: str) -> None:
    catalog = {"assurance": {"bindings": [{"invariant_id": "I-OWNER-CLEANUP"}]}}
    observation = {"native_cycles": 64, "executable": "python3"}
    item = {
        "type": "command_execution",
        "command": "cat trial-catalog.json observations.json",
        "exit_code": 0,
        "aggregated_output": json.dumps(catalog) + "\n" + json.dumps(observation),
    }
    if fault in {
        "projection",
        "projection-without-count",
        "invariants-projection",
        "invariants-without-count",
        "binding-invariants-projection",
        "binding-ids-projection",
        "wrong-ids",
        "wrong-invariants",
        "wrong-binding-invariants",
        "wrong-count",
        "bool-count",
    }:
        item["command"] = (
            "jq '{binding_invariants:[.assurance.bindings[].invariant_id],binding_count:(.assurance.bindings|length)}' trial-catalog.json; cat observations.json"  # noqa: E501
            if fault in {"binding-invariants-projection", "wrong-binding-invariants"}
            else (
                "jq '{binding_ids:[.assurance.bindings[].invariant_id],binding_count:(.assurance.bindings|length)}' trial-catalog.json; cat observations.json"  # noqa: E501
                if fault == "binding-ids-projection"
                else (
                    "jq '{invariants:[.assurance.bindings[].invariant_id],binding_count:(.assurance.bindings|length)}' trial-catalog.json; cat observations.json"  # noqa: E501
                    if fault in {"invariants-projection", "invariants-without-count", "wrong-invariants"}
                    else "jq '{invariant_ids:[.assurance.bindings[].invariant_id],binding_count:(.assurance.bindings|length)}' trial-catalog.json; cat observations.json"  # noqa: E501
                )
            )
        )
        key = (
            "binding_invariants"
            if fault in {"binding-invariants-projection", "wrong-binding-invariants"}
            else "binding_ids"
            if fault == "binding-ids-projection"
            else "invariants"
            if fault in {"invariants-projection", "invariants-without-count", "wrong-invariants"}
            else "invariant_ids"
        )
        projected = {key: ["I-OWNER-CLEANUP"], "binding_count": 1}
        if fault in {"projection-without-count", "invariants-without-count"}:
            projected.pop("binding_count")
        if fault == "wrong-ids":
            projected["invariant_ids"] = ["I-OWNER-IDENTITY"]
        elif fault == "wrong-invariants":
            projected["invariants"] = ["I-OWNER-IDENTITY"]
        elif fault == "wrong-binding-invariants":
            projected["binding_invariants"] = ["I-OWNER-IDENTITY"]
        elif fault in {"wrong-count", "bool-count"}:
            projected["binding_count"] = True if fault == "bool-count" else 2
        item["aggregated_output"] = json.dumps(projected) + "\n" + json.dumps(observation)
    elif fault in {"bindings-projection", "nested-bindings-projection", "wrong-binding-ids"}:
        item["command"] = (
            "jq '{bindings:[.assurance.bindings[]|{invariant_id}],binding_count:(.assurance.bindings|length)}' trial-catalog.json; cat observations.json"  # noqa: E501
        )
        identifier = "I-OWNER-IDENTITY" if fault == "wrong-binding-ids" else "I-OWNER-CLEANUP"
        projection = {"bindings": [{"invariant_id": identifier}], "binding_count": 1}
        if fault == "nested-bindings-projection":
            projection = {"assurance": projection}
        item["aggregated_output"] = json.dumps(projection) + "\n" + json.dumps(observation)
    elif fault == "bindings-without-count":
        item["command"] = "jq '{bindings:[.assurance.bindings[]|{invariant_id}]}' trial-catalog.json; cat observations.json"
        item["aggregated_output"] = json.dumps({"bindings": [{"invariant_id": "I-OWNER-CLEANUP"}]}) + "\n" + json.dumps(observation)
    elif fault == "self-report":
        item["type"] = "agent_message"
    elif fault == "printed-complete":
        item["command"] = "printf 'trial-catalog.json'; cat observations.json"
    elif fault == "printed-projection":
        item["command"] = "printf '.assurance.bindings trial-catalog.json'; cat observations.json"
        item["aggregated_output"] = (
            json.dumps({"invariant_ids": ["I-OWNER-CLEANUP"], "binding_count": 1}) + "\n" + json.dumps(observation)
        )
    elif fault == "nested-observation-projection":
        item["command"] = "cat trial-catalog.json; jq '{top_type:type,top_keys:keys,value:.}' observations.json"
        item["aggregated_output"] = json.dumps(catalog) + "\n" + json.dumps({"value": observation})
    elif fault == "failed-read":
        item["exit_code"] = 1
    elif fault == "only-hashes":
        item["aggregated_output"] = "some-file-hashes"
    elif fault == "partial":
        item["aggregated_output"] = '{"bindings": []}'
    elif fault == "redundant-context":
        item["command"] = str(item["command"]) + "; python tools/cra_task.py prepare --output unused"
    elif fault == "wrong-observation":
        item["aggregated_output"] = json.dumps(catalog) + '\n{"native_cycles": 63, "executable": "python3"}'
    elif fault == "missing-file":
        item["command"] = "cat unrelated-file.json"
    elif fault == "markers-only":
        item["aggregated_output"] = '"bindings" "native_cycles"'
    elif fault == "oversized-output":
        item["aggregated_output"] = " " * (2 * 1024 * 1024 + 1) + str(item["aggregated_output"])
    assert observed_reads(json.dumps({"type": "item.completed", "item": item}), catalog=catalog, observations=observation) is (
        fault
        in {
            "none",
            "projection",
            "projection-without-count",
            "invariants-projection",
            "invariants-without-count",
            "binding-invariants-projection",
            "binding-ids-projection",
            "bindings-projection",
            "bindings-without-count",
            "nested-bindings-projection",
            "nested-observation-projection",
        }
    )


@pytest.mark.parametrize("comm_mode", ("-23", "-13"))
def test_read_proof_accepts_exact_canonical_trial_comm_projection(comm_mode: str) -> None:
    canonical = json.loads((ROOT / gate.CATALOG).read_text())
    catalog = json.loads((ROOT / gate.CATALOG).read_text())
    missing = "I-OWNER-RETENTION"
    catalog["assurance"]["bindings"] = [binding for binding in catalog["assurance"]["bindings"] if binding["invariant_id"] != missing]
    observation = {"native_cycles": 64, "executable": "python3"}
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": (
                    "comm -23 <(jq -r '.assurance.bindings[].invariant_id' "
                    f"{gate.CATALOG} | sort) <(jq -r '.assurance.bindings[].invariant_id' trial-catalog.json | sort)"
                    if comm_mode == "-23"
                    else "comm -13 <(jq -r '.assurance.bindings[].invariant_id' trial-catalog.json | sort) "
                    f"<(jq -r '.assurance.bindings[].invariant_id' {gate.CATALOG} | sort)"
                ),
                "exit_code": 0,
                "aggregated_output": missing + "\nfollow-up read output\n",
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "cat observations.json",
                "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]

    assert missing in {binding["invariant_id"] for binding in canonical["assurance"]["bindings"]}
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation)


def test_read_proof_accepts_jq_optional_iterator_in_exact_comm_projection() -> None:
    catalog = json.loads((ROOT / gate.CATALOG).read_text())
    missing = "I-OWNER-IDENTITY"
    catalog["assurance"]["bindings"] = [
        binding for binding in catalog["assurance"]["bindings"] if binding["invariant_id"] != missing
    ]
    observation = {"native_cycles": 64, "executable": "python3"}
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": (
                    "comm -23 <(jq -r '.assurance.bindings[]?.invariant_id' "
                    f"{gate.CATALOG} | sort -u) <(jq -r '.assurance.bindings[]?.invariant_id' trial-catalog.json | sort -u)"
                ),
                "exit_code": 0,
                "aggregated_output": missing + "\n",
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "jq -r '.' observations.json",
                "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]

    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation)


@pytest.mark.parametrize(
    ("projected_ids", "expected"),
    [
        (["I-OWNER-CLEANUP", "I-OWNER-REGISTRY"], True),
        (["I-OWNER-CLEANUP"], False),
        (["I-OWNER-CLEANUP", "I-OWNER-REGISTRY", "I-OWNER-IDENTITY"], False),
        (["I-OWNER-REGISTRY", "I-OWNER-CLEANUP"], False),
    ],
)
def test_read_proof_accepts_only_exact_direct_invariant_id_projection(
    projected_ids: list[str], expected: bool
) -> None:
    catalog = {
        "assurance": {
            "bindings": [
                {"invariant_id": "I-OWNER-CLEANUP"},
                {"invariant_id": "I-OWNER-REGISTRY"},
            ]
        }
    }
    observation = {"native_cycles": 64, "executable": "python3"}
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "jq -r '.assurance.bindings[] | .invariant_id' trial-catalog.json",
                "exit_code": 0,
                "aggregated_output": "\n".join(projected_ids) + "\n",
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "jq . observations.json",
                "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]

    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is expected


@pytest.mark.parametrize(
    ("fault", "expected"),
    [
        ("none", True),
        ("missing", False),
        ("extra", False),
        ("no-sort", False),
    ],
)
def test_read_proof_accepts_sorted_direct_ids_only_with_sort_command(fault: str, expected: bool) -> None:
    catalog = {
        "assurance": {
            "bindings": [
                {"invariant_id": "I-OWNER-REGISTRY"},
                {"invariant_id": "I-OWNER-CLEANUP"},
            ]
        }
    }
    observation = {"native_cycles": 64, "executable": "python3"}
    command = "jq -r '.assurance.bindings[].invariant_id' trial-catalog.json | sort -u"
    ids = ["I-OWNER-CLEANUP", "I-OWNER-REGISTRY"]
    if fault == "missing":
        ids.pop()
    elif fault == "extra":
        ids.append("I-OWNER-IDENTITY")
    elif fault == "no-sort":
        command = command.replace(" | sort -u", "")
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": command,
                "exit_code": 0,
                "aggregated_output": "\n".join(ids) + "\n",
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "cat observations.json",
                "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is expected


@pytest.mark.parametrize(
    ("projected_count", "projected_ids", "with_schemas", "self_report", "expected"),
    [
        (2, "I-OWNER-CLEANUP,I-OWNER-REGISTRY", False, False, True),
        (2, "I-OWNER-CLEANUP,I-OWNER-REGISTRY", True, False, True),
        (1, "I-OWNER-CLEANUP,I-OWNER-REGISTRY", True, False, False),
        (2, "I-OWNER-CLEANUP", True, False, False),
        (2, "I-OWNER-CLEANUP,I-OWNER-IDENTITY,I-OWNER-REGISTRY", True, False, False),
        (2, "I-OWNER-REGISTRY,I-OWNER-CLEANUP", True, False, False),
        (2, "I-OWNER-CLEANUP,I-OWNER-REGISTRY", True, True, False),
    ],
)
def test_read_proof_accepts_only_exact_jq_sorted_unique_projection(
    projected_count: int,
    projected_ids: str,
    with_schemas: bool,
    self_report: bool,
    expected: bool,
) -> None:
    catalog = {
        "schema": "trial.catalog.v1",
        "assurance": {
            "schema": "trial.assurance.v1",
            "bindings": [
                {"invariant_id": "I-OWNER-REGISTRY"},
                {"invariant_id": "I-OWNER-CLEANUP"},
            ],
        },
    }
    observation = {"native_cycles": 64, "executable": "python3"}
    selection = (
        ".schema, .assurance.schema, " if with_schemas else ""
    ) + '(.assurance.bindings|length), (.assurance.bindings|map(.invariant_id)|unique|join(","))'
    command = f"jq -r '{selection}' trial-catalog.json"
    if self_report:
        command += "; printf '2\\nI-OWNER-CLEANUP,I-OWNER-REGISTRY\\n'"
    lines = ["trial.catalog.v1", "trial.assurance.v1"] if with_schemas else []
    lines.extend([str(projected_count), projected_ids])
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": command,
                "exit_code": 0,
                "aggregated_output": "\n".join(lines) + "\n",
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "cat observations.json",
                "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is expected


@pytest.mark.parametrize("fault", ["none", "wrong-count", "wrong-binding", "null-input", "no-dynamic-count", "self-report"])
def test_read_proof_accepts_only_exact_structured_jq_binding_matrix(fault: str) -> None:
    catalog = {
        "schema": "trial.catalog.v1",
        "assurance": {
            "bindings": [
                {
                    "invariant_id": "I-OWNER-CLEANUP",
                    "verification_case_id": "VC-OWNER-CLEANUP-001",
                    "mechanism": "OC-01",
                    "probes": [{"environment": "fake"}],
                    "sut": ["source.py"],
                },
                {
                    "invariant_id": "I-OWNER-REGISTRY",
                    "verification_case_id": "VC-OWNER-REGISTRY-001",
                    "mechanism": "OC-02",
                    "probes": [],
                    "sut": ["other.py"],
                },
            ]
        },
    }
    observation = {"native_cycles": 64}
    command = (
        "jq '{schema, binding_count:(.assurance.bindings|length? // 0), "
        "binding_ids:[.assurance.bindings[]? | "
        "{verification_case_id,invariant_id,mechanism,probe_count:(.probes|length),sut}]}' trial-catalog.json"
    )
    projection = {
        "schema": catalog["schema"],
        "binding_count": 2,
        "binding_ids": [
            {"verification_case_id": row["verification_case_id"], "invariant_id": row["invariant_id"],
             "mechanism": row["mechanism"], "probe_count": len(row["probes"]), "sut": row["sut"]}
            for row in catalog["assurance"]["bindings"]
        ],
    }
    if fault == "wrong-count":
        projection["binding_count"] = 1
    elif fault == "wrong-binding":
        projection["binding_ids"][1]["mechanism"] = "OC-99"
    elif fault == "null-input":
        command = command.replace("jq '", "jq -n '")
    elif fault == "no-dynamic-count":
        command = command.replace("(.assurance.bindings|length? // 0)", "2")
    elif fault == "self-report":
        command += "; printf '2\\n'"
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": command,
                "exit_code": 0,
                "aggregated_output": json.dumps(projection),
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "cat observations.json",
                "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is (fault == "none")


@pytest.mark.parametrize("fault", ["none", "missing-row", "wrong-nodeid", "null-input", "self-report", "wrong-source"])
def test_read_proof_accepts_only_complete_source_derived_tsv_matrix(fault: str) -> None:
    catalog = {
        "schema": "trial.catalog.v1",
        "assurance": {
            "bindings": [
                {
                    "verification_case_id": "VC-OWNER-CLEANUP-001",
                    "invariant_id": "I-OWNER-CLEANUP",
                    "mechanism": "OC-01",
                    "probes": [{"environment": "fake", "nodeid": "tests/cleanup.py::test_one"}],
                },
                {
                    "verification_case_id": "VC-OWNER-REGISTRY-001",
                    "invariant_id": "I-OWNER-REGISTRY",
                    "mechanism": "OC-02",
                    "probes": [{"environment": "process", "nodeid": "tests/registry.py::test_two"}],
                },
            ]
        },
    }
    observation = {"native_cycles": 64}
    command = (
        "jq -r '.assurance.bindings[] | [.verification_case_id, .invariant_id, .mechanism, "
        "((.probes|length)|tostring), ([.probes[].environment]|unique|join(\",\")), "
        "([.probes[].nodeid]|join(\" ; \"))] | @tsv' trial-catalog.json"
    )
    rows = [
        "\t".join((row["verification_case_id"], row["invariant_id"], row["mechanism"], str(len(row["probes"])),
                   row["probes"][0]["environment"], row["probes"][0]["nodeid"]))
        for row in catalog["assurance"]["bindings"]
    ]
    if fault == "missing-row":
        rows.pop()
    elif fault == "wrong-nodeid":
        rows[1] = rows[1].replace("tests/registry.py::test_two", "tests/registry.py::test_other")
    elif fault == "null-input":
        command = command.replace("jq -r", "jq -r -n")
    elif fault == "self-report":
        command += "; printf 'fabricated\\n'"
    elif fault == "wrong-source":
        command = command.replace("trial-catalog.json", "other-catalog.json")
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": command,
                "exit_code": 0,
                "aggregated_output": "\n".join(rows) + "\n",
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "cat observations.json",
                "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is (fault == "none")


@pytest.mark.parametrize("fault", ["none", "wrong-missing", "wrong-comparison", "partial-read", "self-report"])
def test_read_proof_accepts_only_whole_file_python_binding_difference(fault: str) -> None:
    canonical = json.loads((ROOT / gate.CATALOG).read_text(encoding="utf-8"))
    catalog = copy.deepcopy(canonical)
    omitted = catalog["assurance"]["bindings"].pop(0)["invariant_id"]
    observation = {"native_cycles": 64}
    command = (
        "python3 - <<'PY'\n"
        "import json\nfrom pathlib import Path\n"
        "trial=json.loads(Path('trial-catalog.json').read_text())\n"
        f"canon=json.loads(Path('{gate.CATALOG}').read_text())\n"
        "t={b['invariant_id']:b for b in trial['assurance']['bindings']}\n"
        "c={b['invariant_id']:b for b in canon['assurance']['bindings']}\n"
        "print('missing',sorted(set(c)-set(t)))\n"
        "print('extra',sorted(set(t)-set(c)))\n"
        "print('common_binding_differences',[k for k in sorted(set(t)&set(c)) if t[k]!=c[k]])\nPY"
    )
    output = f"missing ['{omitted}']\nextra []\ncommon_binding_differences []\n"
    if fault == "wrong-missing":
        output = output.replace(omitted, "I-OWNER-NONEXISTENT")
    elif fault == "wrong-comparison":
        output = output.replace("common_binding_differences []", "common_binding_differences ['I-OWNER-CLEANUP']")
    elif fault == "partial-read":
        command = command.replace(".read_text()", ".read_text()[:260]", 1)
    elif fault == "self-report":
        command += "; printf 'missing ['"
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": command,
                "exit_code": 0,
                "aggregated_output": output,
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "cat observations.json",
                "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is (fault == "none")


@pytest.mark.parametrize(
    ("fault", "expected"),
    [
        ("none", True),
        ("wrong-keys", False),
        ("missing-id", False),
        ("self-report", False),
        ("null-input", False),
        ("no-count-expression", False),
        ("wrong-source", False),
    ],
)
def test_read_proof_accepts_complete_jq_array_projection_with_keys(fault: str, expected: bool) -> None:
    catalog = {
        "schema": "trial.catalog.v1",
        "assurance": {
            "schema": "trial.assurance.v1",
            "bindings": [
                {"invariant_id": "I-OWNER-REGISTRY"},
                {"invariant_id": "I-OWNER-CLEANUP"},
            ],
        },
    }
    observation = {"native_cycles": 64, "executable": "python3"}
    command = (
        "jq -r 'keys, .schema, (.assurance.schema // \"NO_ASSURANCE_SCHEMA\"), "
        "(.assurance.bindings | length), ([.assurance.bindings[].invariant_id] | unique | join(\",\"))' "
        "trial-catalog.json"
    )
    output_lines = [
        *json.dumps(sorted(catalog), indent=2).splitlines(),
        catalog["schema"],
        catalog["assurance"]["schema"],
        "2",
        "I-OWNER-CLEANUP,I-OWNER-REGISTRY",
    ]
    if fault == "wrong-keys":
        output_lines[1] = '  "other",'
    elif fault == "missing-id":
        output_lines[-1] = "I-OWNER-CLEANUP"
    elif fault == "self-report":
        command += "; printf '2\\nI-OWNER-CLEANUP,I-OWNER-REGISTRY\\n'"
    elif fault == "null-input":
        command = command.replace("jq -r", "jq -r -n")
    elif fault == "no-count-expression":
        command = command.replace("(.assurance.bindings | length)", "2")
    elif fault == "wrong-source":
        command = command.replace("trial-catalog.json", "other-catalog.json")
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": command,
                "exit_code": 0,
                "aggregated_output": "\n".join(output_lines) + "\n",
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "cat observations.json",
                "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is expected


@pytest.mark.parametrize(
    ("fault", "expected"),
    [
        ("none", True),
        ("missing-id", False),
        ("extra-id", False),
        ("wrong-count", False),
        ("no-sort", False),
    ],
)
def test_read_proof_accepts_sorted_json_projection_only_when_jq_declares_sort(fault: str, expected: bool) -> None:
    catalog = {
        "assurance": {
            "bindings": [
                {"invariant_id": "I-OWNER-REGISTRY"},
                {"invariant_id": "I-OWNER-CLEANUP"},
            ]
        }
    }
    observation = {"native_cycles": 64, "executable": "python3"}
    command = (
        "jq '{binding_count:(.assurance.bindings|length), "
        "invariant_ids:(.assurance.bindings|map(.invariant_id)|unique)}' trial-catalog.json"
    )
    projected = {"binding_count": 2, "invariant_ids": ["I-OWNER-CLEANUP", "I-OWNER-REGISTRY"]}
    if fault == "missing-id":
        projected["invariant_ids"] = ["I-OWNER-CLEANUP"]
    elif fault == "extra-id":
        projected["invariant_ids"].append("I-OWNER-IDENTITY")
    elif fault == "wrong-count":
        projected["binding_count"] = 1
    elif fault == "no-sort":
        command = command.replace("|unique", "")
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": command,
                "exit_code": 0,
                "aggregated_output": json.dumps(projected),
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "cat observations.json",
                "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is expected


@pytest.mark.parametrize(
    ("fault", "expected"),
    [
        ("none", True),
        ("missing-id", False),
        ("wrong-count", False),
        ("wrong-digest", False),
        ("partial-read", False),
        ("printed-list", False),
    ],
)
def test_read_proof_accepts_exact_file_derived_python_summary(fault: str, expected: bool) -> None:
    catalog = {
        "assurance": {
            "schema": "trial.assurance.v1",
            "bindings": [{"invariant_id": "I-OWNER-CLEANUP"}, {"invariant_id": "I-OWNER-REGISTRY"}],
        }
    }
    observation = {"native_cycles": 64, "executable": "python3"}
    payload = (json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    command = (
        "python3 - <<'PY'\n"
        "import hashlib,json\nfrom pathlib import Path\n"
        "p=Path('trial-catalog.json')\ntrial=json.loads(p.read_text())\n"
        "assurance=trial.get('assurance',{})\nbindings=assurance.get('bindings',[])\n"
        "print('binding_count',len(bindings))\n"
        "print('invariants',[b.get('invariant_id') for b in bindings])\n"
        "data=p.read_bytes(); print(hashlib.sha256(data).hexdigest())\nPY"
    )
    count = 1 if fault == "wrong-count" else 2
    ids = ["I-OWNER-CLEANUP"] if fault == "missing-id" else ["I-OWNER-CLEANUP", "I-OWNER-REGISTRY"]
    file_hash = "0" * 64 if fault == "wrong-digest" else hashlib.sha256(payload).hexdigest()
    output = (
        "TRIAL\n"
        "assurance_schema trial.assurance.v1\n"
        f"binding_count {count}\n"
        f"invariants {ids!r}\n"
        f"sha256 trial-catalog.json {file_hash} bytes {len(payload)}\n"
    )
    if fault == "partial-read":
        command = command.replace("p.read_bytes()", "p.read_bytes()[:100]")
    elif fault == "printed-list":
        command = command.replace("[b.get('invariant_id') for b in bindings]", "['I-OWNER-CLEANUP','I-OWNER-REGISTRY']")
    events = [
        {"type": "item.completed", "item": {"type": "command_execution", "command": command, "exit_code": 0, "aggregated_output": output}},
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution", "command": "cat observations.json", "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is expected


@pytest.mark.parametrize(
    ("fault", "expected"),
    [
        ("none", True),
        ("wrong-trial-ids", False),
        ("false-comparison", False),
        ("printed-comparison", False),
        ("wrong-source", False),
        ("catalog-not-equal", False),
    ],
)
def test_read_proof_accepts_exact_whole_catalog_python_comparison(fault: str, expected: bool) -> None:
    canonical = json.loads((ROOT / gate.CATALOG).read_text())
    catalog = copy.deepcopy(canonical)
    catalog["assurance"]["bindings"] = [
        binding for binding in catalog["assurance"]["bindings"] if binding["invariant_id"] != "I-OWNER-IDENTITY"
    ]
    trial_ids = [binding["invariant_id"] for binding in catalog["assurance"]["bindings"]]
    current_ids = [binding["invariant_id"] for binding in canonical["assurance"]["bindings"]]
    observation = {"native_cycles": 64, "executable": "python3"}
    command = (
        "python3 - <<'PY'\nimport json\nfrom pathlib import Path\n"
        "T=json.loads(Path('trial-catalog.json').read_text())\n"
        f"C=json.loads(Path('{gate.CATALOG}').read_text())\n"
        "C2=json.loads(json.dumps(C))\n"
        "C2['assurance']['bindings']=[b for b in C2['assurance']['bindings'] "
        "if b['invariant_id']!='I-OWNER-IDENTITY']\n"
        "print('trial_equals_current_without_identity=',T==C2)\n"
        "print('trial_invariants=',[b['invariant_id'] for b in T['assurance']['bindings']])\n"
        "print('current_invariants=',[b['invariant_id'] for b in C['assurance']['bindings']])\nPY"
    )
    output = (
        "trial_equals_current_without_identity= True\n"
        f"trial_invariants= {trial_ids!r}\n"
        f"current_invariants= {current_ids!r}\n"
    )
    if fault == "wrong-trial-ids":
        output = output.replace(f"trial_invariants= {trial_ids!r}", "trial_invariants= ['I-OWNER-CLEANUP']")
    elif fault == "false-comparison":
        output = output.replace("trial_equals_current_without_identity= True", "trial_equals_current_without_identity= False")
    elif fault == "printed-comparison":
        command = command.replace("T==C2", "True")
    elif fault == "wrong-source":
        command = command.replace("trial-catalog.json", "other-catalog.json")
    elif fault == "catalog-not-equal":
        catalog["assurance"]["bindings"][0]["component_id"] = "other-component"
    events = [
        {"type": "item.completed", "item": {"type": "command_execution", "command": command, "exit_code": 0, "aggregated_output": output}},
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution", "command": "cat observations.json", "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is expected


@pytest.mark.parametrize("key", ["invariant_id", "verification_case_id"])
@pytest.mark.parametrize(
    ("fault", "expected"),
    [
        ("none", True),
        ("hardcoded-true", False),
        ("wrong-output", False),
        ("partial-read", False),
        ("wrong-source", False),
        ("catalog-drift", False),
    ],
)
def test_read_proof_accepts_semantic_filtered_whole_catalog_comparison(
    key: str, fault: str, expected: bool
) -> None:
    canonical = json.loads((ROOT / gate.CATALOG).read_text())
    omitted = canonical["assurance"]["bindings"][0]
    catalog = copy.deepcopy(canonical)
    catalog["assurance"]["bindings"] = catalog["assurance"]["bindings"][1:]
    observation = {"native_cycles": 64, "executable": "python3"}
    value = omitted[key]
    command = (
        "python3 - <<'PY'\nimport copy,json\nfrom pathlib import Path\n"
        "trial=json.loads(Path('trial-catalog.json').read_text())\n"
        f"canon=json.loads(Path('{gate.CATALOG}').read_text())\n"
        "filtered=copy.deepcopy(canon)\n"
        "filtered['assurance']['bindings']=[b for b in filtered['assurance']['bindings'] "
        f"if b.get('{key}') != '{value}']\n"
        "print('whole_catalog_equal=', trial == filtered)\nPY"
    )
    output = "whole_catalog_equal= True\n"
    if fault == "hardcoded-true":
        command = command.replace("trial == filtered", "True")
    elif fault == "wrong-output":
        output = "whole_catalog_equal= False\n"
    elif fault == "partial-read":
        command = command.replace(".read_text()", ".read_text()[:100]", 1)
    elif fault == "wrong-source":
        command = command.replace("trial-catalog.json", "other-catalog.json")
    elif fault == "catalog-drift":
        catalog["execution_boundary"]["production_mutation"] = True
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": command,
                "exit_code": 0,
                "aggregated_output": output,
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": "cat observations.json",
                "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads(
        "\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation
    ) is expected


@pytest.mark.parametrize(
    ("fault", "expected"),
    [
        ("none", True),
        ("missing-hash", False),
        ("wrong-hash", False),
        ("wrong-count", False),
        ("wrong-ids", False),
        ("printed-ids", False),
        ("wrong-source", False),
    ],
)
def test_read_proof_accepts_separate_exact_hash_and_dynamic_summary(fault: str, expected: bool) -> None:
    catalog = {
        "assurance": {
            "schema": "trial.assurance.v1",
            "bindings": [{"invariant_id": "I-OWNER-CLEANUP"}, {"invariant_id": "I-OWNER-REGISTRY"}],
        }
    }
    observation = {"native_cycles": 64, "executable": "python3"}
    payload = (json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    hash_value = "0" * 64 if fault == "wrong-hash" else hashlib.sha256(payload).hexdigest()
    hash_command = "sha256sum trial-catalog.json observations.json"
    if fault == "missing-hash":
        hash_command = "printf 'hash read'"
    summary_command = (
        "python3 - <<'PY'\nimport json\nfrom pathlib import Path\n"
        "trial=json.loads(Path('trial-catalog.json').read_text())\n"
        "def summary(label, data):\n"
        " ass=data.get('assurance',{})\n bindings=ass.get('bindings',[])\n"
        " print(label)\n print('assurance_schema',ass.get('schema'))\n"
        " print('binding_count',len(bindings))\n"
        " print('invariant_ids',[b.get('invariant_id') for b in bindings])\n"
        "summary('TRIAL', trial)\nPY"
    )
    if fault == "printed-ids":
        summary_command = summary_command.replace(
            "[b.get('invariant_id') for b in bindings]", "['I-OWNER-CLEANUP','I-OWNER-REGISTRY']"
        )
    elif fault == "wrong-source":
        summary_command = summary_command.replace("trial-catalog.json", "other-catalog.json")
    count = 1 if fault == "wrong-count" else 2
    ids = ["I-OWNER-CLEANUP"] if fault == "wrong-ids" else ["I-OWNER-CLEANUP", "I-OWNER-REGISTRY"]
    summary_output = (
        f"TRIAL\nassurance_schema trial.assurance.v1\nbinding_count {count}\ninvariant_ids {ids!r}\n"
    )
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution", "command": hash_command, "exit_code": 0,
                "aggregated_output": f"{hash_value}  trial-catalog.json\n",
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution", "command": summary_command, "exit_code": 0,
                "aggregated_output": summary_output,
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution", "command": "cat observations.json", "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is expected


@pytest.mark.parametrize(
    ("fault", "expected"),
    [
        ("none", True),
        ("missing-hash", False),
        ("wrong-ids", False),
        ("wrong-count", False),
        ("wrong-schema", False),
        ("printed-ids", False),
        ("wrong-source", False),
    ],
)
def test_read_proof_accepts_named_python_projection_with_separate_hash(fault: str, expected: bool) -> None:
    catalog = {
        "assurance": {
            "schema": "trial.assurance.v1",
            "bindings": [{"invariant_id": "I-OWNER-CLEANUP"}, {"invariant_id": "I-OWNER-REGISTRY"}],
        }
    }
    observation = {"native_cycles": 64, "executable": "python3"}
    payload = (json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    command = (
        "python3 - <<'PY'\nimport json\nfrom pathlib import Path\n"
        "trial=json.loads(Path('trial-catalog.json').read_text())\n"
        "canon=json.loads(Path('harness/scenarios/owner_observation_chaos_v1.json').read_text())\n"
        "for name,obj in [('trial',trial),('canonical',canon)]:\n"
        " bindings=obj.get('assurance',{}).get('bindings',[])\n"
        " print(name,'top_keys=',sorted(obj),'assurance_schema=',obj.get('assurance',{}).get('schema'))\n"
        " print(name,'invariants=',[b.get('invariant_id') for b in bindings])\n"
        " print(name,'binding_count=',len(bindings),'probe_count=',sum(len(b.get('probes',[])) for b in bindings))\nPY"
    )
    if fault == "printed-ids":
        command = command.replace(
            "[b.get('invariant_id') for b in bindings]", "['I-OWNER-CLEANUP','I-OWNER-REGISTRY']"
        )
    elif fault == "wrong-source":
        command = command.replace("trial-catalog.json", "other-catalog.json")
    ids = ["I-OWNER-CLEANUP"] if fault == "wrong-ids" else ["I-OWNER-CLEANUP", "I-OWNER-REGISTRY"]
    count = 1 if fault == "wrong-count" else 2
    schema = "wrong.assurance.v1" if fault == "wrong-schema" else "trial.assurance.v1"
    output = (
        f"trial top_keys= ['assurance'] assurance_schema= {schema}\n"
        f"trial invariants= {ids!r}\ntrial binding_count= {count} probe_count= 0\n"
    )
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution", "command": "sha256sum trial-catalog.json", "exit_code": 0,
                "aggregated_output": (
                    "" if fault == "missing-hash" else f"{hashlib.sha256(payload).hexdigest()}  trial-catalog.json\n"
                ),
            },
        },
        {"type": "item.completed", "item": {"type": "command_execution", "command": command, "exit_code": 0, "aggregated_output": output}},
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution", "command": "cat observations.json", "exit_code": 0,
                "aggregated_output": json.dumps(observation),
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is expected


@pytest.mark.parametrize(
    ("fault", "expected"),
    [
        ("none", True),
        ("wrong-count", False),
        ("wrong-ids", False),
        ("wrong-schema", False),
        ("printed-ids", False),
        ("wrong-source", False),
        ("printed-observation", False),
    ],
)
def test_read_proof_accepts_labeled_whole_file_projection_without_confusing_stat_printf(
    fault: str, expected: bool,
) -> None:
    catalog = {
        "schema": "trial.catalog.v1",
        "assurance": {"schema": "trial.assurance.v1", "bindings": [
            {"invariant_id": "I-OWNER-CLEANUP"}, {"invariant_id": "I-OWNER-REGISTRY"},
        ]},
    }
    observation = {"native_cycles": 64, "executable": "python3"}
    command = (
        "python3 - <<'PY'\nimport json\nfrom pathlib import Path\n"
        "trial = json.loads(Path('trial-catalog.json').read_text())\n"
        "canon = json.loads(Path('harness/scenarios/owner_observation_chaos_v1.json').read_text())\n"
        "for label, obj in [('trial', trial), ('canonical', canon)]:\n"
        "    print(label, 'schema=', obj.get('schema'))\n"
        "    bindings = obj.get('assurance', {}).get('bindings', [])\n"
        "    print(label, 'binding_count=', len(bindings))\n"
        "    print(label, 'invariants=', [b.get('invariant_id') for b in bindings])\nPY"
    )
    if fault == "printed-ids":
        command = command.replace("[b.get('invariant_id') for b in bindings]", "['I-OWNER-CLEANUP', 'I-OWNER-REGISTRY']")
    if fault == "wrong-source":
        command = command.replace("trial-catalog.json", "other-catalog.json")
    count = 1 if fault == "wrong-count" else 2
    ids = ["I-OWNER-CLEANUP"] if fault == "wrong-ids" else ["I-OWNER-CLEANUP", "I-OWNER-REGISTRY"]
    schema = "wrong.catalog.v1" if fault == "wrong-schema" else "trial.catalog.v1"
    output = f"trial schema= {schema}\ntrial binding_count= {count}\ntrial invariants= {ids!r}\n"
    obs_command = (
        "printf '%s' 'prepared'; stat --printf='%n %s\\n' observations.json"
        if fault == "printed-observation" else
        "stat --printf='%n %s\\n' observations.json; sed -n '1,280p' observations.json"
    )
    obs_output = "observations.json 64\n" + json.dumps(observation)
    events = [
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": command,
                "exit_code": 0,
                "aggregated_output": output,
            },
        },
        {
            "type": "item.completed",
            "item": {
                "type": "command_execution",
                "command": obs_command,
                "exit_code": 0,
                "aggregated_output": obs_output,
            },
        },
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is expected


@pytest.mark.parametrize(
    ("fault", "expected"),
    [
        ("none", True),
        ("wrong-id", False),
        ("wrong-case", False),
        ("wrong-probe-count", False),
        ("wrong-environment", False),
        ("duplicate-id", False),
        ("missing-row", False),
        ("wrong-source", False),
        ("printed-value", False),
        ("missing-id-projection", False),
        ("wrong-observation", False),
    ],
)
def test_read_proof_accepts_exact_jq_binding_matrix_and_id_projection(
    fault: str, expected: bool,
) -> None:
    catalog = {"assurance": {"bindings": [
        {"invariant_id": "I-OWNER-REGISTRY", "verification_case_id": "VC-REGISTRY", "probes": [
            {"environment": "process"}, {"environment": "fake"},
        ]},
        {"invariant_id": "I-OWNER-CLEANUP", "verification_case_id": "VC-CLEANUP", "probes": [
            {"environment": "fake"},
        ]},
    ]}}
    observation = {"native_cycles": 64, "executable": "python3"}
    command = (
        "jq -r '.assurance.bindings[] | [.invariant_id, .verification_case_id // \"<missing>\", "
        "(.probes | length | tostring), ([.probes[].environment] | unique | join(\",\"))] | @tsv' "
        "trial-catalog.json && "
        "jq -r '[.assurance.bindings[].invariant_id] | unique[]' trial-catalog.json && "
        "jq -r '.assurance | keys[]' trial-catalog.json"
    )
    lines = [
        "I-OWNER-REGISTRY\tVC-REGISTRY\t2\tfake,process",
        "I-OWNER-CLEANUP\tVC-CLEANUP\t1\tfake",
        "I-OWNER-CLEANUP", "I-OWNER-REGISTRY", "bindings",
    ]
    if fault == "wrong-id":
        lines[2] = "I-OWNER-IDENTITY"
    elif fault == "wrong-case":
        lines[0] = lines[0].replace("VC-REGISTRY", "VC-IDENTITY")
    elif fault == "wrong-probe-count":
        lines[0] = lines[0].replace("\t2\t", "\t1\t")
    elif fault == "wrong-environment":
        lines[0] = lines[0].replace("fake,process", "fake")
    elif fault == "duplicate-id":
        lines[3] = "I-OWNER-CLEANUP"
    elif fault == "missing-row":
        lines.pop(1)
    elif fault == "wrong-source":
        command = command.replace("trial-catalog.json", "other-catalog.json")
    elif fault == "printed-value":
        command += "; printf 'I-OWNER-CLEANUP'"
    elif fault == "missing-id-projection":
        command = command.replace("[.assurance.bindings[].invariant_id] | unique[]", "keys[]")
    obs_output = json.dumps(observation if fault != "wrong-observation" else {"native_cycles": 63})
    events = [
        {"type": "item.completed", "item": {
            "type": "command_execution", "command": command, "exit_code": 0,
            "aggregated_output": "\n".join(lines) + "\n",
        }},
        {"type": "item.completed", "item": {
            "type": "command_execution", "command": "cat observations.json", "exit_code": 0,
            "aggregated_output": obs_output,
        }},
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is expected


@pytest.mark.parametrize(
    "fault",
    ["none", "wrong-id", "wrong-source", "printed", "hardcoded-true", "false-output", "extra-difference"],
)
def test_read_proof_accepts_only_exact_jq_whole_catalog_difference(fault: str) -> None:
    canonical = json.loads((ROOT / "harness/scenarios/owner_observation_chaos_v1.json").read_text())
    catalog = copy.deepcopy(canonical)
    missing = catalog["assurance"]["bindings"].pop(0)["invariant_id"]
    observation = {"native_cycles": 64}
    command = (
        "jq -n --slurpfile canonical harness/scenarios/owner_observation_chaos_v1.json "
        "--slurpfile trial trial-catalog.json "
        "'($canonical[0] | .assurance.bindings |= map(select(.invariant_id != \""
        + missing + "\"))) == $trial[0]'"
    )
    output = "true\n"
    if fault == "wrong-id":
        command = command.replace(missing, "I-NOT-THE-MISSING-ONE")
    elif fault == "wrong-source":
        command = command.replace("owner_observation_chaos_v1.json", "other.json")
    elif fault == "printed":
        command += "; printf 'true\\n'"
    elif fault == "hardcoded-true":
        command = "jq -n 'true' trial-catalog.json"
    elif fault == "false-output":
        output = "false\n"
    elif fault == "extra-difference":
        catalog["execution_boundary"]["external_network"] = True
    events = [
        {"type": "item.completed", "item": {
            "type": "command_execution", "command": command, "exit_code": 0, "aggregated_output": output,
        }},
        {"type": "item.completed", "item": {
            "type": "command_execution", "command": "jq . observations.json", "exit_code": 0,
            "aggregated_output": json.dumps(observation),
        }},
    ]
    assert observed_reads("\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation) is (fault == "none")


@pytest.mark.parametrize(
    "fault",
    ["none", "wrong-missing", "wrong-shared", "wrong-filter", "wrong-source", "printed", "extra-difference"],
)
def test_read_proof_accepts_only_exact_membership_filtered_catalog_comparison(fault: str) -> None:
    canonical = json.loads((ROOT / "harness/scenarios/owner_observation_chaos_v1.json").read_text())
    catalog = copy.deepcopy(canonical)
    omitted = catalog["assurance"]["bindings"].pop(0)
    observation = {"native_cycles": 64, "executable": "python3"}
    trial_by_case = {row["verification_case_id"]: row for row in catalog["assurance"]["bindings"]}
    canonical_by_case = {row["verification_case_id"]: row for row in canonical["assurance"]["bindings"]}
    shared_equal = {case: trial_by_case[case] == canonical_by_case[case] for case in trial_by_case}
    command = """python3 - <<'PY'
import copy, json
from pathlib import Path
trial=json.loads(Path('trial-catalog.json').read_text())
current=json.loads(Path('harness/scenarios/owner_observation_chaos_v1.json').read_text())
tb=trial['assurance']['bindings']
cb=current['assurance']['bindings']
tids={b['verification_case_id'] for b in tb}
cids={b['verification_case_id'] for b in cb}
print('missing_case_ids', sorted(cids-tids))
print('extra_case_ids', sorted(tids-cids))
print('missing_invariants', sorted({b['invariant_id'] for b in cb if b['verification_case_id'] not in tids}))
COMMON_BINDINGS_LINE
filtered=copy.deepcopy(current)
filtered['assurance']['bindings']=[b for b in cb if b['verification_case_id'] in tids]
print('trial_equals_current_minus_missing_binding', trial == filtered)
PY""".replace(
        "COMMON_BINDINGS_LINE",
        "print('common_bindings_equal', {b['verification_case_id']: b == "
        "next(x for x in cb if x['verification_case_id']==b['verification_case_id']) for b in tb})",
    )
    lines = [
        f"missing_case_ids {[omitted['verification_case_id']]!r}",
        "extra_case_ids []",
        f"missing_invariants {[omitted['invariant_id']]!r}",
        f"common_bindings_equal {shared_equal!r}",
        "trial_equals_current_minus_missing_binding True",
    ]
    if fault == "wrong-missing":
        lines[0] = "missing_case_ids []"
    elif fault == "wrong-shared":
        lines[3] = f"common_bindings_equal {{**{shared_equal!r}, {next(iter(shared_equal))!r}: False}}"
    elif fault == "wrong-filter":
        command = command.replace("in tids]", "not in tids]")
    elif fault == "wrong-source":
        command = command.replace("owner_observation_chaos_v1.json", "other.json")
    elif fault == "printed":
        command += "; printf 'trial_equals_current_minus_missing_binding True'"
    elif fault == "extra-difference":
        catalog["execution_boundary"]["external_network"] = True
    events = [
        {"type": "item.completed", "item": {
            "type": "command_execution", "command": command, "exit_code": 0,
            "aggregated_output": "\n".join(lines) + "\n",
        }},
        {"type": "item.completed", "item": {
            "type": "command_execution", "command": "cat observations.json", "exit_code": 0,
            "aggregated_output": json.dumps(observation),
        }},
    ]
    assert observed_reads(
        "\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation
    ) is (fault == "none")


@pytest.mark.parametrize(
    "fault",
    ["none", "wrong-id", "false-equality", "wrong-binding", "wrong-source", "printed", "id-only"],
)
def test_read_proof_accepts_only_exact_named_jq_whole_catalog_comparison(fault: str) -> None:
    canonical = json.loads((ROOT / "harness/scenarios/owner_observation_chaos_v1.json").read_text())
    catalog = copy.deepcopy(canonical)
    omitted = catalog["assurance"]["bindings"].pop(2)
    observation = {"native_cycles": 64, "executable": "python3"}
    environment_counts: dict[str, int] = {}
    for probe in omitted["probes"]:
        environment_counts[probe["environment"]] = environment_counts.get(probe["environment"], 0) + 1
    missing_binding = {
        key: omitted.get(key)
        for key in ("verification_case_id", "invariant_id", "mechanism", "component_id", "sut")
    }
    missing_binding.update(
        {
            "probe_count": len(omitted["probes"]),
            "probe_environments": [
                {"environment": environment, "count": count}
                for environment, count in sorted(environment_counts.items())
            ],
            "negative_controls": omitted.get("negative_controls"),
            "coverage_gaps": omitted.get("coverage_gaps"),
        }
    )
    command = (
        "jq -n --slurpfile trial trial-catalog.json "
        "--slurpfile canonical harness/scenarios/owner_observation_chaos_v1.json "
        f"'($canonical[0] | .assurance.bindings |= map(select(.invariant_id != \"{omitted['invariant_id']}\"))) "
        "as $without_identity | {entire_trial_equals_canonical_without_identity:($trial[0]==$without_identity), "
        "missing_binding:($canonical[0].assurance.bindings[] | select(.invariant_id==\""
        f"{omitted['invariant_id']}\") | {{verification_case_id,invariant_id,mechanism,component_id,sut,"
        "probe_count:(.probes|length),probe_environments:([.probes[].environment]|group_by(.)|"
        "map({environment:.[0],count:length})),negative_controls,coverage_gaps})}'"
    )
    result = {
        "entire_trial_equals_canonical_without_identity": True,
        "missing_binding": missing_binding,
    }
    if fault == "wrong-id":
        command = command.replace(omitted["invariant_id"], "I-NOT-MISSING")
    elif fault == "false-equality":
        result["entire_trial_equals_canonical_without_identity"] = False
    elif fault == "wrong-binding":
        result["missing_binding"] = {**missing_binding, "probe_count": len(omitted["probes"]) - 1}
    elif fault == "wrong-source":
        command = command.replace("owner_observation_chaos_v1.json", "other.json")
    elif fault == "printed":
        command += "; printf 'whole comparison'"
    elif fault == "id-only":
        result["missing_binding"] = {"invariant_id": omitted["invariant_id"]}
    events = [
        {"type": "item.completed", "item": {
            "type": "command_execution", "command": command, "exit_code": 0,
            "aggregated_output": json.dumps(result),
        }},
        {"type": "item.completed", "item": {
            "type": "command_execution", "command": "cat observations.json", "exit_code": 0,
            "aggregated_output": json.dumps(observation),
        }},
    ]
    assert observed_reads(
        "\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation
    ) is (fault == "none")


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "wrong-source",
        "wrong-count",
        "false-equality",
        "wrong-nodeids",
        "missing-dataflow",
        "printed",
        "id-only",
    ],
)
def test_read_proof_accepts_semantic_jq_whole_document_difference(fault: str) -> None:
    canonical = json.loads((ROOT / "harness/scenarios/owner_observation_chaos_v1.json").read_text())
    catalog = copy.deepcopy(canonical)
    omitted = catalog["assurance"]["bindings"].pop(2)
    observation = {"native_cycles": 64, "executable": "python3"}
    command = (
        "jq -n -e --slurpfile trial /qualification/trial-2/trial-catalog.json "
        "--slurpfile current harness/scenarios/owner_observation_chaos_v1.json '"
        "($current[0].assurance.bindings - $trial[0].assurance.bindings) as $missing | "
        "(($current[0] | .assurance.bindings = (.assurance.bindings - $missing)) == $trial[0]) "
        "as $whole_document_equal_after_removal | "
        "{trial_binding_count:($trial[0].assurance.bindings|length),"
        "current_binding_count:($current[0].assurance.bindings|length),"
        "missing_bindings:($missing|map({verification_case_id,invariant_id,mechanism,"
        "probe_count:(.probes|length),nodeids:[.probes[].nodeid]})),"
        "whole_document_equal_after_removal:$whole_document_equal_after_removal,"
        "exactly_one_missing_binding:(($missing|length)==1)}'"
    )
    missing_summary: dict[str, Any] = {
        "verification_case_id": omitted["verification_case_id"],
        "invariant_id": omitted["invariant_id"],
        "mechanism": omitted["mechanism"],
        "probe_count": len(omitted["probes"]),
        "nodeids": [probe["nodeid"] for probe in omitted["probes"]],
    }
    result = {
        "trial_binding_count": len(catalog["assurance"]["bindings"]),
        "current_binding_count": len(canonical["assurance"]["bindings"]),
        "missing_bindings": [missing_summary],
        "whole_document_equal_after_removal": True,
        "exactly_one_missing_binding": True,
    }
    if fault == "wrong-source":
        command = command.replace("owner_observation_chaos_v1.json", "other.json")
    elif fault == "wrong-count":
        result["current_binding_count"] = len(canonical["assurance"]["bindings"]) - 1
    elif fault == "false-equality":
        result["whole_document_equal_after_removal"] = False
    elif fault == "wrong-nodeids":
        result["missing_bindings"] = [{**missing_summary, "nodeids": missing_summary["nodeids"][:-1]}]
    elif fault == "missing-dataflow":
        command = command.replace(
            "$current[0].assurance.bindings - $trial[0].assurance.bindings", "[$current[0].assurance.bindings[2]]"
        )
    elif fault == "printed":
        command += "; printf 'comparison complete'"
    elif fault == "id-only":
        result["missing_bindings"] = [{"invariant_id": omitted["invariant_id"]}]
    events = [
        {"type": "item.completed", "item": {
            "type": "command_execution", "command": command, "exit_code": 0,
            "aggregated_output": json.dumps(result),
        }},
        {"type": "item.completed", "item": {
            "type": "command_execution", "command": "jq . observations.json", "exit_code": 0,
            "aggregated_output": json.dumps(observation),
        }},
    ]
    assert observed_reads(
        "\n".join(json.dumps(event) for event in events), catalog=catalog, observations=observation
    ) is (fault == "none")


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "empty-output",
        "empty-output-wrong-digest",
        "wrong-canonical",
        "wrong-proof",
        "wrong-observations",
        "printed",
        "nonzero",
    ],
)
def test_read_proof_accepts_only_exact_canonical_owner_trial_tool(fault: str) -> None:
    canonical_raw = (ROOT / "harness/scenarios/owner_observation_chaos_v1.json").read_bytes()
    canonical = json.loads(canonical_raw)
    catalog = copy.deepcopy(canonical)
    catalog["assurance"]["bindings"].pop(2)
    observation = {"native_cycles": 64, "executable": "python3"}
    trial_raw = (json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    observation_raw = (json.dumps(observation, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    proof = build_proof(
        trial_raw, catalog, canonical_raw, canonical, observation_raw, observation
    )
    read_proof_sha256 = proof["read_proof_sha256"]
    command = (
        "python tools/verify_owner_trial_read.py "
        "--trial /qualification/workflow/trial-1/trial-catalog.json "
        "--canonical harness/scenarios/owner_observation_chaos_v1.json "
        "--observations /qualification/workflow/trial-1/observations.json"
    )
    exit_code = 0
    if fault == "wrong-canonical":
        command = command.replace("owner_observation_chaos_v1.json", "other.json")
    elif fault == "wrong-proof":
        proof = {**proof, "trial_sha256": "0" * 64}
    elif fault == "wrong-observations":
        command = command.replace("observations.json", "other.json")
    elif fault == "printed":
        command += "; printf 'prepared proof'"
    elif fault == "nonzero":
        exit_code = 1
    elif fault == "empty-output-wrong-digest":
        read_proof_sha256 = "0" * 64
    events = [{"type": "item.completed", "item": {
        "type": "command_execution",
        "command": command,
        "exit_code": exit_code,
        "aggregated_output": "" if fault.startswith("empty-output") else json.dumps(proof, ensure_ascii=False),
    }}]
    assert observed_reads(
        "\n".join(json.dumps(event) for event in events),
        catalog=catalog,
        observations=observation,
        read_proof_sha256=read_proof_sha256,
    ) is (fault in {"none", "empty-output"})
