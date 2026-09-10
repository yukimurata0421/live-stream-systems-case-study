from __future__ import annotations

import json
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest

from cra_harness import task_contract as gate
from tools.run_cra_workflow_eval import grade, observed_reads

ROOT = Path(__file__).resolve().parents[3]


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
        final = next(binding for binding in bindings if binding["invariant_id"] == "I-OWNER-FINAL")
        final["coverage_gaps"] = []
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
    assert (project / gate.SPEC).read_text() in (output / "context.md").read_text()
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
        assert f"| {key} | 未検証" in text
    del value["contract_gate"]
    assert "総合判定: HOLD" in gate.report(value)


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "annotation",
        "git-head-prefix",
        "git-head-sentence-prefix",
        "current-spec-prefix",
        "markdown-current-spec",
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
            answer[key] += "（現行。trial の準拠は未確認）"  # type: ignore[operator]
    elif fault == "git-head-prefix":
        answer["current_spec"] = "Git HEAD 8ac2fbdc3c59142d195647ffc56215db64f6d1cb の " + gate.SPEC
    elif fault == "git-head-sentence-prefix":
        answer["current_spec"] = "Git HEAD 8ac2fbdc3c59142d195647ffc56215db64f6d1cb。現行 owner 契約は `" + gate.SPEC + "`。"
    elif fault == "current-spec-prefix":
        answer["current_spec"] = "現行正本は " + gate.SPEC + "（sha256: example）。"
    elif fault == "markdown-current-spec":
        answer["current_spec"] = f"`{gate.SPEC}` の現行契約。"
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
            "current-spec-prefix",
            "markdown-current-spec",
        }
    )


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "projection",
        "binding-invariants-projection",
        "binding-ids-projection",
        "bindings-projection",
        "nested-bindings-projection",
        "nested-observation-projection",
        "self-report",
        "failed-read",
        "only-hashes",
        "partial",
        "redundant-context",
        "wrong-ids",
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
        "binding-invariants-projection",
        "binding-ids-projection",
        "wrong-ids",
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
                else "jq '{invariant_ids:[.assurance.bindings[].invariant_id],binding_count:(.assurance.bindings|length)}' trial-catalog.json; cat observations.json"  # noqa: E501
            )
        )
        key = (
            "binding_invariants"
            if fault in {"binding-invariants-projection", "wrong-binding-invariants"}
            else "binding_ids"
            if fault == "binding-ids-projection"
            else "invariant_ids"
        )
        projected = {key: ["I-OWNER-CLEANUP"], "binding_count": 1}
        if fault == "wrong-ids":
            projected["invariant_ids"] = ["I-OWNER-IDENTITY"]
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
    elif fault == "self-report":
        item["type"] = "agent_message"
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
            "binding-invariants-projection",
            "binding-ids-projection",
            "bindings-projection",
            "nested-bindings-projection",
            "nested-observation-projection",
        }
    )


def test_read_proof_accepts_exact_canonical_trial_comm_projection() -> None:
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
