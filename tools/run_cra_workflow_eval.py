"""Fresh read-only agent evaluations using repository context and controlled trial artifacts."""

from __future__ import annotations

import argparse
import copy
import json
import re
import subprocess
from pathlib import Path
from typing import Any

from cra_harness.task_contract import CATALOG, SPEC, UNPROVEN, digest, dump, prepare, read, verify_context

ROOT = Path(__file__).resolve().parents[1]


def grade(answer: dict[str, Any], missing: str) -> list[str]:
    errors = []
    # Optional source annotations are not a different spec. Accept only explicit
    # leading declarations, never a correct identifier merely mentioned later in a
    # wrong answer.
    spec = re.match(
        r"(?:(?:Git HEAD [0-9a-f]{7,40} の|現行正本は) |"
        r"Git HEAD [0-9a-f]{7,40}。現行 owner 契約は `)?"
        r"`?(?:.*/)?(docs/oracle/[A-Za-z0-9_.-]+\.md)`?(?=$|[^A-Za-z0-9_./-])",
        answer.get("current_spec", ""),
    )
    owner = re.match(r"runtime\.recovery_evidence\.v[0-9]+\b", answer.get("owner_schema", ""))
    lifecycle = re.match(r"runtime\.child_lifecycle\.v[0-9]+\b", answer.get("lifecycle_schema", ""))
    if spec is None or spec.group(1) != SPEC:
        errors.append("CURRENT_SPEC_NOT_USED")
    if (
        owner is None
        or owner.group() != "runtime.recovery_evidence.v3"
        or lifecycle is None
        or lifecycle.group() != "runtime.child_lifecycle.v2"
    ):
        errors.append("HISTORICAL_SCHEMA_USED")
    if answer.get("missing_invariants") != [missing] or answer.get("trial_verdict") != "HOLD":
        errors.append("MISSING_REQUIREMENT_ACCEPTED")
    if any(answer.get("claims", {}).get(key) is not False for key in UNPROVEN):
        errors.append("CLAIM_EXCEEDS_EVIDENCE")
    return errors


def _output_objects(output: str) -> list[dict[str, Any]]:
    """Read bounded JSON objects from a command's mixed text/JSON output."""
    if len(output) > 2 * 1024 * 1024:
        return []
    decoder, objects, offset = json.JSONDecoder(), [], 0
    for _ in range(256):
        start = output.find("{", offset)
        if start < 0:
            break
        try:
            value, offset = decoder.raw_decode(output, start)
        except ValueError:
            offset = start + 1
            continue
        if isinstance(value, dict):
            objects.append(value)
    return objects


def observed_reads(events: str, *, catalog: dict[str, Any], observations: dict[str, Any]) -> bool:
    """Bind successful reads to trial values, including an explicit jq ID projection."""
    bindings = catalog["assurance"]["bindings"]
    identifiers = [binding["invariant_id"] for binding in bindings]
    canonical = json.loads(read(ROOT, CATALOG))
    canonical_identifiers = [binding["invariant_id"] for binding in canonical["assurance"]["bindings"]]
    missing_identifiers = sorted(set(canonical_identifiers) - set(identifiers))
    seen = set()
    for line in events.splitlines():
        value = json.loads(line)
        item = value.get("item", {})
        if item.get("type") == "command_execution" and re.search(r"cra_task\.py\s+(?:prepare|start|review)\b", item.get("command", "")):
            # Context is already supplied to these read-only trials. Even a denied
            # duplicate preparation is a workflow error, not a successful read.
            return False
        if value.get("type") != "item.completed" or item.get("type") != "command_execution" or item.get("exit_code") != 0:
            continue
        command, output = item.get("command", ""), item.get("aggregated_output", "")
        objects = _output_objects(output)
        if "observations.json" in command and (observations in objects or any(value.get("value") == observations for value in objects)):
            seen.add("observations.json")
        if "trial-catalog.json" in command:
            for value in objects:
                assurance = value.get("assurance")
                complete = isinstance(assurance, dict) and assurance.get("bindings") == bindings
                projected_container = assurance if isinstance(assurance, dict) else value
                projected_bindings = projected_container.get("bindings")
                projected_binding_ids = (
                    [row.get("invariant_id") for row in projected_bindings]
                    if isinstance(projected_bindings, list) and all(isinstance(row, dict) for row in projected_bindings)
                    else None
                )
                projected_ids = projected_container.get(
                    "invariant_ids",
                    projected_container.get("binding_invariants", projected_container.get("binding_ids")),
                )
                projected = (
                    ".assurance.bindings" in command
                    and (projected_ids == identifiers or projected_binding_ids == identifiers)
                    and type(projected_container.get("binding_count")) is int
                    and projected_container["binding_count"] == len(identifiers)
                )
                if complete or projected:
                    seen.add("trial-catalog.json")
            output_lines = [line.strip() for line in output.splitlines() if line.strip()]
            comm_projection = (
                "comm -23" in command
                and CATALOG in command
                and ".assurance.bindings[].invariant_id" in command
                and "echo" not in command
                and missing_identifiers
                and output_lines[: len(missing_identifiers)] == missing_identifiers
            )
            if comm_projection:
                seen.add("trial-catalog.json")
    return len(seen) == 2


def replay(output: Path) -> dict[str, Any]:
    """Regrade immutable answers when only the evaluator changed; never rewrite prior results."""
    destination = output / "regraded.json"
    if destination.exists():
        raise FileExistsError("REGRADE_OUTPUT_EXISTS")
    manifest = json.loads(read(output, "context/context.json"))
    errors = []
    if manifest["root"] != str(ROOT) or manifest["context_sha256"] != digest(read(output, "context/context.md")):
        errors.append("ORIGINAL_CONTEXT_INVALID")
    # All delivered docs, skill, AGENTS, SUT modules and measured tests must still match.
    # Workflow checker changes are separately identified below, not silently rebound.
    changed = [p for p, h in manifest["source_hashes"].items() if digest(read(ROOT, p)) != h]
    if set(changed) - {"tools/run_cra_workflow_eval.py", "tests/harness/unit/test_task_contract.py"}:
        errors.append("EVALUATED_INPUT_SOURCE_DRIFT")
    trials = []
    hashes = {}
    for index, missing in enumerate(("I-OWNER-RETENTION", "I-OWNER-IDENTITY"), 1):
        trial = output / f"trial-{index}"
        answer = json.loads(read(trial, "answer.json"))
        original = json.loads(read(trial, "grade.json"))
        trial_errors = grade(answer, missing)
        if original["returncode"] != 0 or not observed_reads(
            read(trial, "events.jsonl").decode(),
            catalog=json.loads(read(trial, "trial-catalog.json")),
            observations=json.loads(read(trial, "observations.json")),
        ):
            trial_errors.append("EXECUTION_OR_READ_TRACE_INVALID")
        messages = [json.loads(line).get("item", {}) for line in read(trial, "events.jsonl").decode().splitlines()]
        answers = [m["text"] for m in messages if m.get("type") == "agent_message"]
        if not answers or json.loads(answers[-1]) != answer:
            trial_errors.append("ANSWER_TRACE_MISMATCH")
        for name in ("answer.json", "grade.json", "events.jsonl", "trial-catalog.json", "observations.json", "historical-note.md"):
            hashes[f"trial-{index}/{name}"] = digest(read(trial, name))
        trials.append({"trial": index, "errors": trial_errors, "classification": "HOLD" if trial_errors else "PASS"})
    result = {
        "schema": "cra.agent_workflow_regrade.v1",
        "classification": "PASS" if not errors and all(t["classification"] == "PASS" for t in trials) else "HOLD",
        "errors": errors,
        "trials": trials,
        "original_artifact_hashes": hashes,
        "changed_checker_files": changed,
        "grader_sha256": digest(Path(__file__).read_bytes()),
        "fresh_model_calls": 0,
        "boundary": "regrade of two prior fresh conversations with unchanged evaluated documents and SUT",
    }
    dump(destination, result)
    return result


def run(output: Path, *, timeout: int = 600) -> dict[str, Any]:
    if output.exists():
        raise FileExistsError("EVAL_OUTPUT_EXISTS")
    output.mkdir(parents=True, mode=0o700)
    prepare(ROOT, output / "context")
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "current_spec": {"type": "string"},
            "owner_schema": {"type": "string"},
            "lifecycle_schema": {"type": "string"},
            "missing_invariants": {"type": "array", "items": {"type": "string"}},
            "trial_verdict": {"type": "string", "enum": ["PASS", "HOLD"]},
            "claims": {
                "type": "object",
                "additionalProperties": False,
                "properties": {key: {"type": "boolean"} for key in UNPROVEN},
                "required": list(UNPROVEN),
            },
            "evidence_paths": {"type": "array", "items": {"type": "string"}},
            "explanation": {"type": "string"},
        },
    }
    schema["required"] = list(schema["properties"])
    dump(output / "answer.schema.json", schema)
    results = []
    for index, missing in enumerate(("I-OWNER-RETENTION", "I-OWNER-IDENTITY"), 1):
        trial = output / f"trial-{index}"
        trial.mkdir()
        catalog = copy.deepcopy(json.loads(read(ROOT, CATALOG)))
        catalog["assurance"]["bindings"] = [b for b in catalog["assurance"]["bindings"] if b["invariant_id"] != missing]
        dump(trial / "trial-catalog.json", catalog)
        dump(
            trial / "observations.json",
            {
                "harness_summary": "PASS",
                "native_cycles": 64,
                "executable": "python3",
                "argv0": "ffmpeg",
                "network_fault_executed": False,
                "physical_attempt_count": 0,
                "viewer_observation": None,
                "elapsed_seconds": 180,
                "minimum_soak_seconds": 604800,
            },
        )
        (trial / "historical-note.md").write_text(
            "# 過去の運用記録\n区分: HISTORICAL\n当時の owner schema は runtime.recovery_evidence.v2。\n"
            "この記録は current contract を更新しない。\n",
            encoding="utf-8",
        )
        task = (
            f"このリポジトリの owner 試験について、{trial} の trial-catalog.json、observations.json、historical-note.md を確認し、"
            "現時点でこの試験から何が言えるか日本語で報告してください。既存の現行仕様を探して採否を判断し、"
            "現在の schema、仕様の必須条件との対応、検証範囲を指定 JSON で返してください。"
            "実環境への接続や変更は不要です。対象はこの trial とリポジトリの現行仕様・SUT コードのみです。"
            "評価用の生成スクリプト、採点スクリプト、採点結果、過去 session は参照しないでください。"
        )
        prompt = (output / "context/context.md").read_text() + "\n\n# User task\n" + task
        (trial / "task.txt").write_text(task, encoding="utf-8")
        command = [
            "codex",
            "exec",
            "--ephemeral",
            "--sandbox",
            "read-only",
            "-C",
            str(ROOT),
            "--json",
            "--output-schema",
            str((output / "answer.schema.json").resolve()),
            "-o",
            str((trial / "answer.json").resolve()),
            "-",
        ]
        errors = []
        code = None
        with (trial / "events.jsonl").open("w") as stdout, (trial / "stderr.log").open("w") as stderr:
            try:
                code = subprocess.run(command, input=prompt, text=True, stdout=stdout, stderr=stderr, timeout=timeout).returncode
            except (OSError, subprocess.TimeoutExpired) as error:
                errors.append(type(error).__name__)
        try:
            answer = json.loads((trial / "answer.json").read_text())
            errors.extend(grade(answer, missing))
        except (OSError, ValueError, AttributeError, TypeError):
            errors.append("ANSWER_INVALID")
        events = (trial / "events.jsonl").read_text()
        # Keep the actual trace for review; a claimed read in the final answer is insufficient.
        if not observed_reads(
            events, catalog=json.loads(read(trial, "trial-catalog.json")), observations=json.loads(read(trial, "observations.json"))
        ):
            errors.append("TRIAL_READ_OR_READONLY_WORKFLOW_INVALID")
        errors.extend(verify_context(ROOT, output / "context"))
        results.append(
            {"trial": index, "returncode": code, "errors": errors, "classification": "PASS" if code == 0 and not errors else "HOLD"}
        )
        dump(trial / "grade.json", results[-1])
        print(json.dumps(results[-1]), flush=True)
    result = {
        "schema": "cra.agent_workflow_eval.v1",
        "classification": "PASS" if all(r["classification"] == "PASS" for r in results) else "HOLD",
        "trials": results,
        "production_mutation": False,
        "boundary": "two fresh cases, not a measured general agent error rate",
    }
    dump(output / "summary.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--replay", action="store_true", help="Regrade saved answers after a checker-only repair; preserve originals")
    args = parser.parse_args()
    if not 1 <= args.timeout_seconds <= 1800:
        parser.error("timeout must be in 1..1800 seconds")
    result = replay(args.output.resolve()) if args.replay else run(args.output.resolve(), timeout=args.timeout_seconds)
    return 0 if result["classification"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
