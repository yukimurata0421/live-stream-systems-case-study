"""Prepare verified repository context or start a new bounded read-only task."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from cra_harness import evidence_lineage
from cra_harness.task_contract import REVIEW, dump, prepare, review_status, source_hashes, verify_context
from cra_harness.traceability import build_task_view, check_traceability, render_task_view, render_traceability
from cra_harness.verification_gate import dump_owner_gate, evaluate_owner_gate, render_owner_gate

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("prepare", "check", "review", "start", "check-traceability", "verification-gate", "task-view", "evidence-map"),
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--task-file", type=Path)
    parser.add_argument("--reason")
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--result", type=Path, help="source-bound owner harness result directory")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--changed-file", action="append", default=[])
    parser.add_argument("--changed-symbol", action="append", default=[])
    parser.add_argument("--component")
    parser.add_argument("--verification-case")
    parser.add_argument("--diff-ref")
    parser.add_argument("--layer")
    parser.add_argument("--node")
    parser.add_argument("--restart-domain")
    parser.add_argument("--snapshot", type=Path, help="explicit read-only live overlay; this command performs no collection")
    args = parser.parse_args()
    if args.command == "review":
        if not args.reason or len(args.reason.strip()) < 20:
            parser.error("review requires a substantive --reason after checking spec against source")
        path = ROOT / REVIEW
        path.parent.mkdir(parents=True, exist_ok=True)
        dump(
            path,
            {
                "schema": "cra.owner_contract_review.v1",
                "reviewed_at": datetime.now(UTC).isoformat(),
                "reason": args.reason,
                "source_hashes": source_hashes(ROOT),
            },
        )
        print("REVIEW_RECORDED: source/doc hashes; not proof of semantic correctness")
        return 0
    if args.command == "check":
        state = review_status(ROOT)
        print(state)
        return 0 if state["classification"] == "PASS" else 1
    if args.command == "check-traceability":
        state = check_traceability(ROOT, result_directory=args.result)
        print(json.dumps(state, ensure_ascii=False, indent=2) if args.format == "json" else render_traceability(state), end="")
        return 0 if state["status"] == "PASS" else 1
    if args.command == "verification-gate":
        state = evaluate_owner_gate(ROOT, result_directory=args.result)
        print(dump_owner_gate(state) if args.format == "json" else render_owner_gate(state), end="")
        return int(state["exit_code"])
    if args.command == "task-view":
        view = build_task_view(
            ROOT,
            files=args.changed_file,
            symbols=args.changed_symbol,
            component=args.component,
            verification_case=args.verification_case,
            diff_ref=args.diff_ref,
            result_directory=args.result,
        )
        print(json.dumps(view, ensure_ascii=False, indent=2) if args.format == "json" else render_task_view(view), end="")
        return 0 if view["status"] == "PASS" else 1
    if args.command == "evidence-map":
        state = evidence_lineage.check_map(ROOT)
        if state["status"] == "PASS" and any(item is not None for item in (args.layer, args.node, args.restart_domain)):
            try:
                state = evidence_lineage.build_view(
                    ROOT,
                    layer=args.layer,
                    node=args.node,
                    restart_domain=args.restart_domain,
                )
            except ValueError as error:
                state = {
                    "schema": "cra.evidence_lineage_view.v1",
                    "status": "HOLD",
                    "errors": [str(error)],
                }
        if args.snapshot is not None and state["status"] == "PASS":
            try:
                overlay = evidence_lineage.load_live_overlay(ROOT, args.snapshot)
                overlay_errors = evidence_lineage.validate_live_overlay(ROOT, overlay)
            except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
                overlay_errors = [f"OVERLAY_LOAD:{type(error).__name__}"]
            state["overlay"] = {"status": "PASS" if not overlay_errors else "HOLD", "errors": overlay_errors}
            if overlay_errors:
                state["status"] = "HOLD"
        if args.format == "json":
            print(json.dumps(state, ensure_ascii=False, indent=2))
        elif state.get("schema") == "cra.evidence_lineage_view.v1" and "nodes" in state:
            print(evidence_lineage.render_view(state), end="")
        else:
            print(evidence_lineage.render_check(state), end="")
            if isinstance(state.get("overlay"), dict):
                print(f"live overlay: {state['overlay']['status']}")
                for error in state["overlay"]["errors"]:
                    print(f"- `{error}`")
        return 0 if state["status"] == "PASS" else 1
    if args.output is None:
        parser.error("--output required")
    if args.command == "start" and (args.task_file is None or not 1 <= args.timeout_seconds <= 1800):
        parser.error("start requires --task-file and timeout in 1..1800 seconds")
    manifest = prepare(ROOT, args.output)
    print(f"Context: {args.output / 'context.md'}; review={manifest['review']['classification']}", flush=True)
    if args.command == "prepare":
        return 0
    if verify_context(ROOT, args.output):
        raise ValueError("TASK_CONTEXT_DRIFT")
    prompt = (args.output / "context.md").read_text() + "\n\n# User task\n" + args.task_file.read_text()
    command = [
        "codex",
        "exec",
        "--ephemeral",
        "--sandbox",
        "read-only",
        "-C",
        str(ROOT),
        "--json",
        "-o",
        str((args.output / "answer.md").resolve()),
        "-",
    ]
    with (args.output / "events.jsonl").open("w") as stdout, (args.output / "stderr.log").open("w") as stderr:
        run = subprocess.run(command, input=prompt, text=True, stdout=stdout, stderr=stderr, timeout=args.timeout_seconds)
    errors = verify_context(ROOT, args.output)
    dump(args.output / "launch.json", {"returncode": run.returncode, "context_errors": errors, "sandbox": "read-only"})
    return 0 if run.returncode == 0 and not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
