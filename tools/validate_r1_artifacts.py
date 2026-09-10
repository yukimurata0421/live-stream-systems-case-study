#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

PRIVATE_ADDRESS = re.compile(
    r"(?<![\d.])(?:10(?:\.\d{1,3}){3}|172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2}|192\.168(?:\.\d{1,3}){2})(?![\d.])"
)
SECRET_ASSIGNMENT = re.compile(r"(?i)(?:password|token|secret|private[_-]?key)=[^\s\"']+")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    project = args.project_root.resolve()
    artifact = args.artifact_root.resolve()
    journal = artifact / "commands.jsonl"
    rows = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines() if line.strip()]
    missing_host = [row["command_id"] for row in rows if not row.get("host")]
    hash_mismatch: list[str] = []
    missing_output: list[str] = []
    for row in rows:
        raw = Path(str(row["output_artifact"]))
        output = raw if raw.is_absolute() else project / raw
        if not output.is_file():
            missing_output.append(row["command_id"])
        elif digest(output) != row["output_sha256"]:
            hash_mismatch.append(row["command_id"])

    private_address_files: list[str] = []
    secret_assignment_files: list[str] = []
    for path in sorted(value for value in artifact.rglob("*") if value.is_file()):
        try:
            value = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        relative = str(path.relative_to(artifact))
        if PRIVATE_ADDRESS.search(value):
            private_address_files.append(relative)
        if SECRET_ASSIGNMENT.search(value):
            secret_assignment_files.append(relative)

    required = [
        "records/terminal_summary.json",
        "records/final_state_matrix.json",
        "records/test_results.json",
        "records/decision_journal.jsonl",
        "harness/r1_mp03_gate_terminal.json",
        "harness/mp03_performance.json",
        "live/terminal_runtime_baseline_v2.json",
        "live/terminal_runtime_end.json",
        "release/mp03-r1-audit-p2-disabled-20260824t0831jst-v3/candidate_manifest.json",
        "end/stream_recovery_control_change_ledger.json",
        "end/stream_v3_change_ledger.json",
        "end/stream_v4_change_ledger.json",
    ]
    missing_required = [item for item in required if not (artifact / item).is_file()]
    payload: dict[str, Any] = {
        "schema_version": "recovery_control.r1_artifact_validation.v1",
        "command_count": len(rows),
        "missing_command_host": missing_host,
        "missing_command_output": missing_output,
        "command_output_hash_mismatch": hash_mismatch,
        "private_address_files": private_address_files,
        "secret_assignment_files": secret_assignment_files,
        "missing_required_artifacts": missing_required,
    }
    payload["pass"] = not any(
        payload[key]
        for key in (
            "missing_command_host",
            "missing_command_output",
            "command_output_hash_mismatch",
            "private_address_files",
            "secret_assignment_files",
            "missing_required_artifacts",
        )
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
