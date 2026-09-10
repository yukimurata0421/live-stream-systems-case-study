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
SECRET_ASSIGNMENT = re.compile(r"(?i)(?:password|token|secret|private[_-]?key)=(?!REDACTED\b)[^\s\"']+")


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
    rows = [json.loads(line) for line in (artifact / "commands.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    missing_command_output: list[str] = []
    command_hash_mismatch: list[str] = []
    for row in rows:
        raw = Path(str(row["output_artifact"]))
        output = raw if raw.is_absolute() else project / raw
        if not output.is_file():
            missing_command_output.append(str(row["command_id"]))
        elif digest(output) != row["output_sha256"]:
            command_hash_mismatch.append(str(row["command_id"]))

    private_address_files: list[str] = []
    secret_assignment_files: list[str] = []
    for path in sorted(value for value in artifact.rglob("*") if value.is_file()):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        relative = str(path.relative_to(artifact))
        if PRIVATE_ADDRESS.search(text):
            private_address_files.append(relative)
        if SECRET_ASSIGNMENT.search(text):
            secret_assignment_files.append(relative)

    required = [
        "report.md",
        "decision_journal.jsonl",
        "records/terminal_summary.json",
        "records/final_state_matrix.json",
        "records/test_results.json",
        "harness/summary.json",
        "harness/mp03_gate.json",
        "harness/audit_p2_performance.json",
        "harness/snapshot_projection_performance.json",
        "live/production_baseline.json",
        "live/pre_projection_baseline.json",
        "live/production_terminal.json",
        "live/snapshot_projection_terminal.json",
        "live/identity_comparison.json",
        "release_boundary/release_dependency_graph.json",
        "release_boundary/blast_radius_graph.json",
        "release_boundary/stage_c_bad_gate_result.json",
        "release_boundary/report_hotfix_gate_result.json",
        "release_boundary/mp03_current_topology_gate_result.json",
        "release_boundary/mp10_current_topology_gate_result.json",
        "release/mp03-r2-projection-p2-disabled-20260824t2040jst-v1/candidate_manifest.json",
        "release/mp10-projection-p2-disabled-20260824t2059jst-v1/candidate_manifest.json",
        "end/stream_recovery_control_change_ledger.json",
        "end/stream_v3_change_ledger.json",
        "end/stream_v4_change_ledger.json",
    ]
    missing_required = [relative for relative in required if not (artifact / relative).is_file()]

    candidate_mismatches: list[str] = []
    for relative in (
        "release/mp03-r2-projection-p2-disabled-20260824t2040jst-v1/candidate_manifest.json",
        "release/mp10-projection-p2-disabled-20260824t2059jst-v1/candidate_manifest.json",
    ):
        manifest_path = artifact / relative
        if not manifest_path.is_file():
            continue
        manifest: dict[str, Any] = json.loads(manifest_path.read_text(encoding="utf-8"))
        for source_relative, identity in manifest["source_files"].items():
            overlay = manifest_path.parent / "overlay/app" / source_relative
            if not overlay.is_file() or digest(overlay) != identity["sha256"]:
                candidate_mismatches.append(f"{manifest['release_id']}:{source_relative}:overlay")
            source = Path(identity["source"])
            if not source.is_file() or digest(source) != identity["sha256"]:
                candidate_mismatches.append(f"{manifest['release_id']}:{source_relative}:source")

    payload = {
        "schema_version": "recovery_control.release_boundary_artifact_validation.v1",
        "command_count": len(rows),
        "missing_command_output": missing_command_output,
        "command_hash_mismatch": command_hash_mismatch,
        "private_address_files": private_address_files,
        "secret_assignment_files": secret_assignment_files,
        "missing_required": missing_required,
        "candidate_source_hash_mismatch": candidate_mismatches,
    }
    payload["pass"] = not any(
        payload[key]
        for key in (
            "missing_command_output",
            "command_hash_mismatch",
            "private_address_files",
            "secret_assignment_files",
            "missing_required",
            "candidate_source_hash_mismatch",
        )
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0 if payload["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
