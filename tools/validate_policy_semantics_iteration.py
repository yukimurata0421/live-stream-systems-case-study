#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

LINK_PATTERN = re.compile(r"\[[^]]+\]\(([^)]+)\)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    json_files = (
        args.artifact / "policy/reason_action_matrix.json",
        args.artifact / "preflight/option_bd_terminal.json",
        args.artifact / "artifact_lineage.json",
        args.artifact / "harness/policy_differential_terminal_v3.json",
        args.artifact / "harness/reconcile_ffmpeg_terminal_v2.json",
        args.artifact / "harness/runtime_boundary_regression_terminal_v2.json/manifest.json",
        args.artifact / "performance/runtime_boundary_benchmark.json",
        args.artifact / "end/stream_recovery_control_change_ledger.json",
        args.artifact / "end/stream_v3_change_ledger.json",
        args.artifact / "end/stream_v4_change_ledger.json",
        args.artifact / "release/runtime-boundary-20260825T0712JST-v10/release_manifest.json",
        args.artifact / "release/runtime-boundary-20260825T0712JST-v10/release_compatibility_terminal.json",
        args.artifact / "release/runtime-boundary-20260825T0712JST-v10/image_verification.json",
    )
    json_valid = True
    for path in json_files:
        json.loads(path.read_text(encoding="utf-8"))
    for line in (args.artifact / "decision_journal.jsonl").read_text(encoding="utf-8").splitlines():
        if line.strip():
            json.loads(line)

    required_artifacts = (
        args.artifact / "terminal_report.md",
        args.artifact / "commands.jsonl",
        args.artifact / "live/shadow_before.json",
        args.artifact / "live/shadow_after.json",
        *json_files,
    )
    missing_artifacts = [str(path.relative_to(args.artifact)) for path in required_artifacts if not path.is_file()]

    documents = (
        "docs/engineering/records/2026-08-25_46_fast_recovery_policy_semantics.md",
        "docs/engineering/records/2026-08-25_47_network_down_failure_domain.md",
        "docs/engineering/records/2026-08-25_48_option_bd_migration_continuation.md",
        "docs/engineering/records/2026-08-25_49_recovery_control_plane_evolution_and_evidence_history.md",
        "docs/engineering/decisions/2026-08-25_05_typed_recovery_intents.md",
        "docs/engineering/decisions/2026-08-25_06_runtime_identity.md",
        "docs/implementation/2026-08-25_14_reconcile_ffmpeg.md",
        "docs/harness/2026-08-25_fast_recovery_policy_differential.md",
        "docs/live/2026-08-25_typed_policy_controller_shadow.md",
        "docs/runbooks/2026-08-25_option_bd_typed_policy_next_approval.md",
    )
    broken: list[dict[str, str]] = []
    for relative in documents:
        document = args.repo / relative
        for target in LINK_PATTERN.findall(document.read_text(encoding="utf-8")):
            if target.startswith(("http://", "https://", "#")):
                continue
            resolved = (document.parent / target.split("#", 1)[0]).resolve()
            if not resolved.exists():
                broken.append({"document": relative, "target": target})

    result = {
        "schema_version": "policy_semantics.artifact_validation.v1",
        "json_valid": json_valid,
        "document_count": len(documents),
        "broken_relative_links": broken,
        "required_artifact_count": len(required_artifacts),
        "missing_artifacts": missing_artifacts,
        "pass": json_valid and not broken and not missing_artifacts,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
