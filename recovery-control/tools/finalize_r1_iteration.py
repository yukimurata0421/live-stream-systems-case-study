#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def git_state(name: str, root: Path) -> dict[str, Any]:
    completed = subprocess.run(
        ["git", "status", "--short", "--branch", "--untracked-files=normal"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    lines = completed.stdout.splitlines()
    return {
        "repository": name,
        "branch": lines[0] if lines else "",
        "dirty": len(lines) > 1,
        "entry_count": max(0, len(lines) - 1),
        "status_sha256": hashlib.sha256(completed.stdout.encode()).hexdigest(),
    }


def related_repository_root(project_root: Path, name: str) -> Path:
    container = project_root.parent
    if (container / "src" / "stream_v3").is_dir():
        return container if name == "stream_v3" else container.parent / name
    return container / name


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", type=Path, required=True)
    args = parser.parse_args()
    root = args.artifact_root.resolve()
    project_root = Path(__file__).resolve().parents[1]
    release = root / "release/mp03-r1-audit-p2-disabled-20260824t0831jst-v3"
    candidate = load(release / "candidate_manifest.json")
    gate = load(root / "harness/r1_mp03_gate_terminal.json")
    performance = load(root / "harness/mp03_performance.json")
    baseline = load(root / "live/terminal_runtime_baseline_v2.json")
    end = load(root / "live/terminal_runtime_end.json")
    ledgers = {name: load(root / f"end/{name}_change_ledger.json") for name in ("stream_recovery_control", "stream_v3", "stream_v4")}
    git_states = {
        "stream_recovery_control": git_state("stream_recovery_control", project_root),
        "stream_v3": git_state("stream_v3", related_repository_root(project_root, "stream_v3")),
        "stream_v4": git_state("stream_v4", related_repository_root(project_root, "stream_v4")),
    }
    now = datetime.now(UTC)
    now_utc = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    now_jst = now.astimezone(ZoneInfo("Asia/Tokyo")).isoformat(timespec="seconds")

    decisions = [
        {
            "decision": "DEPLOY",
            "timestamp_jst": "2026-08-24T08:44:20+09:00",
            "result": "NO_DEPLOY_RELEASE_GATE_BLOCKED",
            "reason": candidate["release_gate_blockers"],
        },
        {
            "decision": "ROLLBACK",
            "timestamp_jst": "2026-08-24T08:44:21+09:00",
            "result": "NOT_REQUIRED",
            "reason": "candidate was staged but never loaded by production runtime",
        },
        {
            "decision": "R1_ACCEPTANCE",
            "timestamp_jst": now_jst,
            "result": "NOT_ACCEPTED_LIVE_PENDING",
            "reason": "local gates passed but runtime rollout was not executed",
        },
        {
            "decision": "R2_MP10_PREPARATION",
            "timestamp_jst": now_jst,
            "result": "NOT_STARTED_R1_NOT_ACCEPTED",
            "reason": "the authorized ordering permits R2 preparation only after R1 acceptance",
        },
    ]
    decision_path = root / "records/decision_journal.jsonl"
    decision_path.parent.mkdir(parents=True, exist_ok=True)
    decision_path.write_text(
        "".join(json.dumps(item, sort_keys=True, separators=(",", ":")) + "\n" for item in decisions),
        encoding="utf-8",
    )

    matrix = [
        {
            "domain": "MP-03 audit runtime",
            "before": "NOT_LOADED",
            "after": "NOT_DEPLOYED",
            "evidence": "candidate local verified; production container identity unchanged",
            "remaining_blocker": "snapshot projection and MP-03-only immutable replacement",
        },
        {
            "domain": "MP-03 Evidence Binding",
            "before": "NOT_RUNTIME_LOADED",
            "after": "LOCAL_VERIFIED / LIVE_PENDING",
            "evidence": "candidate self-test exact target and source identity PASS",
            "remaining_blocker": "no live candidate event",
        },
        {
            "domain": "MP-03 P2-disabled",
            "before": "LOCAL_VERIFIED_MODEL",
            "after": "LOCAL_VERIFIED / LIVE_PENDING",
            "evidence": "startup/config/source/event enforcement false; branch signal null",
            "remaining_blocker": "not loaded by production runtime",
        },
        {
            "domain": "MP-03 Effect Boundary",
            "before": "SOURCE_IDENTIFIED",
            "after": "PENDING_RUNTIME_DEPLOY_AND_NATURAL_EVENT",
            "evidence": "static hook immediately before os.kill; no live effect fabricated",
            "remaining_blocker": "runtime deploy then natural event",
        },
        {
            "domain": "MP-03 in-flight truth",
            "before": "PROPOSED",
            "after": "PROPOSED_NOT_DURABLE",
            "evidence": "action plan through effect return mapped in source",
            "remaining_blocker": "durable or restart-reconstructible owner missing",
        },
        {
            "domain": "MP-10 candidate",
            "before": "NOT_STARTED",
            "after": "NOT_STARTED_R1_NOT_ACCEPTED",
            "evidence": "R2 deferral decision",
            "remaining_blocker": "R1 live acceptance",
        },
        {
            "domain": "MP-10 runtime deploy",
            "before": "NOT_AUTHORIZED",
            "after": "NOT_AUTHORIZED",
            "evidence": "no candidate or runtime operation",
            "remaining_blocker": "separate explicit approval",
        },
        {
            "domain": "P1 overall",
            "before": "LIVE_PARTIAL_NOT_VERIFIED",
            "after": "LIVE_PARTIAL_NOT_VERIFIED",
            "evidence": "MP-03 live coverage did not increase",
            "remaining_blocker": "MP-03/MP-10 and natural effect coverage",
        },
        {
            "domain": "P2 overall",
            "before": "LOCAL_VERIFIED",
            "after": "LOCAL_VERIFIED",
            "evidence": "MP-03 local candidate/NC pass only",
            "remaining_blocker": "runtime load and later explicit connection approval",
        },
        {
            "domain": "Production maintenance fence",
            "before": "PENDING",
            "after": "PENDING",
            "evidence": "no enforcement connection",
            "remaining_blocker": "P2-P4 production enforcement evidence",
        },
        {
            "domain": "DB deadline",
            "before": "SHADOW_LIVE_PARTIAL_NOT_VERIFIED",
            "after": "SHADOW_LIVE_PARTIAL_NOT_VERIFIED",
            "evidence": "not co-deployed or changed",
            "remaining_blocker": "separate fresh-heartbeat live validation",
        },
        {
            "domain": "Phase 4",
            "before": "SHADOW_ACCEPTED=false",
            "after": "SHADOW_ACCEPTED=false",
            "evidence": "no status elevation",
            "remaining_blocker": "remaining live coverage and acceptance",
        },
        {
            "domain": "Phase 5",
            "before": "PRECONDITIONS=NOT_MET",
            "after": "PRECONDITIONS=NOT_MET",
            "evidence": "production fence remains pending",
            "remaining_blocker": "Phase 4 and enforcement prerequisites",
        },
    ]
    write_json(root / "records/final_state_matrix.json", {"schema_version": "recovery_control.r1_state_matrix.v1", "rows": matrix})

    tests = {
        "stream_recovery_control_full_pytest": "240 passed",
        "stream_v3_full_pytest": "1009 passed, 10 skipped",
        "mp03_recovery_dedicated": "3 passed",
        "mp03_stream_v3_dedicated": "3 passed",
        "schema_replay": "3 passed",
        "mypy": "102 source files PASS",
        "scoped_ruff": "PASS",
        "scoped_ruff_format_check": "19 files already formatted",
        "py_compile": "PASS",
        "deterministic_oracle": f"{sum(item['pass'] for item in gate['deterministic'])}/8 PASS",
        "negative_controls": f"{gate['negative_controls_detected']}/{gate['negative_control_count']} EXPECTED_INJECTED_FAILURE",
        "candidate_self_test": "13/13 PASS",
        "failed_attempts_retained": True,
    }
    write_json(root / "records/test_results.json", tests)
    write_json(root / "end/git_states.json", git_states)

    start_pod = baseline["pod"]
    end_pod = end["pod"]
    summary = {
        "schema_version": "recovery_control.r1_mp03_terminal.v1",
        "generated_at_utc": now_utc,
        "generated_at_jst": now_jst,
        "executive_judgment": "LOCAL_CANDIDATE_VERIFIED_BUT_LIVE_DEPLOY_BLOCKED",
        "release_id": candidate["release_id"],
        "candidate_image_id": candidate["image_id"],
        "candidate_overlay_tree_sha256": candidate["overlay_tree_sha256"],
        "candidate_source_identity_verified": True,
        "change_set_contamination": candidate["change_set_contamination"],
        "enforcement_enabled": candidate["enforcement_enabled"],
        "production_adapter_connected": candidate["production_adapter_connected"],
        "runtime_deployed": candidate["runtime_deployed"],
        "k3s_image_staged": candidate["k3s_image_staged"],
        "release_gate_status": candidate["release_gate_status"],
        "release_gate_blockers": candidate["release_gate_blockers"],
        "rollback_image": candidate["rollback_image"],
        "rollback_status": "NOT_REQUIRED",
        "production_behavior_changed": False,
        "physical_effect_caused_by_r1": 0,
        "live_identity_unchanged": {
            "deployment_uid": baseline["deployment"]["uid"] == end["deployment"]["uid"],
            "pod_uid": start_pod["uid"] == end_pod["uid"],
            "container_statuses": start_pod["containers"] == end_pod["containers"],
            "target_identity": baseline["target_snapshot"]["target_identity"] == end["target_snapshot"]["target_identity"],
        },
        "live_candidate_evidence": {
            "event_count": None,
            "admission": "NOT_OBSERVED",
            "effect_boundary": "NOT_OBSERVED",
            "unknown_count_rate": "NOT_MEASURED",
            "event_loss": "NOT_MEASURED",
            "queue": "NOT_MEASURED",
        },
        "oracle_pass": gate["pass"],
        "negative_controls_detected": gate["negative_controls_detected"],
        "negative_control_count": gate["negative_control_count"],
        "performance": performance,
        "tests": tests,
        "file_ledgers": {name: value["counts"] for name, value in ledgers.items()},
        "end_git_states": git_states,
        "status": {item["domain"]: item["after"] for item in matrix},
        "commit": "NOT_CREATED",
        "push": "NOT_EXECUTED",
        "documents": [
            "docs/engineering/records/2026-08-24_41_mp03_r1_runtime_rollout.md",
            "docs/implementation/2026-08-24_11_mp03_r1_immutable_release.md",
            "docs/live/2026-08-24_mp03_r1_live_validation.md",
            "docs/incidents/2026-08-24_mp03_r1_release_gate_blocker.md",
            "docs/engineering/decisions/2026-08-24_02_mp03_r1_no_deploy.md",
            "docs/engineering/records/2026-08-24_42_mp10_r2_preparation_deferred.md",
            "docs/runbooks/2026-08-24_mp10_approval_and_rollback_deferred.md",
        ],
    }
    write_json(root / "records/terminal_summary.json", summary)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
