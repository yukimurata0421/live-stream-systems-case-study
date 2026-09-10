from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sqlite3
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra_harness.apply_path import run_apply_path_harness
from cra_harness.credential_binding import run_credential_binding_harness
from cra_harness.deployment_choreography import (
    CONSUMER_FIRST,
    FAILED_UPSTREAM_FIRST,
    evaluate_deployment_sequence,
    run_deployment_chaos,
)
from cra_harness.holdout import run_holdout_harness
from cra_harness.mutation_controls import run_mutation_controls


def _canonical(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def _git(project_root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _write_artifact(path: Path, value: dict[str, Any]) -> dict[str, Any]:
    payload = dict(value)
    payload["artifact_sha256"] = hashlib.sha256(_canonical(payload)).hexdigest()
    path.write_bytes(_canonical(payload) + b"\n")
    return payload


def _worktree_digest(project_root: Path) -> str:
    names = {
        line
        for arguments in (("ls-files",), ("ls-files", "--others", "--exclude-standard"))
        for line in _git(project_root, *arguments).splitlines()
        if line
    }
    digest = hashlib.sha256()
    for name in sorted(names):
        path = project_root / name
        if not path.is_file():
            continue
        digest.update(name.encode() + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def _deployment_report() -> dict[str, Any]:
    failed = evaluate_deployment_sequence(FAILED_UPSTREAM_FIRST)
    consumer_first = evaluate_deployment_sequence(CONSUMER_FIRST)
    chaos = run_deployment_chaos()
    passed = not failed["pass"] and consumer_first["pass"] and chaos["classification"] == "PASS"
    return {
        "schema": "cra.deployment_coverage_closure.v1",
        "classification": "PASS" if passed else "HARNESS_FAILURE",
        "scenario_family": "cutover_choreography_and_compound_faults",
        "scenario_count": 2 + int(chaos["scenario_count"]),
        "expected_failure_count": 1 + int(chaos["expected_failure_count"]),
        "sut_failure_count": 0,
        "harness_failure_count": 0 if passed else 1,
        "failed_upstream_first_detected": not failed["pass"],
        "consumer_first_passed": bool(consumer_first["pass"]),
        "failed_upstream_first": failed,
        "consumer_first": consumer_first,
        "chaos": chaos,
        "safety": {
            "production_mutation_count": 0,
            "physical_effect_count": 0,
            "ffmpeg_signal_count": 0,
            "network_failure_injection_count": 0,
        },
    }


def run_coverage_closure(
    project_root: Path,
    output: Path,
    *,
    seeds: tuple[int, ...] = (20260911, 20260912, 20260913, 20260914, 20260915),
    cases_per_seed: int = 128,
) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=False)
    started_wall = datetime.now(UTC)
    started = time.perf_counter()
    source_commit = _git(project_root, "rev-parse", "HEAD")
    source_tree_digest = _worktree_digest(project_root)

    deployment = _write_artifact(output / "deployment.json", _deployment_report())
    apply_path = _write_artifact(
        output / "apply-path.json",
        run_apply_path_harness(project_root, output / "apply-path-workspace"),
    )
    holdout = _write_artifact(
        output / "independent-holdout.json",
        run_holdout_harness(seeds=seeds, cases_per_seed=cases_per_seed),
    )
    mutation = _write_artifact(
        output / "mutation-controls.json",
        run_mutation_controls(project_root, output / "mutation-workspace"),
    )
    credential_binding = _write_artifact(
        output / "credential-binding.json",
        run_credential_binding_harness(),
    )
    reports = {
        "deployment": deployment,
        "apply_path": apply_path,
        "independent_holdout": holdout,
        "mutation_controls": mutation,
        "credential_binding": credential_binding,
    }
    classifications = {name: str(value["classification"]) for name, value in reports.items()}
    passed = all(value == "PASS" for value in classifications.values())
    finished_wall = datetime.now(UTC)
    manifest = {
        "schema": "cra.coverage_closure_manifest.v1",
        "classification": "PASS" if passed else "HARNESS_FAILURE",
        "source_commit": source_commit,
        "harness_commit": source_commit,
        "source_worktree_diff_sha256": source_tree_digest,
        "source_worktree_dirty": bool(_git(project_root, "status", "--porcelain")),
        "started_at": started_wall.isoformat(),
        "finished_at": finished_wall.isoformat(),
        "wall_duration_seconds": round(time.perf_counter() - started, 6),
        "runtime_versions": {
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "platform": platform.platform(),
        },
        "seeds": list(seeds),
        "scenario_families": [
            "cutover_choreography_and_compound_faults",
            "real_vertical_apply_path_with_fake_effect_boundary",
            "operation_order_and_restart_reentry_holdout",
            "harness_mutation_negative_controls",
            "credential_rehearsal_release_and_endpoint_binding",
        ],
        "scenario_count": int(deployment["scenario_count"])
        + int(apply_path["scenario_count"])
        + int(holdout["case_count"])
        + int(mutation["mutation_count"])
        + int(credential_binding["scenario_count"]),
        "simulated_operation_count": int(holdout["simulated_operation_count"]),
        "sut_failure_count": int(holdout["sut_failure_count"]) + int(mutation["sut_failure_count"]),
        "harness_failure_count": int(deployment["harness_failure_count"])
        + int(holdout["harness_failure_count"])
        + int(mutation["harness_failure_count"])
        + int(credential_binding["harness_failure_count"])
        + int(apply_path["classification"] != "PASS"),
        "expected_failure_count": int(deployment["expected_failure_count"]),
        "negative_control_detection_rate": mutation["detection_rate"],
        "mutation_identities": [item["mutation_identity"] for item in mutation["mutations"]],
        "classifications": classifications,
        "safety": {
            "production_mutation_count": 0,
            "production_database_used": False,
            "production_network_used": False,
            "physical_effect_count": 0,
            "ffmpeg_signal_count": 0,
        },
        "artifacts": {name: value["artifact_sha256"] for name, value in reports.items()},
    }
    return _write_artifact(output / "manifest.json", manifest)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the isolated CRA coverage-closure Harness suite")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cases-per-seed", type=int, default=128)
    args = parser.parse_args()
    result = run_coverage_closure(args.project_root.resolve(), args.output.resolve(), cases_per_seed=args.cases_per_seed)
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    if result["classification"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
