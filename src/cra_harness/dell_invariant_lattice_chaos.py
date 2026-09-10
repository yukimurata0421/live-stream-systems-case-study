from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import sqlite3
import subprocess
import time
from collections import Counter
from pathlib import Path
from typing import Any, cast

from cra_authority.reconciliation import commit_response_matches
from cra_dell_recovery.models import LocalRecoveryCandidate, TargetIdentity, is_expected_ffmpeg_successor
from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.execution import (
    LOCAL_CANDIDATE_ALLOWED_EVIDENCE_KEYS,
    LOCAL_CANDIDATE_MAX_CONFIRM_THRESHOLD,
    LOCAL_CANDIDATE_MAX_DISTINCT_SAMPLES,
    LOCAL_CANDIDATE_MIN_DISTINCT_SAMPLES,
    LOCAL_CANDIDATE_REJECTED_EVIDENCE_KEYS,
    local_candidate_evidence_rejection,
    local_candidate_matches_existing,
)

SCHEMA = "cra.dell_invariant_lattice_chaos.v1"
MAX_FAILURE_SAMPLES = 50


def _target() -> TargetIdentity:
    return TargetIdentity(
        host_id="dell",
        host_boot_id="boot-a",
        namespace="stream-v3",
        pod_uid="pod-a",
        container_name="stream-engine",
        container_id="containerd://a",
        ffmpeg_generation="generation-a",
        ffmpeg_pid=4100,
    )


def _strict_equal(left: object, right: object) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        right_dict = cast(dict[Any, Any], right)
        return set(left) == set(right_dict) and all(_strict_equal(left[key], right_dict[key]) for key in left)
    if isinstance(left, list):
        right_list = cast(list[Any], right)
        return len(left) == len(right_list) and all(_strict_equal(a, b) for a, b in zip(left, right_list, strict=True))
    return left == right


def _record_failure(failures: list[dict[str, Any]], value: dict[str, Any]) -> None:
    if len(failures) < MAX_FAILURE_SAMPLES:
        failures.append(value)


def run_successor_lattice(*, case_count: int, seed: int) -> dict[str, Any]:
    rng = random.Random(seed)
    before = _target()
    fields = ("host_id", "host_boot_id", "namespace", "pod_uid", "container_id", "ffmpeg_generation", "ffmpeg_pid")
    failures: list[dict[str, Any]] = []
    failure_count = 0
    classifications: Counter[str] = Counter()
    for index in range(case_count):
        mask = index if index < 1 << len(fields) else rng.randrange(1 << len(fields))
        after = before.to_dict()
        for bit, field in enumerate(fields):
            if mask & (1 << bit):
                after[field] = 4200 + index if field == "ffmpeg_pid" else f"{field}-changed-{index}"
        observed = TargetIdentity.from_dict(after)
        expected = (
            mask & (1 << fields.index("ffmpeg_generation")) != 0
            and mask & (1 << fields.index("ffmpeg_pid")) != 0
            and mask & ~((1 << fields.index("ffmpeg_generation")) | (1 << fields.index("ffmpeg_pid"))) == 0
        )
        actual = is_expected_ffmpeg_successor(before, observed)
        classifications["ACCEPT" if actual else "REJECT"] += 1
        if actual != expected:
            failure_count += 1
            _record_failure(failures, {"index": index, "mask": mask, "expected": expected, "actual": actual})
    return {
        "family": "post_effect_successor_relation",
        "case_count": case_count,
        "exhaustive_prefix_count": min(case_count, 1 << len(fields)),
        "classifications": dict(classifications),
        "failure_count": failure_count,
        "failure_samples": failures,
        "pass": not failures,
    }


def _evidence_oracle(evidence: object) -> str | None:
    if not isinstance(evidence, dict):
        return "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"
    if len(evidence) > len(LOCAL_CANDIDATE_ALLOWED_EVIDENCE_KEYS):
        return "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"
    if any(type(key) is not str for key in evidence):
        return "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"
    if any(key not in LOCAL_CANDIDATE_ALLOWED_EVIDENCE_KEYS for key in evidence):
        return "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"
    if any(key in evidence and type(evidence[key]) is not bool for key in LOCAL_CANDIDATE_REJECTED_EVIDENCE_KEYS):
        return "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"
    if type(evidence.get("tcp_stall_confirmed")) is not bool or evidence.get("tcp_stall_confirmed") is not True:
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    samples = evidence.get("distinct_sample_count")
    threshold = evidence.get("stall_confirm_threshold")
    if type(samples) is not int or not LOCAL_CANDIDATE_MIN_DISTINCT_SAMPLES <= samples <= LOCAL_CANDIDATE_MAX_DISTINCT_SAMPLES:
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    if type(threshold) is not int or not LOCAL_CANDIDATE_MIN_DISTINCT_SAMPLES <= threshold <= LOCAL_CANDIDATE_MAX_CONFIRM_THRESHOLD:
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    if samples < threshold:
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    if any(evidence.get(key) is True for key in LOCAL_CANDIDATE_REJECTED_EVIDENCE_KEYS):
        return "INSUFFICIENT_LOCAL_EVIDENCE"
    return None


def _random_evidence(rng: random.Random, index: int) -> object:
    non_mappings: list[object] = [None, [], "confirmed", 3, True]
    if index % 23 == 0:
        return non_mappings[index % len(non_mappings)]
    evidence: dict[object, object] = {
        "tcp_stall_confirmed": rng.choice([True, False, 1, 0, None, "true"]),
        "distinct_sample_count": rng.choice([-1, 0, 2, 3, 4, 20, 21, 1_000_000, 1_000_001, True, 3.0, None]),
        "stall_confirm_threshold": rng.choice([-1, 2, 3, 4, 20, 21, True, 3.0, None]),
    }
    for key in LOCAL_CANDIDATE_REJECTED_EVIDENCE_KEYS:
        if rng.randrange(8) == 0:
            evidence[key] = rng.choice([True, False, 0, 1, None, "false"])
    if rng.randrange(17) == 0:
        evidence[f"unknown_{index % 11}"] = True
    if rng.randrange(97) == 0:
        evidence[index] = False
    return evidence


def run_evidence_lattice(*, case_count: int, seed: int) -> dict[str, Any]:
    rng = random.Random(seed)
    deterministic: list[object] = [
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 20, "stall_confirm_threshold": 20},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 1_000_000, "stall_confirm_threshold": 3},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 1_000_001, "stall_confirm_threshold": 3},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3.0, "stall_confirm_threshold": 3},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3, "unknown": False},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3, "network_down": True},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3, "network_down": 0},
        None,
        [],
    ]
    failures: list[dict[str, Any]] = []
    failure_count = 0
    outcomes: Counter[str] = Counter()
    for index in range(case_count):
        evidence = deterministic[index] if index < len(deterministic) else _random_evidence(rng, index)
        expected = _evidence_oracle(evidence)
        actual = local_candidate_evidence_rejection(evidence)
        outcomes[actual or "ACCEPT"] += 1
        if actual != expected:
            failure_count += 1
            _record_failure(
                failures,
                {"index": index, "expected": expected, "actual": actual, "evidence_type": type(evidence).__name__},
            )
    return {
        "family": "local_candidate_evidence",
        "case_count": case_count,
        "outcomes": dict(outcomes),
        "failure_count": failure_count,
        "failure_samples": failures,
        "pass": not failures,
    }


def _candidate(evidence: object, *, mutation: str, index: int) -> LocalRecoveryCandidate:
    target = _target()
    target_id = "stream-target"
    action = "restart_ffmpeg"
    reason = "confirmed_tcp_stall"
    if mutation == "target_id":
        target_id = "other-target"
    elif mutation == "action":
        action = "restart_pod"
    elif mutation == "reason":
        reason = "network_down"
    elif mutation == "target_identity":
        target = TargetIdentity.from_dict({**target.to_dict(), "ffmpeg_generation": f"generation-{index}", "ffmpeg_pid": 4200})
    return LocalRecoveryCandidate(
        local_action_id="local-action-1",
        target_id=target_id,
        action=action,
        reason_code=reason,
        target_identity=target,
        evidence=evidence,  # type: ignore[arg-type]
        observed_at=isoformat_utc(utc_now()),
    )


def run_replay_lattice(*, case_count: int, seed: int) -> dict[str, Any]:
    rng = random.Random(seed)
    base_evidence = {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3}
    base_target = _target().to_dict()
    existing = {
        "state": "EFFECT_OBSERVED",
        "target_id": "stream-target",
        "action": "restart_ffmpeg",
        "reason_code": "confirmed_tcp_stall",
        "target_identity_json": json.dumps(base_target, separators=(",", ":"), sort_keys=True),
        "evidence_json": json.dumps(base_evidence, separators=(",", ":"), sort_keys=True),
    }
    mutations = (
        "exact",
        "reordered",
        "target_id",
        "action",
        "reason",
        "target_identity",
        "evidence_value",
        "numeric_type",
        "extra_evidence",
        "non_mapping",
    )
    failures: list[dict[str, Any]] = []
    failure_count = 0
    outcomes: Counter[str] = Counter()
    for index in range(case_count):
        mutation = mutations[index] if index < len(mutations) else rng.choice(mutations)
        evidence: object = dict(base_evidence)
        if mutation == "reordered":
            evidence = {"stall_confirm_threshold": 3, "distinct_sample_count": 3, "tcp_stall_confirmed": True}
        elif mutation == "evidence_value":
            evidence = {**base_evidence, "distinct_sample_count": 4}
        elif mutation == "numeric_type":
            evidence = {**base_evidence, "distinct_sample_count": 3.0}
        elif mutation == "extra_evidence":
            evidence = {**base_evidence, "unversioned": False}
        elif mutation == "non_mapping":
            evidence = [True, 3, 3]
        candidate = _candidate(evidence, mutation=mutation, index=index)
        expected = (
            candidate.target_id == existing["target_id"]
            and candidate.action == existing["action"]
            and candidate.reason_code == existing["reason_code"]
            and _strict_equal(candidate.target_identity.to_dict(), base_target)
            and _strict_equal(candidate.evidence, base_evidence)
        )
        actual = local_candidate_matches_existing(existing, candidate)
        outcomes["EXACT" if actual else "CONFLICT"] += 1
        if actual != expected:
            failure_count += 1
            _record_failure(failures, {"index": index, "mutation": mutation, "expected": expected, "actual": actual})
    return {
        "family": "local_action_idempotency_binding",
        "case_count": case_count,
        "outcomes": dict(outcomes),
        "failure_count": failure_count,
        "failure_samples": failures,
        "pass": not failures,
    }


def run_commit_lattice(*, case_count: int, seed: int) -> dict[str, Any]:
    rng = random.Random(seed)
    expected_target = "stream-target"
    expected_epoch = 17
    expected_session = "session-expected"
    result_values: list[object] = ["COMMITTED", "REJECTED", "", None, True]
    target_values: list[object] = [expected_target, "other-target", "", None, 17]
    epoch_values: list[object] = [expected_epoch, expected_epoch - 1, expected_epoch + 1, True, 17.0, "17", None]
    session_values: list[object] = [expected_session, "session-other", "", None, 17]
    failures: list[dict[str, Any]] = []
    failure_count = 0
    outcomes: Counter[str] = Counter()
    for index in range(case_count):
        if index == 0:
            result: dict[str, Any] = {
                "result": "COMMITTED",
                "target_id": expected_target,
                "authority_epoch": expected_epoch,
                "authority_session_id": expected_session,
            }
        else:
            result = {
                "result": rng.choice(result_values),
                "target_id": rng.choice(target_values),
                "authority_epoch": rng.choice(epoch_values),
                "authority_session_id": rng.choice(session_values),
            }
            if index < 12:
                field = ("result", "target_id", "authority_epoch", "authority_session_id")[(index - 1) % 4]
                result.update(
                    {
                        "result": "COMMITTED",
                        "target_id": expected_target,
                        "authority_epoch": expected_epoch,
                        "authority_session_id": expected_session,
                    }
                )
                result[field] = {
                    "result": "REJECTED",
                    "target_id": "other-target",
                    "authority_epoch": True,
                    "authority_session_id": "session-other",
                }[field]
        expected = (
            result.get("result") == "COMMITTED"
            and result.get("target_id") == expected_target
            and type(result.get("authority_epoch")) is int
            and result.get("authority_epoch") == expected_epoch
            and result.get("authority_session_id") == expected_session
        )
        actual = commit_response_matches(
            result,
            target_id=expected_target,
            authority_epoch=expected_epoch,
            authority_session_id=expected_session,
        )
        outcomes["ACCEPT" if actual else "REJECT"] += 1
        if actual != expected:
            failure_count += 1
            _record_failure(failures, {"index": index, "expected": expected, "actual": actual, "result": result})
    return {
        "family": "reconciliation_commit_response_binding",
        "case_count": case_count,
        "outcomes": dict(outcomes),
        "failure_count": failure_count,
        "failure_samples": failures,
        "pass": not failures,
    }


def _atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_campaign(project_root: Path, output: Path, *, cases_per_family: int = 250_000, seed: int = 20260901) -> dict[str, Any]:
    if isinstance(cases_per_family, bool) or not isinstance(cases_per_family, int) or not 128 <= cases_per_family <= 1_000_000:
        raise ValueError("DELL_INVARIANT_CASE_COUNT_INVALID")
    project_root = project_root.resolve()
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    started_at = isoformat_utc(utc_now())
    started = time.perf_counter()
    families = [
        run_successor_lattice(case_count=cases_per_family, seed=seed),
        run_evidence_lattice(case_count=cases_per_family, seed=seed + 1),
        run_replay_lattice(case_count=cases_per_family, seed=seed + 2),
        run_commit_lattice(case_count=cases_per_family, seed=seed + 3),
    ]
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=project_root, check=True, capture_output=True, text=True, timeout=10
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.splitlines()
    source_paths = (
        "src/cra_harness/dell_invariant_lattice_chaos.py",
        "src/cra_authority/reconciliation.py",
        "src/cra_dell_recovery/models.py",
        "src/dell_recovery_agent/execution.py",
        "src/dell_recovery_agent/runtime_effect_adapter.py",
        "src/runtime_boundary/reconciliation.py",
        "tests/harness/integration/test_dell_invariant_lattice_chaos.py",
    )
    summary = {
        "schema": SCHEMA,
        "classification": "PASS" if all(family["pass"] for family in families) else "FAIL",
        "started_at": started_at,
        "finished_at": isoformat_utc(utc_now()),
        "duration_seconds": round(time.perf_counter() - started, 6),
        "host": platform.node(),
        "sqlite_version": sqlite3.sqlite_version,
        "source_revision": revision,
        "source_worktree_dirty": bool(dirty),
        "source_modified_paths": sorted(line[3:] for line in dirty),
        "source_hashes": {path: _sha256(project_root / path) for path in source_paths},
        "seed": seed,
        "cases_per_family": cases_per_family,
        "case_count": cases_per_family * len(families),
        "failure_count": sum(int(family["failure_count"]) for family in families),
        "families": {str(family["family"]): family for family in families},
        "safety": {
            "production_network_used": False,
            "production_database_used": False,
            "production_effect_socket_used": False,
            "ffmpeg_signal_count": 0,
            "pod_mutation_count": 0,
            "deployment_mutation_count": 0,
            "host_restart_count": 0,
            "physical_effect_count": 0,
        },
    }
    _atomic_json(output / "families.json", families)
    _atomic_json(output / "summary.json", summary)
    _atomic_json(
        output / "manifest.json",
        {
            "schema": "cra.dell_invariant_lattice_chaos_manifest.v1",
            "files": {name: _sha256(output / name) for name in ("families.json", "summary.json")},
        },
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Dell recovery invariant cross-product chaos without production effects")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cases-per-family", type=int, default=250_000)
    parser.add_argument("--seed", type=int, default=20260901)
    args = parser.parse_args()
    summary = run_campaign(args.project_root, args.output, cases_per_family=args.cases_per_family, seed=args.seed)
    print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
    if summary["classification"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
