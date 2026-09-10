from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

TARGET_FIELDS = {
    "host_id",
    "host_boot_id",
    "namespace",
    "pod_uid",
    "container_name",
    "container_id",
    "ffmpeg_generation",
    "ffmpeg_pid",
}


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    if not path.exists():
        return values
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"{path.name}:{line_number} is not an object")
        values.append(dict(value))
    return values


def _percentile(values: list[float], proportion: float) -> float | None:
    if len(values) < 3:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(proportion * len(ordered)) - 1)]


def _distribution(values: list[float], unit: str) -> dict[str, Any]:
    return {
        "sample_count": len(values),
        "minimum": min(values) if values else None,
        "median": statistics.median(values) if values else None,
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "maximum": max(values) if values else None,
        "unit": unit,
        "definition": "median=statistics.median; p95/p99=nearest-rank ceil(p*n)-1; percentile requires n>=3",
    }


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def analyze_real_shadow(root: Path, fake_summary_path: Path | None = None) -> dict[str, Any]:
    heartbeat = _load_jsonl(root / "heartbeat.jsonl")
    snapshots = _load_jsonl(root / "target_snapshots.jsonl")
    commands = _load_jsonl(root / "commands.jsonl")
    receipts = _load_jsonl(root / "receipts.jsonl")
    states = _load_jsonl(root / "agent_state.jsonl")
    verifications = _load_jsonl(root / "verification.jsonl")
    sent = [item for item in heartbeat if item.get("sent") is True]
    successful = [item for item in sent if item.get("received_authority_state") == "CENTRAL_ACTIVE"]
    heartbeat_times = [_timestamp(str(item["issued_at"])) for item in successful]
    heartbeat_intervals = [
        (after - before).total_seconds()
        for before, after in zip(heartbeat_times, heartbeat_times[1:], strict=False)
        if 0 <= (after - before).total_seconds() < 10
    ]
    roundtrips = [float(item["processing_roundtrip_ms"]) for item in successful]
    cycle_durations = [float(item["cycle_duration_ms"]) for item in heartbeat if "cycle_duration_ms" in item]
    complete = [
        item
        for item in snapshots
        if item.get("status") == "VALID"
        and isinstance(item.get("target_identity"), dict)
        and set(item["target_identity"]) >= TARGET_FIELDS
        and all(item["target_identity"].get(name) not in (None, "") for name in TARGET_FIELDS)
    ]
    unique_snapshots = {str(item.get("snapshot_id")): item for item in snapshots if item.get("snapshot_id")}
    unique_complete = {str(item.get("snapshot_id")) for item in complete}
    decisions = Counter(str(item.get("shadow_decision")) for item in receipts)
    reject_reasons = Counter(str(item.get("reason_code")) for item in receipts if item.get("shadow_decision") == "WOULD_REJECT")
    verdicts = Counter(str(item.get("verdict")) for item in verifications)
    physical_counts = [int(item.get("physical_attempt_count") or 0) for item in heartbeat + receipts + states]
    authority_states = Counter(str(item.get("authority_state")) for item in states if item.get("authority_state"))
    sequences: dict[tuple[int, str], list[int]] = defaultdict(list)
    for item in successful:
        if "authority_epoch" in item and "authority_session_id" in item:
            sequences[(int(item["authority_epoch"]), str(item["authority_session_id"]))].append(int(item["heartbeat_seq"]))
    sequence_divergence = sum(1 for values in sequences.values() if values != list(range(values[0], values[0] + len(values))))
    epoch_events = {int(item["authority_epoch"]) for item in states if item.get("event") == "RECONCILED"}
    heartbeat_epochs = {int(item["authority_epoch"]) for item in successful if "authority_epoch" in item}
    unexplained_epochs = sorted(heartbeat_epochs - epoch_events)
    safety_violations = []
    if max(physical_counts, default=0) != 0:
        safety_violations.append("PHYSICAL_ATTEMPT_NONZERO")
    if sequence_divergence:
        safety_violations.append("SEQUENCE_DIVERGENCE")
    if unexplained_epochs:
        safety_violations.append("UNEXPLAINED_EPOCH")
    missing = []
    if not heartbeat:
        missing.append("HEARTBEAT_EVIDENCE")
    if not snapshots:
        missing.append("TARGET_SNAPSHOT_EVIDENCE")
    if len(unique_complete) != len(unique_snapshots):
        missing.append("TARGET_IDENTITY_INCOMPLETE")
    classification = "PASS"
    if safety_violations:
        classification = "SAFETY_GATE_FAILURE"
    elif missing:
        classification = "MISSING_EVIDENCE"
    fake_comparison: dict[str, Any] = {
        "heartbeat_timing": {"classification": "UNKNOWN", "reason": "fake summary unavailable"},
        "target_snapshot": {
            "classification": "HARNESS_MODEL_GAP",
            "reason": "fake v2 did not measure real Kubernetes/proc snapshot collection",
        },
        "reject_distribution": {"classification": "UNKNOWN", "reason": "real diagnostic sample is not a workload-trigger distribution"},
        "verification_latency": {"classification": "NOT_APPLICABLE", "reason": "no physical effect exists in Phase 4"},
    }
    if fake_summary_path is not None and fake_summary_path.exists():
        fake = json.loads(fake_summary_path.read_text(encoding="utf-8"))
        fake_heartbeat = fake.get("statistics", {}).get("heartbeat_processing", {})
        fake_comparison["heartbeat_timing"] = {
            "classification": "EXPECTED_ENVIRONMENT_DIFFERENCE",
            "fake_median_ms": fake_heartbeat.get("median"),
            "real_median_roundtrip_ms": _distribution(roundtrips, "ms")["median"],
            "reason": "real value includes mTLS, LAN, HTTP, scheduling and two SQLite processes",
        }
    return {
        "profile": "REAL_SHADOW_REPLAY",
        "classification": classification,
        "harness_real_replay_trusted": classification == "PASS",
        "safety_gate": "PASS" if not safety_violations else "FAIL",
        "safety_violations": safety_violations,
        "missing_evidence": missing,
        "heartbeat": {
            "cycle_count": len(heartbeat),
            "sent_count": len(sent),
            "central_active_response_count": len(successful),
            "interval_seconds": _distribution(heartbeat_intervals, "seconds"),
            "roundtrip_ms": _distribution(roundtrips, "ms"),
            "cycle_duration_ms": _distribution(cycle_durations, "ms"),
            "authority_state_distribution": dict(sorted(authority_states.items())),
        },
        "target_snapshot": {
            "sample_count": len(snapshots),
            "unique_count": len(unique_snapshots),
            "unique_complete_count": len(unique_complete),
            "unstable_count": sum(item.get("reason_code") == "SNAPSHOT_UNSTABLE" for item in unique_snapshots.values()),
            "stale_count": sum(item.get("reason_code") == "STALE_TARGET" for item in unique_snapshots.values()),
            "invalid_count": sum(item.get("status") != "VALID" for item in unique_snapshots.values()),
        },
        "shadow_command": {
            "command_count": len(commands),
            "receipt_count": len(receipts),
            "decision_distribution": dict(sorted(decisions.items())),
            "reject_reason_distribution": dict(sorted(reject_reasons.items())),
        },
        "verification": {
            "count": len(verifications),
            "verdict_distribution": dict(sorted(verdicts.items())),
        },
        "divergence": {
            "sequence_divergence_count": sequence_divergence,
            "unexplained_epoch_count": len(unexplained_epochs),
            "unexplained_epochs": unexplained_epochs,
        },
        "physical_attempt_count": max(physical_counts, default=0),
        "fake_vs_real": fake_comparison,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay CRA Phase 4 real shadow evidence")
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--fake-summary", type=Path)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    result = analyze_real_shadow(args.artifact, args.fake_summary)
    if args.write:
        outputs = {
            "summary.json": result,
            "metrics.json": {
                "heartbeat": result["heartbeat"],
                "target_snapshot": result["target_snapshot"],
                "shadow_command": result["shadow_command"],
                "verification": result["verification"],
            },
            "classification.json": {
                "profile": result["profile"],
                "classification": result["classification"],
                "harness_real_replay_trusted": result["harness_real_replay_trusted"],
                "safety_gate": result["safety_gate"],
                "safety_violations": result["safety_violations"],
                "missing_evidence": result["missing_evidence"],
            },
            "comparison.json": result["fake_vs_real"],
        }
        for name, payload in outputs.items():
            (args.artifact / name).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
