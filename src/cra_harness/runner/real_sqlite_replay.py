from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from cra_dell_recovery.time import parse_utc


def _load_json(path: Path) -> dict[str, Any]:
    return dict(json.loads(path.read_text(encoding="utf-8")))


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [dict(json.loads(line)) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _timestamp(row: dict[str, Any], candidates: tuple[str, ...]) -> str | None:
    for name in candidates:
        value = row.get(name)
        if value is not None:
            return str(value)
    return None


def _slice(
    rows: list[dict[str, Any]],
    *,
    started_at: str,
    finished_at: str,
    timestamp_fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    start = parse_utc(started_at)
    finish = parse_utc(finished_at)
    selected: list[dict[str, Any]] = []
    for row in rows:
        stamp = _timestamp(row, timestamp_fields)
        if stamp is None:
            continue
        observed = parse_utc(stamp)
        if start <= observed <= finish:
            selected.append(row)
    return selected


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("x", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")


def _monotonic(values: list[int]) -> bool:
    return all(current >= previous for previous, current in zip(values, values[1:], strict=False))


def replay_live_sqlite_evidence(run_dir: Path, *, window_start: str, window_end: str) -> dict[str, Any]:
    raw = run_dir / "raw/arena"
    heartbeat_full = _load_jsonl(raw / "heartbeat.full.jsonl")
    agent_full = _load_jsonl(raw / "agent_state.full.jsonl")
    target_full = _load_jsonl(raw / "target_snapshots.full.jsonl")
    heartbeats = _slice(
        heartbeat_full,
        started_at=window_start,
        finished_at=window_end,
        timestamp_fields=("cycle_observed_at", "observed_at"),
    )
    agent_states = _slice(
        agent_full,
        started_at=window_start,
        finished_at=window_end,
        timestamp_fields=("observed_at",),
    )
    targets = _slice(
        target_full,
        started_at=window_start,
        finished_at=window_end,
        timestamp_fields=("observed_at",),
    )
    _write_jsonl(run_dir / "heartbeat.jsonl", heartbeats)
    _write_jsonl(run_dir / "agent_state.jsonl", agent_states)
    _write_jsonl(run_dir / "target_snapshots.jsonl", targets)
    probe = _load_json(run_dir / "summary.json")
    status_burst = _load_json(run_dir / "status_burst.json")
    service = _load_json(run_dir / "service_state.json")
    safety = _load_json(run_dir / "safety.json")
    heartbeat_errors = [item for item in heartbeats if item.get("transport_error") or item.get("error")]
    agent_errors = [item for item in agent_states if item.get("error")]
    physical_values = [
        int(item["physical_attempt_count"]) for item in [*heartbeats, *agent_states] if item.get("physical_attempt_count") is not None
    ]
    epochs = [int(item["authority_epoch"]) for item in heartbeats if item.get("authority_epoch") is not None]
    heartbeat_sequences = [int(item["heartbeat_seq"]) for item in heartbeats if item.get("heartbeat_seq") is not None]
    reconciliations = [item for item in agent_states if item.get("event") == "RECONCILED"]
    target_complete = 0
    target_fields = {
        "host_id",
        "host_boot_id",
        "namespace",
        "pod_uid",
        "container_name",
        "container_id",
        "ffmpeg_generation",
        "ffmpeg_pid",
    }
    for item in targets:
        identity = item.get("target_identity")
        if item.get("status") == "VALID" and isinstance(identity, dict) and target_fields <= set(identity):
            target_complete += 1
    authority_states = Counter(str(item.get("authority_state")) for item in agent_states if item.get("authority_state") is not None)
    checks = {
        "live_probe_pass": probe.get("result") == "PASS",
        "checkpoint_read_overlap": int(probe.get("checkpoint_read_overlap_hits", 0)) == int(probe.get("cycles", -1)) > 0,
        "heartbeat_checkpoint_overlap": int(probe.get("heartbeat_write_cycle_hits", 0)) > 0,
        "critical_read_errors_zero": int(probe.get("critical_read_errors", -1)) == 0,
        "checkpoint_errors_zero": int(probe.get("checkpoint_errors", -1)) == 0,
        "inconsistent_reads_zero": int(probe.get("inconsistent_reads", -1)) == 0,
        "status_burst_complete": int(status_burst.get("success", -1)) == int(status_burst.get("requests", -2)) == 600,
        "status_burst_errors_zero": not status_burst.get("errors"),
        "status_burst_all_active": status_burst.get("states") == {"CENTRAL_ACTIVE": 600},
        "real_heartbeat_errors_zero": not heartbeat_errors,
        "real_agent_status_errors_zero": not agent_errors,
        "physical_effect_zero": max(physical_values, default=0) == 0 and not any(int(value) for value in safety.values()),
        "epoch_monotonic": _monotonic(epochs),
        "heartbeat_sequence_monotonic": _monotonic(heartbeat_sequences),
        "controlled_reconciliation_observed": len(reconciliations) >= 1,
        "target_snapshot_complete": bool(targets) and target_complete == len(targets),
        "agent_unexpected_restart_zero": int(service["service"]["n_restarts"]) == 0,
        "journal_key_error_zero": int(service["journal_since_deploy"]["key_error_count"]) == 0,
        "journal_sqlite_error_zero": int(service["journal_since_deploy"]["sqlite_error_count"]) == 0,
        "ledger_integrity_ok": service["ledger"]["integrity_check"] == "ok",
        "unresolved_effect_state_zero": all(
            int(service["ledger"][name]) == 0 for name in ("agent_commands", "execution_attempts", "local_actions")
        ),
        "final_authority_active": service["authority"]["state"] == "CENTRAL_ACTIVE",
    }
    trusted = all(checks.values())
    result = {
        "profile": "REAL_SQLITE_CONCURRENCY_REPLAY",
        "window_start": window_start,
        "window_end": window_end,
        "classification": "PASS" if trusted else "FAIL",
        "trusted": trusted,
        "checks": checks,
        "evidence_counts": {
            "heartbeat": len(heartbeats),
            "agent_state": len(agent_states),
            "target_snapshot": len(targets),
            "controlled_reconciliation": len(reconciliations),
            "status_burst_reads": int(status_burst.get("success", 0)),
            "checkpoints": int(probe.get("checkpoint_count", 0)),
            "checkpoint_read_overlaps": int(probe.get("checkpoint_read_overlap_hits", 0)),
            "heartbeat_checkpoint_overlaps": int(probe.get("heartbeat_write_cycle_hits", 0)),
        },
        "authority_state_counts": dict(sorted(authority_states.items())),
        "epoch_min": min(epochs) if epochs else None,
        "epoch_max": max(epochs) if epochs else None,
        "heartbeat_seq_min": min(heartbeat_sequences) if heartbeat_sequences else None,
        "heartbeat_seq_max": max(heartbeat_sequences) if heartbeat_sequences else None,
        "physical_attempt_max": max(physical_values, default=0),
        "root_cause_classification": "LIKELY",
        "root_cause_reason": (
            "shared-connection/checkpoint/read/write concurrency remains the best-supported explanation; "
            "the old exact race was not instrumented at SQLite internals"
        ),
        "defect_closure": "FIXED" if trusted else "LIVE_VERIFICATION_PENDING",
    }
    (run_dir / "real_replay.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay real Phase 4 SQLite concurrency evidence")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    args = parser.parse_args()
    result = replay_live_sqlite_evidence(args.run_dir, window_start=args.window_start, window_end=args.window_end)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["trusted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
