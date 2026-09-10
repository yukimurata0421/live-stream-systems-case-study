#!/usr/bin/env python3
"""Replay P1 live audit evidence without importing the production evaluator.

The live replay deliberately has a narrow oracle.  It validates the semantics
that can be decided from the captured event alone.  In particular, an absent
or UNKNOWN maintenance snapshot must remain UNKNOWN and must never be promoted
to WOULD_ALLOW.  States that were not observed in the live window are reported
as unsupported by this replay instead of being guessed.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter
from pathlib import Path
from typing import Any

REQUIRED_FIELDS = {
    "timestamp",
    "host",
    "process_service",
    "path_id",
    "phase",
    "operation",
    "maintenance_observed_state",
    "maintenance_id",
    "maintenance_generation",
    "audit_verdict",
    "audit_reason",
    "production_behavior_modified",
    "process_instance_id",
    "queue_depth_before",
    "audit_hook_latency_us",
}
ALLOWED_VERDICTS = {
    "WOULD_ALLOW",
    "WOULD_BLOCK",
    "WOULD_ACK",
    "WOULD_REJECT",
    "UNKNOWN",
    "NOT_APPLICABLE",
}
SECRET_KEY_FRAGMENTS = (
    "password",
    "private_key",
    "secret",
    "token",
    "credential",
)


def percentile(values: list[float], percentile_value: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(0, math.ceil((percentile_value / 100) * len(ordered)) - 1)
    return round(ordered[rank], 3)


def secret_like_keys(value: Any, prefix: str = "") -> list[str]:
    findings: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key).lower()
            path = f"{prefix}.{key}" if prefix else str(key)
            if any(fragment in key_text for fragment in SECRET_KEY_FRAGMENTS):
                findings.append(path)
            findings.extend(secret_like_keys(child, path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            findings.extend(secret_like_keys(child, f"{prefix}[{index}]"))
    return findings


def independent_expected(event: dict[str, Any]) -> tuple[str | None, str]:
    observed_state = event.get("maintenance_observed_state")
    maintenance_id = event.get("maintenance_id")
    observed_at = event.get("maintenance_snapshot_observed_at")
    fresh_until = event.get("maintenance_snapshot_fresh_until")
    unavailable = (
        observed_state in {None, "", "UNKNOWN"}
        or maintenance_id in {None, "", "UNKNOWN"}
        or observed_at in {None, "", "UNKNOWN"}
        or fresh_until in {None, "", "UNKNOWN"}
    )
    if unavailable:
        return "UNKNOWN", "INDEPENDENT_SNAPSHOT_UNAVAILABLE"
    return None, "STATE_NOT_OBSERVED_IN_THIS_LIVE_REPLAY"


def load_events(paths: list[Path]) -> tuple[list[dict[str, Any]], list[str]]:
    events: list[dict[str, Any]] = []
    parse_errors: list[str] = []
    for path in paths:
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    parse_errors.append(f"{path}:{line_number}:{exc.msg}")
                    continue
                event["_source_file"] = path.name
                event["_source_line"] = line_number
                events.append(event)
    return events, parse_errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    events, parse_errors = load_events(args.event)
    failures: list[dict[str, Any]] = []
    verdicts: Counter[str] = Counter()
    paths: Counter[str] = Counter()
    phases: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    process_instances: set[str] = set()
    latencies: list[float] = []
    oracle_checked = 0
    oracle_unsupported = 0
    duplicate_event_ids: list[str] = []
    seen_event_ids: set[str] = set()

    for event in events:
        source = f"{event['_source_file']}:{event['_source_line']}"
        missing = sorted(REQUIRED_FIELDS - set(event))
        if missing:
            failures.append({"source": source, "kind": "MISSING_FIELDS", "fields": missing})
        verdict = str(event.get("audit_verdict", ""))
        if verdict not in ALLOWED_VERDICTS:
            failures.append({"source": source, "kind": "INVALID_VERDICT", "value": verdict})
        if event.get("production_behavior_modified") is not False:
            failures.append({"source": source, "kind": "PRODUCTION_BEHAVIOR_MODIFIED"})
        secret_keys = secret_like_keys(event)
        if secret_keys:
            failures.append({"source": source, "kind": "SECRET_LIKE_KEYS", "keys": secret_keys})

        expected, oracle_reason = independent_expected(event)
        if expected is None:
            oracle_unsupported += 1
        else:
            oracle_checked += 1
            if verdict != expected:
                failures.append(
                    {
                        "source": source,
                        "kind": "INDEPENDENT_ORACLE_MISMATCH",
                        "expected": expected,
                        "actual": verdict,
                        "reason": oracle_reason,
                    }
                )

        event_id = str(event.get("event_id", ""))
        if event_id:
            if event_id in seen_event_ids:
                duplicate_event_ids.append(event_id)
            seen_event_ids.add(event_id)
        verdicts[verdict] += 1
        paths[str(event.get("path_id", ""))] += 1
        phases[str(event.get("phase", ""))] += 1
        reasons[str(event.get("audit_reason", ""))] += 1
        process_instances.add(str(event.get("process_instance_id", "")))
        latency = event.get("audit_hook_latency_us")
        if isinstance(latency, (int, float)):
            latencies.append(float(latency))

    if duplicate_event_ids:
        failures.append(
            {
                "kind": "DUPLICATE_EVENT_ID",
                "event_ids": sorted(set(duplicate_event_ids)),
            }
        )

    result = {
        "schema_version": "maintenance.p1.live_audit_replay.v1",
        "independent_of_production_evaluator": True,
        "production_evaluator_imported": False,
        "input_files": [str(path) for path in args.event],
        "event_count": len(events),
        "parse_error_count": len(parse_errors),
        "parse_errors": parse_errors,
        "path_counts": dict(sorted(paths.items())),
        "phase_counts": dict(sorted(phases.items())),
        "verdict_counts": dict(sorted(verdicts.items())),
        "reason_counts": dict(sorted(reasons.items())),
        "unknown_rate": round(verdicts["UNKNOWN"] / len(events), 6) if events else None,
        "process_instance_count": len(process_instances - {""}),
        "oracle_checked_count": oracle_checked,
        "oracle_unsupported_count": oracle_unsupported,
        "oracle_scope": "snapshot unavailable or UNKNOWN must remain UNKNOWN",
        "latency_us": {
            "count": len(latencies),
            "median": round(statistics.median(latencies), 3) if latencies else None,
            "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99),
            "max": round(max(latencies), 3) if latencies else None,
        },
        "production_behavior_modified_count": sum(event.get("production_behavior_modified") is not False for event in events),
        "duplicate_event_id_count": len(duplicate_event_ids),
        "failure_count": len(failures) + len(parse_errors),
        "failures": failures,
        "result": "PASS" if not failures and not parse_errors and events else "FAIL",
    }
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
