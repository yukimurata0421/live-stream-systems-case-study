"""Post-hoc verification of agent audit logs against the invariants and the published policies.

Violations mean an invariant was broken and must be investigated. The summary is the evidence
used to decide whether to move from dry_run to enforce.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

SILENT_AFTER = timedelta(minutes=25)  # two missed heartbeats (600 s) plus slack
RESULT_GRACE = timedelta(minutes=5)


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts).astimezone(UTC)


def verify(records: list[dict[str, Any]], policies: dict[int, dict[str, Any]], *, now: datetime) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}

    def violation(code: str, record: dict[str, Any], detail: str = "") -> None:
        violations.append({"code": code, "agent_id": record["agent_id"], "seq": record["seq"], "ts": record["ts"], "detail": detail})

    streams: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        streams[(record["agent_id"], record["instance_id"])].append(record)

    last_seen: dict[str, datetime] = {}
    for (agent_id, _instance), stream in streams.items():
        reserved: dict[int, dict[str, Any]] = {}
        results: dict[int, dict[str, Any]] = {}
        verified: Counter[str] = Counter()
        enforce_by_generation: Counter[str] = Counter()
        enforce_times: list[datetime] = []
        decisions: Counter[str] = Counter()
        for record in stream:
            kind, payload = record["kind"], record["payload"]
            last_seen[agent_id] = max(last_seen.get(agent_id, _parse(record["ts"])), _parse(record["ts"]))
            if kind == "decision":
                decisions[f"{payload.get('outcome')}:{payload.get('reason')}"] += 1
            elif kind == "action_reserved":
                reserved[payload["action_id"]] = record
                if payload.get("mode") != "enforce":
                    continue
                generation = payload.get("generation_key", "")
                enforce_by_generation[generation] += 1
                if enforce_by_generation[generation] > 1:
                    violation("I1_DUPLICATE_GENERATION_ACTION", record, generation)
                policy = policies.get(payload.get("policy_version"))
                if policy is None:
                    violation("I8_UNKNOWN_POLICY_VERSION", record, str(payload.get("policy_version")))
                elif policy.get("mode") != "enforce":
                    violation("I7_ENFORCE_WITHOUT_ENFORCE_POLICY", record, f"policy {policy['version']} mode={policy.get('mode')}")
                network = (payload.get("evidence") or {}).get("network") or {}
                if network.get("network_down") is not False or network.get("ingest_ok") is not True:
                    violation("I4_ACTION_WITHOUT_LOCALIZED_STALL", record, str(network))
                at = _parse(record["ts"])
                if policy is not None:
                    if enforce_times and (at - enforce_times[-1]).total_seconds() < policy["cooldown_sec"]:
                        violation("I6_COOLDOWN", record)
                    hour = [t for t in enforce_times if at - t < timedelta(hours=1)]
                    day = [t for t in enforce_times if at - t < timedelta(days=1)]
                    if len(hour) >= policy["max_per_hour"] or len(day) >= policy["max_per_day"]:
                        violation("I6_LIMIT", record)
                enforce_times.append(at)
            elif kind == "action_result":
                results[payload["action_id"]] = payload
            elif kind == "action_verified":
                verified[payload.get("result", "")] += 1
        for action_id, record in reserved.items():
            if action_id not in results and now - _parse(record["ts"]) > RESULT_GRACE:
                violation("I2_ACTION_WITHOUT_RESULT", record, f"action {action_id}")
        modes = Counter(r["payload"].get("mode") for r in reserved.values())
        states = Counter(p.get("state") for p in results.values())
        summary[f"{agent_id}/{_instance}"] = {
            "records": len(stream),
            "decisions": dict(decisions),
            "actions_by_mode": dict(modes),
            "action_states": dict(states),
            "verification_results": dict(verified),
        }

    for agent_id, seen in last_seen.items():
        if now - seen > SILENT_AFTER:
            violations.append({"code": "AGENT_SILENT", "agent_id": agent_id, "seq": None, "ts": seen.isoformat(), "detail": ""})

    return {"checked_at": now.isoformat(), "violations": violations, "summary": summary}
