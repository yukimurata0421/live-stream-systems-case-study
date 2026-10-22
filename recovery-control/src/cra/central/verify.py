"""Post-hoc verification of agent audit logs against the invariants and the published policies.

Violations mean an invariant was broken and must be investigated. The summary is the evidence
used to decide whether to move from dry_run to enforce.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from cra.policy import BUILTIN
from cra.recovery import after_request, sending_progress

SILENT_AFTER = timedelta(minutes=25)  # two missed heartbeats (600 s) plus slack
RESULT_GRACE = timedelta(minutes=5)


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts).astimezone(UTC)


def verify(
    records: list[dict[str, Any]],
    policies: dict[int, dict[str, Any]],
    *,
    now: datetime,
    expected_agents: set[str] | None = None,
) -> dict[str, Any]:
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
        decisions: Counter[str] = Counter()
        for record in stream:
            kind, payload = record["kind"], record["payload"]
            last_seen[agent_id] = max(last_seen.get(agent_id, _parse(record["ts"])), _parse(record["ts"]))
            if kind == "decision":
                decisions[f"{payload.get('outcome')}:{payload.get('reason')}"] += 1
            elif kind == "action_reserved":
                reserved[payload["action_id"]] = record
            elif kind in ("action_result", "action_reconciled"):
                results[payload["action_id"]] = payload
            elif kind == "action_verified":
                verified[payload.get("result", "")] += 1
                if payload.get("result") in {"DELIVERY_RESUMED", "DELIVERY_RESUMED_EXECUTION_UNKNOWN", "RECOVERED"}:
                    action = reserved.get(payload["action_id"], {}).get("payload", {})
                    result = results.get(payload["action_id"], {})
                    response = result.get("response", {})
                    # Historical RECOVERED labels remain readable, without requiring the
                    # frozen runtime successor extension. New labels verify observed progress.
                    if payload["result"] == "RECOVERED":
                        valid = result.get("state") == "EFFECT_OBSERVED" and after_request(
                            {
                                "generation_key": payload.get("observed_generation_key"),
                                "executor_instance_id": payload.get("executor_instance_id"),
                                "observed_at": payload.get("observed_at"),
                                "uptime_sec": payload.get("uptime_sec"),
                            },
                            response,
                        )
                    else:
                        version = action.get("policy_version")
                        recovery_policy = policies.get(version, {})
                        first, second = payload.get("progress_from") or {}, payload.get("sample") or {}
                        expected = "EFFECT_OBSERVED" if payload["result"] == "DELIVERY_RESUMED" else "OUTCOME_UNKNOWN"
                        valid = (
                            action.get("mode") == "enforce"
                            and result.get("state") == expected
                            and payload.get("execution_state") == expected
                            and action.get("generation_key")
                            == (f"{response.get('agent_pod_uid')}/{response.get('agent_previous_generation')}")
                            and after_request(first, response)
                            and after_request(second, response)
                            and all(second.get(k) == response.get(f"agent_{k}") for k in ("delivery_route", "delivery_route_generation"))
                            and response.get("agent_verify_window_sec") == recovery_policy.get("verify_window_sec")
                            and response.get("agent_stall_lastsnd_ms") == recovery_policy.get("stall_lastsnd_ms")
                            and sending_progress(
                                first,
                                second,
                                max_gap_sec=response.get("agent_max_sample_gap_sec", 5),
                                lastsnd_ms=recovery_policy.get("stall_lastsnd_ms", BUILTIN.stall_lastsnd_ms),
                            )
                        )
                    if not valid:
                        violation("RECOVERY_EVIDENCE_INVALID", record)
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

    # Action limits belong to an agent and mode, including across DB instance changes.
    generations: Counter[tuple[str, str, str]] = Counter()
    times: dict[tuple[str, str], list[datetime]] = defaultdict(list)
    for record in sorted(records, key=lambda r: _parse(r["ts"])):
        if record["kind"] != "action_reserved":
            continue
        payload = record["payload"]
        mode = payload.get("mode", "")
        key = (record["agent_id"], mode)
        generation = (*key, payload.get("generation_key", ""))
        generations[generation] += 1
        if generations[generation] > 1:
            violation("I1_DUPLICATE_GENERATION_ACTION", record, generation[-1])
        version = payload.get("policy_version")
        policy = policies.get(version, BUILTIN.to_dict() if version == BUILTIN.version else None)
        if policy is None:
            violation("I8_UNKNOWN_POLICY_VERSION", record, str(version))
        elif mode == "enforce" and policy.get("mode") != "enforce":
            violation("I7_ENFORCE_WITHOUT_ENFORCE_POLICY", record, f"policy {version} mode={policy.get('mode')}")
        network = (payload.get("evidence") or {}).get("network") or {}
        if network.get("network_down") is not False or network.get("ingest_ok") is not True or network.get("timed_out") is True:
            violation("I4_ACTION_WITHOUT_LOCALIZED_STALL", record, str(network))
        at = _parse(record["ts"])
        previous = times[key]
        if policy is not None:
            if previous and (at - previous[-1]).total_seconds() < policy["cooldown_sec"]:
                violation("I6_COOLDOWN", record)
            if (
                sum(at - t <= timedelta(hours=1) for t in previous) >= policy["max_per_hour"]
                or sum(at - t <= timedelta(days=1) for t in previous) >= policy["max_per_day"]
            ):
                violation("I6_LIMIT", record)
        previous.append(at)

    if not records:
        violations.append({"code": "AUDIT_EMPTY", "detail": "no observed audit records"})
    for agent_id in (expected_agents or set()) - last_seen.keys():
        violations.append({"code": "AGENT_UNOBSERVED", "agent_id": agent_id, "detail": "no audit received"})
    for agent_id, seen in last_seen.items():
        if now - seen > SILENT_AFTER:
            violations.append({"code": "AGENT_SILENT", "agent_id": agent_id, "seq": None, "ts": seen.isoformat(), "detail": ""})

    return {"checked_at": now.isoformat(), "violations": violations, "summary": summary}
