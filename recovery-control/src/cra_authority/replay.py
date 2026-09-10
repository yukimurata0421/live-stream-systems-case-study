from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ReplayDecision:
    event_id: str
    automatic_command_count: int
    outcome: str
    reason: str


def replay_2026_08_21(fixture: dict[str, Any]) -> list[ReplayDecision]:
    decisions: list[ReplayDecision] = []
    for event in fixture["events"]:
        event_type = str(event["event_type"])
        if event_type == "deployment_restart":
            decisions.append(ReplayDecision(str(event["event_id"]), 0, "BLOCKED", "ACTION_NOT_ALLOWLISTED"))
            continue
        if event_type == "old_target_command":
            decisions.append(ReplayDecision(str(event["event_id"]), 0, "BLOCKED", "STALE_TARGET"))
            continue
        required = (
            event.get("fresh_target_identity"),
            event.get("fresh_confirmed_tcp_stall"),
            event.get("fresh_authorization"),
        )
        if not all(value is True for value in required):
            decisions.append(
                ReplayDecision(
                    str(event["event_id"]),
                    0,
                    "UNKNOWN",
                    "MISSING_EVIDENCE_NO_AUTOMATIC_COMMAND",
                )
            )
        else:
            decisions.append(ReplayDecision(str(event["event_id"]), 1, "ELIGIBLE", "FRESH_EVIDENCE"))
    return decisions
