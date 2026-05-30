from __future__ import annotations

from dataclasses import dataclass

from . import actions
from .signals import MonitoringSignals


@dataclass(frozen=True)
class MonitoringDecision:
    state: str
    confidence: str
    evidence: list[str]
    recommended_action: str
    blocked_by: list[str]
    caused_by: list[str]
    affects: list[str]


def decide(signals: MonitoringSignals) -> MonitoringDecision:
    names = signals.fresh_names
    stale = signals.stale_names
    failures = signals.failure_names()

    if signals.quota_guard_active or signals.cost_report_degraded:
        evidence = list(names)
        if signals.quota_guard_active:
            evidence.append(actions.FAILURE_QUOTA_GUARD_ACTIVE)
        if signals.cost_report_degraded:
            evidence.append(actions.FAILURE_COST_REPORT_DEGRADED)
        return MonitoringDecision("degraded", "medium", evidence, actions.ACTION_NONE, [actions.BLOCK_YOUTUBE_API], [], ["youtube_lifecycle"])

    if stale:
        state = "unknown" if not names else "degraded"
        confidence = "unknown" if not names else "medium"
        return MonitoringDecision(state, confidence, names, actions.ACTION_NONE, [actions.BLOCK_DESTRUCTIVE, *stale], [], [])

    return MonitoringDecision("healthy", "high", names, actions.ACTION_NONE, [], [], [])
