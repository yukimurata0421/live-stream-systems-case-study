from __future__ import annotations

from dataclasses import dataclass

from . import actions
from .signals import RenderingSignals


@dataclass(frozen=True)
class RenderingDecision:
    state: str
    confidence: str
    evidence: list[str]
    recommended_action: str
    blocked_by: list[str]
    caused_by: list[str]
    affects: list[str]


def decide(signals: RenderingSignals, *, has_any_input: bool) -> RenderingDecision:
    failures = signals.failure_names()
    if signals.has_healthy_signal and not failures:
        return RenderingDecision("healthy", "high" if signals.watchdog_ok else "medium", signals.healthy_evidence_names(), actions.ACTION_NONE, [], [], [])

    if actions.FAILURE_VIDEO_FRAME_UNHEALTHY in failures:
        return RenderingDecision("failed", "medium", failures, actions.ACTION_RESTART_BROWSER, [actions.BLOCK_YOUTUBE_LIFECYCLE], [], [])

    if any(
        name in failures
        for name in [
            actions.FAILURE_OVERLAY_UNAVAILABLE,
            actions.FAILURE_STREAM1090_UNAVAILABLE,
            actions.FAILURE_UPSTREAM_STREAM1090_UNAVAILABLE,
            actions.FAILURE_ADSB_FRESHNESS_STALL,
        ]
    ):
        return RenderingDecision("failed", "medium", failures, actions.ACTION_RELOAD_OVERLAY, [actions.BLOCK_YOUTUBE_LIFECYCLE], [], [])

    if actions.FAILURE_RUNTIME_SNAPSHOT_STALE in failures:
        return RenderingDecision("degraded", "medium", failures, actions.ACTION_RESTART_BROWSER, [actions.BLOCK_YOUTUBE_LIFECYCLE], ["local_delivery"], [])

    if signals.has_healthy_signal:
        return RenderingDecision("healthy", "medium", signals.healthy_evidence_names(), actions.ACTION_NONE, [], [], [])

    if has_any_input:
        return RenderingDecision("unknown", "unknown", [], actions.ACTION_NONE, ["rendering_evidence_missing"], [], [])

    return RenderingDecision("unknown", "unknown", [], actions.ACTION_NONE, ["no_rendering_evidence"], [], [])
