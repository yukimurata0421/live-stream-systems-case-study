from __future__ import annotations

from dataclasses import dataclass

from . import actions
from .signals import MusicSignals


@dataclass(frozen=True)
class MusicDecision:
    state: str
    confidence: str
    evidence: list[str]
    recommended_action: str
    blocked_by: list[str]
    caused_by: list[str]
    affects: list[str]


def decide(signals: MusicSignals, *, has_any_input: bool) -> MusicDecision:
    failures = signals.failure_names()
    if signals.now_playing_fresh and not failures:
        return MusicDecision("healthy", "high", signals.healthy_evidence_names(), actions.ACTION_NONE, [], [], [])

    if actions.FAILURE_AUDIO_ENERGY_LOW_TRANSITION_GRACE in failures:
        return MusicDecision("recovering", "medium", failures, actions.ACTION_DEFER, [actions.BLOCK_YOUTUBE_LIFECYCLE], [], [])

    if actions.FAILURE_PULSE_SOURCE_MISSING in failures:
        return MusicDecision("failed", "medium", failures, actions.ACTION_REPAIR_PULSE, [actions.BLOCK_YOUTUBE_LIFECYCLE], [], ["local_delivery"])

    if actions.FAILURE_PULSE_ROUTE_ANOMALY in failures:
        return MusicDecision("degraded", "medium", failures, actions.ACTION_REPAIR_PULSE, [actions.BLOCK_YOUTUBE_LIFECYCLE], [], ["local_delivery"])

    if actions.FAILURE_AUDIO_ENERGY_LOW in failures:
        if signals.audio_fail_count < 2:
            return MusicDecision("degraded", "medium", failures, actions.ACTION_NONE, [actions.BLOCK_YOUTUBE_LIFECYCLE], [], ["local_delivery"])
        return MusicDecision("degraded", "medium", failures, actions.ACTION_RESTART_DJ, [actions.BLOCK_YOUTUBE_LIFECYCLE], [], ["local_delivery"])

    if actions.FAILURE_NOW_PLAYING_STALE in failures:
        return MusicDecision("degraded", "medium", failures, actions.ACTION_RESTART_DJ, [actions.BLOCK_YOUTUBE_LIFECYCLE], [], [])

    if signals.now_playing_fresh:
        return MusicDecision("healthy", "medium", signals.healthy_evidence_names(), actions.ACTION_NONE, [], [], [])

    if has_any_input:
        return MusicDecision("unknown", "unknown", [], actions.ACTION_NONE, ["music_evidence_missing"], [], [])

    return MusicDecision("unknown", "unknown", [], actions.ACTION_NONE, ["no_music_evidence"], [], [])
