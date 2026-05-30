from __future__ import annotations

from dataclasses import dataclass

from . import actions
from .signals import LocalDeliverySignals


@dataclass(frozen=True)
class LocalDeliveryDecision:
    state: str
    confidence: str
    evidence: list[str]
    recommended_action: str
    blocked_by: list[str]
    caused_by: list[str]
    affects: list[str]


def decide(signals: LocalDeliverySignals, *, has_any_input: bool) -> LocalDeliveryDecision:
    failures = signals.failure_names()
    if signals.has_healthy_signal and not failures:
        confidence = "high" if signals.ingest_connected and signals.runtime_fresh else "medium"
        return LocalDeliveryDecision("healthy", confidence, signals.healthy_evidence_names(), actions.ACTION_NONE, [], [], [])

    if actions.FAILURE_TCP_STALL in failures:
        return LocalDeliveryDecision("recovering", "medium", failures, actions.ACTION_RESTART_FFMPEG, [actions.BLOCK_REPLACEMENT], [], ["youtube_lifecycle"])

    if actions.FAILURE_STREAM_FFMPEG_DUPLICATE in failures:
        return LocalDeliveryDecision("failed", "high", failures, actions.ACTION_RESTART_STREAM, [actions.BLOCK_REPLACEMENT], [], ["youtube_lifecycle"])

    if actions.FAILURE_FFMPEG_MISSING in failures:
        return LocalDeliveryDecision("failed", "high", failures, actions.ACTION_RESTART_FFMPEG, [actions.BLOCK_REPLACEMENT], [], ["youtube_lifecycle"])

    if actions.FAILURE_INGEST_DISCONNECTED in failures or actions.FAILURE_RUNTIME_HEARTBEAT_STALE in failures:
        return LocalDeliveryDecision("failed", "medium", failures, actions.ACTION_RESTART_FFMPEG, [actions.BLOCK_REPLACEMENT], [], ["youtube_lifecycle"])

    if signals.has_healthy_signal:
        return LocalDeliveryDecision("healthy", "medium", signals.healthy_evidence_names(), actions.ACTION_NONE, [], [], [])

    if has_any_input:
        return LocalDeliveryDecision("unknown", "unknown", [], actions.ACTION_NONE, ["local_delivery_evidence_missing"], [], [])

    return LocalDeliveryDecision("unknown", "unknown", [], actions.ACTION_NONE, ["no_local_delivery_evidence"], [], [])
