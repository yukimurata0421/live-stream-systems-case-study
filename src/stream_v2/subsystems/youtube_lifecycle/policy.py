from __future__ import annotations

from dataclasses import dataclass

from ...model import ReplacementPolicy
from . import actions
from .signals import YouTubeLifecycleSignals


@dataclass(frozen=True)
class YouTubeLifecycleDecision:
    state: str
    confidence: str
    evidence: list[str]
    recommended_action: str
    blocked_by: list[str]
    caused_by: list[str]
    affects: list[str]
    replacement_policy: ReplacementPolicy


def decide(signals: YouTubeLifecycleSignals) -> YouTubeLifecycleDecision:
    replacement_policy = decide_replacement_policy(signals)
    evidence = healthy_evidence(signals)

    if signals.candidate_found:
        return YouTubeLifecycleDecision("degraded" if signals.public_ok else "unknown", "medium", [*evidence, actions.FAILURE_CANDIDATE_NEW_URL_FOUND], actions.ACTION_RESYNC_RESOLVER, [], [], [], replacement_policy)

    if signals.public_ok and signals.remote_stale_ended:
        return YouTubeLifecycleDecision("degraded", "medium", [*evidence, actions.FAILURE_INCONSISTENT_REMOTE], actions.ACTION_RESYNC_RESOLVER, [], [], [], replacement_policy)

    if signals.public_ok and signals.authoritative_live:
        confidence = "high" if signals.api_live and signals.oauth_live else "medium"
        return YouTubeLifecycleDecision("healthy", confidence, evidence, actions.ACTION_NONE, [], [], [], replacement_policy)

    if signals.has_watchdog_input:
        if signals.failure_kind in {"remote_ended", "remote_ended_confirmed"}:
            return YouTubeLifecycleDecision("failed", "medium", [actions.FAILURE_REMOTE_ENDED_CONFIRMED], actions.ACTION_FORCE_CURRENT_BROADCAST_LIVE if signals.current_url_recoverable else actions.ACTION_RETRY_PROBE, [], [], [], replacement_policy)
        return YouTubeLifecycleDecision("degraded", "medium", evidence or [actions.FAILURE_PUBLIC_NOT_LIVE], actions.ACTION_FORCE_CURRENT_BROADCAST_LIVE if signals.current_url_recoverable else actions.ACTION_RETRY_PROBE, [], [], [], replacement_policy)

    return YouTubeLifecycleDecision("unknown", "unknown", [], actions.ACTION_NONE, ["no_youtube_lifecycle_evidence"], [], [], replacement_policy)


def healthy_evidence(signals: YouTubeLifecycleSignals) -> list[str]:
    evidence: list[str] = []
    if signals.expected_identity_match:
        evidence.append("expected_video_id_match")
    if signals.public_ok:
        evidence.append("public_probe_live")
    if signals.api_live:
        evidence.append("data_api_live")
    if signals.oauth_live:
        evidence.append("oauth_broadcast_live")
    if signals.auto_stop_disabled:
        evidence.append("auto_stop_disabled")
    if not signals.quota_guard_active:
        evidence.append("quota_guard_inactive")
    return evidence


def decide_replacement_policy(signals: YouTubeLifecycleSignals) -> ReplacementPolicy:
    required_missing: list[str] = []
    if signals.expected_url_state in {"live", "recoverable"}:
        required_missing.extend(["current_url_unrecoverable", "same_broadcast_recovery_failed"])
        return ReplacementPolicy(False, "expected_url_live_or_recoverable", required_missing)
    if signals.current_url_recoverable:
        required_missing.append("current_url_unrecoverable")
        return ReplacementPolicy(False, "current_url_recoverable", required_missing)
    if signals.quota_guard_active:
        required_missing.append("quota_guard_clear")
        return ReplacementPolicy(False, "quota_guard_active", required_missing)
    if signals.oauth_channel_mismatch:
        required_missing.append("oauth_channel_match")
        return ReplacementPolicy(False, "oauth_channel_mismatch", required_missing)
    required_missing.extend(["same_broadcast_recovery_failed", "manual_or_policy_confirmation"])
    return ReplacementPolicy(False, "replacement_requires_explicit_unrecoverable_confirmation", required_missing)
