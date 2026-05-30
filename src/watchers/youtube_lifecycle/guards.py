from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GuardResult:
    allowed: bool
    reason: str


def destructive_action_guard(
    *,
    action: str,
    quota_guard_active: bool = False,
    oauth_channel_match: bool | None = None,
    has_fresh_evidence: bool = True,
    consistency_window_ok: bool = True,
) -> GuardResult:
    if quota_guard_active and action in {"force_transition_live", "create_replacement_broadcast"}:
        return GuardResult(False, "quota guard active")
    if oauth_channel_match is False:
        return GuardResult(False, "oauth channel mismatch")
    if not has_fresh_evidence:
        return GuardResult(False, "fresh evidence missing")
    if not consistency_window_ok:
        return GuardResult(False, "consistency window not satisfied")
    return GuardResult(True, "allowed")


def force_live_precheck(*, feature_enabled: bool, quota_guard_active: bool = False) -> GuardResult:
    if not feature_enabled:
        return GuardResult(False, "feature disabled")
    return destructive_action_guard(action="force_transition_live", quota_guard_active=quota_guard_active)
