from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ...timeutil import age_seconds, parse_utc
from ..common import text, truthy


@dataclass(frozen=True)
class YouTubeLifecycleSignals:
    public_ok: bool
    api_live: bool
    oauth_live: bool
    api_ended: bool
    oauth_complete: bool
    auto_stop_disabled: bool
    current_url_recoverable: bool
    candidate_found: bool
    expected_identity_match: bool
    expected_video_id: str
    candidate_video_id: str
    quota_guard_active: bool
    oauth_channel_mismatch: bool
    oauth_shadow_invalid_grant: bool
    url_preservation_active: bool
    public_probe_age_sec: float | None
    data_api_age_sec: float | None
    oauth_age_sec: float | None
    resolver_age_sec: float | None
    failure_kind: str
    has_watchdog_input: bool
    expected_url_state: str

    @property
    def remote_stale_ended(self) -> bool:
        return self.api_ended or self.oauth_complete

    @property
    def authoritative_live(self) -> bool:
        return self.api_live or self.oauth_live


def collect_signals(*, ytw: dict[str, Any], resolver: dict[str, Any], cost: dict[str, Any], target: dict[str, Any], now: datetime) -> YouTubeLifecycleSignals:
    public_ok = truthy(ytw.get("public_ok") or ytw.get("availability_ok"))
    api_state = text(ytw.get("api_live_state") or resolver.get("api_live_state")).lower()
    oauth_state = text(ytw.get("oauth_life_cycle_status") or resolver.get("oauth_lifecycle")).lower()
    api_live = api_state == "live"
    oauth_live = oauth_state == "live"
    api_ended = api_state == "ended"
    oauth_complete = oauth_state in {"complete", "completed"}
    expected_video_id = text(target.get("expected_video_id", ""))
    candidate_video_id = text(target.get("candidate_video_id", ""))
    selected_video_id = text(target.get("selected_video_id", ""))
    candidate_found = bool(
        truthy(ytw.get("candidate_new_url_found"))
        or truthy(resolver.get("candidate_new_url_found"))
        or (candidate_video_id and candidate_video_id != expected_video_id)
    )
    expected_identity_match = bool(expected_video_id and selected_video_id and expected_video_id == selected_video_id)
    quota_guard_active = truthy(ytw.get("api_cost_burn_rate_active")) or truthy(cost.get("quota_guard_active")) or truthy(resolver.get("quota_guard_active"))
    auto_stop_disabled = ytw.get("oauth_enable_auto_stop") is False
    current_url_recoverable = public_ok or auto_stop_disabled or truthy(ytw.get("force_current_broadcast_live_allowed"))
    if public_ok:
        expected_url_state = "live"
    elif current_url_recoverable:
        expected_url_state = "recoverable"
    elif ytw:
        expected_url_state = "not_live"
    else:
        expected_url_state = "unknown"
    oauth_mode = text(ytw.get("oauth_mode") or resolver.get("oauth_mode")).lower()
    oauth_error = text(ytw.get("oauth_error") or resolver.get("oauth_error")).lower()

    return YouTubeLifecycleSignals(
        public_ok=public_ok,
        api_live=api_live,
        oauth_live=oauth_live,
        api_ended=api_ended,
        oauth_complete=oauth_complete,
        auto_stop_disabled=auto_stop_disabled,
        current_url_recoverable=current_url_recoverable,
        candidate_found=candidate_found,
        expected_identity_match=expected_identity_match,
        expected_video_id=expected_video_id,
        candidate_video_id=candidate_video_id,
        quota_guard_active=quota_guard_active,
        oauth_channel_mismatch=truthy(ytw.get("oauth_channel_mismatch")),
        oauth_shadow_invalid_grant=oauth_mode == "shadow" and "invalid_grant" in oauth_error,
        url_preservation_active=truthy(ytw.get("url_preservation_active")) or truthy(resolver.get("url_preservation_active")),
        public_probe_age_sec=_age(ytw.get("remote_probe_ts_utc") or ytw.get("ts_utc"), now),
        data_api_age_sec=_age(ytw.get("data_api_checked_ts_utc") or resolver.get("data_api_checked_ts_utc"), now),
        oauth_age_sec=_age(ytw.get("oauth_checked_ts_utc") or resolver.get("oauth_checked_ts_utc"), now),
        resolver_age_sec=_age(resolver.get("ts_utc") or resolver.get("updated_at_utc"), now),
        failure_kind=text(ytw.get("failure_kind")),
        has_watchdog_input=bool(ytw),
        expected_url_state=expected_url_state,
    )


def _age(value: Any, now: datetime) -> float | None:
    ts = parse_utc(value)
    return age_seconds(ts, now) if ts else None
