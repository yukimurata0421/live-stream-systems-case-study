from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .classifier import detect_transient_subkind


def build_status_payload(
    *,
    status: str,
    healthy: bool,
    fail_count: int,
    degraded_public_count: int,
    video_id: str,
    live_url: str,
    search_reason: str,
    watch_reason: str,
    api_reason: str,
    api_live_state: str,
    stream_active: bool,
    ffmpeg_pid: int,
    ffmpeg_uptime_sec: int,
    ingest_connected: bool,
    ingest_connection: str,
    local_ok: bool,
    oauth_ok: bool,
    api_ok: bool,
    availability_ok: bool,
    public_ok: bool,
    health_source: str,
    legacy_healthy: bool,
    oauth: Any,
    incident_stage: str,
    incident_reason: str,
    failure_kind: str,
    action: str,
    enforce_restart: bool,
    watch_page_verdict: str = "",
    failure_subkind: str = "",
    force_live_on_upcoming_once_enabled: bool = False,
    force_live_triggered: bool = False,
    force_live_reason: str = "",
    oauth_checked_ts_utc: str = "",
    data_api_checked_ts_utc: str = "",
    api_cost_burn_rate_active: bool = False,
    api_cost_burn_rate_reason: str = "",
    api_cost_projected_units_per_day: int = 0,
    api_cost_threshold_units_per_day: int = 0,
    evidence_state: str = "",
    evidence_reason: str = "",
    evidence_action: str = "",
    evidence_blocked_by: tuple[str, ...] = (),
) -> dict:
    transient_subkind = failure_subkind
    if failure_kind == "transient_net" and not transient_subkind:
        transient_subkind = detect_transient_subkind(
            api_live_state=api_live_state,
            api_reason=api_reason,
            watch_reason=watch_reason,
            oauth_reason=oauth.reason,
        )
    return {
        "status": status,
        "healthy": healthy,
        "fail_count": fail_count,
        "degraded_public_count": degraded_public_count,
        "video_id": video_id,
        "live_url": live_url,
        "search_reason": search_reason,
        "watch_reason": watch_reason,
        "watch_page_verdict": watch_page_verdict,
        "api_reason": api_reason,
        "api_live_state": api_live_state,
        "stream_active": stream_active,
        "ffmpeg_pid": ffmpeg_pid,
        "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
        "ingest_connected": ingest_connected,
        "ingest_connection": ingest_connection,
        "local_ok": local_ok,
        "oauth_ok": oauth_ok,
        "api_ok": api_ok,
        "availability_ok": availability_ok,
        "public_ok": public_ok,
        "health_source": health_source,
        "legacy_healthy": legacy_healthy,
        "oauth_enabled": oauth.enabled,
        "oauth_configured": oauth.configured,
        "oauth_mode": oauth.mode,
        "oauth_probe_ok": oauth.probe_ok,
        "oauth_healthy": oauth.healthy,
        "oauth_reason": oauth.reason,
        "oauth_broadcast_id": oauth.broadcast_id,
        "oauth_video_id": oauth.video_id,
        "oauth_channel_id": oauth.channel_id,
        "oauth_life_cycle_status": oauth.life_cycle_status,
        "oauth_bound_stream_id": oauth.bound_stream_id,
        "oauth_stream_status": oauth.stream_status,
        "oauth_stream_health_status": oauth.stream_health_status,
        "oauth_stream_health_issues": oauth.stream_health_issues,
        "oauth_enable_auto_start": oauth.enable_auto_start,
        "oauth_enable_auto_stop": oauth.enable_auto_stop,
        "oauth_monitor_stream_enabled": oauth.monitor_stream_enabled,
        "oauth_checked_ts_utc": oauth_checked_ts_utc,
        "data_api_checked_ts_utc": data_api_checked_ts_utc,
        "incident_stage": incident_stage,
        "incident_reason": incident_reason,
        "failure_kind": failure_kind,
        "failure_subkind": transient_subkind,
        "enforce_restart": enforce_restart,
        "force_live_on_upcoming_once_enabled": force_live_on_upcoming_once_enabled,
        "force_live_triggered": force_live_triggered,
        "force_live_reason": force_live_reason,
        "api_cost_burn_rate_active": api_cost_burn_rate_active,
        "api_cost_burn_rate_reason": api_cost_burn_rate_reason,
        "api_cost_projected_units_per_day": api_cost_projected_units_per_day,
        "api_cost_threshold_units_per_day": api_cost_threshold_units_per_day,
        "evidence_state": evidence_state,
        "evidence_reason": evidence_reason,
        "evidence_action": evidence_action,
        "evidence_blocked_by": list(evidence_blocked_by),
        "action": action,
    }


def record_status(
    payload: dict,
    *,
    enrich_status_with_recovery_context: Callable[[dict], dict],
    classify_judgment: Callable[[str, bool], tuple[str, str]],
    write_stats: Callable[[dict], object],
    append_event: Callable[[dict], object],
    should_emit_ok_event: Callable[[], bool],
) -> None:
    payload = dict(payload)
    payload = enrich_status_with_recovery_context(payload)
    judgment, judgment_reason = classify_judgment(
        str(payload.get("status", "")),
        bool(payload.get("healthy", False)),
    )
    payload["judgment"] = judgment
    payload["judgment_reason"] = judgment_reason
    write_stats(payload)
    if judgment in {"ok", "deferred"}:
        if should_emit_ok_event():
            append_event(payload)
        return
    append_event(payload)
