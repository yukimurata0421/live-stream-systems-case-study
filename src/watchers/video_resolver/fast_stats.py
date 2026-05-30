from __future__ import annotations

from .identity import build_remote_sample_id, ffmpeg_generation_from_runtime, latest_iso_ts


def build_fast_watchdog_stats_payload(
    *,
    enabled: bool,
    fast_mode: bool,
    local_runtime: dict,
    selected_video_id: str,
    selected_source: str,
    search_reason: str,
    oauth,
    api_checked: bool,
    api_ok: bool,
    api_reason: str,
    api_live_state: str,
    data_api_checked_ts_utc: str,
    fast_mode_reason: str,
    recovery_episode_id: str,
    quota_guard_active: bool,
    quota_guard_reason: str,
    api_cost_guard,
    utc_now_func,
) -> dict | None:
    if not enabled or not fast_mode:
        return None
    if not (oauth.remote_checked or data_api_checked_ts_utc):
        return None

    oauth_ok = oauth.probe_ok and oauth.healthy
    local_ok = bool(local_runtime.get("local_ok", False))
    remote_ok = oauth_ok or api_ok
    oauth_checked_ts_utc = utc_now_func() if oauth.remote_checked else ""
    remote_probe_ts_utc = latest_iso_ts(oauth_checked_ts_utc, data_api_checked_ts_utc)
    stream_status = (oauth.stream_status or "").strip().lower()
    stream_health = (oauth.stream_health_status or "").strip().lower()
    broadcast_live_like = api_live_state == "live" or oauth.life_cycle_status in {
        "live",
        "liveStarting",
        "testing",
        "testStarting",
    }

    warning_reasons: list[str] = []
    if broadcast_live_like and stream_status == "inactive":
        warning_reasons.append("streamStatus=inactive")
    if broadcast_live_like and stream_health == "nodata":
        warning_reasons.append("healthStatus=noData")
    if api_checked and api_live_state and api_live_state not in {"live", "skipped"}:
        warning_reasons.append(f"api_live_state={api_live_state}")

    if warning_reasons:
        remote_status = "warning"
        remote_reason = "; ".join(warning_reasons)
    elif remote_ok:
        remote_status = "ok"
        remote_reason = "fast resolver remote evidence healthy"
    else:
        remote_status = "unknown"
        remote_reason = "fast resolver remote evidence inconclusive"

    remote_sources: list[str] = []
    if oauth.remote_checked:
        remote_sources.append("oauth_api")
    if data_api_checked_ts_utc:
        remote_sources.append("data_api_videos")
    if remote_sources == ["oauth_api", "data_api_videos"]:
        remote_source = "data_api_oauth"
    elif remote_sources:
        remote_source = remote_sources[0]
    elif oauth.remote_checked:
        remote_source = "oauth_api"
    elif api_checked:
        remote_source = "data_api_videos"
    else:
        remote_source = "none"
    ffmpeg_generation = ffmpeg_generation_from_runtime(local_runtime)
    remote_sample_id = build_remote_sample_id(
        remote_probe_ts_utc=remote_probe_ts_utc,
        remote_source=remote_source,
        recovery_episode_id=recovery_episode_id,
        ffmpeg_generation=ffmpeg_generation,
        selected_video_id=selected_video_id,
    )

    healthy = local_ok and remote_ok
    if healthy:
        status = "ok"
        judgment = "ok"
        judgment_reason = "resolver_fast_remote_refresh_healthy"
    elif not local_ok:
        status = "warn"
        judgment = "deferred"
        judgment_reason = "resolver_fast_remote_refresh_waiting_local_ingest"
    else:
        status = "warn"
        judgment = "ng"
        judgment_reason = "resolver_fast_remote_refresh_remote_warning"

    return {
        "status": status,
        "healthy": healthy,
        "judgment": judgment,
        "judgment_reason": judgment_reason,
        "video_id": selected_video_id,
        "search_reason": search_reason,
        "stream_active": bool(local_runtime.get("stream_active", False)),
        "ffmpeg_pid": int(local_runtime.get("ffmpeg_pid", 0) or 0),
        "ffmpeg_uptime_sec": int(local_runtime.get("ffmpeg_uptime_sec", 0) or 0),
        "ingest_connected": bool(local_runtime.get("ingest_connected", False)),
        "ingest_connection": str(local_runtime.get("ingest_connection", "")).strip(),
        "local_ok": local_ok,
        "oauth_ok": oauth_ok,
        "api_ok": api_ok,
        "availability_ok": healthy,
        "public_ok": remote_ok,
        "health_source": "resolver_fast_remote_refresh",
        "legacy_healthy": api_ok,
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
        "api_reason": api_reason,
        "api_live_state": api_live_state,
        "remote_probe_ts_utc": remote_probe_ts_utc,
        "remote_sample_id": remote_sample_id,
        "remote_sample_source": remote_source,
        "remote_probe_source": remote_source,
        "remote_source": remote_source,
        "remote_status": remote_status,
        "remote_reason": remote_reason,
        "recovery_episode_id": recovery_episode_id,
        "ffmpeg_generation": ffmpeg_generation,
        "resolver_source": selected_source,
        "resolver_fast_mode": fast_mode,
        "resolver_fast_mode_reason": fast_mode_reason,
        "quota_guard_active": quota_guard_active,
        "quota_guard_reason": quota_guard_reason,
        "api_cost_burn_rate_active": api_cost_guard.active,
        "api_cost_burn_rate_reason": api_cost_guard.reason,
        "api_cost_projected_units_per_day": api_cost_guard.projected_units_per_day,
        "api_cost_threshold_units_per_day": api_cost_guard.threshold_units_per_day,
        "action": "none",
    }
