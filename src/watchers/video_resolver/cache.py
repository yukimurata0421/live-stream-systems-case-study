from __future__ import annotations

from .identity import parse_iso_ts


def optional_bool(stats: dict, key: str) -> bool | None:
    if key not in stats or stats.get(key) is None:
        return None
    return bool(stats.get(key))


def oauth_from_watchdog_stats_cache(stats: dict, now_ts: int, max_age_sec: int, oauth_result_cls):
    if max_age_sec <= 0 or not isinstance(stats, dict):
        return None
    ts = parse_iso_ts(str(stats.get("oauth_checked_ts_utc", "")).strip())
    if ts <= 0:
        ts = parse_iso_ts(str(stats.get("ts_utc", "")))
    if ts <= 0 or (now_ts - ts) > max_age_sec:
        return None
    if "oauth_probe_ok" not in stats:
        return None

    return oauth_result_cls(
        enabled=bool(stats.get("oauth_enabled", False)),
        configured=bool(stats.get("oauth_configured", False)),
        probe_ok=bool(stats.get("oauth_probe_ok", False)),
        healthy=bool(stats.get("oauth_healthy", False)),
        reason=f"{str(stats.get('oauth_reason', '')).strip() or 'oauth stats cache'}; reused watchdog stats cache",
        mode="stats_cache",
        life_cycle_status=str(stats.get("oauth_life_cycle_status", "")).strip(),
        broadcast_id=str(stats.get("oauth_broadcast_id", "")).strip(),
        video_id=str(stats.get("oauth_video_id", "")).strip(),
        channel_id=str(stats.get("oauth_channel_id", "")).strip(),
        bound_stream_id=str(stats.get("oauth_bound_stream_id", "")).strip(),
        stream_status=str(stats.get("oauth_stream_status", "")).strip(),
        stream_health_status=str(stats.get("oauth_stream_health_status", "")).strip(),
        stream_health_issues=int(stats.get("oauth_stream_health_issues", 0) or 0),
        stream_status_required=False,
        remote_checked=True,
        enable_auto_start=optional_bool(stats, "oauth_enable_auto_start"),
        enable_auto_stop=optional_bool(stats, "oauth_enable_auto_stop"),
        monitor_stream_enabled=optional_bool(stats, "oauth_monitor_stream_enabled"),
    )


def data_api_from_watchdog_stats_cache(
    stats: dict,
    *,
    now_ts: int,
    max_age_sec: int,
    selected_video_id: str,
) -> tuple[bool, str, str]:
    if max_age_sec <= 0 or not isinstance(stats, dict):
        return False, "", ""
    ts = parse_iso_ts(str(stats.get("data_api_checked_ts_utc", "")).strip())
    if ts <= 0:
        ts = parse_iso_ts(str(stats.get("ts_utc", "")))
    if ts <= 0 or (now_ts - ts) > max_age_sec:
        return False, "", ""
    cached_video_id = str(stats.get("video_id", "")).strip()
    if not cached_video_id or cached_video_id != selected_video_id:
        return False, "", ""
    return (
        True,
        str(stats.get("api_reason", "")).strip() or "data api stats cache",
        str(stats.get("api_live_state", "")).strip() or "unknown",
    )
