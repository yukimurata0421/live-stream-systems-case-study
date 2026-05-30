from __future__ import annotations

from typing import Any, Callable

API_REMOTE_SOURCES = {
    "data_api",
    "data_api_oauth",
    "data_api_search",
    "data_api_videos",
    "oauth",
    "oauth_api",
    "oauth_livebroadcasts",
    "oauth_livestreams",
    "oauth_probe",
    "search.list",
    "videos.list",
    "livebroadcasts.list",
    "livestreams.list",
    "youtube_api",
}


def remote_probe_epoch(payload: dict[str, Any], parse_iso_ts: Callable[[str], int]) -> int:
    remote_probe_ts = parse_iso_ts(str(payload.get("remote_probe_ts_utc", "")).strip())
    if remote_probe_ts > 0:
        return remote_probe_ts
    checked_timestamps = [
        parse_iso_ts(str(payload.get("oauth_checked_ts_utc", "")).strip()),
        parse_iso_ts(str(payload.get("data_api_checked_ts_utc", "")).strip()),
    ]
    checked_timestamps = [ts for ts in checked_timestamps if ts > 0]
    if checked_timestamps:
        return max(checked_timestamps)
    return parse_iso_ts(str(payload.get("ts_utc", "")).strip())


def remote_warning_sample_key(payload: dict[str, Any], parse_iso_ts: Callable[[str], int]) -> str:
    sample_id = str(payload.get("remote_sample_id", "") or "").strip()
    if sample_id:
        return f"id:{sample_id}"
    remote_probe_ts = remote_probe_epoch(payload, parse_iso_ts)
    if remote_probe_ts > 0:
        source = str(payload.get("remote_sample_source", "") or payload.get("remote_probe_source", "") or payload.get("remote_source", "") or "").strip()
        episode_id = str(payload.get("recovery_episode_id", "") or "").strip()
        ffmpeg_generation = str(payload.get("ffmpeg_generation", "") or "").strip()
        return f"probe:{remote_probe_ts}:{source}:{episode_id}:{ffmpeg_generation}"
    stats_ts = parse_iso_ts(str(payload.get("ts_utc", "")).strip())
    return f"stats:{stats_ts}" if stats_ts > 0 else ""


def remote_warning_context(payload: dict[str, Any]) -> tuple[str, str, str]:
    episode_id = str(payload.get("recovery_episode_id", "") or "").strip()
    ffmpeg_generation = str(payload.get("ffmpeg_generation", "") or "").strip()
    context_key = f"episode={episode_id}|ffmpeg_generation={ffmpeg_generation}"
    return context_key, episode_id, ffmpeg_generation


def normalize_remote_source(raw_source: str) -> str:
    source = (raw_source or "").strip().lower()
    if not source:
        return ""
    aliases = {
        "oauth_livebroadcasts": "oauth_api",
        "oauth_livestreams": "oauth_api",
        "oauth_probe": "oauth_api",
        "data_api_search": "data_api",
        "data_api_videos": "data_api",
        "search.list": "data_api",
        "videos.list": "data_api",
        "livebroadcasts.list": "oauth_api",
        "livestreams.list": "oauth_api",
    }
    return aliases.get(source, source)


def is_api_remote_source(source: str) -> bool:
    normalized = normalize_remote_source(source)
    return normalized in {"data_api", "oauth_api", "data_api_oauth", "youtube_api"} or source in API_REMOTE_SOURCES


def quota_guard_active_from_state(path, now_ts: int) -> tuple[bool, str]:
    try:
        import json

        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False, "quota state unavailable"
    if not isinstance(payload, dict):
        return False, "quota state invalid"
    if not bool(payload.get("quota_exhausted", False)):
        return False, "quota state inactive"
    until_ts = int(payload.get("quota_exhausted_until_ts", 0) or 0)
    if until_ts > 0 and now_ts >= until_ts:
        return False, "quota state expired"
    source = str(payload.get("quota_exhausted_source", "")).strip() or "unknown"
    return True, f"quota state active (source={source})"


def read_youtube_live_warning(
    *,
    stats_path,
    quota_state_path,
    now_ts: int,
    last_restart_ts: int,
    url_preservation_mode: bool,
    status_max_age_sec: int,
    require_local_ok: bool,
    live_like_lifecycle: set[str],
    parse_iso_ts: Callable[[str], int],
) -> tuple[bool, str, dict[str, Any]]:
    if not url_preservation_mode:
        return False, "url preservation mode disabled", {}
    try:
        import json

        payload = json.loads(stats_path.read_text(encoding="utf-8"))
    except Exception:
        return False, "youtube stats unavailable", {}
    if not isinstance(payload, dict):
        return False, "youtube stats format invalid", {}

    stats_ts = parse_iso_ts(str(payload.get("ts_utc", "")))
    remote_probe_ts = remote_probe_epoch(payload, parse_iso_ts)
    if stats_ts <= 0:
        return False, "youtube stats timestamp missing", payload
    if remote_probe_ts <= 0:
        return False, "youtube remote probe timestamp missing", payload
    if last_restart_ts > 0 and remote_probe_ts <= last_restart_ts:
        return (
            False,
            f"youtube remote probe older than last restart ({remote_probe_ts}<={last_restart_ts})",
            payload,
        )
    age_sec = now_ts - remote_probe_ts
    if age_sec > status_max_age_sec:
        return False, f"youtube remote probe stale ({age_sec}s)", payload

    if require_local_ok:
        local_ok = payload.get("local_ok")
        stream_active = payload.get("stream_active")
        ingest_connected = payload.get("ingest_connected")
        try:
            ffmpeg_pid = int(payload.get("ffmpeg_pid", 0) or 0)
        except (TypeError, ValueError):
            ffmpeg_pid = 0
        if local_ok is not True or stream_active is not True or ingest_connected is not True or ffmpeg_pid <= 1:
            return (
                False,
                (
                    "youtube remote warning ignored until local ingest re-established "
                    f"(local_ok={local_ok} stream_active={stream_active} "
                    f"ingest_connected={ingest_connected} ffmpeg_pid={ffmpeg_pid})"
                ),
                payload,
            )

    remote_source = str(payload.get("remote_source", "")).strip().lower()
    remote_status = str(payload.get("remote_status", "")).strip().lower()
    remote_reason = str(payload.get("remote_reason", "")).strip()
    payload_quota_guard_active = bool(payload.get("quota_guard_active", False))
    state_quota_guard_active, state_quota_guard_reason = quota_guard_active_from_state(quota_state_path, now_ts)
    quota_guard_active = payload_quota_guard_active or state_quota_guard_active
    if remote_status == "warning":
        source = remote_source or "unknown"
        if quota_guard_active and is_api_remote_source(source):
            return (
                False,
                f"youtube watchdog quota guard active (suppressed source={source}; {state_quota_guard_reason})",
                payload,
            )
        return True, (remote_reason or f"remote warning source={source}"), payload

    lifecycle = str(payload.get("oauth_life_cycle_status", "")).strip()
    stream_status = str(payload.get("oauth_stream_status", "")).strip().lower()
    stream_health = str(payload.get("oauth_stream_health_status", "")).strip().lower()
    api_live_state = str(payload.get("api_live_state", "")).strip()

    broadcast_live = (api_live_state == "live") or (lifecycle in live_like_lifecycle)
    if not broadcast_live:
        return False, f"broadcast not live-like (api={api_live_state or '-'} lifecycle={lifecycle or '-'})", payload

    reasons: list[str] = []
    if stream_status == "inactive":
        reasons.append("streamStatus=inactive")
    if stream_health == "nodata":
        reasons.append("healthStatus=noData")
    if reasons:
        source = remote_source or "data_api_oauth"
        if quota_guard_active and is_api_remote_source(source):
            return (
                False,
                f"youtube watchdog quota guard active (suppressed source={source}; {state_quota_guard_reason})",
                payload,
            )
        return True, ", ".join(reasons), {**payload, "remote_source": source}

    return False, "youtube stream status looks normal", payload


def update_remote_warning_streak(
    state: dict[str, Any],
    remote_warning: bool,
    ytw_payload: dict[str, Any],
    *,
    confirm_distinct_stats: bool,
    parse_iso_ts: Callable[[str], int],
) -> int:
    if not remote_warning:
        state["remote_warning_streak"] = 0
        state["remote_warning_last_stats_ts"] = 0
        state["remote_warning_last_sample_key"] = ""
        state["remote_warning_last_probe_ts"] = 0
        state["remote_warning_context_key"] = ""
        state["remote_warning_recovery_episode_id"] = ""
        state["remote_warning_ffmpeg_generation"] = ""
        return 0

    if not confirm_distinct_stats:
        streak = int(state.get("remote_warning_streak", 0) or 0) + 1
        state["remote_warning_streak"] = streak
        return streak

    if not isinstance(ytw_payload, dict):
        state["remote_warning_streak"] = 0
        return 0
    local_ok = ytw_payload.get("local_ok") is True
    stream_active = ytw_payload.get("stream_active") is True
    ingest_connected = ytw_payload.get("ingest_connected") is True
    try:
        ffmpeg_pid = int(ytw_payload.get("ffmpeg_pid", 0) or 0)
    except (TypeError, ValueError):
        ffmpeg_pid = 0
    if not (local_ok and stream_active and ingest_connected and ffmpeg_pid > 1):
        state["remote_warning_streak"] = 0
        return 0

    context_key, episode_id, ffmpeg_generation = remote_warning_context(ytw_payload)
    last_context_key = str(state.get("remote_warning_context_key", "") or "")
    if last_context_key and context_key != last_context_key:
        state["remote_warning_streak"] = 0
        state["remote_warning_last_sample_key"] = ""
        state["remote_warning_last_stats_ts"] = 0
        state["remote_warning_last_probe_ts"] = 0

    stats_ts = parse_iso_ts(str(ytw_payload.get("ts_utc", ""))) if isinstance(ytw_payload, dict) else 0
    sample_key = remote_warning_sample_key(ytw_payload, parse_iso_ts) if isinstance(ytw_payload, dict) else ""
    last_sample_key = str(state.get("remote_warning_last_sample_key", "") or "")
    if sample_key and sample_key == last_sample_key:
        return int(state.get("remote_warning_streak", 0) or 0)
    last_stats_ts = int(state.get("remote_warning_last_stats_ts", 0) or 0)
    if not sample_key and stats_ts > 0 and stats_ts == last_stats_ts:
        return int(state.get("remote_warning_streak", 0) or 0)

    streak = int(state.get("remote_warning_streak", 0) or 0) + 1
    state["remote_warning_streak"] = streak
    if stats_ts > 0:
        state["remote_warning_last_stats_ts"] = stats_ts
    if sample_key:
        state["remote_warning_last_sample_key"] = sample_key
    state["remote_warning_context_key"] = context_key
    state["remote_warning_recovery_episode_id"] = episode_id
    state["remote_warning_ffmpeg_generation"] = ffmpeg_generation
    remote_probe_ts = remote_probe_epoch(ytw_payload, parse_iso_ts) if isinstance(ytw_payload, dict) else 0
    if remote_probe_ts > 0:
        state["remote_warning_last_probe_ts"] = remote_probe_ts
    return streak
