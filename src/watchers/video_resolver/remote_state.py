from __future__ import annotations


def has_recent_remote_ended(
    stats: dict,
    now_ts: int,
    *,
    max_age_sec: int,
    parse_ts,
) -> tuple[bool, str]:
    if not isinstance(stats, dict):
        return False, "watchdog stats unavailable"
    ts = parse_ts(str(stats.get("ts_utc", "")))
    if ts <= 0:
        return False, "watchdog stats timestamp missing"
    age_sec = now_ts - ts
    if age_sec > max_age_sec:
        return False, f"watchdog stats stale ({age_sec}s)"

    ingest_connected = stats.get("ingest_connected")
    selected_video_id = str(stats.get("video_id", "")).strip()
    oauth_video_id = str(stats.get("oauth_video_id", "")).strip()
    api_live_state = str(stats.get("api_live_state", "")).strip().lower()
    oauth_lifecycle = str(stats.get("oauth_life_cycle_status", "")).strip().lower()
    watch_reason = str(stats.get("watch_reason", "")).strip().lower()
    api_ended = api_live_state == "ended"
    oauth_complete = oauth_lifecycle == "complete"
    if ingest_connected is True and api_ended and oauth_complete:
        if not selected_video_id or not oauth_video_id:
            return (
                False,
                "remote ended signal uncorrelated "
                f"(missing id; api={api_live_state or '-'} lifecycle={oauth_lifecycle or '-'})",
            )
        if selected_video_id != oauth_video_id:
            return (
                False,
                "remote ended signal id mismatch "
                f"(selected={selected_video_id} oauth={oauth_video_id}; "
                f"api={api_live_state or '-'} lifecycle={oauth_lifecycle or '-'})",
            )
        if "live marker detected" in watch_reason:
            return False, "remote ended signal contradicted by watch page live marker"
        return (
            True,
            "remote ended while ingest connected "
            f"(video_id={selected_video_id}, api={api_live_state or '-'} lifecycle={oauth_lifecycle or '-'})",
        )
    return False, "remote ended signal not present"
