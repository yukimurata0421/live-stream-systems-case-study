from __future__ import annotations

import json
from datetime import datetime

try:
    from ..youtube_watchdog_config import OAuthProbeResult, STATS_FILE
except ImportError:
    from youtube_watchdog_config import OAuthProbeResult, STATS_FILE


def parse_iso_ts(raw: str) -> int:
    text = (raw or "").strip()
    if not text:
        return 0
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return int(datetime.fromisoformat(text).timestamp())
    except Exception:
        return 0


def load_last_watchdog_stats() -> dict:
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
            if isinstance(payload, dict):
                return payload
    except Exception:
        pass
    return {}


def oauth_from_stats_cache(payload: dict, now_ts: int, max_age_sec: int) -> OAuthProbeResult | None:
    if max_age_sec <= 0 or not isinstance(payload, dict):
        return None
    if "oauth_probe_ok" not in payload:
        return None
    ts = parse_iso_ts(str(payload.get("oauth_checked_ts_utc", "")).strip())
    if ts <= 0:
        ts = parse_iso_ts(str(payload.get("ts_utc", "")))
    if ts <= 0 or (now_ts - ts) > max_age_sec:
        return None

    def optional_bool(key: str) -> bool | None:
        if key not in payload or payload.get(key) is None:
            return None
        return bool(payload.get(key))

    return OAuthProbeResult(
        enabled=bool(payload.get("oauth_enabled", False)),
        configured=bool(payload.get("oauth_configured", False)),
        probe_ok=bool(payload.get("oauth_probe_ok", False)),
        healthy=bool(payload.get("oauth_healthy", False)),
        reason=f"{str(payload.get('oauth_reason', '')).strip() or 'oauth stats cache'}; reused watchdog stats cache",
        mode="stats_cache",
        life_cycle_status=str(payload.get("oauth_life_cycle_status", "")).strip(),
        broadcast_id=str(payload.get("oauth_broadcast_id", "")).strip(),
        video_id=str(payload.get("oauth_video_id", "")).strip(),
        channel_id=str(payload.get("oauth_channel_id", "")).strip(),
        bound_stream_id=str(payload.get("oauth_bound_stream_id", "")).strip(),
        stream_status=str(payload.get("oauth_stream_status", "")).strip(),
        stream_health_status=str(payload.get("oauth_stream_health_status", "")).strip(),
        stream_health_issues=int(payload.get("oauth_stream_health_issues", 0) or 0),
        stream_status_required=False,
        remote_checked=True,
        enable_auto_start=optional_bool("oauth_enable_auto_start"),
        enable_auto_stop=optional_bool("oauth_enable_auto_stop"),
        monitor_stream_enabled=optional_bool("oauth_monitor_stream_enabled"),
    )


def data_api_from_stats_cache(
    payload: dict,
    *,
    now_ts: int,
    max_age_sec: int,
    selected_video_id: str,
) -> tuple[bool, bool, str, str]:
    if max_age_sec <= 0 or not isinstance(payload, dict):
        return False, False, "", ""
    if "api_live_state" not in payload:
        return False, False, "", ""
    ts = parse_iso_ts(str(payload.get("data_api_checked_ts_utc", "")).strip())
    if ts <= 0:
        ts = parse_iso_ts(str(payload.get("ts_utc", "")))
    if ts <= 0 or (now_ts - ts) > max_age_sec:
        return False, False, "", ""
    cached_video_id = str(payload.get("video_id", "")).strip()
    if not selected_video_id or not cached_video_id or cached_video_id != selected_video_id:
        return False, False, "", ""
    return (
        True,
        bool(payload.get("api_ok", False)),
        str(payload.get("api_reason", "")).strip() or "data api stats cache",
        str(payload.get("api_live_state", "")).strip() or "unknown",
    )
