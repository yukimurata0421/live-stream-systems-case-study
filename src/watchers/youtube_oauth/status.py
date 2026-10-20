from __future__ import annotations

import os
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Callable


YOUTUBE_OAUTH_SCOPE = "https://www.googleapis.com/auth/youtube.force-ssl"


def authorization_url(cfg: dict[str, str], *, scope: str = YOUTUBE_OAUTH_SCOPE) -> str:
    client_id = cfg.get("YTW_OAUTH_CLIENT_ID", "").strip()
    if not client_id:
        return ""
    redirect_uri = cfg.get("YTW_OAUTH_REDIRECT_URI", "http://127.0.0.1:8080/").strip() or "http://127.0.0.1:8080/"
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": scope,
        "access_type": "offline",
        "prompt": "consent",
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)


def build_status_payload(
    *,
    cfg: dict[str, str],
    token_state: dict,
    stats: dict,
    now_ts: int,
    token_state_file: Path,
    watchdog_stats_file: Path,
    state_base_dir: Path,
    parse_bool: Callable[[object], bool | None],
    utc_now_text: Callable[[int | None], str],
) -> dict:
    del state_base_dir
    expires_at = int(token_state.get("expires_at", 0) or 0)
    reason = str(stats.get("oauth_reason", "") or "")
    reason_lower = reason.lower()
    invalid_grant = (
        "invalid_grant" in reason_lower
        or "expired or revoked" in reason_lower
        or "test-user restriction" in reason_lower
    )
    enabled = parse_bool(cfg.get("YTW_OAUTH_ENABLE", "0")) is True
    shadow_mode = parse_bool(cfg.get("YTW_OAUTH_SHADOW_MODE", "1")) is not False
    client_id_configured = bool(cfg.get("YTW_OAUTH_CLIENT_ID", "").strip())
    refresh_token_configured = bool(cfg.get("YTW_OAUTH_REFRESH_TOKEN", "").strip())
    client_secret_configured = bool(cfg.get("YTW_OAUTH_CLIENT_SECRET", "").strip())
    if not enabled:
        judgment = "oauth_disabled"
    elif not client_id_configured or not refresh_token_configured:
        judgment = "oauth_not_configured"
    elif bool(stats.get("oauth_probe_ok")) and bool(stats.get("oauth_healthy")):
        judgment = "oauth_control_plane_ok"
    elif invalid_grant:
        judgment = "oauth_refresh_token_invalid"
    else:
        judgment = "oauth_control_plane_degraded"

    actions: list[str] = []
    if judgment == "oauth_refresh_token_invalid":
        actions.extend(
            [
                "Move the Google OAuth consent app from Testing to Production if necessary",
                "Repeat offline consent with the YouTube channel owner account and replace YTW_OAUTH_REFRESH_TOKEN",
                "After replacement, run stream-new oauth-status --probe and check oauth_probe_ok, oauth_stream_status, and enableAutoStop",
                "Monitor recurrence for 24 hours with YTW_OAUTH_SHADOW_MODE=1",
            ]
        )
    elif judgment == "oauth_not_configured":
        actions.append("Set YTW_OAUTH_CLIENT_ID and YTW_OAUTH_REFRESH_TOKEN in the systemd environment")
    elif judgment == "oauth_control_plane_degraded":
        actions.append("Run stream-new oauth-status --probe to inspect actual liveBroadcasts/liveStreams responses")

    return {
        "ts_utc": utc_now_text(now_ts),
        "judgment": judgment,
        "enabled": enabled,
        "shadow_mode": shadow_mode,
        "configured": client_id_configured and refresh_token_configured,
        "client_id_configured": client_id_configured,
        "client_secret_configured": client_secret_configured,
        "refresh_token_configured": refresh_token_configured,
        "token_state_file": str(token_state_file),
        "access_token_cached": bool(str(token_state.get("access_token", "") or "").strip()),
        "access_token_expires_at": expires_at,
        "access_token_expires_in_sec": expires_at - now_ts if expires_at > 0 else None,
        "access_token_expired": expires_at > 0 and expires_at <= now_ts,
        "watchdog_stats_file": str(watchdog_stats_file),
        "stats": {
            "oauth_ok": bool(stats.get("oauth_ok")),
            "oauth_probe_ok": bool(stats.get("oauth_probe_ok")),
            "oauth_healthy": bool(stats.get("oauth_healthy")),
            "oauth_mode": stats.get("oauth_mode", ""),
            "oauth_reason": reason,
            "oauth_checked_ts_utc": stats.get("oauth_checked_ts_utc", ""),
            "oauth_stream_status": stats.get("oauth_stream_status", ""),
            "oauth_stream_health_status": stats.get("oauth_stream_health_status", ""),
            "oauth_stream_health_issues": stats.get("oauth_stream_health_issues", 0),
            "oauth_stream_health_issue_details": stats.get("oauth_stream_health_issue_details", []),
            "oauth_enable_auto_stop": stats.get("oauth_enable_auto_stop", None),
            "api_ok": stats.get("api_ok", None),
            "status": stats.get("status", ""),
            "judgment": stats.get("judgment", ""),
            "health_source": stats.get("health_source", ""),
        },
        "invalid_grant": invalid_grant,
        "authorization_url": authorization_url(cfg),
        "actions": actions,
        "live_probe": None,
    }


def attach_live_probe(payload: dict, *, cfg: dict[str, str], base_dir: Path) -> None:
    for key, value in cfg.items():
        if key.startswith("YTW_") or key.startswith("STREAM_RUNTIME_"):
            os.environ.setdefault(key, value)
    src_dir = str(base_dir / "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    try:
        from watchers.youtube_api import probe_with_oauth  # type: ignore

        probe = probe_with_oauth()
        payload["live_probe"] = {
            "enabled": probe.enabled,
            "configured": probe.configured,
            "probe_ok": probe.probe_ok,
            "healthy": probe.healthy,
            "reason": probe.reason,
            "mode": probe.mode,
            "broadcast_id": probe.broadcast_id,
            "video_id": probe.video_id,
            "channel_id": probe.channel_id,
            "stream_status": probe.stream_status,
            "stream_health_status": probe.stream_health_status,
            "stream_health_issues": probe.stream_health_issues,
            "stream_health_issue_details": list(probe.stream_health_issue_details),
            "enable_auto_stop": probe.enable_auto_stop,
            "remote_checked": probe.remote_checked,
        }
    except Exception as exc:
        payload["live_probe"] = {"probe_ok": False, "reason": f"oauth probe failed: {exc}"}
