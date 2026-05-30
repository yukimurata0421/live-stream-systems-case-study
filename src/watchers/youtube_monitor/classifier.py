from __future__ import annotations


def detect_failure_kind(
    *,
    stream_active: bool,
    ingest_connected: bool,
    selected_video_id: str,
    api_live_state: str,
    api_reason: str,
    watch_reason: str,
    oauth_reason: str,
    oauth_life_cycle_status: str,
    oauth_video_id: str,
) -> str:
    api_ended = api_live_state == "ended"
    oauth_complete = oauth_life_cycle_status == "complete"
    selected = (selected_video_id or "").strip()
    oauth_vid = (oauth_video_id or "").strip()
    watch_text = (watch_reason or "").lower()
    watch_contradicts_remote_end = "live marker detected" in watch_text
    remote_ended_correlated = (
        stream_active
        and ingest_connected
        and api_ended
        and oauth_complete
        and bool(selected)
        and bool(oauth_vid)
        and selected == oauth_vid
        and not watch_contradicts_remote_end
    )
    if remote_ended_correlated:
        return "remote_ended"
    if api_live_state == "rate_limited":
        return "transient_net"
    if (not stream_active) or (not ingest_connected):
        return "local_pipeline"

    text = " | ".join((api_reason, watch_reason, oauth_reason)).lower()
    transient_markers = (
        "network is unreachable",
        "temporary failure in name resolution",
        "timed out",
        "name resolution",
        "connection reset",
        "connection refused",
        "rate limit exceeded",
        "ratelimitexceeded",
    )
    if any(marker in text for marker in transient_markers):
        return "transient_net"

    return "unknown"


def detect_transient_subkind(
    *,
    api_live_state: str,
    api_reason: str,
    watch_reason: str,
    oauth_reason: str,
) -> str:
    state = (api_live_state or "").strip().lower()
    text = " | ".join((api_reason, watch_reason, oauth_reason)).lower()
    if state == "rate_limited" or "ratelimitexceeded" in text or "rate limit exceeded" in text:
        return "rate_limited"
    if any(marker in text for marker in ("timed out", "timeout", "temporary failure in name resolution", "network is unreachable")):
        return "network_timeout"
    if "http 5" in text:
        return "api_5xx"
    return "other"

