from __future__ import annotations

try:
    from ..youtube_watchdog_config import (
        FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC,
        URL_PRESERVATION_WINDOW_SEC,
    )
    from ..youtube_watchdog_state import (
        append_event,
        classify_judgment,
        load_state,
        load_video_resolver_state,
        log,
        should_emit_ok_event,
        write_stats,
    )
    from ..youtube_monitor import stats_writer
except ImportError:
    from youtube_watchdog_config import (
        FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC,
        URL_PRESERVATION_WINDOW_SEC,
    )
    from youtube_watchdog_state import (
        append_event,
        classify_judgment,
        load_state,
        load_video_resolver_state,
        log,
        should_emit_ok_event,
        write_stats,
    )
    from youtube_monitor import stats_writer


URL_RECOVERY_FIELD_DEFAULTS = {
    "url_recovery_phase": "healthy",
    "url_recovery_first_ts": 0,
    "url_recovery_elapsed_sec": 0,
    "url_recovery_window_sec": URL_PRESERVATION_WINDOW_SEC,
    "url_recovery_key": "",
    "force_current_broadcast_live_allowed": True,
    "replacement_broadcast_allowed": False,
    "replacement_broadcast_min_elapsed_sec": FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC,
    "replacement_broadcast_blocked_reason": "",
}


def reset_url_recovery_fields() -> dict:
    return dict(URL_RECOVERY_FIELD_DEFAULTS)


def make_url_recovery_key(
    *,
    failure_kind: str,
    video_id: str,
    oauth_broadcast_id: str,
    api_live_state: str,
    oauth_life_cycle_status: str,
    public_ok: bool,
    stream_active: bool,
    ingest_connected: bool,
) -> str:
    del api_live_state, oauth_life_cycle_status, public_ok, stream_active, ingest_connected
    target = f"video={video_id or '-'}|broadcast={oauth_broadcast_id or '-'}"
    if video_id or oauth_broadcast_id:
        return target
    return f"failure={failure_kind or 'unknown'}|target=unknown"


def compute_url_recovery_fields(
    state: dict,
    *,
    now_ts: int,
    recovery_key: str,
) -> dict:
    previous_key = str(state.get("url_recovery_key", "")).strip()
    previous_first_ts = int(state.get("url_recovery_first_ts", 0) or 0)
    first_ts = previous_first_ts if previous_key == recovery_key and previous_first_ts > 0 else now_ts
    elapsed = max(0, now_ts - first_ts)
    phase = "url_preservation" if elapsed < URL_PRESERVATION_WINDOW_SEC else "replacement_allowed"
    replacement_allowed = elapsed >= FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC
    blocked_reason = ""
    if not replacement_allowed:
        blocked_reason = (
            "replacement blocked during url preservation window "
            f"({elapsed}s<{FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC}s)"
        )
    return {
        "url_recovery_phase": phase,
        "url_recovery_first_ts": first_ts,
        "url_recovery_elapsed_sec": elapsed,
        "url_recovery_window_sec": URL_PRESERVATION_WINDOW_SEC,
        "url_recovery_key": recovery_key,
        "force_current_broadcast_live_allowed": True,
        "replacement_broadcast_allowed": replacement_allowed,
        "replacement_broadcast_min_elapsed_sec": FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC,
        "replacement_broadcast_blocked_reason": blocked_reason,
    }


def enrich_status_with_recovery_context(payload: dict) -> dict:
    if "url_recovery_phase" in payload:
        return payload
    try:
        state = load_state()
    except Exception:
        state = {}
    for key, default in URL_RECOVERY_FIELD_DEFAULTS.items():
        payload[key] = state.get(key, default)
    try:
        resolver_state = load_video_resolver_state()
    except Exception:
        resolver_state = {}
    payload.setdefault("expected_video_id", str(resolver_state.get("expected_video_id", "")).strip())
    payload.setdefault("candidate_new_url_found", bool(resolver_state.get("candidate_new_url_found", False)))
    payload.setdefault("candidate_new_video_id", str(resolver_state.get("candidate_new_video_id", "")).strip())
    payload.setdefault("candidate_new_video_source", str(resolver_state.get("candidate_new_video_source", "")).strip())
    payload.setdefault("candidate_new_video_reason", str(resolver_state.get("candidate_new_video_reason", "")).strip())
    payload.setdefault(
        "resolver_url_preservation_active",
        bool(resolver_state.get("url_preservation_active", False)),
    )
    payload.setdefault(
        "resolver_url_preservation_elapsed_sec",
        int(resolver_state.get("url_preservation_elapsed_sec", 0) or 0),
    )
    return payload


def should_emit_ok_event_safe() -> bool:
    try:
        return should_emit_ok_event()
    except Exception as e:
        log(f"OK heartbeat emission check failed: {e}")
        return False


def record_status(payload: dict) -> None:
    stats_writer.record_status(
        payload,
        enrich_status_with_recovery_context=enrich_status_with_recovery_context,
        classify_judgment=classify_judgment,
        write_stats=write_stats,
        append_event=append_event,
        should_emit_ok_event=should_emit_ok_event_safe,
    )
