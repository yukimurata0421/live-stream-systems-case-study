from __future__ import annotations


def apply_fast_mode_hysteresis(
    state: dict,
    raw_fast_mode: bool,
    raw_reason: str,
    *,
    enter_streak: int,
    exit_streak: int,
) -> tuple[bool, str, int, int]:
    mode = bool(state.get("fast_mode_active", state.get("fast_mode", False)))
    bad_streak = int(state.get("fast_mode_bad_streak", 0) or 0)
    good_streak = int(state.get("fast_mode_good_streak", 0) or 0)

    transition = "hold"
    if raw_fast_mode:
        bad_streak += 1
        good_streak = 0
        if not mode and bad_streak >= enter_streak:
            mode = True
            transition = "enter_fast"
    else:
        good_streak += 1
        bad_streak = 0
        if mode and good_streak >= exit_streak:
            mode = False
            transition = "exit_normal"

    reason = (
        f"{raw_reason}; hysteresis={transition} "
        f"(bad={bad_streak}/{enter_streak}, "
        f"good={good_streak}/{exit_streak}, "
        f"mode={'fast' if mode else 'normal'})"
    )
    return mode, reason, bad_streak, good_streak


def has_local_video_context(local_runtime_video_id: str, cached_fresh_video_id: str) -> bool:
    return bool(local_runtime_video_id or cached_fresh_video_id)


def live_page_video_id_is_strong(reason: str) -> bool:
    return "channel live redirect" in (reason or "").strip().lower()


def ingest_ready_for_search_from_fast_signal(raw_fast_mode: bool, raw_fast_mode_reason: str) -> tuple[bool, str]:
    reason = (raw_fast_mode_reason or "").strip()
    if raw_fast_mode:
        return False, "ingest disconnected by fast signal"
    lowered = reason.lower()
    if "runtime tcp connected" in lowered:
        return True, "runtime ingest connected"
    if "stats fallback: ingest connected" in lowered:
        return True, "stats ingest connected fallback"
    return False, f"ingest readiness unknown ({reason or 'no signal'})"


def should_run_data_api_search(
    *,
    fast_mode: bool,
    oauth_video_id: str,
    runtime_video_id_resolved: bool,
    local_video_id_resolved: bool,
    effective_channel_id: str,
    api_key: str,
    quota_guard_active: bool,
    api_cost_burn_rate_active: bool,
    api_cost_burn_rate_reason: str,
    ingest_ready_for_search: bool,
    ingest_ready_reason: str,
    require_ingest_for_search: bool,
    remote_ended_confirmed: bool,
    remote_ended_reason: str,
) -> tuple[bool, str]:
    if quota_guard_active:
        return False, "quota guard active"
    if api_cost_burn_rate_active:
        return False, f"api cost burn guard active ({api_cost_burn_rate_reason})"
    if not effective_channel_id or not api_key:
        return False, "missing channel id or api key"
    if oauth_video_id:
        return False, "oauth already resolved video id"
    if runtime_video_id_resolved and fast_mode:
        return False, "fast mode search suppressed: video id already resolved"
    if require_ingest_for_search and (not ingest_ready_for_search) and (not remote_ended_confirmed):
        return False, f"ingest not ready for search.list ({ingest_ready_reason})"
    if not local_video_id_resolved:
        return True, "video id unresolved"
    if remote_ended_confirmed:
        return True, remote_ended_reason
    if fast_mode and ingest_ready_for_search:
        return True, "fast mode active with ingest ready"
    return False, "normal mode with known video id"


def allow_configured_fallback(
    *,
    state: dict,
    now_ts: int,
    cfg_video_id: str,
    cached_resolved_ts: int,
    boot_sec: int,
) -> tuple[bool, int, str]:
    startup_anchor_ts = int(state.get("startup_anchor_ts", 0) or 0)
    if startup_anchor_ts <= 0:
        startup_anchor_ts = now_ts
        state["startup_anchor_ts"] = startup_anchor_ts

    if not cfg_video_id:
        return False, startup_anchor_ts, "configured video id missing"
    if boot_sec <= 0:
        return False, startup_anchor_ts, "configured fallback disabled (boot window=0)"

    configured_fallback_uses = int(state.get("configured_fallback_uses", 0) or 0)
    if configured_fallback_uses > 0:
        return False, startup_anchor_ts, "configured fallback already used"
    if cached_resolved_ts > 0:
        return False, startup_anchor_ts, "configured fallback disabled after first resolution"

    age_sec = now_ts - startup_anchor_ts
    if age_sec > boot_sec:
        return (
            False,
            startup_anchor_ts,
            f"configured fallback boot window elapsed ({age_sec}s>{boot_sec}s)",
        )
    return True, startup_anchor_ts, "configured fallback allowed during boot window"
