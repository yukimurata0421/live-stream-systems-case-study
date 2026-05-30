from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CachedVideoContext:
    cached_video_id: str
    cached_source: str
    cached_resolved_ts: int
    cached_fresh_video_id: str
    cached_runtime_video_id: str


@dataclass(frozen=True)
class RemoteEndedState:
    raw: bool
    confirmed: bool
    reason: str
    since_ts: int
    elapsed_sec: int


@dataclass(frozen=True)
class IngestReadyState:
    ready: bool
    reason: str
    last_true_ts: int


@dataclass(frozen=True)
class FastSearchEpisode:
    window_start_ts: int
    episode_calls: int
    recovery_episode_id: str


@dataclass(frozen=True)
class UrlPreservationState:
    expected_video_id: str
    elapsed_sec: int
    window_sec: int
    active: bool


@dataclass(frozen=True)
class ResolverCadence:
    search_active: bool
    target_interval_sec: int
    skip_current_attempt: bool


@dataclass(frozen=True)
class ReuseWindows:
    oauth_sec: int
    data_api_sec: int


def reuse_windows(
    *,
    api_cost_guard_active: bool,
    oauth_sec: int,
    data_api_sec: int,
    min_oauth_sec: int,
    min_data_api_sec: int,
) -> ReuseWindows:
    if not api_cost_guard_active:
        return ReuseWindows(oauth_sec=oauth_sec, data_api_sec=data_api_sec)
    return ReuseWindows(
        oauth_sec=max(oauth_sec, min_oauth_sec),
        data_api_sec=max(data_api_sec, min_data_api_sec),
    )


def cached_video_context(state: dict, *, now_ts: int, max_age_sec: int) -> CachedVideoContext:
    cached_video_id = str(state.get("video_id", "")).strip()
    cached_source = str(state.get("source", "")).strip().lower()
    cached_resolved_ts = int(state.get("resolved_ts", 0) or 0)
    cached_fresh_video_id = (
        cached_video_id
        if cached_video_id and cached_resolved_ts > 0 and (now_ts - cached_resolved_ts) <= max_age_sec
        else ""
    )
    cached_runtime_video_id = cached_fresh_video_id if cached_source != "configured" else ""
    return CachedVideoContext(
        cached_video_id=cached_video_id,
        cached_source=cached_source,
        cached_resolved_ts=cached_resolved_ts,
        cached_fresh_video_id=cached_fresh_video_id,
        cached_runtime_video_id=cached_runtime_video_id,
    )


def update_remote_ended_state(
    state: dict,
    *,
    now_ts: int,
    raw: bool,
    raw_reason: str,
    confirm_sec: int,
) -> RemoteEndedState:
    remote_ended_since_ts = int(state.get("remote_ended_since_ts", 0) or 0)
    if raw:
        if remote_ended_since_ts <= 0:
            remote_ended_since_ts = now_ts
    else:
        remote_ended_since_ts = 0
    state["remote_ended_since_ts"] = remote_ended_since_ts
    elapsed_sec = (now_ts - remote_ended_since_ts) if remote_ended_since_ts > 0 else 0
    confirmed = raw and (elapsed_sec >= confirm_sec)
    if confirmed:
        reason = f"{raw_reason}; confirmed ({elapsed_sec}s>={confirm_sec}s)"
    elif raw:
        reason = f"{raw_reason}; waiting confirm ({elapsed_sec}s<{confirm_sec}s)"
    else:
        reason = raw_reason
    return RemoteEndedState(
        raw=raw,
        confirmed=confirmed,
        reason=reason,
        since_ts=remote_ended_since_ts,
        elapsed_sec=elapsed_sec,
    )


def apply_ingest_ready_memory(
    state: dict,
    *,
    now_ts: int,
    ready: bool,
    reason: str,
    memory_sec: int,
) -> IngestReadyState:
    last_true_ts = int(state.get("ingest_ready_last_true_ts", 0) or 0)
    if ready:
        last_true_ts = now_ts
    elif memory_sec > 0 and last_true_ts > 0 and (now_ts - last_true_ts) <= memory_sec:
        ready = True
        reason = f"{reason}; using ingest ready memory ({now_ts - last_true_ts}s<={memory_sec}s)"
    state["ingest_ready_last_true_ts"] = last_true_ts
    return IngestReadyState(ready=ready, reason=reason, last_true_ts=last_true_ts)


def update_fast_search_episode(
    state: dict,
    *,
    now_ts: int,
    fast_mode: bool,
    prev_fast_mode: bool,
    ingest_ready_for_search: bool,
) -> FastSearchEpisode:
    window_start_ts = int(state.get("fast_search_window_start_ts", 0) or 0)
    episode_calls = int(state.get("fast_search_episode_calls", 0) or 0)
    if fast_mode:
        if not prev_fast_mode:
            window_start_ts = 0
            episode_calls = 0
        if ingest_ready_for_search and window_start_ts <= 0:
            window_start_ts = now_ts
    else:
        window_start_ts = 0
        episode_calls = 0
    state["fast_search_window_start_ts"] = window_start_ts
    state["fast_search_episode_calls"] = episode_calls
    recovery_episode_id = f"fast-{window_start_ts or now_ts}" if fast_mode else ""
    return FastSearchEpisode(
        window_start_ts=window_start_ts,
        episode_calls=episode_calls,
        recovery_episode_id=recovery_episode_id,
    )


def url_preservation_state(
    *,
    expected_video_id: str,
    now_ts: int,
    fast_mode: bool,
    fast_search_window_start_ts: int,
    window_sec: int,
) -> UrlPreservationState:
    elapsed_sec = now_ts - fast_search_window_start_ts if fast_search_window_start_ts > 0 else 0
    active = fast_mode and fast_search_window_start_ts > 0 and elapsed_sec < window_sec
    return UrlPreservationState(
        expected_video_id=expected_video_id,
        elapsed_sec=elapsed_sec,
        window_sec=window_sec,
        active=active,
    )


def resolver_cadence(
    *,
    fast_mode: bool,
    unresolved_pre: bool,
    remote_ended_confirmed: bool,
    last_attempt_ts: int,
    now_ts: int,
    unhealthy_interval_sec: int,
    normal_interval_sec: int,
) -> ResolverCadence:
    target_interval_sec = unhealthy_interval_sec if fast_mode else normal_interval_sec
    return ResolverCadence(
        search_active=fast_mode or unresolved_pre or remote_ended_confirmed,
        target_interval_sec=target_interval_sec,
        skip_current_attempt=last_attempt_ts > 0 and (now_ts - last_attempt_ts) < target_interval_sec,
    )


def build_skip_state(
    state: dict,
    *,
    ts_utc: str,
    fast_mode: bool,
    fast_mode_reason: str,
    fast_mode_bad_streak: int,
    fast_mode_good_streak: int,
    episode: FastSearchEpisode,
    cadence: ResolverCadence,
    startup_anchor_ts: int,
    allow_cfg_fallback: bool,
    configured_fallback_reason: str,
    remote_ended: RemoteEndedState,
    ingest_ready: IngestReadyState,
) -> dict:
    out = dict(state)
    out.update(
        {
            "ts_utc": ts_utc,
            "fast_mode": fast_mode,
            "fast_mode_active": fast_mode,
            "fast_mode_reason": fast_mode_reason,
            "fast_mode_bad_streak": fast_mode_bad_streak,
            "fast_mode_good_streak": fast_mode_good_streak,
            "fast_search_window_start_ts": episode.window_start_ts,
            "unhealthy_mode": fast_mode,
            "search_cadence_active": cadence.search_active,
            "target_interval_sec": cadence.target_interval_sec,
            "startup_anchor_ts": startup_anchor_ts,
            "configured_fallback_allowed": allow_cfg_fallback,
            "configured_fallback_reason": configured_fallback_reason,
            "remote_ended_raw": remote_ended.raw,
            "remote_ended_confirmed": remote_ended.confirmed,
            "remote_ended_reason": remote_ended.reason,
            "remote_ended_since_ts": remote_ended.since_ts,
            "remote_ended_elapsed_sec": remote_ended.elapsed_sec,
            "ingest_ready_for_search": ingest_ready.ready,
            "ingest_ready_reason": ingest_ready.reason,
            "fast_search_episode_calls": episode.episode_calls,
            "recovery_episode_id": episode.recovery_episode_id,
        }
    )
    return out


def build_resolved_state(
    state: dict,
    *,
    ts_utc: str,
    now_ts: int,
    selected_video_id: str,
    selected_source: str,
    effective_channel_id: str,
    effective_live_url: str,
    fast_mode: bool,
    fast_mode_reason: str,
    fast_mode_bad_streak: int,
    fast_mode_good_streak: int,
    episode: FastSearchEpisode,
    cadence: ResolverCadence,
    startup_anchor_ts: int,
    allow_cfg_fallback: bool,
    configured_fallback_reason: str,
    api_reason: str,
    api_live_state: str,
    oauth,
    data_api_reason: str,
    live_page_reason: str,
    candidate_details: dict,
    url_preservation: UrlPreservationState,
    remote_ended: RemoteEndedState,
    quota_guard_active: bool,
    quota_guard_reason: str,
    ingest_ready: IngestReadyState,
    api_cost_guard,
) -> dict:
    out = dict(state)
    out.update(
        {
            "ts_utc": ts_utc,
            "last_attempt_ts": now_ts,
            "resolved_ts": now_ts if selected_video_id else int(state.get("resolved_ts", 0) or 0),
            "video_id": selected_video_id or str(state.get("video_id", "")).strip(),
            "source": selected_source,
            "channel_id": effective_channel_id,
            "channel_live_url": effective_live_url,
            "unhealthy_mode": fast_mode,
            "fast_mode": fast_mode,
            "fast_mode_active": fast_mode,
            "fast_mode_reason": fast_mode_reason,
            "fast_mode_bad_streak": fast_mode_bad_streak,
            "fast_mode_good_streak": fast_mode_good_streak,
            "fast_search_window_start_ts": episode.window_start_ts,
            "search_cadence_active": cadence.search_active,
            "target_interval_sec": cadence.target_interval_sec,
            "startup_anchor_ts": startup_anchor_ts,
            "configured_fallback_allowed": allow_cfg_fallback,
            "configured_fallback_reason": configured_fallback_reason,
            "api_reason": api_reason,
            "api_live_state": api_live_state,
            "oauth_probe_ok": oauth.probe_ok,
            "oauth_broadcast_id": oauth.broadcast_id,
            "oauth_video_id": oauth.video_id,
            "oauth_lifecycle": oauth.life_cycle_status,
            "data_api_search_reason": data_api_reason,
            "last_data_api_search_ts": int(state.get("last_data_api_search_ts", 0) or 0),
            "live_page_reason": live_page_reason,
            "expected_video_id": candidate_details["expected_video_id"],
            "candidate_new_url_found": candidate_details["candidate_new_url_found"],
            "candidate_new_video_id": candidate_details["candidate_new_video_id"],
            "candidate_new_video_source": candidate_details["candidate_new_video_source"],
            "candidate_new_video_reason": candidate_details["candidate_new_video_reason"],
            "selected_candidate_policy": candidate_details["selected_candidate_policy"],
            "url_preservation_active": url_preservation.active,
            "url_preservation_elapsed_sec": url_preservation.elapsed_sec,
            "url_preservation_window_sec": url_preservation.window_sec,
            "remote_ended_raw": remote_ended.raw,
            "remote_ended_confirmed": remote_ended.confirmed,
            "remote_ended_reason": remote_ended.reason,
            "remote_ended_since_ts": remote_ended.since_ts,
            "remote_ended_elapsed_sec": remote_ended.elapsed_sec,
            "quota_guard_active": quota_guard_active,
            "quota_guard_reason": quota_guard_reason,
            "ingest_ready_for_search": ingest_ready.ready,
            "ingest_ready_reason": ingest_ready.reason,
            "ingest_ready_last_true_ts": ingest_ready.last_true_ts,
            "fast_search_episode_calls": int(state.get("fast_search_episode_calls", 0) or 0),
            "recovery_episode_id": episode.recovery_episode_id,
            "api_cost_burn_rate_active": api_cost_guard.active,
            "api_cost_burn_rate_reason": api_cost_guard.reason,
            "api_cost_projected_units_per_day": api_cost_guard.projected_units_per_day,
            "api_cost_threshold_units_per_day": api_cost_guard.threshold_units_per_day,
        }
    )
    if selected_source == "configured":
        out["configured_fallback_uses"] = int(state.get("configured_fallback_uses", 0) or 0) + 1
    else:
        out["configured_fallback_uses"] = int(state.get("configured_fallback_uses", 0) or 0)
    return out
