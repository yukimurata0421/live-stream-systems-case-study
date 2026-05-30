from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LivePageCandidate:
    video_id: str
    reason: str
    source: str
    strong: bool


@dataclass(frozen=True)
class DataApiSearchResult:
    video_id: str
    reason: str
    episode_calls: int


@dataclass(frozen=True)
class SelectedVideoApiCheck:
    reason: str
    live_state: str
    checked: bool
    ok: bool
    checked_ts_utc: str


def oauth_deferred_result(oauth_result_cls, *, reason: str, mode: str = "shadow"):
    return oauth_result_cls(
        enabled=True,
        configured=True,
        probe_ok=False,
        healthy=False,
        reason=reason,
        mode=mode,
    )


def resolve_oauth_probe(
    *,
    quota_guard_active: bool,
    quota_guard_reason: str,
    fast_mode: bool,
    fast_remote_probe: bool,
    ingest_ready: bool,
    remote_ended_confirmed: bool,
    api_cost_guard,
    stats: dict,
    now_ts: int,
    reuse_oauth_sec: int,
    oauth_result_cls,
    oauth_cache_func,
    probe_func,
):
    if quota_guard_active:
        return oauth_result_cls(
            enabled=False,
            configured=False,
            probe_ok=False,
            healthy=False,
            reason=f"oauth probe bypassed: {quota_guard_reason}",
            mode="quota_guard",
        )

    if (not fast_mode) or fast_remote_probe or ingest_ready or remote_ended_confirmed:
        cached = oauth_cache_func(stats, now_ts, reuse_oauth_sec)
        if cached is not None and not fast_mode:
            return cached
        if api_cost_guard.active:
            return oauth_deferred_result(
                oauth_result_cls,
                reason=f"oauth probe deferred by api cost burn guard; {api_cost_guard.reason}",
            )
        return probe_func()

    return oauth_deferred_result(
        oauth_result_cls,
        reason="oauth probe deferred: fast mode ingest not ready",
    )


def effective_live_url(*, channel_live_url: str, channel_id: str) -> str:
    if channel_live_url:
        return channel_live_url
    if channel_id:
        return f"https://www.youtube.com/channel/{channel_id}/live"
    return ""


def resolve_live_page_candidate(
    *,
    live_url: str,
    fast_mode: bool,
    fast_timeout_sec: float,
    resolve_func,
    strong_func,
) -> LivePageCandidate:
    if not live_url:
        return LivePageCandidate(
            video_id="",
            reason="live page resolve skipped (missing channel live url)",
            source="channel_live_page",
            strong=False,
        )

    resolved, reason = resolve_func(live_url, timeout_sec=fast_timeout_sec if fast_mode else None)
    video_id = str(resolved or "").strip()
    strong = bool(video_id) and strong_func(reason)
    source = "channel_live_page" if strong else "channel_live_page_html"
    return LivePageCandidate(video_id=video_id, reason=reason, source=source, strong=strong)


def run_data_api_search(
    state: dict,
    *,
    should_search: bool,
    search_gate_reason: str,
    live_page_video_id: str,
    live_page_strong: bool,
    quota_guard_active: bool,
    quota_guard_reason: str,
    fast_mode: bool,
    now_ts: int,
    episode_window_start_ts: int,
    episode_calls: int,
    fast_window_sec: int,
    fast_max_calls: int,
    search_min_interval_sec: int,
    effective_channel_id: str,
    api_key: str,
    fast_timeout_sec: float,
    resolve_func,
) -> DataApiSearchResult:
    if quota_guard_active:
        return DataApiSearchResult("", f"data api search bypassed: {quota_guard_reason}", episode_calls)
    if live_page_video_id and live_page_strong:
        return DataApiSearchResult("", "data api search skipped: channel live page resolved video id", episode_calls)
    if not should_search:
        return DataApiSearchResult("", f"data api search gated: {search_gate_reason}", episode_calls)

    window_elapsed_sec = now_ts - episode_window_start_ts if fast_mode and episode_window_start_ts > 0 else 0
    if fast_mode and window_elapsed_sec >= fast_window_sec:
        return DataApiSearchResult(
            "",
            f"data api search fast window elapsed ({window_elapsed_sec}s>={fast_window_sec}s); fallback live page/cache",
            episode_calls,
        )
    if fast_mode and episode_calls >= fast_max_calls:
        return DataApiSearchResult(
            "",
            f"data api search max calls reached ({episode_calls}>={fast_max_calls}) in fast window; fallback live page/cache",
            episode_calls,
        )

    last_search_ts = int(state.get("last_data_api_search_ts", 0) or 0)
    since_last_search_sec = now_ts - last_search_ts if last_search_ts > 0 else 9_999_999
    if since_last_search_sec < search_min_interval_sec:
        return DataApiSearchResult(
            "",
            (
                f"data api search throttled by time ({since_last_search_sec}s<"
                f"{search_min_interval_sec}s); fallback live page/cache"
            ),
            episode_calls,
        )

    resolved, reason = resolve_func(
        effective_channel_id,
        api_key,
        timeout_sec=fast_timeout_sec if fast_mode else None,
    )
    state["last_data_api_search_ts"] = now_ts
    next_episode_calls = episode_calls + 1 if fast_mode else episode_calls
    if fast_mode:
        state["fast_search_episode_calls"] = next_episode_calls
    return DataApiSearchResult(str(resolved or "").strip(), reason, next_episode_calls)


def check_selected_video_api(
    *,
    selected_video_id: str,
    api_key: str,
    quota_guard_active: bool,
    quota_guard_reason: str,
    api_cost_guard,
    stats: dict,
    now_ts: int,
    max_cache_age_sec: int,
    fast_mode: bool,
    fast_timeout_sec: float,
    data_api_cache_func,
    check_func,
    utc_now_func,
) -> SelectedVideoApiCheck:
    if not selected_video_id or not api_key:
        if quota_guard_active:
            return SelectedVideoApiCheck(
                reason=f"data api check bypassed: {quota_guard_reason}",
                live_state="quota_exhausted",
                checked=False,
                ok=False,
                checked_ts_utc="",
            )
        return SelectedVideoApiCheck(
            reason="data api check skipped",
            live_state="skipped",
            checked=False,
            ok=False,
            checked_ts_utc="",
        )

    if quota_guard_active:
        return SelectedVideoApiCheck(
            reason=f"data api check bypassed: {quota_guard_reason}",
            live_state="quota_exhausted",
            checked=False,
            ok=False,
            checked_ts_utc="",
        )

    reused, cached_reason, cached_live_state = data_api_cache_func(
        stats,
        now_ts=now_ts,
        max_age_sec=max_cache_age_sec if not fast_mode else 0,
        selected_video_id=selected_video_id,
    )
    if reused:
        return SelectedVideoApiCheck(
            reason=f"{cached_reason}; reused watchdog stats cache",
            live_state=cached_live_state,
            checked=True,
            ok=cached_live_state == "live",
            checked_ts_utc="",
        )

    if api_cost_guard.active:
        return SelectedVideoApiCheck(
            reason=f"data api check deferred by api cost burn guard; {api_cost_guard.reason}",
            live_state="deferred",
            checked=False,
            ok=False,
            checked_ts_utc="",
        )

    api_check = check_func(
        selected_video_id,
        api_key,
        timeout_sec=fast_timeout_sec if fast_mode else None,
    )
    return SelectedVideoApiCheck(
        reason=api_check.reason,
        live_state=api_check.live_state,
        checked=api_check.checked,
        ok=api_check.api_ok,
        checked_ts_utc=utc_now_func() if api_check.checked else "",
    )
