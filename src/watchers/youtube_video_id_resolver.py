#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from .youtube_oauth.readonly_probe import (
        check_data_api,
        parse_ingest_ports,
        probe_with_oauth,
        quota_guard_status,
        resolve_live_video_id,
        resolve_video_id_from_live_page,
    )
    from .video_resolver import cache as resolver_cache
    from .video_resolver import (
        candidate_policy,
        fast_stats,
        identity,
        policy,
        probe_flow,
        remote_state,
        session,
        url_context,
    )
    from .video_resolver.process_probe import (
        ffmpeg_has_ingest_connection,
        ffmpeg_has_ingest_connection_any,
        get_child_ffmpeg_pid,
        get_main_pid,
        get_process_elapsed_sec,
        is_service_active,
        run,
    )
    from .youtube_watchdog_config import (
        API_KEY,
        CHANNEL_ID,
        CHANNEL_LIVE_URL,
        LOG_BASE_DIR,
        LIVE_URL,
        STREAM_SERVICE,
        STATS_FILE,
        VIDEO_ID,
        VIDEO_RESOLVER_FAST_ENTER_STREAK,
        VIDEO_RESOLVER_FAST_EXIT_STREAK,
        VIDEO_RESOLVER_FAST_SEARCH_MAX_CALLS,
        VIDEO_RESOLVER_FAST_SEARCH_WINDOW_SEC,
        VIDEO_RESOLVER_FAST_HTTP_TIMEOUT_SEC,
        VIDEO_RESOLVER_FAST_REMOTE_PROBE,
        VIDEO_RESOLVER_INGEST_READY_MEMORY_SEC,
        VIDEO_RESOLVER_MAX_AGE_SEC,
        VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC,
        VIDEO_RESOLVER_REFRESH_WATCHDOG_STATS_FAST_MODE,
        VIDEO_RESOLVER_NORMAL_INTERVAL_SEC,
        RESOLVER_REUSE_WATCHDOG_DATA_API_SEC,
        RESOLVER_REUSE_WATCHDOG_OAUTH_SEC,
        API_COST_BURN_RATE_OAUTH_MIN_INTERVAL_SEC,
        API_COST_BURN_RATE_DATA_API_MIN_INTERVAL_SEC,
        VIDEO_RESOLVER_REQUIRE_INGEST_FOR_SEARCH,
        VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC,
        VIDEO_RESOLVER_UNHEALTHY_INTERVAL_SEC,
        VIDEO_ID_CONFIGURED_FALLBACK_BOOT_SEC,
        URL_PRESERVATION_WINDOW_SEC,
        OAuthProbeResult,
    )
    from .youtube_watchdog_state import load_video_resolver_state, log, save_video_resolver_state, update_stats
except ImportError:
    from youtube_oauth.readonly_probe import (
        check_data_api,
        parse_ingest_ports,
        probe_with_oauth,
        quota_guard_status,
        resolve_live_video_id,
        resolve_video_id_from_live_page,
    )
    from video_resolver import cache as resolver_cache
    from video_resolver import candidate_policy, fast_stats, identity, policy, probe_flow, remote_state, session, url_context
    from video_resolver.process_probe import (
        ffmpeg_has_ingest_connection,
        ffmpeg_has_ingest_connection_any,
        get_child_ffmpeg_pid,
        get_main_pid,
        get_process_elapsed_sec,
        is_service_active,
        run,
    )
    from youtube_watchdog_config import (
        API_KEY,
        CHANNEL_ID,
        CHANNEL_LIVE_URL,
        LOG_BASE_DIR,
        LIVE_URL,
        STREAM_SERVICE,
        STATS_FILE,
        VIDEO_ID,
        VIDEO_RESOLVER_FAST_ENTER_STREAK,
        VIDEO_RESOLVER_FAST_EXIT_STREAK,
        VIDEO_RESOLVER_FAST_SEARCH_MAX_CALLS,
        VIDEO_RESOLVER_FAST_SEARCH_WINDOW_SEC,
        VIDEO_RESOLVER_FAST_HTTP_TIMEOUT_SEC,
        VIDEO_RESOLVER_FAST_REMOTE_PROBE,
        VIDEO_RESOLVER_INGEST_READY_MEMORY_SEC,
        VIDEO_RESOLVER_MAX_AGE_SEC,
        VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC,
        VIDEO_RESOLVER_REFRESH_WATCHDOG_STATS_FAST_MODE,
        VIDEO_RESOLVER_NORMAL_INTERVAL_SEC,
        RESOLVER_REUSE_WATCHDOG_DATA_API_SEC,
        RESOLVER_REUSE_WATCHDOG_OAUTH_SEC,
        API_COST_BURN_RATE_OAUTH_MIN_INTERVAL_SEC,
        API_COST_BURN_RATE_DATA_API_MIN_INTERVAL_SEC,
        VIDEO_RESOLVER_REQUIRE_INGEST_FOR_SEARCH,
        VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC,
        VIDEO_RESOLVER_UNHEALTHY_INTERVAL_SEC,
        VIDEO_ID_CONFIGURED_FALLBACK_BOOT_SEC,
        URL_PRESERVATION_WINDOW_SEC,
        OAuthProbeResult,
    )
    from youtube_watchdog_state import load_video_resolver_state, log, save_video_resolver_state, update_stats
try:
    from .youtube_api_cost_guard import load_api_cost_burn_rate_status
except ImportError:
    from youtube_api_cost_guard import load_api_cost_burn_rate_status


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


RESOLVER_EVENT_LOG_FILE = Path(
    os.environ.get(
        "YTW_VIDEO_RESOLVER_EVENT_LOG_FILE",
        str(LOG_BASE_DIR / "youtube_video_id_resolver_events.jsonl"),
    )
).expanduser()


def append_resolver_event(event: str, **fields: object) -> None:
    payload = {"ts_utc": utc_now(), "event": event, **fields}
    try:
        RESOLVER_EVENT_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with RESOLVER_EVENT_LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    except Exception as e:
        log(f"WARN failed to append resolver event log: {e}")


def parse_iso_ts(raw: str) -> int:
    return identity.parse_iso_ts(raw)


def load_watchdog_stats() -> dict:
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
            if isinstance(payload, dict):
                return payload
    except Exception:
        pass
    return {}


def _latest_iso_ts(*values: str) -> str:
    return identity.latest_iso_ts(*values)


def build_remote_sample_id(
    *,
    remote_probe_ts_utc: str,
    remote_source: str,
    recovery_episode_id: str,
    ffmpeg_generation: str,
    selected_video_id: str,
) -> str:
    return identity.build_remote_sample_id(
        remote_probe_ts_utc=remote_probe_ts_utc,
        remote_source=remote_source,
        recovery_episode_id=recovery_episode_id,
        ffmpeg_generation=ffmpeg_generation,
        selected_video_id=selected_video_id,
    )


def ffmpeg_generation_from_runtime(local_runtime: dict) -> str:
    return identity.ffmpeg_generation_from_runtime(local_runtime)


def recovery_episode_id_from_state(state: dict, now_ts: int, fast_mode: bool) -> str:
    return identity.recovery_episode_id_from_state(state, now_ts, fast_mode)


def oauth_from_watchdog_stats_cache(stats: dict, now_ts: int, max_age_sec: int) -> OAuthProbeResult | None:
    return resolver_cache.oauth_from_watchdog_stats_cache(stats, now_ts, max_age_sec, OAuthProbeResult)


def data_api_from_watchdog_stats_cache(
    stats: dict,
    *,
    now_ts: int,
    max_age_sec: int,
    selected_video_id: str,
) -> tuple[bool, str, str]:
    return resolver_cache.data_api_from_watchdog_stats_cache(
        stats,
        now_ts=now_ts,
        max_age_sec=max_age_sec,
        selected_video_id=selected_video_id,
    )


def choose_video_candidate(
    candidates: list[tuple[str, str]],
    *,
    expected_video_id: str,
    url_preservation_active: bool,
) -> tuple[str, str, dict]:
    return candidate_policy.choose_video_candidate(
        candidates,
        expected_video_id=expected_video_id,
        url_preservation_active=url_preservation_active,
    )


def detect_ingest_connected_now() -> tuple[bool | None, str]:
    if not is_service_active(STREAM_SERVICE):
        return None, "stream service inactive"
    main_pid = get_main_pid(STREAM_SERVICE)
    if main_pid <= 1:
        return None, "stream main pid unavailable"
    ffmpeg_pid = get_child_ffmpeg_pid(main_pid)
    if ffmpeg_pid <= 1:
        return None, "ffmpeg child pid unavailable"

    ingest_ports = parse_ingest_ports()
    connected, conn_line = ffmpeg_has_ingest_connection_any(ffmpeg_pid, ingest_ports)
    if connected:
        return True, f"runtime tcp connected ({conn_line})"
    return False, "runtime tcp disconnected"


def read_local_runtime_status() -> dict:
    stream_active = is_service_active(STREAM_SERVICE)
    main_pid = get_main_pid(STREAM_SERVICE) if stream_active else 0
    ffmpeg_pid = get_child_ffmpeg_pid(main_pid)
    ffmpeg_uptime_sec = get_process_elapsed_sec(ffmpeg_pid)
    ingest_ports = parse_ingest_ports()
    ingest_connected, ingest_connection = ffmpeg_has_ingest_connection_any(ffmpeg_pid, ingest_ports)
    return {
        "stream_active": stream_active,
        "stream_main_pid": main_pid,
        "ffmpeg_pid": ffmpeg_pid,
        "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
        "ingest_connected": ingest_connected,
        "ingest_connection": ingest_connection,
        "local_ok": stream_active and ingest_connected,
    }


def resolve_fast_mode(stats: dict) -> tuple[bool, str]:
    connected_now, runtime_reason = detect_ingest_connected_now()
    if connected_now is True:
        return False, runtime_reason
    if connected_now is False:
        return True, runtime_reason

    ingest_connected = stats.get("ingest_connected")
    if isinstance(ingest_connected, bool):
        if ingest_connected:
            return False, f"stats fallback: ingest connected ({runtime_reason})"
        return True, f"stats fallback: ingest disconnected ({runtime_reason})"

    return False, f"no ingest signal; normal mode ({runtime_reason})"


def apply_fast_mode_hysteresis(
    state: dict,
    raw_fast_mode: bool,
    raw_reason: str,
) -> tuple[bool, str, int, int]:
    return policy.apply_fast_mode_hysteresis(
        state,
        raw_fast_mode,
        raw_reason,
        enter_streak=VIDEO_RESOLVER_FAST_ENTER_STREAK,
        exit_streak=VIDEO_RESOLVER_FAST_EXIT_STREAK,
    )


def has_recent_remote_ended(stats: dict, now_ts: int) -> tuple[bool, str]:
    return remote_state.has_recent_remote_ended(
        stats,
        now_ts,
        max_age_sec=VIDEO_RESOLVER_MAX_AGE_SEC,
        parse_ts=parse_iso_ts,
    )


def has_local_video_context(local_runtime_video_id: str, cached_fresh_video_id: str) -> bool:
    return policy.has_local_video_context(local_runtime_video_id, cached_fresh_video_id)


def live_page_video_id_is_strong(reason: str) -> bool:
    return policy.live_page_video_id_is_strong(reason)


def ingest_ready_for_search_from_fast_signal(raw_fast_mode: bool, raw_fast_mode_reason: str) -> tuple[bool, str]:
    return policy.ingest_ready_for_search_from_fast_signal(raw_fast_mode, raw_fast_mode_reason)


def should_run_data_api_search(
    *,
    fast_mode: bool,
    oauth_video_id: str,
    runtime_video_id_resolved: bool,
    local_video_id_resolved: bool,
    effective_channel_id: str,
    quota_guard_active: bool,
    api_cost_burn_rate_active: bool,
    api_cost_burn_rate_reason: str,
    ingest_ready_for_search: bool,
    ingest_ready_reason: str,
    require_ingest_for_search: bool,
    remote_ended_confirmed: bool,
    remote_ended_reason: str,
) -> tuple[bool, str]:
    return policy.should_run_data_api_search(
        fast_mode=fast_mode,
        oauth_video_id=oauth_video_id,
        runtime_video_id_resolved=runtime_video_id_resolved,
        local_video_id_resolved=local_video_id_resolved,
        effective_channel_id=effective_channel_id,
        api_key=API_KEY,
        quota_guard_active=quota_guard_active,
        api_cost_burn_rate_active=api_cost_burn_rate_active,
        api_cost_burn_rate_reason=api_cost_burn_rate_reason,
        ingest_ready_for_search=ingest_ready_for_search,
        ingest_ready_reason=ingest_ready_reason,
        require_ingest_for_search=require_ingest_for_search,
        remote_ended_confirmed=remote_ended_confirmed,
        remote_ended_reason=remote_ended_reason,
    )


def refresh_watchdog_stats_from_fast_mode(
    *,
    fast_mode: bool,
    local_runtime: dict,
    selected_video_id: str,
    selected_source: str,
    search_reason: str,
    oauth: OAuthProbeResult,
    api_checked: bool,
    api_ok: bool,
    api_reason: str,
    api_live_state: str,
    data_api_checked_ts_utc: str,
    fast_mode_reason: str,
    recovery_episode_id: str,
    quota_guard_active: bool,
    quota_guard_reason: str,
    api_cost_guard,
) -> bool:
    payload = fast_stats.build_fast_watchdog_stats_payload(
        enabled=VIDEO_RESOLVER_REFRESH_WATCHDOG_STATS_FAST_MODE,
        fast_mode=fast_mode,
        local_runtime=local_runtime,
        selected_video_id=selected_video_id,
        selected_source=selected_source,
        search_reason=search_reason,
        oauth=oauth,
        api_checked=api_checked,
        api_ok=api_ok,
        api_reason=api_reason,
        api_live_state=api_live_state,
        data_api_checked_ts_utc=data_api_checked_ts_utc,
        fast_mode_reason=fast_mode_reason,
        recovery_episode_id=recovery_episode_id,
        quota_guard_active=quota_guard_active,
        quota_guard_reason=quota_guard_reason,
        api_cost_guard=api_cost_guard,
        utc_now_func=utc_now,
    )
    if payload is None:
        return False
    update_stats(payload)
    return True


def configured_video_id() -> str:
    return url_context.configured_video_id_from(video_id=VIDEO_ID, live_url=LIVE_URL)


def allow_configured_fallback(
    *,
    state: dict,
    now_ts: int,
    cfg_video_id: str,
    cached_resolved_ts: int,
) -> tuple[bool, int, str]:
    return policy.allow_configured_fallback(
        state=state,
        now_ts=now_ts,
        cfg_video_id=cfg_video_id,
        cached_resolved_ts=cached_resolved_ts,
        boot_sec=VIDEO_ID_CONFIGURED_FALLBACK_BOOT_SEC,
    )


def main() -> int:
    now_ts = int(time.time())
    api_cost_guard = load_api_cost_burn_rate_status(now_ts)
    reuse = session.reuse_windows(
        api_cost_guard_active=api_cost_guard.active,
        oauth_sec=RESOLVER_REUSE_WATCHDOG_OAUTH_SEC,
        data_api_sec=RESOLVER_REUSE_WATCHDOG_DATA_API_SEC,
        min_oauth_sec=API_COST_BURN_RATE_OAUTH_MIN_INTERVAL_SEC,
        min_data_api_sec=API_COST_BURN_RATE_DATA_API_MIN_INTERVAL_SEC,
    )
    stats = load_watchdog_stats()
    state = load_video_resolver_state()
    prev_fast_mode = bool(state.get("fast_mode_active", state.get("fast_mode", False)))

    cfg_video_id = configured_video_id()
    cached = session.cached_video_context(state, now_ts=now_ts, max_age_sec=VIDEO_RESOLVER_MAX_AGE_SEC)
    allow_cfg_fallback, startup_anchor_ts, configured_fallback_reason = allow_configured_fallback(
        state=state,
        now_ts=now_ts,
        cfg_video_id=cfg_video_id,
        cached_resolved_ts=cached.cached_resolved_ts,
    )

    remote_ended_raw, remote_ended_raw_reason = has_recent_remote_ended(stats, now_ts)
    remote_ended = session.update_remote_ended_state(
        state,
        now_ts=now_ts,
        raw=remote_ended_raw,
        raw_reason=remote_ended_raw_reason,
        confirm_sec=VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC,
    )

    unresolved_pre = not has_local_video_context(
        cfg_video_id if allow_cfg_fallback else "",
        cached.cached_fresh_video_id,
    )

    raw_fast_mode, raw_fast_mode_reason = resolve_fast_mode(stats)
    fast_mode, fast_mode_reason, fast_mode_bad_streak, fast_mode_good_streak = apply_fast_mode_hysteresis(
        state,
        raw_fast_mode,
        raw_fast_mode_reason,
    )
    ingest_ready_for_search, ingest_ready_reason = ingest_ready_for_search_from_fast_signal(
        raw_fast_mode,
        raw_fast_mode_reason,
    )
    ingest_ready = session.apply_ingest_ready_memory(
        state,
        now_ts=now_ts,
        ready=ingest_ready_for_search,
        reason=ingest_ready_reason,
        memory_sec=VIDEO_RESOLVER_INGEST_READY_MEMORY_SEC,
    )
    episode = session.update_fast_search_episode(
        state,
        now_ts=now_ts,
        fast_mode=fast_mode,
        prev_fast_mode=prev_fast_mode,
        ingest_ready_for_search=ingest_ready.ready,
    )

    last_attempt_ts = int(state.get("last_attempt_ts", 0) or 0)
    cadence = session.resolver_cadence(
        fast_mode=fast_mode,
        unresolved_pre=unresolved_pre,
        remote_ended_confirmed=remote_ended.confirmed,
        last_attempt_ts=last_attempt_ts,
        now_ts=now_ts,
        unhealthy_interval_sec=VIDEO_RESOLVER_UNHEALTHY_INTERVAL_SEC,
        normal_interval_sec=VIDEO_RESOLVER_NORMAL_INTERVAL_SEC,
    )
    if fast_mode != prev_fast_mode:
        append_resolver_event(
            "fast_mode_enter" if fast_mode else "fast_mode_exit",
            fast_mode_active=fast_mode,
            previous_fast_mode_active=prev_fast_mode,
            fast_mode_reason=fast_mode_reason,
            fast_mode_bad_streak=fast_mode_bad_streak,
            fast_mode_good_streak=fast_mode_good_streak,
            fast_search_window_start_ts=episode.window_start_ts,
            fast_search_episode_calls=episode.episode_calls,
            ingest_ready_for_search=ingest_ready.ready,
            ingest_ready_reason=ingest_ready.reason,
            remote_ended_confirmed=remote_ended.confirmed,
            recovery_episode_id=episode.recovery_episode_id,
        )
    if cadence.skip_current_attempt:
        save_video_resolver_state(
            session.build_skip_state(
                state,
                ts_utc=utc_now(),
                fast_mode=fast_mode,
                fast_mode_reason=fast_mode_reason,
                fast_mode_bad_streak=fast_mode_bad_streak,
                fast_mode_good_streak=fast_mode_good_streak,
                episode=episode,
                cadence=cadence,
                startup_anchor_ts=startup_anchor_ts,
                allow_cfg_fallback=allow_cfg_fallback,
                configured_fallback_reason=configured_fallback_reason,
                remote_ended=remote_ended,
                ingest_ready=ingest_ready,
            )
        )
        return 0

    quota_guard_active, quota_guard_reason, _quota_state = quota_guard_status(now_ts)
    oauth = probe_flow.resolve_oauth_probe(
        quota_guard_active=quota_guard_active,
        quota_guard_reason=quota_guard_reason,
        fast_mode=fast_mode,
        fast_remote_probe=VIDEO_RESOLVER_FAST_REMOTE_PROBE,
        ingest_ready=ingest_ready.ready,
        remote_ended_confirmed=remote_ended.confirmed,
        api_cost_guard=api_cost_guard,
        stats=stats,
        now_ts=now_ts,
        reuse_oauth_sec=reuse.oauth_sec,
        oauth_result_cls=OAuthProbeResult,
        oauth_cache_func=oauth_from_watchdog_stats_cache,
        probe_func=probe_with_oauth,
    )
    effective_channel_id = CHANNEL_ID or oauth.channel_id
    effective_live_url = probe_flow.effective_live_url(
        channel_live_url=CHANNEL_LIVE_URL,
        channel_id=effective_channel_id,
    )

    # Only the resolver cache represents the current URL we are trying to preserve.
    # Configured VIDEO_ID is a boot seed and must not suppress fast recovery discovery.
    url_preservation = session.url_preservation_state(
        expected_video_id=cached.cached_video_id.strip(),
        now_ts=now_ts,
        fast_mode=fast_mode,
        fast_search_window_start_ts=episode.window_start_ts,
        window_sec=URL_PRESERVATION_WINDOW_SEC,
    )

    candidates: list[tuple[str, str]] = []
    if oauth.probe_ok and oauth.video_id:
        candidates.append((oauth.video_id.strip(), "oauth"))

    live_page = probe_flow.resolve_live_page_candidate(
        live_url=effective_live_url,
        fast_mode=fast_mode,
        fast_timeout_sec=VIDEO_RESOLVER_FAST_HTTP_TIMEOUT_SEC,
        resolve_func=resolve_video_id_from_live_page,
        strong_func=live_page_video_id_is_strong,
    )
    if live_page.video_id and live_page.strong:
        candidates.append((live_page.video_id, live_page.source))

    should_search, search_gate_reason = should_run_data_api_search(
        fast_mode=fast_mode,
        oauth_video_id=oauth.video_id.strip(),
        runtime_video_id_resolved=has_local_video_context(
            "",
            cached.cached_runtime_video_id,
        ),
        local_video_id_resolved=has_local_video_context(
            cfg_video_id if allow_cfg_fallback else "",
            cached.cached_fresh_video_id,
        ),
        effective_channel_id=effective_channel_id,
        quota_guard_active=quota_guard_active,
        api_cost_burn_rate_active=api_cost_guard.active,
        api_cost_burn_rate_reason=api_cost_guard.reason,
        ingest_ready_for_search=ingest_ready.ready,
        ingest_ready_reason=ingest_ready.reason,
        require_ingest_for_search=VIDEO_RESOLVER_REQUIRE_INGEST_FOR_SEARCH,
        remote_ended_confirmed=remote_ended.confirmed,
        remote_ended_reason=remote_ended.reason,
    )
    search_result = probe_flow.run_data_api_search(
        state,
        should_search=should_search,
        search_gate_reason=search_gate_reason,
        live_page_video_id=live_page.video_id,
        live_page_strong=live_page.strong,
        quota_guard_active=quota_guard_active,
        quota_guard_reason=quota_guard_reason,
        fast_mode=fast_mode,
        now_ts=now_ts,
        episode_window_start_ts=episode.window_start_ts,
        episode_calls=episode.episode_calls,
        fast_window_sec=VIDEO_RESOLVER_FAST_SEARCH_WINDOW_SEC,
        fast_max_calls=VIDEO_RESOLVER_FAST_SEARCH_MAX_CALLS,
        search_min_interval_sec=VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC,
        effective_channel_id=effective_channel_id,
        api_key=API_KEY,
        fast_timeout_sec=VIDEO_RESOLVER_FAST_HTTP_TIMEOUT_SEC,
        resolve_func=resolve_live_video_id,
    )
    data_api_reason = search_result.reason
    if search_result.video_id:
        candidates.append((search_result.video_id, "data_api_search"))

    if live_page.video_id and not live_page.strong:
        candidates.append((live_page.video_id, live_page.source))

    if cfg_video_id and allow_cfg_fallback:
        candidates.append((cfg_video_id, "configured"))

    if cached.cached_fresh_video_id:
        candidates.append((cached.cached_video_id, "resolver_cache"))

    selected_video_id, selected_source, candidate_details = choose_video_candidate(
        candidates,
        expected_video_id=url_preservation.expected_video_id,
        url_preservation_active=url_preservation.active,
    )

    api_check = probe_flow.check_selected_video_api(
        selected_video_id=selected_video_id,
        api_key=API_KEY,
        quota_guard_active=quota_guard_active,
        quota_guard_reason=quota_guard_reason,
        api_cost_guard=api_cost_guard,
        stats=stats,
        now_ts=now_ts,
        max_cache_age_sec=reuse.data_api_sec,
        fast_mode=fast_mode,
        fast_timeout_sec=VIDEO_RESOLVER_FAST_HTTP_TIMEOUT_SEC,
        data_api_cache_func=data_api_from_watchdog_stats_cache,
        check_func=check_data_api,
        utc_now_func=utc_now,
    )
    api_reason = api_check.reason
    api_live_state = api_check.live_state
    api_checked = api_check.checked
    api_ok = api_check.ok
    data_api_checked_ts_utc = api_check.checked_ts_utc

    out = session.build_resolved_state(
        state,
        ts_utc=utc_now(),
        now_ts=now_ts,
        selected_video_id=selected_video_id,
        selected_source=selected_source,
        effective_channel_id=effective_channel_id,
        effective_live_url=effective_live_url,
        fast_mode=fast_mode,
        fast_mode_reason=fast_mode_reason,
        fast_mode_bad_streak=fast_mode_bad_streak,
        fast_mode_good_streak=fast_mode_good_streak,
        episode=episode,
        cadence=cadence,
        startup_anchor_ts=startup_anchor_ts,
        allow_cfg_fallback=allow_cfg_fallback,
        configured_fallback_reason=configured_fallback_reason,
        api_reason=api_reason,
        api_live_state=api_live_state,
        oauth=oauth,
        data_api_reason=data_api_reason,
        live_page_reason=live_page.reason,
        candidate_details=candidate_details,
        url_preservation=url_preservation,
        remote_ended=remote_ended,
        quota_guard_active=quota_guard_active,
        quota_guard_reason=quota_guard_reason,
        ingest_ready=ingest_ready,
        api_cost_guard=api_cost_guard,
    )
    save_video_resolver_state(out)
    if fast_mode and VIDEO_RESOLVER_REFRESH_WATCHDOG_STATS_FAST_MODE:
        local_runtime = read_local_runtime_status()
        if refresh_watchdog_stats_from_fast_mode(
            fast_mode=fast_mode,
            local_runtime=local_runtime,
            selected_video_id=selected_video_id,
            selected_source=selected_source,
            search_reason=(
                f"resolved source={selected_source}; "
                f"resolver={data_api_reason}; live_page={live_page.reason}"
            ),
            oauth=oauth,
            api_checked=api_checked,
            api_ok=api_ok,
            api_reason=api_reason,
            api_live_state=api_live_state,
            data_api_checked_ts_utc=data_api_checked_ts_utc,
            fast_mode_reason=fast_mode_reason,
            recovery_episode_id=str(out.get("recovery_episode_id", "")),
            quota_guard_active=quota_guard_active,
            quota_guard_reason=quota_guard_reason,
            api_cost_guard=api_cost_guard,
        ):
            log("VIDEO_RESOLVER refreshed watchdog stats from fast remote evidence")

    if selected_video_id:
        log(
            f"VIDEO_RESOLVER ok source={selected_source} video_id={selected_video_id} "
            f"mode={'fast' if fast_mode else 'normal'} policy={candidate_details['selected_candidate_policy']} "
            f"url_preservation_elapsed={url_preservation.elapsed_sec}s reason={fast_mode_reason}"
        )
    else:
        log(
            f"VIDEO_RESOLVER unresolved mode={'fast' if fast_mode else 'normal'} reason={fast_mode_reason} "
            f"(oauth={oauth.reason}; api_search={data_api_reason}; live_page={live_page.reason})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
