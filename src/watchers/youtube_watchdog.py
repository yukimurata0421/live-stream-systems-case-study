#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time

try:
    from .systemctl_control import run_systemctl
    from .youtube_lifecycle import actions as lifecycle_actions
    from .youtube_monitor import classifier as lifecycle_classifier
    from .youtube_monitor import action_proposer
    from .youtube_monitor import sources as monitor_sources
    from .youtube_monitor import stats_writer
    from stream_core.supervisor.factory import build_runtime_supervisor
except ImportError:
    from systemctl_control import run_systemctl
    from youtube_lifecycle import actions as lifecycle_actions
    from youtube_monitor import action_proposer
    from youtube_monitor import classifier as lifecycle_classifier
    from youtube_monitor import sources as monitor_sources
    from youtube_monitor import stats_writer
    from stream_core.supervisor.factory import build_runtime_supervisor

try:
    from .youtube_health import RestartDecision, decide_restart_action, judge_incident_stage
except ImportError:
    from youtube_health import RestartDecision, decide_restart_action, judge_incident_stage

try:
    from .youtube_watchdog_config import (
        API_KEY,
        CHANNEL_ID,
        ENFORCE_RESTART,
        FORCE_LIVE_ON_UPCOMING_ONCE,
        FORCE_LIVE_AUTO_RECOVERY,
        FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC,
        INCIDENT_CONFIRM_FAILS,
        LIVE_URL,
        MAX_FAILS,
        MIN_RESTART_UPTIME_SEC,
        OAUTH_PROBE_MIN_INTERVAL_SEC,
        API_COST_BURN_RATE_OAUTH_MIN_INTERVAL_SEC,
        RECOVERING_GRACE_SEC,
        DATA_API_CHECK_MIN_INTERVAL_SEC,
        EVIDENCE_API_COST_TTL_SEC,
        EVIDENCE_DATA_API_TTL_SEC,
        EVIDENCE_INGEST_TTL_SEC,
        EVIDENCE_LEDGER_FILE,
        EVIDENCE_OAUTH_TTL_SEC,
        EVIDENCE_RESOLVER_TTL_SEC,
        EVIDENCE_WATCH_TTL_SEC,
        API_COST_BURN_RATE_DATA_API_MIN_INTERVAL_SEC,
        RESTART_BUDGET_DAILY,
        RESTART_BUDGET_EMERGENCY_OVERRIDE_SEC,
        RESTART_BUDGET_HOURLY,
        RESTART_BUDGET_RELEASE_RECONFIRM_SEC,
        RESTART_COOLDOWN_SEC,
        RESTART_FAILURE_BACKOFF_SEC,
        SKIP_RESTART_IF_INGEST_CONNECTED,
        STARTUP_GRACE_SEC,
        STATS_FILE,
        STREAM_SERVICE,
        VIDEO_ID,
        VIDEO_ID_CONFIGURED_FALLBACK_BOOT_SEC,
        VIDEO_RESOLVER_MAX_AGE_SEC,
        OAuthProbeResult,
        PUBLIC_LIVE_PROBE_ENABLE,
        URL_PRESERVATION_WINDOW_SEC,
    )
    from .youtube_watchdog_state import (
        append_event,
        classify_judgment,
        load_state,
        load_video_resolver_state,
        log,
        save_state,
        should_emit_ok_event,
        utc_now,
        write_restart_reason,
        write_stats,
    )
    from .youtube_oauth.readonly_probe import (
        check_data_api,
        check_public_watch_page_verdict,
        choose_transition_target_broadcast,
        extract_video_id,
        force_live_transition_statuses,
        parse_ingest_ports,
        probe_public_live_status,
        probe_with_oauth,
        quota_guard_status,
        select_primary_broadcast,
    )
    force_transition_live_once = lifecycle_actions.force_transition_live_once
except ImportError:
    from youtube_watchdog_config import (
        API_KEY,
        CHANNEL_ID,
        ENFORCE_RESTART,
        FORCE_LIVE_ON_UPCOMING_ONCE,
        FORCE_LIVE_AUTO_RECOVERY,
        FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC,
        INCIDENT_CONFIRM_FAILS,
        LIVE_URL,
        MAX_FAILS,
        MIN_RESTART_UPTIME_SEC,
        OAUTH_PROBE_MIN_INTERVAL_SEC,
        API_COST_BURN_RATE_OAUTH_MIN_INTERVAL_SEC,
        RECOVERING_GRACE_SEC,
        DATA_API_CHECK_MIN_INTERVAL_SEC,
        EVIDENCE_API_COST_TTL_SEC,
        EVIDENCE_DATA_API_TTL_SEC,
        EVIDENCE_INGEST_TTL_SEC,
        EVIDENCE_LEDGER_FILE,
        EVIDENCE_OAUTH_TTL_SEC,
        EVIDENCE_RESOLVER_TTL_SEC,
        EVIDENCE_WATCH_TTL_SEC,
        API_COST_BURN_RATE_DATA_API_MIN_INTERVAL_SEC,
        RESTART_BUDGET_DAILY,
        RESTART_BUDGET_EMERGENCY_OVERRIDE_SEC,
        RESTART_BUDGET_HOURLY,
        RESTART_BUDGET_RELEASE_RECONFIRM_SEC,
        RESTART_COOLDOWN_SEC,
        RESTART_FAILURE_BACKOFF_SEC,
        SKIP_RESTART_IF_INGEST_CONNECTED,
        STARTUP_GRACE_SEC,
        STATS_FILE,
        STREAM_SERVICE,
        VIDEO_ID,
        VIDEO_ID_CONFIGURED_FALLBACK_BOOT_SEC,
        VIDEO_RESOLVER_MAX_AGE_SEC,
        OAuthProbeResult,
        PUBLIC_LIVE_PROBE_ENABLE,
        URL_PRESERVATION_WINDOW_SEC,
    )
    from youtube_watchdog_state import (
        append_event,
        classify_judgment,
        load_state,
        load_video_resolver_state,
        log,
        save_state,
        should_emit_ok_event,
        utc_now,
        write_restart_reason,
        write_stats,
    )
    from youtube_oauth.readonly_probe import (
        check_data_api,
        check_public_watch_page_verdict,
        choose_transition_target_broadcast,
        extract_video_id,
        force_live_transition_statuses,
        parse_ingest_ports,
        probe_public_live_status,
        probe_with_oauth,
        quota_guard_status,
        select_primary_broadcast,
    )
    force_transition_live_once = lifecycle_actions.force_transition_live_once
try:
    from .youtube_api_cost_guard import load_api_cost_burn_rate_status
except ImportError:
    from youtube_api_cost_guard import load_api_cost_burn_rate_status

try:
    from .decision.action_gate import GateContext, decide_action
    from .decision.evaluator import evaluate
    from .decision.policy import Policy
    from .evidence.identity import TargetIdentity
    from .evidence.ledger import EvidenceLedger
    from .evidence.sources import EvidenceRecord, SourceKind
except ImportError:
    from decision.action_gate import GateContext, decide_action
    from decision.evaluator import evaluate
    from decision.policy import Policy
    from evidence.identity import TargetIdentity
    from evidence.ledger import EvidenceLedger
    from evidence.sources import EvidenceRecord, SourceKind

try:
    from .youtube_watchdog_core.cache import (
        data_api_from_stats_cache,
        load_last_watchdog_stats,
        oauth_from_stats_cache,
        parse_iso_ts,
    )
    from .youtube_watchdog_core.evidence import (
        active_evidence_first_ts,
        active_evidence_key,
        build_evidence_policy,
        detect_failure_kind,
        detect_transient_subkind,
        evidence_target,
        record_monitoring_evidence,
        verdict_from_api_live_state,
        verdict_from_oauth,
        verdict_from_watch_reason,
    )
    from .youtube_watchdog_core.process import (
        ffmpeg_has_ingest_connection,
        ffmpeg_has_ingest_connection_any,
        get_child_ffmpeg_pid,
        get_main_pid,
        get_process_elapsed_sec,
        is_service_active,
        restart_stream,
        run,
        trim_restart_history,
    )
    from .youtube_watchdog_core.recovery import (
        URL_RECOVERY_FIELD_DEFAULTS,
        compute_url_recovery_fields,
        enrich_status_with_recovery_context,
        make_url_recovery_key,
        record_status,
        reset_url_recovery_fields,
        should_emit_ok_event_safe,
    )
    from .youtube_watchdog_core.status import build_status_payload
except ImportError:
    from youtube_watchdog_core.cache import (
        data_api_from_stats_cache,
        load_last_watchdog_stats,
        oauth_from_stats_cache,
        parse_iso_ts,
    )
    from youtube_watchdog_core.evidence import (
        active_evidence_first_ts,
        active_evidence_key,
        build_evidence_policy,
        detect_failure_kind,
        detect_transient_subkind,
        evidence_target,
        record_monitoring_evidence,
        verdict_from_api_live_state,
        verdict_from_oauth,
        verdict_from_watch_reason,
    )
    from youtube_watchdog_core.process import (
        ffmpeg_has_ingest_connection,
        ffmpeg_has_ingest_connection_any,
        get_child_ffmpeg_pid,
        get_main_pid,
        get_process_elapsed_sec,
        is_service_active,
        restart_stream,
        run,
        trim_restart_history,
    )
    from youtube_watchdog_core.recovery import (
        URL_RECOVERY_FIELD_DEFAULTS,
        compute_url_recovery_fields,
        enrich_status_with_recovery_context,
        make_url_recovery_key,
        record_status,
        reset_url_recovery_fields,
        should_emit_ok_event_safe,
    )
    from youtube_watchdog_core.status import build_status_payload


def restart_stream(reason: str) -> tuple[bool, str]:
    return lifecycle_actions.restart_stream(
        reason=reason,
        stream_service=STREAM_SERVICE,
        write_restart_reason=write_restart_reason,
        run_systemctl=run_systemctl,
        log=log,
        supervisor=build_runtime_supervisor(
            run_systemctl=lambda args, check: run_systemctl(args, require_privilege=True, check=check),
        ),
    )


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


def main(force_live_once: bool = False) -> int:
    state = load_state()
    ledger = EvidenceLedger(EVIDENCE_LEDGER_FILE)
    evidence_policy = build_evidence_policy()
    last_stats = load_last_watchdog_stats()
    fail_count = int(state.get("fail_count", 0))
    degraded_public_count = int(state.get("degraded_public_count", 0))
    prev_last_reason = str(state.get("last_reason", ""))
    last_video_id = str(state.get("last_video_id", "")).strip()
    last_video_id_ts = int(state.get("last_video_id_ts", 0))
    last_api_search_ts = int(state.get("last_api_search_ts", 0))
    last_restart_ts = int(state.get("last_restart_ts", 0))
    now_ts = int(time.time())
    force_live_feature_enabled = FORCE_LIVE_ON_UPCOMING_ONCE or FORCE_LIVE_AUTO_RECOVERY or force_live_once
    api_cost_guard = load_api_cost_burn_rate_status(now_ts)
    oauth_probe_interval_sec = OAUTH_PROBE_MIN_INTERVAL_SEC
    data_api_check_interval_sec = DATA_API_CHECK_MIN_INTERVAL_SEC
    if api_cost_guard.active:
        oauth_probe_interval_sec = max(
            oauth_probe_interval_sec,
            API_COST_BURN_RATE_OAUTH_MIN_INTERVAL_SEC,
        )
        data_api_check_interval_sec = max(
            data_api_check_interval_sec,
            API_COST_BURN_RATE_DATA_API_MIN_INTERVAL_SEC,
        )
    restart_history_ts = trim_restart_history(
        state.get("restart_history_ts", []),
        now_ts,
        retention_sec=86400 + RESTART_BUDGET_RELEASE_RECONFIRM_SEC,
    )
    stream_active = is_service_active(STREAM_SERVICE)
    stream_main_pid = get_main_pid(STREAM_SERVICE)
    ffmpeg_pid = get_child_ffmpeg_pid(stream_main_pid)
    ffmpeg_uptime_sec = get_process_elapsed_sec(ffmpeg_pid)
    ingest_ports = parse_ingest_ports()
    ingest_connected, ingest_connection = ffmpeg_has_ingest_connection_any(ffmpeg_pid, ingest_ports)

    if stream_active and ffmpeg_uptime_sec > 0 and ffmpeg_uptime_sec < STARTUP_GRACE_SEC:
        in_recovering = last_restart_ts > 0 and (now_ts - last_restart_ts) < RECOVERING_GRACE_SEC
        grace_stage = "recovering" if in_recovering else "none"
        grace_reason = "recovering grace" if in_recovering else "startup grace"
        reason = (
            f"startup grace active ({ffmpeg_uptime_sec}s<{STARTUP_GRACE_SEC}s); "
            "skip remote youtube checks to avoid delay-induced false negatives"
        )
        save_state(
            {
                **reset_url_recovery_fields(),
                "fail_count": 0,
                "degraded_public_count": 0,
                "last_reason": reason,
                "last_incident_stage": grace_stage,
                "last_incident_reason": grace_reason,
                "last_video_id": last_video_id,
                "last_video_id_ts": last_video_id_ts,
                "last_api_search_ts": last_api_search_ts,
                "last_restart_ts": last_restart_ts,
                "restart_history_ts": restart_history_ts,
            }
        )
        record_status(
            {
                "status": "startup_grace",
                "healthy": True,
                "fail_count": 0,
                "degraded_public_count": 0,
                "stream_active": stream_active,
                "ffmpeg_pid": ffmpeg_pid,
                "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
                "ingest_connected": ingest_connected,
                "ingest_connection": ingest_connection,
                "action": "none",
                "reason": reason,
            }
        )
        log(f"GRACE: {reason}")
        return 0

    quota_guard = monitor_sources.read_quota_guard(now_ts, quota_guard_status=quota_guard_status)
    quota_guard_active = bool(quota_guard.get("active"))
    quota_guard_reason = str(quota_guard.get("reason", ""))
    oauth_checked_ts_utc = str(last_stats.get("oauth_checked_ts_utc", "")).strip()
    data_api_checked_ts_utc = str(last_stats.get("data_api_checked_ts_utc", "")).strip()
    legacy_stats_ts_utc = str(last_stats.get("ts_utc", "")).strip()
    if not oauth_checked_ts_utc and legacy_stats_ts_utc and "oauth_probe_ok" in last_stats:
        oauth_checked_ts_utc = legacy_stats_ts_utc
    if not data_api_checked_ts_utc and legacy_stats_ts_utc and "api_live_state" in last_stats:
        data_api_checked_ts_utc = legacy_stats_ts_utc
    if quota_guard_active:
        oauth = OAuthProbeResult(
            enabled=False,
            configured=False,
            probe_ok=False,
            healthy=False,
            reason=f"oauth probe bypassed: {quota_guard_reason}",
            mode="quota_guard",
        )
    else:
        oauth_cached = oauth_from_stats_cache(last_stats, now_ts, oauth_probe_interval_sec)
        if oauth_cached is not None:
            oauth = oauth_cached
        else:
            oauth = probe_with_oauth()
            if oauth.remote_checked:
                oauth_checked_ts_utc = utc_now()
    resolver_state = load_video_resolver_state()

    live_url = LIVE_URL
    configured_video_id = VIDEO_ID or extract_video_id(LIVE_URL)
    resolver_video_id = str(resolver_state.get("video_id", "")).strip()
    resolver_source = str(resolver_state.get("source", "")).strip() or "unknown"
    resolver_last_attempt_ts = int(resolver_state.get("last_attempt_ts", 0) or 0)
    resolver_resolved_ts = int(resolver_state.get("resolved_ts", 0) or 0)
    resolver_age_sec = now_ts - resolver_resolved_ts if resolver_resolved_ts > 0 else -1
    resolver_is_fresh = resolver_resolved_ts > 0 and resolver_age_sec <= VIDEO_RESOLVER_MAX_AGE_SEC
    resolver_is_after_restart = resolver_resolved_ts > last_restart_ts
    resolver_recent_attempt = resolver_last_attempt_ts > 0 and (now_ts - resolver_last_attempt_ts) <= (
        VIDEO_RESOLVER_MAX_AGE_SEC * 3
    )
    configured_fallback_boot_window = (
        VIDEO_ID_CONFIGURED_FALLBACK_BOOT_SEC > 0
        and ffmpeg_uptime_sec >= 0
        and ffmpeg_uptime_sec <= VIDEO_ID_CONFIGURED_FALLBACK_BOOT_SEC
    )
    configured_fallback_allowed = configured_fallback_boot_window and resolver_resolved_ts <= 0 and not last_video_id

    video_id = ""
    search_reason = "video id unresolved"
    if oauth.probe_ok and oauth.video_id:
        video_id = oauth.video_id
        search_reason = "resolved from oauth mine=true broadcast"
    elif resolver_video_id and resolver_is_fresh and resolver_is_after_restart:
        video_id = resolver_video_id
        search_reason = (
            f"resolved from resolver cache (source={resolver_source}, "
            f"age={max(0, resolver_age_sec)}s, post-restart)"
        )
    elif last_video_id and last_video_id_ts > last_restart_ts:
        video_id = last_video_id
        search_reason = "resolved from cached video id (post-restart fresh)"
    elif configured_video_id and not resolver_recent_attempt and configured_fallback_allowed:
        if last_restart_ts > 0 and resolver_resolved_ts <= last_restart_ts and (now_ts - last_restart_ts) <= VIDEO_RESOLVER_MAX_AGE_SEC:
            search_reason = "configured video id temporarily ignored until resolver refresh after restart"
        else:
            video_id = configured_video_id
            search_reason = "resolved from configured video id (boot fallback only)"
    else:
        details: list[str] = []
        if resolver_video_id and not resolver_is_after_restart:
            details.append("resolver cache stale vs last restart")
        elif resolver_video_id and not resolver_is_fresh:
            details.append(f"resolver cache too old (age={max(0, resolver_age_sec)}s)")
        if last_video_id and last_video_id_ts <= last_restart_ts:
            details.append("state cached video id stale vs last restart")
        if configured_video_id:
            if resolver_recent_attempt:
                details.append("configured video id ignored while resolver loop is active")
            elif not configured_fallback_boot_window:
                details.append("configured video id ignored after boot window")
            elif resolver_resolved_ts > 0 or last_video_id:
                details.append("configured video id ignored after runtime id initialization")
        if details:
            search_reason = f"{search_reason}; {'; '.join(details)}"

    if video_id:
        aligned_live_url = f"https://youtube.com/watch?v={video_id}"
        current_live_url_vid = extract_video_id(live_url)
        if not live_url:
            live_url = aligned_live_url
        elif current_live_url_vid and current_live_url_vid != video_id:
            live_url = aligned_live_url
            search_reason = f"{search_reason}; aligned live_url with selected video id"

    if quota_guard_active:
        watch_ok = True
        watch_reason = f"watch page check bypassed: {quota_guard_reason}"
        watch_page_verdict = "unknown"
        api_ok = False
        api_reason = f"data api check bypassed: {quota_guard_reason}"
        api_live_state = "quota_exhausted"
    else:
        watch_page = check_public_watch_page_verdict(live_url)
        watch_ok = watch_page.ok_for_availability
        watch_reason = watch_page.reason
        watch_page_verdict = watch_page.verdict
        if PUBLIC_LIVE_PROBE_ENABLE and live_url:
            public_probe = probe_public_live_status(live_url)
            watch_reason = f"{watch_reason}; {public_probe.reason}"
            if public_probe.checked:
                if public_probe.verdict == "live":
                    watch_ok = True
                    watch_page_verdict = "live"
                elif public_probe.verdict == "not_live":
                    watch_ok = False
                    watch_page_verdict = "not_live"
        reused, cached_api_ok, cached_api_reason, cached_api_live_state = data_api_from_stats_cache(
            last_stats,
            now_ts=now_ts,
            max_age_sec=data_api_check_interval_sec,
            selected_video_id=video_id,
        )
        if reused:
            api_ok = cached_api_ok
            api_reason = f"{cached_api_reason}; reused watchdog stats cache"
            api_live_state = cached_api_live_state
        else:
            api_check = check_data_api(video_id, API_KEY)
            api_ok = api_check.api_ok
            api_reason = api_check.reason
            api_live_state = api_check.live_state
            if api_check.checked:
                data_api_checked_ts_utc = utc_now()

    has_remote_check = bool(live_url or (video_id and API_KEY) or oauth.probe_ok or quota_guard_active)
    if not has_remote_check:
        watch_reason = "all remote checks skipped (no URL/VIDEO_ID/API_KEY/CHANNEL_ID)"
        watch_page_verdict = "unknown"
        api_reason = "all remote checks skipped (no URL/VIDEO_ID/API_KEY/CHANNEL_ID)"
        api_live_state = "skipped"

    local_ok = stream_active and ingest_connected
    oauth_ok = oauth.probe_ok and oauth.healthy
    if quota_guard_active:
        availability_ok = local_ok
        public_ok = True
        availability_signal_ok = local_ok
        legacy_healthy = local_ok
    else:
        availability_ok = local_ok and (oauth_ok or api_ok)
        public_ok = watch_ok and api_ok
        availability_signal_ok = oauth_ok or api_ok
        legacy_healthy = api_ok

    oauth_reason = oauth.reason
    if quota_guard_active:
        health_source = f"local_ingest_only(quota_guard, oauth_mode={oauth.mode})"
    else:
        health_source = f"availability(local&& (oauth||api), oauth_mode={oauth.mode})"
    healthy = availability_ok
    record_monitoring_evidence(
        ledger,
        now_ts=now_ts,
        video_id=video_id,
        resolver_state=resolver_state,
        stream_active=stream_active,
        ingest_connected=ingest_connected,
        api_live_state=api_live_state,
        api_reason=api_reason,
        watch_reason=watch_reason,
        watch_page_verdict=watch_page_verdict,
        oauth=oauth,
        oauth_checked_ts_utc=oauth_checked_ts_utc,
        data_api_checked_ts_utc=data_api_checked_ts_utc,
        api_cost_guard=api_cost_guard,
    )
    evidence_snapshot = ledger.snapshot()
    evidence_decision = evaluate(evidence_snapshot, evidence_policy)
    active_key = active_evidence_key(evidence_decision)
    active_first_ts = active_evidence_first_ts(state, evidence_decision.state, now_ts, active_key)
    evidence_action = "none"
    evidence_blocked_by: tuple[str, ...] = ()

    incident = judge_incident_stage(
        healthy=healthy,
        fail_count=(fail_count + 1) if not healthy else 0,
        stream_active=stream_active,
        ingest_connected=ingest_connected,
        availability_signal_ok=availability_signal_ok,
        oauth_probe_ok=oauth.probe_ok,
        oauth_life_cycle_status=oauth.life_cycle_status,
        oauth_stream_status_required=oauth.stream_status_required,
        oauth_stream_status=oauth.stream_status,
        incident_confirm_fails=INCIDENT_CONFIRM_FAILS,
    )

    def emit_status(
        *,
        status: str,
        healthy: bool,
        status_fail_count: int,
        status_degraded_public_count: int,
        incident_stage: str,
        incident_reason: str,
        failure_kind_value: str,
        action: str,
        public_ok_value: bool | None = None,
        force_live_enabled_value: bool | None = None,
        force_live_triggered_value: bool = False,
        force_live_reason_value: str = "",
    ) -> None:
        record_status(
            build_status_payload(
                status=status,
                healthy=healthy,
                fail_count=status_fail_count,
                degraded_public_count=status_degraded_public_count,
                video_id=video_id or "",
                live_url=live_url,
                search_reason=search_reason,
                watch_reason=watch_reason,
                watch_page_verdict=watch_page_verdict,
                api_reason=api_reason,
                api_live_state=api_live_state,
                stream_active=stream_active,
                ffmpeg_pid=ffmpeg_pid,
                ffmpeg_uptime_sec=ffmpeg_uptime_sec,
                ingest_connected=ingest_connected,
                ingest_connection=ingest_connection,
                local_ok=local_ok,
                oauth_ok=oauth_ok,
                api_ok=api_ok,
                availability_ok=availability_ok,
                public_ok=public_ok if public_ok_value is None else public_ok_value,
                health_source=health_source,
                legacy_healthy=legacy_healthy,
                oauth=oauth,
                incident_stage=incident_stage,
                incident_reason=incident_reason,
                failure_kind=failure_kind_value,
                oauth_checked_ts_utc=oauth_checked_ts_utc,
                data_api_checked_ts_utc=data_api_checked_ts_utc,
                api_cost_burn_rate_active=api_cost_guard.active,
                api_cost_burn_rate_reason=api_cost_guard.reason,
                api_cost_projected_units_per_day=api_cost_guard.projected_units_per_day,
                api_cost_threshold_units_per_day=api_cost_guard.threshold_units_per_day,
                force_live_on_upcoming_once_enabled=(
                    force_live_feature_enabled if force_live_enabled_value is None else force_live_enabled_value
                ),
                force_live_triggered=force_live_triggered_value,
                force_live_reason=force_live_reason_value,
                evidence_state=evidence_decision.state,
                evidence_reason=evidence_decision.reason,
                evidence_action=evidence_action,
                evidence_blocked_by=evidence_blocked_by,
                action=action,
            )
        )

    if quota_guard_active and local_ok:
        save_state(
            {
                **reset_url_recovery_fields(),
                "fail_count": 0,
                "degraded_public_count": 0,
                "last_reason": quota_guard_reason,
                "last_incident_stage": "none",
                "last_incident_reason": "quota guard active; api-derived checks bypassed",
                "last_video_id": video_id or "",
                "last_video_id_ts": now_ts if video_id else last_video_id_ts,
                "last_api_search_ts": last_api_search_ts,
                "last_restart_ts": last_restart_ts,
                "restart_history_ts": restart_history_ts,
            }
        )
        emit_status(
            status="quota_guard",
            healthy=True,
            status_fail_count=0,
            status_degraded_public_count=0,
            public_ok_value=True,
            incident_stage="none",
            incident_reason="quota guard active; local ingest healthy",
            failure_kind_value="none",
            action="restart deferred: quota exhausted guard active",
        )
        log(f"QUOTA_GUARD local ingest healthy: {quota_guard_reason}; video_id={video_id or '-'}")
        return 0

    if healthy:
        if fail_count != 0:
            log("Recovered: reset fail counter")
        if public_ok:
            degraded_public_count = 0
            save_state(
                {
                    **reset_url_recovery_fields(),
                    "fail_count": 0,
                    "degraded_public_count": 0,
                    "last_reason": "ok",
                    "last_incident_stage": incident.stage,
                    "last_incident_reason": incident.reason,
                    "last_video_id": video_id or "",
                    "last_video_id_ts": now_ts if video_id else last_video_id_ts,
                    "last_api_search_ts": last_api_search_ts,
                    "last_restart_ts": last_restart_ts,
                    "restart_history_ts": restart_history_ts,
                }
            )
            msg = (
                f"OK[{health_source}]: {search_reason}; {watch_reason}; {api_reason}; "
                f"oauth={oauth_reason}; video_id={video_id or '-'}"
            )
            emit_status(
                status="ok",
                healthy=True,
                status_fail_count=0,
                status_degraded_public_count=0,
                incident_stage=incident.stage,
                incident_reason=incident.reason,
                failure_kind_value="none",
                action="none",
            )
            log(msg)
            return 0

        degraded_public_count += 1
        degraded_reason = (
            f"public degraded while availability is healthy: {watch_reason}; {api_reason}; "
            f"oauth={oauth_reason}; video_id={video_id or '-'}"
        )
        save_state(
            {
                "fail_count": 0,
                "degraded_public_count": degraded_public_count,
                "last_reason": degraded_reason,
                "last_incident_stage": "none",
                "last_incident_reason": "availability healthy; public degraded",
                "last_video_id": video_id or "",
                "last_video_id_ts": now_ts if video_id else last_video_id_ts,
                "last_api_search_ts": last_api_search_ts,
                "last_restart_ts": last_restart_ts,
                "restart_history_ts": restart_history_ts,
            }
        )
        emit_status(
            status="degraded_public",
            healthy=True,
            status_fail_count=0,
            status_degraded_public_count=degraded_public_count,
            incident_stage="none",
            incident_reason="availability healthy; public degraded",
            failure_kind_value="none",
            action="none",
        )
        log(
            f"DEGRADED_PUBLIC count={degraded_public_count}: watch={watch_reason}; "
            f"api={api_reason}; api_live_state={api_live_state}; video_id={video_id or '-'}"
        )
        return 0

    fail_count += 1
    degraded_public_count = 0
    reason = (
        f"{search_reason}; {watch_reason}; {api_reason}; "
        f"oauth={oauth_reason}; source={health_source}; video_id={video_id or '-'}"
    )
    failure_kind = detect_failure_kind(
        stream_active=stream_active,
        ingest_connected=ingest_connected,
        selected_video_id=video_id,
        api_live_state=api_live_state,
        api_reason=api_reason,
        watch_reason=watch_reason,
        oauth_reason=oauth_reason,
        oauth_life_cycle_status=oauth.life_cycle_status,
        oauth_video_id=oauth.video_id,
    )
    url_recovery_key = make_url_recovery_key(
        failure_kind=failure_kind,
        video_id=video_id or "",
        oauth_broadcast_id=oauth.broadcast_id,
        api_live_state=api_live_state,
        oauth_life_cycle_status=oauth.life_cycle_status,
        public_ok=public_ok,
        stream_active=stream_active,
        ingest_connected=ingest_connected,
    )
    url_recovery = compute_url_recovery_fields(
        state,
        now_ts=now_ts,
        recovery_key=url_recovery_key,
    )
    save_state(
        {
            **url_recovery,
            "fail_count": fail_count,
            "degraded_public_count": degraded_public_count,
            "last_reason": reason,
            "last_incident_stage": incident.stage,
            "last_incident_reason": incident.reason,
            "last_video_id": video_id or last_video_id,
            "last_video_id_ts": now_ts if video_id else last_video_id_ts,
            "last_api_search_ts": last_api_search_ts,
            "last_restart_ts": last_restart_ts,
            "restart_history_ts": restart_history_ts,
            "active_evidence_state": evidence_decision.state if active_first_ts else "",
            "active_evidence_key": active_key if active_first_ts else "",
            "active_evidence_first_ts": active_first_ts,
        }
    )
    log(f"WARN[{incident.stage}] fail_count={fail_count}/{MAX_FAILS}: {reason}; incident={incident.reason}")

    if (
        prev_last_reason.startswith("restart failed:")
        and last_restart_ts > 0
        and RESTART_FAILURE_BACKOFF_SEC > 0
        and (now_ts - last_restart_ts) < RESTART_FAILURE_BACKOFF_SEC
    ):
        left = RESTART_FAILURE_BACKOFF_SEC - (now_ts - last_restart_ts)
        action = f"restart failure backoff active ({left}s remaining)"
        emit_status(
            status="warn",
            healthy=False,
            status_fail_count=fail_count,
            status_degraded_public_count=degraded_public_count,
            incident_stage=incident.stage,
            incident_reason=incident.reason,
            failure_kind_value=failure_kind,
            force_live_enabled_value=False,
            action=action,
        )
        log(action)
        return 0

    forced_live_triggered, forced_live_reason = force_transition_live_once(
        feature_enabled=force_live_feature_enabled,
        fail_count=fail_count,
        video_id=video_id or "",
        api_reason=api_reason,
        stream_active=stream_active,
        ingest_connected=ingest_connected,
        oauth=oauth,
        ffmpeg_uptime_sec=ffmpeg_uptime_sec,
        force_live_once_cli=force_live_once,
        url_recovery_elapsed_sec=int(url_recovery.get("url_recovery_elapsed_sec", 0) or 0),
        replacement_min_elapsed_sec=FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC,
        quota_guard_active=quota_guard_active,
    )
    if forced_live_triggered:
        log(f"Force transition attempted once: {forced_live_reason}")
    elif force_live_feature_enabled:
        log(f"Force transition skipped: {forced_live_reason}")

    gate_decision = decide_action(
        evidence_decision,
        evidence_snapshot,
        evidence_policy,
        GateContext(
            fail_count=fail_count,
            max_fails=MAX_FAILS,
            enforce_restart=ENFORCE_RESTART,
            stream_uptime_sec=ffmpeg_uptime_sec,
            min_restart_uptime_sec=MIN_RESTART_UPTIME_SEC,
            restart_budget_hourly=RESTART_BUDGET_HOURLY,
            restart_budget_daily=RESTART_BUDGET_DAILY,
            restart_history_ts=tuple(restart_history_ts),
            last_restart_ts=last_restart_ts,
            restart_cooldown_sec=RESTART_COOLDOWN_SEC,
            budget_release_reconfirm_sec=RESTART_BUDGET_RELEASE_RECONFIRM_SEC,
            budget_emergency_override_sec=RESTART_BUDGET_EMERGENCY_OVERRIDE_SEC,
            active_state_first_ts=active_first_ts,
            api_cost_degraded=api_cost_guard.active,
            stream_service=STREAM_SERVICE,
        ),
    )
    evidence_action = gate_decision.action
    evidence_blocked_by = gate_decision.blocked_by
    if gate_decision.reason == "available":
        restart_decision = RestartDecision(False, "none", gate_decision.reason)
    elif "cooldown" in gate_decision.blocked_by:
        restart_decision = RestartDecision(
            False,
            f"restart cooldown active ({gate_decision.cooldown_left}s remaining)",
            gate_decision.reason,
            cooldown_left=gate_decision.cooldown_left,
        )
    else:
        blocked = ",".join(gate_decision.blocked_by)
        detail = gate_decision.reason if not blocked else f"{gate_decision.reason}; blocked_by={blocked}"
        restart_decision = action_proposer.restart_decision_from_gate(
            gate_decision=gate_decision,
            restart_decision_cls=RestartDecision,
            stream_service=STREAM_SERVICE,
            detail=detail,
        )

    if restart_decision.action == "restart suppressed: ingest tcp connected":
        emit_status(
            status="warn",
            healthy=False,
            status_fail_count=fail_count,
            status_degraded_public_count=degraded_public_count,
            incident_stage=incident.stage,
            incident_reason=incident.reason,
            failure_kind_value=failure_kind,
            force_live_triggered_value=forced_live_triggered,
            force_live_reason_value=forced_live_reason,
            action=restart_decision.action,
        )
        log("Restart suppressed: ffmpeg ingest connection is established")
        return 0
    if restart_decision.action.startswith("restart cooldown active"):
        emit_status(
            status="warn",
            healthy=False,
            status_fail_count=fail_count,
            status_degraded_public_count=degraded_public_count,
            incident_stage=incident.stage,
            incident_reason=incident.reason,
            failure_kind_value=failure_kind,
            force_live_triggered_value=forced_live_triggered,
            force_live_reason_value=forced_live_reason,
            action=restart_decision.action,
        )
        log(f"Restart suppressed by cooldown ({restart_decision.cooldown_left}s remaining)")
        return 0
    if restart_decision.action.startswith("restart deferred:") or restart_decision.action.startswith(
        "restart budget exceeded:"
    ):
        emit_status(
            status="warn",
            healthy=False,
            status_fail_count=fail_count,
            status_degraded_public_count=degraded_public_count,
            incident_stage=incident.stage,
            incident_reason=incident.reason,
            failure_kind_value=failure_kind,
            force_live_triggered_value=forced_live_triggered,
            force_live_reason_value=forced_live_reason,
            action=restart_decision.action,
        )
        log(f"Restart deferred: {restart_decision.action}")
        return 0
    if restart_decision.should_restart:
        restart_ok, restart_detail = restart_stream(reason)
        if restart_ok:
            ledger.bump_restart_epoch(now_ts=now_ts)
            restart_history_ts = trim_restart_history([*restart_history_ts, now_ts], now_ts)
            save_state(
                {
                    **reset_url_recovery_fields(),
                    "fail_count": 0,
                    "degraded_public_count": 0,
                    "last_reason": "restarted",
                    "last_incident_stage": incident.stage,
                    "last_incident_reason": incident.reason,
                    "last_video_id": video_id or last_video_id,
                    "last_video_id_ts": now_ts if video_id else last_video_id_ts,
                    "last_api_search_ts": last_api_search_ts,
                    "last_restart_ts": now_ts,
                    "restart_history_ts": restart_history_ts,
                }
            )
            emit_status(
                status="restart",
                healthy=False,
                status_fail_count=fail_count,
                status_degraded_public_count=degraded_public_count,
                incident_stage=incident.stage,
                incident_reason=incident.reason,
                failure_kind_value=failure_kind,
                force_live_triggered_value=forced_live_triggered,
                force_live_reason_value=forced_live_reason,
                action=restart_decision.action,
            )
            log("Restart done; fail counter reset")
            return 0

        restart_history_ts = trim_restart_history([*restart_history_ts, now_ts], now_ts)
        save_state(
            {
                "fail_count": fail_count,
                "degraded_public_count": degraded_public_count,
                "last_reason": f"restart failed: {restart_detail}",
                "last_incident_stage": incident.stage,
                "last_incident_reason": incident.reason,
                "last_video_id": video_id or last_video_id,
                "last_video_id_ts": now_ts if video_id else last_video_id_ts,
                "last_api_search_ts": last_api_search_ts,
                "last_restart_ts": now_ts,
                "restart_history_ts": restart_history_ts,
            }
        )
        emit_status(
            status="warn",
            healthy=False,
            status_fail_count=fail_count,
            status_degraded_public_count=degraded_public_count,
            incident_stage=incident.stage,
            incident_reason=incident.reason,
            failure_kind_value=failure_kind,
            force_live_triggered_value=forced_live_triggered,
            force_live_reason_value=forced_live_reason,
            action=f"restart failed; backoff active ({RESTART_FAILURE_BACKOFF_SEC}s): {restart_detail}",
        )
        log(f"Restart failed; cooldown/backoff engaged ({RESTART_FAILURE_BACKOFF_SEC}s): {restart_detail}")
        return 0
    if restart_decision.action == "threshold reached; restart disabled":
        emit_status(
            status="warn",
            healthy=False,
            status_fail_count=fail_count,
            status_degraded_public_count=degraded_public_count,
            incident_stage=incident.stage,
            incident_reason=incident.reason,
            failure_kind_value=failure_kind,
            force_live_triggered_value=forced_live_triggered,
            force_live_reason_value=forced_live_reason,
            action="threshold reached; restart disabled",
        )
        log("Reached fail threshold, but restart enforcement is disabled (YTW_ENFORCE_RESTART=0)")
        return 0

    emit_status(
        status="warn",
        healthy=False,
        status_fail_count=fail_count,
        status_degraded_public_count=degraded_public_count,
        incident_stage=incident.stage,
        incident_reason=incident.reason,
        failure_kind_value=failure_kind,
        force_live_triggered_value=forced_live_triggered,
        force_live_reason_value=forced_live_reason,
        action="none",
    )
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="YouTube live watchdog for stream-new.")
    p.add_argument(
        "--force-live-once",
        action="store_true",
        help="Attempt one-time liveBroadcasts.transition when upcoming stalls.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    raise SystemExit(main(force_live_once=args.force_live_once))
