from __future__ import annotations

import time
from pathlib import Path
from typing import Callable

try:
    from stream_core.common.json_io import iter_jsonl, read_json_file
    from stream_core.common.timeutil import parse_utc_ts
except ModuleNotFoundError:
    from common.json_io import iter_jsonl, read_json_file
    from common.timeutil import parse_utc_ts

ObservePayload = Callable[[int], tuple[int, dict, str]]

NOISE_ONLY_REPORT_WARNINGS = {
    "aircraft_messages_and_positions_not_moving_in_sample",
}


def seconds_to_human(seconds: int | float | None) -> str:
    try:
        total = max(0, int(seconds or 0))
    except Exception:
        total = 0
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def latest_jsonl_item(path: Path, *, target: str = "", now_ts: int | None = None) -> tuple[dict, int | None]:
    latest: dict = {}
    latest_ts = 0
    for item in iter_jsonl(path):
        if target and str(item.get("target", "")) != target:
            continue
        ts = parse_utc_ts(str(item.get("ts_utc", "")))
        if ts >= latest_ts:
            latest_ts = ts
            latest = item
    if latest_ts <= 0:
        return {}, None
    now = int(time.time() if now_ts is None else now_ts)
    return latest, max(0, now - latest_ts)


def report_incident_spec(
    ident: str,
    *,
    stream1090_report_events_file: Path,
    upstream_report_events_file: Path,
) -> tuple[Path, str] | None:
    if ident == "stream1090:overlay_report":
        return stream1090_report_events_file, "overlay_stream1090"
    if ident == "stream1090:upstream_report":
        return upstream_report_events_file, "upstream_readsb_tar1090_stream1090"
    return None


def is_report_problem(item: dict, age_sec: int | None, *, max_age_sec: int) -> bool:
    if not item:
        return True
    if age_sec is None or age_sec > max_age_sec:
        return True
    baseline = item.get("baseline") if isinstance(item.get("baseline"), dict) else {}
    if bool(baseline.get("alert")):
        return True
    if str(item.get("judgment", "")) == "report_only_ok":
        return False
    warnings = item.get("warnings") if isinstance(item.get("warnings"), list) else []
    warning_set = {str(w) for w in warnings}
    if warning_set and warning_set.issubset(NOISE_ONLY_REPORT_WARNINGS):
        return False
    return True


def compact_report_evidence(item: dict, age_sec: int | None) -> str:
    if not item:
        return "latest report missing"
    checks = item.get("checks") if isinstance(item.get("checks"), dict) else {}
    baseline = item.get("baseline") if isinstance(item.get("baseline"), dict) else {}
    warnings = item.get("warnings") if isinstance(item.get("warnings"), list) else []
    parts = [
        f"judgment={item.get('judgment', '')}",
        f"age={seconds_to_human(age_sec)}",
        f"warn_rate_24h={baseline.get('warn_rate', 0)}",
        f"baseline_alert={baseline.get('alert', False)}",
        f"position_change={checks.get('position_change_count', 0)}",
        f"messages_delta={checks.get('messages_delta', '')}",
    ]
    if warnings:
        parts.append("warnings=" + ",".join(str(w) for w in warnings[:3]))
    return " ".join(parts)


def _compact_counts(value: object, *, limit: int = 3) -> str:
    if not isinstance(value, dict) or not value:
        return "{}"
    parts: list[str] = []
    for key, count in sorted(value.items(), key=lambda item: str(item[0]))[: max(1, limit)]:
        parts.append(f"{key}:{count}")
    suffix = "" if len(value) <= limit else f",+{len(value) - limit}"
    return "{" + ",".join(parts) + suffix + "}"


def compact_observe_context(payload: dict, checks: dict | None = None) -> str:
    checks = checks if isinstance(checks, dict) else {}
    parts = [
        f"pass={payload.get('pass', '')}",
        f"current_fail={checks.get('current_fail', '')}",
        f"historical_degraded={checks.get('historical_degraded', '')}",
        f"fast_recovery_1h={payload.get('fast_recovery_restart_count_1h', '')}",
        f"fast_recovery_24h={payload.get('fast_recovery_restart_count_24h', '')}",
        f"fr_triggers={_compact_counts(payload.get('fast_recovery_restart_triggers'))}",
        f"upload_p95={payload.get('ffmpeg_tcp_send_mbps_24h_p95', '')}",
        f"upload_max={payload.get('ffmpeg_tcp_send_mbps_24h_max', '')}",
        f"public_probe={payload.get('public_probe_judgment', '')}",
        f"api_report={payload.get('api_report_judgment', '')}",
    ]
    return " ".join(parts)[:320]


def compact_api_report_context(payload: dict) -> str:
    reports = payload.get("api_cost_reports") if isinstance(payload.get("api_cost_reports"), dict) else {}
    open_day = reports.get("open_day_latest") if isinstance(reports.get("open_day_latest"), dict) else {}
    closed_day = reports.get("closed_day_latest") if isinstance(reports.get("closed_day_latest"), dict) else {}
    timers = reports.get("timers") if isinstance(reports.get("timers"), dict) else {}
    timer_parts: list[str] = []
    for name, timer in sorted(timers.items(), key=lambda item: str(item[0]))[:3]:
        timer_parts.append(f"{name}:active={bool(isinstance(timer, dict) and timer.get('active') is True)}")
    return (
        f"open_day_fresh={payload.get('api_report_open_day_fresh', open_day.get('fresh', ''))} "
        f"closed_day_fresh={payload.get('api_report_closed_day_fresh', closed_day.get('fresh', ''))} "
        f"timers_active={payload.get('api_report_timers_active', '')} "
        f"timers={{{','.join(timer_parts)}}}"
    )[:320]


def compact_report_context(*, item: dict, age_sec: int | None, max_age_sec: int, path: Path) -> str:
    state = "missing" if not item else ("stale" if age_sec is None or age_sec > max_age_sec else "fresh")
    return (
        f"report_state={state} source={path.name} age={seconds_to_human(age_sec)} "
        f"max_age={seconds_to_human(max_age_sec)} target={item.get('target', '') if item else ''}"
    )[:320]


def _status_age(payload: dict, now_ts: int) -> tuple[int, int | None]:
    observed_ts = parse_utc_ts(str(payload.get("checked_at_utc") or payload.get("ts_utc") or ""))
    return observed_ts, max(0, now_ts - observed_ts) if observed_ts > 0 else None


def _trailing_problem_duration(
    path: Path,
    *,
    now_ts: int,
    predicate: Callable[[dict], bool],
    max_sample_gap_sec: int = 180,
) -> tuple[int, int]:
    started_ts = 0
    latest_ts = 0
    for item in iter_jsonl(path):
        ts = parse_utc_ts(str(item.get("checked_at_utc") or item.get("ts_utc") or ""))
        if ts <= 0 or ts > now_ts + 60:
            continue
        if latest_ts and ts - latest_ts > max_sample_gap_sec:
            started_ts = 0
        if predicate(item):
            if started_ts <= 0:
                started_ts = ts
        else:
            started_ts = 0
        latest_ts = ts
    if started_ts <= 0 or latest_ts <= 0 or now_ts - latest_ts > max_sample_gap_sec:
        return 0, 0
    return started_ts, max(0, latest_ts - started_ts)


def map_runtime_incidents(
    *,
    status_file: Path | None,
    history_file: Path | None,
    now_ts: int,
) -> list[dict]:
    if status_file is None:
        return []
    status = read_json_file(status_file)
    observed_ts, age = _status_age(status, now_ts)
    if not status or age is None or age > 360:
        return [
            incident(
                ident="map:monitor_missing_or_stale",
                severity="warning",
                component="production_map_monitoring",
                summary="production map monitor sample is missing or stale",
                evidence=f"sample_age={seconds_to_human(age)} source={status_file.name}",
                recovery_type="arena_monitor_probe_recovery",
                follow_up="arena monitor control loop と map_runtime_probe の更新を確認する",
                observed_ts=observed_ts or now_ts,
            )
        ]
    if history_file is None:
        return []

    incidents: list[dict] = []
    webgl_bad = lambda item: item.get("webgl2_blocklisted") is True or item.get("webgl_context_fatal") is True
    webgl_since, webgl_sec = _trailing_problem_duration(history_file, now_ts=now_ts, predicate=webgl_bad)
    delivery_since, delivery_sec = _trailing_problem_duration(
        history_file,
        now_ts=now_ts,
        predicate=lambda item: item.get("delivery_critical_ok") is False,
    )
    weather_since, weather_sec = _trailing_problem_duration(
        history_file,
        now_ts=now_ts,
        predicate=lambda item: item.get("weather_ok") is False,
    )
    restart_since, restart_sec = _trailing_problem_duration(
        history_file,
        now_ts=now_ts,
        predicate=lambda item: any(
            int(value or 0) > 0
            for value in (item.get("container_restart_counts") or {}).values()
        ),
    )

    browser = status.get("browser") if isinstance(status.get("browser"), dict) else {}
    if webgl_sec >= 60:
        incidents.append(
            incident(
                ident="map:webgl_failure",
                severity="critical",
                component="production_map_delivery",
                summary="production Chromium reported a WebGL failure",
                evidence=(
                    f"active={seconds_to_human(webgl_sec)} "
                    f"webgl2_blocklisted={browser.get('webgl2_blocklisted')} "
                    f"context_fatal={browser.get('context_fatal_failure')}"
                ),
                recovery_type="runtime_browser_or_pod_recovery",
                follow_up="browser.log、stream_engine_events.jsonl、render heartbeatを突き合わせる",
                observed_ts=webgl_since,
            )
        )
    elif delivery_sec >= 120:
        incidents.append(
            incident(
                ident="map:delivery_critical",
                severity="critical",
                component="production_map_delivery",
                summary="production map delivery contract is unhealthy",
                evidence=f"active={seconds_to_human(delivery_sec)} reasons={','.join(str(v) for v in status.get('critical_reasons', [])[:5])}",
                recovery_type="runtime_delivery_recovery",
                follow_up="Pod topology、NVENC/RTMP、render heartbeat、Chromiumを確認する",
                observed_ts=delivery_since,
            )
        )
    if weather_sec >= 1200:
        incidents.append(
            incident(
                ident="map:precipitation_unavailable",
                severity="warning",
                component="production_map_precipitation",
                summary="JMA precipitation layer or fetcher is unhealthy",
                evidence=f"active={seconds_to_human(weather_sec)} reasons={','.join(str(v) for v in status.get('weather_reasons', [])[:4])}",
                recovery_type="precipitation_fetch_retry_recovery",
                follow_up="降水status/healthとJMA取得履歴を確認する。配信runtimeは自動restartしない",
                observed_ts=weather_since,
            )
        )
    if restart_sec >= 120:
        pod = status.get("pod") if isinstance(status.get("pod"), dict) else {}
        containers = pod.get("containers") if isinstance(pod.get("containers"), dict) else {}
        restarted = [f"{name}:{item.get('restart_count')}" for name, item in containers.items() if isinstance(item, dict) and int(item.get("restart_count") or 0) > 0]
        incidents.append(
            incident(
                ident="map:container_restart",
                severity="warning",
                component="production_map_runtime",
                summary="a production map runtime container has restarted",
                evidence=f"active={seconds_to_human(restart_sec)} containers={','.join(restarted[:5])}",
                recovery_type="k8s_container_self_recovery",
                follow_up="Pod/container historyとrestart前後の永続event logを確認する",
                observed_ts=restart_since,
            )
        )
    return incidents


def viewer_synthetic_incidents(*, status_file: Path | None, now_ts: int) -> list[dict]:
    if status_file is None:
        return []
    status = read_json_file(status_file)
    observed_ts, age = _status_age(status, now_ts)
    if not status or age is None or age > 900:
        return [
            incident(
                ident="viewer:synthetic_missing_or_stale",
                severity="warning",
                component="youtube_viewer_synthetic",
                summary="viewer-side synthetic sample is missing or stale",
                evidence=f"sample_age={seconds_to_human(age)} source={status_file.name}",
                recovery_type="viewer_probe_recovery",
                follow_up="yt-dlp/ffmpegとarena monitor viewer probeの更新を確認する",
                observed_ts=observed_ts or now_ts,
            )
        ]
    visual_failures = int(status.get("consecutive_visual_failures") or 0)
    probe_failures = int(status.get("consecutive_probe_failures") or 0)
    if visual_failures >= 2:
        return [
            incident(
                ident="viewer:visual_failure",
                severity="critical",
                component="youtube_viewer_delivery",
                summary="public viewer frames are repeatedly black or frozen",
                evidence=(
                    f"visual_failures={visual_failures} black={status.get('black_detected')} "
                    f"freeze={status.get('freeze_detected')} video_id={status.get('video_id', '')}"
                ),
                recovery_type="youtube_or_runtime_delivery_recovery",
                follow_up="viewer capture、YouTube health、local NVENC/RTMP、render heartbeatを突き合わせる",
                observed_ts=observed_ts,
            )
        ]
    if probe_failures >= 2:
        return [
            incident(
                ident="viewer:synthetic_probe_failed",
                severity="warning",
                component="youtube_viewer_synthetic",
                summary="viewer-side synthetic capture repeatedly failed",
                evidence=f"probe_failures={probe_failures} reason={status.get('reason', '')}",
                recovery_type="viewer_probe_or_public_path_recovery",
                follow_up="YouTube live stateとyt-dlp/ffmpeg probeを確認し、local delivery failureと分離する",
                observed_ts=observed_ts,
            )
        ]
    return []


def recovery_type_from_observe(payload: dict) -> str:
    if payload.get("watchdog_restart_reasons"):
        return "stream_watchdog_restart"
    triggers = payload.get("fast_recovery_restart_triggers")
    if isinstance(triggers, dict) and triggers:
        trigger = sorted(triggers.items(), key=lambda part: str(part[0]))[0][0]
        return f"fast_recovery_restart:{trigger}"
    if int(payload.get("stream_engine_ffmpeg_exit_224_count_1h", 0) or 0) > 0:
        return "ffmpeg_child_self_recovery:exit_224_broken_pipe"
    return "observe_only_pending_or_external_recovery"


def observe_payload_has_current_stream_problem(checks: dict, payload: dict) -> bool:
    if is_monitoring_only_youtube_stats_gap(checks):
        return False
    if checks.get("current_fail") is True:
        return True
    if checks.get("youtube_current_degraded") is True:
        return True
    if checks.get("youtube_observability_current_fail") is True:
        return True
    if checks.get("fast_mode_current_active") is True:
        return True
    if payload.get("fast_mode_current_active") is True:
        return True
    return False


def fast_mode_notification_currently_active(checks: dict, payload: dict) -> bool:
    return checks.get("fast_mode_current_active") is True or payload.get("fast_mode_current_active") is True


def youtube_encoder_gap_currently_active(stats: dict, *, now_ts: int | None = None, max_age_sec: int = 300) -> bool:
    if not isinstance(stats, dict) or not stats:
        return False

    stats_ts = parse_utc_ts(
        str(
            stats.get("stats_file_updated_at_utc")
            or stats.get("ts_utc")
            or stats.get("remote_probe_ts_utc")
            or ""
        )
    )
    if stats_ts <= 0:
        return False
    now = int(time.time() if now_ts is None else now_ts)
    if max(0, now - stats_ts) > max_age_sec:
        return False

    if stats.get("oauth_enable_auto_stop") is not False:
        return False

    remote_live = str(stats.get("api_live_state", "")).lower() == "live"
    lifecycle = str(stats.get("oauth_life_cycle_status", "")).lower()
    if lifecycle in {"live", "livestarting", "testing", "teststarting"}:
        remote_live = True

    try:
        ffmpeg_pid = int(stats.get("ffmpeg_pid") or 0)
    except Exception:
        ffmpeg_pid = 0
    encoder_ok = (
        stats.get("stream_active") is True
        and stats.get("ingest_connected") is True
        and stats.get("local_ok") is True
        and ffmpeg_pid > 1
    )
    return remote_live and not encoder_ok


def is_bootstrap_youtube_stats_gap(checks: dict) -> bool:
    return is_monitoring_only_youtube_stats_gap(checks)


def is_monitoring_only_youtube_stats_gap(checks: dict) -> bool:
    return (
        checks.get("current_fail") is True
        and checks.get("youtube_stats_stale") is True
        and not str(checks.get("youtube_current_status", "") or "").strip()
        and not str(checks.get("youtube_current_judgment", "") or "").strip()
        and checks.get("youtube_observability_current_fail") is not True
        and checks.get("fast_mode_current_active") is not True
        and checks.get("pulse_pass") is True
    )


def is_bootstrap_api_report_gap(payload: dict) -> bool:
    judgment = str(payload.get("api_report_judgment", "") or "")
    reason = str(payload.get("api_report_judgment_reason", "") or "").lower()
    return judgment == "api_open_day_report_stale" and ("missing" in reason or "stale" in reason)


def is_api_report_timer_monitoring_gap(payload: dict) -> bool:
    if str(payload.get("api_report_judgment", "") or "") != "api_report_timer_attention":
        return False
    if payload.get("api_report_timers_active") is False:
        return True
    reports = payload.get("api_cost_reports") if isinstance(payload.get("api_cost_reports"), dict) else {}
    timers = reports.get("timers") if isinstance(reports.get("timers"), dict) else {}
    if not timers:
        return False
    return not any(isinstance(item, dict) and item.get("active") is True for item in timers.values())


def is_report_output_gap(item: dict, age_sec: int | None, *, max_age_sec: int) -> bool:
    return not item or age_sec is None or age_sec > max_age_sec


def incident(
    *,
    ident: str,
    severity: str,
    component: str,
    summary: str,
    evidence: str,
    recovery_type: str,
    follow_up: str,
    observed_ts: int | None = None,
    diagnostic_context: str = "",
) -> dict:
    payload = {
        "id": ident,
        "severity": severity,
        "component": component,
        "summary": summary,
        "evidence": evidence,
        "recovery_type": recovery_type,
        "follow_up": follow_up,
    }
    if diagnostic_context:
        payload["diagnostic_context"] = str(diagnostic_context)[:320]
    try:
        value = int(observed_ts or 0)
    except Exception:
        value = 0
    if value > 0:
        payload["observed_ts"] = value
    return payload


def collect_notification_incidents(
    *,
    observe_payload: ObservePayload,
    stream1090_report_events_file: Path,
    upstream_report_events_file: Path,
    youtube_watchdog_stats_file: Path | None = None,
    map_runtime_status_file: Path | None = None,
    map_runtime_history_file: Path | None = None,
    viewer_synthetic_status_file: Path | None = None,
    now_ts: int | None = None,
    report_stale_sec: int = 1800,
    bootstrap_grace_active: bool = False,
) -> list[dict]:
    now = int(time.time() if now_ts is None else now_ts)
    incidents: list[dict] = []
    if not bootstrap_grace_active or (map_runtime_status_file is not None and map_runtime_status_file.exists()):
        incidents.extend(
            map_runtime_incidents(
                status_file=map_runtime_status_file,
                history_file=map_runtime_history_file,
                now_ts=now,
            )
        )
    if not bootstrap_grace_active or (
        viewer_synthetic_status_file is not None and viewer_synthetic_status_file.exists()
    ):
        incidents.extend(viewer_synthetic_incidents(status_file=viewer_synthetic_status_file, now_ts=now))
    _rc, payload, error = observe_payload(24)
    checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
    recovery_type = recovery_type_from_observe(payload)
    observe_context = compact_observe_context(payload, checks)

    if error and not payload:
        incidents.append(
            incident(
                ident="observe:execution_failed",
                severity="critical",
                component="observe_stream_health",
                summary="observe_stream_health.py execution failed",
                evidence=error[:240],
                recovery_type="timer_or_script_recovery",
                follow_up="observe script stderr and systemd notify timer statusを確認する",
            )
        )
    monitoring_only_youtube_gap = is_monitoring_only_youtube_stats_gap(checks)

    if (
        checks.get("current_fail") is True
        and not monitoring_only_youtube_gap
        and not (bootstrap_grace_active and is_bootstrap_youtube_stats_gap(checks))
    ):
        incidents.append(
            incident(
                ident="stream:current_fail",
                severity="critical",
                component="stream_health",
                summary="current_fail=true",
                evidence=(
                    f"youtube_status={checks.get('youtube_current_status')} "
                    f"youtube_judgment={checks.get('youtube_current_judgment')} "
                    f"youtube_stats_stale={checks.get('youtube_stats_stale')} "
                    f"pulse_pass={checks.get('pulse_pass')}"
                ),
                recovery_type=recovery_type,
                follow_up="recovery event後に health-summary と stream_engine_events.jsonl を突き合わせる",
                diagnostic_context=observe_context,
            )
        )
    elif checks.get("youtube_current_degraded") is True and not monitoring_only_youtube_gap:
        incidents.append(
            incident(
                ident="youtube:current_degraded",
                severity="warning",
                component="youtube_remote_observability",
                summary="YouTube current state is degraded but not current_fail",
                evidence=(
                    f"youtube_status={checks.get('youtube_current_status')} "
                    f"remote_status={checks.get('youtube_current_remote_status')}"
                ),
                recovery_type=recovery_type,
                follow_up="OAuth/Data API/local ingest のどれが degraded を支えているか確認する",
                diagnostic_context=observe_context,
            )
        )

    if (
        payload.get("api_report_judgment") != "ok"
        and not is_api_report_timer_monitoring_gap(payload)
        and not (bootstrap_grace_active and is_bootstrap_api_report_gap(payload))
    ):
        incidents.append(
            incident(
                ident="api_report:freshness_or_timer",
                severity="warning",
                component="youtube_api_usage_report",
                summary=f"api_report_judgment={payload.get('api_report_judgment')}",
                evidence=str(payload.get("api_report_judgment_reason", "")),
                recovery_type="api_cost_report_timer_recovery",
                follow_up="latest.json/open_day_latest.json と api cost report timer の更新時刻を確認する",
                diagnostic_context=compact_api_report_context(payload),
            )
        )

    if fast_mode_notification_currently_active(checks, payload):
        incidents.append(
            incident(
                ident="resolver:fast_mode_active_or_runaway",
                severity="warning",
                component="youtube_video_id_resolver_fast_mode",
                summary=f"fast_mode_judgment={payload.get('fast_mode_judgment')}",
                evidence=(
                    f"active={payload.get('fast_mode_current_active')} "
                    f"episodes_24h={payload.get('fast_mode_episode_count_24h')} "
                    f"duration_24h={payload.get('fast_mode_active_duration_sec_24h')} "
                    f"units_est={payload.get('fast_mode_api_units_estimated_24h')}"
                ),
                recovery_type="resolver_hysteresis_exit_or_api_guard",
                follow_up="fast mode exit 条件と PT API usage の増分を確認する",
                diagnostic_context=observe_context,
            )
        )

    if payload.get("encoder_gap_enable_auto_stop_false_judgment") in {
        "observe_encoder_gap_viewer_state",
        "investigate_encoder_gap_viewer_state",
    } and youtube_encoder_gap_currently_active(
        read_json_file(youtube_watchdog_stats_file) if youtube_watchdog_stats_file else {},
        now_ts=now,
    ):
        incidents.append(
            incident(
                ident="youtube:enable_auto_stop_false_encoder_gap",
                severity="warning",
                component="youtube_encoder_viewer_state",
                summary=str(payload.get("encoder_gap_enable_auto_stop_false_judgment")),
                evidence=(
                    f"samples_24h={payload.get('encoder_gap_enable_auto_stop_false_sample_count_24h')} "
                    f"duration_24h={payload.get('encoder_gap_enable_auto_stop_false_duration_sec_24h')}"
                ),
                recovery_type="encoder_or_stream_restart_if_current_fail",
                follow_up="YouTube public state と local encoder state の gap が回復したか確認する",
                diagnostic_context=observe_context,
            )
        )

    if payload.get("remote_warning_restart_judgment") in {"review_confirm_condition_immediate", "review_confirm_condition"}:
        incidents.append(
            incident(
                ident="fast_recovery:remote_warning_restart_repeated",
                severity="warning",
                component="fast_recovery_remote_warning",
                summary=str(payload.get("remote_warning_restart_judgment")),
                evidence=(
                    f"count_1h={payload.get('remote_warning_restart_count_1h')} "
                    f"count_24h={payload.get('remote_warning_restart_count_24h')}"
                ),
                recovery_type="fast_recovery_restart:remote_warning",
                follow_up="remote-warning-compare で local TCP と YouTube remote warning を再分離する",
                diagnostic_context=observe_context,
            )
        )

    if payload.get("stream_engine_ffmpeg_exit_224_judgment") in {"investigate_immediate", "investigate_network_or_ingest"}:
        incidents.append(
            incident(
                ident="ffmpeg:exit_224_repeated",
                severity="warning",
                component="rtmp_ffmpeg_transport",
                summary=str(payload.get("stream_engine_ffmpeg_exit_224_judgment")),
                evidence=(
                    f"count_1h={payload.get('stream_engine_ffmpeg_exit_224_count_1h')} "
                    f"count_24h={payload.get('stream_engine_ffmpeg_exit_224_count_24h')}"
                ),
                recovery_type="ffmpeg_child_self_recovery:exit_224_broken_pipe",
                follow_up="Broken pipe が複数回なら ISP/RTMP ingest/上流ネットワークの切り分けに昇格する",
                diagnostic_context=observe_context,
            )
        )

    current_stream_problem = observe_payload_has_current_stream_problem(checks, payload)

    if current_stream_problem and payload.get("rtmps_ssl_tls_judgment") in {
        "investigate_rtmps_ssl_tls_immediate",
        "investigate_rtmps_ssl_tls_repeated",
    }:
        journal_ssl_tls = payload.get("journal_ssl_tls") if isinstance(payload.get("journal_ssl_tls"), dict) else {}
        incidents.append(
            incident(
                ident="rtmps:ssl_tls_specific_event",
                severity="warning",
                component="rtmps_ingest_tls",
                summary=str(payload.get("rtmps_ssl_tls_judgment")),
                evidence=(
                    f"count_1h={payload.get('rtmps_ssl_tls_count_1h')} "
                    f"count_24h={payload.get('rtmps_ssl_tls_count_24h')} "
                    f"stream_engine={payload.get('stream_engine_ffmpeg_ssl_tls_count_24h')} "
                    f"fast_recovery={payload.get('fast_recovery_ssl_tls_count_24h')} "
                    f"journal={journal_ssl_tls.get('count_24h')}"
                ),
                recovery_type="observe_rtmps_ssl_tls_before_transport_reclassification",
                follow_up="journal / stream_engine_events / fast_recovery_events の SSL/TLS reason を見て RTMPS 固有か通常 transport か切り分ける",
                diagnostic_context=observe_context,
            )
        )

    if current_stream_problem and payload.get("public_probe_judgment") in {
        "observe_public_probe_noise_clustered",
        "observe_public_probe_noise_frequent",
    }:
        incidents.append(
            incident(
                ident="public_probe:429_or_bot_confirmation_repeated",
                severity="info",
                component="public_watch_page_probe",
                summary=str(payload.get("public_probe_judgment")),
                evidence=(
                    f"count_1h={payload.get('public_probe_degraded_count_1h')} "
                    f"count_24h={payload.get('public_probe_degraded_count_24h')} "
                    f"live_ok_24h={payload.get('public_probe_authoritative_live_ok_count_24h')}"
                ),
                recovery_type="observe_only_no_restart_when_oauth_data_api_local_ok",
                follow_up="OAuth/Data API/local ingest が正常なら outage ではなく観測ノイズとして扱う",
                diagnostic_context=observe_context,
            )
        )

    report_specs = (
        (
            "stream1090:overlay_report",
            "overlay_stream1090",
            stream1090_report_events_file,
            "overlay_stream1090",
            "local stream1090 overlay report",
        ),
        (
            "stream1090:upstream_report",
            "upstream_readsb_tar1090_stream1090",
            upstream_report_events_file,
            "upstream_readsb_tar1090_stream1090",
            "upstream readsb/tar1090/stream1090 report",
        ),
    )
    for ident, component, path, target, label in report_specs:
        item, age = latest_jsonl_item(path, target=target, now_ts=now)
        if is_report_problem(item, age, max_age_sec=report_stale_sec):
            if is_report_output_gap(item, age, max_age_sec=report_stale_sec) and not current_stream_problem:
                continue
            if bootstrap_grace_active and not item:
                continue
            observed_ts = parse_utc_ts(str(item.get("ts_utc", ""))) if item else None
            incidents.append(
                incident(
                    ident=ident,
                    severity="warning",
                    component=component,
                    summary=f"{label} is not report_only_ok",
                    evidence=compact_report_evidence(item, age),
                    recovery_type="report_only_observation_no_stream_restart",
                    follow_up="次の report-only sample で report_only_ok に戻るか確認する。連続するなら upstream/overlay を個別復旧する",
                    observed_ts=observed_ts,
                    diagnostic_context=compact_report_context(
                        item=item,
                        age_sec=age,
                        max_age_sec=report_stale_sec,
                        path=path,
                    ),
                )
            )

    return incidents


def recovery_observation_for_incident(
    ident: str,
    now_ts: int,
    *,
    stream1090_report_events_file: Path,
    upstream_report_events_file: Path,
    observe_payload: ObservePayload | None = None,
) -> tuple[int, str]:
    spec = report_incident_spec(
        ident,
        stream1090_report_events_file=stream1090_report_events_file,
        upstream_report_events_file=upstream_report_events_file,
    )
    if spec is None:
        if observe_payload is None:
            return now_ts, "current incident absent; no component-specific recovery probe configured"
        _rc, payload, error = observe_payload(24)
        if error and not payload:
            return now_ts, f"current incident absent; observe_probe_error={error[:160]}"
        checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
        if ident == "stream:current_fail":
            return (
                now_ts,
                (
                    f"current_fail={checks.get('current_fail')} "
                    f"youtube_status={checks.get('youtube_current_status')} "
                    f"youtube_judgment={checks.get('youtube_current_judgment')} "
                    f"youtube_stats_stale={checks.get('youtube_stats_stale')} "
                    f"pulse_pass={checks.get('pulse_pass')} "
                    f"pass={payload.get('pass')}"
                )[:320],
            )
        if ident == "api_report:freshness_or_timer":
            return now_ts, compact_api_report_context(payload)
        return now_ts, f"current incident absent; {compact_observe_context(payload, checks)}"
    path, target = spec
    item, age = latest_jsonl_item(path, target=target, now_ts=now_ts)
    observed_ts = parse_utc_ts(str(item.get("ts_utc", ""))) if item else 0
    if observed_ts <= 0:
        observed_ts = now_ts
    return observed_ts, compact_report_evidence(item, age)
