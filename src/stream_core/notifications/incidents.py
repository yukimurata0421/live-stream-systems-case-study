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
    return (
        checks.get("current_fail") is True
        and checks.get("youtube_stats_stale") is True
        and not str(checks.get("youtube_current_status", "") or "").strip()
        and not str(checks.get("youtube_current_judgment", "") or "").strip()
        and checks.get("pulse_pass") is True
    )


def is_bootstrap_api_report_gap(payload: dict) -> bool:
    judgment = str(payload.get("api_report_judgment", "") or "")
    reason = str(payload.get("api_report_judgment_reason", "") or "").lower()
    return judgment == "api_open_day_report_stale" and ("missing" in reason or "stale" in reason)


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
    now_ts: int | None = None,
    report_stale_sec: int = 1800,
    bootstrap_grace_active: bool = False,
) -> list[dict]:
    now = int(time.time() if now_ts is None else now_ts)
    incidents: list[dict] = []
    _rc, payload, error = observe_payload(24)
    checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
    recovery_type = recovery_type_from_observe(payload)

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
    if checks.get("current_fail") is True and not (bootstrap_grace_active and is_bootstrap_youtube_stats_gap(checks)):
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
            )
        )
    elif checks.get("youtube_current_degraded") is True:
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
            )
        )

    if payload.get("api_report_judgment") != "ok" and not (bootstrap_grace_active and is_bootstrap_api_report_gap(payload)):
        incidents.append(
            incident(
                ident="api_report:freshness_or_timer",
                severity="warning",
                component="youtube_api_usage_report",
                summary=f"api_report_judgment={payload.get('api_report_judgment')}",
                evidence=str(payload.get("api_report_judgment_reason", "")),
                recovery_type="api_cost_report_timer_recovery",
                follow_up="latest.json/open_day_latest.json と api cost report timer の更新時刻を確認する",
            )
        )

    if checks.get("fast_mode_current_active") is True or payload.get("fast_mode_judgment") == "investigate_fast_mode_runaway":
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
                )
            )

    return incidents


def recovery_observation_for_incident(
    ident: str,
    now_ts: int,
    *,
    stream1090_report_events_file: Path,
    upstream_report_events_file: Path,
) -> tuple[int, str]:
    spec = report_incident_spec(
        ident,
        stream1090_report_events_file=stream1090_report_events_file,
        upstream_report_events_file=upstream_report_events_file,
    )
    if spec is None:
        return now_ts, ""
    path, target = spec
    item, age = latest_jsonl_item(path, target=target, now_ts=now_ts)
    observed_ts = parse_utc_ts(str(item.get("ts_utc", ""))) if item else 0
    if observed_ts <= 0:
        observed_ts = now_ts
    return observed_ts, compact_report_evidence(item, age)
