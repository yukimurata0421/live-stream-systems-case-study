from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

try:
    from stream_core.common.ffmpeg_restarts import summarize_ffmpeg_restart_attempts
    from stream_core.common.json_io import append_jsonl, iter_jsonl
    from stream_core.common.timeutil import jst_day, jst_text_or_unknown, parse_utc_ts, pt_day, utc_now_text, utc_text_from_ts
except ModuleNotFoundError:
    from common.ffmpeg_restarts import summarize_ffmpeg_restart_attempts
    from common.json_io import append_jsonl, iter_jsonl
    from common.timeutil import jst_day, jst_text_or_unknown, parse_utc_ts, pt_day, utc_now_text, utc_text_from_ts


OBJECTIVE_SLI_REGIMES: dict[str, dict[str, str]] = {
    "post_stabilization": {
        "regime_start_ts_utc": "2026-05-07T00:00:00Z",
        "regime_start_jst": "2026-05-07 09:00:00 JST",
        "regime_reason": "post 2026-05-05..06 unstable period; watchdog/recovery design stabilized",
    },
    "report_only_visual": {
        "regime_start_ts_utc": "2026-05-09T20:00:00Z",
        "regime_start_jst": "2026-05-10 05:00:00 JST",
        "regime_reason": "stream1090/upstream report-only SLI started",
    },
    "upload_budget": {
        "regime_start_ts_utc": "2026-05-10T03:33:05Z",
        "regime_start_jst": "2026-05-10 12:33:05 JST",
        "regime_reason": "ffmpeg tcp send Mbps samples started",
    },
    "rtmps_low_bandwidth": {
        "regime_start_ts_utc": "2026-05-10T03:21:00Z",
        "regime_start_jst": "2026-05-10 12:21:00 JST",
        "regime_reason": "RTMPS/443 with low-bandwidth fps/bitrate contract",
    },
}
MIB = 1024 * 1024


@dataclass(frozen=True)
class ObjectiveSliContext:
    log_base_dir: Path
    youtube_watchdog_events_file: Path
    fast_recovery_events_file: Path
    stream_engine_events_file: Path
    stream1090_report_events_file: Path
    upstream_report_events_file: Path
    notify_events_file: Path
    memory_status_events_file: Path
    objective_sli_file: Path
    objective_sli_events_file: Path


def ratio_percent(numerator: int | float, denominator: int | float) -> float | None:
    if denominator == 0:
        return None
    return round((float(numerator) / float(denominator)) * 100.0, 3)


def percentile(values: list[float], percentile_value: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    rank = (len(ordered) - 1) * (percentile_value / 100.0)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 3)


def timestamped_jsonl_items(path: Path) -> list[tuple[int, dict]]:
    items: list[tuple[int, dict]] = []
    for payload in iter_jsonl(path):
        ts = parse_utc_ts(str(payload.get("ts_utc", "")))
        if ts <= 0:
            continue
        items.append((ts, payload))
    return sorted(items, key=lambda item: item[0])


def time_bounds(items: list[tuple[int, dict]]) -> dict:
    if not items:
        return {
            "sample_count": 0,
            "start_ts_utc": "",
            "start_jst": "",
            "end_ts_utc": "",
            "end_jst": "",
            "coverage_sec": 0,
        }
    start_ts = items[0][0]
    end_ts = items[-1][0]
    return {
        "sample_count": len(items),
        "start_ts_utc": utc_text_from_ts(start_ts),
        "start_jst": jst_text_or_unknown(start_ts),
        "end_ts_utc": utc_text_from_ts(end_ts),
        "end_jst": jst_text_or_unknown(end_ts),
        "coverage_sec": max(0, end_ts - start_ts),
    }


def youtube_sli_for_items(items: list[tuple[int, dict]]) -> dict:
    bounds = time_bounds(items)
    ok_count = sum(1 for _ts, payload in items if payload.get("status") == "ok")
    bad_count = len(items) - ok_count
    return {
        **bounds,
        "ok_count": ok_count,
        "bad_count": bad_count,
        "ok_ratio_pct": ratio_percent(ok_count, len(items)),
        "denominator": "youtube_watchdog.jsonl samples with ts_utc",
        "ok_definition": "status == ok",
    }


def stream_watchdog_sli(items: list[tuple[int, dict]]) -> dict:
    kinds: dict[str, int] = {}
    triggers: dict[str, int] = {}
    for _ts, payload in items:
        kind = str(payload.get("kind") or payload.get("status") or "unknown")
        kinds[kind] = kinds.get(kind, 0) + 1
        trigger = payload.get("trigger") or payload.get("restart_reason")
        if trigger:
            trigger_key = str(trigger)
            triggers[trigger_key] = triggers.get(trigger_key, 0) + 1
    return {
        **time_bounds(items),
        "by_kind": kinds,
        "restart_triggers": triggers,
        "denominator": "stream_watchdog_events.jsonl samples with ts_utc",
    }


def fast_recovery_sli(items: list[tuple[int, dict]]) -> dict:
    restart_items = [(ts, p) for ts, p in items if p.get("kind") == "restart"]
    triggers: dict[str, int] = {}
    for _ts, payload in restart_items:
        trigger = str(payload.get("trigger") or "unknown")
        triggers[trigger] = triggers.get(trigger, 0) + 1
    bounds = time_bounds(items)
    days = max(bounds["coverage_sec"] / 86400.0, 1.0 / 86400.0) if items else 0.0
    return {
        **bounds,
        "restart_count": len(restart_items),
        "restart_per_day": round(len(restart_items) / days, 3) if days > 0 else None,
        "restart_by_trigger": triggers,
        "denominator": "fast_recovery_events.jsonl restart events over observed coverage",
    }


def upload_budget_sli(items: list[tuple[int, dict]]) -> dict:
    samples = [(ts, p) for ts, p in items if p.get("kind") == "tcp_send_sample"]
    mbps_values: list[float] = []
    over_5_sec = 0.0
    sampled_sec = 0.0
    for _ts, payload in samples:
        try:
            mbps = float(payload.get("send_mbps", payload.get("mbps")))
        except Exception:
            continue
        try:
            interval = float(payload.get("sample_interval_sec", 0) or 0)
        except Exception:
            interval = 0.0
        mbps_values.append(mbps)
        sampled_sec += max(0.0, interval)
        if mbps > 5.0:
            over_5_sec += max(0.0, interval)
    within_sec = max(0.0, sampled_sec - over_5_sec)
    return {
        **time_bounds(samples),
        "p50_mbps": percentile(mbps_values, 50),
        "p95_mbps": percentile(mbps_values, 95),
        "p99_mbps": percentile(mbps_values, 99),
        "max_mbps": round(max(mbps_values), 3) if mbps_values else None,
        "sampled_sec": round(sampled_sec, 3),
        "over_5mbps_sec": round(over_5_sec, 3),
        "within_5mbps_ratio_pct": ratio_percent(within_sec, sampled_sec),
        "budget_mbps": 5.0,
        "denominator": "fast_recovery_events.jsonl kind=tcp_send_sample sampled seconds",
    }


def stream_engine_sli(items: list[tuple[int, dict]]) -> dict:
    exit224 = 0
    self_recovery = 0
    by_kind: dict[str, int] = {}
    for _ts, payload in items:
        kind = str(payload.get("event_type") or payload.get("kind") or "unknown")
        by_kind[kind] = by_kind.get(kind, 0) + 1
        if payload.get("exit_code") == 224 or payload.get("returncode") == 224 or payload.get("code") == 224:
            exit224 += 1
        if kind in {"ffmpeg_restart_scheduled", "ffmpeg_restarted", "self_recovery"}:
            self_recovery += 1
    restart_summary = summarize_ffmpeg_restart_attempts(items)
    return {
        **time_bounds(items),
        "ffmpeg_exit_224_count": exit224,
        "self_recovery_count": self_recovery,
        "ffmpeg_restart_attempt_count": restart_summary["attempt_count"],
        "ffmpeg_restart_episode_count": restart_summary["retry_episode_count"],
        "ffmpeg_restart_retry_episode_count": restart_summary["retry_episode_count"],
        "ffmpeg_restart_incident_cluster_count": restart_summary["incident_cluster_count"],
        "ffmpeg_restart_episodes_root_cause": restart_summary["episode_root_causes"],
        "ffmpeg_restart_episode_root_causes": restart_summary["episode_root_causes"],
        "ffmpeg_restart_incident_root_causes": restart_summary["incident_root_causes"],
        "ffmpeg_restart_max_episode_duration_sec": restart_summary["max_episode_duration_sec"],
        "ffmpeg_restart_max_attempts_per_episode": restart_summary["max_attempts_per_episode"],
        "by_kind": by_kind,
        "denominator": "stream_engine_events.jsonl samples with ts_utc",
    }


def report_only_sli(items: list[tuple[int, dict]], *, expected_target: str) -> dict:
    scoped = [
        (ts, payload)
        for ts, payload in items
        if not payload.get("target") or payload.get("target") == expected_target
    ]
    ok_count = sum(1 for _ts, payload in scoped if payload.get("judgment") == "report_only_ok")
    warn_count = len(scoped) - ok_count
    incident_count = 0
    degraded_sec = 0
    incident_start: int | None = None
    last_bad_ts: int | None = None
    for ts, payload in scoped:
        is_ok = payload.get("judgment") == "report_only_ok"
        if is_ok:
            if incident_start is not None:
                incident_count += 1
                degraded_sec += max(0, ts - incident_start)
                incident_start = None
            last_bad_ts = None
            continue
        if incident_start is None:
            incident_start = ts
        last_bad_ts = ts
    if incident_start is not None:
        incident_count += 1
        degraded_sec += max(0, (last_bad_ts or incident_start) - incident_start)
    bounds = time_bounds(scoped)
    coverage_sec = bounds.get("coverage_sec", 0)
    availability = ratio_percent(max(0, coverage_sec - degraded_sec), coverage_sec)
    return {
        **bounds,
        "ok_count": ok_count,
        "warn_count": warn_count,
        "ok_ratio_pct": ratio_percent(ok_count, len(scoped)),
        "incident_count": incident_count,
        "degraded_sec": degraded_sec,
        "time_availability_pct": availability,
        "denominator": f"{expected_target} report-only samples with ts_utc",
    }


def discord_notify_sli(items: list[tuple[int, dict]]) -> dict:
    delivery = [(ts, p) for ts, p in items if p.get("kind") in {"send_ok", "send_failed"} or "send_ok" in p]
    ok_count = 0
    fail_count = 0
    by_kind: dict[str, int] = {}
    for _ts, payload in delivery:
        kind = str(payload.get("kind") or payload.get("phase") or "unknown")
        by_kind[kind] = by_kind.get(kind, 0) + 1
        if payload.get("kind") == "send_ok" or payload.get("send_ok") is True:
            ok_count += 1
        elif payload.get("kind") == "send_failed" or payload.get("send_ok") is False:
            fail_count += 1
    return {
        **time_bounds(delivery),
        "send_ok_count": ok_count,
        "send_failed_count": fail_count,
        "delivery_ratio_pct": ratio_percent(ok_count, ok_count + fail_count),
        "by_kind": by_kind,
        "denominator": "stream_notify_events.jsonl delivery events",
    }


def api_usage_sli(items: list[tuple[int, dict]]) -> dict:
    by_pt_day: dict[str, dict[str, int]] = {}
    for ts, payload in items:
        day = pt_day(ts)
        bucket = by_pt_day.setdefault(day, {"calls": 0, "units": 0, "quota_exceeded_events": 0})
        bucket["calls"] += 1
        try:
            bucket["units"] += int(payload.get("cost_units", payload.get("units", 1)) or 0)
        except Exception:
            bucket["units"] += 0
        if payload.get("quota_exceeded") is True or payload.get("error_reason") == "quotaExceeded":
            bucket["quota_exceeded_events"] += 1
    return {
        **time_bounds(items),
        "quota_day_timezone": "America/Los_Angeles",
        "by_pt_day": [
            {"pt_day": day, **values}
            for day, values in sorted(by_pt_day.items())
        ],
        "denominator": "youtube_api_calls.jsonl calls with ts_utc",
    }


def memory_guardrail_sli(items: list[tuple[int, dict]]) -> dict:
    by_severity: dict[str, int] = {}
    by_policy_version: dict[str, int] = {}
    by_operational_adequacy_severity: dict[str, int] = {}
    by_active_oneshot_peak_severity: dict[str, int] = {}
    by_systemd_peak_history_severity: dict[str, int] = {}
    critical_count = 0
    warn_count = 0
    operational_adequacy_warn_count = 0
    operational_adequacy_critical_count = 0
    active_oneshot_peak_warn_count = 0
    active_oneshot_peak_critical_count = 0
    systemd_peak_history_warn_count = 0
    systemd_peak_history_critical_count = 0
    host_non_reclaimable_mib: list[float] = []
    host_mem_available_mib: list[float] = []
    host_swap_used_mib: list[float] = []
    top_service_non_reclaimable_mib: list[float] = []
    for _ts, payload in items:
        policy_version = str(payload.get("classification_policy_version") or "null")
        by_policy_version[policy_version] = by_policy_version.get(policy_version, 0) + 1
        overall = payload.get("overall") or {}
        host = payload.get("host") or {}
        operational_adequacy = payload.get("operational_adequacy") or {}
        severity = str(overall.get("severity") or "unknown")
        by_severity[severity] = by_severity.get(severity, 0) + 1
        if severity == "critical":
            critical_count += 1
        elif severity == "warn":
            warn_count += 1
        adequacy_severity = str(
            operational_adequacy.get("severity")
            or overall.get("operational_adequacy_severity")
            or "unknown"
        )
        by_operational_adequacy_severity[adequacy_severity] = (
            by_operational_adequacy_severity.get(adequacy_severity, 0) + 1
        )
        if adequacy_severity == "critical":
            operational_adequacy_critical_count += 1
        elif adequacy_severity == "warn":
            operational_adequacy_warn_count += 1
        if "active_oneshot_peak_severity" in overall:
            active_peak_severity = str(overall.get("active_oneshot_peak_severity") or "unknown")
            by_active_oneshot_peak_severity[active_peak_severity] = (
                by_active_oneshot_peak_severity.get(active_peak_severity, 0) + 1
            )
            if active_peak_severity == "critical":
                active_oneshot_peak_critical_count += 1
            elif active_peak_severity == "warn":
                active_oneshot_peak_warn_count += 1
        if "systemd_peak_history_severity" in overall:
            peak_history_severity = str(overall.get("systemd_peak_history_severity") or "unknown")
            by_systemd_peak_history_severity[peak_history_severity] = (
                by_systemd_peak_history_severity.get(peak_history_severity, 0) + 1
            )
            if peak_history_severity == "critical":
                systemd_peak_history_critical_count += 1
            elif peak_history_severity == "warn":
                systemd_peak_history_warn_count += 1
        non_reclaimable_bytes = host.get("non_reclaimable_estimate_bytes")
        if isinstance(non_reclaimable_bytes, int | float):
            host_non_reclaimable_mib.append(float(non_reclaimable_bytes) / MIB)
        mem_available_bytes = host.get("mem_available_bytes")
        if isinstance(mem_available_bytes, int | float):
            host_mem_available_mib.append(float(mem_available_bytes) / MIB)
        swap_used_bytes = host.get("swap_used_bytes")
        if isinstance(swap_used_bytes, int | float):
            host_swap_used_mib.append(float(swap_used_bytes) / MIB)
        top_non_reclaimable = payload.get("top_non_reclaimable_consumers") or []
        if top_non_reclaimable:
            value = top_non_reclaimable[0].get("non_reclaimable_estimate_bytes")
            if isinstance(value, int | float):
                top_service_non_reclaimable_mib.append(float(value) / MIB)
    latest = items[-1][1] if items else {}
    latest_overall = latest.get("overall") or {}
    latest_operational_adequacy = latest.get("operational_adequacy") or {}
    latest_policy_version = str(latest.get("classification_policy_version") or "null") if latest else "unknown"
    latest_policy_items = [
        payload
        for _ts, payload in items
        if str(payload.get("classification_policy_version") or "null") == latest_policy_version
    ]
    latest_policy_warn_count = 0
    latest_policy_critical_count = 0
    latest_policy_operational_adequacy_warn_count = 0
    latest_policy_operational_adequacy_critical_count = 0
    for payload in latest_policy_items:
        severity = str(((payload.get("overall") or {}).get("severity")) or "unknown")
        if severity == "critical":
            latest_policy_critical_count += 1
        elif severity == "warn":
            latest_policy_warn_count += 1
        adequacy_severity = str(
            ((payload.get("operational_adequacy") or {}).get("severity"))
            or ((payload.get("overall") or {}).get("operational_adequacy_severity"))
            or "unknown"
        )
        if adequacy_severity == "critical":
            latest_policy_operational_adequacy_critical_count += 1
        elif adequacy_severity == "warn":
            latest_policy_operational_adequacy_warn_count += 1
    return {
        **time_bounds(items),
        "latest_severity": str(latest_overall.get("severity") or "unknown") if latest else "unknown",
        "latest_operational_adequacy_severity": str(
            latest_operational_adequacy.get("severity")
            or latest_overall.get("operational_adequacy_severity")
            or "unknown"
        )
        if latest
        else "unknown",
        "latest_policy_version": latest_policy_version,
        "latest_active_oneshot_peak_severity": str(latest_overall.get("active_oneshot_peak_severity") or "unknown")
        if latest
        else "unknown",
        "latest_systemd_peak_history_severity": str(latest_overall.get("systemd_peak_history_severity") or "unknown")
        if latest
        else "unknown",
        "by_severity": by_severity,
        "by_policy_version": by_policy_version,
        "by_operational_adequacy_severity": by_operational_adequacy_severity,
        "by_active_oneshot_peak_severity": by_active_oneshot_peak_severity,
        "by_systemd_peak_history_severity": by_systemd_peak_history_severity,
        "warn_count": warn_count,
        "critical_count": critical_count,
        "operational_adequacy_warn_count": operational_adequacy_warn_count,
        "operational_adequacy_critical_count": operational_adequacy_critical_count,
        "latest_policy_sample_count": len(latest_policy_items),
        "latest_policy_warn_count": latest_policy_warn_count,
        "latest_policy_critical_count": latest_policy_critical_count,
        "latest_policy_operational_adequacy_warn_count": latest_policy_operational_adequacy_warn_count,
        "latest_policy_operational_adequacy_critical_count": latest_policy_operational_adequacy_critical_count,
        "latest_policy_warn_or_critical_sample_ratio_pct": ratio_percent(
            latest_policy_warn_count + latest_policy_critical_count,
            len(latest_policy_items),
        ),
        "latest_policy_operational_adequacy_warn_or_critical_sample_ratio_pct": ratio_percent(
            latest_policy_operational_adequacy_warn_count + latest_policy_operational_adequacy_critical_count,
            len(latest_policy_items),
        ),
        "active_oneshot_peak_warn_count": active_oneshot_peak_warn_count,
        "active_oneshot_peak_critical_count": active_oneshot_peak_critical_count,
        "systemd_peak_history_warn_count": systemd_peak_history_warn_count,
        "systemd_peak_history_critical_count": systemd_peak_history_critical_count,
        "host_non_reclaimable_estimate_mib_p50": percentile(host_non_reclaimable_mib, 50),
        "host_non_reclaimable_estimate_mib_p95": percentile(host_non_reclaimable_mib, 95),
        "host_non_reclaimable_estimate_mib_max": max(host_non_reclaimable_mib) if host_non_reclaimable_mib else None,
        "host_mem_available_mib_min": min(host_mem_available_mib) if host_mem_available_mib else None,
        "host_mem_available_mib_p50": percentile(host_mem_available_mib, 50),
        "host_swap_used_mib_max": max(host_swap_used_mib) if host_swap_used_mib else None,
        "top_service_non_reclaimable_estimate_mib_p95": percentile(top_service_non_reclaimable_mib, 95),
        "top_service_non_reclaimable_estimate_mib_max": max(top_service_non_reclaimable_mib)
        if top_service_non_reclaimable_mib
        else None,
        "critical_sample_ratio_pct": ratio_percent(critical_count, len(items)),
        "warn_or_critical_sample_ratio_pct": ratio_percent(warn_count + critical_count, len(items)),
        "operational_adequacy_warn_or_critical_sample_ratio_pct": ratio_percent(
            operational_adequacy_warn_count + operational_adequacy_critical_count,
            len(items),
        ),
        "systemd_peak_history_warn_or_critical_sample_ratio_pct": ratio_percent(
            systemd_peak_history_warn_count + systemd_peak_history_critical_count,
            len(items),
        ),
        "denominator": "memory_status.jsonl guardrail snapshots with ts_utc",
        "classification": "guardrail; current severity, operational adequacy, and peak history are separate axes",
    }


def daily_objective_sli(youtube_items: list[tuple[int, dict]]) -> list[dict]:
    by_day: dict[str, list[tuple[int, dict]]] = {}
    for ts, payload in youtube_items:
        by_day.setdefault(jst_day(ts), []).append((ts, payload))
    return [
        {"jst_day": day, **youtube_sli_for_items(items)}
        for day, items in sorted(by_day.items())
    ]


def objective_sli_payload(ctx: ObjectiveSliContext, *, now_ts: int | None = None) -> dict:
    now_ts = int(time.time() if now_ts is None else now_ts)
    youtube_items = timestamped_jsonl_items(ctx.youtube_watchdog_events_file)
    fast_recovery_items = timestamped_jsonl_items(ctx.fast_recovery_events_file)
    stream_engine_items = timestamped_jsonl_items(ctx.stream_engine_events_file)
    stream_watchdog_items = timestamped_jsonl_items(ctx.log_base_dir / "stream_watchdog_events.jsonl")
    overlay_items = timestamped_jsonl_items(ctx.stream1090_report_events_file)
    upstream_items = timestamped_jsonl_items(ctx.upstream_report_events_file)
    notify_items = timestamped_jsonl_items(ctx.notify_events_file)
    api_items = timestamped_jsonl_items(ctx.log_base_dir / "youtube_api_calls.jsonl")
    memory_items = timestamped_jsonl_items(ctx.memory_status_events_file)

    regime_starts = {
        name: parse_utc_ts(meta["regime_start_ts_utc"])
        for name, meta in OBJECTIVE_SLI_REGIMES.items()
    }
    metrics = {
        "youtube_live": {
            "cumulative": youtube_sli_for_items(youtube_items),
            "since_post_stabilization": youtube_sli_for_items(
                [(ts, p) for ts, p in youtube_items if ts >= regime_starts["post_stabilization"]]
            ),
            "since_report_only_visual": youtube_sli_for_items(
                [(ts, p) for ts, p in youtube_items if ts >= regime_starts["report_only_visual"]]
            ),
            "since_upload_budget": youtube_sli_for_items(
                [(ts, p) for ts, p in youtube_items if ts >= regime_starts["upload_budget"]]
            ),
        },
        "stream_watchdog": {"cumulative": stream_watchdog_sli(stream_watchdog_items)},
        "fast_recovery": {"cumulative": fast_recovery_sli(fast_recovery_items)},
        "stream_engine": {
            "cumulative": stream_engine_sli(stream_engine_items),
            "rolling_24h": stream_engine_sli([(ts, p) for ts, p in stream_engine_items if ts >= now_ts - 86400]),
            "rolling_8h": stream_engine_sli([(ts, p) for ts, p in stream_engine_items if ts >= now_ts - 8 * 3600]),
            "rolling_1h": stream_engine_sli([(ts, p) for ts, p in stream_engine_items if ts >= now_ts - 3600]),
        },
        "upload_budget": {
            "since_samples_started": upload_budget_sli(
                [(ts, p) for ts, p in fast_recovery_items if ts >= regime_starts["upload_budget"]]
            )
        },
        "visual_upstream": {
            "overlay_stream1090": report_only_sli(overlay_items, expected_target="overlay_stream1090"),
            "upstream_readsb_tar1090_stream1090": report_only_sli(
                upstream_items,
                expected_target="upstream_readsb_tar1090_stream1090",
            ),
            "ab_interpretation": {
                "status": "pending_next_incident_or_deep_log_review",
                "a": "same upstream/stream1090 cause visible in both overlay and upstream",
                "b": "same sampling cadence is too coarse to separate independent causes",
                "runbook": "docs/20_runbooks/2026-05-11_01_stream1090_visual_upstream_ab_triage_playbook.md",
            },
        },
        "discord_notify": {"cumulative": discord_notify_sli(notify_items)},
        "youtube_api_usage": api_usage_sli(api_items),
        "memory_pressure": {
            "cumulative": memory_guardrail_sli(memory_items),
            "rolling_24h": memory_guardrail_sli([(ts, p) for ts, p in memory_items if ts >= now_ts - 86400]),
            "rolling_8h": memory_guardrail_sli([(ts, p) for ts, p in memory_items if ts >= now_ts - 8 * 3600]),
            "rolling_1h": memory_guardrail_sli([(ts, p) for ts, p in memory_items if ts >= now_ts - 3600]),
        },
    }
    return {
        "schema_version": 1,
        "generated_at_utc": utc_text_from_ts(now_ts),
        "generated_at_jst": jst_text_or_unknown(now_ts),
        "source": "stream-new objective-sli",
        "window_policy": {
            "cumulative": "historical record; do not report as current SLI without label",
            "rolling": "current recent quality",
            "regime_bounded": "preferred current SLI after explicit change point",
            "api_quota_day_timezone": "America/Los_Angeles",
        },
        "regimes": OBJECTIVE_SLI_REGIMES,
        "metrics": metrics,
        "daily_jst": daily_objective_sli(youtube_items),
    }


def save_objective_sli(ctx: ObjectiveSliContext, payload: dict) -> None:
    ctx.objective_sli_file.parent.mkdir(parents=True, exist_ok=True)
    ctx.objective_sli_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    append_jsonl(
        ctx.objective_sli_events_file,
        {
            "ts_utc": payload.get("generated_at_utc", utc_now_text()),
            "kind": "objective_sli_snapshot",
            "snapshot_path": str(ctx.objective_sli_file),
            "schema_version": payload.get("schema_version", 1),
            "metrics": payload.get("metrics", {}),
        },
    )


def objective_sli(ctx: ObjectiveSliContext, *, json_output: bool = False, record: bool = True) -> int:
    payload = objective_sli_payload(ctx)
    if record:
        save_objective_sli(ctx, payload)
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0
    yt = payload["metrics"]["youtube_live"]
    upload = payload["metrics"]["upload_budget"]["since_samples_started"]
    visual = payload["metrics"]["visual_upstream"]
    discord = payload["metrics"]["discord_notify"]["cumulative"]
    memory = payload["metrics"]["memory_pressure"]["rolling_24h"]
    print(
        "[objective-sli] "
        f"generated_at={payload['generated_at_jst']} "
        f"youtube_cumulative_ok={yt['cumulative']['ok_ratio_pct']} "
        f"youtube_post_stabilization_ok={yt['since_post_stabilization']['ok_ratio_pct']} "
        f"youtube_report_only_visual_ok={yt['since_report_only_visual']['ok_ratio_pct']} "
        f"upload_p95_mbps={upload['p95_mbps']} upload_max_mbps={upload['max_mbps']} "
        f"upload_over_5mbps_sec={upload['over_5mbps_sec']} "
        f"overlay_availability={visual['overlay_stream1090']['time_availability_pct']} "
        f"upstream_availability={visual['upstream_readsb_tar1090_stream1090']['time_availability_pct']} "
        f"discord_delivery={discord['delivery_ratio_pct']} "
        f"memory_24h_latest={memory['latest_severity']} "
        f"memory_24h_adequacy={memory['latest_operational_adequacy_severity']} "
        f"memory_24h_warn={memory['warn_count']} memory_24h_critical={memory['critical_count']} "
        f"memory_24h_adequacy_warn={memory['operational_adequacy_warn_count']} "
        f"memory_24h_adequacy_critical={memory['operational_adequacy_critical_count']} "
        f"memory_24h_non_reclaimable_p95_mib={memory['host_non_reclaimable_estimate_mib_p95']} "
        f"memory_24h_non_reclaimable_max_mib={memory['host_non_reclaimable_estimate_mib_max']} "
        f"memory_24h_latest_policy={memory['latest_policy_version']} "
        f"memory_24h_latest_policy_warn={memory['latest_policy_warn_count']} "
        f"memory_24h_latest_policy_critical={memory['latest_policy_critical_count']} "
        f"memory_24h_active_oneshot_peak_latest={memory['latest_active_oneshot_peak_severity']} "
        f"memory_24h_active_oneshot_peak_warn={memory['active_oneshot_peak_warn_count']} "
        f"memory_24h_active_oneshot_peak_critical={memory['active_oneshot_peak_critical_count']} "
        f"memory_24h_peak_history_latest={memory['latest_systemd_peak_history_severity']} "
        f"memory_24h_peak_history_warn={memory['systemd_peak_history_warn_count']} "
        f"memory_24h_peak_history_critical={memory['systemd_peak_history_critical_count']}"
    )
    if record:
        print(f"[objective-sli] snapshot={ctx.objective_sli_file} history={ctx.objective_sli_events_file}")
    return 0
