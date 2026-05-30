#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import gzip
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

BASE_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from stream_core.common.ffmpeg_restarts import summarize_ffmpeg_restart_attempts
from stream_core.ops_health.judgments import (
    api_report_judgment,
    encoder_gap_active,
    encoder_gap_judgment,
    estimate_fast_mode_units,
    exit_224_judgment,
    fast_mode_judgment,
    public_probe_authoritative_live_ok,
    public_probe_degraded_reason,
    public_probe_judgment,
    remote_warning_restart_judgment,
    rtmps_ssl_tls_judgment,
    sample_duration,
    ssl_tls_reason,
    tcp_send_budget_judgment,
)

DEFAULT_STATE_BASE_DIR = BASE_DIR / ".state" / "adsb-streamnew-v2"
STATE_BASE_DIR = Path(
    os.environ.get(
        "STREAM_RUNTIME_STATE_DIR",
        str(DEFAULT_STATE_BASE_DIR),
    )
).expanduser()
LOG_BASE_DIR = Path(
    os.environ.get(
        "STREAM_RUNTIME_LOG_DIR",
        str(STATE_BASE_DIR / "logs"),
    )
).expanduser()
WATCHDOG_EVENTS = LOG_BASE_DIR / "stream_watchdog_events.jsonl"
WATCHDOG_TIMELINE = LOG_BASE_DIR / "watchdog_state_timeline.jsonl"
YTW_EVENTS = LOG_BASE_DIR / "youtube_watchdog.jsonl"
FAST_RECOVERY_EVENTS = LOG_BASE_DIR / "fast_recovery_events.jsonl"
STREAM_ENGINE_EVENTS = LOG_BASE_DIR / "stream_engine_events.jsonl"
VIDEO_RESOLVER_EVENTS = LOG_BASE_DIR / "youtube_video_id_resolver_events.jsonl"
SLO_FILE = STATE_BASE_DIR / "slo_snapshot.json"
YTW_STATS_FILE = STATE_BASE_DIR / "youtube_watchdog_stats.json"
VIDEO_RESOLVER_STATE_FILE = STATE_BASE_DIR / "youtube_video_id_resolver_state.json"
SUBSYSTEMS_STATUS_FILE = STATE_BASE_DIR / "subsystems_status.json"
RECOVERY_ORCHESTRATOR_EVENTS_FILE = LOG_BASE_DIR / "recovery_orchestrator.jsonl"
API_COST_REPORT_DIR = STATE_BASE_DIR / "reports" / "youtube_api_cost"
API_COST_OPEN_DAY_LATEST_FILE = API_COST_REPORT_DIR / "open_day_latest.json"
API_COST_LATEST_FILE = API_COST_REPORT_DIR / "latest.json"
API_COST_OPEN_DAY_TIMER = "adsb-streamnew-youtube-api-cost-open-day-report.timer"
API_COST_CLOSED_DAY_TIMER = "adsb-streamnew-youtube-api-cost-report.timer"
STREAM_SERVICE = os.environ.get("OBSERVE_STREAM_SERVICE", "adsb-streamnew-youtube-stream.service").strip()


def parse_utc(ts: str) -> int | None:
    try:
        return int(calendar.timegm(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")))
    except Exception:
        return None


def rotated_jsonl_paths(path: Path) -> list[Path]:
    candidates = [path]
    candidates.extend(path.parent.glob(path.name + ".*"))

    def sort_key(candidate: Path) -> tuple[int, float, str]:
        if candidate == path:
            return (1, 0.0, candidate.name)
        try:
            return (0, candidate.stat().st_mtime, candidate.name)
        except OSError:
            return (0, 0.0, candidate.name)

    return sorted((p for p in candidates if p.exists() and p.is_file()), key=sort_key)


def iter_jsonl(path: Path):
    for candidate in rotated_jsonl_paths(path):
        opener = gzip.open if candidate.suffix == ".gz" else open
        try:
            with opener(candidate, "rt", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except Exception:
                        continue
        except OSError:
            continue


def jsonl_file_count(path: Path) -> int:
    return len(rotated_jsonl_paths(path))


def increment_counts(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def empty_hour_counts() -> dict[str, int]:
    return {f"{hour:02d}": 0 for hour in range(24)}


def jst_hour_key(ts: int) -> str:
    return f"{time.gmtime(ts + 9 * 3600).tm_hour:02d}"


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return round(float(ordered[idx]), 3)


def read_json_file(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def subsystem_summary() -> dict:
    payload = read_json_file(SUBSYSTEMS_STATUS_FILE)
    subsystems = payload.get("subsystems") if isinstance(payload.get("subsystems"), dict) else {}
    states = {
        name: str(item.get("state", "unknown"))
        for name, item in subsystems.items()
        if isinstance(item, dict)
    }
    overall = payload.get("overall") if isinstance(payload.get("overall"), dict) else {}
    return {
        "status_file": str(SUBSYSTEMS_STATUS_FILE),
        "present": bool(payload),
        "updated_at_utc": payload.get("updated_at_utc", ""),
        "overall_state": overall.get("state", ""),
        "stream_public_state": overall.get("stream_public_state", ""),
        "states": states,
        "degraded_subsystems": overall.get("degraded_subsystems", []),
        "failed_subsystems": overall.get("failed_subsystems", []),
        "recovery_orchestrator_events": jsonl_file_count(RECOVERY_ORCHESTRATOR_EVENTS_FILE),
    }
    return payload if isinstance(payload, dict) else {}


def report_effective_end_ts(payload: dict) -> int:
    window = payload.get("window") if isinstance(payload.get("window"), dict) else {}
    return parse_utc(str(window.get("effective_end_utc", ""))) or 0


def api_report_info(path: Path, *, now_ts: int, max_mtime_age_sec: int, max_effective_end_age_sec: int | None = None) -> dict:
    payload = read_json_file(path)
    exists = path.exists()
    mtime_ts = 0
    try:
        mtime_ts = int(path.stat().st_mtime) if exists else 0
    except OSError:
        mtime_ts = 0
    effective_end_ts = report_effective_end_ts(payload)
    mtime_age_sec = now_ts - mtime_ts if mtime_ts > 0 else None
    effective_end_age_sec = now_ts - effective_end_ts if effective_end_ts > 0 else None
    fresh = exists and mtime_age_sec is not None and mtime_age_sec <= max_mtime_age_sec
    if max_effective_end_age_sec is not None:
        fresh = fresh and effective_end_age_sec is not None and effective_end_age_sec <= max_effective_end_age_sec
    totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
    window = payload.get("window") if isinstance(payload.get("window"), dict) else {}
    return {
        "path": str(path),
        "exists": exists,
        "status": payload.get("status", ""),
        "target_day": payload.get("target_day", ""),
        "open_day": window.get("open_day", ""),
        "mtime_ts": mtime_ts,
        "mtime_age_sec": mtime_age_sec,
        "effective_end_utc": window.get("effective_end_utc", ""),
        "effective_end_age_sec": effective_end_age_sec,
        "fresh": fresh,
        "units": totals.get("units", 0),
        "calls": totals.get("calls", 0),
        "quota_exceeded_events": totals.get("quota_exceeded_events", 0),
        "max_mtime_age_sec": max_mtime_age_sec,
        "max_effective_end_age_sec": max_effective_end_age_sec,
    }


def systemd_timer_status(unit: str) -> dict:
    if os.environ.get("OBSERVE_SKIP_SYSTEMD", "0").strip() == "1":
        return {"unit": unit, "active": None, "reason": "systemd check skipped"}
    cp = subprocess.run(
        ["systemctl", "show", unit, "--property=LoadState,ActiveState,SubState,NextElapseUSecRealtime,LastTriggerUSec"],
        text=True,
        capture_output=True,
        check=False,
    )
    if cp.returncode != 0:
        return {"unit": unit, "active": False, "reason": (cp.stderr or cp.stdout or "").strip()}
    fields: dict[str, str] = {}
    for line in (cp.stdout or "").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        fields[key] = value
    active = fields.get("LoadState") == "loaded" and fields.get("ActiveState") == "active"
    return {
        "unit": unit,
        "active": active,
        "load_state": fields.get("LoadState", ""),
        "active_state": fields.get("ActiveState", ""),
        "sub_state": fields.get("SubState", ""),
        "next_elapse": fields.get("NextElapseUSecRealtime", ""),
        "last_trigger": fields.get("LastTriggerUSec", ""),
        "reason": "",
    }


def journal_ssl_tls_events(*, since_ts: int, now_ts: int, cutoff_1h: int, cutoff_24h: int) -> dict:
    if os.environ.get("OBSERVE_SKIP_JOURNAL", "0").strip() == "1":
        return {
            "enabled": False,
            "reason": "journal check skipped",
            "count": 0,
            "count_1h": 0,
            "count_24h": 0,
            "reasons": {},
            "samples": [],
        }
    if STATE_BASE_DIR != DEFAULT_STATE_BASE_DIR and os.environ.get("OBSERVE_ENABLE_JOURNAL", "0").strip() != "1":
        return {
            "enabled": False,
            "reason": "journal check skipped for overridden state dir",
            "count": 0,
            "count_1h": 0,
            "count_24h": 0,
            "reasons": {},
            "samples": [],
        }
    cp = subprocess.run(
        [
            "journalctl",
            "-u",
            STREAM_SERVICE,
            "--since",
            f"@{max(0, since_ts)}",
            "--until",
            f"@{max(0, now_ts)}",
            "--no-pager",
            "-o",
            "json",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if cp.returncode != 0:
        return {
            "enabled": True,
            "reason": (cp.stderr or cp.stdout or "journalctl failed").strip(),
            "count": 0,
            "count_1h": 0,
            "count_24h": 0,
            "reasons": {},
            "samples": [],
        }
    count = 0
    count_1h = 0
    count_24h = 0
    reasons: dict[str, int] = {}
    samples: list[dict] = []
    for line in (cp.stdout or "").splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except Exception:
            continue
        reason = ssl_tls_reason(item)
        if not reason:
            continue
        ts = 0
        try:
            ts = int(int(item.get("__REALTIME_TIMESTAMP", 0) or 0) / 1_000_000)
        except Exception:
            ts = 0
        message = str(item.get("MESSAGE", "")).strip()
        count += 1
        if ts >= cutoff_1h:
            count_1h += 1
        if ts >= cutoff_24h:
            count_24h += 1
        increment_counts(reasons, reason)
        if len(samples) < 5:
            samples.append(
                {
                    "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)) if ts > 0 else "",
                    "reason": reason,
                    "message": message[:300],
                }
            )
    return {
        "enabled": True,
        "reason": "",
        "count": count,
        "count_1h": count_1h,
        "count_24h": count_24h,
        "reasons": reasons,
        "samples": samples,
    }


def fast_mode_sli_from_events(
    events: list[dict],
    *,
    resolver_state: dict,
    now_ts: int,
    cutoff_24h: int,
    interval_sec: int,
    units_per_probe: int,
) -> dict:
    transitions: list[tuple[int, bool]] = []
    for item in events:
        ts = parse_utc(str(item.get("ts_utc", ""))) or 0
        if ts <= 0:
            continue
        event = str(item.get("event", item.get("event_type", ""))).strip()
        if event == "fast_mode_enter":
            transitions.append((ts, True))
        elif event == "fast_mode_exit":
            transitions.append((ts, False))
    transitions.sort(key=lambda part: part[0])

    episode_count = 0
    active_duration = 0
    active_since: int | None = None
    last_event: dict = {}
    for ts, active in transitions:
        if ts < cutoff_24h:
            if active:
                active_since = cutoff_24h
            else:
                active_since = None
            continue
        if active:
            episode_count += 1
            active_since = ts
        else:
            if active_since is not None:
                active_duration += max(0, ts - max(active_since, cutoff_24h))
            active_since = None
    state_active = bool(resolver_state.get("fast_mode_active", resolver_state.get("fast_mode", False)))
    if state_active and active_since is None:
        state_start = int(resolver_state.get("fast_search_window_start_ts", 0) or 0)
        if state_start <= 0:
            state_start = int(resolver_state.get("last_attempt_ts", 0) or 0)
        if state_start > 0:
            active_since = max(state_start, cutoff_24h)
            if not any(active for _ts, active in transitions if _ts >= cutoff_24h):
                episode_count += 1
    if active_since is not None:
        active_duration += max(0, now_ts - max(active_since, cutoff_24h))
    if events:
        last_event = sorted(events, key=lambda item: parse_utc(str(item.get("ts_utc", ""))) or 0)[-1]
    estimated_units = estimate_fast_mode_units(active_duration, interval_sec=interval_sec, units_per_probe=units_per_probe)
    judgment, judgment_reason = fast_mode_judgment(episode_count, active_duration, estimated_units)
    return {
        "fast_mode_episode_count_24h": episode_count,
        "fast_mode_active_duration_sec": active_duration,
        "fast_mode_active_duration_sec_24h": active_duration,
        "fast_mode_api_units_estimated": estimated_units,
        "fast_mode_api_units_estimated_24h": estimated_units,
        "fast_mode_estimated_interval_sec": interval_sec,
        "fast_mode_estimated_units_per_probe": units_per_probe,
        "fast_mode_current_active": state_active,
        "fast_mode_current_reason": resolver_state.get("fast_mode_reason", ""),
        "fast_mode_last_event": last_event,
        "fast_mode_judgment": judgment,
        "fast_mode_judgment_reason": judgment_reason,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24, help="Observation window in hours (24-72 recommended)")
    ap.add_argument("--max-youtube-unknown", type=int, default=0)
    ap.add_argument("--max-youtube-warn", type=int, default=0)
    ap.add_argument("--max-youtube-restart", type=int, default=0)
    ap.add_argument("--max-youtube-quota-guard", type=int, default=0)
    ap.add_argument("--max-youtube-stats-stale-sec", type=int, default=180)
    ap.add_argument("--max-api-open-day-report-stale-sec", type=int, default=1800)
    ap.add_argument("--max-api-closed-day-report-stale-sec", type=int, default=10800)
    ap.add_argument(
        "--tcp-send-budget-mbps",
        type=float,
        default=float(os.environ.get("OBSERVE_TCP_SEND_BUDGET_MBPS", "5.0") or "5.0"),
        help="Report-only bandwidth budget for ffmpeg tcp send samples.",
    )
    ap.add_argument(
        "--strict-history",
        action="store_true",
        help="Return non-zero when historical degraded events exist, even if current health is OK.",
    )
    args = ap.parse_args()
    hours = max(1, args.hours)
    now_ts = int(time.time())
    cutoff = now_ts - hours * 3600
    cutoff_1h = now_ts - 3600
    cutoff_24h = now_ts - 24 * 3600
    intervention_read_cutoff = min(cutoff, cutoff_24h)

    wd_counts: dict[str, int] = {}
    wd_restart_reasons: dict[str, int] = {}
    ytw_counts: dict[str, int] = {}
    ytw_event_only_counts: dict[str, int] = {}
    ytw_health_source_counts: dict[str, int] = {}
    ytw_oauth_probe_ok_counts: dict[str, int] = {}
    ytw_oauth_healthy_counts: dict[str, int] = {}
    fast_recovery_event_counts: dict[str, int] = {}
    fast_recovery_restart_triggers: dict[str, int] = {}
    fast_recovery_restart_reasons: dict[str, int] = {}
    stream_engine_event_counts: dict[str, int] = {}
    stream_engine_ffmpeg_exit_codes: dict[str, int] = {}
    stream_engine_ffmpeg_ssl_tls_reasons: dict[str, int] = {}
    fast_recovery_ssl_tls_reasons: dict[str, int] = {}
    fast_recovery_restart_count_1h = 0
    fast_recovery_restart_count_24h = 0
    remote_warning_restart_count_1h = 0
    remote_warning_restart_count_24h = 0
    stream_engine_ffmpeg_restart_count_1h = 0
    stream_engine_ffmpeg_restart_count_24h = 0
    stream_engine_ffmpeg_exit_224_count_1h = 0
    stream_engine_ffmpeg_exit_224_count_24h = 0
    stream_engine_ffmpeg_ssl_tls_count = 0
    stream_engine_ffmpeg_ssl_tls_count_1h = 0
    stream_engine_ffmpeg_ssl_tls_count_24h = 0
    fast_recovery_ssl_tls_count = 0
    fast_recovery_ssl_tls_count_1h = 0
    fast_recovery_ssl_tls_count_24h = 0
    public_probe_degraded_count_1h = 0
    public_probe_degraded_count_24h = 0
    public_probe_authoritative_live_ok_count_1h = 0
    public_probe_authoritative_live_ok_count_24h = 0
    public_probe_degraded_reasons: dict[str, int] = {}
    stream_engine_items_for_restart_summary: list[tuple[int, dict]] = []
    tcp_send_mbps_samples_24h: list[float] = []
    tcp_send_over_budget_duration_sec_24h = 0
    tcp_stall_count_by_hour = empty_hour_counts()
    exit_224_count_by_hour = empty_hour_counts()
    ytw_24h_samples: list[dict] = []
    encoder_gap_samples: list[tuple[int, bool]] = []
    timeline_anomaly = 0

    for item in iter_jsonl(WATCHDOG_EVENTS):
        ts = parse_utc(str(item.get("ts_utc", "")))
        if ts is None or ts < cutoff:
            continue
        et = str(item.get("event_type", "unknown"))
        wd_counts[et] = wd_counts.get(et, 0) + 1
        if et == "restart_trigger":
            reason = str(item.get("reason", "unknown"))
            wd_restart_reasons[reason] = wd_restart_reasons.get(reason, 0) + 1

    for item in iter_jsonl(WATCHDOG_TIMELINE):
        ts = parse_utc(str(item.get("ts_utc", "")))
        if ts is None or ts < cutoff:
            continue
        if str(item.get("entry_type", "")) == "anomaly":
            timeline_anomaly += 1

    ytw_read_cutoff = min(cutoff, cutoff_24h)
    for item in iter_jsonl(YTW_EVENTS):
        ts = parse_utc(str(item.get("ts_utc", "")))
        if ts is None or ts < ytw_read_cutoff:
            continue
        if ts >= cutoff_24h:
            ytw_24h_samples.append(item)
            encoder_gap_samples.append((ts, encoder_gap_active(item)))
        public_probe_reason = public_probe_degraded_reason(item)
        if public_probe_reason:
            if ts >= cutoff_24h:
                public_probe_degraded_count_24h += 1
                if public_probe_authoritative_live_ok(item):
                    public_probe_authoritative_live_ok_count_24h += 1
            if ts >= cutoff_1h:
                public_probe_degraded_count_1h += 1
                if public_probe_authoritative_live_ok(item):
                    public_probe_authoritative_live_ok_count_1h += 1
            if ts >= cutoff:
                increment_counts(public_probe_degraded_reasons, public_probe_reason)
        if ts < cutoff:
            continue
        if "status" not in item:
            event_name = str(item.get("event", "unknown_event")).strip() or "unknown_event"
            ytw_event_only_counts[event_name] = ytw_event_only_counts.get(event_name, 0) + 1
            continue
        st = str(item.get("status", "unknown"))
        ytw_counts[st] = ytw_counts.get(st, 0) + 1
        hs = str(item.get("health_source", "unknown"))
        ytw_health_source_counts[hs] = ytw_health_source_counts.get(hs, 0) + 1
        op = str(item.get("oauth_probe_ok", "n/a")).lower()
        ytw_oauth_probe_ok_counts[op] = ytw_oauth_probe_ok_counts.get(op, 0) + 1
        oh = str(item.get("oauth_healthy", "n/a")).lower()
        ytw_oauth_healthy_counts[oh] = ytw_oauth_healthy_counts.get(oh, 0) + 1

    for item in iter_jsonl(FAST_RECOVERY_EVENTS):
        ts = parse_utc(str(item.get("ts_utc", "")))
        if ts is None or ts < intervention_read_cutoff:
            continue
        kind = str(item.get("kind", "unknown")).strip() or "unknown"
        ssl_reason = ssl_tls_reason(item)
        if ssl_reason:
            if ts >= cutoff:
                fast_recovery_ssl_tls_count += 1
                increment_counts(fast_recovery_ssl_tls_reasons, ssl_reason)
            if ts >= cutoff_24h:
                fast_recovery_ssl_tls_count_24h += 1
            if ts >= cutoff_1h:
                fast_recovery_ssl_tls_count_1h += 1
        if ts >= cutoff:
            increment_counts(fast_recovery_event_counts, kind)
        if kind == "tcp_send_sample" and ts >= cutoff_24h:
            try:
                mbps = float(item.get("mbps", 0) or 0)
            except (TypeError, ValueError):
                mbps = 0.0
            tcp_send_mbps_samples_24h.append(round(mbps, 3))
            if mbps > float(args.tcp_send_budget_mbps):
                try:
                    interval_sec = int(item.get("sample_interval_sec", 0) or 0)
                except (TypeError, ValueError):
                    interval_sec = 0
                tcp_send_over_budget_duration_sec_24h += max(1, min(interval_sec, 600))
        trigger_for_hour = str(item.get("trigger", "")).strip()
        if trigger_for_hour == "tcp_stall" and kind in {
            "restart",
            "restart_failed",
            "restart_budget_block",
            "restart_budget_override",
        } and ts >= cutoff_24h:
            increment_counts(tcp_stall_count_by_hour, jst_hour_key(ts))
        if kind == "restart":
            trigger = str(item.get("trigger", "unknown")).strip() or "unknown"
            message = str(item.get("message", "")).strip() or "unknown"
            if ts >= cutoff:
                increment_counts(fast_recovery_restart_triggers, trigger)
                increment_counts(fast_recovery_restart_reasons, message)
            if ts >= cutoff_24h:
                fast_recovery_restart_count_24h += 1
                if trigger == "remote_warning":
                    remote_warning_restart_count_24h += 1
            if ts >= cutoff_1h:
                fast_recovery_restart_count_1h += 1
                if trigger == "remote_warning":
                    remote_warning_restart_count_1h += 1

    for item in iter_jsonl(STREAM_ENGINE_EVENTS):
        ts = parse_utc(str(item.get("ts_utc", "")))
        if ts is None or ts < intervention_read_cutoff:
            continue
        stream_engine_items_for_restart_summary.append((ts, item))
        et = str(item.get("event_type", "unknown")).strip() or "unknown"
        ssl_reason = ssl_tls_reason(item)
        if ssl_reason:
            if ts >= cutoff:
                stream_engine_ffmpeg_ssl_tls_count += 1
                increment_counts(stream_engine_ffmpeg_ssl_tls_reasons, ssl_reason)
            if ts >= cutoff_24h:
                stream_engine_ffmpeg_ssl_tls_count_24h += 1
            if ts >= cutoff_1h:
                stream_engine_ffmpeg_ssl_tls_count_1h += 1
        if ts >= cutoff:
            increment_counts(stream_engine_event_counts, et)
        if et == "ffmpeg_restart_scheduled":
            if ts >= cutoff_24h:
                stream_engine_ffmpeg_restart_count_24h += 1
            if ts >= cutoff_1h:
                stream_engine_ffmpeg_restart_count_1h += 1
        if et == "ffmpeg_exited":
            exit_code = str(item.get("exit_code", "unknown")).strip() or "unknown"
            if ts >= cutoff:
                increment_counts(stream_engine_ffmpeg_exit_codes, exit_code)
            if exit_code == "224":
                if ts >= cutoff_24h:
                    stream_engine_ffmpeg_exit_224_count_24h += 1
                    increment_counts(exit_224_count_by_hour, jst_hour_key(ts))
                if ts >= cutoff_1h:
                    stream_engine_ffmpeg_exit_224_count_1h += 1

    slo = {}
    if SLO_FILE.exists():
        try:
            slo = json.loads(SLO_FILE.read_text(encoding="utf-8"))
        except Exception:
            slo = {}

    pulse_unavail = int(slo.get("pulse_unavailable_count", 0) or 0)
    restart_count = int(slo.get("restart_trigger_count", 0) or 0)
    slo_max = int(slo.get("slo_pulse_unavailable_24h_max", 0) or 0)
    pulse_pass = pulse_unavail <= slo_max if slo_max > 0 else True

    ytw_stats: dict = {}
    ytw_stats_ts = 0
    ytw_stats_stale = True
    if YTW_STATS_FILE.exists():
        try:
            ytw_stats = json.loads(YTW_STATS_FILE.read_text(encoding="utf-8"))
            if isinstance(ytw_stats, dict):
                ytw_stats_ts = parse_utc(str(ytw_stats.get("ts_utc", ""))) or 0
        except Exception:
            ytw_stats_ts = 0
    if ytw_stats_ts > 0:
        ytw_stats_stale = (now_ts - ytw_stats_ts) > max(1, int(args.max_youtube_stats_stale_sec))

    ytw_unknown = int(ytw_counts.get("unknown", 0))
    ytw_warn = int(ytw_counts.get("warn", 0))
    ytw_restart = int(ytw_counts.get("restart", 0))
    ytw_quota_guard = int(ytw_counts.get("quota_guard", 0))
    fast_recovery_restart_count = int(fast_recovery_event_counts.get("restart", 0))
    stream_engine_ffmpeg_restart_count = int(stream_engine_event_counts.get("ffmpeg_restart_scheduled", 0))
    stream_engine_ffmpeg_restart_summary = summarize_ffmpeg_restart_attempts(
        [(ts, item) for ts, item in stream_engine_items_for_restart_summary if ts >= cutoff]
    )
    stream_engine_ffmpeg_restart_summary_1h = summarize_ffmpeg_restart_attempts(
        [(ts, item) for ts, item in stream_engine_items_for_restart_summary if ts >= cutoff_1h]
    )
    stream_engine_ffmpeg_restart_summary_24h = summarize_ffmpeg_restart_attempts(
        [(ts, item) for ts, item in stream_engine_items_for_restart_summary if ts >= cutoff_24h]
    )
    stream_engine_ffmpeg_exit_224_count = int(stream_engine_ffmpeg_exit_codes.get("224", 0))
    remote_warning_restart_j, remote_warning_restart_j_reason = remote_warning_restart_judgment(
        remote_warning_restart_count_1h,
        remote_warning_restart_count_24h,
    )
    exit_224_j, exit_224_j_reason = exit_224_judgment(
        stream_engine_ffmpeg_exit_224_count_1h,
        stream_engine_ffmpeg_exit_224_count_24h,
    )
    journal_ssl_tls = journal_ssl_tls_events(
        since_ts=intervention_read_cutoff,
        now_ts=now_ts,
        cutoff_1h=cutoff_1h,
        cutoff_24h=cutoff_24h,
    )
    rtmps_ssl_tls_count = (
        stream_engine_ffmpeg_ssl_tls_count
        + fast_recovery_ssl_tls_count
        + int(journal_ssl_tls.get("count", 0) or 0)
    )
    rtmps_ssl_tls_count_1h = (
        stream_engine_ffmpeg_ssl_tls_count_1h
        + fast_recovery_ssl_tls_count_1h
        + int(journal_ssl_tls.get("count_1h", 0) or 0)
    )
    rtmps_ssl_tls_count_24h = (
        stream_engine_ffmpeg_ssl_tls_count_24h
        + fast_recovery_ssl_tls_count_24h
        + int(journal_ssl_tls.get("count_24h", 0) or 0)
    )
    rtmps_ssl_tls_j, rtmps_ssl_tls_j_reason = rtmps_ssl_tls_judgment(
        rtmps_ssl_tls_count_1h,
        rtmps_ssl_tls_count_24h,
    )
    public_probe_j, public_probe_j_reason = public_probe_judgment(
        public_probe_degraded_count_1h,
        public_probe_degraded_count_24h,
        public_probe_authoritative_live_ok_count_24h,
    )
    resolver_state = read_json_file(VIDEO_RESOLVER_STATE_FILE)
    resolver_events = [
        item
        for item in iter_jsonl(VIDEO_RESOLVER_EVENTS)
        if (parse_utc(str(item.get("ts_utc", ""))) or 0) >= cutoff_24h - 3600
    ]
    fast_mode_sli = fast_mode_sli_from_events(
        resolver_events,
        resolver_state=resolver_state,
        now_ts=now_ts,
        cutoff_24h=cutoff_24h,
        interval_sec=max(1, int(os.environ.get("OBSERVE_FAST_MODE_ESTIMATED_INTERVAL_SEC", "5") or "5")),
        units_per_probe=max(0, int(os.environ.get("OBSERVE_FAST_MODE_ESTIMATED_UNITS_PER_PROBE", "3") or "3")),
    )
    encoder_gap_sample_count, encoder_gap_duration_sec = sample_duration(encoder_gap_samples, now_ts=now_ts)
    encoder_gap_j, encoder_gap_j_reason = encoder_gap_judgment(encoder_gap_sample_count, encoder_gap_duration_sec)
    tcp_send_sample_count_24h = len(tcp_send_mbps_samples_24h)
    tcp_send_p50 = percentile(tcp_send_mbps_samples_24h, 50)
    tcp_send_p95 = percentile(tcp_send_mbps_samples_24h, 95)
    tcp_send_max = round(max(tcp_send_mbps_samples_24h), 3) if tcp_send_mbps_samples_24h else None
    tcp_send_j, tcp_send_j_reason = tcp_send_budget_judgment(
        tcp_send_sample_count_24h,
        tcp_send_over_budget_duration_sec_24h,
    )

    open_day_report = api_report_info(
        API_COST_OPEN_DAY_LATEST_FILE,
        now_ts=now_ts,
        max_mtime_age_sec=max(1, int(args.max_api_open_day_report_stale_sec)),
        max_effective_end_age_sec=max(1, int(args.max_api_open_day_report_stale_sec)),
    )
    closed_day_report = api_report_info(
        API_COST_LATEST_FILE,
        now_ts=now_ts,
        max_mtime_age_sec=max(1, int(args.max_api_closed_day_report_stale_sec)),
    )
    api_cost_timers = {
        API_COST_OPEN_DAY_TIMER: systemd_timer_status(API_COST_OPEN_DAY_TIMER),
        API_COST_CLOSED_DAY_TIMER: systemd_timer_status(API_COST_CLOSED_DAY_TIMER),
    }
    api_cost_timers_active = all(item.get("active") is True for item in api_cost_timers.values())
    api_report_j, api_report_j_reason = api_report_judgment(
        bool(open_day_report.get("fresh")),
        bool(closed_day_report.get("fresh")),
        api_cost_timers_active,
    )

    youtube_stream_history_pass = (
        ytw_unknown <= max(0, int(args.max_youtube_unknown))
        and ytw_warn <= max(0, int(args.max_youtube_warn))
        and ytw_restart <= max(0, int(args.max_youtube_restart))
    )
    youtube_observability_history_pass = ytw_quota_guard <= max(0, int(args.max_youtube_quota_guard))
    current_youtube_status = str(ytw_stats.get("status", "") or "").strip().lower()
    current_youtube_judgment = str(ytw_stats.get("judgment", "") or "").strip().lower()
    current_youtube_remote_status = str(ytw_stats.get("remote_status", "") or "").strip().lower()
    current_youtube_quota_guard = bool(ytw_stats.get("quota_guard_active", False)) or current_youtube_status == "quota_guard"
    youtube_current_fail = ytw_stats_stale or current_youtube_status in {"unknown", "warn", "restart"} or current_youtube_judgment == "ng"
    youtube_current_degraded = youtube_current_fail or current_youtube_remote_status == "warning"
    youtube_observability_current_fail = current_youtube_quota_guard
    youtube_stream_pass = not youtube_current_fail
    youtube_observability_pass = not youtube_observability_current_fail
    youtube_history_pass = youtube_stream_history_pass and youtube_observability_history_pass
    youtube_pass = youtube_stream_pass and youtube_observability_pass
    historical_degraded = (
        (not youtube_stream_history_pass)
        or (not youtube_observability_history_pass)
        or timeline_anomaly > 0
        or bool(wd_restart_reasons)
        or rtmps_ssl_tls_count > 0
    )
    current_fail = (not pulse_pass) or (not youtube_pass)
    strict_pass = (not current_fail) and (not historical_degraded)

    summary = {
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "window_hours": hours,
        "watchdog_event_counts": wd_counts,
        "watchdog_restart_reasons": wd_restart_reasons,
        "watchdog_timeline_anomaly_count": timeline_anomaly,
        "youtube_watchdog_status_counts": ytw_counts,
        "youtube_watchdog_event_only_counts": ytw_event_only_counts,
        "youtube_health_source_counts": ytw_health_source_counts,
        "youtube_oauth_probe_ok_counts": ytw_oauth_probe_ok_counts,
        "youtube_oauth_healthy_counts": ytw_oauth_healthy_counts,
        "public_probe_degraded_count_1h": public_probe_degraded_count_1h,
        "public_probe_degraded_count_24h": public_probe_degraded_count_24h,
        "public_probe_authoritative_live_ok_count_1h": public_probe_authoritative_live_ok_count_1h,
        "public_probe_authoritative_live_ok_count_24h": public_probe_authoritative_live_ok_count_24h,
        "public_probe_degraded_reasons": public_probe_degraded_reasons,
        "public_probe_judgment": public_probe_j,
        "public_probe_judgment_reason": public_probe_j_reason,
        **fast_mode_sli,
        "api_cost_reports": {
            "open_day_latest": open_day_report,
            "latest_closed_day": closed_day_report,
            "timers": api_cost_timers,
        },
        "api_report_open_day_fresh": bool(open_day_report.get("fresh")),
        "api_report_closed_day_fresh": bool(closed_day_report.get("fresh")),
        "api_report_timers_active": api_cost_timers_active,
        "api_report_judgment": api_report_j,
        "api_report_judgment_reason": api_report_j_reason,
        "encoder_gap_enable_auto_stop_false_sample_count_24h": encoder_gap_sample_count,
        "encoder_gap_enable_auto_stop_false_duration_sec_24h": encoder_gap_duration_sec,
        "encoder_gap_enable_auto_stop_false_judgment": encoder_gap_j,
        "encoder_gap_enable_auto_stop_false_judgment_reason": encoder_gap_j_reason,
        "ffmpeg_tcp_send_mbps_24h_sample_count": tcp_send_sample_count_24h,
        "ffmpeg_tcp_send_mbps_24h_p50": tcp_send_p50,
        "ffmpeg_tcp_send_mbps_24h_p95": tcp_send_p95,
        "ffmpeg_tcp_send_mbps_24h_max": tcp_send_max,
        "ffmpeg_tcp_send_mbps_24h_over_5mbps_duration_sec": tcp_send_over_budget_duration_sec_24h,
        "ffmpeg_tcp_send_mbps_24h_over_budget_duration_sec": tcp_send_over_budget_duration_sec_24h,
        "ffmpeg_tcp_send_budget_mbps": float(args.tcp_send_budget_mbps),
        "ffmpeg_tcp_send_budget_judgment": tcp_send_j,
        "ffmpeg_tcp_send_budget_judgment_reason": tcp_send_j_reason,
        "tcp_stall_count_by_hour": tcp_stall_count_by_hour,
        "fast_recovery_event_counts": fast_recovery_event_counts,
        "fast_recovery_restart_count": fast_recovery_restart_count,
        "fast_recovery_restart_count_1h": fast_recovery_restart_count_1h,
        "fast_recovery_restart_count_24h": fast_recovery_restart_count_24h,
        "remote_warning_restart_count_1h": remote_warning_restart_count_1h,
        "remote_warning_restart_count_24h": remote_warning_restart_count_24h,
        "remote_warning_restart_judgment": remote_warning_restart_j,
        "remote_warning_restart_judgment_reason": remote_warning_restart_j_reason,
        "fast_recovery_restart_triggers": fast_recovery_restart_triggers,
        "fast_recovery_restart_reasons": fast_recovery_restart_reasons,
        "stream_engine_event_counts": stream_engine_event_counts,
        "stream_engine_ffmpeg_restart_count": stream_engine_ffmpeg_restart_count,
        "stream_engine_ffmpeg_restart_count_1h": stream_engine_ffmpeg_restart_count_1h,
        "stream_engine_ffmpeg_restart_count_24h": stream_engine_ffmpeg_restart_count_24h,
        "stream_engine_ffmpeg_restart_attempts_count": stream_engine_ffmpeg_restart_summary["attempt_count"],
        "stream_engine_ffmpeg_restart_attempts_1h": stream_engine_ffmpeg_restart_summary_1h["attempt_count"],
        "stream_engine_ffmpeg_restart_attempts_24h": stream_engine_ffmpeg_restart_summary_24h["attempt_count"],
        "ffmpeg_restart_attempts_1h": stream_engine_ffmpeg_restart_summary_1h["attempt_count"],
        "ffmpeg_restart_attempts_24h": stream_engine_ffmpeg_restart_summary_24h["attempt_count"],
        "stream_engine_ffmpeg_restart_retry_episodes_count": stream_engine_ffmpeg_restart_summary[
            "retry_episode_count"
        ],
        "stream_engine_ffmpeg_restart_retry_episodes_1h": stream_engine_ffmpeg_restart_summary_1h[
            "retry_episode_count"
        ],
        "stream_engine_ffmpeg_restart_retry_episodes_24h": stream_engine_ffmpeg_restart_summary_24h[
            "retry_episode_count"
        ],
        "ffmpeg_restart_episodes_1h": stream_engine_ffmpeg_restart_summary_1h["retry_episode_count"],
        "ffmpeg_restart_episodes_24h": stream_engine_ffmpeg_restart_summary_24h["retry_episode_count"],
        "ffmpeg_restart_retry_episodes_1h": stream_engine_ffmpeg_restart_summary_1h["retry_episode_count"],
        "ffmpeg_restart_retry_episodes_24h": stream_engine_ffmpeg_restart_summary_24h["retry_episode_count"],
        "stream_engine_ffmpeg_restart_incident_clusters_count": stream_engine_ffmpeg_restart_summary[
            "incident_cluster_count"
        ],
        "stream_engine_ffmpeg_restart_incident_clusters_1h": stream_engine_ffmpeg_restart_summary_1h[
            "incident_cluster_count"
        ],
        "stream_engine_ffmpeg_restart_incident_clusters_24h": stream_engine_ffmpeg_restart_summary_24h[
            "incident_cluster_count"
        ],
        "ffmpeg_restart_incident_clusters_1h": stream_engine_ffmpeg_restart_summary_1h["incident_cluster_count"],
        "ffmpeg_restart_incident_clusters_24h": stream_engine_ffmpeg_restart_summary_24h["incident_cluster_count"],
        "stream_engine_ffmpeg_restart_episode_root_causes": stream_engine_ffmpeg_restart_summary[
            "episode_root_causes"
        ],
        "stream_engine_ffmpeg_restart_episode_root_causes_1h": stream_engine_ffmpeg_restart_summary_1h[
            "episode_root_causes"
        ],
        "stream_engine_ffmpeg_restart_episode_root_causes_24h": stream_engine_ffmpeg_restart_summary_24h[
            "episode_root_causes"
        ],
        "ffmpeg_restart_episode_root_causes_1h": stream_engine_ffmpeg_restart_summary_1h["episode_root_causes"],
        "ffmpeg_restart_episode_root_causes_24h": stream_engine_ffmpeg_restart_summary_24h[
            "episode_root_causes"
        ],
        "ffmpeg_restart_episodes_root_cause_1h": stream_engine_ffmpeg_restart_summary_1h["episode_root_causes"],
        "ffmpeg_restart_episodes_root_cause_24h": stream_engine_ffmpeg_restart_summary_24h[
            "episode_root_causes"
        ],
        "stream_engine_ffmpeg_restart_incident_root_causes": stream_engine_ffmpeg_restart_summary[
            "incident_root_causes"
        ],
        "stream_engine_ffmpeg_restart_incident_root_causes_1h": stream_engine_ffmpeg_restart_summary_1h[
            "incident_root_causes"
        ],
        "stream_engine_ffmpeg_restart_incident_root_causes_24h": stream_engine_ffmpeg_restart_summary_24h[
            "incident_root_causes"
        ],
        "ffmpeg_restart_incident_root_causes_1h": stream_engine_ffmpeg_restart_summary_1h["incident_root_causes"],
        "ffmpeg_restart_incident_root_causes_24h": stream_engine_ffmpeg_restart_summary_24h[
            "incident_root_causes"
        ],
        "stream_engine_ffmpeg_restart_max_episode_duration_sec": stream_engine_ffmpeg_restart_summary[
            "max_episode_duration_sec"
        ],
        "stream_engine_ffmpeg_restart_max_episode_duration_sec_1h": stream_engine_ffmpeg_restart_summary_1h[
            "max_episode_duration_sec"
        ],
        "stream_engine_ffmpeg_restart_max_episode_duration_sec_24h": stream_engine_ffmpeg_restart_summary_24h[
            "max_episode_duration_sec"
        ],
        "ffmpeg_restart_max_episode_duration_sec_1h": stream_engine_ffmpeg_restart_summary_1h[
            "max_episode_duration_sec"
        ],
        "ffmpeg_restart_max_episode_duration_sec_24h": stream_engine_ffmpeg_restart_summary_24h[
            "max_episode_duration_sec"
        ],
        "stream_engine_ffmpeg_restart_max_attempts_per_episode": stream_engine_ffmpeg_restart_summary[
            "max_attempts_per_episode"
        ],
        "stream_engine_ffmpeg_restart_max_attempts_per_episode_1h": stream_engine_ffmpeg_restart_summary_1h[
            "max_attempts_per_episode"
        ],
        "stream_engine_ffmpeg_restart_max_attempts_per_episode_24h": stream_engine_ffmpeg_restart_summary_24h[
            "max_attempts_per_episode"
        ],
        "ffmpeg_restart_max_attempts_per_episode_1h": stream_engine_ffmpeg_restart_summary_1h[
            "max_attempts_per_episode"
        ],
        "ffmpeg_restart_max_attempts_per_episode_24h": stream_engine_ffmpeg_restart_summary_24h[
            "max_attempts_per_episode"
        ],
        "stream_engine_ffmpeg_restart_retry_episode_samples_24h": stream_engine_ffmpeg_restart_summary_24h[
            "episodes"
        ],
        "stream_engine_ffmpeg_restart_incident_cluster_samples_24h": stream_engine_ffmpeg_restart_summary_24h[
            "incident_clusters"
        ],
        "ffmpeg_restart_retry_episode_samples_24h": stream_engine_ffmpeg_restart_summary_24h["episodes"],
        "ffmpeg_restart_incident_cluster_samples_24h": stream_engine_ffmpeg_restart_summary_24h[
            "incident_clusters"
        ],
        "stream_engine_ffmpeg_exit_codes": stream_engine_ffmpeg_exit_codes,
        "stream_engine_ffmpeg_exit_224_count": stream_engine_ffmpeg_exit_224_count,
        "stream_engine_ffmpeg_exit_224_count_1h": stream_engine_ffmpeg_exit_224_count_1h,
        "stream_engine_ffmpeg_exit_224_count_24h": stream_engine_ffmpeg_exit_224_count_24h,
        "exit_224_count_by_hour": exit_224_count_by_hour,
        "stream_engine_ffmpeg_exit_224_judgment": exit_224_j,
        "stream_engine_ffmpeg_exit_224_judgment_reason": exit_224_j_reason,
        "stream_engine_ffmpeg_ssl_tls_count": stream_engine_ffmpeg_ssl_tls_count,
        "stream_engine_ffmpeg_ssl_tls_count_1h": stream_engine_ffmpeg_ssl_tls_count_1h,
        "stream_engine_ffmpeg_ssl_tls_count_24h": stream_engine_ffmpeg_ssl_tls_count_24h,
        "stream_engine_ffmpeg_ssl_tls_reasons": stream_engine_ffmpeg_ssl_tls_reasons,
        "fast_recovery_ssl_tls_count": fast_recovery_ssl_tls_count,
        "fast_recovery_ssl_tls_count_1h": fast_recovery_ssl_tls_count_1h,
        "fast_recovery_ssl_tls_count_24h": fast_recovery_ssl_tls_count_24h,
        "fast_recovery_ssl_tls_reasons": fast_recovery_ssl_tls_reasons,
        "journal_ssl_tls": journal_ssl_tls,
        "rtmps_ssl_tls_count": rtmps_ssl_tls_count,
        "rtmps_ssl_tls_count_1h": rtmps_ssl_tls_count_1h,
        "rtmps_ssl_tls_count_24h": rtmps_ssl_tls_count_24h,
        "rtmps_ssl_tls_judgment": rtmps_ssl_tls_j,
        "rtmps_ssl_tls_judgment_reason": rtmps_ssl_tls_j_reason,
        "log_files_read": {
            "stream_watchdog_events": jsonl_file_count(WATCHDOG_EVENTS),
            "watchdog_state_timeline": jsonl_file_count(WATCHDOG_TIMELINE),
            "youtube_watchdog": jsonl_file_count(YTW_EVENTS),
            "fast_recovery_events": jsonl_file_count(FAST_RECOVERY_EVENTS),
            "stream_engine_events": jsonl_file_count(STREAM_ENGINE_EVENTS),
            "recovery_orchestrator_events": jsonl_file_count(RECOVERY_ORCHESTRATOR_EVENTS_FILE),
        },
        "subsystems": subsystem_summary(),
        "slo_snapshot": {
            "pulse_unavailable_count": pulse_unavail,
            "restart_trigger_count": restart_count,
            "slo_pulse_unavailable_24h_max": slo_max,
            "ts_utc": slo.get("ts_utc", ""),
        },
        "checks": {
            "pulse_pass": pulse_pass,
            "youtube_pass": youtube_pass,
            "youtube_stream_pass": youtube_stream_pass,
            "youtube_observability_pass": youtube_observability_pass,
            "youtube_history_pass": youtube_history_pass,
            "youtube_observability_history_pass": youtube_observability_history_pass,
            "youtube_current_fail": youtube_current_fail,
            "youtube_current_degraded": youtube_current_degraded,
            "youtube_observability_current_fail": youtube_observability_current_fail,
            "youtube_current_status": current_youtube_status,
            "youtube_current_judgment": current_youtube_judgment,
            "youtube_current_remote_status": current_youtube_remote_status,
            "youtube_unknown_count": ytw_unknown,
            "youtube_warn_count": ytw_warn,
            "youtube_restart_count": ytw_restart,
            "youtube_quota_guard_count": ytw_quota_guard,
            "youtube_stats_stale": ytw_stats_stale,
            "youtube_stats_ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ytw_stats_ts)) if ytw_stats_ts > 0 else "",
            "api_report_open_day_fresh": bool(open_day_report.get("fresh")),
            "api_report_closed_day_fresh": bool(closed_day_report.get("fresh")),
            "api_report_timers_active": api_cost_timers_active,
            "api_report_judgment": api_report_j,
            "ffmpeg_tcp_send_budget_judgment": tcp_send_j,
            "rtmps_ssl_tls_judgment": rtmps_ssl_tls_j,
            "fast_mode_current_active": bool(fast_mode_sli.get("fast_mode_current_active")),
            "max_youtube_unknown": max(0, int(args.max_youtube_unknown)),
            "max_youtube_warn": max(0, int(args.max_youtube_warn)),
            "max_youtube_restart": max(0, int(args.max_youtube_restart)),
            "max_youtube_quota_guard": max(0, int(args.max_youtube_quota_guard)),
            "max_youtube_stats_stale_sec": max(1, int(args.max_youtube_stats_stale_sec)),
            "current_fail": current_fail,
            "historical_degraded": historical_degraded,
            "strict_history": bool(args.strict_history),
            "strict_pass": strict_pass,
        },
        "pass": not current_fail,
    }
    print(json.dumps(summary, ensure_ascii=False, separators=(",", ":")))
    return 0 if (strict_pass if args.strict_history else summary["pass"]) else 2


if __name__ == "__main__":
    raise SystemExit(main())
