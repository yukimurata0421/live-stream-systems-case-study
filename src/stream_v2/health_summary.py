from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import DEFAULT_SOURCE_STATE_ROOT, DEFAULT_V2_STATE_ROOT
from .source_reader import SourceReader
from .status_summary import build_status_summary
from .timeutil import age_seconds, isoformat_utc, now_utc, parse_utc


def build_health_summary(
    *,
    source_state_root: Path = DEFAULT_SOURCE_STATE_ROOT,
    state_root: Path = DEFAULT_V2_STATE_ROOT,
    now: datetime | None = None,
    max_youtube_stats_stale_sec: float = 180.0,
    max_v2_status_stale_sec: float = 300.0,
) -> dict[str, Any]:
    """Build a native stream_v2 operator summary without invoking legacy code.

    This is intentionally read-only against production. It reads current
    production runtime state through ``SourceReader`` and joins it with the v2
    subsystem/orchestrator shadow outputs.
    """
    now = now or now_utc()
    inputs = SourceReader(source_state_root).read()
    v2 = build_status_summary(state_root)

    source = _source_current_summary(
        inputs.youtube_watchdog_stats,
        inputs.latest_runtime_state,
        inputs.stream_watchdog_stats,
        inputs.latest_fast_recovery_event,
        now=now,
        max_youtube_stats_stale_sec=max_youtube_stats_stale_sec,
    )
    v2_freshness = _v2_freshness(v2, now=now, max_v2_status_stale_sec=max_v2_status_stale_sec)

    checks = {
        "source_current_fail": source["current_fail"],
        "source_youtube_stale": source["youtube"]["stale"],
        "v2_available": v2_freshness["available"],
        "v2_stale": v2_freshness["stale"],
        "v2_overall": v2.get("observed_state", {}).get("overall", "unknown"),
        "v2_selected_action": v2.get("selected_action", {}).get("action", "none"),
    }
    return {
        "schema_version": 1,
        "ts_utc": isoformat_utc(now),
        "state_roots": {
            "source_state_root": str(source_state_root),
            "stream_v2_state_root": str(state_root),
        },
        "answer": _answer(source, v2, v2_freshness),
        "source_current": source,
        "stream_v2": {
            "freshness": v2_freshness,
            "observed_state": v2.get("observed_state", {}),
            "subsystems": v2.get("subsystems", {}),
            "decision": v2.get("decision", {}),
            "selected_action": v2.get("selected_action", {}),
            "blocked_actions": v2.get("blocked_actions", []),
            "replacement_policy": v2.get("replacement_policy", {}),
            "objective_sli": v2.get("objective_sli", {}),
            "warnings": v2.get("warnings", []),
        },
        "checks": checks,
    }


def render_text_health_summary(summary: dict[str, Any]) -> str:
    source = summary.get("source_current", {})
    youtube = source.get("youtube", {}) if isinstance(source.get("youtube"), dict) else {}
    runtime = source.get("local_runtime", {}) if isinstance(source.get("local_runtime"), dict) else {}
    v2 = summary.get("stream_v2", {}) if isinstance(summary.get("stream_v2"), dict) else {}
    observed = v2.get("observed_state", {}) if isinstance(v2.get("observed_state"), dict) else {}
    selected = v2.get("selected_action", {}) if isinstance(v2.get("selected_action"), dict) else {}
    freshness = v2.get("freshness", {}) if isinstance(v2.get("freshness"), dict) else {}
    replacement = v2.get("replacement_policy", {}) if isinstance(v2.get("replacement_policy"), dict) else {}
    lines = [
        f"answer: {summary.get('answer', '')}",
        (
            "source: "
            f"current_fail={source.get('current_fail')} "
            f"youtube_status={youtube.get('status', '')} "
            f"youtube_stale={youtube.get('stale')} "
            f"runtime_status={runtime.get('status', '')} "
            f"ffmpeg_pid={runtime.get('ffmpeg_pid', '')}"
        ),
        (
            "v2: "
            f"available={freshness.get('available')} "
            f"stale={freshness.get('stale')} "
            f"overall={observed.get('overall', 'unknown')} "
            f"public={observed.get('stream_public_state', 'unknown')} "
            f"selected_action={selected.get('action', 'none')} "
            f"execute={selected.get('execute', False)}"
        ),
        (
            "v2_consistency: "
            f"{observed.get('consistency_window_sec')}/{observed.get('max_consistency_window_sec')}"
        ),
        f"replacement: allowed={replacement.get('allowed', False)} reason={replacement.get('reason', '')}",
    ]
    warnings = v2.get("warnings", [])
    if warnings:
        lines.append("warnings: " + ", ".join(str(item) for item in warnings))
    return "\n".join(lines)


def dumps_health_summary(summary: dict[str, Any], *, pretty: bool = False) -> str:
    return json.dumps(summary, ensure_ascii=False, indent=2 if pretty else None)


def _source_current_summary(
    ytw_stats: dict[str, Any],
    runtime: dict[str, Any],
    stream_watchdog: dict[str, Any],
    fast_recovery: dict[str, Any],
    *,
    now: datetime,
    max_youtube_stats_stale_sec: float,
) -> dict[str, Any]:
    ytw_ts = parse_utc(ytw_stats.get("ts_utc"))
    ytw_age = age_seconds(ytw_ts, now)
    ytw_stale = ytw_age is None or ytw_age > max_youtube_stats_stale_sec
    ytw_status = str(ytw_stats.get("status", "") or "").strip().lower()
    ytw_judgment = str(ytw_stats.get("judgment", "") or "").strip().lower()
    remote_status = str(ytw_stats.get("remote_status", "") or "").strip().lower()
    quota_guard = bool(ytw_stats.get("quota_guard_active", False)) or ytw_status == "quota_guard"
    youtube_current_fail = ytw_stale or ytw_status in {"unknown", "warn", "restart"} or ytw_judgment == "ng"
    youtube_current_degraded = youtube_current_fail or remote_status == "warning"

    runtime_ts = parse_utc(runtime.get("updated_at_utc") or runtime.get("ts_utc"))
    runtime_age = age_seconds(runtime_ts, now)
    stream_watchdog_ts = parse_utc(stream_watchdog.get("ts_utc"))
    fast_recovery_ts = parse_utc(fast_recovery.get("ts_utc"))

    return {
        "current_fail": bool(youtube_current_fail),
        "current_degraded": bool(youtube_current_degraded),
        "youtube": {
            "status": ytw_status,
            "judgment": ytw_judgment,
            "remote_status": remote_status,
            "quota_guard_active": quota_guard,
            "stats_ts_utc": ytw_stats.get("ts_utc", ""),
            "stats_age_sec": _round_age(ytw_age),
            "stale": bool(ytw_stale),
            "max_stale_sec": max_youtube_stats_stale_sec,
            "expected_video_id": ytw_stats.get("expected_video_id") or ytw_stats.get("video_id", ""),
            "healthy": bool(ytw_stats.get("healthy", False)),
        },
        "local_runtime": {
            "status": runtime.get("status", ""),
            "run_id": runtime.get("run_id", ""),
            "ffmpeg_pid": runtime.get("ffmpeg_pid", ""),
            "stream_pid": runtime.get("stream_pid", ""),
            "updated_at_utc": runtime.get("updated_at_utc") or runtime.get("ts_utc", ""),
            "age_sec": _round_age(runtime_age),
            "source_file": runtime.get("_source_file", ""),
        },
        "stream_watchdog": {
            "status": stream_watchdog.get("status", ""),
            "judgment": stream_watchdog.get("judgment", ""),
            "ts_utc": stream_watchdog.get("ts_utc", ""),
            "age_sec": _round_age(age_seconds(stream_watchdog_ts, now)),
        },
        "latest_fast_recovery": {
            "kind": fast_recovery.get("kind", ""),
            "trigger": fast_recovery.get("trigger", ""),
            "ts_utc": fast_recovery.get("ts_utc", ""),
            "age_sec": _round_age(age_seconds(fast_recovery_ts, now)),
        },
    }


def _v2_freshness(summary: dict[str, Any], *, now: datetime, max_v2_status_stale_sec: float) -> dict[str, Any]:
    status_ts_utc = str(summary.get("when", {}).get("status_ts_utc", "") if isinstance(summary.get("when"), dict) else "")
    status_ts = parse_utc(status_ts_utc)
    age = age_seconds(status_ts, now)
    available = bool(status_ts_utc)
    return {
        "available": available,
        "status_ts_utc": status_ts_utc,
        "age_sec": _round_age(age),
        "stale": (not available) or age is None or age > max_v2_status_stale_sec,
        "max_stale_sec": max_v2_status_stale_sec,
    }


def _answer(source: dict[str, Any], v2: dict[str, Any], freshness: dict[str, Any]) -> str:
    source_fail = bool(source.get("current_fail"))
    v2_overall = str(v2.get("observed_state", {}).get("overall", "unknown") if isinstance(v2.get("observed_state"), dict) else "unknown")
    selected_action = str(v2.get("selected_action", {}).get("action", "none") if isinstance(v2.get("selected_action"), dict) else "none")
    if source_fail:
        return "source_current_fail: production runtime evidence reports current failure"
    if freshness.get("stale"):
        return "source_ok_v2_stale: production looks OK but v2 shadow summary is missing or stale"
    if v2_overall == "healthy" and selected_action == "none":
        return "healthy: source current health OK and v2 same-url shadow selected no action"
    if v2_overall == "unknown":
        return "source_ok_v2_unknown: production current health OK; v2 blocks destructive action because evidence is insufficient or stale"
    return f"source_ok_v2_{v2_overall}: v2 selected shadow action is {selected_action}"


def _round_age(value: float | None) -> float | None:
    return None if value is None else round(value, 3)
