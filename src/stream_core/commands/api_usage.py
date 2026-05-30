from __future__ import annotations

import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class ApiUsageContext:
    api_cost_report_script: Path
    youtube_quota_state_file: Path
    youtube_watchdog_stats_file: Path
    youtube_api_cost_open_day_latest_file: Path
    youtube_api_cost_latest_file: Path
    api_cost_open_day_report_timer: str
    api_cost_report_timer: str
    run: Callable[..., subprocess.CompletedProcess[str]]
    read_json_file: Callable[[Path], dict]
    parse_utc_ts: Callable[[str], int]


def file_mtime_age(path: Path, now_ts: int | None = None) -> int | None:
    now = int(time.time() if now_ts is None else now_ts)
    try:
        return now - int(path.stat().st_mtime)
    except OSError:
        return None


def api_report_effective_end_ts(ctx: ApiUsageContext, payload: dict) -> int:
    window = payload.get("window") if isinstance(payload.get("window"), dict) else {}
    return ctx.parse_utc_ts(str(window.get("effective_end_utc", "")))


def api_report_freshness(
    ctx: ApiUsageContext,
    path: Path,
    *,
    max_mtime_age_sec: int,
    max_effective_end_age_sec: int | None = None,
) -> dict:
    now = int(time.time())
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        loaded = {}
    payload = loaded if isinstance(loaded, dict) else {}
    age = file_mtime_age(path, now)
    effective_end_ts = api_report_effective_end_ts(ctx, payload)
    effective_end_age = now - effective_end_ts if effective_end_ts > 0 else None
    fresh = path.exists() and age is not None and age <= max_mtime_age_sec
    if max_effective_end_age_sec is not None:
        fresh = fresh and effective_end_age is not None and effective_end_age <= max_effective_end_age_sec
    totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
    window = payload.get("window") if isinstance(payload.get("window"), dict) else {}
    return {
        "path": str(path),
        "exists": path.exists(),
        "fresh": fresh,
        "mtime_age_sec": age,
        "effective_end_utc": window.get("effective_end_utc", ""),
        "effective_end_age_sec": effective_end_age,
        "target_day": payload.get("target_day", ""),
        "open_day": window.get("open_day", ""),
        "status": payload.get("status", ""),
        "units": totals.get("units", 0),
        "calls": totals.get("calls", 0),
        "quota_exceeded_events": totals.get("quota_exceeded_events", 0),
        "max_mtime_age_sec": max_mtime_age_sec,
        "max_effective_end_age_sec": max_effective_end_age_sec,
    }


def systemd_timer_status(ctx: ApiUsageContext, unit: str) -> dict:
    cp = ctx.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=LoadState,ActiveState,SubState,NextElapseUSecRealtime,LastTriggerUSec",
        ],
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


def api_report_observation_payload(ctx: ApiUsageContext) -> dict:
    open_day = api_report_freshness(
        ctx,
        ctx.youtube_api_cost_open_day_latest_file,
        max_mtime_age_sec=1800,
        max_effective_end_age_sec=1800,
    )
    closed = api_report_freshness(ctx, ctx.youtube_api_cost_latest_file, max_mtime_age_sec=10800)
    timers = {
        ctx.api_cost_open_day_report_timer: systemd_timer_status(ctx, ctx.api_cost_open_day_report_timer),
        ctx.api_cost_report_timer: systemd_timer_status(ctx, ctx.api_cost_report_timer),
    }
    timers_active = all(item.get("active") is True for item in timers.values())
    if not timers_active:
        judgment = "api_report_timer_attention"
    elif not open_day.get("fresh"):
        judgment = "api_open_day_report_stale"
    elif not closed.get("fresh"):
        judgment = "api_closed_day_report_stale"
    else:
        judgment = "ok"
    return {
        "open_day_latest": open_day,
        "latest_closed_day": closed,
        "timers": timers,
        "timers_active": timers_active,
        "judgment": judgment,
    }


def report_command(ctx: ApiUsageContext, *, closed_day: bool, day: str) -> list[str]:
    cmd = [
        sys.executable,
        str(ctx.api_cost_report_script),
        "--tz",
        "America/Los_Angeles",
        "--allow-near-boundary",
        "--deferred-exit-code",
        "0",
    ]
    if day:
        cmd.extend(["--day", day])
    if closed_day:
        cmd.append("--allow-just-closed-day")
    else:
        cmd.extend(
            [
                "--include-open-day",
                "--coverage-start-gap-mode",
                "warn",
                "--coverage-end-gap-grace-sec",
                "900",
            ]
        )
    return cmd


def format_summary(payload: dict, quota_state: dict, watchdog_stats: dict, report_observation: dict) -> str:
    window = payload.get("window") if isinstance(payload.get("window"), dict) else {}
    totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
    ingest = payload.get("ingest") if isinstance(payload.get("ingest"), dict) else {}
    by_method = payload.get("by_method") if isinstance(payload.get("by_method"), dict) else {}
    by_source = payload.get("by_source") if isinstance(payload.get("by_source"), dict) else {}

    lines = [
        (
            "[api-usage] "
            f"tz={window.get('tz', 'America/Los_Angeles')} "
            f"target_day={payload.get('target_day', '')} "
            f"open_day={window.get('open_day', '')} "
            f"status={payload.get('status', '')}"
        ),
        (
            "[api-usage] "
            f"calls={totals.get('calls', 0)} "
            f"units={totals.get('units', 0)} "
            f"quota_exceeded_events={totals.get('quota_exceeded_events', 0)}"
        ),
        (
            "[api-usage] "
            f"coverage_ok={ingest.get('coverage_ok', '')} "
            f"coverage_observed_ratio={ingest.get('coverage_observed_ratio', '')} "
            f"coverage_gap_start_ratio={ingest.get('coverage_gap_start_ratio', '')} "
            f"coverage_gap_end_ratio={ingest.get('coverage_gap_end_ratio', '')}"
        ),
        f"[api-usage] by_method={json.dumps(by_method, ensure_ascii=False, sort_keys=True)}",
        f"[api-usage] by_source={json.dumps(by_source, ensure_ascii=False, sort_keys=True)}",
        (
            "[api-usage] quota_state "
            f"quota_exhausted={quota_state.get('quota_exhausted', '')} "
            f"until_ts={quota_state.get('quota_exhausted_until_ts', '')} "
            f"source={quota_state.get('quota_exhausted_source', '')}"
        ),
        (
            "[api-usage] burn_guard "
            f"active={watchdog_stats.get('api_cost_burn_rate_active', '')} "
            f"projected_units_per_day={watchdog_stats.get('api_cost_projected_units_per_day', '')} "
            f"threshold_units_per_day={watchdog_stats.get('api_cost_threshold_units_per_day', '')}"
        ),
    ]
    open_day = report_observation.get("open_day_latest") if isinstance(report_observation.get("open_day_latest"), dict) else {}
    closed = report_observation.get("latest_closed_day") if isinstance(report_observation.get("latest_closed_day"), dict) else {}
    lines.append(
        "[api-usage] report_freshness "
        f"judgment={report_observation.get('judgment', '')} "
        f"timers_active={report_observation.get('timers_active', '')} "
        f"open_day_fresh={open_day.get('fresh', '')} open_day_age_sec={open_day.get('mtime_age_sec', '')} "
        f"closed_day_fresh={closed.get('fresh', '')} closed_day_age_sec={closed.get('mtime_age_sec', '')}"
    )
    warnings = ingest.get("coverage_warnings")
    if warnings:
        lines.append(f"[api-usage] coverage_warnings={json.dumps(warnings, ensure_ascii=False)}")
    reason = payload.get("reason")
    if reason:
        lines.append(f"[api-usage] reason={reason}")
    return "\n".join(lines)


def api_usage(ctx: ApiUsageContext, *, closed_day: bool = False, day: str = "", json_output: bool = False) -> int:
    cp = ctx.run(report_command(ctx, closed_day=closed_day, day=day), check=False)
    raw = (cp.stdout or "").strip()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        if cp.stdout:
            print(cp.stdout, end="")
        if cp.stderr:
            print(cp.stderr, end="", file=sys.stderr)
        return cp.returncode if cp.returncode else 2

    quota_state = ctx.read_json_file(ctx.youtube_quota_state_file)
    watchdog_stats = ctx.read_json_file(ctx.youtube_watchdog_stats_file)
    report_observation = api_report_observation_payload(ctx)
    if json_output:
        print(
            json.dumps(
                {
                    "api_cost_report": payload,
                    "quota_state": quota_state,
                    "api_report_observation": report_observation,
                    "watchdog_stats": {
                        "api_cost_burn_rate_active": watchdog_stats.get("api_cost_burn_rate_active"),
                        "api_cost_projected_units_per_day": watchdog_stats.get("api_cost_projected_units_per_day"),
                        "api_cost_threshold_units_per_day": watchdog_stats.get("api_cost_threshold_units_per_day"),
                        "api_cost_burn_rate_reason": watchdog_stats.get("api_cost_burn_rate_reason"),
                        "judgment": watchdog_stats.get("judgment"),
                        "status": watchdog_stats.get("status"),
                    },
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
    else:
        print(format_summary(payload, quota_state, watchdog_stats, report_observation))
    if cp.stderr:
        print(cp.stderr, end="", file=sys.stderr)
    return cp.returncode
