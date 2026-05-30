#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parents[2]
STATE_BASE_DIR = Path(
    os.environ.get(
        "STREAM_RUNTIME_STATE_DIR",
        str(BASE_DIR / ".state" / "adsb-streamnew-v2"),
    )
).expanduser()
LOG_BASE_DIR = Path(
    os.environ.get(
        "STREAM_RUNTIME_LOG_DIR",
        str(STATE_BASE_DIR / "logs"),
    )
).expanduser()
DEFAULT_LOG_FILE = Path(
    os.environ.get(
        "YTW_API_CALL_LOG_FILE",
        str(LOG_BASE_DIR / "youtube_api_calls.jsonl"),
    )
)


class Window:
    def __init__(
        self,
        *,
        target_day: date,
        tz_name: str,
        start_ts: int,
        end_ts: int,
        effective_end_ts: int,
        open_day: bool,
    ) -> None:
        self.target_day = target_day
        self.tz_name = tz_name
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.effective_end_ts = effective_end_ts
        self.open_day = open_day


def parse_ts_utc(raw: str) -> int | None:
    text = (raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return int(datetime.fromisoformat(text).timestamp())
    except Exception:
        return None


def day_start_ts(day: date, tz: ZoneInfo) -> int:
    return int(datetime.combine(day, dt_time(0, 0, 0), tzinfo=tz).timestamp())


def resolve_target_day(now_local: datetime, day_arg: str, include_open_day: bool) -> date:
    if day_arg:
        return date.fromisoformat(day_arg)
    if include_open_day:
        return now_local.date()
    return now_local.date() - timedelta(days=1)


def build_window(
    *,
    now_utc_ts: int,
    tz_name: str,
    day_arg: str,
    include_open_day: bool,
    lag_sec: int,
) -> Window:
    tz = ZoneInfo(tz_name)
    now_local = datetime.fromtimestamp(now_utc_ts, tz=tz)
    target = resolve_target_day(now_local, day_arg, include_open_day)
    start_ts = day_start_ts(target, tz)
    end_ts = day_start_ts(target + timedelta(days=1), tz)
    open_day = target == now_local.date()
    effective_end_ts = end_ts
    if open_day:
        effective_end_ts = min(end_ts, now_utc_ts - lag_sec)
    return Window(
        target_day=target,
        tz_name=tz_name,
        start_ts=start_ts,
        end_ts=end_ts,
        effective_end_ts=effective_end_ts,
        open_day=open_day,
    )


def seconds_to_next_midnight(now_utc_ts: int, tz_name: str) -> int:
    tz = ZoneInfo(tz_name)
    now_local = datetime.fromtimestamp(now_utc_ts, tz=tz)
    next_midnight = datetime.combine(now_local.date() + timedelta(days=1), dt_time(0, 0, 0), tzinfo=tz)
    return max(0, int(next_midnight.timestamp()) - now_utc_ts)


def resolve_log_files(path: Path) -> list[Path]:
    candidates = [p for p in sorted(path.parent.glob(path.name + "*")) if p.is_file()]
    log_files: list[Path] = []
    for candidate in candidates:
        name = candidate.name
        if name.endswith((".lock", ".tmp")) or ".lock." in name or ".tmp." in name:
            continue
        if name == path.name or name.startswith(path.name + "."):
            log_files.append(candidate)
    if not log_files and path.exists():
        log_files.append(path)
    return log_files


def iter_jsonl(path: Path):
    opener = gzip.open if path.name.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            raw = line.strip()
            if not raw:
                continue
            try:
                yield json.loads(raw)
            except Exception:
                yield None


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp.replace(path)


def _append_jsonl(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def _emit_and_maybe_persist(payload: dict, *, output_dir: str, output_latest_file: str) -> None:
    if output_dir and payload.get("status") == "ok" and payload.get("target_day"):
        day = str(payload.get("target_day"))
        tz = str((payload.get("window") or {}).get("tz", "")).strip() or "tz"
        tz_slug = tz.replace("/", "_")
        out_path = Path(output_dir).expanduser() / f"youtube_api_cost_{day}_{tz_slug}.jsonl"
        _append_jsonl(out_path, payload)
    if output_latest_file:
        _write_json_atomic(Path(output_latest_file).expanduser(), payload)
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Aggregate YouTube API quota cost from youtube_api_calls.jsonl. "
            "Default target is the latest closed PT day to avoid partial-day misreads."
        )
    )
    ap.add_argument("--log-file", default=str(DEFAULT_LOG_FILE), help="Path to youtube_api_calls.jsonl")
    ap.add_argument("--tz", default="America/Los_Angeles", help="Quota reset timezone (default: America/Los_Angeles)")
    ap.add_argument("--day", default="", help="Target day in YYYY-MM-DD of --tz")
    ap.add_argument(
        "--include-open-day",
        action="store_true",
        help="Aggregate current open day instead of last closed day (safe lag is applied)",
    )
    ap.add_argument(
        "--lag-sec",
        type=int,
        default=120,
        help="Exclude newest N seconds to avoid write race (default: 120)",
    )
    ap.add_argument(
        "--coverage-gap-grace-sec",
        type=int,
        default=300,
        help="Allowed gap from day start to first telemetry timestamp before marking degraded",
    )
    ap.add_argument(
        "--coverage-start-gap-mode",
        choices=("strict", "warn"),
        default="strict",
        help="How to treat day-start coverage gap: strict=degraded, warn=ok with warning",
    )
    ap.add_argument(
        "--coverage-end-gap-grace-sec",
        type=int,
        default=900,
        help="Allowed gap from window end to last in-window telemetry before marking degraded",
    )
    ap.add_argument(
        "--near-boundary-guard-sec",
        type=int,
        default=180,
        help="Defer when running this close to midnight in --tz unless --allow-near-boundary",
    )
    ap.add_argument(
        "--allow-near-boundary",
        action="store_true",
        help="Allow execution near midnight boundary",
    )
    ap.add_argument(
        "--allow-just-closed-day",
        action="store_true",
        help="Allow reporting the previous day immediately after midnight without lag wait",
    )
    ap.add_argument(
        "--deferred-exit-code",
        type=int,
        default=3,
        help="Exit code to return for deferred status (default: 3)",
    )
    ap.add_argument(
        "--output-dir",
        default="",
        help="Optional directory to append day summary as youtube_api_cost_<YYYY-MM-DD>_<TZ>.jsonl",
    )
    ap.add_argument(
        "--output-latest-file",
        default="",
        help="Optional file path to persist latest run payload (including deferred)",
    )
    args = ap.parse_args()

    log_file = Path(args.log_file).expanduser()
    lag_sec = max(0, int(args.lag_sec))
    now_utc_ts = int(datetime.now(timezone.utc).timestamp())

    sec_to_midnight = seconds_to_next_midnight(now_utc_ts, args.tz)
    if sec_to_midnight <= max(0, int(args.near_boundary_guard_sec)) and not args.allow_near_boundary:
        payload = {
            "status": "deferred",
            "reason": "near_midnight_boundary",
            "tz": args.tz,
            "seconds_to_next_midnight": sec_to_midnight,
            "hint": "Use --allow-near-boundary or run after boundary.",
        }
        _emit_and_maybe_persist(
            payload,
            output_dir=args.output_dir,
            output_latest_file=args.output_latest_file,
        )
        return int(args.deferred_exit_code)

    try:
        window = build_window(
            now_utc_ts=now_utc_ts,
            tz_name=args.tz,
            day_arg=args.day,
            include_open_day=bool(args.include_open_day),
            lag_sec=lag_sec,
        )
    except Exception as e:
        _emit_and_maybe_persist(
            {"status": "error", "reason": f"invalid window: {e}"},
            output_dir=args.output_dir,
            output_latest_file=args.output_latest_file,
        )
        return 2

    # Guard: a day that has just closed may still be receiving delayed flushes.
    if (
        (not window.open_day)
        and (not args.allow_just_closed_day)
        and now_utc_ts < (window.end_ts + lag_sec)
    ):
        payload = {
            "status": "deferred",
            "reason": "just_closed_day_within_lag",
            "tz": args.tz,
            "target_day": window.target_day.isoformat(),
            "lag_sec": lag_sec,
            "seconds_since_day_end": max(0, now_utc_ts - window.end_ts),
            "hint": "Re-run after lag or use --allow-just-closed-day.",
        }
        _emit_and_maybe_persist(
            payload,
            output_dir=args.output_dir,
            output_latest_file=args.output_latest_file,
        )
        return int(args.deferred_exit_code)

    if window.effective_end_ts <= window.start_ts:
        payload = {
            "status": "deferred",
            "reason": "no_stable_window",
            "target_day": window.target_day.isoformat(),
            "hint": "Open day with lag removed the full window; re-run later or lower --lag-sec.",
        }
        _emit_and_maybe_persist(
            payload,
            output_dir=args.output_dir,
            output_latest_file=args.output_latest_file,
        )
        return int(args.deferred_exit_code)

    log_files = resolve_log_files(log_file)
    if not log_files:
        payload = {
            "status": "degraded",
            "reason": "telemetry_missing",
            "target_day": window.target_day.isoformat(),
            "window": {
                "tz": args.tz,
                "start_utc": datetime.fromtimestamp(window.start_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                "end_utc": datetime.fromtimestamp(window.effective_end_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            },
            "totals": {"calls": 0, "units": 0},
            "by_method": {},
            "ingest": {
                "log_file": str(log_file),
                "log_files": [],
                "log_exists": False,
                "coverage_ok": False,
                "coverage_reason": "log file not found",
                "parse_errors": 0,
                "missing_ts": 0,
                "out_of_window": 0,
            },
        }
        _emit_and_maybe_persist(
            payload,
            output_dir=args.output_dir,
            output_latest_file=args.output_latest_file,
        )
        return 0

    by_method: dict[str, dict[str, int]] = {}
    by_status: dict[str, int] = {}
    by_source: dict[str, int] = {}
    totals_calls = 0
    totals_units = 0
    parse_errors = 0
    missing_ts = 0
    out_of_window = 0
    quota_exceeded_events = 0
    min_seen_ts = 0
    max_seen_ts = 0
    first_in_window_ts = 0
    last_in_window_ts = 0

    for path in log_files:
        for item in iter_jsonl(path):
            if item is None:
                parse_errors += 1
                continue
            ts = parse_ts_utc(str(item.get("ts_utc", "")))
            if ts is None:
                missing_ts += 1
                continue
            if min_seen_ts <= 0 or ts < min_seen_ts:
                min_seen_ts = ts
            if ts > max_seen_ts:
                max_seen_ts = ts
            if ts < window.start_ts or ts >= window.effective_end_ts:
                out_of_window += 1
                continue
            if first_in_window_ts <= 0 or ts < first_in_window_ts:
                first_in_window_ts = ts
            if ts > last_in_window_ts:
                last_in_window_ts = ts

            method = str(item.get("method", "unknown")).strip() or "unknown"
            status = str(item.get("status", "unknown")).strip() or "unknown"
            source = str(item.get("source", "unknown")).strip() or "unknown"
            units = int(item.get("cost_units", 0) or 0)
            quota_exceeded = bool(item.get("quota_exceeded", False))

            bucket = by_method.setdefault(
                method,
                {
                    "calls": 0,
                    "units": 0,
                    "ok": 0,
                    "http_error": 0,
                    "error": 0,
                    "quota_exceeded": 0,
                },
            )
            bucket["calls"] += 1
            bucket["units"] += units
            if status in {"ok", "http_error", "error"}:
                bucket[status] += 1
            if quota_exceeded:
                bucket["quota_exceeded"] += 1
                quota_exceeded_events += 1

            totals_calls += 1
            totals_units += units
            by_status[status] = by_status.get(status, 0) + 1
            by_source[source] = by_source.get(source, 0) + 1

    coverage_gap_start_sec = (
        max(0, first_in_window_ts - window.start_ts)
        if first_in_window_ts > 0
        else max(0, window.effective_end_ts - window.start_ts)
    )
    coverage_gap_end_sec = (
        max(0, window.effective_end_ts - last_in_window_ts)
        if last_in_window_ts > 0
        else max(0, window.effective_end_ts - window.start_ts)
    )
    coverage_window_sec = max(0, window.effective_end_ts - window.start_ts)
    coverage_observed_sec = (
        max(0, last_in_window_ts - first_in_window_ts)
        if first_in_window_ts > 0 and last_in_window_ts > 0
        else 0
    )
    coverage_gap_start_ratio = (
        coverage_gap_start_sec / coverage_window_sec if coverage_window_sec > 0 else 0.0
    )
    coverage_gap_end_ratio = (
        coverage_gap_end_sec / coverage_window_sec if coverage_window_sec > 0 else 0.0
    )
    coverage_observed_ratio = (
        coverage_observed_sec / coverage_window_sec if coverage_window_sec > 0 else 0.0
    )
    coverage_start_gap_grace_sec = max(0, int(args.coverage_gap_grace_sec))
    coverage_end_gap_grace_sec = max(0, int(args.coverage_end_gap_grace_sec))
    coverage_start_gap_mode = str(args.coverage_start_gap_mode).strip().lower()
    coverage_ok = True
    coverage_reason = "ok"
    coverage_warnings: list[str] = []
    coverage_failures: list[str] = []
    if parse_errors > 0:
        coverage_ok = False
        coverage_reason = f"jsonl parse errors present ({parse_errors})"
    elif missing_ts > 0:
        coverage_ok = False
        coverage_reason = f"missing ts records present ({missing_ts})"
    elif first_in_window_ts <= 0:
        coverage_ok = False
        coverage_reason = "no in-window telemetry timestamps"
    else:
        if coverage_gap_start_sec > coverage_start_gap_grace_sec:
            start_gap_msg = (
                f"coverage gap at day start ({coverage_gap_start_sec}s>"
                f"{coverage_start_gap_grace_sec}s)"
            )
            if coverage_start_gap_mode == "strict":
                coverage_failures.append(start_gap_msg)
            else:
                coverage_warnings.append(start_gap_msg)
        if coverage_gap_end_sec > coverage_end_gap_grace_sec:
            coverage_failures.append(
                f"coverage gap at window end ({coverage_gap_end_sec}s>"
                f"{coverage_end_gap_grace_sec}s)"
            )
        if coverage_failures:
            coverage_ok = False
            coverage_reason = "; ".join(coverage_failures)

    summary = {
        "status": "ok" if coverage_ok else "degraded",
        "reason": "" if coverage_ok else "telemetry_coverage_degraded",
        "target_day": window.target_day.isoformat(),
        "window": {
            "tz": args.tz,
            "open_day": window.open_day,
            "start_utc": datetime.fromtimestamp(window.start_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            "end_utc": datetime.fromtimestamp(window.end_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            "effective_end_utc": datetime.fromtimestamp(window.effective_end_ts, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "lag_sec": lag_sec,
        },
        "totals": {
            "calls": totals_calls,
            "units": totals_units,
            "quota_exceeded_events": quota_exceeded_events,
        },
        "by_method": dict(sorted(by_method.items())),
        "by_status": dict(sorted(by_status.items())),
        "by_source": dict(sorted(by_source.items())),
        "ingest": {
            "log_file": str(log_file),
            "log_files": [str(p) for p in log_files],
            "log_exists": True,
            "coverage_ok": coverage_ok,
            "coverage_reason": coverage_reason,
            "parse_errors": parse_errors,
            "missing_ts": missing_ts,
            "out_of_window": out_of_window,
            "coverage_window_sec": coverage_window_sec,
            "coverage_observed_sec": coverage_observed_sec,
            "coverage_observed_ratio": round(coverage_observed_ratio, 6),
            "coverage_gap_start_sec": coverage_gap_start_sec,
            "coverage_gap_start_ratio": round(coverage_gap_start_ratio, 6),
            "coverage_gap_end_sec": coverage_gap_end_sec,
            "coverage_gap_end_ratio": round(coverage_gap_end_ratio, 6),
            "coverage_start_gap_grace_sec": coverage_start_gap_grace_sec,
            "coverage_start_gap_mode": coverage_start_gap_mode,
            "coverage_end_gap_grace_sec": coverage_end_gap_grace_sec,
            "coverage_warnings": coverage_warnings,
            "first_seen_utc": datetime.fromtimestamp(min_seen_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            if min_seen_ts > 0
            else "",
            "last_seen_utc": datetime.fromtimestamp(max_seen_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            if max_seen_ts > 0
            else "",
            "first_in_window_utc": datetime.fromtimestamp(first_in_window_ts, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
            if first_in_window_ts > 0
            else "",
            "last_in_window_utc": datetime.fromtimestamp(last_in_window_ts, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
            if last_in_window_ts > 0
            else "",
        },
    }
    _emit_and_maybe_persist(
        summary,
        output_dir=args.output_dir,
        output_latest_file=args.output_latest_file,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
