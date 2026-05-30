from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class HealthContext:
    observe_stream_health_script: Path
    log_base_dir: Path
    fast_recovery_events_file: Path
    youtube_watchdog_events_file: Path
    run: Callable[..., object]
    iter_jsonl: Callable[[Path], object]
    parse_utc_ts: Callable[[str], int]


def remote_warning_restart_judgment(count_1h: int, count_24h: int) -> tuple[str, str]:
    if count_1h >= 2:
        return "review_confirm_condition_immediate", "remote_warning restart count >=2 in 1h"
    if count_24h >= 4:
        return "review_confirm_condition", "remote_warning restart count >=4 in 24h"
    if count_24h >= 2:
        return "observe", "remote_warning restart count is 2-3 in 24h"
    return "ok_single_or_none", "remote_warning restart count <=1 in 24h"


def exit_224_judgment(count_1h: int, count_24h: int) -> tuple[str, str]:
    if count_1h >= 2:
        return "investigate_immediate", "ffmpeg exit_224 count >=2 in 1h"
    if count_24h >= 4:
        return "investigate_network_or_ingest", "ffmpeg exit_224 count >=4 in 24h"
    if count_24h >= 2:
        return "observe_rtmp_path", "ffmpeg exit_224 count is 2-3 in 24h"
    return "ok_single_or_none", "ffmpeg exit_224 count <=1 in 24h"


def compact_watchdog_event(item: dict) -> dict:
    return {
        "ts_utc": item.get("ts_utc", ""),
        "status": item.get("status", ""),
        "judgment": item.get("judgment", ""),
        "api_live_state": item.get("api_live_state", ""),
        "stream_active": item.get("stream_active", ""),
        "ingest_connected": item.get("ingest_connected", ""),
        "local_ok": item.get("local_ok", ""),
        "oauth_stream_status": item.get("oauth_stream_status", ""),
        "oauth_stream_health_status": item.get("oauth_stream_health_status", ""),
        "health_source": item.get("health_source", ""),
    }


def nearest_watchdog_context(ctx: HealthContext, items: list[dict], ts: int) -> tuple[dict, dict]:
    before: dict = {}
    after: dict = {}
    for item in items:
        item_ts = ctx.parse_utc_ts(str(item.get("ts_utc", "")))
        if item_ts <= 0:
            continue
        if item_ts <= ts:
            before = item
            continue
        after = item
        break
    return compact_watchdog_event(before) if before else {}, compact_watchdog_event(after) if after else {}


def remote_warning_comparison_payload(
    ctx: HealthContext,
    *,
    log_dir: Path | None = None,
    hours: int = 24,
    limit: int = 5,
    now_ts: int | None = None,
) -> dict:
    now = int(now_ts if now_ts is not None else time.time())
    cutoff = now - max(1, int(hours)) * 3600
    root = log_dir or ctx.log_base_dir
    fast_events_path = root / ctx.fast_recovery_events_file.name
    ytw_events_path = root / ctx.youtube_watchdog_events_file.name

    ytw_items = sorted(
        (
            item
            for item in ctx.iter_jsonl(ytw_events_path)
            if "status" in item and ctx.parse_utc_ts(str(item.get("ts_utc", ""))) >= cutoff - 900
        ),
        key=lambda item: ctx.parse_utc_ts(str(item.get("ts_utc", ""))),
    )
    events: list[dict] = []
    for item in ctx.iter_jsonl(fast_events_path):
        ts = ctx.parse_utc_ts(str(item.get("ts_utc", "")))
        if ts <= 0 or ts < cutoff:
            continue
        if str(item.get("kind", "")).strip() != "restart":
            continue
        if str(item.get("trigger", "")).strip() != "remote_warning":
            continue
        before, after = nearest_watchdog_context(ctx, ytw_items, ts)
        metrics = item.get("metrics") if isinstance(item.get("metrics"), dict) else {}
        youtube_hint = item.get("youtube_hint") if isinstance(item.get("youtube_hint"), dict) else {}
        events.append(
            {
                "ts_utc": item.get("ts_utc", ""),
                "message": item.get("message", ""),
                "ffmpeg_pid": item.get("ffmpeg_pid", ""),
                "ffmpeg_uptime_sec": item.get("ffmpeg_uptime_sec", ""),
                "metrics": {
                    "bytes_sent_delta": metrics.get("bytes_sent_delta"),
                    "lastsnd_ms": metrics.get("lastsnd_ms"),
                    "notsent": metrics.get("notsent"),
                    "unacked": metrics.get("unacked"),
                    "network_down": metrics.get("network_down"),
                    "remote_warning": metrics.get("remote_warning"),
                },
                "youtube_hint": {
                    "api_live_state": youtube_hint.get("api_live_state", ""),
                    "oauth_life_cycle_status": youtube_hint.get("oauth_life_cycle_status", ""),
                    "oauth_stream_status": youtube_hint.get("oauth_stream_status", ""),
                    "oauth_stream_health_status": youtube_hint.get("oauth_stream_health_status", ""),
                    "remote_source": youtube_hint.get("remote_source", ""),
                    "remote_status": youtube_hint.get("remote_status", ""),
                },
                "youtube_watchdog_before": before,
                "youtube_watchdog_after": after,
            }
        )

    events = sorted(events, key=lambda item: str(item.get("ts_utc", "")), reverse=True)
    limited = events[: max(1, int(limit))]
    return {
        "mode": "read_only",
        "hours": max(1, int(hours)),
        "remote_warning_restart_count": len(events),
        "shown": len(limited),
        "events": limited,
    }


def remote_warning_compare(ctx: HealthContext, *, hours: int = 24, limit: int = 5, json_output: bool = False) -> int:
    payload = remote_warning_comparison_payload(ctx, hours=hours, limit=limit)
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0
    print(
        "[remote-warning-compare] "
        f"mode=read_only hours={payload['hours']} count={payload['remote_warning_restart_count']} shown={payload['shown']}"
    )
    for item in payload["events"]:
        metrics = item.get("metrics", {})
        hint = item.get("youtube_hint", {})
        before = item.get("youtube_watchdog_before", {})
        after = item.get("youtube_watchdog_after", {})
        print(f"[remote-warning-compare] ts={item.get('ts_utc', '')} message={item.get('message', '')}")
        print(
            "[remote-warning-compare] "
            f"metrics bytes_sent_delta={metrics.get('bytes_sent_delta')} lastsnd_ms={metrics.get('lastsnd_ms')} "
            f"notsent={metrics.get('notsent')} unacked={metrics.get('unacked')} "
            f"network_down={metrics.get('network_down')}"
        )
        print(
            "[remote-warning-compare] "
            f"youtube_hint api_live_state={hint.get('api_live_state', '')} "
            f"oauth_stream_status={hint.get('oauth_stream_status', '')} "
            f"oauth_stream_health_status={hint.get('oauth_stream_health_status', '')} "
            f"remote_source={hint.get('remote_source', '')}"
        )
        print(
            "[remote-warning-compare] "
            f"watchdog_before={before.get('ts_utc', '')}:{before.get('status', '')}/"
            f"{before.get('oauth_stream_status', '')}/{before.get('oauth_stream_health_status', '')} "
            f"watchdog_after={after.get('ts_utc', '')}:{after.get('status', '')}/"
            f"{after.get('oauth_stream_status', '')}/{after.get('oauth_stream_health_status', '')}"
        )
    return 0


def parse_windows(raw: str) -> list[int]:
    out: list[int] = []
    for part in raw.split(","):
        s = part.strip().lower().removesuffix("h")
        if not s:
            continue
        try:
            value = int(s)
        except ValueError:
            continue
        if value > 0 and value not in out:
            out.append(value)
    return out or [1, 8, 24]


def observe_payload(ctx: HealthContext, hours: int) -> tuple[int, dict, str]:
    cp = ctx.run([sys.executable, str(ctx.observe_stream_health_script), "--hours", str(hours)], check=False)
    raw = (cp.stdout or "").strip()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return cp.returncode or 2, {}, (cp.stderr or cp.stdout or "").strip()
    return cp.returncode, payload if isinstance(payload, dict) else {}, (cp.stderr or "").strip()


def health_summary(
    *,
    observe: Callable[[int], tuple[int, dict, str]],
    windows: str = "1,8,24",
    json_output: bool = False,
) -> int:
    payloads: list[dict] = []
    rc = 0
    for hours in parse_windows(windows):
        item_rc, payload, error = observe(hours)
        if item_rc != 0 and rc == 0:
            rc = item_rc
        payloads.append({"hours": hours, "returncode": item_rc, "error": error, "observe": payload})

    combined = {"mode": "read_only", "windows": payloads}
    if json_output:
        print(json.dumps(combined, ensure_ascii=False, separators=(",", ":")))
        return rc

    for item in payloads:
        payload = item.get("observe") if isinstance(item.get("observe"), dict) else {}
        checks = payload.get("checks") if isinstance(payload.get("checks"), dict) else {}
        print(
            "[health-summary] "
            f"window={item.get('hours')}h rc={item.get('returncode')} pass={payload.get('pass', '')} "
            f"current_fail={checks.get('current_fail', '')} historical_degraded={checks.get('historical_degraded', '')} "
            f"remote_warning_1h={payload.get('remote_warning_restart_count_1h', 0)} "
            f"remote_warning_24h={payload.get('remote_warning_restart_count_24h', 0)} "
            f"remote_warning_judgment={payload.get('remote_warning_restart_judgment', '')} "
            f"public_probe_1h={payload.get('public_probe_degraded_count_1h', 0)} "
            f"public_probe_24h={payload.get('public_probe_degraded_count_24h', 0)} "
            f"public_probe_live_ok_24h={payload.get('public_probe_authoritative_live_ok_count_24h', 0)} "
            f"public_probe_judgment={payload.get('public_probe_judgment', '')} "
            f"fast_mode_episodes_24h={payload.get('fast_mode_episode_count_24h', 0)} "
            f"fast_mode_duration_24h={payload.get('fast_mode_active_duration_sec_24h', 0)} "
            f"fast_mode_units_est={payload.get('fast_mode_api_units_estimated_24h', 0)} "
            f"api_report={payload.get('api_report_judgment', '')} "
            f"send_mbps_p50={payload.get('ffmpeg_tcp_send_mbps_24h_p50', '')} "
            f"send_mbps_p95={payload.get('ffmpeg_tcp_send_mbps_24h_p95', '')} "
            f"send_mbps_max={payload.get('ffmpeg_tcp_send_mbps_24h_max', '')} "
            f"send_over_5mbps_sec={payload.get('ffmpeg_tcp_send_mbps_24h_over_5mbps_duration_sec', 0)} "
            f"send_judgment={payload.get('ffmpeg_tcp_send_budget_judgment', '')} "
            f"encoder_gap_24h={payload.get('encoder_gap_enable_auto_stop_false_duration_sec_24h', 0)} "
            f"ffmpeg_restart_attempts_24h={payload.get('stream_engine_ffmpeg_restart_attempts_24h', payload.get('stream_engine_ffmpeg_restart_count_24h', 0))} "
            f"ffmpeg_restart_episodes_24h={payload.get('stream_engine_ffmpeg_restart_retry_episodes_24h', 0)} "
            f"ffmpeg_restart_clusters_24h={payload.get('stream_engine_ffmpeg_restart_incident_clusters_24h', 0)} "
            f"ffmpeg_restart_root_causes_24h={payload.get('stream_engine_ffmpeg_restart_incident_root_causes_24h', {})} "
            f"exit224_1h={payload.get('stream_engine_ffmpeg_exit_224_count_1h', 0)} "
            f"exit224_24h={payload.get('stream_engine_ffmpeg_exit_224_count_24h', 0)} "
            f"exit224_judgment={payload.get('stream_engine_ffmpeg_exit_224_judgment', '')} "
            f"rtmps_ssl_tls_1h={payload.get('rtmps_ssl_tls_count_1h', 0)} "
            f"rtmps_ssl_tls_24h={payload.get('rtmps_ssl_tls_count_24h', 0)} "
            f"rtmps_ssl_tls_judgment={payload.get('rtmps_ssl_tls_judgment', '')}"
        )
    return rc
