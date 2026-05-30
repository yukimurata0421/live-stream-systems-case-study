from __future__ import annotations

import calendar
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

try:
    from stream_core.notifications import incidents as notify_incidents
except ModuleNotFoundError:
    from notifications import incidents as notify_incidents


@dataclass(frozen=True)
class NotifyCliContext:
    base_dir: Path
    state_base_dir: Path
    notify_env_file: Path
    notify_timer: str
    maintenance_state_file: Path
    stream1090_report_events_file: Path
    upstream_report_events_file: Path
    youtube_watchdog_stats_file: Path
    read_env_file: Callable[[Path], dict]
    read_json_file: Callable[[Path], dict]
    parse_bool: Callable[[str], bool | None]
    parse_utc_ts: Callable[[str], int]
    read_maintenance_state: Callable[[Path | None], dict]
    observe_payload: Callable[[int], tuple[int, dict, str]]


def load_stream_notify_config(ctx: NotifyCliContext) -> dict:
    env_files = [
        Path("/etc/default/adsb-streamnew"),
        ctx.notify_env_file,
        ctx.base_dir / ".state" / "env" / "adsb-streamnew.env",
        ctx.base_dir / ".state" / "env" / "adsb-streamnew-notify.env",
    ]
    override = os.environ.get("STREAM_NOTIFY_ENV_FILE", "").strip()
    if override:
        env_files.append(Path(override))
    cfg: dict[str, str] = {}
    for env_file in env_files:
        cfg.update(ctx.read_env_file(env_file))
    cfg.update({key: value for key, value in os.environ.items() if key.startswith("STREAM_NOTIFY_")})
    fast_recovery_event_triggers = [
        part.strip()
        for part in str(
            cfg.get(
                "STREAM_NOTIFY_FAST_RECOVERY_TRIGGERS",
                "tcp_stall,network_down,ffmpeg_missing",
            )
            or ""
        ).split(",")
        if part.strip()
    ]
    return {
        "enabled": ctx.parse_bool(cfg.get("STREAM_NOTIFY_ENABLED", "1")) is not False,
        "webhook_url": cfg.get("STREAM_NOTIFY_DISCORD_WEBHOOK_URL", "").strip(),
        "repeat_sec": max(30, int(cfg.get("STREAM_NOTIFY_REPEAT_SEC", "60") or "60")),
        "maintenance_repeat_sec": max(60, int(cfg.get("STREAM_NOTIFY_MAINTENANCE_REPEAT_SEC", "600") or "600")),
        "report_stale_sec": max(60, int(cfg.get("STREAM_NOTIFY_REPORT_STALE_SEC", "1800") or "1800")),
        "startup_grace_sec": max(0, int(cfg.get("STREAM_NOTIFY_STARTUP_GRACE_SEC", "300") or "300")),
        "username": cfg.get("STREAM_NOTIFY_USERNAME", "ADS-B Stream Watchdog").strip() or "ADS-B Stream Watchdog",
        "outbox_ttl_sec": max(3600, int(cfg.get("STREAM_NOTIFY_OUTBOX_TTL_SEC", "86400") or "86400")),
        "outbox_max_pending": max(1, int(cfg.get("STREAM_NOTIFY_OUTBOX_MAX_PENDING", "50") or "50")),
        "outbox_flush_limit": max(1, int(cfg.get("STREAM_NOTIFY_OUTBOX_FLUSH_LIMIT", "10") or "10")),
        "fast_recovery_event_recent_sec": max(
            60,
            int(cfg.get("STREAM_NOTIFY_FAST_RECOVERY_EVENT_RECENT_SEC", "1800") or "1800"),
        ),
        "fast_recovery_event_triggers": fast_recovery_event_triggers,
    }


def report_incident_spec(ctx: NotifyCliContext, ident: str) -> tuple[Path, str] | None:
    return notify_incidents.report_incident_spec(
        ident,
        stream1090_report_events_file=ctx.stream1090_report_events_file,
        upstream_report_events_file=ctx.upstream_report_events_file,
    )


def runtime_start_ts_from_run_id(run_id: str) -> int:
    raw = str(run_id or "").strip()
    token = raw.split("-", 1)[0]
    try:
        return int(calendar.timegm(time.strptime(token, "%Y%m%dT%H%M%SZ")))
    except Exception:
        return 0


def latest_runtime_start_ts(ctx: NotifyCliContext, state_base_dir: Path | None = None) -> int:
    root = state_base_dir or ctx.state_base_dir
    candidates = list(root.glob("stream_runtime_state_*.json"))
    direct = root / "stream_runtime_state.json"
    if direct.exists():
        candidates.append(direct)
    latest_start = 0
    for path in candidates:
        payload = ctx.read_json_file(path)
        if str(payload.get("status", "")).lower() != "running":
            continue
        start_ts = runtime_start_ts_from_run_id(str(payload.get("run_id", "")))
        if start_ts > latest_start:
            latest_start = start_ts
    return latest_start


def notify_bootstrap_grace_active(
    ctx: NotifyCliContext,
    now_ts: int,
    startup_grace_sec: int,
    *,
    state_base_dir: Path | None = None,
) -> bool:
    if startup_grace_sec <= 0:
        return False
    start_ts = latest_runtime_start_ts(ctx, state_base_dir)
    return start_ts > 0 and 0 <= (int(now_ts) - start_ts) <= startup_grace_sec


def collect_notification_incidents(
    ctx: NotifyCliContext,
    *,
    now_ts: int | None = None,
    report_stale_sec: int = 1800,
    startup_grace_sec: int = 0,
) -> list[dict]:
    now = int(time.time() if now_ts is None else now_ts)
    return notify_incidents.collect_notification_incidents(
        observe_payload=ctx.observe_payload,
        stream1090_report_events_file=ctx.stream1090_report_events_file,
        upstream_report_events_file=ctx.upstream_report_events_file,
        youtube_watchdog_stats_file=ctx.youtube_watchdog_stats_file,
        now_ts=now,
        report_stale_sec=report_stale_sec,
        bootstrap_grace_active=notify_bootstrap_grace_active(ctx, now, startup_grace_sec),
    )


def recovery_observation_for_incident(ctx: NotifyCliContext, ident: str, now_ts: int) -> tuple[int, str]:
    return notify_incidents.recovery_observation_for_incident(
        ident,
        now_ts,
        stream1090_report_events_file=ctx.stream1090_report_events_file,
        upstream_report_events_file=ctx.upstream_report_events_file,
    )


def maintenance_notification_incident(ctx: NotifyCliContext, now_ts: int, state_file: Path | None = None) -> dict | None:
    maintenance_state = ctx.read_maintenance_state(state_file)
    if maintenance_state.get("active") is not True:
        return None
    started_ts = ctx.parse_utc_ts(str(maintenance_state.get("started_at_utc", "") or ""))
    if started_ts <= 0:
        started_ts = now_ts
    item = notify_incidents.incident(
        ident="maintenance:mode_active",
        severity="info",
        component="maintenance_mode",
        summary="maintenance_mode_active",
        evidence=(
            f"state_file={ctx.maintenance_state_file} "
            f"started_at_utc={maintenance_state.get('started_at_utc', '')} "
            f"notify_timer={ctx.notify_timer}"
        ),
        recovery_type="human_maintenance_resume_required",
        follow_up="作業完了後は stream m off を実行し、stream m status と health-summary を確認する",
        observed_ts=started_ts,
    )
    item["_first_seen_ts"] = started_ts
    return item
