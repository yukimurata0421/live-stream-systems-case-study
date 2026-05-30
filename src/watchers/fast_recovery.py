#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .systemctl_control import run_systemctl
    from .fast_recovery_core import budget as budget_policy
    from .fast_recovery_core import decision as recovery_decision
    from .fast_recovery_core import executor as recovery_executor
    from .fast_recovery_core import probes, state as recovery_state
    from .fast_recovery_core import remote_warning as remote_warning_core
    from .fast_recovery_core import restart_context as restart_context_writer
    from .fast_recovery_core import tcp_metrics
    from stream_core.supervisor.factory import build_runtime_supervisor
except ImportError:
    from systemctl_control import run_systemctl
    from fast_recovery_core import budget as budget_policy
    from fast_recovery_core import decision as recovery_decision
    from fast_recovery_core import executor as recovery_executor
    from fast_recovery_core import probes, state as recovery_state
    from fast_recovery_core import remote_warning as remote_warning_core
    from fast_recovery_core import restart_context as restart_context_writer
    from fast_recovery_core import tcp_metrics
    from stream_core.supervisor.factory import build_runtime_supervisor

LIVE_LIKE_LIFECYCLE = {"live", "liveStarting", "testing", "testStarting"}
API_REMOTE_SOURCES = {
    "data_api",
    "data_api_oauth",
    "data_api_search",
    "data_api_videos",
    "oauth",
    "oauth_api",
    "oauth_livebroadcasts",
    "oauth_livestreams",
    "oauth_probe",
    "search.list",
    "videos.list",
    "livebroadcasts.list",
    "livestreams.list",
    "youtube_api",
}
PUBLIC_REMOTE_SOURCES = {
    "channel_live_page",
    "public_watch_page",
}


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def int_env(name: str, default: int) -> int:
    raw = env(name, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def float_env(name: str, default: float) -> float:
    raw = env(name, str(default))
    try:
        return float(raw)
    except ValueError:
        return default


def bool_env(name: str, default: bool) -> bool:
    fallback = "1" if default else "0"
    return env(name, fallback).lower() in {"1", "true", "yes", "on"}


BASE_DIR = Path(__file__).resolve().parents[2]
STATE_ROOT = BASE_DIR / ".state" / "adsb-streamnew-v2"


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_iso_ts(value: str) -> int:
    s = (value or "").strip()
    if not s:
        return 0
    try:
        return int(datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return 0


def _normalize_remote_source(raw_source: str) -> str:
    return remote_warning_core.normalize_remote_source(raw_source)


def _is_api_remote_source(source: str) -> bool:
    return remote_warning_core.is_api_remote_source(source)


def _quota_guard_active_from_state(now_ts: int) -> tuple[bool, str]:
    return remote_warning_core.quota_guard_active_from_state(QUOTA_STATE_FILE, now_ts)


STREAM_SERVICE = env("FR_STREAM_SERVICE", "adsb-streamnew-youtube-stream.service")
RTMP_HOST = env("FR_RTMP_HOST", "a.rtmp.youtube.com")
DNS_HOST = env("FR_DNS_HOST", RTMP_HOST)
STATE_FILE = Path(env("FR_STATE_FILE", "/dev/shm/adsb-streamnew/fast_recovery_state.json"))
EVENT_LOG_FILE = Path(
    env("FR_EVENT_LOG_FILE", str(STATE_ROOT / "logs" / "fast_recovery_events.jsonl"))
)
YTW_STATS_FILE = Path(env("FR_YTW_STATS_FILE", str(STATE_ROOT / "youtube_watchdog_stats.json")))
QUOTA_STATE_FILE = Path(
    env("FR_QUOTA_STATE_FILE", str(STATE_ROOT / "youtube_quota_state.json"))
)
RESTART_REASON_FILE = Path(
    env("FR_RESTART_REASON_FILE", str(STATE_ROOT / "restart_reason.json"))
)

RTMP_PORTS = [int(p.strip()) for p in env("FR_RTMP_PORTS", "1935,443").split(",") if p.strip().isdigit()]
if not RTMP_PORTS:
    RTMP_PORTS = [1935, 443]
PUBLIC_PING_TARGETS = [x.strip() for x in env("FR_PUBLIC_PING_TARGETS", "1.1.1.1,8.8.8.8").split(",") if x.strip()]

NET_FAIL_CONFIRM = max(1, int_env("FR_NET_FAIL_CONFIRM", 1))
STALL_CONFIRM = max(1, int_env("FR_STALL_CONFIRM", 2))
REMOTE_WARNING_CONFIRM = max(1, int_env("FR_REMOTE_WARNING_CONFIRM", 1))
REMOTE_WARNING_REQUIRE_LOCAL_OK = bool_env("FR_REMOTE_WARNING_REQUIRE_LOCAL_OK", True)
REMOTE_WARNING_CONFIRM_DISTINCT_STATS = bool_env("FR_REMOTE_WARNING_CONFIRM_DISTINCT_STATS", True)
URL_PRESERVATION_MODE = bool_env("FR_URL_PRESERVATION_MODE", True)
YTW_STATUS_MAX_AGE_SEC = max(15, int_env("FR_YTW_STATUS_MAX_AGE_SEC", 180))

STALL_LASTSND_MS = max(1000, int_env("FR_STALL_LASTSND_MS", 4000))
STALL_NOTSENT_BYTES = max(4096, int_env("FR_STALL_NOTSENT_BYTES", 8192))
STALL_UNACKED = max(8, int_env("FR_STALL_UNACKED", 64))

MIN_FFMPEG_UPTIME_SEC = max(0, int_env("FR_MIN_FFMPEG_UPTIME_SEC", 20))
FFMPEG_MISSING_RESTART_SEC = max(5, int_env("FR_FFMPEG_MISSING_RESTART_SEC", 20))
FFMPEG_MISSING_SUCCESS_BACKOFF_SEC = max(0, int_env("FR_FFMPEG_MISSING_SUCCESS_BACKOFF_SEC", 60))
RESTART_GUARD_SEC = max(1, int_env("FR_RESTART_GUARD_SEC", 5))
RESTART_FAILURE_BACKOFF_SEC = max(1, int_env("FR_RESTART_FAILURE_BACKOFF_SEC", 30))

HOURLY_DOWNTIME_BUDGET_SEC = max(0, int_env("FR_HOURLY_DOWNTIME_BUDGET_SEC", 300))
DAILY_DOWNTIME_BUDGET_SEC = max(0, int_env("FR_DAILY_DOWNTIME_BUDGET_SEC", 1800))
RESTART_DOWNTIME_COST_SEC = max(1, int_env("FR_RESTART_DOWNTIME_COST_SEC", 30))
BUDGET_EMERGENCY_OVERRIDE_SEC = max(0, int_env("FR_BUDGET_EMERGENCY_OVERRIDE_SEC", 90))

SAMPLES_MAX = max(120, int_env("FR_SAMPLES_MAX", 1024))
TCP_SEND_SAMPLE_LOG_SEC = max(0, int_env("FR_TCP_SEND_SAMPLE_LOG_SEC", 60))

LOW_UPLOAD_PRESSURE_ENABLED = bool_env("FR_LOW_UPLOAD_PRESSURE_ENABLED", True)
LOW_UPLOAD_PRESSURE_CONFIRM = max(1, int_env("FR_LOW_UPLOAD_PRESSURE_CONFIRM", 3))
LOW_UPLOAD_PRESSURE_MAX_MBPS = max(0.1, float_env("FR_LOW_UPLOAD_PRESSURE_MAX_MBPS", 3.2))
LOW_UPLOAD_PRESSURE_NOTSENT_BYTES = max(4096, int_env("FR_LOW_UPLOAD_PRESSURE_NOTSENT_BYTES", 524288))
LOW_UPLOAD_PRESSURE_UNACKED = max(8, int_env("FR_LOW_UPLOAD_PRESSURE_UNACKED", 256))
LOW_UPLOAD_PRESSURE_LASTSND_MS = max(100, int_env("FR_LOW_UPLOAD_PRESSURE_LASTSND_MS", 1000))

EMERGENCY_LOW_UPLOAD_ENABLED = bool_env("FR_EMERGENCY_LOW_UPLOAD_ENABLED", True)
EMERGENCY_LOW_UPLOAD_TRIGGERS = {
    item.strip()
    for item in env("FR_EMERGENCY_LOW_UPLOAD_TRIGGERS", "network_down,low_upload_pressure").split(",")
    if item.strip()
}
EMERGENCY_LOW_UPLOAD_DURATION_SEC = max(60, int_env("FR_EMERGENCY_LOW_UPLOAD_DURATION_SEC", 900))
EMERGENCY_LOW_UPLOAD_VIDEO_BITRATE = env("FR_EMERGENCY_LOW_UPLOAD_VIDEO_BITRATE", "2500k")
EMERGENCY_LOW_UPLOAD_VIDEO_MAXRATE = env("FR_EMERGENCY_LOW_UPLOAD_VIDEO_MAXRATE", "2500k")
EMERGENCY_LOW_UPLOAD_VIDEO_BUFSIZE = env("FR_EMERGENCY_LOW_UPLOAD_VIDEO_BUFSIZE", "5000k")
EMERGENCY_LOW_UPLOAD_AUDIO_BITRATE = env("FR_EMERGENCY_LOW_UPLOAD_AUDIO_BITRATE", "")


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=False)


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def append_event(kind: str, message: str, extra: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {
        "ts_utc": iso_now(),
        "kind": kind,
        "message": message,
        "stream_service": STREAM_SERVICE,
    }
    if extra:
        payload.update(extra)

    try:
        EVENT_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with EVENT_LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")
    except Exception as e:
        log(f"WARN failed to append event log: {e}")


def write_restart_reason(
    *,
    reason_kind: str,
    reason: str,
    now_ts: int,
    ffmpeg_pid: int,
    ffmpeg_uptime_sec: int,
    metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = restart_context_writer.build_fast_recovery_restart_context(
        reason_kind=reason_kind,
        reason=reason,
        now_ts=now_ts,
        stream_service=STREAM_SERVICE,
        ffmpeg_pid=ffmpeg_pid,
        ffmpeg_uptime_sec=ffmpeg_uptime_sec,
        metrics=metrics,
        emergency_low_upload_enabled=EMERGENCY_LOW_UPLOAD_ENABLED,
        emergency_low_upload_triggers=EMERGENCY_LOW_UPLOAD_TRIGGERS,
        emergency_low_upload_duration_sec=EMERGENCY_LOW_UPLOAD_DURATION_SEC,
        emergency_low_upload_video_bitrate=EMERGENCY_LOW_UPLOAD_VIDEO_BITRATE,
        emergency_low_upload_video_maxrate=EMERGENCY_LOW_UPLOAD_VIDEO_MAXRATE,
        emergency_low_upload_video_bufsize=EMERGENCY_LOW_UPLOAD_VIDEO_BUFSIZE,
        emergency_low_upload_audio_bitrate=EMERGENCY_LOW_UPLOAD_AUDIO_BITRATE,
    )
    try:
        restart_context_writer.write_fast_recovery_restart_context(RESTART_REASON_FILE, payload)
    except Exception as e:
        log(f"WARN failed to write restart reason: {e}")
    return payload


def trim_samples(raw: Any) -> list[dict[str, Any]]:
    return recovery_state.trim_samples(raw, maxlen=SAMPLES_MAX)


def trim_restart_events(raw: Any, now_ts: int) -> list[dict[str, int | str]]:
    return recovery_state.trim_restart_events(
        raw,
        now_ts=now_ts,
        restart_downtime_cost_sec=RESTART_DOWNTIME_COST_SEC,
    )


def load_state(now_ts: int) -> dict[str, Any]:
    default = {
        "last_pid": 0,
        "last_bytes_sent": 0,
        "net_fail_streak": 0,
        "stall_streak": 0,
        "remote_warning_streak": 0,
        "remote_warning_last_stats_ts": 0,
        "remote_warning_last_sample_key": "",
        "remote_warning_last_probe_ts": 0,
        "remote_warning_context_key": "",
        "remote_warning_recovery_episode_id": "",
        "remote_warning_ffmpeg_generation": "",
        "last_restart_ts": 0,
        "last_restart_failure_ts": 0,
        "restart_failure_count": 0,
        "last_reason": "",
        "last_budget_block_key": "",
        "last_budget_block_ts": 0,
        "last_tcp_send_sample_ts": 0,
        "last_tcp_send_sample_pid": 0,
        "last_tcp_send_sample_bytes_sent": 0,
        "restart_events": [],
        "samples": [],
    }
    return recovery_state.load_state_file(
        STATE_FILE,
        now_ts=now_ts,
        default=default,
        samples_max=SAMPLES_MAX,
        restart_downtime_cost_sec=RESTART_DOWNTIME_COST_SEC,
    )


def save_state(state: dict[str, Any]) -> None:
    recovery_state.save_state_file(STATE_FILE, state)


def get_main_pid(unit: str) -> int:
    cp = run_systemctl(["show", unit, "--property=MainPID", "--value"], require_privilege=False, check=False)
    if cp.returncode != 0:
        return 0
    raw = (cp.stdout or "").strip()
    try:
        pid = int(raw)
    except ValueError:
        return 0
    return pid if pid > 1 else 0


def get_child_ffmpeg_pid(main_pid: int) -> int:
    if main_pid <= 1:
        return 0
    cp = run(["pgrep", "-P", str(main_pid), "ffmpeg"])
    if cp.returncode != 0:
        return 0
    for line in (cp.stdout or "").splitlines():
        try:
            pid = int(line.strip())
        except ValueError:
            continue
        if pid > 1:
            return pid
    return 0


def get_k8s_stream_ffmpeg_pid() -> int:
    cp = run(["pgrep", "-a", "ffmpeg"])
    if cp.returncode != 0:
        return 0
    for line in (cp.stdout or "").splitlines():
        if " x11grab " not in line and "rtmp://" not in line and "rtmps://" not in line:
            continue
        try:
            pid = int(line.strip().split(maxsplit=1)[0])
        except (ValueError, IndexError):
            continue
        if pid > 1:
            return pid
    return 0


def get_stream_ffmpeg_pid(main_pid: int) -> int:
    pid = get_child_ffmpeg_pid(main_pid)
    if pid > 1:
        return pid
    supervisor_mode = env("STREAM_RUNTIME_SUPERVISOR", "systemd").strip().lower()
    if supervisor_mode in {"k8s", "k3s", "kubernetes"}:
        return get_k8s_stream_ffmpeg_pid()
    return 0


def get_process_elapsed_sec(pid: int) -> int:
    if pid <= 1:
        return 0
    cp = run(["ps", "-o", "etimes=", "-p", str(pid)])
    if cp.returncode != 0:
        return 0
    raw = (cp.stdout or "").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def get_default_gateway() -> str:
    return probes.get_default_gateway(run_cmd=run)


def ping_ok(target: str, timeout_sec: int = 1) -> bool:
    return probes.ping_ok(target, run_cmd=run, timeout_sec=timeout_sec)


def dns_ok(host: str) -> bool:
    return probes.dns_ok(host, run_cmd=run)


def tcp_probe_ok(host: str, ports: list[int], timeout_sec: float = 1.0) -> bool:
    return probes.tcp_probe_ok(host, ports, timeout_sec=timeout_sec)


def parse_ffmpeg_tcp_metrics(ffmpeg_pid: int, ports: list[int]) -> dict[str, int | str]:
    return tcp_metrics.parse_ffmpeg_tcp_metrics(ffmpeg_pid=ffmpeg_pid, ports=ports, run_cmd=run)


def read_youtube_live_warning(now_ts: int, last_restart_ts: int) -> tuple[bool, str, dict[str, Any]]:
    return remote_warning_core.read_youtube_live_warning(
        stats_path=YTW_STATS_FILE,
        quota_state_path=QUOTA_STATE_FILE,
        now_ts=now_ts,
        last_restart_ts=last_restart_ts,
        url_preservation_mode=URL_PRESERVATION_MODE,
        status_max_age_sec=YTW_STATUS_MAX_AGE_SEC,
        require_local_ok=REMOTE_WARNING_REQUIRE_LOCAL_OK,
        live_like_lifecycle=LIVE_LIKE_LIFECYCLE,
        parse_iso_ts=parse_iso_ts,
    )


def remote_probe_epoch(payload: dict[str, Any]) -> int:
    return remote_warning_core.remote_probe_epoch(payload, parse_iso_ts)


def remote_warning_sample_key(payload: dict[str, Any]) -> str:
    return remote_warning_core.remote_warning_sample_key(payload, parse_iso_ts)


def remote_warning_context(payload: dict[str, Any]) -> tuple[str, str, str]:
    return remote_warning_core.remote_warning_context(payload)


def update_remote_warning_streak(state: dict[str, Any], remote_warning: bool, ytw_payload: dict[str, Any]) -> int:
    return remote_warning_core.update_remote_warning_streak(
        state,
        remote_warning,
        ytw_payload,
        confirm_distinct_stats=REMOTE_WARNING_CONFIRM_DISTINCT_STATS,
        parse_iso_ts=parse_iso_ts,
    )


def restart_stream(reason: str) -> tuple[bool, str]:
    return recovery_executor.restart_stream(
        stream_service=STREAM_SERVICE,
        reason=reason,
        run_systemctl=run_systemctl,
        log=log,
        supervisor=build_runtime_supervisor(
            run_systemctl=lambda args, check: run_systemctl(args, require_privilege=True, check=check),
        ),
    )


def used_downtime_budget_sec(events: list[dict[str, int | str]], now_ts: int, window_sec: int) -> int:
    return budget_policy.used_downtime_budget_sec(
        events,
        now_ts=now_ts,
        window_sec=window_sec,
        default_downtime_cost_sec=RESTART_DOWNTIME_COST_SEC,
    )


def maybe_record_budget_block(state: dict[str, Any], block_key: str, reason: str, extra: dict[str, Any]) -> None:
    now_ts = int(time.time())
    last_key = str(state.get("last_budget_block_key", ""))
    last_ts = int(state.get("last_budget_block_ts", 0) or 0)
    if block_key != last_key or now_ts - last_ts >= 60:
        append_event("restart_budget_block", reason, extra)
        state["last_budget_block_key"] = block_key
        state["last_budget_block_ts"] = now_ts


def emergency_budget_override_active(reason_kind: str, reason_first_ts: int, now_ts: int) -> bool:
    return budget_policy.emergency_budget_override_active(
        reason_kind=reason_kind,
        reason_first_ts=reason_first_ts,
        now_ts=now_ts,
        override_sec=BUDGET_EMERGENCY_OVERRIDE_SEC,
    )


def maybe_record_budget_override(state: dict[str, Any], block_key: str, reason: str, extra: dict[str, Any]) -> None:
    now_ts = int(time.time())
    last_key = str(state.get("last_budget_override_key", ""))
    last_ts = int(state.get("last_budget_override_ts", 0) or 0)
    if block_key != last_key or now_ts - last_ts >= 60:
        append_event("restart_budget_override", reason, extra)
        state["last_budget_override_key"] = block_key
        state["last_budget_override_ts"] = now_ts


def maybe_append_tcp_send_sample(
    state: dict[str, Any],
    *,
    now_ts: int,
    ffmpeg_pid: int,
    bytes_sent: int,
    metrics: dict[str, int | str],
) -> None:
    if TCP_SEND_SAMPLE_LOG_SEC <= 0 or ffmpeg_pid <= 1 or not metrics:
        return

    last_pid = int(state.get("last_tcp_send_sample_pid", 0) or 0)
    last_ts = int(state.get("last_tcp_send_sample_ts", 0) or 0)
    last_bytes_sent = int(state.get("last_tcp_send_sample_bytes_sent", 0) or 0)
    if last_pid != ffmpeg_pid or last_ts <= 0 or last_bytes_sent <= 0 or bytes_sent < last_bytes_sent:
        state["last_tcp_send_sample_ts"] = now_ts
        state["last_tcp_send_sample_pid"] = ffmpeg_pid
        state["last_tcp_send_sample_bytes_sent"] = bytes_sent
        return

    elapsed_sec = max(0, now_ts - last_ts)
    if elapsed_sec < TCP_SEND_SAMPLE_LOG_SEC:
        return

    bytes_delta = max(0, bytes_sent - last_bytes_sent)
    mbps = round((bytes_delta * 8) / (elapsed_sec * 1_000_000), 3) if elapsed_sec > 0 else 0.0
    append_event(
        "tcp_send_sample",
        "ffmpeg tcp send sample",
        {
            "ffmpeg_pid": ffmpeg_pid,
            "sample_interval_sec": elapsed_sec,
            "bytes_sent_delta": bytes_delta,
            "bytes_sent": bytes_sent,
            "mbps": mbps,
            "notsent": int(metrics.get("notsent", 0) or 0),
            "unacked": int(metrics.get("unacked", 0) or 0),
            "lastsnd_ms": int(metrics.get("lastsnd_ms", 0) or 0),
            "conn": str(metrics.get("conn", "") or ""),
        },
    )
    state["last_tcp_send_sample_ts"] = now_ts
    state["last_tcp_send_sample_pid"] = ffmpeg_pid
    state["last_tcp_send_sample_bytes_sent"] = bytes_sent


def restart_failure_backoff_left(now_ts: int, last_restart_failure_ts: int, backoff_sec: int) -> int:
    return budget_policy.restart_failure_backoff_left(
        now_ts=now_ts,
        last_restart_failure_ts=last_restart_failure_ts,
        backoff_sec=backoff_sec,
    )


def main() -> int:
    now_ts = int(time.time())
    state = load_state(now_ts)
    last_restart_ts = int(state.get("last_restart_ts", 0) or 0)

    main_pid = get_main_pid(STREAM_SERVICE)
    ffmpeg_pid = get_stream_ffmpeg_pid(main_pid)
    ffmpeg_uptime_sec = get_process_elapsed_sec(ffmpeg_pid)

    if ffmpeg_pid <= 1:
        missing_first_ts = int(state.get("ffmpeg_missing_first_ts", 0) or 0)
        if missing_first_ts <= 0:
            missing_first_ts = now_ts
        missing_for = now_ts - missing_first_ts
        state["ffmpeg_missing_first_ts"] = missing_first_ts
        reason = f"ffmpeg pid not found for {missing_for}s"
        should_restart_missing = missing_for >= FFMPEG_MISSING_RESTART_SEC
        guard_active = now_ts - last_restart_ts < RESTART_GUARD_SEC
        backoff_left = restart_failure_backoff_left(
            now_ts,
            int(state.get("last_restart_failure_ts", 0) or 0),
            RESTART_FAILURE_BACKOFF_SEC,
        )
        success_backoff_until = int(state.get("ffmpeg_missing_success_backoff_until", 0) or 0)
        success_backoff_left = max(0, success_backoff_until - now_ts)
        if should_restart_missing and not guard_active and backoff_left <= 0 and success_backoff_left <= 0:
            restart_events = trim_restart_events(state.get("restart_events", []), now_ts)
            state["restart_events"] = restart_events
            used_hour = used_downtime_budget_sec(restart_events, now_ts, 3600)
            used_day = used_downtime_budget_sec(restart_events, now_ts, 86400)
            emergency_override = emergency_budget_override_active("ffmpeg_missing", missing_first_ts, now_ts)
            for budget_name, used, budget, window in (
                ("hourly", used_hour, HOURLY_DOWNTIME_BUDGET_SEC, 3600),
                ("daily", used_day, DAILY_DOWNTIME_BUDGET_SEC, 86400),
            ):
                if budget > 0 and used + RESTART_DOWNTIME_COST_SEC > budget:
                    block = (
                        f"{budget_name} downtime budget exceeded "
                        f"({used}+{RESTART_DOWNTIME_COST_SEC}>{budget}s)"
                    )
                    if emergency_override:
                        maybe_record_budget_override(
                            state,
                            block_key=f"{budget_name}:{used}:ffmpeg_missing",
                            reason=f"{block}; emergency override after {now_ts - missing_first_ts}s",
                            extra={
                                "trigger": "ffmpeg_missing",
                                "reason": reason,
                                "reason_first_ts": missing_first_ts,
                                "window_sec": window,
                                "override_after_sec": BUDGET_EMERGENCY_OVERRIDE_SEC,
                            },
                        )
                        continue
                    maybe_record_budget_block(
                        state,
                        block_key=f"{budget_name}:{used}:ffmpeg_missing",
                        reason=block,
                        extra={"trigger": "ffmpeg_missing", "reason": reason, "window_sec": window},
                    )
                    state["last_reason"] = block
                    state["last_pid"] = 0
                    state["last_bytes_sent"] = 0
                    state["last_tcp_send_sample_pid"] = 0
                    state["last_tcp_send_sample_bytes_sent"] = 0
                    save_state(state)
                    return 0
            restart_context = write_restart_reason(
                reason_kind="ffmpeg_missing",
                reason=reason,
                now_ts=now_ts,
                ffmpeg_pid=0,
                ffmpeg_uptime_sec=0,
                metrics=None,
            )
            restart_ok, restart_detail = restart_stream(reason)
            append_event(
                "restart" if restart_ok else "restart_failed",
                reason,
                {
                    "trigger": "ffmpeg_missing",
                    "missing_for_sec": missing_for,
                    "detail": restart_detail,
                    "restart_context": restart_context,
                },
            )
            if restart_ok:
                restart_events = trim_restart_events(
                    [
                        *restart_events,
                        {
                            "ts": now_ts,
                            "downtime_sec": RESTART_DOWNTIME_COST_SEC,
                            "reason": "ffmpeg_missing",
                        },
                    ],
                    now_ts,
                )
                state.update(
                    {
                        "restart_events": restart_events,
                        "last_restart_ts": now_ts,
                        "last_reason": "restarted: ffmpeg pid missing",
                        "ffmpeg_missing_first_ts": 0,
                        "ffmpeg_missing_success_backoff_until": now_ts + FFMPEG_MISSING_SUCCESS_BACKOFF_SEC,
                        "restart_failure_count": 0,
                    }
                )
            else:
                state.update(
                    {
                        "last_restart_failure_ts": now_ts,
                        "restart_failure_count": int(state.get("restart_failure_count", 0) or 0) + 1,
                        "last_reason": f"restart failed: {restart_detail}",
                    }
                )
            save_state(state)
            return 0
        state.update(
            {
                "last_pid": 0,
                "last_bytes_sent": 0,
                "last_bytes_sent_ts": 0,
                "net_fail_streak": 0,
                "stall_streak": 0,
                "low_upload_pressure_streak": 0,
                "remote_warning_streak": 0,
                "remote_warning_last_sample_key": "",
                "remote_warning_last_probe_ts": 0,
                "remote_warning_context_key": "",
                "remote_warning_recovery_episode_id": "",
                "remote_warning_ffmpeg_generation": "",
                "last_reason": reason
                if not guard_active and backoff_left <= 0 and success_backoff_left <= 0
                else f"{reason}; restart guard/backoff active",
                "last_tcp_send_sample_pid": 0,
                "last_tcp_send_sample_bytes_sent": 0,
            }
        )
        if success_backoff_left > 0:
            state["last_reason"] = f"{reason}; success backoff active ({success_backoff_left}s remaining)"
        save_state(state)
        return 0
    state["ffmpeg_missing_first_ts"] = 0

    if ffmpeg_uptime_sec < MIN_FFMPEG_UPTIME_SEC:
        state.update(
            {
                "last_pid": ffmpeg_pid,
                "last_bytes_sent": 0,
                "last_bytes_sent_ts": now_ts,
                "net_fail_streak": 0,
                "stall_streak": 0,
                "low_upload_pressure_streak": 0,
                "remote_warning_streak": 0,
                "remote_warning_last_sample_key": "",
                "remote_warning_last_probe_ts": 0,
                "remote_warning_context_key": "",
                "remote_warning_recovery_episode_id": "",
                "remote_warning_ffmpeg_generation": "",
                "last_reason": f"ffmpeg warmup ({ffmpeg_uptime_sec}s<{MIN_FFMPEG_UPTIME_SEC}s)",
            }
        )
        save_state(state)
        return 0

    metrics = parse_ffmpeg_tcp_metrics(ffmpeg_pid, RTMP_PORTS)
    recovery_decision.reset_pid_dependent_state(state, ffmpeg_pid)

    gw = get_default_gateway()
    gw_ok = ping_ok(gw) if gw else False
    public_ok_count = sum(1 for target in PUBLIC_PING_TARGETS if ping_ok(target))
    dns_probe_ok = dns_ok(DNS_HOST)
    tcp_probe = tcp_probe_ok(RTMP_HOST, RTMP_PORTS)
    network = recovery_decision.network_observation(
        gateway=gw,
        gateway_ok=gw_ok,
        public_ok_count=public_ok_count,
        dns_ok=dns_probe_ok,
        tcp_probe_ok=tcp_probe,
    )
    state["net_fail_streak"] = recovery_decision.update_streak(
        state,
        "net_fail_streak",
        network.network_down,
    )
    tcp = recovery_decision.tcp_observation(
        state,
        now_ts=now_ts,
        metrics=metrics,
        send_mbps_func=tcp_metrics.send_mbps,
        low_upload_pressure_func=tcp_metrics.low_upload_pressure_now,
        stall_lastsnd_ms=STALL_LASTSND_MS,
        stall_notsent_bytes=STALL_NOTSENT_BYTES,
        stall_unacked=STALL_UNACKED,
        low_upload_enabled=LOW_UPLOAD_PRESSURE_ENABLED,
        low_upload_max_mbps=LOW_UPLOAD_PRESSURE_MAX_MBPS,
        low_upload_notsent_bytes=LOW_UPLOAD_PRESSURE_NOTSENT_BYTES,
        low_upload_unacked=LOW_UPLOAD_PRESSURE_UNACKED,
        low_upload_lastsnd_ms=LOW_UPLOAD_PRESSURE_LASTSND_MS,
        network_down=network.network_down,
        tcp_probe=network.tcp_probe_ok,
    )
    maybe_append_tcp_send_sample(
        state,
        now_ts=now_ts,
        ffmpeg_pid=ffmpeg_pid,
        bytes_sent=tcp.bytes_sent,
        metrics=metrics,
    )
    state["stall_streak"] = recovery_decision.update_streak(
        state,
        "stall_streak",
        tcp.stall_now,
    )
    state["low_upload_pressure_streak"] = recovery_decision.update_streak(
        state,
        "low_upload_pressure_streak",
        tcp.low_upload_pressure_now,
    )

    remote_warning, remote_warning_reason, ytw_payload = read_youtube_live_warning(now_ts, last_restart_ts)
    remote_warning_streak = update_remote_warning_streak(state, remote_warning, ytw_payload)

    samples = deque(trim_samples(state.get("samples", [])), maxlen=SAMPLES_MAX)
    samples.append(
        recovery_decision.sample_row(
            now_ts=now_ts,
            ffmpeg_pid=ffmpeg_pid,
            tcp=tcp,
            network_down=network.network_down,
            remote_warning=remote_warning,
        )
    )
    state["samples"] = list(samples)

    reason_kind, reason = recovery_decision.select_restart_reason(
        state,
        url_preservation_mode=URL_PRESERVATION_MODE,
        remote_warning_streak=remote_warning_streak,
        remote_warning_confirm=REMOTE_WARNING_CONFIRM,
        remote_warning_reason=remote_warning_reason,
        network=network,
        net_fail_confirm=NET_FAIL_CONFIRM,
        stall_confirm=STALL_CONFIRM,
        low_upload_confirm=LOW_UPLOAD_PRESSURE_CONFIRM,
        low_upload_max_mbps=LOW_UPLOAD_PRESSURE_MAX_MBPS,
        tcp=tcp,
    )
    restart_reason = recovery_decision.update_active_reason(
        state,
        now_ts=now_ts,
        reason_kind=reason_kind,
        reason=reason,
    )

    if reason and now_ts - last_restart_ts < RESTART_GUARD_SEC:
        recovery_decision.mark_latest_transport_sample(
            state,
            ffmpeg_pid=ffmpeg_pid,
            bytes_sent=tcp.bytes_sent,
            now_ts=now_ts,
            last_reason=f"restart guard active ({now_ts - last_restart_ts}s<{RESTART_GUARD_SEC}s)",
        )
        save_state(state)
        return 0

    last_restart_failure_ts = int(state.get("last_restart_failure_ts", 0) or 0)
    restart_failure_count = int(state.get("restart_failure_count", 0) or 0)
    failure_backoff_left = restart_failure_backoff_left(
        now_ts,
        last_restart_failure_ts,
        RESTART_FAILURE_BACKOFF_SEC,
    )
    if reason and failure_backoff_left > 0:
        recovery_decision.mark_latest_transport_sample(
            state,
            ffmpeg_pid=ffmpeg_pid,
            bytes_sent=tcp.bytes_sent,
            now_ts=now_ts,
            last_reason=(
                f"restart failure backoff active ({failure_backoff_left}s<{RESTART_FAILURE_BACKOFF_SEC}s)"
            ),
        )
        save_state(state)
        return 0

    restart_events = trim_restart_events(state.get("restart_events", []), now_ts)
    state["restart_events"] = restart_events
    if reason:
        used_hour = used_downtime_budget_sec(restart_events, now_ts, 3600)
        used_day = used_downtime_budget_sec(restart_events, now_ts, 86400)
        emergency_override = emergency_budget_override_active(reason_kind, restart_reason.first_ts, now_ts)

        if HOURLY_DOWNTIME_BUDGET_SEC > 0 and used_hour + RESTART_DOWNTIME_COST_SEC > HOURLY_DOWNTIME_BUDGET_SEC:
            block = (
                f"hourly downtime budget exceeded "
                f"({used_hour}+{RESTART_DOWNTIME_COST_SEC}>{HOURLY_DOWNTIME_BUDGET_SEC}s)"
            )
            if emergency_override:
                maybe_record_budget_override(
                    state,
                    block_key=f"hourly:{used_hour}:{reason_kind}",
                    reason=f"{block}; emergency override after {now_ts - restart_reason.first_ts}s",
                    extra={
                        "trigger": reason_kind,
                        "reason": reason,
                        "reason_first_ts": restart_reason.first_ts,
                        "override_after_sec": BUDGET_EMERGENCY_OVERRIDE_SEC,
                    },
                )
            else:
                maybe_record_budget_block(
                    state,
                    block_key=f"hourly:{used_hour}:{reason_kind}",
                    reason=block,
                    extra={"trigger": reason_kind, "reason": reason},
                )
                state["last_reason"] = block
                recovery_decision.mark_latest_transport_sample(
                    state,
                    ffmpeg_pid=ffmpeg_pid,
                    bytes_sent=tcp.bytes_sent,
                    now_ts=now_ts,
                )
                save_state(state)
                return 0

        if DAILY_DOWNTIME_BUDGET_SEC > 0 and used_day + RESTART_DOWNTIME_COST_SEC > DAILY_DOWNTIME_BUDGET_SEC:
            block = (
                f"daily downtime budget exceeded "
                f"({used_day}+{RESTART_DOWNTIME_COST_SEC}>{DAILY_DOWNTIME_BUDGET_SEC}s)"
            )
            if emergency_override:
                maybe_record_budget_override(
                    state,
                    block_key=f"daily:{used_day}:{reason_kind}",
                    reason=f"{block}; emergency override after {now_ts - restart_reason.first_ts}s",
                    extra={
                        "trigger": reason_kind,
                        "reason": reason,
                        "reason_first_ts": restart_reason.first_ts,
                        "override_after_sec": BUDGET_EMERGENCY_OVERRIDE_SEC,
                    },
                )
            else:
                maybe_record_budget_block(
                    state,
                    block_key=f"daily:{used_day}:{reason_kind}",
                    reason=block,
                    extra={"trigger": reason_kind, "reason": reason},
                )
                state["last_reason"] = block
                recovery_decision.mark_latest_transport_sample(
                    state,
                    ffmpeg_pid=ffmpeg_pid,
                    bytes_sent=tcp.bytes_sent,
                    now_ts=now_ts,
                )
                save_state(state)
                return 0

        restart_metrics = recovery_decision.restart_metrics(
            tcp=tcp,
            network_down=network.network_down,
            remote_warning=remote_warning,
        )
        restart_context = write_restart_reason(
            reason_kind=reason_kind,
            reason=reason,
            now_ts=now_ts,
            ffmpeg_pid=ffmpeg_pid,
            ffmpeg_uptime_sec=ffmpeg_uptime_sec,
            metrics=restart_metrics,
        )
        restart_ok, restart_detail = restart_stream(reason)
        if restart_ok:
            restart_events = trim_restart_events(
                [
                    *restart_events,
                    {
                        "ts": now_ts,
                        "downtime_sec": RESTART_DOWNTIME_COST_SEC,
                        "reason": reason_kind or "unknown",
                    },
                ],
                now_ts,
            )
            state["restart_events"] = restart_events
            state["last_restart_ts"] = now_ts
            state["last_restart_failure_ts"] = 0
            state["restart_failure_count"] = 0
            recovery_decision.clear_recovery_streaks(state)
            state["last_bytes_sent"] = 0
            state["last_bytes_sent_ts"] = 0
            state["last_tcp_send_sample_pid"] = 0
            state["last_tcp_send_sample_bytes_sent"] = 0
            state["last_reason"] = reason
            state["last_budget_block_key"] = ""
            state["last_budget_block_ts"] = 0
            append_event(
                "restart",
                reason,
                {
                    "trigger": reason_kind,
                    "ffmpeg_pid": ffmpeg_pid,
                    "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
                    "metrics": restart_metrics,
                    "restart_context": restart_context,
                    "youtube_hint": recovery_decision.youtube_hint(ytw_payload),
                },
            )
            save_state(state)
            return 0
        restart_failure_count += 1
        state["last_restart_failure_ts"] = now_ts
        state["restart_failure_count"] = restart_failure_count
        state["last_reason"] = f"restart failed ({restart_failure_count}): {reason}"
        recovery_decision.mark_latest_transport_sample(
            state,
            ffmpeg_pid=ffmpeg_pid,
            bytes_sent=tcp.bytes_sent,
            now_ts=now_ts,
        )
        append_event(
            "restart_failed",
            reason,
            {
                "trigger": reason_kind,
                "ffmpeg_pid": ffmpeg_pid,
                "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
                "restart_failure_count": restart_failure_count,
                "backoff_sec": RESTART_FAILURE_BACKOFF_SEC,
                "detail": restart_detail,
            },
        )
        save_state(state)
        return 0

    recovery_decision.mark_latest_transport_sample(
        state,
        ffmpeg_pid=ffmpeg_pid,
        bytes_sent=tcp.bytes_sent,
        now_ts=now_ts,
        last_reason=reason or "healthy",
    )
    save_state(state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
