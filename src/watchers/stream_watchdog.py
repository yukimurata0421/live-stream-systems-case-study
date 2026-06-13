#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import time
import uuid
import calendar
from pathlib import Path

try:
    import pwd
except ModuleNotFoundError:  # pragma: no cover - exercised on Windows
    pwd = None  # type: ignore[assignment]

try:
    from .systemctl_control import run_systemctl
    from .local_health import actions as local_actions
    from .local_health import audio_signals, delivery_signals, recovery_stage, rendering_signals
    from .stream_watchdog_core import audio_transition, overlay_health, pulse_metrics, pulse_routes
    from stream_core.supervisor.factory import build_runtime_supervisor
except ImportError:
    from systemctl_control import run_systemctl
    from local_health import actions as local_actions
    from local_health import audio_signals, delivery_signals, recovery_stage, rendering_signals
    from stream_watchdog_core import audio_transition, overlay_health, pulse_metrics, pulse_routes
    from stream_core.supervisor.factory import build_runtime_supervisor


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def env_int(name: str, default: int) -> int:
    try:
        return int(env(name, str(default)))
    except ValueError:
        return default


def run(cmd: list[str], check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=check)


def parse_url_port(url: str, default: int) -> int:
    return rendering_signals.parse_url_port(url, default)


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


SCRIPT_PATH = Path(__file__).resolve()
BASE_DIR = SCRIPT_PATH.parents[2]
STATE_ROOT = Path(
    env(
        "STREAM_RUNTIME_STATE_DIR",
        str(BASE_DIR / ".state" / "adsb-streamnew-v2"),
    )
).expanduser()
STREAM_SERVICE = env("STREAM_SERVICE", "adsb-streamnew-youtube-stream.service")
DJ_SERVICE = env("DJ_SERVICE", "adsb-streamnew-auto-dj.service")
STREAM_WATCHDOG_REMOTE_ONLY = env("STREAM_WATCHDOG_REMOTE_ONLY", "0") == "1"
STREAM_K8S_NAMESPACE = env("STREAM_K8S_NAMESPACE", "stream-v3")
STREAM_KUBECTL_BIN = env("STREAM_KUBECTL_BIN", "kubectl")
STREAM_V3_RUNTIME_WORKLOAD = env("STREAM_V3_RUNTIME_WORKLOAD", "deployment/stream-v3-runtime")
STREAM_V3_RUNTIME_CONTAINER = env("STREAM_V3_RUNTIME_CONTAINER", "stream-engine")
OVERLAY_URL = env("OVERLAY_URL", "http://127.0.0.1:18080")
OVERLAY_REQUIRE_MAP_PROXY = env("OVERLAY_REQUIRE_MAP_PROXY", "1") == "1"
OVERLAY_REQUIRE_ADSB_JSON = env("OVERLAY_REQUIRE_ADSB_JSON", "1") == "1"
OVERLAY_REQUIRE_OUTLINE_JSON = env("OVERLAY_REQUIRE_OUTLINE_JSON", "1") == "1"
OVERLAY_RECOVER_BEFORE_STREAM_RESTART = env("OVERLAY_RECOVER_BEFORE_STREAM_RESTART", "1") == "1"
OVERLAY_RECOVERY_WAIT_SEC = max(1, env_int("OVERLAY_RECOVERY_WAIT_SEC", 2))
OVERLAY_PYTHON = env("OVERLAY_PYTHON", str(BASE_DIR / "venv" / "bin" / "python3"))
OVERLAY_PROCESS_PATTERN = env(
    "OVERLAY_PROCESS_PATTERN",
    str(BASE_DIR / "src" / "stream_core" / "overlay_server.py"),
)
OVERLAY_BIND_HOST = env("OVERLAY_BIND_HOST", "0.0.0.0")
OVERLAY_PORT = max(1, env_int("OVERLAY_PORT", parse_url_port(OVERLAY_URL, 18080)))
OVERLAY_DIR = env("OVERLAY_DIR", str(BASE_DIR / "ui" / "overlay"))
STREAM1090_URL = env("STREAM1090_URL", "http://stream1090.lan/stream1090/")
NOW_PLAYING_FILE = env("NOW_PLAYING_FILE", str(BASE_DIR / "now_playing.txt"))
OVERLAY_ACTUAL_RANGE_SUPPLEMENT_FILE = env(
    "OVERLAY_ACTUAL_RANGE_SUPPLEMENT_FILE",
    "/dev/shm/adsb-streamnew/overlay_actual_range_supplement.json",
)
OVERLAY_ACTUAL_RANGE_SUPPLEMENT_HOURS = env("OVERLAY_ACTUAL_RANGE_SUPPLEMENT_HOURS", "24")
ADSB_JSON_MAX_AGE_SEC = max(0, env_int("ADSB_JSON_MAX_AGE_SEC", 30))
ADSB_MESSAGE_STALL_SEC = max(0, env_int("ADSB_MESSAGE_STALL_SEC", 120))
DISPLAY_NAME = env("DISPLAY_NAME", env("DISPLAY", ":99"))
VIDEO_SIZE = env("VIDEO_SIZE", "1920x1080")
ENABLE_VIDEO_FRAME_PROBE = env("ENABLE_VIDEO_FRAME_PROBE", "0") == "1"
VIDEO_FRAME_MIN_LUMA = max(0, env_int("VIDEO_FRAME_MIN_LUMA", 4))
PULSE_SOURCE = env("PULSE_SOURCE", "stream_sink.monitor")
PULSE_USER = env("PULSE_USER", "yuki")
PULSE_HOME = env("PULSE_HOME", "/home/yuki")
PULSE_RUNTIME_DIR = env("PULSE_RUNTIME_DIR", "/run/user/1000")
ENSURE_PULSE_SCRIPT = env("ENSURE_PULSE_SCRIPT", str(BASE_DIR / "ops" / "scripts" / "ensure-pulse.sh"))
WORK_DIR = Path(env("WATCHDOG_WORK_DIR", str(STATE_ROOT / "watchdog"))).expanduser()
AUDIO_FAIL_THRESHOLD = max(1, env_int("AUDIO_FAIL_THRESHOLD", 2))
AUDIO_DJ_RESTART_FAILS = max(1, env_int("AUDIO_DJ_RESTART_FAILS", 2))
AUDIO_STREAM_RESTART_FAILS = max(AUDIO_DJ_RESTART_FAILS + 1, env_int("AUDIO_STREAM_RESTART_FAILS", 3))
AUDIO_TRACK_TRANSITION_GRACE_SEC = max(0, env_int("AUDIO_TRACK_TRANSITION_GRACE_SEC", 30))
AUDIO_BUCKET_BOUNDARY_GRACE_SEC = max(0, env_int("AUDIO_BUCKET_BOUNDARY_GRACE_SEC", 90))
AUDIO_STARTUP_GRACE_SEC = max(10, env_int("AUDIO_STARTUP_GRACE_SEC", 45))
ENABLE_AUDIO_PROBE = env("ENABLE_AUDIO_PROBE", "0") == "1"
ENABLE_PULSE_PRECISION_PROBE = env("ENABLE_PULSE_PRECISION_PROBE", "1") == "1"
PULSE_RECOVER_ONLY_DJ_FIRST = env("PULSE_RECOVER_ONLY_DJ_FIRST", "1") == "1"
PULSE_RECOVERY_WAIT_SEC = max(1, env_int("PULSE_RECOVERY_WAIT_SEC", 2))
PULSE_WARN_RECENT_SEC = max(60, env_int("PULSE_WARN_RECENT_SEC", 300))
PULSE_ROUTE_FAIL_THRESHOLD = max(1, env_int("PULSE_ROUTE_FAIL_THRESHOLD", 2))
PULSE_DJ_BUFFER_LATENCY_WARN_USEC = max(0, env_int("PULSE_DJ_BUFFER_LATENCY_WARN_USEC", 350_000))
PULSE_DJ_BUFFER_LATENCY_CRIT_USEC = max(
    PULSE_DJ_BUFFER_LATENCY_WARN_USEC,
    env_int("PULSE_DJ_BUFFER_LATENCY_CRIT_USEC", 700_000),
)
PULSE_CAPTURE_BUFFER_LATENCY_WARN_USEC = max(0, env_int("PULSE_CAPTURE_BUFFER_LATENCY_WARN_USEC", 120_000))
PULSE_CAPTURE_BUFFER_LATENCY_CRIT_USEC = max(
    PULSE_CAPTURE_BUFFER_LATENCY_WARN_USEC,
    env_int("PULSE_CAPTURE_BUFFER_LATENCY_CRIT_USEC", 300_000),
)
RESTART_WINDOW_SEC = max(60, env_int("RESTART_WINDOW_SEC", 600))
RESTART_MAX_ATTEMPTS = max(1, env_int("RESTART_MAX_ATTEMPTS", 3))
RESTART_COOLDOWN_SEC = max(30, env_int("RESTART_COOLDOWN_SEC", 300))
SLO_PULSE_UNAVAILABLE_24H_MAX = max(1, env_int("SLO_PULSE_UNAVAILABLE_24H_MAX", 1))
SLO_FILE = Path(env("SLO_FILE", str(STATE_ROOT / "slo_snapshot.json"))).expanduser()
YTW_STATE_FILE = Path(env("YTW_STATE_FILE", str(STATE_ROOT / "youtube_watchdog_state.json"))).expanduser()
NOW_PLAYING_JSON = Path(env("NOW_PLAYING_JSON", str(BASE_DIR / "ui" / "overlay" / "now_playing.json")))
PATTERN_STATE_JSON = Path(env("PATTERN_STATE_JSON", str(BASE_DIR / "ui" / "overlay" / "pattern_state.json")))
RUNTIME_STATE_GLOB = env(
    "RUNTIME_STATE_GLOB",
    str(BASE_DIR / "state" / "runtime" / "stream_runtime_state_*.json"),
)
RUNTIME_SNAPSHOT_STALE_SEC = max(0, env_int("RUNTIME_SNAPSHOT_STALE_SEC", 0))
RECOVERY_STAGE_MAX = max(1, env_int("RECOVERY_STAGE_MAX", 3))
PULSE_STAGE_WINDOW_SEC = max(60, env_int("PULSE_STAGE_WINDOW_SEC", 600))
AUDIO_STAGE_WINDOW_SEC = max(60, env_int("AUDIO_STAGE_WINDOW_SEC", 600))
STREAM_KEY = env("STREAM_KEY", "")
RTMP_URL = env("RTMP_URL", "")
EVENT_LOG_FILE = Path(env("WATCHDOG_EVENT_LOG_FILE", str(STATE_ROOT / "logs" / "stream_watchdog_events.jsonl"))).expanduser()
RESTART_REASON_FILE = Path(env("RESTART_REASON_FILE", str(STATE_ROOT / "restart_reason.json"))).expanduser()
SNAPSHOT_TIMELINE_FILE = Path(
    env("WATCHDOG_SNAPSHOT_TIMELINE_FILE", str(STATE_ROOT / "logs" / "watchdog_state_timeline.jsonl"))
).expanduser()
WATCHDOG_OK_LOG_EVERY_SEC = max(0, env_int("WATCHDOG_OK_LOG_EVERY_SEC", 300))
WATCHDOG_STATS_FILE = Path(
    env("WATCHDOG_STATS_FILE", str(STATE_ROOT / "stream_watchdog_stats.json"))
).expanduser()
WATCHDOG_OK_HEARTBEAT_FILE = Path(
    env("WATCHDOG_OK_HEARTBEAT_FILE", str(WORK_DIR / "watchdog_ok_heartbeat_state.json"))
).expanduser()
ADSB_FRESHNESS_STATE_FILE = Path(
    env("ADSB_FRESHNESS_STATE_FILE", str(WORK_DIR / "adsb_freshness_state.json"))
).expanduser()

AUDIO_FAIL_COUNT_FILE = WORK_DIR / "audio_fail_count"
PULSE_SOURCE_MISSING_COUNT_FILE = WORK_DIR / "pulse_source_missing_count"
RESTART_STATE_FILE = WORK_DIR / "restart_events.log"
RESTART_COOLDOWN_FILE = WORK_DIR / "restart_cooldown_until"
RECOVERY_STAGE_FILE = WORK_DIR / "recovery_stage_state.json"
PULSE_HEALTH_STATE_FILE = WORK_DIR / "pulse_health_state.json"


def next_event_id() -> str:
    return f"evt-watchdog-{int(time.time())}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


def append_event(event_type: str, **fields: object) -> str:
    event_id = next_event_id()
    payload = {
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "event_id": event_id,
        "event_type": event_type,
        "stream_service": STREAM_SERVICE,
        "dj_service": DJ_SERVICE,
        **fields,
    }
    EVENT_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with EVENT_LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        fh.write("\n")
    return event_id


def append_snapshot_timeline(entry_type: str, **fields: object) -> str:
    event_id = next_event_id()
    payload = {
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "event_id": event_id,
        "entry_type": entry_type,
        **collect_diagnostic_snapshot(),
        **fields,
    }
    SNAPSHOT_TIMELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with SNAPSHOT_TIMELINE_FILE.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        fh.write("\n")
    return event_id


def write_restart_reason(component: str, reason: str, unit: str, event_id: str) -> None:
    RESTART_REASON_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "stream_watchdog",
        "event_id": event_id,
        "component": component,
        "reason": reason,
        "target_unit": unit,
    }
    RESTART_REASON_FILE.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def now_epoch() -> int:
    return int(time.time())


def service_substate(unit: str) -> str:
    cp = run_systemctl(["show", "-p", "SubState", "--value", unit], require_privilege=False, check=False)
    return (cp.stdout or "").strip()


def service_uptime_sec(unit: str) -> int:
    cp = run_systemctl(
        ["show", "-p", "ActiveEnterTimestampMonotonic", "--value", unit],
        require_privilege=False,
        check=False,
    )
    raw = (cp.stdout or "").strip()
    if not raw.isdigit():
        return 0
    active_usec = int(raw)
    if active_usec <= 0:
        return 0
    with open("/proc/uptime", "r", encoding="utf-8") as fh:
        uptime_sec = float(fh.read().split()[0])
    now_usec = int(uptime_sec * 1_000_000)
    if now_usec <= active_usec:
        return 0
    return (now_usec - active_usec) // 1_000_000


def is_service_stable(unit: str) -> bool:
    return service_substate(unit) == "running"


def read_int_file(path: Path, default: int = 0) -> int:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except Exception:
        return default


def write_int_file(path: Path, value: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{value}\n", encoding="utf-8")


def read_json_file(path: Path, default: dict) -> dict:
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        return data if isinstance(data, dict) else default.copy()
    except Exception:
        return default.copy()


def pick_runtime_state_path() -> Path | None:
    return delivery_signals.pick_runtime_state_path(RUNTIME_STATE_GLOB)


def maybe_read_json(path: Path) -> dict:
    return read_json_file(path, {})


def write_json_file(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")


def append_jsonl_if_changed(path: Path, payload: dict) -> None:
    line = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    last = ""
    try:
        with path.open("r", encoding="utf-8") as fh:
            for raw in fh:
                if raw.strip():
                    last = raw.strip()
    except FileNotFoundError:
        pass
    if last == line:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line)
        fh.write("\n")


def classify_watchdog_judgment(status: str) -> str:
    normalized = (status or "").strip().lower()
    if normalized == "ok":
        return "ok"
    if normalized == "warmup_grace":
        return "deferred"
    return "ng"


def record_watchdog_stats(status: str, reason: str = "", **fields: object) -> None:
    payload = {
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "status": status,
        "judgment": classify_watchdog_judgment(status),
        "reason": reason,
        "stream_service": STREAM_SERVICE,
        "dj_service": DJ_SERVICE,
        **fields,
    }
    write_json_file(WATCHDOG_STATS_FILE, payload)


def should_emit_watchdog_ok(now_ts: int | None = None) -> bool:
    if WATCHDOG_OK_LOG_EVERY_SEC <= 0:
        return True
    current = now_epoch() if now_ts is None else int(now_ts)
    state = read_json_file(WATCHDOG_OK_HEARTBEAT_FILE, {})
    try:
        last_ok_ts = int(state.get("last_ok_event_ts", 0))
    except Exception:
        last_ok_ts = 0
    if last_ok_ts > 0 and current - last_ok_ts < WATCHDOG_OK_LOG_EVERY_SEC:
        return False
    write_json_file(WATCHDOG_OK_HEARTBEAT_FILE, {"last_ok_event_ts": current})
    return True


def load_recovery_stage_state() -> dict[str, int]:
    return recovery_stage.load(RECOVERY_STAGE_FILE, read_json=read_json_file)


def save_recovery_stage_state(state: dict[str, int]) -> None:
    recovery_stage.save(RECOVERY_STAGE_FILE, state, write_json=write_json_file)


def bump_stage(state: dict[str, int], stage_key: str, ts_key: str, window_sec: int) -> int:
    return recovery_stage.bump(
        state,
        stage_key=stage_key,
        ts_key=ts_key,
        window_sec=window_sec,
        max_stage=RECOVERY_STAGE_MAX,
        now_epoch=now_epoch,
    )


def reset_stage(state: dict[str, int], stage_key: str, ts_key: str) -> None:
    recovery_stage.reset(state, stage_key=stage_key, ts_key=ts_key)


def prune_restart_events() -> None:
    cutoff = now_epoch() - RESTART_WINDOW_SEC
    lines: list[str] = []
    if RESTART_STATE_FILE.exists():
        for line in RESTART_STATE_FILE.read_text(encoding="utf-8").splitlines():
            parts = line.split("|", 2)
            if len(parts) < 2:
                continue
            if parts[0].isdigit() and int(parts[0]) >= cutoff:
                lines.append(line)
    RESTART_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESTART_STATE_FILE.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def restart_event_count() -> int:
    prune_restart_events()
    if not RESTART_STATE_FILE.exists():
        return 0
    return len([x for x in RESTART_STATE_FILE.read_text(encoding="utf-8").splitlines() if x.strip()])


def is_cooldown_active() -> bool:
    until = read_int_file(RESTART_COOLDOWN_FILE, 0)
    return until > now_epoch()


def allow_restart(component: str, reason: str) -> bool:
    if is_cooldown_active():
        until = read_int_file(RESTART_COOLDOWN_FILE, 0)
        log(f"Circuit breaker active. Skip {component} restart ({reason}). cooldown_until={until}")
        return False
    count = restart_event_count()
    if count >= RESTART_MAX_ATTEMPTS:
        until = now_epoch() + RESTART_COOLDOWN_SEC
        write_int_file(RESTART_COOLDOWN_FILE, until)
        log(f"Circuit breaker tripped. Skip {component} restart ({reason}). count={count}/{RESTART_MAX_ATTEMPTS}.")
        return False
    RESTART_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with RESTART_STATE_FILE.open("a", encoding="utf-8") as fh:
        fh.write(f"{now_epoch()}|{component}|{reason}\n")
    return True


def restart_service(unit: str, component: str, reason: str) -> None:
    local_actions.restart_service(
        local_actions.RestartActionContext(unit=unit, component=component, reason=reason),
        allow_restart=allow_restart,
        append_event=append_event,
        write_restart_reason=write_restart_reason,
        run_systemctl=run_systemctl,
        log=log,
        supervisor=runtime_supervisor_or_none(),
    )


def runtime_supervisor_or_none():
    mode = env("STREAM_RUNTIME_SUPERVISOR", "systemd").strip().lower()
    if mode not in {"k8s", "k3s", "kubernetes"}:
        return None
    return build_runtime_supervisor(
        run_systemctl=lambda args, check: run_systemctl(args, require_privilege=True, check=check),
    )


def kubectl_exec_stream_engine(script: str) -> subprocess.CompletedProcess[str]:
    return run(
        [
            STREAM_KUBECTL_BIN,
            "-n",
            STREAM_K8S_NAMESPACE,
            "exec",
            STREAM_V3_RUNTIME_WORKLOAD,
            "-c",
            STREAM_V3_RUNTIME_CONTAINER,
            "--",
            "sh",
            "-lc",
            script,
        ],
        check=False,
    )


def remote_cat_first(paths: list[str]) -> subprocess.CompletedProcess[str]:
    quoted = " ".join(shlex.quote(path) for path in paths)
    return kubectl_exec_stream_engine(
        f'''
        set -eu
        for p in {quoted}; do
            if [ -s "$p" ]; then
                cat "$p"
                exit 0
            fi
        done
        exit 3
        '''
    )


def remote_cat_latest_runtime_state() -> subprocess.CompletedProcess[str]:
    return kubectl_exec_stream_engine(
        r'''
        set -eu
        latest="$(ls -t /app/state/runtime/stream_runtime_state_*.json /state/stream_runtime_state_*.json 2>/dev/null | head -n 1 || true)"
        if [ -z "$latest" ]; then
            exit 3
        fi
        cat "$latest"
        '''
    )


def remote_tail_jsonl(path: str) -> subprocess.CompletedProcess[str]:
    return kubectl_exec_stream_engine(
        f'''
        set -eu
        if [ -s {shlex.quote(path)} ]; then
            tail -n 1 {shlex.quote(path)}
            exit 0
        fi
        exit 3
        '''
    )


def decode_json_stdout(cp: subprocess.CompletedProcess[str]) -> dict:
    if cp.returncode != 0:
        return {}
    try:
        payload = json.loads(cp.stdout or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def sync_remote_runtime_evidence() -> dict[str, dict]:
    synced: dict[str, dict] = {}

    runtime = decode_json_stdout(remote_cat_latest_runtime_state())
    if runtime:
        runtime_dest = STATE_ROOT / "stream_runtime_state_remote.json"
        write_json_file(runtime_dest, runtime)
        synced["runtime"] = runtime

    now_playing = decode_json_stdout(remote_cat_first(["/state/overlay/now_playing.json"]))
    if now_playing:
        write_json_file(STATE_ROOT / "overlay" / "now_playing.json", now_playing)
        synced["now_playing"] = now_playing

    fast_recovery = decode_json_stdout(remote_cat_first(["/state/fast_recovery_state.json"]))
    if fast_recovery:
        write_json_file(STATE_ROOT / "fast_recovery_state.json", fast_recovery)
        synced["fast_recovery"] = fast_recovery

    for remote_path, local_path, key in [
        ("/state/logs/play_history.jsonl", STATE_ROOT / "logs" / "play_history.jsonl", "play_history"),
        ("/state/logs/stream_engine_events.jsonl", STATE_ROOT / "logs" / "stream_engine_events.jsonl", "stream_engine"),
        ("/state/logs/fast_recovery_events.jsonl", STATE_ROOT / "logs" / "fast_recovery_events.jsonl", "fast_recovery_event"),
    ]:
        payload = decode_json_stdout(remote_tail_jsonl(remote_path))
        if payload:
            append_jsonl_if_changed(local_path, payload)
            synced[key] = payload

    return synced


def append_remote_snapshot_timeline(detail: str, synced: dict[str, dict]) -> None:
    runtime = synced.get("runtime", {})
    now_playing = synced.get("now_playing", {})
    now_playing_nested = now_playing.get("now_playing") if isinstance(now_playing.get("now_playing"), dict) else {}
    append_snapshot_timeline(
        "watchdog_ok",
        reason="",
        stream_service_substate="running",
        dj_service_substate="running",
        ffmpeg_count=1,
        runtime_snapshot={
            "path": str(STATE_ROOT / "stream_runtime_state_remote.json") if runtime else "",
            "age_sec": 0 if runtime else None,
            "run_id": runtime.get("run_id", ""),
            "status": runtime.get("status", "running" if runtime else ""),
            "ffmpeg_pid": runtime.get("ffmpeg_pid", ""),
            "restart_count": runtime.get("restart_count", 0),
            "last_health_ok": runtime.get("last_health_ok", ""),
            "last_event_id": runtime.get("last_event_id", ""),
            "updated_at_utc": runtime.get("updated_at_utc", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        },
        now_playing_state={
            "updated_at_utc": now_playing.get("updated_at_utc", ""),
            "status": now_playing.get("status", ""),
            "title": now_playing_nested.get("title", ""),
            "bucket": now_playing_nested.get("bucket", ""),
            "prefix": now_playing_nested.get("prefix", ""),
            "retry_attempt": ((now_playing.get("retry") or {}).get("attempt", 0)),
        },
        remote_probe=detail,
    )


def remote_only_watchdog() -> int:
    supervisor = runtime_supervisor_or_none()
    if supervisor is None:
        record_watchdog_stats("anomaly", reason="remote_only requires k8s supervisor")
        log("ERROR remote-only watchdog requires STREAM_RUNTIME_SUPERVISOR=k8s")
        return 0

    stream_status = supervisor.status(STREAM_SERVICE)
    dj_status = supervisor.status(DJ_SERVICE)
    if not stream_status.active or not dj_status.active:
        reason = f"k8s runtime inactive stream={stream_status.detail} dj={dj_status.detail}"
        append_event("service_unstable", service=STREAM_V3_RUNTIME_WORKLOAD, substate=reason)
        record_watchdog_stats("anomaly", reason=reason, stream_status=stream_status.detail, dj_status=dj_status.detail)
        restart_service(STREAM_SERVICE, "stream", reason)
        return 0

    probe = kubectl_exec_stream_engine(
        r'''
        set -eu
        rtmp_count="$(pgrep -a ffmpeg | grep -E 'rtmp://|rtmps://' | wc -l | tr -d ' ')"
        dj_count="$(pgrep -a ffmpeg | grep -v -E 'rtmp://|rtmps://' | wc -l | tr -d ' ')"
        test "${rtmp_count}" = "1"
        test "${dj_count}" -ge "1"
        curl -fsS -m 3 http://127.0.0.1:18080/index.html >/dev/null
        curl -fsS -m 4 http://127.0.0.1:18080/stream1090/data/aircraft.json >/dev/null
        printf 'rtmp_count=%s dj_count=%s overlay=ok aircraft=ok\n' "$rtmp_count" "$dj_count"
        '''
    )
    detail = (probe.stdout or probe.stderr or "").strip()
    if probe.returncode != 0:
        reason = f"k8s runtime probe failed: {detail[:240]}"
        append_event("remote_runtime_probe_failed", workload=STREAM_V3_RUNTIME_WORKLOAD, detail=detail)
        record_watchdog_stats("anomaly", reason=reason, stream_status=stream_status.detail, dj_status=dj_status.detail)
        return 0

    synced = sync_remote_runtime_evidence()
    record_watchdog_stats("ok", reason="remote k8s runtime ok", detail=detail, stream_status=stream_status.detail)
    if should_emit_watchdog_ok():
        append_event("watchdog_ok", **collect_diagnostic_snapshot(), remote_probe=detail)
    append_remote_snapshot_timeline(detail, synced)
    log(f"Remote k8s runtime ok: {detail}")
    return 0


def overlay_server_pids() -> list[int]:
    patterns = [OVERLAY_PROCESS_PATTERN]
    basename = Path(OVERLAY_PROCESS_PATTERN).name
    if basename and basename not in patterns:
        patterns.append(basename)
    pids: list[int] = []
    for pattern in patterns:
        cp = run(["pgrep", "-af", pattern], check=False)
        for line in (cp.stdout or "").splitlines():
            head = line.strip().split(" ", 1)[0]
            if head.isdigit() and int(head) != os.getpid():
                pids.append(int(head))
    return sorted(set(pids))


def overlay_server_command() -> list[str]:
    return [
        OVERLAY_PYTHON,
        str(BASE_DIR / "src" / "stream_core" / "overlay_server.py"),
        "--host",
        OVERLAY_BIND_HOST,
        "--port",
        str(OVERLAY_PORT),
        "--directory",
        OVERLAY_DIR,
        "--stream1090-url",
        STREAM1090_URL,
        "--now-playing-file",
        NOW_PLAYING_FILE,
        "--actual-range-supplement-file",
        OVERLAY_ACTUAL_RANGE_SUPPLEMENT_FILE,
        "--actual-range-supplement-hours",
        OVERLAY_ACTUAL_RANGE_SUPPLEMENT_HOURS,
    ]


def start_overlay_server_process() -> tuple[bool, str]:
    cmd = overlay_server_command()
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(BASE_DIR),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError as exc:
        return False, f"overlay process start failed: {exc}"
    append_event("overlay_recovery_process_started", pid=proc.pid, command=" ".join(cmd))
    return True, f"overlay process started pid={proc.pid}"


def recover_overlay_server(reason: str) -> tuple[bool, str]:
    pids = overlay_server_pids()
    append_event("overlay_recovery_start", overlay_url=OVERLAY_URL, overlay_reason=reason, overlay_pids=pids)
    if pids:
        run(["kill", "-TERM", *[str(pid) for pid in pids]], check=False)
        time.sleep(min(1.0, float(OVERLAY_RECOVERY_WAIT_SEC)))
        remaining = overlay_server_pids()
        if remaining:
            run(["kill", "-KILL", *[str(pid) for pid in remaining]], check=False)
            append_event("overlay_recovery_forced_kill", overlay_pids=remaining)

    started, start_reason = start_overlay_server_process()
    if not started:
        append_event("overlay_recovery_failed", overlay_url=OVERLAY_URL, recovery_reason=start_reason)
        return False, start_reason

    time.sleep(float(OVERLAY_RECOVERY_WAIT_SEC))
    ok, detail = check_overlay_detail()
    if ok:
        append_event("overlay_recovery_ok", overlay_url=OVERLAY_URL, recovery_reason=detail)
        return True, detail

    append_event("overlay_recovery_failed", overlay_url=OVERLAY_URL, recovery_reason=detail)
    return False, detail


def handle_overlay_unavailable(overlay_reason: str) -> None:
    append_event("overlay_unavailable", overlay_url=OVERLAY_URL, overlay_reason=overlay_reason)
    append_snapshot_timeline("anomaly", reason="overlay_unavailable", overlay_reason=overlay_reason)
    if OVERLAY_RECOVER_BEFORE_STREAM_RESTART:
        recovered, recovery_reason = recover_overlay_server(overlay_reason)
        if recovered:
            record_watchdog_stats(
                "ok",
                reason="overlay recovered without stream restart",
                overlay_url=OVERLAY_URL,
                overlay_reason=overlay_reason,
                overlay_recovery_reason=recovery_reason,
            )
            return
        overlay_reason = f"{overlay_reason}; overlay-only recovery failed: {recovery_reason}"

    restart_service(STREAM_SERVICE, "stream", f"overlay unhealthy: {overlay_reason}")
    record_watchdog_stats("anomaly", reason="overlay_unavailable", overlay_url=OVERLAY_URL, overlay_reason=overlay_reason)


def stream_ffmpeg_pids() -> list[int]:
    cp = run(["pgrep", "-af", "ffmpeg"], check=False)
    lines = (cp.stdout or "").splitlines()
    matched: list[str]
    if STREAM_KEY:
        needle = f"live2/{STREAM_KEY}"
        matched = [line for line in lines if needle in line]
    elif RTMP_URL:
        matched = [line for line in lines if RTMP_URL in line]
    else:
        matched = [line for line in lines if "rtmp://a.rtmp.youtube.com/live2" in line]
    pids: list[int] = []
    for line in matched:
        head = line.strip().split(" ", 1)[0]
        if head.isdigit():
            pids.append(int(head))
    return pids


def stream_ffmpeg_count() -> int:
    return len(stream_ffmpeg_pids())


def as_pulse_user(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    current_user = ""
    if pwd is not None:
        try:
            current_user = pwd.getpwuid(os.geteuid()).pw_name
        except Exception:
            current_user = ""
    env_cmd = [
        "env",
        "-u",
        "PULSE_SERVER",
        "PULSE_SHM=0",
        f"HOME={PULSE_HOME}",
        f"XDG_RUNTIME_DIR={PULSE_RUNTIME_DIR}",
        *cmd,
    ]
    if current_user == PULSE_USER:
        return run(env_cmd, check=False)
    full = [
        "sudo",
        "-u",
        PULSE_USER,
        *env_cmd,
    ]
    return run(full, check=False)


def pulse_server_ok() -> bool:
    return as_pulse_user(["pactl", "info"]).returncode == 0


def pulse_memfd_warning_recent() -> bool:
    cp = run(
        [
            "journalctl",
            "--since",
            f"{PULSE_WARN_RECENT_SEC} seconds ago",
            "--no-pager",
        ],
        check=False,
    )
    text = ((cp.stdout or "") + "\n" + (cp.stderr or "")).lower()
    return (
        "non-registered memfd id" in text
        or "memblock_replace_import(). aborting" in text
        or "stat_remove(). aborting" in text
    )


def pulse_source_exists() -> bool:
    cp = as_pulse_user(["pactl", "list", "short", "sources"])
    return any(
        len(line.split()) >= 2 and line.split()[1] == PULSE_SOURCE
        for line in (cp.stdout or "").splitlines()
    )


def load_pulse_health_state() -> dict[str, int]:
    return pulse_routes.normalize_health_state(read_json_file(PULSE_HEALTH_STATE_FILE, pulse_routes.PULSE_HEALTH_DEFAULT))


def save_pulse_health_state(state: dict[str, int]) -> None:
    write_json_file(PULSE_HEALTH_STATE_FILE, state)


def _latency_usec_from_line(line: str, prefix: str) -> int:
    return pulse_metrics.latency_usec_from_line(line, prefix)


def _parse_pactl_entries(raw: str, header_prefix: str) -> list[dict]:
    return pulse_metrics.parse_pactl_entries(raw, header_prefix)


def collect_pulse_route_metrics(stream_ffmpeg_pid: int | None) -> dict:
    sink_cp = as_pulse_user(["pactl", "list", "sink-inputs"])
    source_cp = as_pulse_user(["pactl", "list", "source-outputs"])
    if sink_cp.returncode != 0 or source_cp.returncode != 0:
        return {
            "ok": False,
            "error": "pactl_list_failed",
            "sink_inputs_rc": sink_cp.returncode,
            "source_outputs_rc": source_cp.returncode,
        }

    sink_entries = _parse_pactl_entries(sink_cp.stdout or "", "Sink Input #")
    source_entries = _parse_pactl_entries(source_cp.stdout or "", "Source Output #")

    dj_entry = None
    for entry in sink_entries:
        media_name = str((entry.get("properties") or {}).get("media.name", ""))
        if media_name == "adsb-streamnew-auto-dj":
            dj_entry = entry
            break

    capture_entry = None
    for entry in source_entries:
        props = entry.get("properties") or {}
        app_bin = str(props.get("application.process.binary", ""))
        media_name = str(props.get("media.name", ""))
        proc_id_raw = str(props.get("application.process.id", "")).strip()
        proc_id = int(proc_id_raw) if proc_id_raw.isdigit() else None
        if stream_ffmpeg_pid and proc_id == stream_ffmpeg_pid:
            capture_entry = entry
            break
        if capture_entry is None and app_bin == "ffmpeg" and media_name == "record":
            capture_entry = entry

    return {
        "ok": True,
        "stream_ffmpeg_pid": stream_ffmpeg_pid or 0,
        "sink_input_count": len(sink_entries),
        "source_output_count": len(source_entries),
        "dj_sink_input_present": dj_entry is not None,
        "dj_buffer_latency_usec": int(dj_entry.get("buffer_latency_usec", -1)) if dj_entry else -1,
        "dj_peer_latency_usec": int(dj_entry.get("peer_latency_usec", -1)) if dj_entry else -1,
        "capture_source_output_present": capture_entry is not None,
        "capture_buffer_latency_usec": int(capture_entry.get("buffer_latency_usec", -1)) if capture_entry else -1,
        "capture_peer_latency_usec": int(capture_entry.get("peer_latency_usec", -1)) if capture_entry else -1,
    }


def audio_has_energy() -> bool:
    wav = WORK_DIR / "pulse_3s.wav"
    cp = as_pulse_user(
        [
            "ffmpeg",
            "-nostdin",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "pulse",
            "-i",
            PULSE_SOURCE,
            "-t",
            "3",
            "-ac",
            "2",
            "-ar",
            "44100",
            "-c:a",
            "pcm_s16le",
            str(wav),
        ]
    )
    if cp.returncode != 0:
        return False
    cp2 = run(
        ["ffmpeg", "-hide_banner", "-i", str(wav), "-af", "volumedetect", "-f", "null", "-"],
        check=False,
    )
    text = (cp2.stderr or "") + "\n" + (cp2.stdout or "")
    m = re.findall(r"mean_volume:\s*([-0-9.]+)\s*dB", text)
    if not m:
        return False
    try:
        return float(m[-1]) > -45.0
    except ValueError:
        return False


def check_overlay_detail() -> tuple[bool, str]:
    cp = run(["curl", "-fsS", "-m", "3", f"{OVERLAY_URL}/index.html"], check=False)
    if cp.returncode != 0:
        return False, "overlay index unavailable"
    index_body = (cp.stdout or "") + (cp.stderr or "")
    if "Stream1090 Overlay" not in index_body or 'id="map"' not in index_body:
        return False, "overlay index missing expected map markers"

    if OVERLAY_REQUIRE_MAP_PROXY:
        cp_map = run(["curl", "-fsS", "-m", "5", f"{OVERLAY_URL}/stream1090/"], check=False)
        if cp_map.returncode != 0:
            return False, "overlay stream1090 proxy unavailable"
        map_body = (cp_map.stdout or "") + (cp_map.stderr or "")
        if "error" in map_body[:200].lower():
            return False, "overlay stream1090 proxy returned error payload"

    if OVERLAY_REQUIRE_ADSB_JSON:
        cp_adsb = run(["curl", "-fsS", "-m", "4", f"{OVERLAY_URL}/adsb/aircraft.json"], check=False)
        if cp_adsb.returncode != 0:
            return False, "overlay adsb aircraft json unavailable"
        try:
            payload = json.loads(cp_adsb.stdout or "{}")
        except json.JSONDecodeError:
            return False, "overlay adsb aircraft json invalid"
        if not isinstance(payload, dict) or not isinstance(payload.get("aircraft"), list):
            return False, "overlay adsb aircraft json missing aircraft list"
        fresh_ok, fresh_reason = check_adsb_freshness(payload)
        if not fresh_ok:
            return False, fresh_reason

    if OVERLAY_REQUIRE_OUTLINE_JSON:
        cp_outline = run(["curl", "-fsS", "-m", "4", f"{OVERLAY_URL}/stream1090/data/outline.json"], check=False)
        if cp_outline.returncode != 0:
            return False, "overlay actual range outline json unavailable"
        try:
            outline = json.loads(cp_outline.stdout or "{}")
        except json.JSONDecodeError:
            return False, "overlay actual range outline json invalid"
        outline_ok, outline_reason = check_overlay_outline_json(outline)
        if not outline_ok:
            return False, outline_reason

    return True, "overlay index/map proxy/adsb json/outline ok"


def check_overlay_ok() -> bool:
    ok, _reason = check_overlay_detail()
    return ok


def check_overlay_outline_json(payload: object) -> tuple[bool, str]:
    return overlay_health.check_overlay_outline_json(payload)


def check_adsb_freshness(payload: dict, current_ts: int | None = None) -> tuple[bool, str]:
    now_ts = now_epoch() if current_ts is None else int(current_ts)
    state = read_json_file(ADSB_FRESHNESS_STATE_FILE, {})
    ok, reason, next_state, event = overlay_health.adsb_freshness_judgment(
        payload,
        now_ts=now_ts,
        state=state,
        max_age_sec=ADSB_JSON_MAX_AGE_SEC,
        message_stall_sec=ADSB_MESSAGE_STALL_SEC,
    )
    if next_state is not None:
        write_json_file(ADSB_FRESHNESS_STATE_FILE, next_state)
    if event is not None:
        append_event("adsb_messages_counter_reset", **event)
    return ok, reason


def x11grab_input() -> str:
    display = DISPLAY_NAME or ":99"
    if "." in display:
        return f"{display}+0,0"
    return f"{display}.0+0,0"


def check_video_frame_detail() -> tuple[bool, str]:
    cp = run(
        [
            "ffmpeg",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "x11grab",
            "-video_size",
            VIDEO_SIZE,
            "-i",
            x11grab_input(),
            "-frames:v",
            "1",
            "-vf",
            "scale=1:1:flags=area,signalstats,metadata=print:file=-",
            "-f",
            "null",
            "-",
        ],
        check=False,
    )
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or "").strip()
        return False, f"video frame probe failed: {detail[:160]}"
    text = (cp.stdout or "") + "\n" + (cp.stderr or "")
    m = re.search(r"lavfi\.signalstats\.YAVG=([0-9.]+)", text)
    if not m:
        return False, "video frame probe missing YAVG"
    try:
        yavg = float(m.group(1))
    except ValueError:
        return False, "video frame probe invalid YAVG"
    if yavg < VIDEO_FRAME_MIN_LUMA:
        return False, f"video frame too dark (YAVG={yavg:.2f}<{VIDEO_FRAME_MIN_LUMA})"
    return True, f"video frame luma ok (YAVG={yavg:.2f})"


def runtime_snapshot_age_sec() -> int:
    return delivery_signals.runtime_snapshot_age_sec(
        RUNTIME_STATE_GLOB,
        read_json=read_json_file,
        now_epoch=now_epoch,
    )


def audio_bucket_boundary_detail(current_ts: int | None = None) -> dict[str, object]:
    now_ts = now_epoch() if current_ts is None else int(current_ts)
    return audio_transition.audio_bucket_boundary_detail(
        current_ts=now_ts,
        boundary_grace_sec=AUDIO_BUCKET_BOUNDARY_GRACE_SEC,
    )


def now_playing_transition_detail(current_ts: int | None = None) -> dict[str, object]:
    now_ts = now_epoch() if current_ts is None else int(current_ts)
    data = read_json_file(NOW_PLAYING_JSON, {})
    return audio_transition.now_playing_transition_detail(
        data if isinstance(data, dict) else {},
        current_ts=now_ts,
        transition_grace_sec=AUDIO_TRACK_TRANSITION_GRACE_SEC,
        boundary_grace_sec=AUDIO_BUCKET_BOUNDARY_GRACE_SEC,
    )


def now_playing_transition_age_sec(current_ts: int | None = None) -> int | None:
    value = now_playing_transition_detail(current_ts=current_ts).get("track_transition_age_sec")
    return int(value) if isinstance(value, int) else None


def collect_diagnostic_snapshot() -> dict:
    runtime_path = pick_runtime_state_path()
    runtime = maybe_read_json(runtime_path) if runtime_path else {}
    ytw = maybe_read_json(YTW_STATE_FILE)
    nowp = maybe_read_json(NOW_PLAYING_JSON)
    pattern = maybe_read_json(PATTERN_STATE_JSON)
    slo = maybe_read_json(SLO_FILE)
    restart_reason = maybe_read_json(RESTART_REASON_FILE)
    runtime_age = runtime_snapshot_age_sec()

    return {
        "stream_service_substate": service_substate(STREAM_SERVICE),
        "dj_service_substate": service_substate(DJ_SERVICE),
        "ffmpeg_count": stream_ffmpeg_count(),
        "runtime_snapshot": {
            "path": str(runtime_path) if runtime_path else "",
            "age_sec": runtime_age,
            "run_id": runtime.get("run_id", ""),
            "status": runtime.get("status", ""),
            "ffmpeg_pid": runtime.get("ffmpeg_pid", ""),
            "restart_count": runtime.get("restart_count", 0),
            "last_health_ok": runtime.get("last_health_ok", ""),
            "last_event_id": runtime.get("last_event_id", ""),
            "updated_at_utc": runtime.get("updated_at_utc", ""),
        },
        "youtube_watchdog_state": {
            "fail_count": ytw.get("fail_count", 0),
            "last_reason": ytw.get("last_reason", ""),
            "last_video_id": ytw.get("last_video_id", ""),
            "last_api_search_ts": ytw.get("last_api_search_ts", 0),
        },
        "now_playing_state": {
            "updated_at_utc": nowp.get("updated_at_utc", ""),
            "status": nowp.get("status", ""),
            "title": ((nowp.get("now_playing") or {}).get("title", "")),
            "bucket": ((nowp.get("now_playing") or {}).get("bucket", "")),
            "prefix": ((nowp.get("now_playing") or {}).get("prefix", "")),
            "retry_attempt": ((nowp.get("retry") or {}).get("attempt", 0)),
        },
        "pattern_state": {
            "by_folder": pattern.get("by_folder", {}),
        },
        "restart_reason_state": restart_reason,
        "slo_state": {
            "ts_utc": slo.get("ts_utc", ""),
            "pulse_unavailable_count": slo.get("pulse_unavailable_count", 0),
            "restart_trigger_count": slo.get("restart_trigger_count", 0),
            "slo_pulse_unavailable_24h_max": slo.get("slo_pulse_unavailable_24h_max", 0),
        },
    }


def pulse_slo_summary_24h() -> dict[str, int]:
    cutoff = now_epoch() - 24 * 3600
    pulse_unavailable = 0
    restart_trigger = 0
    if EVENT_LOG_FILE.exists():
        for line in EVENT_LOG_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = item.get("ts_utc", "")
            et = item.get("event_type", "")
            try:
                epoch = int(calendar.timegm(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")))
            except Exception:
                continue
            if epoch < cutoff:
                continue
            if et == "pulse_unavailable":
                pulse_unavailable += 1
            if et == "restart_trigger":
                restart_trigger += 1
    return {
        "window_sec": 24 * 3600,
        "pulse_unavailable_count": pulse_unavailable,
        "restart_trigger_count": restart_trigger,
    }


def write_slo_snapshot(summary: dict[str, int]) -> None:
    payload = {
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "slo_pulse_unavailable_24h_max": SLO_PULSE_UNAVAILABLE_24H_MAX,
        **summary,
    }
    SLO_FILE.parent.mkdir(parents=True, exist_ok=True)
    SLO_FILE.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")


def recover_pulse_then_restart(stage: int) -> None:
    append_event("pulse_recovery_stage", stage=stage, max_stage=RECOVERY_STAGE_MAX, window_sec=PULSE_STAGE_WINDOW_SEC)
    if stage <= 2:
        for unit, component, reason in audio_signals.choose_staged_audio_recovery(
            stage=stage,
            dj_service=DJ_SERVICE,
            stream_service=STREAM_SERVICE,
            reason_prefix="pulse server unavailable",
        ):
            restart_service(unit, component, reason)
        return
    append_event("pulse_recovery_start", ensure_pulse_script=ENSURE_PULSE_SCRIPT)
    run([ENSURE_PULSE_SCRIPT], check=False)
    time.sleep(PULSE_RECOVERY_WAIT_SEC)
    if pulse_server_ok() and PULSE_RECOVER_ONLY_DJ_FIRST:
        append_event("pulse_recovery_dj_first", wait_sec=PULSE_RECOVERY_WAIT_SEC)
        restart_service(DJ_SERVICE, "dj", "pulse server unavailable [stage3 pulse-ok dj-first]")
        if stream_ffmpeg_count() < 1:
            restart_service(STREAM_SERVICE, "stream", "pulse recovered but stream ffmpeg missing [stage3]")
        return
    restart_service(DJ_SERVICE, "dj", "pulse server unavailable [stage3]")
    restart_service(STREAM_SERVICE, "stream", "pulse server unavailable [stage3]")


def main() -> int:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    if STREAM_WATCHDOG_REMOTE_ONLY:
        return remote_only_watchdog()
    try:
        shutil.chown(str(WORK_DIR), user=PULSE_USER, group=PULSE_USER)
    except Exception:
        pass
    stage_state = load_recovery_stage_state()
    pulse_health_state = load_pulse_health_state()
    append_snapshot_timeline("watchdog_tick")

    if not is_service_stable(DJ_SERVICE):
        append_event("service_unstable", service=DJ_SERVICE, substate=service_substate(DJ_SERVICE))
        append_snapshot_timeline("anomaly", reason="dj_service_unstable")
        restart_service(DJ_SERVICE, "dj", "DJ service is not active")
    if not is_service_stable(STREAM_SERVICE):
        append_event("service_unstable", service=STREAM_SERVICE, substate=service_substate(STREAM_SERVICE))
        append_snapshot_timeline("anomaly", reason="stream_service_unstable")
        restart_service(STREAM_SERVICE, "stream", "stream service is not active")
        record_watchdog_stats("anomaly", reason="stream_service_unstable")
        return 0

    stream_age = service_uptime_sec(STREAM_SERVICE)
    dj_age = service_uptime_sec(DJ_SERVICE)
    if stream_age < AUDIO_STARTUP_GRACE_SEC or dj_age < AUDIO_STARTUP_GRACE_SEC:
        write_int_file(AUDIO_FAIL_COUNT_FILE, 0)
        log(f"Warm-up grace active (stream={stream_age}s dj={dj_age}s < {AUDIO_STARTUP_GRACE_SEC}s)")
        append_event("warmup_grace", stream_age_sec=stream_age, dj_age_sec=dj_age, grace_sec=AUDIO_STARTUP_GRACE_SEC)
        record_watchdog_stats(
            "warmup_grace",
            reason="startup_grace_active",
            stream_age_sec=stream_age,
            dj_age_sec=dj_age,
            grace_sec=AUDIO_STARTUP_GRACE_SEC,
        )
        return 0

    if pulse_memfd_warning_recent():
        append_event("pulse_memfd_warning_recent", lookback_sec=PULSE_WARN_RECENT_SEC)

    if not pulse_server_ok():
        append_event("pulse_unavailable")
        append_snapshot_timeline("anomaly", reason="pulse_unavailable")
        stage = bump_stage(stage_state, "pulse_stage", "pulse_last_ts", PULSE_STAGE_WINDOW_SEC)
        save_recovery_stage_state(stage_state)
        recover_pulse_then_restart(stage=stage)
        summary = pulse_slo_summary_24h()
        write_slo_snapshot(summary)
        if summary["pulse_unavailable_count"] > SLO_PULSE_UNAVAILABLE_24H_MAX:
            append_event(
                "slo_breach",
                slo="pulse_unavailable_24h",
                value=summary["pulse_unavailable_count"],
                threshold=SLO_PULSE_UNAVAILABLE_24H_MAX,
            )
        record_watchdog_stats("anomaly", reason="pulse_unavailable", stage=stage)
        return 0
    reset_stage(stage_state, "pulse_stage", "pulse_last_ts")
    save_recovery_stage_state(stage_state)

    count = stream_ffmpeg_count()
    if count < 1:
        append_event("stream_ffmpeg_missing", count=count)
        append_snapshot_timeline("anomaly", reason="stream_ffmpeg_missing", ffmpeg_count=count)
        restart_service(STREAM_SERVICE, "stream", "no RTMP ffmpeg process found")
        record_watchdog_stats("anomaly", reason="stream_ffmpeg_missing", ffmpeg_count=count)
        return 0
    if count > 1:
        append_event("stream_ffmpeg_duplicate", count=count)
        append_snapshot_timeline("anomaly", reason="stream_ffmpeg_duplicate", ffmpeg_count=count)
        restart_service(STREAM_SERVICE, "stream", f"duplicate RTMP ffmpeg processes detected ({count})")
        record_watchdog_stats("anomaly", reason="stream_ffmpeg_duplicate", ffmpeg_count=count)
        return 0

    if ENABLE_PULSE_PRECISION_PROBE:
        ffmpeg_pids = stream_ffmpeg_pids()
        stream_pid = ffmpeg_pids[0] if ffmpeg_pids else None
        pulse_metrics = collect_pulse_route_metrics(stream_pid)
        if not pulse_metrics.get("ok", False):
            append_event("pulse_precision_probe_failed", **pulse_metrics)
        else:
            pulse_health_state = pulse_routes.update_health_state(
                pulse_health_state,
                pulse_metrics,
                dj_latency_crit_usec=PULSE_DJ_BUFFER_LATENCY_CRIT_USEC,
                capture_latency_crit_usec=PULSE_CAPTURE_BUFFER_LATENCY_CRIT_USEC,
            )
            save_pulse_health_state(pulse_health_state)

            anomaly = pulse_routes.anomaly_decision(
                pulse_health_state,
                pulse_metrics,
                threshold=PULSE_ROUTE_FAIL_THRESHOLD,
                dj_latency_crit_usec=PULSE_DJ_BUFFER_LATENCY_CRIT_USEC,
                capture_latency_crit_usec=PULSE_CAPTURE_BUFFER_LATENCY_CRIT_USEC,
            )
            if anomaly:
                event_fields = dict(anomaly.get("event_fields", {}) or {})
                append_event(
                    "pulse_route_anomaly",
                    case=anomaly["case"],
                    consecutive_fail_count=anomaly["count"],
                    threshold=PULSE_ROUTE_FAIL_THRESHOLD,
                    **event_fields,
                    **pulse_metrics,
                )
                append_snapshot_timeline("anomaly", reason=anomaly["case"], **pulse_metrics)
                service = DJ_SERVICE if anomaly["component"] == "dj" else STREAM_SERVICE
                restart_service(service, anomaly["component"], anomaly["reason"])
                if anomaly.get("extra_stream_reason"):
                    restart_service(STREAM_SERVICE, "stream", anomaly["extra_stream_reason"])
                record_watchdog_stats("anomaly", reason=anomaly["case"], **pulse_metrics)
                return 0

            # Warning-only telemetry for proactive tuning (no restart).
            if pulse_routes.warning_due(
                pulse_metrics,
                dj_latency_warn_usec=PULSE_DJ_BUFFER_LATENCY_WARN_USEC,
                capture_latency_warn_usec=PULSE_CAPTURE_BUFFER_LATENCY_WARN_USEC,
            ):
                append_event(
                    "pulse_route_warning",
                    observed_dj_buffer_latency_usec=pulse_routes.int_value(
                        pulse_metrics.get("dj_buffer_latency_usec", -1),
                        -1,
                    ),
                    observed_capture_buffer_latency_usec=pulse_routes.int_value(
                        pulse_metrics.get("capture_buffer_latency_usec", -1),
                        -1,
                    ),
                    dj_warn_usec=PULSE_DJ_BUFFER_LATENCY_WARN_USEC,
                    capture_warn_usec=PULSE_CAPTURE_BUFFER_LATENCY_WARN_USEC,
                    **pulse_metrics,
                )

    overlay_ok, overlay_reason = check_overlay_detail()
    if not overlay_ok:
        handle_overlay_unavailable(overlay_reason)
        return 0
    if ENABLE_VIDEO_FRAME_PROBE:
        frame_ok, frame_reason = check_video_frame_detail()
        if not frame_ok:
            append_event("video_frame_unhealthy", frame_reason=frame_reason)
            append_snapshot_timeline("anomaly", reason="video_frame_unhealthy", frame_reason=frame_reason)
            restart_service(STREAM_SERVICE, "stream", frame_reason)
            record_watchdog_stats("anomaly", reason="video_frame_unhealthy", frame_reason=frame_reason)
            return 0
    runtime_age = runtime_snapshot_age_sec()
    if RUNTIME_SNAPSHOT_STALE_SEC > 0 and runtime_age > RUNTIME_SNAPSHOT_STALE_SEC:
        append_event("runtime_snapshot_stale", stale_age_sec=runtime_age, threshold_sec=RUNTIME_SNAPSHOT_STALE_SEC)
        append_snapshot_timeline("anomaly", reason="runtime_snapshot_stale", stale_age_sec=runtime_age)
        restart_service(STREAM_SERVICE, "stream", f"runtime snapshot stale ({runtime_age}s)")
        record_watchdog_stats(
            "anomaly",
            reason="runtime_snapshot_stale",
            stale_age_sec=runtime_age,
            threshold_sec=RUNTIME_SNAPSHOT_STALE_SEC,
        )
        return 0

    if ENABLE_AUDIO_PROBE:
        if not pulse_source_exists():
            fails = read_int_file(PULSE_SOURCE_MISSING_COUNT_FILE, 0) + 1
            write_int_file(PULSE_SOURCE_MISSING_COUNT_FILE, fails)
            write_int_file(AUDIO_FAIL_COUNT_FILE, 0)
            stage = bump_stage(stage_state, "audio_stage", "audio_last_ts", AUDIO_STAGE_WINDOW_SEC)
            save_recovery_stage_state(stage_state)
            append_event(
                "pulse_source_missing",
                pulse_source=PULSE_SOURCE,
                consecutive_fail_count=fails,
                threshold=AUDIO_FAIL_THRESHOLD,
                stage=stage,
            )
            append_snapshot_timeline("anomaly", reason="pulse_source_missing", pulse_source=PULSE_SOURCE)
            if fails < AUDIO_FAIL_THRESHOLD or stage <= 1:
                restart_service(DJ_SERVICE, "dj", f"pulse source missing ({fails}) [stage1 dj-only]")
            elif stage == 2:
                restart_service(STREAM_SERVICE, "stream", f"pulse source missing ({fails}) [stage2 stream-only]")
            else:
                restart_service(DJ_SERVICE, "dj", f"pulse source missing ({fails}) [stage3]")
                restart_service(STREAM_SERVICE, "stream", f"pulse source missing ({fails}) [stage3]")
            record_watchdog_stats(
                "anomaly",
                reason="pulse_source_missing",
                pulse_source=PULSE_SOURCE,
                consecutive_fail_count=fails,
                threshold=AUDIO_FAIL_THRESHOLD,
                stage=stage,
            )
            return 0
        write_int_file(PULSE_SOURCE_MISSING_COUNT_FILE, 0)
        if not audio_has_energy():
            fails = read_int_file(AUDIO_FAIL_COUNT_FILE, 0) + 1
            transition_detail = now_playing_transition_detail()
            transition_age = transition_detail.get("track_transition_age_sec")
            if (
                AUDIO_TRACK_TRANSITION_GRACE_SEC > 0
                and isinstance(transition_age, int)
                and transition_age <= AUDIO_TRACK_TRANSITION_GRACE_SEC
            ):
                write_int_file(AUDIO_FAIL_COUNT_FILE, 0)
                append_event(
                    "audio_energy_low_transition_grace",
                    transition_age_sec=transition_age,
                    grace_sec=AUDIO_TRACK_TRANSITION_GRACE_SEC,
                    **transition_detail,
                )
                record_watchdog_stats(
                    "warmup_grace",
                    reason="audio energy low during track transition grace",
                    transition_age_sec=transition_age,
                    grace_sec=AUDIO_TRACK_TRANSITION_GRACE_SEC,
                    **transition_detail,
                )
                return 0
            write_int_file(AUDIO_FAIL_COUNT_FILE, fails)
            append_event(
                "audio_energy_low",
                consecutive_fail_count=fails,
                dj_restart_fails=AUDIO_DJ_RESTART_FAILS,
                stream_restart_fails=AUDIO_STREAM_RESTART_FAILS,
                **transition_detail,
            )
            if fails < AUDIO_DJ_RESTART_FAILS:
                record_watchdog_stats(
                    "ok",
                    reason="audio energy low observed; waiting for confirmation",
                    consecutive_fail_count=fails,
                    dj_restart_fails=AUDIO_DJ_RESTART_FAILS,
                    stream_restart_fails=AUDIO_STREAM_RESTART_FAILS,
                    **transition_detail,
                )
                return 0
            stage = bump_stage(stage_state, "audio_stage", "audio_last_ts", AUDIO_STAGE_WINDOW_SEC)
            save_recovery_stage_state(stage_state)
            append_snapshot_timeline("anomaly", reason="audio_energy_low", consecutive_fail_count=fails)
            if fails >= AUDIO_STREAM_RESTART_FAILS:
                restart_service(STREAM_SERVICE, "stream", f"audio energy missing ({fails}) [stream]")
            else:
                restart_service(DJ_SERVICE, "dj", f"audio energy missing ({fails}) [dj]")
            record_watchdog_stats(
                "anomaly",
                reason="audio_energy_low",
                consecutive_fail_count=fails,
                dj_restart_fails=AUDIO_DJ_RESTART_FAILS,
                stream_restart_fails=AUDIO_STREAM_RESTART_FAILS,
                stage=stage,
                **transition_detail,
            )
            return 0
    write_int_file(AUDIO_FAIL_COUNT_FILE, 0)
    reset_stage(stage_state, "audio_stage", "audio_last_ts")
    save_recovery_stage_state(stage_state)
    log("OK")
    write_slo_snapshot(pulse_slo_summary_24h())
    record_watchdog_stats("ok", ffmpeg_count=count, runtime_snapshot_age_sec=runtime_age)
    if should_emit_watchdog_ok():
        append_event("watchdog_ok")
        append_snapshot_timeline("watchdog_ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
