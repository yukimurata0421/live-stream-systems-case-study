#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from maintenance_audit import audit_maintenance_decision

try:
    from .fast_recovery_core import budget as budget_policy
    from .fast_recovery_core import (
        connectivity_policy,
        effect_contract,
        probes,
        tcp_metrics,
        tcp_send_sample,
    )
    from .fast_recovery_core import decision as recovery_decision
    from .fast_recovery_core import policy as recovery_policy
    from .fast_recovery_core import remote_warning as remote_warning_core
    from .fast_recovery_core import restart_context as restart_context_writer
    from .fast_recovery_core import state as recovery_state
except ImportError:
    from fast_recovery_core import budget as budget_policy
    from fast_recovery_core import (
        connectivity_policy,
        effect_contract,
        probes,
        tcp_metrics,
        tcp_send_sample,
    )
    from fast_recovery_core import decision as recovery_decision
    from fast_recovery_core import policy as recovery_policy
    from fast_recovery_core import remote_warning as remote_warning_core
    from fast_recovery_core import restart_context as restart_context_writer
    from fast_recovery_core import state as recovery_state

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
        parsed = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.astimezone(timezone.utc).timestamp())
    except (TypeError, ValueError):
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
TRANSPORT_SNAPSHOT_FILE = Path(
    env(
        "FR_TRANSPORT_SNAPSHOT_FILE",
        str(STATE_FILE.parent / "runtime" / "ffmpeg_transport_latest.json"),
    )
)
CONTROLLER_ID = env("FR_CONTROLLER_ID", "dell_fast_recovery")
EXECUTION_MODE = env("FR_EXECUTION_MODE", "execute")
EFFECT_AUTHORITY_MODE = env("FR_EFFECT_AUTHORITY_MODE", "LEGACY_DIRECT").upper()
EFFECT_EXECUTOR_SOCKET = env("FR_EFFECT_EXECUTOR_SOCKET")
EFFECT_PRODUCER_ID = env("FR_EFFECT_PRODUCER_ID", "legacy-in-pod")
EFFECT_PRODUCER_GENERATION = max(1, int_env("FR_EFFECT_PRODUCER_GENERATION", 1))
CONTROLLER_RUNTIME_MODE = env("FR_CONTROLLER_RUNTIME_MODE", "LEGACY").upper()
RUNTIME_OBSERVATION_FILE = Path(env("FR_RUNTIME_OBSERVATION_FILE", "/run/stream-v3-control/runtime-observation.json"))
GPU_PREFLIGHT_ENABLED = bool_env("FR_GPU_PREFLIGHT_ENABLED", True)
GPU_PREFLIGHT_TIMEOUT_SEC = max(1.0, float_env("FR_GPU_PREFLIGHT_TIMEOUT_SEC", 3.0))
GPU_PREFLIGHT_DEPLOYMENT = env("FR_GPU_PREFLIGHT_DEPLOYMENT", "stream-v3-runtime")
GPU_PREFLIGHT_SELECTOR = env(
    "FR_GPU_PREFLIGHT_SELECTOR",
    "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime",
)
GPU_PREFLIGHT_CONTAINER = env("FR_GPU_PREFLIGHT_CONTAINER", "stream-engine")
FFMPEG_MISSING_REQUIRE_CURRENT_POD_ESTABLISHED = bool_env(
    "FR_FFMPEG_MISSING_REQUIRE_CURRENT_POD_ESTABLISHED",
    True,
)
BOOT_ESTABLISHED_FILE = Path(
    env("STREAM_BOOT_ESTABLISHED_FILE", str(STATE_ROOT / "runtime" / "stream_boot_established.json"))
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
    recovery_action: dict[str, Any] | None = None,
) -> dict[str, Any]:
    action = recovery_action or {}
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
        recovery_action_id=str(action.get("recovery_action_id") or ""),
        controller_id=str(action.get("controller_id") or ""),
        execution_mode=str(action.get("execution_mode") or ""),
        execute=bool(action.get("execute")) if "execute" in action else None,
        idempotency_key=str(action.get("idempotency_key") or ""),
        requested_signal=str(action.get("requested_signal") or ""),
        recovery_scope=str(action.get("recovery_scope") or ""),
    )
    try:
        restart_context_writer.write_fast_recovery_restart_context(RESTART_REASON_FILE, payload)
    except Exception as e:
        log(f"WARN failed to write restart reason: {e}")
    return payload


def planned_recovery_scope(reason_kind: str) -> str:
    if EFFECT_EXECUTOR_SOCKET:
        decision = recovery_policy.select_recovery_intent(
            reason_kind,
            runtime_observation=effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE),
        )
        return decision.effect_scope
    if k8s_supervisor_active() and reason_kind in {"tcp_stall", "remote_warning"}:
        return "ffmpeg_child"
    return "runtime"


def record_typed_policy_decision(
    reason_kind: str,
    *,
    runtime_observation: dict[str, Any] | None = None,
) -> recovery_policy.RecoveryIntentDecision:
    observation = (
        effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
        if runtime_observation is None and CONTROLLER_RUNTIME_MODE != "LEGACY"
        else (runtime_observation or {})
    )
    decision = recovery_policy.select_recovery_intent(
        reason_kind,
        runtime_observation=observation,
    )
    if CONTROLLER_RUNTIME_MODE != "LEGACY":
        append_event(
            "typed_policy_decision",
            decision.decision_reason,
            {**decision.to_dict(), "physical_effect_count": 0},
        )
    return decision


def effect_authority_active() -> bool:
    if not EFFECT_EXECUTOR_SOCKET:
        return EFFECT_AUTHORITY_MODE != "SHADOW_ONLY"
    if EFFECT_AUTHORITY_MODE == "SHADOW_ONLY":
        return False
    if EFFECT_AUTHORITY_MODE != "AUTO_FENCED":
        return True
    observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
    return (
        str(observation.get("active_producer_id") or "") == EFFECT_PRODUCER_ID
        and int(observation.get("active_producer_generation") or 0) == EFFECT_PRODUCER_GENERATION
    )


def new_recovery_action(
    *,
    now_ts: int,
    reason_kind: str,
    reason_first_ts: int,
    ffmpeg_pid: int,
    recovery_scope: str,
    execute: bool | None = None,
) -> dict[str, Any]:
    action_id = f"fra-{now_ts}-{uuid.uuid4().hex[:12]}"
    idempotency_key = ":".join(
        (
            CONTROLLER_ID,
            recovery_scope,
            str(ffmpeg_pid),
            reason_kind or "unknown",
        )
    )
    return {
        "recovery_action_id": action_id,
        "controller_id": CONTROLLER_ID,
        "execution_mode": EXECUTION_MODE,
        "execute": effect_authority_active() if execute is None else execute,
        "idempotency_key": idempotency_key,
        "requested_signal": "SIGTERM" if recovery_scope == "ffmpeg_child" else "",
        "recovery_scope": recovery_scope,
        "recovery_episode_started_ts": reason_first_ts or now_ts,
    }


def append_recovery_requested(
    *,
    action: dict[str, Any],
    reason_kind: str,
    reason: str,
    ffmpeg_pid: int,
    ffmpeg_uptime_sec: int,
    metrics: dict[str, Any] | None,
) -> None:
    append_event(
        "recovery_requested",
        reason,
        {
            **action,
            "trigger": reason_kind,
            "ffmpeg_pid": ffmpeg_pid,
            "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
            "transport_snapshot": metrics or {},
        },
    )


def append_recovery_dispatch_result(
    *,
    action: dict[str, Any],
    reason_kind: str,
    reason: str,
    ffmpeg_pid: int,
    ok: bool,
    detail: str,
    automatic_retry: bool | None = None,
) -> None:
    scope = str(action.get("recovery_scope") or "runtime")
    if not ok and automatic_retry is False:
        kind = "recovery_outcome_unknown"
    elif not ok:
        kind = "recovery_action_failed"
    elif scope == "ffmpeg_child":
        kind = "recovery_signal_sent"
    else:
        kind = "recovery_action_dispatched"
    append_event(
        kind,
        reason,
        {
            **action,
            "trigger": reason_kind,
            "ffmpeg_pid": ffmpeg_pid,
            "dispatch_ok": ok,
            "automatic_retry": automatic_retry,
            "process_still_running_after_dispatch": bool(
                ok and scope == "ffmpeg_child" and "owns final child cleanup" in detail
            ),
            "detail": detail,
        },
    )


def remember_pending_recovery(
    state: dict[str, Any],
    *,
    action: dict[str, Any],
    now_ts: int,
    reason_kind: str,
    reason: str,
    ffmpeg_pid: int,
) -> None:
    raw = state.get("pending_recovery_actions", [])
    pending = [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []
    pending.append(
        {
            **action,
            "requested_at_ts": now_ts,
            "trigger": reason_kind,
            "reason": reason,
            "requested_ffmpeg_pid": ffmpeg_pid,
        }
    )
    state["pending_recovery_actions"] = pending[-16:]


def pending_recovery_count(state: dict[str, Any]) -> int:
    raw = state.get("pending_recovery_actions", [])
    if not isinstance(raw, list):
        return 0
    return sum(1 for item in raw if isinstance(item, dict))


def sync_pending_recovery_from_executor(state: dict[str, Any], scopes: list[dict[str, Any]]) -> None:
    """Make the durable executor ledger authoritative after controller re-entry."""

    raw = state.get("pending_recovery_actions", [])
    local = {
        str(item.get("recovery_action_id") or item.get("owner_request_id") or ""): item
        for item in raw
        if isinstance(item, dict)
    } if isinstance(raw, list) else {}
    pending: list[dict[str, Any]] = []
    for scope in scopes:
        owner = str(scope.get("owner_request_id") or "")
        identity = scope.get("identity") if isinstance(scope.get("identity"), dict) else {}
        existing = local.get(owner, {})
        pending.append(
            {
                **existing,
                "recovery_action_id": owner,
                "owner_request_id": owner,
                "owner_request_digest": str(scope.get("owner_request_digest") or ""),
                "effect_scope_id": str(scope.get("effect_scope_id") or ""),
                "executor_scope_state": str(scope.get("state") or "UNKNOWN"),
                "requested_ffmpeg_pid": int(identity.get("ffmpeg_pid", 0) or 0),
                "requested_ffmpeg_generation": str(identity.get("ffmpeg_generation") or ""),
                "executor_identity": identity,
            }
        )
    state["pending_recovery_actions"] = pending[-16:]


def read_executor_unresolved_scopes(state: dict[str, Any]) -> list[dict[str, Any]] | None:
    if not EFFECT_EXECUTOR_SOCKET:
        return []
    try:
        response = effect_contract.unresolved_effect_scopes(socket_path=Path(EFFECT_EXECUTOR_SOCKET))
    except BaseException as exc:  # noqa: BLE001 - inability to prove zero unresolved is fail-closed
        state["last_reason"] = f"effect ledger unavailable; new action suppressed: {type(exc).__name__}"
        return None
    scopes = [dict(item) for item in response["unresolved_scopes"]]
    sync_pending_recovery_from_executor(state, scopes)
    return scopes


def record_unresolved_recovery_dispatch(
    state: dict[str, Any],
    *,
    action: dict[str, Any],
    now_ts: int,
    reason_kind: str,
    reason: str,
    ffmpeg_pid: int,
    detail: str,
    restart_events: list[dict[str, int | str]],
) -> None:
    remember_pending_recovery(
        state,
        action=action,
        now_ts=now_ts,
        reason_kind=reason_kind,
        reason=reason,
        ffmpeg_pid=ffmpeg_pid,
    )
    state["restart_events"] = trim_restart_events(
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
    state["last_restart_ts"] = now_ts
    state["last_restart_failure_ts"] = 0
    state["restart_failure_count"] = 0
    state["last_reason"] = f"effect outcome unresolved; automatic retry suppressed: {detail}"


def write_transport_snapshot(
    *,
    now_ts: int,
    ffmpeg_pid: int,
    ffmpeg_uptime_sec: int,
    tcp,
    network,
    remote_warning: bool,
) -> dict[str, Any]:
    payload = {
        "ts_utc": datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "controller_id": CONTROLLER_ID,
        "ffmpeg_pid": ffmpeg_pid,
        "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
        "metrics": recovery_decision.restart_metrics(
            tcp=tcp,
            network_down=network.network_down,
            remote_warning=remote_warning,
        ),
        "network": {
            "gateway_ok": network.gateway_ok,
            "public_ok_count": network.public_ok_count,
            "dns_ok": network.dns_ok,
            "tcp_probe_ok": network.tcp_probe_ok,
            "network_down": network.network_down,
        },
    }
    try:
        TRANSPORT_SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = TRANSPORT_SNAPSHOT_FILE.with_suffix(TRANSPORT_SNAPSHOT_FILE.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp.replace(TRANSPORT_SNAPSHOT_FILE)
    except Exception as exc:
        log(f"WARN failed to write transport snapshot: {exc}")
    return payload


def maybe_record_recovery_completed(
    state: dict[str, Any],
    *,
    now_ts: int,
    ffmpeg_pid: int,
    ffmpeg_uptime_sec: int,
    transport_snapshot: dict[str, Any],
    youtube_hint: dict[str, Any],
) -> None:
    raw = state.get("pending_recovery_actions", [])
    pending = [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []
    if not pending:
        return
    metrics = transport_snapshot.get("metrics") if isinstance(transport_snapshot.get("metrics"), dict) else {}
    network = transport_snapshot.get("network") if isinstance(transport_snapshot.get("network"), dict) else {}
    if (
        ffmpeg_pid <= 1
        or int(metrics.get("bytes_sent", 0) or 0) <= 0
        or bool(network.get("network_down"))
        or not bool(network.get("tcp_probe_ok"))
    ):
        return
    completed: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    for item in pending:
        requested_pid = int(item.get("requested_ffmpeg_pid", 0) or 0)
        if requested_pid > 1 and requested_pid == ffmpeg_pid:
            remaining.append(item)
            continue
        completed.append(item)
    for item in completed:
        same_key_count = sum(
            1 for other in completed if other.get("idempotency_key") == item.get("idempotency_key")
        )
        append_event(
            "recovery_completed",
            str(item.get("reason") or "recovery completed"),
            {
                **{key: item.get(key) for key in (
                    "recovery_action_id",
                    "controller_id",
                    "execution_mode",
                    "execute",
                    "idempotency_key",
                    "requested_signal",
                    "recovery_scope",
                    "trigger",
                )},
                "requested_ffmpeg_pid": int(item.get("requested_ffmpeg_pid", 0) or 0),
                "ffmpeg_pid": ffmpeg_pid,
                "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
                "recovery_elapsed_sec": max(0, now_ts - int(item.get("requested_at_ts", now_ts) or now_ts)),
                "same_idempotency_action_count": same_key_count,
                "transport_snapshot": transport_snapshot,
                "youtube_hint": youtube_hint,
            },
        )
    state["pending_recovery_actions"] = remaining[-16:]


def maybe_reconcile_delayed_executor_effects(
    state: dict[str, Any],
    *,
    unresolved_scopes: list[dict[str, Any]],
    now_ts: int,
    ffmpeg_pid: int,
    ffmpeg_uptime_sec: int,
    transport_snapshot: dict[str, Any],
    youtube_hint: dict[str, Any],
) -> bool:
    if not unresolved_scopes or ffmpeg_pid <= 1:
        return False
    observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
    observed_target = observation.get("target_identity") if isinstance(observation, dict) else None
    metrics = transport_snapshot.get("metrics") if isinstance(transport_snapshot.get("metrics"), dict) else {}
    network = transport_snapshot.get("network") if isinstance(transport_snapshot.get("network"), dict) else {}
    if (
        not isinstance(observed_target, dict)
        or int(metrics.get("bytes_sent", 0) or 0) <= 0
        or bool(network.get("network_down"))
        or not bool(network.get("tcp_probe_ok"))
    ):
        return False
    reconciled_any = False
    stable_fields = ("host_id", "host_boot_id", "namespace", "pod_uid", "container_name", "container_id")
    for scope in unresolved_scopes:
        before = scope.get("identity") if isinstance(scope.get("identity"), dict) else None
        if not isinstance(before, dict):
            continue
        if any(before.get(name) != observed_target.get(name) for name in stable_fields):
            continue
        if (
            int(before.get("ffmpeg_pid", 0) or 0) == int(observed_target.get("ffmpeg_pid", 0) or 0)
            or str(before.get("ffmpeg_generation") or "") == str(observed_target.get("ffmpeg_generation") or "")
        ):
            continue
        try:
            response = effect_contract.reconcile_delayed_effect(
                socket_path=Path(EFFECT_EXECUTOR_SOCKET),
                unresolved_scope=scope,
                runtime_observation=observation,
                transport={
                    "bytes_sent": int(metrics.get("bytes_sent", 0) or 0),
                    "network_down": bool(network.get("network_down")),
                    "tcp_probe_ok": bool(network.get("tcp_probe_ok")),
                },
            )
        except BaseException as exc:  # noqa: BLE001 - keep the scope unresolved and suppress action creation
            state["last_reason"] = f"effect reconciliation pending: {type(exc).__name__}"
            continue
        if response.get("ok") is not True or response.get("state") != "RECONCILED_EFFECT_OBSERVED":
            state["last_reason"] = f"effect reconciliation rejected: {response.get('reason', 'UNKNOWN')}"
            continue
        append_event(
            "recovery_completed",
            "delayed FFmpeg exit reconciled from executor ledger",
            {
                "recovery_action_id": scope.get("owner_request_id"),
                "effect_scope_id": scope.get("effect_scope_id"),
                "reconciliation_id": response.get("reconciliation_id"),
                "reconciliation_evidence_digest": response.get("evidence_digest"),
                "requested_ffmpeg_pid": int(before.get("ffmpeg_pid", 0) or 0),
                "requested_ffmpeg_generation": str(before.get("ffmpeg_generation") or ""),
                "ffmpeg_pid": ffmpeg_pid,
                "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
                "recovery_elapsed_sec": max(0, now_ts - parse_iso_ts(str(scope.get("created_at") or ""))),
                "same_idempotency_action_count": 1,
                "transport_snapshot": transport_snapshot,
                "youtube_hint": youtube_hint,
                "append_only_reconciliation": True,
                "automatic_retry_count": 0,
            },
        )
        reconciled_any = True
    return reconciled_any


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
        "last_gpu_restart_block_key": "",
        "last_gpu_restart_block_ts": 0,
        "last_startup_restart_block_key": "",
        "last_startup_restart_block_ts": 0,
        "last_tcp_send_sample_ts": 0,
        "last_tcp_send_sample_pid": 0,
        "last_tcp_send_sample_bytes_sent": 0,
        "observed_ts": 0,
        "connectivity_wait_active": False,
        "connectivity_wait_since_ts": 0,
        "connectivity_last_observed_ts": 0,
        "connectivity_recovered_ts": 0,
        "connectivity_gateway_present": False,
        "connectivity_gateway_ok": False,
        "connectivity_public_ok_count": 0,
        "connectivity_dns_ok": False,
        "connectivity_tcp_probe_ok": False,
        "restart_events": [],
        "pending_recovery_actions": [],
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
    state["observed_ts"] = int(time.time())
    recovery_state.save_state_file(STATE_FILE, state)


def get_main_pid(unit: str) -> int:
    cp = run(["systemctl", "show", unit, "--property=MainPID", "--value"])
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
    if EFFECT_EXECUTOR_SOCKET:
        observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
        pid = int(observation.get("protocol_ffmpeg_pid") or 0)
        return pid if bool(observation.get("ffmpeg_running")) and pid > 1 else 0
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
    if EFFECT_EXECUTOR_SOCKET:
        observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
        if int(observation.get("protocol_ffmpeg_pid") or 0) == pid:
            return max(0, int(observation.get("ffmpeg_uptime_sec") or 0))
        return 0
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
    if EFFECT_EXECUTOR_SOCKET:
        observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
        if int(observation.get("protocol_ffmpeg_pid") or 0) != ffmpeg_pid:
            return {}
        return tcp_send_sample.bind_runtime_metrics(
            observation, consumer_monotonic_ns=time.monotonic_ns()
        )
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


def audit_mp03(
    *,
    phase: str,
    operation: str,
    resource_identity: str,
    correlation_id: str,
    in_flight_evidence: dict[str, Any],
    generation_evidence: dict[str, Any],
    actual_production_decision: str,
    actual_production_result: str = "",
) -> None:
    """Emit R1 audit/P2-disabled evidence without returning a branch signal."""

    resolved_generation_evidence = dict(generation_evidence)
    native_token = str(
        resolved_generation_evidence.get("recovery_action_id")
        or resolved_generation_evidence.get("native_token")
        or ""
    )
    resolved_generation_evidence.update(
        {
            "native_token": native_token,
            "native_token_kind": "recovery_action_id" if resolved_generation_evidence.get("recovery_action_id") else "audit_loop_id",
            "monotonic": False,
            "restart_persistent": bool(resolved_generation_evidence.get("recovery_action_id")),
            "stale_action_detection_suitable": False,
            "maintenance_generation_mapping": "MISSING",
        }
    )
    try:
        audit_maintenance_decision(
            path_id="MP-03",
            phase=phase,
            operation=operation,
            path_role="NORMAL_MUTATOR",
            process_service="fast-recovery-loop",
            resource_identity=resource_identity,
            correlation_id=correlation_id,
            in_flight_evidence=in_flight_evidence,
            generation_evidence=resolved_generation_evidence,
            bind_source_target=True,
            p2_disabled_evaluation=True,
            native_operation_id=correlation_id,
            native_operation_generation="",
            actual_production_decision=actual_production_decision,
            actual_production_result=actual_production_result,
        )
    except BaseException:  # noqa: BLE001 - R1 evidence must never alter legacy recovery
        return


def restart_stream(reason: str, *, correlation_id: str = "") -> tuple[bool, str]:
    try:
        from stream_core.supervisor.factory import build_runtime_supervisor

        from .fast_recovery_core import executor as recovery_executor
        from .systemctl_control import run_systemctl
    except ImportError:
        from fast_recovery_core import executor as recovery_executor
        from systemctl_control import run_systemctl

        from stream_core.supervisor.factory import build_runtime_supervisor

    audit_mp03(
        phase="EFFECT_BOUNDARY",
        operation="restart_runtime",
        resource_identity=STREAM_SERVICE,
        correlation_id=correlation_id,
        in_flight_evidence={"status": "PROPOSED", "count": 1, "source": "fast recovery call stack and recovery event"},
        generation_evidence={"status": "PROPOSED", "recovery_action_id": correlation_id},
        actual_production_decision="LEGACY_EFFECT_CALL_PROCEEDS",
    )
    result = recovery_executor.restart_stream(
        stream_service=STREAM_SERVICE,
        reason=reason,
        run_systemctl=run_systemctl,
        log=log,
        supervisor=build_runtime_supervisor(
            run_systemctl=lambda args, check: run_systemctl(args, require_privilege=True, check=check),
        ),
    )
    audit_mp03(
        phase="EFFECT_RETURNED",
        operation="restart_runtime",
        resource_identity=STREAM_SERVICE,
        correlation_id=correlation_id,
        in_flight_evidence={"status": "PROPOSED", "count": 0, "source": "legacy executor returned"},
        generation_evidence={"status": "PROPOSED", "recovery_action_id": correlation_id},
        actual_production_decision="LEGACY_EFFECT_RETURNED",
        actual_production_result="SUCCESS" if result[0] else "FAILURE",
    )
    return result


def restart_ffmpeg_child(ffmpeg_pid: int, reason: str, *, correlation_id: str = "") -> tuple[bool, str]:
    if EFFECT_EXECUTOR_SOCKET:
        try:
            paths = effect_contract.configured_paths()
            response = effect_contract.execute_effect_request(
                socket_path=paths["socket"],
                runtime_observation_path=paths["runtime_observation"],
                producer_id=EFFECT_PRODUCER_ID,
                producer_generation=EFFECT_PRODUCER_GENERATION,
                reason=reason,
                correlation_id=correlation_id,
            )
        except BaseException as exc:  # noqa: BLE001 - failure is returned to legacy policy
            return False, f"effect executor unavailable: {type(exc).__name__}"
        detail = f"{response.get('state', 'UNKNOWN')}: {response.get('reason', 'UNKNOWN')}"
        return bool(response.get("ok")), detail

    try:
        from .fast_recovery_core import executor as recovery_executor
    except ImportError:
        from fast_recovery_core import executor as recovery_executor

    def audit_immediately_before_signal(_pid: int, _signal: int) -> None:
        audit_mp03(
            phase="EFFECT_BOUNDARY",
            operation="restart_ffmpeg",
            resource_identity=f"ffmpeg/pid/{ffmpeg_pid}",
            correlation_id=correlation_id,
            in_flight_evidence={
                "status": "PROPOSED",
                "count": 1,
                "source": "fast recovery call stack and recovery event",
            },
            generation_evidence={
                "status": "PROPOSED",
                "recovery_action_id": correlation_id,
                "ffmpeg_pid": ffmpeg_pid,
            },
            actual_production_decision="LEGACY_EFFECT_CALL_PROCEEDS",
        )

    result = recovery_executor.restart_ffmpeg_child(
        ffmpeg_pid=ffmpeg_pid,
        reason=reason,
        log=log,
        before_signal=audit_immediately_before_signal,
    )
    audit_mp03(
        phase="EFFECT_RETURNED",
        operation="restart_ffmpeg",
        resource_identity=f"ffmpeg/pid/{ffmpeg_pid}",
        correlation_id=correlation_id,
        in_flight_evidence={"status": "PROPOSED", "count": 0, "source": "legacy executor returned"},
        generation_evidence={"status": "PROPOSED", "recovery_action_id": correlation_id, "ffmpeg_pid": ffmpeg_pid},
        actual_production_decision="LEGACY_EFFECT_RETURNED",
        actual_production_result="SUCCESS" if result[0] else "FAILURE",
    )
    return result


def k8s_supervisor_active() -> bool:
    if EFFECT_EXECUTOR_SOCKET:
        return True
    return env("STREAM_RUNTIME_SUPERVISOR", "systemd").strip().lower() in {"k8s", "k3s", "kubernetes"}


def current_network_observation() -> recovery_decision.NetworkObservation:
    gateway = get_default_gateway()
    gateway_ok = ping_ok(gateway) if gateway else False
    public_ok_count = sum(1 for target in PUBLIC_PING_TARGETS if ping_ok(target))
    dns_probe_ok = dns_ok(DNS_HOST)
    tcp_probe = tcp_probe_ok(RTMP_HOST, RTMP_PORTS)
    return recovery_decision.network_observation(
        gateway=gateway,
        gateway_ok=gateway_ok,
        public_ok_count=public_ok_count,
        dns_ok=dns_probe_ok,
        tcp_probe_ok=tcp_probe,
    )


def mark_connectivity_wait(
    state: dict[str, Any],
    *,
    now_ts: int,
    network: recovery_decision.NetworkObservation,
    ffmpeg_pid: int,
) -> None:
    connectivity_policy.mark_wait(
        state,
        now_ts=now_ts,
        network=network,
        ffmpeg_pid=ffmpeg_pid,
        append_event=append_event,
    )


def clear_connectivity_wait(
    state: dict[str, Any],
    *,
    now_ts: int,
    network: recovery_decision.NetworkObservation | None = None,
) -> None:
    connectivity_policy.clear_wait(
        state,
        now_ts=now_ts,
        append_event=append_event,
        network=network,
    )


def execute_recovery_action_with_policy(
    *,
    reason_kind: str,
    reason: str,
    ffmpeg_pid: int,
    correlation_id: str = "",
) -> tuple[bool, str, str, bool | None]:
    if EFFECT_EXECUTOR_SOCKET:
        observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
        decision = recovery_policy.select_recovery_intent(reason_kind, runtime_observation=observation)
        if decision.intent_type == recovery_policy.NO_ACTION:
            return False, f"typed policy no action: {decision.decision_reason}", "none", None
        paths = effect_contract.configured_paths()
        socket_path = paths["socket"]
        observation_path = paths["runtime_observation"]
        if decision.intent_type == recovery_policy.ESCALATE_RUNTIME_RECOVERY:
            socket_path = paths["escalation_socket"]
            observation_path = paths["escalation_observation"]
        try:
            response = effect_contract.execute_typed_effect_request(
                socket_path=socket_path,
                runtime_observation_path=observation_path,
                producer_id=EFFECT_PRODUCER_ID,
                producer_generation=EFFECT_PRODUCER_GENERATION,
                intent_type=decision.intent_type,
                failure_domain=decision.failure_domain,
                reason=reason,
                correlation_id=correlation_id,
            )
        except BaseException as exc:  # noqa: BLE001 - typed executor failure stays inside legacy policy
            return False, f"typed effect executor unavailable: {type(exc).__name__}", decision.effect_scope, None
        detail = f"{response.get('state', 'UNKNOWN')}: {response.get('reason', 'UNKNOWN')}"
        automatic_retry = response.get("automatic_retry")
        return (
            bool(response.get("ok")),
            detail,
            decision.effect_scope,
            automatic_retry if isinstance(automatic_retry, bool) else None,
        )
    operation = "restart_ffmpeg" if k8s_supervisor_active() and reason_kind in {"tcp_stall", "remote_warning"} else "restart_runtime"
    audit_mp03(
        phase="ADMISSION",
        operation=operation,
        resource_identity=f"ffmpeg/pid/{ffmpeg_pid}" if operation == "restart_ffmpeg" else STREAM_SERVICE,
        correlation_id=correlation_id,
        in_flight_evidence={"status": "PROPOSED", "count": 1, "source": "accepted legacy recovery call stack"},
        generation_evidence={"status": "PROPOSED", "recovery_action_id": correlation_id, "ffmpeg_pid": ffmpeg_pid},
        actual_production_decision="LEGACY_RECOVERY_ADMITTED",
    )
    if k8s_supervisor_active() and reason_kind in {"tcp_stall", "remote_warning"}:
        if correlation_id:
            ok, detail = restart_ffmpeg_child(ffmpeg_pid, reason, correlation_id=correlation_id)
        else:
            ok, detail = restart_ffmpeg_child(ffmpeg_pid, reason)
        return ok, detail, "ffmpeg_child", None
    if correlation_id:
        ok, detail = restart_stream(reason, correlation_id=correlation_id)
    else:
        ok, detail = restart_stream(reason)
    return ok, detail, "runtime", None


def execute_recovery_action(
    *,
    reason_kind: str,
    reason: str,
    ffmpeg_pid: int,
    correlation_id: str = "",
) -> tuple[bool, str, str]:
    ok, detail, scope, _automatic_retry = execute_recovery_action_with_policy(
        reason_kind=reason_kind,
        reason=reason,
        ffmpeg_pid=ffmpeg_pid,
        correlation_id=correlation_id,
    )
    return ok, detail, scope


def runtime_gpu_restart_block() -> dict[str, Any]:
    if EFFECT_EXECUTOR_SOCKET:
        observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
        if not observation:
            return {
                "available": False,
                "status": "runtime_observation_unavailable",
                "restart_blocked": False,
                "error": "effect executor performs target validation",
            }
        return {
            "available": True,
            "status": "stream_engine_owner_active" if observation.get("ffmpeg_running") else "ffmpeg_missing",
            "restart_blocked": False,
            "stream_engine_running": True,
            "effect_executor_revalidates_target": True,
        }
    if not GPU_PREFLIGHT_ENABLED or not k8s_supervisor_active():
        return {}
    try:
        from .fast_recovery_core import legacy_preflight
    except ImportError:
        try:
            from fast_recovery_core import legacy_preflight
        except ImportError:
            return {
                "available": False,
                "status": "gpu_preflight_module_unavailable",
                "restart_blocked": True,
                "error": "legacy GPU preflight capability is unavailable",
            }
    return legacy_preflight.runtime_gpu_restart_block(
        kubectl=env("STREAM_KUBECTL_BIN", "kubectl"),
        namespace=env("STREAM_K8S_NAMESPACE", "stream-v3"),
        deployment=GPU_PREFLIGHT_DEPLOYMENT,
        selector=GPU_PREFLIGHT_SELECTOR,
        container_name=GPU_PREFLIGHT_CONTAINER,
        timeout_sec=GPU_PREFLIGHT_TIMEOUT_SEC,
    )


def maybe_record_gpu_restart_block(
    state: dict[str, Any],
    *,
    trigger: str,
    reason: str,
    gpu_status: dict[str, Any],
) -> None:
    now_ts = int(time.time())
    block_key = f"{trigger}:{gpu_status.get('status')}:{gpu_status.get('restart_block_reason')}"
    last_key = str(state.get("last_gpu_restart_block_key", ""))
    last_ts = int(state.get("last_gpu_restart_block_ts", 0) or 0)
    if block_key != last_key or now_ts - last_ts >= 60:
        append_event(
            "restart_blocked_gpu",
            str(gpu_status.get("restart_block_reason") or gpu_status.get("status") or "gpu preflight blocked restart"),
            {
                "trigger": trigger,
                "reason": reason,
                "gpu_status": gpu_status,
            },
        )
        state["last_gpu_restart_block_key"] = block_key
        state["last_gpu_restart_block_ts"] = now_ts


def block_restart_if_gpu_preflight_fails(
    state: dict[str, Any],
    *,
    trigger: str,
    reason: str,
) -> bool:
    gpu_status = runtime_gpu_restart_block()
    if not gpu_status.get("restart_blocked"):
        return False
    maybe_record_gpu_restart_block(state, trigger=trigger, reason=reason, gpu_status=gpu_status)
    detail = str(gpu_status.get("restart_block_reason") or gpu_status.get("status") or "gpu preflight")
    state["last_reason"] = f"restart blocked by GPU preflight: {detail}"
    return True


def current_stream_establishment() -> dict[str, Any]:
    if EFFECT_EXECUTOR_SOCKET:
        observation = effect_contract.read_runtime_observation(RUNTIME_OBSERVATION_FILE)
        if not observation:
            return {"established": False, "reason": "runtime observation unavailable"}
        return {
            "established": bool(observation.get("stream_established")),
            "reason": (
                "stream-engine reports established FFmpeg delivery"
                if observation.get("stream_established")
                else "stream-engine has not established FFmpeg delivery"
            ),
            "pod_uid": str(observation.get("pod_uid") or ""),
            "pod_name": str(observation.get("pod_name") or ""),
        }
    if not FFMPEG_MISSING_REQUIRE_CURRENT_POD_ESTABLISHED or not k8s_supervisor_active():
        return {"established": True, "reason": "current Pod establishment gate disabled"}
    try:
        from .fast_recovery_core import legacy_preflight
    except ImportError:
        try:
            from fast_recovery_core import legacy_preflight
        except ImportError:
            return {"established": False, "reason": "legacy runtime readiness capability unavailable"}
    return legacy_preflight.stream_establishment(
        BOOT_ESTABLISHED_FILE,
        pod_uid=env("STREAM_V3_POD_UID"),
        pod_name=env("STREAM_V3_POD_NAME"),
    )


def block_ffmpeg_missing_restart_before_established(state: dict[str, Any], *, reason: str) -> bool:
    establishment = current_stream_establishment()
    if establishment.get("established"):
        return False
    now_ts = int(time.time())
    detail = str(establishment.get("reason") or "current Pod has not established streaming")
    block_key = f"ffmpeg_missing:{detail}"
    last_key = str(state.get("last_startup_restart_block_key", ""))
    last_ts = int(state.get("last_startup_restart_block_ts", 0) or 0)
    if block_key != last_key or now_ts - last_ts >= 60:
        append_event(
            "restart_blocked_startup",
            detail,
            {
                "trigger": "ffmpeg_missing",
                "reason": reason,
                "establishment": establishment,
            },
        )
        state["last_startup_restart_block_key"] = block_key
        state["last_startup_restart_block_ts"] = now_ts
    state["last_reason"] = f"restart blocked before current Pod established streaming: {detail}"
    return True


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
    if EFFECT_EXECUTOR_SOCKET:
        payload = tcp_send_sample.source_timed_sample(
            state, ffmpeg_pid=ffmpeg_pid, bytes_sent=bytes_sent,
            metrics=metrics, interval_seconds=TCP_SEND_SAMPLE_LOG_SEC,
        )
        if payload is not None:
            append_event("tcp_send_sample", "ffmpeg tcp send sample", payload)
        return
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
            "bytes_acked": int(metrics.get("bytes_acked", 0) or 0),
            "send_q": int(metrics.get("send_q", 0) or 0),
            "notsent": int(metrics.get("notsent", 0) or 0),
            "unacked": int(metrics.get("unacked", 0) or 0),
            "lastsnd_ms": int(metrics.get("lastsnd_ms", 0) or 0),
            "rto_ms": int(metrics.get("rto_ms", 0) or 0),
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


def validate_controller_runtime_contract() -> None:
    enforcement_enabled = env("MAINTENANCE_ENFORCEMENT_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
    if enforcement_enabled:
        raise RuntimeError("P2_ENFORCEMENT_NOT_AUTHORIZED")
    if CONTROLLER_RUNTIME_MODE == "SHADOW_OBSERVER_ONLY":
        if EFFECT_AUTHORITY_MODE != "SHADOW_ONLY":
            raise RuntimeError("SHADOW_CONTROLLER_AUTHORITY_MODE_INVALID")
        if GPU_PREFLIGHT_ENABLED:
            raise RuntimeError("SHADOW_CONTROLLER_GPU_PREFLIGHT_CAPABILITY_NOT_ALLOWED")
        return
    if CONTROLLER_RUNTIME_MODE == "NARROW_EXECUTOR_ONLY":
        if not EFFECT_EXECUTOR_SOCKET:
            raise RuntimeError("EFFECT_EXECUTOR_SOCKET_REQUIRED")
        if EFFECT_AUTHORITY_MODE != "AUTO_FENCED":
            raise RuntimeError("AUTO_FENCED_AUTHORITY_REQUIRED")


def main() -> int:
    validate_controller_runtime_contract()
    now_ts = int(time.time())
    state = load_state(now_ts)
    last_restart_ts = int(state.get("last_restart_ts", 0) or 0)

    executor_unresolved = read_executor_unresolved_scopes(state)
    if executor_unresolved is None:
        save_state(state)
        return 0

    main_pid = get_main_pid(STREAM_SERVICE)
    ffmpeg_pid = get_stream_ffmpeg_pid(main_pid)
    ffmpeg_uptime_sec = get_process_elapsed_sec(ffmpeg_pid)
    loop_correlation_id = f"mp03-loop-{now_ts}-{uuid.uuid4().hex[:12]}"
    audit_mp03(
        phase="OBSERVATION",
        operation="observe_fast_recovery_loop",
        resource_identity=f"ffmpeg/pid/{ffmpeg_pid}" if ffmpeg_pid > 1 else STREAM_SERVICE,
        correlation_id=loop_correlation_id,
        in_flight_evidence={"status": "PROPOSED", "count": 0, "source": "fast recovery loop call stack"},
        generation_evidence={
            "status": "PROPOSED",
            "native_scope": "process-local loop observation",
            "native_token": loop_correlation_id,
            "ffmpeg_pid": ffmpeg_pid,
        },
        actual_production_decision="LEGACY_LOOP_EVALUATION_CONTINUES",
    )

    if ffmpeg_pid <= 1 and (executor_unresolved or pending_recovery_count(state) > 0):
        state["last_reason"] = "effect outcome unresolved; automatic retry suppressed until reconciliation"
        state["last_pid"] = 0
        state["last_bytes_sent"] = 0
        state["last_tcp_send_sample_pid"] = 0
        state["last_tcp_send_sample_bytes_sent"] = 0
        save_state(state)
        return 0

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
        if state.get("connectivity_wait_active") is True or should_restart_missing:
            missing_network = current_network_observation()
            if missing_network.network_down:
                mark_connectivity_wait(
                    state,
                    now_ts=now_ts,
                    network=missing_network,
                    ffmpeg_pid=0,
                )
                state.update(
                    {
                        "last_pid": 0,
                        "last_bytes_sent": 0,
                        "last_bytes_sent_ts": 0,
                        "last_tcp_send_sample_pid": 0,
                        "last_tcp_send_sample_bytes_sent": 0,
                    }
                )
                save_state(state)
                return 0
            clear_connectivity_wait(state, now_ts=now_ts, network=missing_network)
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
            if block_ffmpeg_missing_restart_before_established(state, reason=reason):
                save_state(state)
                return 0
            if block_restart_if_gpu_preflight_fails(state, trigger="ffmpeg_missing", reason=reason):
                save_state(state)
                return 0
            typed_decision = record_typed_policy_decision("ffmpeg_missing")
            if EFFECT_EXECUTOR_SOCKET and typed_decision.intent_type == recovery_policy.NO_ACTION:
                state["last_reason"] = f"typed policy no action: {typed_decision.decision_reason}"
                save_state(state)
                return 0
            recovery_scope = planned_recovery_scope("ffmpeg_missing")
            active_producer = effect_authority_active()
            recovery_action = new_recovery_action(
                now_ts=now_ts,
                reason_kind="ffmpeg_missing",
                reason_first_ts=missing_first_ts,
                ffmpeg_pid=0,
                recovery_scope=recovery_scope,
                execute=active_producer,
            )
            restart_context = write_restart_reason(
                reason_kind="ffmpeg_missing",
                reason=reason,
                now_ts=now_ts,
                ffmpeg_pid=0,
                ffmpeg_uptime_sec=0,
                metrics=None,
                recovery_action=recovery_action,
            )
            append_recovery_requested(
                action=recovery_action,
                reason_kind="ffmpeg_missing",
                reason=reason,
                ffmpeg_pid=0,
                ffmpeg_uptime_sec=0,
                metrics=None,
            )
            correlation_id = str(recovery_action.get("recovery_action_id") or "")
            planned_operation = (
                typed_decision.intent_type.lower() if EFFECT_EXECUTOR_SOCKET else "restart_runtime"
            )
            audit_mp03(
                phase="ACTION_PLAN_CREATED",
                operation=planned_operation,
                resource_identity="stream-engine/ffmpeg" if EFFECT_EXECUTOR_SOCKET else STREAM_SERVICE,
                correlation_id=correlation_id,
                in_flight_evidence={"status": "PROPOSED", "count": 0, "source": "ffmpeg-missing action plan"},
                generation_evidence={"status": "PROPOSED", "recovery_action_id": correlation_id, "ffmpeg_pid": 0},
                actual_production_decision="LEGACY_ACTION_PLAN_CREATED",
            )
            if not active_producer:
                restart_ok = True
                restart_detail = "shadow producer inactive; physical effect skipped"
                dispatch_scope = recovery_scope
                automatic_retry = None
            else:
                restart_ok, restart_detail, dispatch_scope, automatic_retry = execute_recovery_action_with_policy(
                    reason_kind="ffmpeg_missing",
                    reason=reason,
                    ffmpeg_pid=0,
                    correlation_id=correlation_id,
                )
            append_recovery_dispatch_result(
                action={**recovery_action, "recovery_scope": dispatch_scope},
                reason_kind="ffmpeg_missing",
                reason=reason,
                ffmpeg_pid=0,
                ok=restart_ok,
                detail=restart_detail,
                automatic_retry=automatic_retry,
            )
            append_event(
                "shadow_recovery_candidate"
                if not active_producer
                else ("restart" if restart_ok else "restart_failed"),
                reason,
                {
                    **recovery_action,
                    "trigger": "ffmpeg_missing",
                    "missing_for_sec": missing_for,
                    "detail": restart_detail,
                    "restart_context": restart_context,
                    "physical_effect_count": 0 if not active_producer else None,
                },
            )
            if restart_ok:
                if active_producer:
                    remember_pending_recovery(
                        state,
                        action=recovery_action,
                        now_ts=now_ts,
                        reason_kind="ffmpeg_missing",
                        reason=reason,
                        ffmpeg_pid=0,
                    )
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
                        "last_reason": (
                            "restarted: ffmpeg pid missing"
                            if active_producer
                            else "shadow would restart: ffmpeg pid missing"
                        ),
                        "ffmpeg_missing_first_ts": 0,
                        "ffmpeg_missing_success_backoff_until": now_ts + FFMPEG_MISSING_SUCCESS_BACKOFF_SEC,
                        "restart_failure_count": 0,
                    }
                )
            elif automatic_retry is False:
                record_unresolved_recovery_dispatch(
                    state,
                    action={**recovery_action, "recovery_scope": dispatch_scope},
                    now_ts=now_ts,
                    reason_kind="ffmpeg_missing",
                    reason=reason,
                    ffmpeg_pid=0,
                    detail=restart_detail,
                    restart_events=restart_events,
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
        record_typed_policy_decision("ffmpeg_missing")
        save_state(state)
        return 0
    state["ffmpeg_missing_first_ts"] = 0

    if ffmpeg_uptime_sec < MIN_FFMPEG_UPTIME_SEC:
        if state.get("connectivity_wait_active") is True:
            warmup_network = current_network_observation()
            if warmup_network.network_down:
                mark_connectivity_wait(
                    state,
                    now_ts=now_ts,
                    network=warmup_network,
                    ffmpeg_pid=ffmpeg_pid,
                )
            else:
                clear_connectivity_wait(state, now_ts=now_ts, network=warmup_network)
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
        record_typed_policy_decision("startup_transient")
        save_state(state)
        return 0

    metrics = parse_ffmpeg_tcp_metrics(ffmpeg_pid, RTMP_PORTS)
    recovery_decision.reset_pid_dependent_state(state, ffmpeg_pid)

    network = current_network_observation()
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
    transport_snapshot = write_transport_snapshot(
        now_ts=now_ts,
        ffmpeg_pid=ffmpeg_pid,
        ffmpeg_uptime_sec=ffmpeg_uptime_sec,
        tcp=tcp,
        network=network,
        remote_warning=remote_warning,
    )
    youtube_hint = recovery_decision.youtube_hint(ytw_payload)
    if EFFECT_EXECUTOR_SOCKET:
        reconciled_delayed_effect = maybe_reconcile_delayed_executor_effects(
            state,
            unresolved_scopes=executor_unresolved,
            now_ts=now_ts,
            ffmpeg_pid=ffmpeg_pid,
            ffmpeg_uptime_sec=ffmpeg_uptime_sec,
            transport_snapshot=transport_snapshot,
            youtube_hint=youtube_hint,
        )
        refreshed_unresolved = read_executor_unresolved_scopes(state)
        if refreshed_unresolved is None:
            save_state(state)
            return 0
        executor_unresolved = refreshed_unresolved
        if reconciled_delayed_effect:
            recovery_decision.mark_latest_transport_sample(
                state,
                ffmpeg_pid=ffmpeg_pid,
                bytes_sent=tcp.bytes_sent,
                now_ts=now_ts,
                last_reason="delayed effect reconciled; candidate evaluation deferred to next cycle",
            )
            save_state(state)
            return 0
    else:
        maybe_record_recovery_completed(
            state,
            now_ts=now_ts,
            ffmpeg_pid=ffmpeg_pid,
            ffmpeg_uptime_sec=ffmpeg_uptime_sec,
            transport_snapshot=transport_snapshot,
            youtube_hint=youtube_hint,
        )
    if executor_unresolved or pending_recovery_count(state) > 0:
        recovery_decision.mark_latest_transport_sample(
            state,
            ffmpeg_pid=ffmpeg_pid,
            bytes_sent=tcp.bytes_sent,
            now_ts=now_ts,
            last_reason="effect outcome unresolved; automatic retry suppressed until reconciliation",
        )
        save_state(state)
        return 0

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

    if network.network_down:
        record_typed_policy_decision("network_down")
        mark_connectivity_wait(
            state,
            now_ts=now_ts,
            network=network,
            ffmpeg_pid=ffmpeg_pid,
        )
        state["remote_warning_streak"] = 0
        recovery_decision.mark_latest_transport_sample(
            state,
            ffmpeg_pid=ffmpeg_pid,
            bytes_sent=tcp.bytes_sent,
            now_ts=now_ts,
        )
        save_state(state)
        return 0
    clear_connectivity_wait(state, now_ts=now_ts, network=network)

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
        if block_restart_if_gpu_preflight_fails(state, trigger=reason_kind or "unknown", reason=reason):
            recovery_decision.mark_latest_transport_sample(
                state,
                ffmpeg_pid=ffmpeg_pid,
                bytes_sent=tcp.bytes_sent,
                now_ts=now_ts,
            )
            save_state(state)
            return 0
        recovery_scope = planned_recovery_scope(reason_kind)
        typed_decision = record_typed_policy_decision(reason_kind)
        active_producer = effect_authority_active()
        recovery_action = new_recovery_action(
            now_ts=now_ts,
            reason_kind=reason_kind,
            reason_first_ts=restart_reason.first_ts,
            ffmpeg_pid=ffmpeg_pid,
            recovery_scope=recovery_scope,
            execute=active_producer,
        )
        restart_context = write_restart_reason(
            reason_kind=reason_kind,
            reason=reason,
            now_ts=now_ts,
            ffmpeg_pid=ffmpeg_pid,
            ffmpeg_uptime_sec=ffmpeg_uptime_sec,
            metrics=restart_metrics,
            recovery_action=recovery_action,
        )
        append_recovery_requested(
            action=recovery_action,
            reason_kind=reason_kind,
            reason=reason,
            ffmpeg_pid=ffmpeg_pid,
            ffmpeg_uptime_sec=ffmpeg_uptime_sec,
            metrics=restart_metrics,
        )
        correlation_id = str(recovery_action.get("recovery_action_id") or "")
        planned_operation = (
            typed_decision.intent_type.lower()
            if EFFECT_EXECUTOR_SOCKET
            else (
                "restart_ffmpeg"
                if k8s_supervisor_active() and reason_kind in {"tcp_stall", "remote_warning"}
                else "restart_runtime"
            )
        )
        audit_mp03(
            phase="ACTION_PLAN_CREATED",
            operation=planned_operation,
            resource_identity=f"ffmpeg/pid/{ffmpeg_pid}" if planned_operation == "restart_ffmpeg" else STREAM_SERVICE,
            correlation_id=correlation_id,
            in_flight_evidence={"status": "PROPOSED", "count": 0, "source": "legacy recovery action plan"},
            generation_evidence={"status": "PROPOSED", "recovery_action_id": correlation_id, "ffmpeg_pid": ffmpeg_pid},
            actual_production_decision="LEGACY_ACTION_PLAN_CREATED",
        )
        if not active_producer:
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
            state["last_reason"] = f"shadow would execute: {reason}"
            append_event(
                "shadow_recovery_candidate",
                reason,
                {
                    **recovery_action,
                    "trigger": reason_kind,
                    "ffmpeg_pid": ffmpeg_pid,
                    "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
                    "metrics": restart_metrics,
                    "physical_effect_count": 0,
                    "effect_authority_mode": EFFECT_AUTHORITY_MODE,
                    "active_producer": False,
                },
            )
            save_state(state)
            return 0
        restart_ok, restart_detail, recovery_scope, automatic_retry = execute_recovery_action_with_policy(
            reason_kind=reason_kind,
            reason=reason,
            ffmpeg_pid=ffmpeg_pid,
            correlation_id=correlation_id,
        )
        append_recovery_dispatch_result(
            action={**recovery_action, "recovery_scope": recovery_scope},
            reason_kind=reason_kind,
            reason=reason,
            ffmpeg_pid=ffmpeg_pid,
            ok=restart_ok,
            detail=restart_detail,
            automatic_retry=automatic_retry,
        )
        if restart_ok:
            remember_pending_recovery(
                state,
                action={**recovery_action, "recovery_scope": recovery_scope},
                now_ts=now_ts,
                reason_kind=reason_kind,
                reason=reason,
                ffmpeg_pid=ffmpeg_pid,
            )
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
                    **recovery_action,
                    "trigger": reason_kind,
                    "ffmpeg_pid": ffmpeg_pid,
                    "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
                    "metrics": restart_metrics,
                    "restart_context": restart_context,
                    "recovery_scope": recovery_scope,
                    "youtube_hint": recovery_decision.youtube_hint(ytw_payload),
                },
            )
            save_state(state)
            return 0
        if automatic_retry is False:
            record_unresolved_recovery_dispatch(
                state,
                action={**recovery_action, "recovery_scope": recovery_scope},
                now_ts=now_ts,
                reason_kind=reason_kind,
                reason=reason,
                ffmpeg_pid=ffmpeg_pid,
                detail=restart_detail,
                restart_events=restart_events,
            )
            recovery_decision.mark_latest_transport_sample(
                state,
                ffmpeg_pid=ffmpeg_pid,
                bytes_sent=tcp.bytes_sent,
                now_ts=now_ts,
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
                **recovery_action,
                "trigger": reason_kind,
                "ffmpeg_pid": ffmpeg_pid,
                "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
                "restart_failure_count": restart_failure_count,
                "backoff_sec": RESTART_FAILURE_BACKOFF_SEC,
                "detail": restart_detail,
                "recovery_scope": recovery_scope,
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
    record_typed_policy_decision("healthy")
    save_state(state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
