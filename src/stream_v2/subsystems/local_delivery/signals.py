from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ...timeutil import age_seconds, parse_utc
from ..common import text, truthy
from . import actions


@dataclass(frozen=True)
class LocalDeliverySignals:
    ffmpeg_alive: bool
    ingest_connected: bool
    runtime_fresh: bool
    stream_watchdog_ok: bool
    tcp_send_healthy: bool
    tcp_stall_recent: bool
    stream_engine_recent: bool
    runtime_age_sec: float | None
    ffmpeg_count: int
    tcp_bytes_sent_delta: float | None
    tcp_mbps: float | None
    tcp_notsent: int | None
    tcp_unacked: int | None
    tcp_lastsnd_ms: int | None
    tcp_conn_established: bool
    fast_recovery_kind: str
    fast_recovery_trigger: str
    fast_recovery_age_sec: float | None
    fast_recovery_restart_trigger: str
    fast_recovery_restart_age_sec: float | None
    fast_recovery_stream_restart_recent: bool
    stream_engine_event: str
    observed_ts_utc: str
    reason: str

    @property
    def has_healthy_signal(self) -> bool:
        return self.ffmpeg_alive and (self.ingest_connected or self.runtime_fresh or self.stream_watchdog_ok or self.tcp_send_healthy)

    def healthy_evidence_names(self) -> list[str]:
        names: list[str] = []
        if self.ffmpeg_alive:
            names.append("ffmpeg_alive")
        if self.ingest_connected:
            names.append("ingest_connected")
        if self.runtime_fresh:
            names.append("runtime_heartbeat_fresh")
        if self.stream_watchdog_ok:
            names.append("stream_watchdog_ok")
        if self.tcp_send_healthy:
            names.append("tcp_send_healthy")
        if self.stream_engine_recent:
            names.append("stream_engine_recent")
        return names

    def failure_names(self) -> list[str]:
        failures: list[str] = []
        if self.fast_recovery_stream_restart_recent:
            failure = actions.FAST_RECOVERY_STREAM_RESTART_FAILURE_BY_TRIGGER.get(self.fast_recovery_restart_trigger)
            if failure:
                failures.append(failure)
        if self.ffmpeg_count > 1:
            failures.append(actions.FAILURE_STREAM_FFMPEG_DUPLICATE)
        if not self.ffmpeg_alive and self.ffmpeg_count <= 0:
            failures.append(actions.FAILURE_FFMPEG_MISSING)
        if self.ffmpeg_alive and not self.ingest_connected:
            failures.append(actions.FAILURE_INGEST_DISCONNECTED)
        if self.runtime_age_sec is not None and self.runtime_age_sec > 120:
            failures.append(actions.FAILURE_RUNTIME_HEARTBEAT_STALE)
        if self.tcp_stall_recent:
            failures.append(actions.FAILURE_TCP_STALL)
        return failures


def collect_signals(
    *,
    ytw: dict[str, Any],
    stream_stats: dict[str, Any],
    runtime: dict[str, Any],
    fast_recovery: dict[str, Any],
    fast_recovery_restart: dict[str, Any],
    stream_engine_event: dict[str, Any],
    restart_reason: dict[str, Any],
    recovery_stage: dict[str, Any],
    now: datetime,
) -> LocalDeliverySignals:
    runtime_ts = parse_utc(runtime.get("updated_at_utc"))
    runtime_age = age_seconds(runtime_ts, now)
    runtime_fresh = runtime_age is not None and runtime_age <= 90 and text(runtime.get("status")).lower() == "running"
    ffmpeg_count = _int(stream_stats.get("ffmpeg_count"), default=0)
    ffmpeg_alive = truthy(ytw.get("ffmpeg_pid")) or truthy(runtime.get("ffmpeg_pid")) or ffmpeg_count > 0
    ingest_connected = truthy(ytw.get("ingest_connected"))
    stream_watchdog_ok = text(stream_stats.get("status")).lower() == "ok" and text(stream_stats.get("judgment")).lower() in {"ok", ""}

    fast_ts = parse_utc(fast_recovery.get("ts_utc"))
    fast_age = age_seconds(fast_ts, now)
    restart_event = fast_recovery_restart or fast_recovery
    restart_ts = parse_utc(restart_event.get("ts_utc"))
    restart_age = age_seconds(restart_ts, now)
    fast_context = fast_recovery.get("restart_context") if isinstance(fast_recovery.get("restart_context"), dict) else {}
    restart_context = restart_event.get("restart_context") if isinstance(restart_event.get("restart_context"), dict) else {}
    fast_kind = text(fast_recovery.get("kind")).lower()
    fast_trigger = text(fast_recovery.get("trigger") or fast_context.get("trigger")).lower()
    restart_kind = text(restart_event.get("kind")).lower()
    restart_trigger = text(restart_event.get("trigger") or restart_context.get("trigger")).lower()
    tcp = _tcp_metrics(fast_recovery)
    tcp_stall_recent = (
        (
            text(fast_recovery.get("trigger")).lower() == "tcp_stall"
            or text(fast_recovery.get("kind")).lower() == "tcp_stall"
            or (tcp["bytes_sent_delta"] == 0 and (_int(fast_recovery.get("notsent"), default=0) > 0 or _int(fast_recovery.get("unacked"), default=0) > 0))
        )
        and fast_age is not None
        and fast_age <= 180
    )
    tcp_send_healthy = (
        text(fast_recovery.get("kind")).lower() == "tcp_send_sample"
        and fast_age is not None
        and fast_age <= 180
        and (tcp["bytes_sent_delta"] or 0) > 0
        and tcp["conn_established"]
    )
    fast_recovery_stream_restart_recent = (
        restart_kind == "restart"
        and restart_trigger in actions.FAST_RECOVERY_STREAM_RESTART_FAILURE_BY_TRIGGER
        and restart_age is not None
        and restart_age <= 180
    )
    engine_ts = parse_utc(stream_engine_event.get("ts_utc"))
    engine_age = age_seconds(engine_ts, now) if engine_ts else None
    stream_engine_recent = bool(stream_engine_event) and engine_age is not None and engine_age <= 180

    return LocalDeliverySignals(
        ffmpeg_alive=ffmpeg_alive,
        ingest_connected=ingest_connected,
        runtime_fresh=runtime_fresh,
        stream_watchdog_ok=stream_watchdog_ok,
        tcp_send_healthy=tcp_send_healthy,
        tcp_stall_recent=tcp_stall_recent,
        stream_engine_recent=stream_engine_recent,
        runtime_age_sec=runtime_age,
        ffmpeg_count=ffmpeg_count,
        tcp_bytes_sent_delta=tcp["bytes_sent_delta"],
        tcp_mbps=tcp["mbps"],
        tcp_notsent=tcp["notsent"],
        tcp_unacked=tcp["unacked"],
        tcp_lastsnd_ms=tcp["lastsnd_ms"],
        tcp_conn_established=tcp["conn_established"],
        fast_recovery_kind=fast_kind,
        fast_recovery_trigger=fast_trigger,
        fast_recovery_age_sec=fast_age,
        fast_recovery_restart_trigger=restart_trigger,
        fast_recovery_restart_age_sec=restart_age,
        fast_recovery_stream_restart_recent=fast_recovery_stream_restart_recent,
        stream_engine_event=text(stream_engine_event.get("event") or stream_engine_event.get("event_type") or stream_engine_event.get("kind")),
        observed_ts_utc=text(runtime.get("updated_at_utc") or ytw.get("ts_utc") or stream_stats.get("ts_utc") or fast_recovery.get("ts_utc") or stream_engine_event.get("ts_utc")),
        reason=" ".join(part for part in [text(stream_stats.get("reason")), text(fast_recovery.get("message")), text(restart_reason.get("reason")), text(recovery_stage.get("reason"))] if part),
    )


def _int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any, *, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _tcp_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    conn = text(payload.get("conn") or payload.get("connection_state") or payload.get("state")).upper()
    return {
        "bytes_sent_delta": _float(_first_present(payload, metrics, "bytes_sent_delta", "bytes_delta"), default=None),
        "mbps": _float(_first_present(payload, metrics, "mbps", "send_mbps"), default=None),
        "notsent": _int(_first_present(payload, metrics, "notsent"), default=0),
        "unacked": _int(_first_present(payload, metrics, "unacked"), default=0),
        "lastsnd_ms": _int(_first_present(payload, metrics, "lastsnd_ms"), default=0),
        "conn_established": "ESTAB" in conn or "ESTABLISHED" in conn or truthy(payload.get("ingest_connected")),
    }


def _first_present(payload: dict[str, Any], metrics: dict[str, Any], *names: str) -> Any:
    for source in (payload, metrics):
        for name in names:
            if name in source and source[name] is not None:
                return source[name]
    return None
