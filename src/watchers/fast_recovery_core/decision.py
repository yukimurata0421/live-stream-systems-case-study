from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TcpObservation:
    metrics: dict[str, int | str]
    bytes_sent: int
    prev_bytes_sent: int
    prev_bytes_ts: int
    bytes_delta: int
    bytes_elapsed_sec: int
    send_mbps: float | None
    notsent: int
    unacked: int
    lastsnd_ms: int
    stall_now: bool
    low_upload_pressure_now: bool


@dataclass(frozen=True)
class NetworkObservation:
    gateway: str
    gateway_ok: bool
    public_ok_count: int
    dns_ok: bool
    tcp_probe_ok: bool
    network_down: bool


@dataclass(frozen=True)
class RestartReason:
    kind: str
    reason: str
    first_ts: int


def reset_pid_dependent_state(state: dict[str, Any], ffmpeg_pid: int) -> None:
    if int(state.get("last_pid", 0)) == ffmpeg_pid:
        return
    state["stall_streak"] = 0
    state["low_upload_pressure_streak"] = 0
    state["last_bytes_sent"] = 0
    state["last_bytes_sent_ts"] = 0


def tcp_observation(
    state: dict[str, Any],
    *,
    now_ts: int,
    metrics: dict[str, int | str],
    send_mbps_func,
    low_upload_pressure_func,
    stall_lastsnd_ms: int,
    stall_notsent_bytes: int,
    stall_unacked: int,
    low_upload_enabled: bool,
    low_upload_max_mbps: float,
    low_upload_notsent_bytes: int,
    low_upload_unacked: int,
    low_upload_lastsnd_ms: int,
    network_down: bool,
    tcp_probe: bool,
) -> TcpObservation:
    bytes_sent = int(metrics.get("bytes_sent", 0))
    notsent = int(metrics.get("notsent", 0))
    unacked = int(metrics.get("unacked", 0))
    lastsnd_ms = int(metrics.get("lastsnd_ms", 0))
    prev_bytes_sent = int(state.get("last_bytes_sent", 0) or 0)
    prev_bytes_ts = int(state.get("last_bytes_sent_ts", 0) or 0)
    bytes_delta = max(0, bytes_sent - prev_bytes_sent)
    bytes_elapsed_sec = max(0, now_ts - prev_bytes_ts) if prev_bytes_ts > 0 else 0
    send_mbps = send_mbps_func(bytes_delta=bytes_delta, elapsed_sec=bytes_elapsed_sec)
    stall_now = bool(metrics) and (bytes_delta == 0) and lastsnd_ms >= stall_lastsnd_ms and (
        notsent >= stall_notsent_bytes or unacked >= stall_unacked
    )
    low_upload_pressure_now = low_upload_pressure_func(
        enabled=low_upload_enabled,
        metrics=metrics,
        bytes_elapsed_sec=bytes_elapsed_sec,
        send_mbps_value=send_mbps,
        max_mbps=low_upload_max_mbps,
        notsent=notsent,
        unacked=unacked,
        lastsnd_ms=lastsnd_ms,
        notsent_threshold=low_upload_notsent_bytes,
        unacked_threshold=low_upload_unacked,
        lastsnd_ms_threshold=low_upload_lastsnd_ms,
        network_down=network_down,
        tcp_probe=tcp_probe,
        stall_now=stall_now,
    )
    return TcpObservation(
        metrics=metrics,
        bytes_sent=bytes_sent,
        prev_bytes_sent=prev_bytes_sent,
        prev_bytes_ts=prev_bytes_ts,
        bytes_delta=bytes_delta,
        bytes_elapsed_sec=bytes_elapsed_sec,
        send_mbps=send_mbps,
        notsent=notsent,
        unacked=unacked,
        lastsnd_ms=lastsnd_ms,
        stall_now=stall_now,
        low_upload_pressure_now=low_upload_pressure_now,
    )


def network_observation(
    *,
    gateway: str,
    gateway_ok: bool,
    public_ok_count: int,
    dns_ok: bool,
    tcp_probe_ok: bool,
) -> NetworkObservation:
    network_down = (not dns_ok and not tcp_probe_ok) or (
        (not gateway_ok) and public_ok_count == 0 and (not tcp_probe_ok)
    )
    return NetworkObservation(
        gateway=gateway,
        gateway_ok=gateway_ok,
        public_ok_count=public_ok_count,
        dns_ok=dns_ok,
        tcp_probe_ok=tcp_probe_ok,
        network_down=network_down,
    )


def update_streak(state: dict[str, Any], key: str, active: bool) -> int:
    streak = int(state.get(key, 0) or 0) + 1 if active else 0
    state[key] = streak
    return streak


def sample_row(
    *,
    now_ts: int,
    ffmpeg_pid: int,
    tcp: TcpObservation,
    network_down: bool,
    remote_warning: bool,
) -> dict[str, Any]:
    return {
        "ts": now_ts,
        "pid": ffmpeg_pid,
        "bytes_sent_delta": tcp.bytes_delta,
        "notsent": tcp.notsent,
        "unacked": tcp.unacked,
        "lastsnd_ms": tcp.lastsnd_ms,
        "network_down": network_down,
        "remote_warning": remote_warning,
        "low_upload_pressure": tcp.low_upload_pressure_now,
        "send_mbps": tcp.send_mbps,
    }


def select_restart_reason(
    state: dict[str, Any],
    *,
    url_preservation_mode: bool,
    remote_warning_streak: int,
    remote_warning_confirm: int,
    remote_warning_reason: str,
    network: NetworkObservation,
    net_fail_confirm: int,
    stall_confirm: int,
    low_upload_confirm: int,
    low_upload_max_mbps: float,
    tcp: TcpObservation,
) -> tuple[str, str]:
    if url_preservation_mode and remote_warning_streak >= remote_warning_confirm:
        return "remote_warning", f"youtube pre-loss warning while broadcast live: {remote_warning_reason}"
    if int(state.get("net_fail_streak", 0)) >= net_fail_confirm:
        return (
            "network_down",
            (
                f"network down: gw_ok={network.gateway_ok} public_ok_count={network.public_ok_count} "
                f"dns_ok={network.dns_ok} tcp_probe_ok={network.tcp_probe_ok}"
            ),
        )
    if int(state.get("stall_streak", 0)) >= stall_confirm:
        return (
            "tcp_stall",
            (
                f"tcp stall: bytes_delta={tcp.bytes_delta} lastsnd_ms={tcp.lastsnd_ms} "
                f"notsent={tcp.notsent} unacked={tcp.unacked}"
            ),
        )
    if int(state.get("low_upload_pressure_streak", 0)) >= low_upload_confirm:
        return (
            "low_upload_pressure",
            (
                f"low upload pressure: send_mbps={tcp.send_mbps}<={low_upload_max_mbps} "
                f"bytes_delta={tcp.bytes_delta} elapsed_sec={tcp.bytes_elapsed_sec} "
                f"lastsnd_ms={tcp.lastsnd_ms} notsent={tcp.notsent} unacked={tcp.unacked}"
            ),
        )
    return "", ""


def update_active_reason(state: dict[str, Any], *, now_ts: int, reason_kind: str, reason: str) -> RestartReason:
    if reason:
        if str(state.get("active_reason_kind", "")) != reason_kind:
            state["active_reason_kind"] = reason_kind
            state["active_reason_first_ts"] = now_ts
        first_ts = int(state.get("active_reason_first_ts", now_ts) or now_ts)
    else:
        state.pop("active_reason_kind", None)
        state.pop("active_reason_first_ts", None)
        first_ts = 0
    return RestartReason(kind=reason_kind, reason=reason, first_ts=first_ts)


def mark_latest_transport_sample(
    state: dict[str, Any],
    *,
    ffmpeg_pid: int,
    bytes_sent: int,
    now_ts: int,
    last_reason: str | None = None,
) -> None:
    state["last_pid"] = ffmpeg_pid
    state["last_bytes_sent"] = bytes_sent
    state["last_bytes_sent_ts"] = now_ts
    if last_reason is not None:
        state["last_reason"] = last_reason


def restart_metrics(*, tcp: TcpObservation, network_down: bool, remote_warning: bool) -> dict[str, Any]:
    return {
        "bytes_sent_delta": tcp.bytes_delta,
        "bytes_elapsed_sec": tcp.bytes_elapsed_sec,
        "send_mbps": tcp.send_mbps,
        "lastsnd_ms": tcp.lastsnd_ms,
        "notsent": tcp.notsent,
        "unacked": tcp.unacked,
        "network_down": network_down,
        "remote_warning": remote_warning,
        "low_upload_pressure": tcp.low_upload_pressure_now,
    }


def youtube_hint(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "api_live_state": "",
            "oauth_life_cycle_status": "",
            "oauth_stream_status": "",
            "oauth_stream_health_status": "",
            "remote_source": "",
            "remote_status": "",
        }
    return {
        "api_live_state": payload.get("api_live_state", ""),
        "oauth_life_cycle_status": payload.get("oauth_life_cycle_status", ""),
        "oauth_stream_status": payload.get("oauth_stream_status", ""),
        "oauth_stream_health_status": payload.get("oauth_stream_health_status", ""),
        "remote_source": payload.get("remote_source", ""),
        "remote_status": payload.get("remote_status", ""),
    }


def clear_recovery_streaks(state: dict[str, Any]) -> None:
    state["net_fail_streak"] = 0
    state["stall_streak"] = 0
    state["low_upload_pressure_streak"] = 0
    state["remote_warning_streak"] = 0
    state["remote_warning_last_stats_ts"] = 0
    state["remote_warning_last_sample_key"] = ""
    state["remote_warning_last_probe_ts"] = 0
    state["remote_warning_context_key"] = ""
    state["remote_warning_recovery_episode_id"] = ""
    state["remote_warning_ffmpeg_generation"] = ""
