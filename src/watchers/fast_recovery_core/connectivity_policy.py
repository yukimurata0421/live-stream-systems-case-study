from __future__ import annotations

from typing import Any, Callable

from .decision import NetworkObservation


AppendEvent = Callable[[str, str, dict[str, Any] | None], None]


def mark_wait(
    state: dict[str, Any],
    *,
    now_ts: int,
    network: NetworkObservation,
    ffmpeg_pid: int,
    append_event: AppendEvent,
) -> None:
    was_active = state.get("connectivity_wait_active") is True
    if not was_active:
        state["connectivity_wait_since_ts"] = now_ts
        append_event(
            "connectivity_wait",
            "network unavailable; runtime restart intentionally suppressed",
            {
                "trigger": "network_down",
                "ffmpeg_pid": ffmpeg_pid,
                "gateway_present": bool(network.gateway),
                "gateway_ok": network.gateway_ok,
                "public_ok_count": network.public_ok_count,
                "dns_ok": network.dns_ok,
                "tcp_probe_ok": network.tcp_probe_ok,
                "recovery_scope": "connectivity_restore",
            },
        )
    state.update(
        {
            "connectivity_wait_active": True,
            "connectivity_last_observed_ts": now_ts,
            "connectivity_gateway_present": bool(network.gateway),
            "connectivity_gateway_ok": network.gateway_ok,
            "connectivity_public_ok_count": network.public_ok_count,
            "connectivity_dns_ok": network.dns_ok,
            "connectivity_tcp_probe_ok": network.tcp_probe_ok,
            "last_reason": "network unavailable; runtime restart suppressed until connectivity recovers",
        }
    )


def clear_wait(
    state: dict[str, Any],
    *,
    now_ts: int,
    append_event: AppendEvent,
    network: NetworkObservation | None = None,
) -> None:
    if state.get("connectivity_wait_active") is True:
        since_ts = int(state.get("connectivity_wait_since_ts", now_ts) or now_ts)
        append_event(
            "connectivity_recovered",
            "network probes recovered; normal fast recovery actions re-enabled",
            {
                "trigger": "network_down",
                "active_sec": max(0, now_ts - since_ts),
                "recovery_scope": "connectivity_restore",
            },
        )
        state["connectivity_recovered_ts"] = now_ts
    state["connectivity_wait_active"] = False
    state["connectivity_last_observed_ts"] = now_ts
    if network is not None:
        state.update(
            {
                "connectivity_gateway_present": bool(network.gateway),
                "connectivity_gateway_ok": network.gateway_ok,
                "connectivity_public_ok_count": network.public_ok_count,
                "connectivity_dns_ok": network.dns_ok,
                "connectivity_tcp_probe_ok": network.tcp_probe_ok,
            }
        )
