from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

from .connectivity import ConnectivityResult, IngestEndpoint


@dataclass
class RenderRecoveryState:
    browser_started_monotonic: float = 0.0
    failed_samples: int = 0
    last_recovery_monotonic: float = 0.0
    connectivity_blocked: bool = False


def wait_for_connectivity(
    *,
    enabled: bool,
    endpoint: IngestEndpoint,
    poll_sec: float,
    probe: Callable[[], ConnectivityResult],
    stop_requested: Callable[[], bool],
    append_event: Callable[..., str],
    write_snapshot: Callable[[str, str, str], None],
    log: Callable[[str], None],
    sleep: Callable[[float], None] = time.sleep,
) -> bool:
    if not enabled:
        return not stop_requested()

    waiting = False
    while not stop_requested():
        result = probe()
        if result.ready:
            if waiting:
                log(f"Ingest connectivity recovered: {endpoint.host}:{endpoint.port}")
                append_event(
                    "connectivity_wait_recovered",
                    host=endpoint.host,
                    port=endpoint.port,
                    detail=result.detail,
                )
                write_snapshot("restarting", "", "connectivity recovered")
            return True
        if not waiting:
            waiting = True
            log(
                "Ingest connectivity unavailable; holding FFmpeg child restart "
                f"and polling every {poll_sec:g}s: {result.detail}"
            )
            append_event(
                "connectivity_wait_started",
                host=endpoint.host,
                port=endpoint.port,
                poll_sec=poll_sec,
                dns_ok=result.dns_ok,
                tcp_ok=result.tcp_ok,
                detail=result.detail,
            )
            write_snapshot("waiting_connectivity", "", result.detail)
        sleep(poll_sec)
    return False


def recover_stale_render(
    state: RenderRecoveryState,
    *,
    enabled: bool,
    confirmations: int,
    grace_sec: float,
    cooldown_sec: float,
    render_probe: Callable[[], tuple[bool, str]],
    connectivity_probe: Callable[[], ConnectivityResult],
    restart_browser: Callable[[], bool],
    browser_pid: Callable[[], int],
    ffmpeg_pid: Callable[[], int],
    append_event: Callable[..., str],
    monotonic: Callable[[], float] = time.monotonic,
) -> bool:
    if not enabled:
        return False

    ready, detail = render_probe()
    if ready:
        if state.failed_samples > 0:
            append_event(
                "render_heartbeat_recovered",
                failed_samples=state.failed_samples,
                detail=detail,
            )
        state.failed_samples = 0
        state.connectivity_blocked = False
        return False

    now = monotonic()
    if now - state.browser_started_monotonic < grace_sec:
        return False
    state.failed_samples += 1
    if state.failed_samples == 1:
        append_event("render_heartbeat_degraded", detail=detail)
    if state.failed_samples < confirmations:
        return False
    if now - state.last_recovery_monotonic < cooldown_sec:
        return False

    network = connectivity_probe()
    if not network.ready:
        if not state.connectivity_blocked:
            append_event(
                "render_browser_recovery_blocked",
                reason="connectivity_unavailable",
                dns_ok=network.dns_ok,
                tcp_ok=network.tcp_ok,
                detail=network.detail,
            )
            state.connectivity_blocked = True
        return False

    state.connectivity_blocked = False
    state.last_recovery_monotonic = now
    append_event(
        "render_browser_self_recovery_requested",
        failed_samples=state.failed_samples,
        detail=detail,
    )
    try:
        restarted = restart_browser()
    except Exception as exc:
        append_event(
            "render_browser_self_recovery_failed",
            error=f"{type(exc).__name__}: {exc}",
        )
        return False
    state.failed_samples = 0
    if restarted:
        append_event(
            "render_browser_self_recovery_completed",
            browser_pid=browser_pid(),
            ffmpeg_pid=ffmpeg_pid(),
        )
    return restarted
