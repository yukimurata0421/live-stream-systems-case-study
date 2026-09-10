from __future__ import annotations

from pathlib import Path
from typing import Callable


IncidentFactory = Callable[..., dict]
ReadJson = Callable[[Path], dict]

ROOT_INCIDENT_ID = "network:delivery_connectivity_unavailable"
DEFERRED_ACTIVE_STATE_KEY = "connectivity_deferred_active"


DERIVATIVE_INCIDENT_IDS = {
    "stream:current_fail",
    "youtube:current_degraded",
    "resolver:fast_mode_active_or_runaway",
    "map:delivery_critical",
    "map:precipitation_unavailable",
    "map:precipitation_render_mismatch",
    "viewer:visual_failure",
    "viewer:synthetic_probe_failed",
    "stream1090:overlay_report",
    "stream1090:upstream_report",
    "reliability:youtube_input_quality_fast_feedback",
    "reliability:youtube_input_quality_coverage_or_freshness",
}


def active_wait(
    *,
    state_file: Path | None,
    now_ts: int,
    read_json: ReadJson,
) -> dict:
    if state_file is None:
        return {}
    state = read_json(state_file)
    if state.get("connectivity_wait_active") is not True:
        return {}
    try:
        observed_ts = int(
            state.get("observed_ts")
            or state.get("connectivity_last_observed_ts")
            or 0
        )
    except (TypeError, ValueError):
        observed_ts = 0
    if observed_ts <= 0 or now_ts - observed_ts < -60 or now_ts - observed_ts > 180:
        return {}
    return state


def correlate(
    incidents: list[dict],
    *,
    state_file: Path | None,
    now_ts: int,
    read_json: ReadJson,
    incident_factory: IncidentFactory,
) -> list[dict]:
    state = active_wait(state_file=state_file, now_ts=now_ts, read_json=read_json)
    if not state:
        return incidents

    suppressed = [
        str(item.get("id"))
        for item in incidents
        if str(item.get("id")) in DERIVATIVE_INCIDENT_IDS
    ]
    independent = [
        item
        for item in incidents
        if str(item.get("id")) not in DERIVATIVE_INCIDENT_IDS
    ]
    try:
        started_ts = int(state.get("connectivity_wait_since_ts") or now_ts)
    except (TypeError, ValueError):
        started_ts = now_ts
    root = incident_factory(
        ident=ROOT_INCIDENT_ID,
        severity="critical",
        component="local_delivery_connectivity",
        summary="local delivery connectivity is unavailable",
        evidence=(
            f"gateway_present={state.get('connectivity_gateway_present')} "
            f"gateway_ok={state.get('connectivity_gateway_ok')} "
            f"public_ok_count={state.get('connectivity_public_ok_count')} "
            f"dns_ok={state.get('connectivity_dns_ok')} "
            f"rtmps_tcp_ok={state.get('connectivity_tcp_probe_ok')} "
            f"suppressed_derivatives={','.join(suppressed) or 'none'}"
        ),
        recovery_type="connectivity_restore_no_runtime_restart",
        follow_up="carrier、default route、DNS、RTMPS TCPの順に回復を確認し、その後map/viewerを再評価する",
        observed_ts=started_ts,
        repeat_sec=600,
    )
    return [root, *independent]


def reconcile_deferred_active(
    *,
    state: dict,
    active_state: dict,
    current_ids: set[str],
) -> None:
    """Prevent correlation from falsely closing an already-open derivative.

    While the connectivity root is active, derivative incidents omitted by
    ``correlate`` are moved out of the visible active set instead of being
    treated as recovered. Once connectivity is restored they are reintroduced
    for a normal component-specific evaluation and recovery transition.
    """

    stored = state.get(DEFERRED_ACTIVE_STATE_KEY)
    deferred = dict(stored) if isinstance(stored, dict) else {}
    if ROOT_INCIDENT_ID in current_ids:
        for ident in list(active_state):
            if ident in DERIVATIVE_INCIDENT_IDS and ident not in current_ids:
                deferred[ident] = active_state.pop(ident)
        if deferred:
            state[DEFERRED_ACTIVE_STATE_KEY] = deferred
        else:
            state.pop(DEFERRED_ACTIVE_STATE_KEY, None)
        return

    for ident, incident_state in deferred.items():
        if ident not in active_state and isinstance(incident_state, dict):
            active_state[ident] = incident_state
    state.pop(DEFERRED_ACTIVE_STATE_KEY, None)
