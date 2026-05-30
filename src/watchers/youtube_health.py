from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IncidentJudge:
    stage: str
    reason: str


@dataclass(frozen=True)
class RestartDecision:
    should_restart: bool
    action: str
    reason: str
    cooldown_left: int = 0


def judge_incident_stage(
    healthy: bool,
    fail_count: int,
    stream_active: bool,
    ingest_connected: bool,
    availability_signal_ok: bool,
    oauth_probe_ok: bool,
    oauth_life_cycle_status: str,
    oauth_stream_status_required: bool,
    oauth_stream_status: str,
    incident_confirm_fails: int,
) -> IncidentJudge:
    if healthy:
        return IncidentJudge("none", "healthy")

    local_unhealthy = (not stream_active) or (not ingest_connected)
    oauth_remote_bad = oauth_probe_ok and (
        oauth_life_cycle_status in {"created", "ready", "testStarting", "testing"}
        or (oauth_stream_status_required and oauth_stream_status not in {"", "active"})
    )

    if local_unhealthy:
        return IncidentJudge("confirmed", "local pipeline unhealthy (service inactive or ingest disconnected)")
    if fail_count >= incident_confirm_fails and (oauth_remote_bad or not availability_signal_ok):
        return IncidentJudge(
            "confirmed",
            (
                f"consecutive unhealthy checks reached threshold "
                f"({fail_count}>={incident_confirm_fails}) with remote unhealthy signal"
            ),
        )
    if fail_count >= incident_confirm_fails + 1:
        return IncidentJudge(
            "confirmed",
            f"consecutive unhealthy checks reached hard threshold ({fail_count}>={incident_confirm_fails + 1})",
        )
    if oauth_remote_bad and stream_active and ingest_connected:
        return IncidentJudge("suspected", "remote-side mismatch while local ingest is connected (possible API/UI jitter)")
    return IncidentJudge("suspected", "single-cycle or early-stage unhealthy signal")


def decide_restart_action(
    fail_count: int,
    max_fails: int,
    enforce_restart: bool,
    skip_restart_if_ingest_connected: bool,
    stream_active: bool,
    ingest_connected: bool,
    failure_kind: str,
    incident_stage: str,
    stream_uptime_sec: int,
    min_restart_uptime_sec: int,
    restart_budget_hourly: int,
    restart_budget_daily: int,
    restart_history_ts: list[int],
    last_restart_ts: int,
    restart_cooldown_sec: int,
    now_ts: int,
    stream_service: str,
    restart_budget_release_reconfirm_sec: int = 0,
) -> RestartDecision:
    if fail_count < max_fails:
        return RestartDecision(False, "none", "below restart threshold")

    if not enforce_restart:
        return RestartDecision(False, "threshold reached; restart disabled", "restart enforcement disabled")

    if failure_kind == "transient_net":
        return RestartDecision(False, "restart deferred: transient network signal", "transient network signal")

    if incident_stage != "confirmed":
        return RestartDecision(False, "restart deferred: incident not confirmed", "incident not confirmed")

    if stream_uptime_sec > 0 and stream_uptime_sec < min_restart_uptime_sec:
        return RestartDecision(
            False,
            f"restart deferred: minimum uptime not met ({stream_uptime_sec}s<{min_restart_uptime_sec}s)",
            "minimum uptime not met",
        )

    if skip_restart_if_ingest_connected and stream_active and ingest_connected and failure_kind != "remote_ended":
        return RestartDecision(
            False,
            "restart suppressed: ingest tcp connected",
            "stream service active and ingest tcp connected",
        )

    cooldown_left = (last_restart_ts + restart_cooldown_sec) - now_ts
    if last_restart_ts > 0 and cooldown_left > 0:
        return RestartDecision(
            False,
            f"restart cooldown active ({cooldown_left}s remaining)",
            "restart cooldown active",
            cooldown_left=cooldown_left,
        )

    hourly = sum(1 for ts in restart_history_ts if now_ts - ts <= 3600)
    daily = sum(1 for ts in restart_history_ts if now_ts - ts <= 86400)
    if hourly >= restart_budget_hourly:
        return RestartDecision(
            False,
            f"restart budget exceeded: hourly ({hourly}/{restart_budget_hourly})",
            "restart budget exceeded (hourly)",
        )
    if daily >= restart_budget_daily:
        return RestartDecision(
            False,
            f"restart budget exceeded: daily ({daily}/{restart_budget_daily})",
            "restart budget exceeded (daily)",
        )

    reconfirm_sec = max(0, int(restart_budget_release_reconfirm_sec))
    if reconfirm_sec > 0:
        hourly_release_left = _recent_budget_release_reconfirm_left(
            restart_history_ts,
            now_ts=now_ts,
            window_sec=3600,
            budget=restart_budget_hourly,
            used_in_window=hourly,
            reconfirm_sec=reconfirm_sec,
        )
        if hourly_release_left > 0:
            return RestartDecision(
                False,
                f"restart deferred: hourly budget slot released recently ({hourly_release_left}s reconfirm remaining)",
                "restart budget slot recently released (hourly)",
                cooldown_left=hourly_release_left,
            )

        daily_release_left = _recent_budget_release_reconfirm_left(
            restart_history_ts,
            now_ts=now_ts,
            window_sec=86400,
            budget=restart_budget_daily,
            used_in_window=daily,
            reconfirm_sec=reconfirm_sec,
        )
        if daily_release_left > 0:
            return RestartDecision(
                False,
                f"restart deferred: daily budget slot released recently ({daily_release_left}s reconfirm remaining)",
                "restart budget slot recently released (daily)",
                cooldown_left=daily_release_left,
            )

    return RestartDecision(True, f"restart {stream_service}", "restart threshold reached")


def _recent_budget_release_reconfirm_left(
    restart_history_ts: list[int],
    *,
    now_ts: int,
    window_sec: int,
    budget: int,
    used_in_window: int,
    reconfirm_sec: int,
) -> int:
    if budget <= 0 or used_in_window != budget - 1:
        return 0
    left_values: list[int] = []
    for ts in restart_history_ts:
        try:
            age_sec = now_ts - int(ts)
        except (TypeError, ValueError):
            continue
        released_age_sec = age_sec - window_sec
        if 0 < released_age_sec <= reconfirm_sec:
            left_values.append(reconfirm_sec - released_age_sec)
    return max(left_values) if left_values else 0
