from __future__ import annotations


def used_downtime_budget_sec(
    events: list[dict[str, int | str]],
    *,
    now_ts: int,
    window_sec: int,
    default_downtime_cost_sec: int,
) -> int:
    used = 0
    for event in events:
        ts = int(event.get("ts", 0) or 0)
        if ts <= 0 or now_ts - ts > window_sec:
            continue
        used += int(event.get("downtime_sec", default_downtime_cost_sec) or default_downtime_cost_sec)
    return max(0, used)


def emergency_budget_override_active(
    *,
    reason_kind: str,
    reason_first_ts: int,
    now_ts: int,
    override_sec: int,
) -> bool:
    if override_sec <= 0:
        return False
    if reason_kind not in {"remote_warning", "network_down", "tcp_stall", "low_upload_pressure"}:
        return False
    if reason_first_ts <= 0:
        return False
    return now_ts - reason_first_ts >= override_sec


def restart_failure_backoff_left(*, now_ts: int, last_restart_failure_ts: int, backoff_sec: int) -> int:
    if last_restart_failure_ts <= 0 or backoff_sec <= 0:
        return 0
    left = (last_restart_failure_ts + backoff_sec) - now_ts
    return left if left > 0 else 0
