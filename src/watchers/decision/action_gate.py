from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

try:
    from decision.evaluator import Decision
    from decision.policy import Policy
    from evidence.ledger import LedgerSnapshot
    from evidence.sources import SourceKind
except ImportError:
    from .evaluator import Decision  # type: ignore
    from .policy import Policy  # type: ignore
    from ..evidence.ledger import LedgerSnapshot  # type: ignore
    from ..evidence.sources import SourceKind  # type: ignore


ActionKind = Literal["none", "resync_resolver", "restart_stream", "alert"]


@dataclass(frozen=True)
class GateContext:
    fail_count: int
    max_fails: int
    enforce_restart: bool
    stream_uptime_sec: int
    min_restart_uptime_sec: int
    restart_budget_hourly: int
    restart_budget_daily: int
    restart_history_ts: tuple[int, ...]
    last_restart_ts: int
    restart_cooldown_sec: int
    budget_release_reconfirm_sec: int
    budget_emergency_override_sec: int
    active_state_first_ts: int
    api_cost_degraded: bool
    stream_service: str


@dataclass(frozen=True)
class ActionDecision:
    action: ActionKind
    reason: str
    blocked_by: tuple[str, ...] = ()
    cooldown_left: int = 0


def decide_action(decision: Decision, snap: LedgerSnapshot, policy: Policy, ctx: GateContext) -> ActionDecision:
    if decision.state == "available":
        return ActionDecision("none", "available")
    if decision.state == "remote_unconfirmed":
        return ActionDecision("none", "remote state unconfirmed; refuse destructive action", ("remote_unconfirmed",))
    if decision.state == "inconsistent_remote":
        return ActionDecision("resync_resolver", decision.reason)
    if decision.state in {"telemetry_degraded", "local_only"}:
        return ActionDecision("none", "blind or local-only; refuse destructive action", ("blind_safety",))
    if decision.state in {"public_degraded", "remote_ended_suspected"}:
        return ActionDecision("alert", decision.reason)

    if decision.state not in {"remote_ended_confirmed", "local_unhealthy"}:
        return ActionDecision("none", f"unsupported decision state: {decision.state}")

    blocked: list[str] = []
    cooldown_left = 0
    if ctx.fail_count < ctx.max_fails:
        blocked.append("below_restart_threshold")
    if not ctx.enforce_restart:
        blocked.append("restart_enforcement_disabled")
    if ctx.stream_uptime_sec > 0 and ctx.stream_uptime_sec < ctx.min_restart_uptime_sec:
        blocked.append("minimum_uptime_not_met")

    cooldown_left = max(0, (ctx.last_restart_ts + ctx.restart_cooldown_sec) - int(snap.taken_at))
    if ctx.last_restart_ts > 0 and cooldown_left > 0:
        blocked.append("cooldown")

    budget_block, budget_left = _budget_block(ctx.restart_history_ts, int(snap.taken_at), ctx, policy, decision, snap)
    if budget_block:
        blocked.append(budget_block)
        cooldown_left = max(cooldown_left, budget_left)

    if decision.state == "remote_ended_confirmed":
        ingest = snap.latest_by_source.get(SourceKind.INGEST_LOCAL)
        if ingest is not None and ingest.verdict == "live" and ingest.is_fresh(
            snap.taken_at,
            snap.current_target_epoch,
            snap.current_restart_epoch,
        ):
            blocked.append("local_ingest_alive_contradiction")
        if ctx.api_cost_degraded:
            blocked.append("api_cost_degraded")

    if blocked:
        return ActionDecision("none", f"restart blocked for {decision.state}", tuple(blocked), cooldown_left)
    return ActionDecision("restart_stream", decision.reason)


def _budget_block(
    history: tuple[int, ...],
    now_ts: int,
    ctx: GateContext,
    policy: Policy,
    decision: Decision,
    snap: LedgerSnapshot,
) -> tuple[str, int]:
    hourly = sum(1 for ts in history if now_ts - ts <= 3600)
    daily = sum(1 for ts in history if now_ts - ts <= 86400)
    emergency_override = _budget_emergency_override_active(now_ts, ctx) and _has_fresh_override_confirmation(
        decision,
        snap,
        policy,
        ctx,
        now_ts,
    )
    if hourly >= ctx.restart_budget_hourly:
        if emergency_override:
            return "", 0
        return f"budget_exhausted_hourly({hourly}/{ctx.restart_budget_hourly})", 0
    if daily >= ctx.restart_budget_daily:
        if emergency_override:
            return "", 0
        return f"budget_exhausted_daily({daily}/{ctx.restart_budget_daily})", 0

    reconfirm_sec = max(0, int(ctx.budget_release_reconfirm_sec or policy.budget_release_reconfirm_sec))
    if reconfirm_sec <= 0:
        return "", 0
    hourly_left = _recent_budget_release_left(
        history,
        now_ts=now_ts,
        window_sec=3600,
        budget=ctx.restart_budget_hourly,
        used_in_window=hourly,
        reconfirm_sec=reconfirm_sec,
    )
    if hourly_left > 0:
        return "budget_just_released_need_reconfirm_hourly", hourly_left
    daily_left = _recent_budget_release_left(
        history,
        now_ts=now_ts,
        window_sec=86400,
        budget=ctx.restart_budget_daily,
        used_in_window=daily,
        reconfirm_sec=reconfirm_sec,
    )
    if daily_left > 0:
        return "budget_just_released_need_reconfirm_daily", daily_left
    return "", 0


def _budget_emergency_override_active(now_ts: int, ctx: GateContext) -> bool:
    override_sec = max(0, int(ctx.budget_emergency_override_sec or 0))
    first_ts = int(ctx.active_state_first_ts or 0)
    if override_sec <= 0 or first_ts <= 0:
        return False
    return now_ts - first_ts >= override_sec


def _has_fresh_override_confirmation(
    decision: Decision,
    snap: LedgerSnapshot,
    policy: Policy,
    ctx: GateContext,
    now_ts: int,
) -> bool:
    max_age = max(30, min(120, int(ctx.budget_emergency_override_sec or 0) or 90))
    if decision.state == "remote_ended_confirmed":
        required = (SourceKind.DATA_API, SourceKind.OAUTH)
    elif decision.state == "local_unhealthy":
        required = (SourceKind.INGEST_LOCAL,)
    else:
        return False
    for source in required:
        ev = snap.latest_by_source.get(source)
        if ev is None:
            return False
        source_ttl = int(policy.ttl.get(source, max_age) or max_age)
        allowed_age = min(max_age, max(1, source_ttl))
        if now_ts - int(ev.observed_at) > allowed_age:
            return False
        if not ev.is_fresh(snap.taken_at, snap.current_target_epoch, snap.current_restart_epoch):
            return False
    return True


def _recent_budget_release_left(
    history: tuple[int, ...],
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
    for ts in history:
        age = now_ts - int(ts)
        released_age = age - window_sec
        if 0 < released_age <= reconfirm_sec:
            left_values.append(reconfirm_sec - released_age)
    return max(left_values) if left_values else 0
