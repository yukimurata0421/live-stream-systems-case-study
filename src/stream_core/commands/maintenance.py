from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

try:
    from stream_core.common.timeutil import utc_now_text
except ModuleNotFoundError:
    from common.timeutil import utc_now_text


@dataclass(frozen=True)
class MaintenanceContext:
    state_file: Path
    timers: tuple[str, ...]
    services: tuple[str, ...]
    status_actions: set[str]
    notify_timer: str
    unit_installed: Callable[[str], bool]
    is_active: Callable[[str], bool]
    start_unit: Callable[[str], bool]
    stop_unit: Callable[[str], bool]


def managed_units(ctx: MaintenanceContext) -> tuple[str, ...]:
    return (*ctx.timers, *ctx.services)


def installed_timers(ctx: MaintenanceContext) -> list[str]:
    return [unit for unit in ctx.timers if ctx.unit_installed(unit)]


def write_state(ctx: MaintenanceContext, payload: dict, path: Path | None = None) -> None:
    target = ctx.state_file if path is None else path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_state(ctx: MaintenanceContext, path: Path | None = None) -> dict:
    target = ctx.state_file if path is None else path
    if not target.exists():
        return {}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def status_payload(ctx: MaintenanceContext) -> dict:
    state = read_state(ctx)
    units = {
        unit: {
            "installed": ctx.unit_installed(unit),
            "active": ctx.is_active(unit),
        }
        for unit in managed_units(ctx)
    }
    active_managed = [unit for unit, info in units.items() if info["active"]]
    active_services = [unit for unit in ctx.services if units.get(unit, {}).get("active")]
    timer_active = [unit for unit in ctx.timers if units.get(unit, {}).get("active")]
    timer_inactive = [
        unit
        for unit in ctx.timers
        if units.get(unit, {}).get("installed") and not units.get(unit, {}).get("active")
    ]
    notify_timer = {
        "unit": ctx.notify_timer,
        "installed": ctx.unit_installed(ctx.notify_timer),
        "active": ctx.is_active(ctx.notify_timer),
    }
    inferred_active = not active_managed and bool(state.get("active", False))
    return {
        "state_file": str(ctx.state_file),
        "active": bool(state.get("active", False)),
        "inferred_active": inferred_active,
        "started_at_utc": state.get("started_at_utc", ""),
        "resumed_at_utc": state.get("resumed_at_utc", ""),
        "last_action": state.get("last_action", ""),
        "managed_units": list(managed_units(ctx)),
        "active_managed_units": active_managed,
        "active_services": active_services,
        "inactive_installed_timers": timer_inactive,
        "active_timers": timer_active,
        "maintenance_notification_timer": notify_timer,
        "units": units,
    }


def on(ctx: MaintenanceContext, *, json_output: bool = False) -> int:
    units = managed_units(ctx)
    before_active = [unit for unit in units if ctx.is_active(unit)]
    ok = True
    # Stop timers first so oneshot services cannot be triggered again during maintenance.
    for unit in ctx.timers:
        if ctx.unit_installed(unit) and ctx.is_active(unit):
            ok = ctx.stop_unit(unit) and ok
        elif ctx.unit_installed(unit):
            print(f"[skip] {unit} is already inactive")
    for unit in ctx.services:
        if ctx.unit_installed(unit) and ctx.is_active(unit):
            ok = ctx.stop_unit(unit) and ok
        elif ctx.unit_installed(unit):
            print(f"[skip] {unit} is already inactive")
    if ctx.unit_installed(ctx.notify_timer) and not ctx.is_active(ctx.notify_timer):
        ok = ctx.start_unit(ctx.notify_timer) and ok
    after_active = [unit for unit in units if ctx.unit_installed(unit) and ctx.is_active(unit)]
    state = {
        "active": True,
        "last_action": "on",
        "started_at_utc": utc_now_text(),
        "resumed_at_utc": "",
        "mode": "maintenance",
        "managed_units": list(units),
        "maintenance_notification_timer": ctx.notify_timer,
        "previous_active_units": before_active,
        "still_active_units": after_active,
        "note": "Stream and AutoDJ services are intentionally left untouched. Notify timer stays active for maintenance reminders.",
    }
    write_state(ctx, state)
    if json_output:
        print(json.dumps({**state, "ok": ok and not after_active}, ensure_ascii=False, separators=(",", ":")))
    else:
        print("[maintenance] ON: monitoring/recovery/report timers are stopped")
        print("[maintenance] notify timer remains active for maintenance reminders")
        print("[maintenance] stream and AutoDJ were not stopped")
        print(f"[maintenance] state={ctx.state_file}")
        if after_active:
            print("[warn] some maintenance-managed units are still active:")
            for unit in after_active:
                print(f"  - {unit}")
    return 0 if ok and not after_active else 1


def off(ctx: MaintenanceContext, *, json_output: bool = False) -> int:
    timers = installed_timers(ctx)
    ok = True
    for unit in timers:
        if ctx.is_active(unit):
            print(f"[skip] {unit} is already active")
            continue
        ok = ctx.start_unit(unit) and ok
    inactive_timers = [unit for unit in timers if not ctx.is_active(unit)]
    state = read_state(ctx)
    state.update(
        {
            "active": False,
            "last_action": "off",
            "resumed_at_utc": utc_now_text(),
            "managed_units": list(managed_units(ctx)),
            "maintenance_notification_timer": ctx.notify_timer,
            "inactive_timers_after_resume": inactive_timers,
            "note": "Maintenance monitoring timers resumed; stream and AutoDJ were intentionally untouched.",
        }
    )
    write_state(ctx, state)
    if json_output:
        print(json.dumps({**state, "ok": ok and not inactive_timers}, ensure_ascii=False, separators=(",", ":")))
    else:
        print("[maintenance] OFF: monitoring/recovery/report/notify timers are resumed")
        print("[maintenance] stream and AutoDJ were not restarted")
        print(f"[maintenance] state={ctx.state_file}")
        if inactive_timers:
            print("[warn] some maintenance timers are still inactive:")
            for unit in inactive_timers:
                print(f"  - {unit}")
    return 0 if ok and not inactive_timers else 1


def status(ctx: MaintenanceContext, *, json_output: bool = False) -> int:
    payload = status_payload(ctx)
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0
    state = "on" if payload["active"] else "off"
    print(f"[maintenance] state={state} state_file={payload['state_file']}")
    if payload.get("started_at_utc"):
        print(f"[maintenance] started_at_utc={payload['started_at_utc']}")
    if payload.get("resumed_at_utc"):
        print(f"[maintenance] resumed_at_utc={payload['resumed_at_utc']}")
    if payload["active"] and payload["active_managed_units"]:
        print("[warn] maintenance is marked on, but these managed units are still active:")
        for unit in payload["active_managed_units"]:
            print(f"  - {unit}")
    if payload["inactive_installed_timers"]:
        print("[maintenance] inactive installed timers:")
        for unit in payload["inactive_installed_timers"]:
            print(f"  - {unit}")
    if not payload["active"] and not payload["inactive_installed_timers"]:
        print("[maintenance] all installed maintenance timers are active")
    if not payload["active_services"]:
        print("[maintenance] no managed oneshot service is running")
    notify_timer = payload.get("maintenance_notification_timer") if isinstance(payload.get("maintenance_notification_timer"), dict) else {}
    if notify_timer.get("installed"):
        timer_status = "active" if notify_timer.get("active") else "inactive"
        print(f"[maintenance] notification reminder timer={timer_status} unit={notify_timer.get('unit')}")
    return 0


def dispatch(ctx: MaintenanceContext, action: str, *, json_output: bool = False) -> int:
    normalized = (action or "status").strip().lower()
    if normalized in {"on", "start", "pause", "enter"}:
        return on(ctx, json_output=json_output)
    if normalized in {"off", "stop", "resume", "exit"}:
        return off(ctx, json_output=json_output)
    if normalized in ctx.status_actions:
        return status(ctx, json_output=json_output)
    print("[error] usage: stream m on|off|status")
    return 2
