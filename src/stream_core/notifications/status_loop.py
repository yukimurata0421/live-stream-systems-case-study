from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

try:
    from stream_core.common.json_io import append_jsonl, iter_jsonl
    from stream_core.common.timeutil import parse_utc_ts, utc_now_text
    from stream_core.notifications import outbox as notify_outbox
except ModuleNotFoundError:
    from common.json_io import append_jsonl, iter_jsonl
    from common.timeutil import parse_utc_ts, utc_now_text
    from notifications import outbox as notify_outbox


@dataclass(frozen=True)
class NotifyStatusContext:
    notify_events_file: Path
    notify_outbox_file: Path
    load_config: Callable[[], dict]
    load_state: Callable[[], dict]
    save_state: Callable[[dict], None]
    collect_incidents: Callable[..., list[dict]]
    recovery_observation_for_incident: Callable[[str, int], tuple[int, str]]
    format_message: Callable[..., str]
    send_webhook: Callable[..., tuple[bool, str]]
    maintenance_notification_incident: Callable[[int], dict | None]
    fast_recovery_events_file: Path


def fast_recovery_auto_recovered_events(
    *,
    state: dict,
    now_ts: int,
    recent_sec: int,
    triggers: list[str],
    events_file: Path,
    max_events: int = 8,
) -> list[dict]:
    trigger_set = {str(item).strip() for item in triggers if str(item).strip()}
    if not trigger_set or not events_file.exists():
        return []

    acknowledged = state.get("fast_recovery_auto_recovered_notified")
    if not isinstance(acknowledged, dict):
        acknowledged = {}

    cutoff = int(now_ts) - max(60, int(recent_sec or 0))
    events: list[dict] = []
    for item in iter_jsonl(events_file):
        if str(item.get("kind", "")) != "restart":
            continue
        trigger = str(item.get("trigger", "")).strip()
        if trigger not in trigger_set:
            continue
        event_ts = parse_utc_ts(str(item.get("ts_utc", "")))
        if event_ts <= 0 or event_ts < cutoff or event_ts > int(now_ts) + 60:
            continue
        key = f"{item.get('ts_utc')}|{trigger}"
        if acknowledged.get(key):
            continue
        evidence = str(item.get("message", "") or item.get("reason", "") or f"trigger={trigger}")
        events.append(
            {
                "id": f"fast_recovery:auto_recovered:{trigger}:{int(event_ts)}",
                "severity": "info",
                "component": "fast_recovery",
                "summary": "stream service restart completed",
                "evidence": evidence[:240],
                "recovery_type": f"fast_recovery_restart:{trigger}",
                "follow_up": "次回 routine check で同じ時間帯・trigger が再発していないか確認する",
                "observed_ts": int(event_ts),
                "trigger": trigger,
                "_event_key": key,
            }
        )

    events.sort(key=lambda event: int(event.get("observed_ts", 0) or 0))
    return events[-max(1, int(max_events)) :]


def mark_fast_recovery_auto_recovered_events_notified(state: dict, events: list[dict], *, now_ts: int) -> None:
    acknowledged = state.get("fast_recovery_auto_recovered_notified")
    if not isinstance(acknowledged, dict):
        acknowledged = {}

    cutoff = int(now_ts) - 86400
    compacted: dict[str, int] = {}
    for key, value in acknowledged.items():
        try:
            ts = int(value or 0)
        except Exception:
            ts = 0
        if ts >= cutoff:
            compacted[str(key)] = ts

    for event in events:
        key = str(event.get("_event_key", "") or "")
        if key:
            compacted[key] = int(now_ts)
    state["fast_recovery_auto_recovered_notified"] = compacted


def notify_maintenance_message_due(state: dict, item: dict, *, now_ts: int, repeat_sec: int, dry_run: bool) -> bool:
    if dry_run:
        return True
    last_sent = int(state.get("last_maintenance_status_sent_ts", 0) or 0)
    started_ts = int(item.get("_first_seen_ts", now_ts) or now_ts)
    return last_sent < started_ts or (now_ts - last_sent) >= repeat_sec


def deliver_notify_messages(
    *,
    ctx: NotifyStatusContext,
    messages: list[tuple[str, list[dict]]],
    state: dict,
    cfg: dict,
    now_ts: int,
    dry_run: bool,
) -> tuple[int, int, int]:
    rendered_messages: list[tuple[str, list[dict], str]] = []
    for phase, phase_incidents in messages:
        content = ctx.format_message(phase=phase, incidents=phase_incidents, state=state, now_ts=now_ts)
        if dry_run or not cfg["enabled"]:
            print(content)
        rendered_messages.append((phase, phase_incidents, content))

    sent = 0
    failures = 0
    pending = 0
    if not dry_run and cfg["enabled"] and cfg["webhook_url"]:
        outbox = notify_outbox.load_notify_outbox(
            ctx.notify_outbox_file,
            now_ts=now_ts,
            ttl_sec=int(cfg["outbox_ttl_sec"]),
        )
        outbox = notify_outbox.enqueue_notify_messages(
            outbox,
            rendered_messages,
            username=str(cfg["username"]),
            now_ts=now_ts,
            max_pending=int(cfg["outbox_max_pending"]),
        )
        notify_outbox.save_notify_outbox(ctx.notify_outbox_file, outbox)
        sent, failures, pending = notify_outbox.flush_notify_outbox(
            outbox_path=ctx.notify_outbox_file,
            events_path=ctx.notify_events_file,
            cfg=cfg,
            now_ts=now_ts,
            send_webhook=ctx.send_webhook,
            dry_run=dry_run,
        )
    elif not dry_run and rendered_messages:
        reason = "disabled" if not cfg["enabled"] else "missing_webhook_url"
        if reason == "missing_webhook_url":
            print("[warn] STREAM_NOTIFY_DISCORD_WEBHOOK_URL is not configured")
        for phase, phase_incidents, content in rendered_messages:
            event = {
                "ts_utc": utc_now_text(now_ts),
                "phase": phase,
                "incident_ids": [item.get("id") for item in phase_incidents],
                "dry_run": False,
                "enabled": cfg["enabled"],
                "message": content,
                "outbox": False,
                "send_ok": reason == "disabled",
                "send_reason": reason,
            }
            append_jsonl(ctx.notify_events_file, event)
            if event["send_ok"]:
                sent += 1
            else:
                failures += 1
    elif not dry_run:
        sent, failures, pending = notify_outbox.flush_notify_outbox(
            outbox_path=ctx.notify_outbox_file,
            events_path=ctx.notify_events_file,
            cfg=cfg,
            now_ts=now_ts,
            send_webhook=ctx.send_webhook,
            dry_run=dry_run,
        )
    return sent, failures, pending


def notify_status(*, ctx: NotifyStatusContext, dry_run: bool = False, force_test: bool = False, now_ts: int | None = None) -> int:
    now = int(time.time() if now_ts is None else now_ts)
    cfg = ctx.load_config()
    state = ctx.load_state()
    active_state = state.get("active") if isinstance(state.get("active"), dict) else {}
    messages: list[tuple[str, list[dict]]] = []
    if force_test:
        messages.append(("test", []))

    maintenance_item = ctx.maintenance_notification_incident(now)
    if maintenance_item is not None:
        if notify_maintenance_message_due(
            state,
            maintenance_item,
            now_ts=now,
            repeat_sec=int(cfg.get("maintenance_repeat_sec", 600)),
            dry_run=dry_run,
        ):
            messages.append(("maintenance", [maintenance_item]))
            state["last_maintenance_status_sent_ts"] = now
        state["maintenance_active"] = True
        state["maintenance_started_ts"] = int(maintenance_item.get("_first_seen_ts", now) or now)
        state["active"] = active_state
        state["updated_ts_utc"] = utc_now_text(now)
        sent, failures, pending = deliver_notify_messages(
            ctx=ctx,
            messages=messages,
            state=state,
            cfg=cfg,
            now_ts=now,
            dry_run=dry_run,
        )
        if not dry_run:
            ctx.save_state(state)
        if not messages and pending <= 0 and sent <= 0 and failures <= 0:
            print("[notify-status] maintenance mode active; no reminder due")
        else:
            print(f"[notify-status] messages={len(messages)} sent={sent} failures={failures} pending={pending}")
        return 0 if failures == 0 else 1

    state["maintenance_active"] = False

    incidents = ctx.collect_incidents(
        now_ts=now,
        report_stale_sec=int(cfg["report_stale_sec"]),
        startup_grace_sec=int(cfg.get("startup_grace_sec", 0) or 0),
    )
    incident_by_id = {str(item.get("id")): item for item in incidents}
    previous_ids = set(active_state.keys())
    current_ids = set(incident_by_id.keys())
    new_ids = current_ids - previous_ids
    recovered_ids = previous_ids - current_ids

    for ident in current_ids:
        item = incident_by_id[ident]
        existing = active_state.get(ident, {})
        observed_ts = int(item.get("observed_ts", 0) or 0)
        first_default = observed_ts if observed_ts > 0 else now
        existing_first = int(existing.get("first_seen_ts", 0) or 0)
        first_seen = min(existing_first, first_default) if existing_first > 0 else first_default
        active_state[ident] = {
            **existing,
            "first_seen_ts": first_seen,
            "first_notified_ts": int(existing.get("first_notified_ts", now) or now),
            "last_bad_ts": observed_ts if observed_ts > 0 else now,
            "last_notified_ts": now,
            "last_incident": item,
        }

    if incidents:
        last_sent = int(state.get("last_status_sent_ts", 0) or 0)
        due = (now - last_sent) >= int(cfg["repeat_sec"])
        if new_ids or due:
            phase = "detected" if new_ids else "status"
            messages.append((phase, incidents))
            state["last_status_sent_ts"] = now
    if recovered_ids:
        recovered: list[dict] = []
        for ident in sorted(recovered_ids):
            stored = active_state.get(ident, {})
            item = dict(stored.get("last_incident", {"id": ident, "summary": "recovered"}))
            item["_first_seen_ts"] = int(stored.get("first_seen_ts", now) or now)
            item["_first_notified_ts"] = int(stored.get("first_notified_ts", item["_first_seen_ts"]) or item["_first_seen_ts"])
            item["_last_bad_ts"] = int(stored.get("last_bad_ts", 0) or 0)
            recovered_ts, recovery_evidence = ctx.recovery_observation_for_incident(ident, now)
            item["_recovered_ts"] = recovered_ts
            item["_recovery_evidence"] = recovery_evidence
            recovered.append(item)
        messages.append(("recovered", recovered))
        for ident in recovered_ids:
            active_state.pop(ident, None)
        if not current_ids:
            state["last_status_sent_ts"] = 0

    auto_recovered_events = fast_recovery_auto_recovered_events(
        state=state,
        now_ts=now,
        recent_sec=int(cfg.get("fast_recovery_event_recent_sec", 1800) or 1800),
        triggers=list(cfg.get("fast_recovery_event_triggers", [])),
        events_file=ctx.fast_recovery_events_file,
    )
    if auto_recovered_events:
        messages.append(("auto_recovered", auto_recovered_events))

    state["active"] = active_state
    state["updated_ts_utc"] = utc_now_text(now)

    sent, failures, pending = deliver_notify_messages(
        ctx=ctx,
        messages=messages,
        state=state,
        cfg=cfg,
        now_ts=now,
        dry_run=dry_run,
    )

    if auto_recovered_events and not dry_run:
        mark_fast_recovery_auto_recovered_events_notified(state, auto_recovered_events, now_ts=now)

    if not dry_run:
        ctx.save_state(state)
    if not messages and pending <= 0 and sent <= 0 and failures <= 0:
        print("[notify-status] no active incidents; no notification due")
    else:
        print(f"[notify-status] messages={len(messages)} sent={sent} failures={failures} pending={pending}")
    return 0 if failures == 0 else 1
