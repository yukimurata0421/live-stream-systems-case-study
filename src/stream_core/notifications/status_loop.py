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


@dataclass(frozen=True)
class RecoveryEvidence:
    restart_observed: bool
    recovery_confirmed: bool
    recovery_lag_sec: int | None
    diagnostic_context: str


def fast_recovery_recovery_events(
    *,
    state: dict,
    now_ts: int,
    recent_sec: int,
    triggers: list[str],
    events_file: Path,
    max_events: int = 8,
    confirmation_wait_sec: int = 180,
) -> list[dict]:
    trigger_set = {str(item).strip() for item in triggers if str(item).strip()}
    if not trigger_set or not events_file.exists():
        return []

    acknowledged = state.get("fast_recovery_recovery_evidence_notified")
    if not isinstance(acknowledged, dict):
        acknowledged = {}
    legacy_acknowledged = state.get("fast_recovery_auto_recovered_notified")
    if not isinstance(legacy_acknowledged, dict):
        legacy_acknowledged = {}

    cutoff = int(now_ts) - max(60, int(recent_sec or 0))
    context_cutoff = cutoff - 300
    context_end = int(now_ts) + 60
    tcp_samples: list[tuple[int, dict]] = []
    restart_candidates: list[tuple[int, str, str, dict]] = []
    for item in iter_jsonl(events_file):
        event_ts = parse_utc_ts(str(item.get("ts_utc", "")))
        if event_ts > 0 and context_cutoff <= event_ts <= context_end and str(item.get("kind", "")) == "tcp_send_sample":
            tcp_samples.append((event_ts, item))
        if str(item.get("kind", "")) != "restart":
            continue
        trigger = str(item.get("trigger", "")).strip()
        if trigger not in trigger_set:
            continue
        if event_ts <= 0 or event_ts < cutoff or event_ts > int(now_ts) + 60:
            continue
        key = f"{item.get('ts_utc')}|{trigger}"
        if legacy_acknowledged.get(key):
            continue
        restart_candidates.append((event_ts, trigger, key, item))

    events: list[dict] = []
    for event_ts, trigger, key, item in restart_candidates:
        recovery = fast_recovery_restart_evidence(event_ts, tcp_samples)
        if recovery.recovery_confirmed:
            phase = "auto_recovered"
            severity = "info"
            summary = "stream service restart and TCP send recovery confirmed"
        elif (int(now_ts) - event_ts) < max(1, int(confirmation_wait_sec)):
            phase = "restart_observed"
            severity = "info"
            summary = "stream service restart observed; TCP send recovery pending"
        else:
            phase = "recovery_unconfirmed"
            severity = "warning"
            summary = "stream service restart observed; TCP send recovery unconfirmed"

        notification_key = f"{key}|{phase}"
        if acknowledged.get(notification_key):
            continue

        evidence = str(item.get("message", "") or item.get("reason", "") or f"trigger={trigger}")
        events.append(
            {
                "id": f"fast_recovery:{phase}:{trigger}:{int(event_ts)}",
                "severity": severity,
                "component": "fast_recovery",
                "summary": summary,
                "evidence": evidence[:240],
                "recovery_type": f"fast_recovery_restart:{trigger}",
                "follow_up": "次回 routine check で同じ時間帯・trigger が再発していないか確認する",
                "observed_ts": int(event_ts),
                "trigger": trigger,
                "_event_key": key,
                "_notification_phase": phase,
                "restart_observed": recovery.restart_observed,
                "recovery_confirmed": recovery.recovery_confirmed,
                "recovery_lag_sec": recovery.recovery_lag_sec,
                "diagnostic_context": recovery.diagnostic_context,
            }
        )

    events.sort(key=lambda event: int(event.get("observed_ts", 0) or 0))
    return events[-max(1, int(max_events)) :]


def fast_recovery_auto_recovered_events(
    *,
    state: dict,
    now_ts: int,
    recent_sec: int,
    triggers: list[str],
    events_file: Path,
    max_events: int = 8,
) -> list[dict]:
    return [
        event
        for event in fast_recovery_recovery_events(
            state=state,
            now_ts=now_ts,
            recent_sec=recent_sec,
            triggers=triggers,
            events_file=events_file,
            max_events=max_events,
        )
        if event.get("_notification_phase") == "auto_recovered"
    ]


def _tcp_sample_number(item: dict, key: str) -> str:
    value = item.get(key, "")
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value)


def compact_tcp_send_sample(label: str, event_ts: int, sample_ts: int, item: dict) -> str:
    offset = sample_ts - event_ts
    sign = "+" if offset >= 0 else ""
    return (
        f"{label}={sign}{offset}s "
        f"mbps={_tcp_sample_number(item, 'mbps')} "
        f"delta={_tcp_sample_number(item, 'bytes_sent_delta')} "
        f"lastsnd={_tcp_sample_number(item, 'lastsnd_ms')} "
        f"notsent={_tcp_sample_number(item, 'notsent')} "
        f"unacked={_tcp_sample_number(item, 'unacked')} "
        f"pid={_tcp_sample_number(item, 'ffmpeg_pid')}"
    )


def fast_recovery_restart_evidence(
    event_ts: int,
    tcp_samples: list[tuple[int, dict]],
    *,
    before_sec: int = 180,
    after_sec: int = 300,
    recovered_mbps: float = 4.5,
) -> str:
    before = [
        (ts, item)
        for ts, item in tcp_samples
        if event_ts - max(1, before_sec) <= ts <= event_ts and str(item.get("kind", "")) == "tcp_send_sample"
    ]
    after = [
        (ts, item)
        for ts, item in tcp_samples
        if event_ts < ts <= event_ts + max(1, after_sec) and str(item.get("kind", "")) == "tcp_send_sample"
    ]
    parts: list[str] = []
    if before:
        ts, item = before[-1]
        parts.append(compact_tcp_send_sample("pre", event_ts, ts, item))
    recovered = []
    for ts, item in after:
        try:
            mbps = float(item.get("mbps", 0) or 0)
        except Exception:
            mbps = 0.0
        if mbps >= recovered_mbps:
            recovered.append((ts, item))
            break
    if recovered:
        ts, item = recovered[0]
        parts.append(compact_tcp_send_sample("post", event_ts, ts, item))
        parts.append(f"recovery_lag_sec={ts - event_ts}")
        return RecoveryEvidence(
            restart_observed=True,
            recovery_confirmed=True,
            recovery_lag_sec=ts - event_ts,
            diagnostic_context="; ".join(parts)[:320],
        )
    elif after:
        ts, item = after[0]
        parts.append(compact_tcp_send_sample("post_first", event_ts, ts, item))
        parts.append("recovery_lag_sec=unknown")
    if not parts:
        context = "tcp_sample_context=missing"
    else:
        context = "; ".join(parts)[:320]
    return RecoveryEvidence(
        restart_observed=True,
        recovery_confirmed=False,
        recovery_lag_sec=None,
        diagnostic_context=context,
    )


def fast_recovery_restart_diagnostic_context(
    event_ts: int,
    tcp_samples: list[tuple[int, dict]],
    *,
    before_sec: int = 180,
    after_sec: int = 300,
    recovered_mbps: float = 4.5,
) -> str:
    return fast_recovery_restart_evidence(
        event_ts,
        tcp_samples,
        before_sec=before_sec,
        after_sec=after_sec,
        recovered_mbps=recovered_mbps,
    ).diagnostic_context


def mark_fast_recovery_recovery_events_notified(state: dict, events: list[dict], *, now_ts: int) -> None:
    acknowledged = state.get("fast_recovery_recovery_evidence_notified")
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
        phase = str(event.get("_notification_phase", "") or "")
        if key:
            compacted[f"{key}|{phase}"] = int(now_ts)
    state["fast_recovery_recovery_evidence_notified"] = compacted


def mark_fast_recovery_auto_recovered_events_notified(state: dict, events: list[dict], *, now_ts: int) -> None:
    normalized = [
        {**event, "_notification_phase": str(event.get("_notification_phase", "") or "auto_recovered")}
        for event in events
    ]
    mark_fast_recovery_recovery_events_notified(state, normalized, now_ts=now_ts)


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

    recovery_events = fast_recovery_recovery_events(
        state=state,
        now_ts=now,
        recent_sec=int(cfg.get("fast_recovery_event_recent_sec", 1800) or 1800),
        triggers=list(cfg.get("fast_recovery_event_triggers", [])),
        events_file=ctx.fast_recovery_events_file,
    )
    for phase in ("restart_observed", "recovery_unconfirmed", "auto_recovered"):
        phase_events = [event for event in recovery_events if event.get("_notification_phase") == phase]
        if phase_events:
            messages.append((phase, phase_events))

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

    if recovery_events and not dry_run:
        mark_fast_recovery_recovery_events_notified(state, recovery_events, now_ts=now)

    if not dry_run:
        ctx.save_state(state)
    if not messages and pending <= 0 and sent <= 0 and failures <= 0:
        print("[notify-status] no active incidents; no notification due")
    else:
        print(f"[notify-status] messages={len(messages)} sent={sent} failures={failures} pending={pending}")
    return 0 if failures == 0 else 1
