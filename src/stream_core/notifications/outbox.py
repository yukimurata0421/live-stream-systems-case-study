from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Callable

try:
    from stream_core.common.json_io import append_jsonl, write_jsonl_atomic
    from stream_core.common.timeutil import utc_now_text
except ModuleNotFoundError:
    from common.json_io import append_jsonl, write_jsonl_atomic
    from common.timeutil import utc_now_text


SendWebhook = Callable[[str, str], tuple[bool, str]]
DEFAULT_ROUTE = "discord"


def notify_route(item: dict) -> str:
    return str(item.get("route", DEFAULT_ROUTE) or DEFAULT_ROUTE)


def load_notify_outbox(path: Path, *, now_ts: int | None = None, ttl_sec: int | None = None) -> list[dict]:
    now = int(time.time() if now_ts is None else now_ts)
    ttl = max(0, int(ttl_sec or 0))
    rows: list[dict] = []
    if not path.exists():
        return rows
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(item, dict):
            continue
        created_ts = int(item.get("created_ts", 0) or 0)
        if ttl > 0 and created_ts > 0 and now - created_ts > ttl:
            continue
        if str(item.get("status", "pending")) != "pending":
            continue
        rows.append(item)
    return rows


def save_notify_outbox(path: Path, rows: list[dict]) -> None:
    write_jsonl_atomic(path, rows)


def notify_message_id(*, phase: str, incidents: list[dict], now_ts: int, route: str = DEFAULT_ROUTE) -> str:
    ids = ",".join(sorted(str(item.get("id", "")) for item in incidents if item.get("id")))
    if phase == "status":
        base = f"status|{ids}"
    elif phase == "detected":
        first_seen = min(
            (int(item.get("_first_seen_ts", item.get("observed_ts", now_ts)) or now_ts) for item in incidents),
            default=now_ts,
        )
        base = f"detected|{ids}|{first_seen}"
    elif phase == "recovered":
        recovered_ts = max((int(item.get("_recovered_ts", now_ts) or now_ts) for item in incidents), default=now_ts)
        base = f"recovered|{ids}|{recovered_ts}"
    elif phase in {"restart_observed", "recovery_unconfirmed", "auto_recovered"}:
        event_ts = max((int(item.get("observed_ts", now_ts) or now_ts) for item in incidents), default=now_ts)
        base = f"{phase}|{ids}|{event_ts}"
    else:
        base = f"{phase}|{ids}|{now_ts}"
    route_name = str(route or DEFAULT_ROUTE)
    if route_name == DEFAULT_ROUTE:
        return base
    return f"{route_name}|{base}"


def enqueue_notify_messages(
    outbox: list[dict],
    messages: list[tuple[str, list[dict], str]],
    *,
    username: str,
    now_ts: int,
    max_pending: int,
    route: str = DEFAULT_ROUTE,
) -> list[dict]:
    route_name = str(route or DEFAULT_ROUTE)
    by_id = {str(item.get("message_id")): dict(item) for item in outbox if item.get("message_id")}
    order = [str(item.get("message_id")) for item in outbox if item.get("message_id")]
    for phase, phase_incidents, content in messages:
        message_id = notify_message_id(phase=phase, incidents=phase_incidents, now_ts=now_ts, route=route_name)
        incident_ids = [item.get("id") for item in phase_incidents]
        existing = by_id.get(message_id)
        if existing:
            existing.update(
                {
                    "updated_ts": now_ts,
                    "updated_ts_utc": utc_now_text(now_ts),
                    "phase": phase,
                    "incident_ids": incident_ids,
                    "content": content,
                    "username": username,
                    "route": route_name,
                    "status": "pending",
                }
            )
            by_id[message_id] = existing
            continue
        by_id[message_id] = {
            "message_id": message_id,
            "phase": phase,
            "incident_ids": incident_ids,
            "content": content,
            "username": username,
            "route": route_name,
            "status": "pending",
            "attempts": 0,
            "created_ts": now_ts,
            "created_ts_utc": utc_now_text(now_ts),
            "updated_ts": now_ts,
            "updated_ts_utc": utc_now_text(now_ts),
            "last_error": "",
        }
        order.append(message_id)
    rows = [by_id[mid] for mid in order if mid in by_id]
    same_route_ids = [str(item.get("message_id")) for item in rows if notify_route(item) == route_name]
    keep_same_route = set(same_route_ids[-max(1, int(max_pending)) :])
    return [
        item
        for item in rows
        if notify_route(item) != route_name or str(item.get("message_id")) in keep_same_route
    ]


def flush_notify_outbox(
    *,
    outbox_path: Path,
    events_path: Path,
    cfg: dict,
    now_ts: int,
    send_webhook: Callable[..., tuple[bool, str]],
    dry_run: bool = False,
    route: str = DEFAULT_ROUTE,
) -> tuple[int, int, int]:
    route_name = str(route or DEFAULT_ROUTE)
    outbox = load_notify_outbox(outbox_path, now_ts=now_ts, ttl_sec=int(cfg["outbox_ttl_sec"]))
    if dry_run or not cfg["enabled"] or not cfg["webhook_url"]:
        return 0, 0, sum(1 for item in outbox if notify_route(item) == route_name)
    remaining: list[dict] = []
    sent = 0
    failures = 0
    flush_limit = max(1, int(cfg["outbox_flush_limit"]))
    attempted = 0
    for item in outbox:
        if notify_route(item) != route_name:
            remaining.append(item)
            continue
        if attempted >= flush_limit:
            remaining.append(item)
            continue
        attempted += 1
        content = str(item.get("content", ""))
        username = str(item.get("username", cfg["username"]))
        ok, reason = send_webhook(str(cfg["webhook_url"]), content, username=username)
        attempts = int(item.get("attempts", 0) or 0) + 1
        event = {
            "ts_utc": utc_now_text(now_ts),
            "phase": item.get("phase", ""),
            "incident_ids": item.get("incident_ids", []),
            "dry_run": False,
            "enabled": cfg["enabled"],
            "route": route_name,
            "message": content,
            "message_id": item.get("message_id", ""),
            "outbox": True,
            "outbox_attempt": attempts,
            "queued_ts_utc": item.get("created_ts_utc", ""),
            "send_ok": ok,
            "send_reason": reason,
        }
        append_jsonl(events_path, event)
        if ok:
            sent += 1
        else:
            failures += 1
            item["attempts"] = attempts
            item["last_error"] = reason
            item["updated_ts"] = now_ts
            item["updated_ts_utc"] = utc_now_text(now_ts)
            remaining.append(item)
    save_notify_outbox(outbox_path, remaining)
    pending = sum(1 for item in remaining if notify_route(item) == route_name)
    return sent, failures, pending
