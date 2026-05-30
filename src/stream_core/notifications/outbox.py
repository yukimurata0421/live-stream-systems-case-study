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


def notify_message_id(*, phase: str, incidents: list[dict], now_ts: int) -> str:
    ids = ",".join(sorted(str(item.get("id", "")) for item in incidents if item.get("id")))
    if phase == "status":
        return f"status|{ids}"
    if phase == "detected":
        first_seen = min(
            (int(item.get("_first_seen_ts", item.get("observed_ts", now_ts)) or now_ts) for item in incidents),
            default=now_ts,
        )
        return f"detected|{ids}|{first_seen}"
    if phase == "recovered":
        recovered_ts = max((int(item.get("_recovered_ts", now_ts) or now_ts) for item in incidents), default=now_ts)
        return f"recovered|{ids}|{recovered_ts}"
    if phase == "auto_recovered":
        event_ts = max((int(item.get("observed_ts", now_ts) or now_ts) for item in incidents), default=now_ts)
        return f"auto_recovered|{ids}|{event_ts}"
    return f"{phase}|{ids}|{now_ts}"


def enqueue_notify_messages(
    outbox: list[dict],
    messages: list[tuple[str, list[dict], str]],
    *,
    username: str,
    now_ts: int,
    max_pending: int,
) -> list[dict]:
    by_id = {str(item.get("message_id")): dict(item) for item in outbox if item.get("message_id")}
    order = [str(item.get("message_id")) for item in outbox if item.get("message_id")]
    for phase, phase_incidents, content in messages:
        message_id = notify_message_id(phase=phase, incidents=phase_incidents, now_ts=now_ts)
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
    return rows[-max(1, int(max_pending)) :]


def flush_notify_outbox(
    *,
    outbox_path: Path,
    events_path: Path,
    cfg: dict,
    now_ts: int,
    send_webhook: Callable[..., tuple[bool, str]],
    dry_run: bool = False,
) -> tuple[int, int, int]:
    outbox = load_notify_outbox(outbox_path, now_ts=now_ts, ttl_sec=int(cfg["outbox_ttl_sec"]))
    if dry_run or not cfg["enabled"] or not cfg["webhook_url"]:
        return 0, 0, len(outbox)
    remaining: list[dict] = []
    sent = 0
    failures = 0
    flush_limit = max(1, int(cfg["outbox_flush_limit"]))
    attempted = 0
    for item in outbox:
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
    return sent, failures, len(remaining)
