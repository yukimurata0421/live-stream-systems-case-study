from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .jsonio import atomic_write_json, read_json
from .timeutil import isoformat_utc, now_utc, parse_utc


@dataclass(frozen=True)
class LockState:
    active: bool
    stale: bool
    reason: str
    lock_owner_event_id: str = ""
    action: str = ""
    scope: str = ""

    def to_gate(self) -> dict[str, Any]:
        return {
            "passed": not self.active,
            "reason": self.reason,
            "lock_owner_event_id": self.lock_owner_event_id,
            "action": self.action,
            "scope": self.scope,
            "stale": self.stale,
        }


class FileActionLock:
    """TTL based global lock for destructive recovery actions."""

    def __init__(self, path: Path):
        self.path = path

    def check(self) -> LockState:
        payload = read_json(self.path)
        if not payload:
            return LockState(False, False, "no_destructive_action_in_progress")
        acquired_at = parse_utc(payload.get("acquired_at_utc"))
        ttl_sec = float(payload.get("ttl_sec") or 0)
        if acquired_at is None or ttl_sec <= 0:
            return LockState(False, True, "stale_lock_invalid_record")
        age = (now_utc() - acquired_at).total_seconds()
        if age > ttl_sec:
            return LockState(False, True, "stale_lock_expired", str(payload.get("lock_owner_event_id", "")), str(payload.get("action", "")), str(payload.get("scope", "")))
        return LockState(True, False, "destructive_action_in_progress", str(payload.get("lock_owner_event_id", "")), str(payload.get("action", "")), str(payload.get("scope", "")))

    def acquire_record(self, *, lock_owner_event_id: str, action: str, scope: str, ttl_sec: float) -> dict[str, Any]:
        payload = {
            "lock_owner_event_id": lock_owner_event_id,
            "action": action,
            "scope": scope,
            "acquired_at_utc": isoformat_utc(now_utc()),
            "ttl_sec": ttl_sec,
            "owner_pid": os.getpid(),
        }
        atomic_write_json(self.path, payload)
        return payload

    def cleanup_stale(self) -> LockState:
        state = self.check()
        if state.stale and self.path.exists():
            self.path.unlink()
        return state

    def acquire(self, *, lock_owner_event_id: str, action: str, scope: str, ttl_sec: float) -> tuple[bool, LockState, dict[str, Any]]:
        before = self.cleanup_stale()
        if before.active:
            return False, before, {}
        payload = self.acquire_record(
            lock_owner_event_id=lock_owner_event_id,
            action=action,
            scope=scope,
            ttl_sec=ttl_sec,
        )
        return True, self.check(), payload

    def release(self, *, lock_owner_event_id: str) -> bool:
        payload = read_json(self.path)
        if not payload:
            return False
        if str(payload.get("lock_owner_event_id", "")) != lock_owner_event_id:
            return False
        self.path.unlink(missing_ok=True)
        return True
