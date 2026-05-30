from __future__ import annotations

from typing import Callable


def read_quota_guard(now_ts: int, *, quota_guard_status: Callable[[int], tuple[bool, str, dict]]) -> dict:
    active, reason, state = quota_guard_status(now_ts)
    return {"active": active, "reason": reason, "state": state}


def read_oauth_probe(*, probe_with_oauth: Callable[[], object]) -> object:
    return probe_with_oauth()
