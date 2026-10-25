"""Pure facts contract for explicitly frozen, unresolved historical effects.

No database, signing, action or automatic retirement admission. An exception
is pinned before the new epoch and never changes UNKNOWN to success.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any

import rfc8785

from .time import parse_utc

POLICY_SCHEMA = "cra.recovery_effect_history_policy.v1"
ENTRY_FIELDS = {
    "effect_scope_id",
    "target_sha256",
    "retirement_sha256",
    "created_at",
    "retired_at",
    "physical_attempt_count",
    "physical_effect_outcome",
}
HISTORY_FIELDS = {
    "history_policy_sha256",
    "historical_retired_unknown",
    "unresolved_scope_count_total",
    "current_target_unresolved_scope_count",
    "current_target_sha256",
}


def history_hash(value: dict[str, Any]) -> str:
    return hashlib.sha256(rfc8785.dumps({"raw": value})).hexdigest()


def empty_policy() -> dict[str, Any]:
    return {"schema": POLICY_SCHEMA, "frozen_at": None, "retired_unknown": []}


def validate_policy(value: dict[str, Any] | None, *, now: datetime | None = None) -> dict[str, Any]:
    policy = empty_policy() if value is None else value
    if not isinstance(policy, dict) or set(policy) != {"schema", "frozen_at", "retired_unknown"} or policy["schema"] != POLICY_SCHEMA:
        raise ValueError("EFFECT_HISTORY_POLICY_INVALID")
    entries = policy["retired_unknown"]
    if not isinstance(entries, list) or len(entries) > 16:
        raise ValueError("EFFECT_HISTORY_POLICY_ENTRIES_INVALID")
    if not entries:
        if policy["frozen_at"] is not None:
            raise ValueError("EFFECT_HISTORY_EMPTY_POLICY_FREEZE_INVALID")
        return empty_policy()
    if not isinstance(policy["frozen_at"], str):
        raise ValueError("EFFECT_HISTORY_FREEZE_TIME_REQUIRED")
    frozen = parse_utc(policy["frozen_at"])
    if now is not None and frozen > now:
        raise ValueError("EFFECT_HISTORY_FREEZE_AFTER_EPOCH")
    ids: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != ENTRY_FIELDS:
            raise ValueError("EFFECT_HISTORY_ENTRY_INVALID")
        if any(
            not isinstance(entry[k], str) or re.fullmatch(r"[0-9a-f]{64}", entry[k]) is None
            for k in ("effect_scope_id", "target_sha256", "retirement_sha256")
        ):
            raise ValueError("EFFECT_HISTORY_ENTRY_HASH_INVALID")
        if (
            type(entry["physical_attempt_count"]) is not int
            or entry["physical_attempt_count"] != 1
            or entry["physical_effect_outcome"] != "UNKNOWN"
            or not isinstance(entry["created_at"], str)
            or not isinstance(entry["retired_at"], str)
            or not parse_utc(entry["created_at"]) <= parse_utc(entry["retired_at"]) < frozen
        ):
            raise ValueError("EFFECT_HISTORY_NOT_PRIOR_RETIRED_UNKNOWN")
        ids.append(entry["effect_scope_id"])
    if ids != sorted(set(ids)):
        raise ValueError("EFFECT_HISTORY_SCOPE_IDS_NOT_UNIQUE_SORTED")
    return {**policy, "retired_unknown": [dict(entry) for entry in entries]}


def validate_effect_history(
    effects: dict[str, Any], *, policy: dict[str, Any] | None, target: str | None, now: datetime, allow_unknown_target: bool = False
) -> None:
    expected = validate_policy(policy, now=now)
    entries = expected["retired_unknown"]
    validate_policy({**expected, "retired_unknown": effects.get("historical_retired_unknown")}, now=now)
    if effects.get("history_policy_sha256") != history_hash(expected) or effects.get("historical_retired_unknown") != entries:
        raise ValueError("EFFECT_HISTORY_FROZEN_SET_CHANGED")
    current = effects.get("current_target_sha256")
    target_status = effects.get("target_status")
    if target_status not in (None, "VALID", "UNKNOWN"):
        raise ValueError("EFFECT_HISTORY_TARGET_STATUS_INVALID")
    unknown_target = allow_unknown_target and target_status == "UNKNOWN" and current is None
    if target_status == "UNKNOWN" and not unknown_target:
        raise ValueError("EFFECT_HISTORY_CURRENT_TARGET_UNBOUND")
    if not unknown_target and (not isinstance(current, str) or not current or (target is not None and current != target)):
        raise ValueError("EFFECT_HISTORY_CURRENT_TARGET_UNBOUND")
    if any(entry["target_sha256"] == current for entry in entries):
        raise ValueError("EFFECT_HISTORY_RETIRED_TARGET_REAPPEARED")
    counters = ("unresolved_scope_count", "unresolved_scope_count_total", "physical_attempt_count")
    if any(type(effects.get(k)) is not int or effects[k] < 0 for k in counters):
        raise ValueError("EFFECT_HISTORY_COUNTER_INVALID")
    current_count = effects.get("current_target_unresolved_scope_count")
    if (unknown_target and current_count is not None) or (not unknown_target and (type(current_count) is not int or current_count < 0)):
        raise ValueError("EFFECT_HISTORY_CURRENT_TARGET_COUNTER_INVALID")
    if (
        effects["unresolved_scope_count_total"] != effects["unresolved_scope_count"] + len(entries)
        or (not unknown_target and current_count > effects["unresolved_scope_count"])
        or effects["physical_attempt_count"] < len(entries)
    ):
        raise ValueError("EFFECT_HISTORY_COUNTER_PARTITION_INVALID")
