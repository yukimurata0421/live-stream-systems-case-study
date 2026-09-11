from __future__ import annotations

from typing import Any, Mapping

from ._operational_base import OperationalSnapshotAdapter
from .snapshot_support import primitive_fields


def recovery_status(payload: Mapping[str, Any]) -> str:
    action = str(payload.get("action", "")).strip().lower()
    if not action:
        return "unknown"
    execute = payload.get("execute")
    if not isinstance(execute, bool):
        return "unknown"
    return "good" if action == "none" and execute is False else "bad"


class RecoveryPolicyAdapter(OperationalSnapshotAdapter):
    domain = "recovery_policy"
    source = "recovery_plan"
    timestamp_keys = ("ts_utc",)
    freshness_limit_sec = 180

    def status(self, payload: Mapping[str, Any]) -> str:
        return recovery_status(payload)

    def safe_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return primitive_fields(
            payload,
            ("action", "scope", "mode", "executable", "execute", "reason"),
        )
