from __future__ import annotations

from typing import Any, Mapping

from ._operational_base import OperationalSnapshotAdapter, mapping, nonnegative_int


def _bounded_task_failures(tasks: Mapping[str, Any]) -> int:
    total = 0
    for item in tasks.values():
        failures = nonnegative_int(mapping(item).get("consecutive_failures"))
        if failures is not None:
            total += failures
    return total


def control_status(payload: Mapping[str, Any]) -> str:
    if payload.get("schema") != "stream_v3.control_loop_state.v2":
        return "unknown"
    tasks = mapping(payload.get("tasks"))
    configured = nonnegative_int(payload.get("configured_task_count"))
    failed = payload.get("failed_tasks")
    stale = payload.get("stale_or_unobserved_tasks")
    if (
        configured is None
        or configured != len(tasks)
        or not isinstance(failed, list)
        or not isinstance(stale, list)
        or any(not isinstance(item, Mapping) for item in tasks.values())
    ):
        return "unknown"
    return (
        "good"
        if (
            payload.get("ok") is True
            and payload.get("fresh") is True
            and payload.get("all_tasks_observed") is True
            and not failed
            and not stale
            and all(mapping(item).get("status") == "good" for item in tasks.values())
        )
        else "bad"
    )


class ControlLoopAdapter(OperationalSnapshotAdapter):
    domain = "control_loop"
    source = "control_loop_state"
    timestamp_keys = ("updated_at_utc",)
    freshness_limit_sec = 180

    def status(self, payload: Mapping[str, Any]) -> str:
        return control_status(payload)

    def safe_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        tasks = mapping(payload.get("tasks"))
        return {
            "schema": str(payload.get("schema", ""))[:96],
            "mode": str(payload.get("mode", ""))[:32],
            "configured_task_count": payload.get("configured_task_count"),
            "observed_task_count": len(tasks),
            "all_tasks_observed": payload.get("all_tasks_observed") is True,
            "fresh": payload.get("fresh") is True,
            "ok": payload.get("ok") is True,
            "failed_task_count": (
                len(payload.get("failed_tasks", []))
                if isinstance(payload.get("failed_tasks"), list)
                else 0
            ),
            "stale_or_unobserved_task_count": (
                len(payload.get("stale_or_unobserved_tasks", []))
                if isinstance(payload.get("stale_or_unobserved_tasks"), list)
                else 0
            ),
            "consecutive_failure_total": _bounded_task_failures(tasks),
        }
