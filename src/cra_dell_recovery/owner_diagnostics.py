"""Bounded, credential-free diagnostics carried in signed owner facts.

This contract has no process, database, network or effect access. Historical
lifecycle v1 remains readable; lifecycle v2 requires these diagnostics.
"""

from __future__ import annotations

import errno
from typing import Any

DIAGNOSTICS_SCHEMA = "runtime.owner_read_diagnostics.v2"

STAGES = frozenset(
    {
        "owner_stat",
        "process_supplier",
        "task_list",
        "thread_children",
        "child_stat",
        "child_command",
        "child_command_final",
        "child_stat_final",
        "child_registry",
        "task_list_final",
        "child_set_final",
        "retry_anchor",
        "anchor_runtime",
        "child_identity",
        "target_snapshot",
        "process_identity",
        "absence_check",
        "snapshot_consistency",
        "activation",
        "boot_snapshot",
        "ledger_snapshot",
        "snapshot_deadline",
        "export_write",
    }
)
ERROR_CODES = frozenset(
    {
        "RECOVERY_OWNER_ACTIVATION_UNAVAILABLE",
        "RECOVERY_OWNER_ANCHOR_INVALID",
        "RECOVERY_OWNER_ANCHOR_RUNTIME_CHANGED",
        "RECOVERY_OWNER_AUXILIARY_DISAPPEARED",
        "RECOVERY_OWNER_AUXILIARY_COMMAND_UNAVAILABLE",
        "RECOVERY_OWNER_AUXILIARY_EXITED",
        "RECOVERY_OWNER_CHILD_CARDINALITY_INVALID",
        "RECOVERY_OWNER_CHILD_REGISTRY_IDENTITY_INVALID",
        "RECOVERY_OWNER_CHILD_REGISTRY_UNAVAILABLE",
        "RECOVERY_OWNER_CHILD_UNREGISTERED",
        "RECOVERY_OWNER_CHILD_CHANGED_DURING_READ",
        "RECOVERY_OWNER_CHILD_COMMAND_INVALID",
        "RECOVERY_OWNER_CHILD_COMMAND_CHANGED",
        "RECOVERY_OWNER_MANAGED_CHILD_DISAPPEARED",
        "RECOVERY_OWNER_REGISTERED_CHILD_DISAPPEARED",
        "RECOVERY_OWNER_AUXILIARY_EXECUTABLE_CHANGED",
        "RECOVERY_OWNER_AUXILIARY_MATCHES_DELIVERY_EXECUTABLE",
        "RECOVERY_OWNER_AUXILIARY_EXECUTABLE_UNAVAILABLE",
        "RECOVERY_OWNER_DELIVERY_CHILD_UNBOUND",
        "RECOVERY_OWNER_CHILD_IDENTITY_INVALID",
        "RECOVERY_OWNER_CHILD_NOT_ANCHORED",
        "RECOVERY_OWNER_CHILD_STATE_UNPROVEN",
        "RECOVERY_OWNER_CHILD_PARENT_CHANGED",
        "RECOVERY_OWNER_CHILD_SET_CHANGED",
        "RECOVERY_OWNER_CHILD_SET_INVALID",
        "RECOVERY_OWNER_CHILD_SET_TOO_LARGE",
        "RECOVERY_OWNER_CHILD_STATE_INVALID",
        "RECOVERY_OWNER_CLOCK_REGRESSION",
        "RECOVERY_OWNER_CONFIG_INVALID",
        "RECOVERY_OWNER_CONTEXT_CHANGED",
        "RECOVERY_OWNER_DIAGNOSTIC_LIMIT",
        "RECOVERY_OWNER_DIRECTORY_OWNER_INVALID",
        "RECOVERY_OWNER_DIRECTORY_UNSAFE",
        "RECOVERY_OWNER_EXPORT_TOO_LARGE",
        "RECOVERY_OWNER_EXPORT_UNAVAILABLE",
        "RECOVERY_OWNER_FALSE_CHILD_ABSENCE",
        "RECOVERY_OWNER_HOST_OR_BOOT_MISMATCH",
        "RECOVERY_OWNER_LEDGER_INVALID",
        "RECOVERY_OWNER_LIFECYCLE_UNKNOWN",
        "RECOVERY_OWNER_NOT_LIVE",
        "RECOVERY_OWNER_NOT_LIVE_DIRECT_CHILD",
        "RECOVERY_OWNER_OUTPUT_UNSAFE",
        "RECOVERY_OWNER_PATH_COLLISION_OR_RELATIVE",
        "RECOVERY_OWNER_PATH_INVALID",
        "RECOVERY_OWNER_PHYSICAL_BOOT_CHANGED",
        "RECOVERY_OWNER_PHYSICAL_BOOT_MISSING",
        "RECOVERY_OWNER_PID_NAMESPACE_OR_GENERATION_MISMATCH",
        "RECOVERY_OWNER_PROCESS_IDENTITY_INVALID",
        "RECOVERY_OWNER_PROC_RETRY_DEADLINE",
        "RECOVERY_OWNER_PROC_RETRY_EXHAUSTED",
        "RECOVERY_OWNER_PROC_STAT_INVALID",
        "RECOVERY_OWNER_RETRY_ANCHOR_CHANGED",
        "RECOVERY_OWNER_SNAPSHOT_DEADLINE",
        "RECOVERY_OWNER_SOURCE_TOO_LARGE",
        "RECOVERY_OWNER_SOURCE_UNSAFE",
        "RECOVERY_OWNER_TARGET_CHANGED_DURING_EXPORT",
        "RECOVERY_OWNER_TARGET_STALE_OR_INVALID",
        "RECOVERY_OWNER_TASK_RETIRED",
        "RECOVERY_OWNER_TASK_SET_CHANGED",
        "RECOVERY_OWNER_TASK_SET_INVALID",
        "SQL_READ_FAILED",
        "FILE_DISAPPEARED",
        "PERMISSION_DENIED",
        "OS_READ_FAILED",
        "INVALID_VALUE",
    }
)


def empty_diagnostics() -> dict[str, Any]:
    return {
        "schema": DIAGNOSTICS_SCHEMA,
        "proc_scan_attempt_count": 0,
        "proc_scan_retry_count": 0,
        "events": [],
    }


def diagnostic_event(stage: str, error: BaseException, *, process: dict[str, Any] | None = None) -> dict[str, Any]:
    number = error.errno if isinstance(error, OSError) else None
    if isinstance(error, OSError):
        code = {
            errno.ENOENT: "FILE_DISAPPEARED",
            errno.EACCES: "PERMISSION_DENIED",
            errno.EPERM: "PERMISSION_DENIED",
        }.get(number if number is not None else -1, "OS_READ_FAILED")
    else:
        code = str(error) if str(error) in ERROR_CODES else "INVALID_VALUE"
    return {
        "stage": stage,
        "code": code,
        "errno": number if type(number) is int and 0 <= number <= 4095 else None,
        "process": process,
    }


def _validate_process(value: object) -> None:
    fields = {
        "pid",
        "state",
        "parent_pid",
        "start_ticks",
        "command_bytes",
        "anchor_relation",
        "executable_relation",
        "anchor_target_sha256",
        "owner_state_relation",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or any(type(value[name]) is not int or value[name] < 1 for name in ("pid", "parent_pid", "start_ticks"))
        or value["state"] not in {"R", "S", "D", "I", "T", "t", "X", "x", "Z"}
        or value["command_bytes"] != 0
        or value["anchor_relation"] not in {"MANAGED", "AUXILIARY", "UNPROVEN"}
        or value["executable_relation"] not in {"SAME", "DIFFERENT", "UNAVAILABLE"}
        or value["owner_state_relation"] not in {"MATCHED", "UNPROVEN"}
        or (
            value["anchor_target_sha256"] is not None
            and (
                not isinstance(value["anchor_target_sha256"], str)
                or len(value["anchor_target_sha256"]) != 64
                or any(character not in "0123456789abcdef" for character in value["anchor_target_sha256"])
            )
        )
    ):
        raise ValueError("OWNER_READ_DIAGNOSTICS_PROCESS_INVALID")


def validate_diagnostics(value: object) -> None:
    if not isinstance(value, dict):
        raise ValueError("OWNER_READ_DIAGNOSTICS_FIELDS_INVALID")
    legacy = set(value) == {"proc_scan_attempt_count", "proc_scan_retry_count", "events"}
    current = set(value) == {"schema", "proc_scan_attempt_count", "proc_scan_retry_count", "events"}
    if not legacy and not current:
        raise ValueError("OWNER_READ_DIAGNOSTICS_FIELDS_INVALID")
    if current and value.get("schema") != DIAGNOSTICS_SCHEMA:
        raise ValueError("OWNER_READ_DIAGNOSTICS_SCHEMA_INVALID")
    attempts, retries, events = value["proc_scan_attempt_count"], value["proc_scan_retry_count"], value["events"]
    if (
        type(attempts) is not int
        or not 0 <= attempts <= 6
        or type(retries) is not int
        or not 0 <= retries <= 4
        or retries > max(0, attempts - 1)
        or not isinstance(events, list)
        or len(events) > 16
        or retries > len(events)
    ):
        raise ValueError("OWNER_READ_DIAGNOSTICS_BOUNDS_INVALID")
    for event in events:
        event_fields = {"stage", "code", "errno"} if legacy else {"stage", "code", "errno", "process"}
        if (
            not isinstance(event, dict)
            or set(event) != event_fields
            or not isinstance(event["stage"], str)
            or event["stage"] not in STAGES
            or not isinstance(event["code"], str)
            or event["code"] not in ERROR_CODES
            or (event["errno"] is not None and (type(event["errno"]) is not int or not 0 <= event["errno"] <= 4095))
        ):
            raise ValueError("OWNER_READ_DIAGNOSTICS_EVENT_INVALID")
        if not legacy and event["process"] is not None:
            _validate_process(event["process"])
