from __future__ import annotations

import argparse
import fcntl
import json
import os
import stat
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra_no_action_soak.gate import (
    CREDENTIAL_FIELDS,
    HOST_FIELDS,
    INFRASTRUCTURE_FIELDS,
    INVOCATION_ID,
    OPERATION_FIELDS,
    PERSISTENT_SERVICE_FIELDS,
    RECOVERY_FIELDS,
    RECOVERY_HEALTH_FIELDS,
    REHEARSAL_FIELDS,
    SCHEMA,
    SERVICE_FAILURE_FIELDS,
    SHA256,
)
from cra_no_action_soak.time import isoformat_utc, parse_utc

CAPTURE_SCHEMA = "cra.no_action_soak_capture.v9"
DB_SUMMARY_SCHEMA = "cra.no_action_db_summary.v1"
DELL_SUMMARY_SCHEMA = "cra.dell_observation_soak_summary.v1"
RESTART_SUMMARY_SCHEMA = "cra.no_action_restart_summary.v1"
INFRASTRUCTURE_SUMMARY_SCHEMA = "cra.no_action_infrastructure_summary.v4"
OPERATION_SUMMARY_SCHEMA = "cra.no_action_operation_summary.v1"


def _read_json(path: Path, *, maximum_bytes: int = 2 * 1024 * 1024) -> dict[str, Any]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("SOAK_CAPTURE_INPUT_NOT_REGULAR_FILE")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("SOAK_CAPTURE_INPUT_PERMISSIONS_UNSAFE")
        if metadata.st_size > maximum_bytes:
            raise ValueError("SOAK_CAPTURE_INPUT_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > maximum_bytes:
        raise ValueError("SOAK_CAPTURE_INPUT_TOO_LARGE")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("SOAK_CAPTURE_INPUT_NOT_OBJECT")
    return dict(value)


def _integer(value: object, code: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(code)
    return value


def _exact_mapping(value: object, fields: frozenset[str], code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError(code)
    return dict(value)


def _string_map(value: object, fields: frozenset[str], code: str, *, sha256: bool = False) -> dict[str, str]:
    item = _exact_mapping(value, fields, code)
    result: dict[str, str] = {}
    for name, raw in item.items():
        if not isinstance(raw, str) or not raw.strip() or (sha256 and SHA256.fullmatch(raw) is None):
            raise ValueError(code)
        result[name] = raw
    return result


def _integer_map(value: object, fields: frozenset[str], code: str) -> dict[str, int]:
    item = _exact_mapping(value, fields, code)
    return {name: _integer(raw, code) for name, raw in item.items()}


def _fresh(value: Mapping[str, Any], current: datetime, maximum_age_seconds: float, code: str) -> None:
    observed = parse_utc(str(value.get("observed_at", "")))
    age = (current - observed).total_seconds()
    if observed > current or age > maximum_age_seconds:
        raise ValueError(code)


def _release(value: Mapping[str, Any], field: str, expected: str, code: str) -> None:
    if value.get(field) != expected:
        raise ValueError(code)


def compose_no_action_sample(
    capture: Mapping[str, Any],
    *,
    expected_releases: Mapping[str, str],
    now: datetime | None = None,
    maximum_input_age_seconds: float = 120.0,
) -> dict[str, Any]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    required = {
        "schema",
        "arena_dell_pull",
        "arena_live_adapter",
        "arena_projection",
        "cra_projection_pull",
        "cra_runtime",
        "cra_database",
        "dell",
        "infrastructure",
        "restarts",
        "operations",
    }
    if set(capture) != required or capture.get("schema") != CAPTURE_SCHEMA:
        raise ValueError("SOAK_CAPTURE_FIELDS_NOT_EXACT")
    if set(expected_releases) != {"arena", "cra", "dell"} or not all(
        isinstance(value, str) and value for value in expected_releases.values()
    ):
        raise ValueError("SOAK_CAPTURE_RELEASES_INVALID")
    if (
        isinstance(maximum_input_age_seconds, bool)
        or not isinstance(maximum_input_age_seconds, (int, float))
        or not 1 <= maximum_input_age_seconds <= 600
    ):
        raise ValueError("SOAK_CAPTURE_MAXIMUM_AGE_INVALID")
    values: dict[str, dict[str, Any]] = {}
    for name in required - {"schema"}:
        item = capture[name]
        if not isinstance(item, Mapping):
            raise ValueError(f"SOAK_CAPTURE_SECTION_INVALID:{name}")
        values[name] = dict(item)
        _fresh(values[name], current, maximum_input_age_seconds, f"SOAK_CAPTURE_SECTION_STALE:{name}")

    arena_release = expected_releases["arena"]
    cra_release = expected_releases["cra"]
    dell_release = expected_releases["dell"]
    arena_pull = values["arena_dell_pull"]
    arena_adapter = values["arena_live_adapter"]
    arena_projection = values["arena_projection"]
    cra_pull = values["cra_projection_pull"]
    cra_runtime = values["cra_runtime"]
    cra_database = values["cra_database"]
    dell = values["dell"]
    restarts = values["restarts"]
    infrastructure = values["infrastructure"]
    operations = values["operations"]

    if arena_pull.get("schema") != "monitoring_v4.dell_observation_pull_status.v1":
        raise ValueError("SOAK_CAPTURE_ARENA_PULL_SCHEMA_INVALID")
    if arena_adapter.get("schema") != "monitoring_v4.cra_live_adapter_status.v2":
        raise ValueError("SOAK_CAPTURE_ARENA_ADAPTER_SCHEMA_INVALID")
    if arena_projection.get("schema") != "monitoring_v4.cra_projection_producer_status.v1":
        raise ValueError("SOAK_CAPTURE_ARENA_PROJECTION_SCHEMA_INVALID")
    if cra_pull.get("schema") != "cra.monitoring_projection_pull_status.v1":
        raise ValueError("SOAK_CAPTURE_CRA_PULL_SCHEMA_INVALID")
    if cra_runtime.get("schema") != "cra.runtime_status.v1":
        raise ValueError("SOAK_CAPTURE_CRA_RUNTIME_SCHEMA_INVALID")
    if cra_database.get("schema") != DB_SUMMARY_SCHEMA:
        raise ValueError("SOAK_CAPTURE_CRA_DATABASE_SCHEMA_INVALID")
    if dell.get("schema") != DELL_SUMMARY_SCHEMA:
        raise ValueError("SOAK_CAPTURE_DELL_SCHEMA_INVALID")
    if restarts.get("schema") != RESTART_SUMMARY_SCHEMA:
        raise ValueError("SOAK_CAPTURE_RESTART_SCHEMA_INVALID")
    if set(operations) != OPERATION_FIELDS | {"schema", "observed_at"} or operations.get("schema") != OPERATION_SUMMARY_SCHEMA:
        raise ValueError("SOAK_CAPTURE_OPERATION_SCHEMA_INVALID")
    expected_infrastructure_fields = INFRASTRUCTURE_FIELDS | {"schema", "observed_at"}
    if set(infrastructure) != expected_infrastructure_fields or infrastructure.get("schema") != INFRASTRUCTURE_SUMMARY_SCHEMA:
        raise ValueError("SOAK_CAPTURE_INFRASTRUCTURE_SCHEMA_INVALID")

    _release(arena_pull, "puller_release_id", arena_release, "SOAK_CAPTURE_ARENA_PULL_RELEASE_MISMATCH")
    _release(arena_adapter, "adapter_release_id", arena_release, "SOAK_CAPTURE_ARENA_ADAPTER_RELEASE_MISMATCH")
    _release(arena_projection, "producer_release_id", arena_release, "SOAK_CAPTURE_ARENA_PROJECTION_RELEASE_MISMATCH")
    _release(cra_pull, "puller_release_id", cra_release, "SOAK_CAPTURE_CRA_PULL_RELEASE_MISMATCH")
    _release(cra_runtime, "runtime_release_id", cra_release, "SOAK_CAPTURE_CRA_RUNTIME_RELEASE_MISMATCH")
    _release(cra_database, "runtime_release_id", cra_release, "SOAK_CAPTURE_CRA_DATABASE_RELEASE_MISMATCH")
    _release(dell, "release_id", dell_release, "SOAK_CAPTURE_DELL_RELEASE_MISMATCH")

    arena_capabilities = [
        _integer(item.get("control_capability_count"), "SOAK_CAPTURE_ARENA_CONTROL_COUNT_INVALID")
        for item in (arena_pull, arena_adapter, arena_projection)
    ]
    arena_effects = [
        _integer(item.get("physical_effect_count"), "SOAK_CAPTURE_ARENA_EFFECT_COUNT_INVALID")
        for item in (arena_pull, arena_adapter, arena_projection)
    ]
    cra_capabilities = [
        _integer(item.get("control_capability_count"), "SOAK_CAPTURE_CRA_CONTROL_COUNT_INVALID") for item in (cra_pull, cra_runtime)
    ]
    cra_effects = [_integer(item.get("physical_effect_count"), "SOAK_CAPTURE_CRA_EFFECT_COUNT_INVALID") for item in (cra_pull, cra_runtime)]
    if cra_runtime.get("command_delivery_enabled") is not False:
        raise ValueError("SOAK_CAPTURE_COMMAND_DELIVERY_ENABLED")
    monitoring_readiness = _exact_mapping(
        cra_runtime.get("monitoring_readiness"),
        frozenset({"input_fresh", "parity_clean", "projection_clean", "source_ready"}),
        "SOAK_CAPTURE_MONITORING_READINESS_FIELDS_INVALID",
    )
    if any(value is not True and value is not False for value in monitoring_readiness.values()):
        raise ValueError("SOAK_CAPTURE_MONITORING_READINESS_VALUE_INVALID")
    monitoring_cycle_id = arena_adapter.get("monitoring_cycle_id")
    if not isinstance(monitoring_cycle_id, str) or not monitoring_cycle_id:
        raise ValueError("SOAK_CAPTURE_MONITORING_CYCLE_ID_INVALID")
    monitoring_parity = arena_adapter.get("parity")
    if not isinstance(monitoring_parity, Mapping):
        raise ValueError("SOAK_CAPTURE_MONITORING_PARITY_INVALID")
    raw_policy_blockers = cra_runtime.get("policy_blockers")
    if not isinstance(raw_policy_blockers, list) or not all(
        isinstance(item, str) and item and len(item) <= 160 for item in raw_policy_blockers
    ):
        raise ValueError("SOAK_CAPTURE_POLICY_BLOCKERS_INVALID")
    if len(set(raw_policy_blockers)) != len(raw_policy_blockers):
        raise ValueError("SOAK_CAPTURE_POLICY_BLOCKERS_DUPLICATE")
    policy_blockers = list(raw_policy_blockers)
    normalized_infrastructure: dict[str, Any] = {
        "host_boot_ids": _string_map(infrastructure.get("host_boot_ids"), HOST_FIELDS, "SOAK_CAPTURE_HOST_BOOT_IDS_INVALID"),
        "release_manifest_sha256": _string_map(
            infrastructure.get("release_manifest_sha256"),
            HOST_FIELDS,
            "SOAK_CAPTURE_RELEASE_MANIFEST_SHA256_INVALID",
            sha256=True,
        ),
        "runtime_manifest_sha256": _string_map(
            infrastructure.get("runtime_manifest_sha256"),
            HOST_FIELDS,
            "SOAK_CAPTURE_RUNTIME_MANIFEST_SHA256_INVALID",
            sha256=True,
        ),
        "configuration_set_sha256": _string_map(
            infrastructure.get("configuration_set_sha256"),
            HOST_FIELDS,
            "SOAK_CAPTURE_CONFIGURATION_SET_SHA256_INVALID",
            sha256=True,
        ),
        "maintenance_restart_policy_sha256": _string_map(
            infrastructure.get("maintenance_restart_policy_sha256"),
            HOST_FIELDS,
            "SOAK_CAPTURE_MAINTENANCE_RESTART_POLICY_SHA256_INVALID",
            sha256=True,
        ),
        "rehearsal_evidence_sha256": _string_map(
            infrastructure.get("rehearsal_evidence_sha256"),
            REHEARSAL_FIELDS,
            "SOAK_CAPTURE_REHEARSAL_EVIDENCE_SHA256_INVALID",
            sha256=True,
        ),
        "target_identity_sha256": str(infrastructure.get("target_identity_sha256")),
        "cra_database_integrity_check": infrastructure.get("cra_database_integrity_check"),
        "cra_database_journal_mode": infrastructure.get("cra_database_journal_mode"),
        "cra_archive_quick_check": infrastructure.get("cra_archive_quick_check"),
        "cra_archive_integrity_check": infrastructure.get("cra_archive_integrity_check"),
        "cra_single_writer_lock": infrastructure.get("cra_single_writer_lock"),
        "cra_backup_restore_rehearsal": infrastructure.get("cra_backup_restore_rehearsal"),
        "credential_rotation_rehearsal": infrastructure.get("credential_rotation_rehearsal"),
        "credential_minimum_remaining_seconds": _integer_map(
            infrastructure.get("credential_minimum_remaining_seconds"),
            CREDENTIAL_FIELDS,
            "SOAK_CAPTURE_CREDENTIAL_REMAINING_INVALID",
        ),
        "disk_free_bytes": _integer_map(infrastructure.get("disk_free_bytes"), HOST_FIELDS, "SOAK_CAPTURE_DISK_FREE_BYTES_INVALID"),
        "resident_memory_bytes": _integer_map(
            infrastructure.get("resident_memory_bytes"),
            HOST_FIELDS,
            "SOAK_CAPTURE_RESIDENT_MEMORY_BYTES_INVALID",
        ),
        "open_fd_count": _integer_map(infrastructure.get("open_fd_count"), HOST_FIELDS, "SOAK_CAPTURE_OPEN_FD_COUNT_INVALID"),
        "resource_limit_breach_count": _integer_map(
            infrastructure.get("resource_limit_breach_count"),
            HOST_FIELDS,
            "SOAK_CAPTURE_RESOURCE_LIMIT_BREACH_COUNT_INVALID",
        ),
        "service_failed_invocation_count": _integer_map(
            infrastructure.get("service_failed_invocation_count"),
            SERVICE_FAILURE_FIELDS,
            "SOAK_CAPTURE_SERVICE_FAILED_INVOCATION_COUNT_INVALID",
        ),
        "persistent_service_invocation_ids": _string_map(
            infrastructure.get("persistent_service_invocation_ids"),
            PERSISTENT_SERVICE_FIELDS,
            "SOAK_CAPTURE_PERSISTENT_SERVICE_INVOCATION_IDS_INVALID",
        ),
        "legacy_arena_recovery_active": infrastructure.get("legacy_arena_recovery_active"),
        "legacy_arena_recovery_restart_count": _integer(
            infrastructure.get("legacy_arena_recovery_restart_count"),
            "SOAK_CAPTURE_LEGACY_ARENA_RESTART_COUNT_INVALID",
        ),
        "executable_command_route_present": infrastructure.get("executable_command_route_present"),
        "transport_recovery_health": {
            name: _exact_mapping(raw, RECOVERY_HEALTH_FIELDS, "SOAK_CAPTURE_TRANSPORT_RECOVERY_STATE_INVALID")
            for name, raw in _exact_mapping(
                infrastructure.get("transport_recovery_health"),
                RECOVERY_FIELDS,
                "SOAK_CAPTURE_TRANSPORT_RECOVERY_HEALTH_INVALID",
            ).items()
        },
    }
    if SHA256.fullmatch(normalized_infrastructure["target_identity_sha256"]) is None:
        raise ValueError("SOAK_CAPTURE_TARGET_IDENTITY_SHA256_INVALID")
    if any(INVOCATION_ID.fullmatch(value) is None for value in normalized_infrastructure["persistent_service_invocation_ids"].values()):
        raise ValueError("SOAK_CAPTURE_PERSISTENT_SERVICE_INVOCATION_ID_INVALID")
    for name in (
        "cra_database_integrity_check",
        "cra_database_journal_mode",
        "cra_archive_quick_check",
        "cra_archive_integrity_check",
        "cra_single_writer_lock",
        "cra_backup_restore_rehearsal",
        "credential_rotation_rehearsal",
    ):
        if not isinstance(normalized_infrastructure[name], str) or not normalized_infrastructure[name]:
            raise ValueError(f"SOAK_CAPTURE_INFRASTRUCTURE_VALUE_INVALID:{name}")
    if normalized_infrastructure["legacy_arena_recovery_active"] is not False:
        raise ValueError("SOAK_CAPTURE_LEGACY_ARENA_RECOVERY_NOT_QUIESCED")
    if normalized_infrastructure["executable_command_route_present"] is not False:
        raise ValueError("SOAK_CAPTURE_EXECUTABLE_COMMAND_ROUTE_PRESENT")
    sample: dict[str, Any] = {
        "schema": SCHEMA,
        "observed_at": isoformat_utc(current),
        "release_ids": dict(expected_releases),
        "arena": {
            "dell_pull_status": arena_pull.get("status"),
            "live_adapter_status": arena_adapter.get("status"),
            "projection_status": arena_projection.get("status"),
            "observation_sequence": _integer(
                arena_pull.get("observation_sequence"),
                "SOAK_CAPTURE_OBSERVATION_SEQUENCE_INVALID",
                minimum=1,
            ),
            "projection_sequence": _integer(
                arena_projection.get("observation_sequence"),
                "SOAK_CAPTURE_PROJECTION_SEQUENCE_INVALID",
                minimum=1,
            ),
            "control_capability_count": max(arena_capabilities),
            "physical_effect_count": max(arena_effects),
            "service_restart_count": _integer(restarts.get("arena"), "SOAK_CAPTURE_ARENA_RESTART_COUNT_INVALID"),
        },
        "cra": {
            "projection_pull_status": cra_pull.get("status"),
            "runtime_readiness": cra_runtime.get("readiness"),
            "policy_decision": cra_runtime.get("policy_decision"),
            "policy_action": cra_runtime.get("policy_action"),
            "policy_reason_code": cra_runtime.get("policy_reason_code"),
            "policy_candidate_reason_code": cra_runtime.get("policy_candidate_reason_code"),
            "policy_decision_reason_code": cra_runtime.get("policy_decision_reason_code"),
            "policy_decision_digest": cra_runtime.get("policy_decision_digest"),
            "policy_reason_binding": cra_runtime.get("policy_reason_binding"),
            "policy_blockers": policy_blockers,
            "monitoring_readiness": monitoring_readiness,
            "monitoring_parity": {
                "monitoring_cycle_id": monitoring_cycle_id,
                "parity": dict(monitoring_parity),
            },
            "incident_state": cra_runtime.get("incident_state"),
            "process_cpu_seconds": cra_runtime.get("process_cpu_seconds"),
            "command_delivery_enabled": cra_runtime.get("command_delivery_enabled"),
            "control_capability_count": max(cra_capabilities),
            "physical_effect_count": max(cra_effects),
            "authorization_row_count": _integer(
                cra_database.get("authorization_row_count"),
                "SOAK_CAPTURE_AUTHORIZATION_ROW_COUNT_INVALID",
            ),
            "command_row_count": _integer(cra_database.get("command_row_count"), "SOAK_CAPTURE_COMMAND_ROW_COUNT_INVALID"),
            "service_restart_count": _integer(restarts.get("cra"), "SOAK_CAPTURE_CRA_RESTART_COUNT_INVALID"),
        },
        "dell": {
            "observation_server_active": dell.get("observation_server_active"),
            "target_snapshot_valid": dell.get("target_snapshot_valid"),
            "control_capability_count": _integer(
                dell.get("control_capability_count"),
                "SOAK_CAPTURE_DELL_CONTROL_COUNT_INVALID",
            ),
            "physical_effect_count": _integer(
                dell.get("physical_effect_count"),
                "SOAK_CAPTURE_DELL_EFFECT_COUNT_INVALID",
            ),
            "service_restart_count": _integer(restarts.get("dell"), "SOAK_CAPTURE_DELL_RESTART_COUNT_INVALID"),
        },
        "infrastructure": normalized_infrastructure,
        "operations": {name: _integer(operations.get(name), f"SOAK_CAPTURE_OPERATION_COUNTER_INVALID:{name}") for name in OPERATION_FIELDS},
    }
    if sample["dell"]["observation_server_active"] is not True or sample["dell"]["target_snapshot_valid"] is not True:
        raise ValueError("SOAK_CAPTURE_DELL_NOT_READY")
    return sample


def append_sample(path: Path, sample: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("SOAK_EVIDENCE_OUTPUT_NOT_REGULAR_FILE")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("SOAK_EVIDENCE_OUTPUT_PERMISSIONS_UNSAFE")
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "ab") as handle:
            descriptor = -1
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            handle.write(json.dumps(dict(sample), separators=(",", ":"), sort_keys=True).encode() + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compose one strict CRA no-action soak JSONL sample")
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--arena-release", required=True)
    parser.add_argument("--cra-release", required=True)
    parser.add_argument("--dell-release", required=True)
    parser.add_argument("--maximum-input-age-seconds", type=float, default=120.0)
    args = parser.parse_args()
    sample = compose_no_action_sample(
        _read_json(args.capture),
        expected_releases={"arena": args.arena_release, "cra": args.cra_release, "dell": args.dell_release},
        maximum_input_age_seconds=args.maximum_input_age_seconds,
    )
    append_sample(args.output, sample)
    print(json.dumps(sample, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
