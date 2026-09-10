from __future__ import annotations

import argparse
import json
import math
import os
import re
import stat
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from cra_no_action_soak.parity import ParityRecord, evaluate_monitoring_parity
from cra_no_action_soak.time import parse_utc

SCHEMA = "cra.no_action_soak_sample.v9"
RELEASE_FIELDS = frozenset({"arena", "cra", "dell"})
HOST_FIELDS = RELEASE_FIELDS
CREDENTIAL_FIELDS = frozenset({"arena_cra", "arena_dell", "dell_target_observer"})
REHEARSAL_FIELDS = frozenset({"cra_backup_restore", "cra_single_writer", "credential_rotation"})
SERVICE_FAILURE_FIELDS = frozenset(
    {
        "arena_dell_host_status_pull",
        "arena_dell_pull",
        "arena_host_status_publisher",
        "arena_live_adapter",
        "arena_projection",
        "arena_projection_server",
        "cra_arena_host_status_pull",
        "cra_projection_pull",
        "cra_runtime",
        "cra_soak_collector",
        "dell_host_status_publisher",
        "dell_observation_server",
    }
)
PERSISTENT_SERVICE_FIELDS = frozenset(
    {
        "arena_projection_server",
        "cra_runtime",
        "dell_observation_server",
    }
)
RECOVERY_FIELDS = frozenset({"arena_dell_pull", "cra_projection_pull"})
RECOVERY_HEALTH_FIELDS = frozenset(
    {
        "schema",
        "component",
        "release_id",
        "current_state",
        "current_episode",
        "last_episode",
        "episode_count",
        "recovered_episode_count",
        "terminal_episode_count",
        "failure_attempt_count",
        "retry_attempt_count",
        "exhausted_invocation_count",
        "maximum_recovery_duration_ms",
        "event_count",
        "last_event_hash",
        "updated_at",
    }
)
ARENA_FIELDS = frozenset(
    {
        "dell_pull_status",
        "live_adapter_status",
        "projection_status",
        "observation_sequence",
        "projection_sequence",
        "control_capability_count",
        "physical_effect_count",
        "service_restart_count",
    }
)
CRA_FIELDS = frozenset(
    {
        "projection_pull_status",
        "runtime_readiness",
        "policy_decision",
        "policy_action",
        "policy_reason_code",
        "policy_candidate_reason_code",
        "policy_decision_reason_code",
        "policy_decision_digest",
        "policy_reason_binding",
        "policy_blockers",
        "monitoring_readiness",
        "monitoring_parity",
        "incident_state",
        "process_cpu_seconds",
        "command_delivery_enabled",
        "control_capability_count",
        "physical_effect_count",
        "authorization_row_count",
        "command_row_count",
        "service_restart_count",
    }
)
DELL_FIELDS = frozenset(
    {
        "observation_server_active",
        "target_snapshot_valid",
        "control_capability_count",
        "physical_effect_count",
        "service_restart_count",
    }
)
INFRASTRUCTURE_FIELDS = frozenset(
    {
        "host_boot_ids",
        "release_manifest_sha256",
        "runtime_manifest_sha256",
        "configuration_set_sha256",
        "maintenance_restart_policy_sha256",
        "rehearsal_evidence_sha256",
        "target_identity_sha256",
        "cra_database_integrity_check",
        "cra_database_journal_mode",
        "cra_archive_quick_check",
        "cra_archive_integrity_check",
        "cra_single_writer_lock",
        "cra_backup_restore_rehearsal",
        "credential_rotation_rehearsal",
        "credential_minimum_remaining_seconds",
        "disk_free_bytes",
        "resident_memory_bytes",
        "open_fd_count",
        "resource_limit_breach_count",
        "service_failed_invocation_count",
        "persistent_service_invocation_ids",
        "legacy_arena_recovery_active",
        "legacy_arena_recovery_restart_count",
        "executable_command_route_present",
        "transport_recovery_health",
    }
)
OPERATION_FIELDS = frozenset(
    {
        "effect_request_count",
        "effect_boundary_count",
        "effect_scope_count",
        "raw_outcome_unknown_count",
        "unresolved_scope_count",
        "oldest_unresolved_age_seconds",
        "reconciliation_count",
        "duplicate_scope_count",
        "epoch_effect_request_count",
        "epoch_effect_boundary_count",
        "epoch_outcome_unknown_count",
        "epoch_reconciliation_count",
        "epoch_duplicate_scope_count",
        "target_transition_count",
        "safe_target_transition_count",
        "unexplained_target_transition_count",
        "legacy_arena_epoch_invocation_count",
        "cra_safe_blocked_episode_count",
        "cra_max_safe_blocked_duration_seconds",
        "cra_current_safe_blocked_duration_seconds",
        "cra_hot_database_bytes",
        "cra_hot_wal_bytes",
        "cra_hot_shm_bytes",
        "cra_hot_sqlite_total_bytes",
        "cra_archive_database_bytes",
        "cra_archive_wal_bytes",
        "cra_archive_shm_bytes",
        "cra_archive_sqlite_total_bytes",
        "cra_archive_record_count",
        "cra_archive_checkpoint_count",
    }
)
EPOCH_COUNTER_PAIRS = {
    "effect_request_count": "epoch_effect_request_count",
    "effect_boundary_count": "epoch_effect_boundary_count",
    "raw_outcome_unknown_count": "epoch_outcome_unknown_count",
    "reconciliation_count": "epoch_reconciliation_count",
    "duplicate_scope_count": "epoch_duplicate_scope_count",
}
SHA256 = re.compile(r"^[0-9a-f]{64}$")
INVOCATION_ID = re.compile(r"^[0-9a-f]{32}$")


def _object(value: object, fields: frozenset[str], code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError(code)
    return dict(value)


def _integer(value: object, code: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(code)
    return value


def _string_map(value: object, fields: frozenset[str], code: str, *, sha256: bool = False) -> dict[str, str]:
    item = _object(value, fields, code)
    result: dict[str, str] = {}
    for name, raw in item.items():
        if not isinstance(raw, str) or not raw.strip() or (sha256 and SHA256.fullmatch(raw) is None):
            raise ValueError(code)
        result[name] = raw
    return result


def _integer_map(value: object, fields: frozenset[str], code: str) -> dict[str, int]:
    item = _object(value, fields, code)
    return {name: _integer(raw, code) for name, raw in item.items()}


def _digest(value: object, code: str) -> str:
    if not isinstance(value, str) or SHA256.fullmatch(value) is None:
        raise ValueError(code)
    return value


def read_samples(path: Path, *, maximum_bytes: int = 128 * 1024 * 1024) -> list[dict[str, Any]]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("SOAK_EVIDENCE_NOT_REGULAR_FILE")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("SOAK_EVIDENCE_PERMISSIONS_UNSAFE")
        if metadata.st_size > maximum_bytes:
            raise ValueError("SOAK_EVIDENCE_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > maximum_bytes:
        raise ValueError("SOAK_EVIDENCE_TOO_LARGE")
    samples: list[dict[str, Any]] = []
    for line_number, line in enumerate(raw.splitlines(), start=1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"SOAK_SAMPLE_NOT_OBJECT:{line_number}")
        samples.append(dict(value))
    return samples


def evaluate_no_action_soak(
    samples: Sequence[Mapping[str, Any]],
    *,
    expected_releases: Mapping[str, str],
    minimum_duration_seconds: float = 24 * 3600,
    maximum_sample_gap_seconds: float = 90,
    minimum_disk_free_bytes: int = 1024 * 1024 * 1024,
    minimum_credential_remaining_seconds: int = 3600,
    maximum_resident_memory_growth_bytes: int = 256 * 1024 * 1024,
    maximum_open_fd_growth: int = 64,
    maximum_unresolved_scope_age_seconds: int = 60,
    maximum_safe_blocked_duration_seconds: int = 60,
    maximum_database_growth_bytes: int = 512 * 1024 * 1024,
) -> dict[str, Any]:
    blockers: set[str] = set()
    if set(expected_releases) != RELEASE_FIELDS or not all(
        isinstance(value, str) and value.strip() for value in expected_releases.values()
    ):
        raise ValueError("SOAK_EXPECTED_RELEASES_INVALID")
    if (
        isinstance(minimum_duration_seconds, bool)
        or not isinstance(minimum_duration_seconds, (int, float))
        or not math.isfinite(float(minimum_duration_seconds))
        or minimum_duration_seconds <= 0
        or isinstance(maximum_sample_gap_seconds, bool)
        or not isinstance(maximum_sample_gap_seconds, (int, float))
        or not math.isfinite(float(maximum_sample_gap_seconds))
        or maximum_sample_gap_seconds <= 0
        or isinstance(minimum_disk_free_bytes, bool)
        or not isinstance(minimum_disk_free_bytes, int)
        or minimum_disk_free_bytes < 0
        or isinstance(minimum_credential_remaining_seconds, bool)
        or not isinstance(minimum_credential_remaining_seconds, int)
        or minimum_credential_remaining_seconds < 0
        or isinstance(maximum_resident_memory_growth_bytes, bool)
        or not isinstance(maximum_resident_memory_growth_bytes, int)
        or maximum_resident_memory_growth_bytes < 0
        or isinstance(maximum_open_fd_growth, bool)
        or not isinstance(maximum_open_fd_growth, int)
        or maximum_open_fd_growth < 0
        or isinstance(maximum_unresolved_scope_age_seconds, bool)
        or not isinstance(maximum_unresolved_scope_age_seconds, int)
        or maximum_unresolved_scope_age_seconds < 0
        or isinstance(maximum_safe_blocked_duration_seconds, bool)
        or not isinstance(maximum_safe_blocked_duration_seconds, int)
        or maximum_safe_blocked_duration_seconds < 0
        or isinstance(maximum_database_growth_bytes, bool)
        or not isinstance(maximum_database_growth_bytes, int)
        or maximum_database_growth_bytes < 0
    ):
        raise ValueError("SOAK_GATE_LIMIT_INVALID")
    if len(samples) < 2:
        blockers.add("SOAK_SAMPLE_COUNT_INSUFFICIENT")

    times: list[datetime] = []
    observation_sequences: list[int] = []
    projection_sequences: list[int] = []
    previous_restarts: dict[str, int] | None = None
    initial_restarts: dict[str, int] | None = None
    initial_infrastructure_identity: dict[str, Any] | None = None
    initial_resources: dict[str, dict[str, int]] | None = None
    previous_legacy_restarts: int | None = None
    initial_legacy_restarts: int | None = None
    previous_service_failures: dict[str, int] | None = None
    failed_services: set[str] = set()
    recovery_evidence = {name: {"recovered_episode_count": 0, "maximum_recovery_duration_ms": 0} for name in RECOVERY_FIELDS}
    previous_recovery_counters: dict[str, tuple[int, int, int]] | None = None
    readiness_started: dict[str, datetime | None] = {"input_fresh": None, "projection_clean": None, "source_ready": None}
    maximum_readiness_outage = {"input_fresh": 0.0, "projection_clean": 0.0, "source_ready": 0.0}
    previous_operations: dict[str, int] | None = None
    previous_target_digest: str | None = None
    initial_epoch_counter_baselines: dict[str, int] | None = None
    initial_database_bytes: int | None = None
    previous_process_cpu_seconds: float | None = None
    initial_persistent_invocations: dict[str, str] | None = None
    parity_records: list[ParityRecord] = []
    final_unresolved_scope_count = 0
    for index, raw in enumerate(samples):
        sample = dict(raw)
        if set(sample) != {
            "schema",
            "observed_at",
            "release_ids",
            "arena",
            "cra",
            "dell",
            "infrastructure",
            "operations",
        }:
            blockers.add(f"SOAK_SAMPLE_FIELDS_INVALID:{index}")
            continue
        if sample["schema"] != SCHEMA:
            blockers.add(f"SOAK_SAMPLE_SCHEMA_INVALID:{index}")
            continue
        try:
            observed = parse_utc(str(sample["observed_at"]))
            releases = _object(sample["release_ids"], RELEASE_FIELDS, "SOAK_RELEASE_FIELDS_INVALID")
            arena = _object(sample["arena"], ARENA_FIELDS, "SOAK_ARENA_FIELDS_INVALID")
            cra = _object(sample["cra"], CRA_FIELDS, "SOAK_CRA_FIELDS_INVALID")
            dell = _object(sample["dell"], DELL_FIELDS, "SOAK_DELL_FIELDS_INVALID")
            infrastructure = _object(
                sample["infrastructure"],
                INFRASTRUCTURE_FIELDS,
                "SOAK_INFRASTRUCTURE_FIELDS_INVALID",
            )
            operations = _object(sample["operations"], OPERATION_FIELDS, "SOAK_OPERATION_FIELDS_INVALID")
        except (TypeError, ValueError) as error:
            blockers.add(f"SOAK_SAMPLE_INVALID:{index}:{error}")
            continue
        times.append(observed)
        if releases != dict(expected_releases):
            blockers.add(f"SOAK_RELEASE_IDENTITY_MISMATCH:{index}")
        if arena["live_adapter_status"] != "READY":
            blockers.add(f"SOAK_ARENA_ADAPTER_NOT_READY:{index}")
        if arena["projection_status"] != "READY":
            blockers.add(f"SOAK_ARENA_PROJECTION_NOT_READY:{index}")
        if cra["runtime_readiness"] != "NO_ACTION_READY":
            blockers.add(f"SOAK_CRA_RUNTIME_NOT_READY:{index}")
        if cra["policy_decision"] not in {"WOULD_AUTHORIZE", "BLOCKED", "NO_ACTION"}:
            blockers.add(f"SOAK_CRA_POLICY_DECISION_INVALID:{index}")
        policy_action = cra["policy_action"]
        policy_reason = cra["policy_reason_code"]
        candidate_reason = cra["policy_candidate_reason_code"]
        decision_reason = cra["policy_decision_reason_code"]
        decision_digest = cra["policy_decision_digest"]
        reason_binding = cra["policy_reason_binding"]
        policy_blockers = cra["policy_blockers"]
        if policy_action is not None:
            blockers.add(f"SOAK_CRA_POLICY_ACTION_NON_NULL:{index}")
        if not isinstance(policy_reason, str) or not policy_reason:
            blockers.add(f"SOAK_CRA_POLICY_REASON_INVALID:{index}")
        if not isinstance(candidate_reason, str) or not candidate_reason:
            blockers.add(f"SOAK_CRA_CANDIDATE_REASON_INVALID:{index}")
        if not isinstance(decision_reason, str) or not decision_reason:
            blockers.add(f"SOAK_CRA_DECISION_REASON_INVALID:{index}")
        if policy_reason != decision_reason:
            blockers.add(f"SOAK_CRA_LEGACY_REASON_ALIAS_DRIFT:{index}")
        if not isinstance(decision_digest, str) or SHA256.fullmatch(decision_digest) is None:
            blockers.add(f"SOAK_CRA_DECISION_DIGEST_INVALID:{index}")
        if reason_binding != "CANDIDATE_AND_DECISION_BOUND_V1":
            blockers.add(f"SOAK_CRA_REASON_BINDING_INVALID:{index}")
        if not isinstance(policy_blockers, list) or not all(isinstance(item, str) and item for item in policy_blockers):
            blockers.add(f"SOAK_CRA_POLICY_BLOCKERS_INVALID:{index}")
            policy_blockers = []
        elif len(set(policy_blockers)) != len(policy_blockers):
            blockers.add(f"SOAK_CRA_POLICY_BLOCKERS_DUPLICATE:{index}")
        try:
            readiness = _object(
                cra["monitoring_readiness"],
                frozenset({"input_fresh", "parity_clean", "projection_clean", "source_ready"}),
                "MONITORING_READINESS_FIELDS_INVALID",
            )
            for name in readiness_started:
                started = readiness_started[name]
                if readiness[name] is False and started is None:
                    readiness_started[name] = observed
                elif readiness[name] is True and started is not None:
                    maximum_readiness_outage[name] = max(
                        maximum_readiness_outage[name],
                        (observed - started).total_seconds(),
                    )
                    readiness_started[name] = None
            parity_clean = readiness["parity_clean"]
            if not isinstance(parity_clean, bool):
                raise ValueError("MONITORING_PARITY_READINESS_INVALID")
            parity_wrapper = _object(
                cra["monitoring_parity"],
                frozenset({"monitoring_cycle_id", "parity"}),
                "MONITORING_PARITY_WRAPPER_INVALID",
            )
            monitoring_cycle_id = parity_wrapper["monitoring_cycle_id"]
            parity_payload = parity_wrapper["parity"]
            if not isinstance(monitoring_cycle_id, str) or not monitoring_cycle_id:
                raise ValueError("MONITORING_PARITY_CYCLE_ID_INVALID")
            if not isinstance(parity_payload, Mapping):
                raise ValueError("MONITORING_PARITY_PAYLOAD_INVALID")
            parity_records.append(
                ParityRecord(
                    sample_index=index,
                    observed_at=observed,
                    monitoring_cycle_id=monitoring_cycle_id,
                    parity_clean=parity_clean,
                    payload=dict(parity_payload),
                )
            )
        except ValueError as error:
            blockers.add(f"SOAK_CRA_MONITORING_READINESS_INVALID:{index}:{error}")
        if cra["policy_decision"] == "WOULD_AUTHORIZE" and (
            cra["incident_state"] != "CONFIRMED"
            or policy_blockers != ["CRA_OPERATING_MODE_NO_ACTION"]
            or decision_reason != "CRA_OPERATING_MODE_NO_ACTION"
            or candidate_reason != "confirmed_tcp_stall"
        ):
            blockers.add(f"SOAK_CRA_WOULD_AUTHORIZE_SEMANTICS_INVALID:{index}")
        if cra["policy_decision"] == "NO_ACTION" and (
            cra["incident_state"] == "CONFIRMED"
            or decision_reason != "INCIDENT_NOT_CONFIRMED"
            or candidate_reason != "NO_CONFIRMED_RECOVERY_CANDIDATE"
        ):
            blockers.add(f"SOAK_CRA_NO_ACTION_SEMANTICS_INVALID:{index}")
        if cra["policy_decision"] == "BLOCKED" and (
            cra["incident_state"] != "CONFIRMED" or not any(item != "CRA_OPERATING_MODE_NO_ACTION" for item in policy_blockers)
        ):
            blockers.add(f"SOAK_CRA_BLOCKED_SEMANTICS_INVALID:{index}")
        cpu_seconds = cra["process_cpu_seconds"]
        if (
            isinstance(cpu_seconds, bool)
            or not isinstance(cpu_seconds, (int, float))
            or not math.isfinite(float(cpu_seconds))
            or cpu_seconds < 0
        ):
            blockers.add(f"SOAK_CRA_PROCESS_CPU_INVALID:{index}")
        elif previous_process_cpu_seconds is not None and float(cpu_seconds) < previous_process_cpu_seconds:
            blockers.add(f"SOAK_CRA_PROCESS_CPU_REGRESSION:{index}")
        else:
            previous_process_cpu_seconds = float(cpu_seconds)
        if cra["command_delivery_enabled"] is not False:
            blockers.add(f"SOAK_COMMAND_DELIVERY_ENABLED:{index}")
        if dell["observation_server_active"] is not True:
            blockers.add(f"SOAK_DELL_OBSERVATION_SERVER_INACTIVE:{index}")
        if dell["target_snapshot_valid"] is not True:
            blockers.add(f"SOAK_DELL_TARGET_INVALID:{index}")
        try:
            for component, item in (("ARENA", arena), ("CRA", cra), ("DELL", dell)):
                if _integer(item["control_capability_count"], "CONTROL_CAPABILITY_COUNT_INVALID") != 0:
                    blockers.add(f"SOAK_{component}_CONTROL_CAPABILITY_NONZERO:{index}")
                physical_effect_count = _integer(item["physical_effect_count"], "PHYSICAL_EFFECT_COUNT_INVALID")
                if component != "DELL" and physical_effect_count != 0:
                    blockers.add(f"SOAK_{component}_PHYSICAL_EFFECT_NONZERO:{index}")
            if _integer(cra["authorization_row_count"], "AUTHORIZATION_ROW_COUNT_INVALID") != 0:
                blockers.add(f"SOAK_AUTHORIZATION_ROW_NONZERO:{index}")
            if _integer(cra["command_row_count"], "COMMAND_ROW_COUNT_INVALID") != 0:
                blockers.add(f"SOAK_COMMAND_ROW_NONZERO:{index}")
            observation_sequences.append(_integer(arena["observation_sequence"], "OBSERVATION_SEQUENCE_INVALID", minimum=1))
            projection_sequences.append(_integer(arena["projection_sequence"], "PROJECTION_SEQUENCE_INVALID", minimum=1))
            restarts = {
                "arena": _integer(arena["service_restart_count"], "ARENA_RESTART_COUNT_INVALID"),
                "cra": _integer(cra["service_restart_count"], "CRA_RESTART_COUNT_INVALID"),
                "dell": _integer(dell["service_restart_count"], "DELL_RESTART_COUNT_INVALID"),
            }
            infrastructure_identity: dict[str, Any] = {
                "host_boot_ids": _string_map(infrastructure["host_boot_ids"], HOST_FIELDS, "HOST_BOOT_IDS_INVALID"),
                "release_manifest_sha256": _string_map(
                    infrastructure["release_manifest_sha256"],
                    HOST_FIELDS,
                    "RELEASE_MANIFEST_SHA256_INVALID",
                    sha256=True,
                ),
                "runtime_manifest_sha256": _string_map(
                    infrastructure["runtime_manifest_sha256"],
                    HOST_FIELDS,
                    "RUNTIME_MANIFEST_SHA256_INVALID",
                    sha256=True,
                ),
                "configuration_set_sha256": _string_map(
                    infrastructure["configuration_set_sha256"],
                    HOST_FIELDS,
                    "CONFIGURATION_SET_SHA256_INVALID",
                    sha256=True,
                ),
                "maintenance_restart_policy_sha256": _string_map(
                    infrastructure["maintenance_restart_policy_sha256"],
                    HOST_FIELDS,
                    "MAINTENANCE_RESTART_POLICY_SHA256_INVALID",
                    sha256=True,
                ),
                "rehearsal_evidence_sha256": _string_map(
                    infrastructure["rehearsal_evidence_sha256"],
                    REHEARSAL_FIELDS,
                    "REHEARSAL_EVIDENCE_SHA256_INVALID",
                    sha256=True,
                ),
            }
            target_digest = _digest(infrastructure["target_identity_sha256"], "TARGET_IDENTITY_SHA256_INVALID")
            credentials = _integer_map(
                infrastructure["credential_minimum_remaining_seconds"],
                CREDENTIAL_FIELDS,
                "CREDENTIAL_REMAINING_INVALID",
            )
            disks = _integer_map(infrastructure["disk_free_bytes"], HOST_FIELDS, "DISK_FREE_BYTES_INVALID")
            resources = {
                "resident_memory_bytes": _integer_map(
                    infrastructure["resident_memory_bytes"], HOST_FIELDS, "RESIDENT_MEMORY_BYTES_INVALID"
                ),
                "open_fd_count": _integer_map(infrastructure["open_fd_count"], HOST_FIELDS, "OPEN_FD_COUNT_INVALID"),
            }
            resource_breaches = _integer_map(
                infrastructure["resource_limit_breach_count"], HOST_FIELDS, "RESOURCE_LIMIT_BREACH_COUNT_INVALID"
            )
            service_failures = _integer_map(
                infrastructure["service_failed_invocation_count"],
                SERVICE_FAILURE_FIELDS,
                "SERVICE_FAILED_INVOCATION_COUNT_INVALID",
            )
            persistent_invocations = _string_map(
                infrastructure["persistent_service_invocation_ids"],
                PERSISTENT_SERVICE_FIELDS,
                "PERSISTENT_SERVICE_INVOCATION_IDS_INVALID",
            )
            if any(INVOCATION_ID.fullmatch(value) is None for value in persistent_invocations.values()):
                raise ValueError("PERSISTENT_SERVICE_INVOCATION_ID_INVALID")
            recovery_health_raw = _object(
                infrastructure["transport_recovery_health"],
                RECOVERY_FIELDS,
                "TRANSPORT_RECOVERY_HEALTH_INVALID",
            )
            recovery_health: dict[str, dict[str, Any]] = {}
            for name, raw_health in recovery_health_raw.items():
                health = _object(raw_health, RECOVERY_HEALTH_FIELDS, "TRANSPORT_RECOVERY_STATE_INVALID")
                expected_release = expected_releases["arena" if name == "arena_dell_pull" else "cra"]
                if health["schema"] != "cra.transport_recovery_state.v1" or health["component"] != name:
                    raise ValueError("TRANSPORT_RECOVERY_IDENTITY_INVALID")
                if health["release_id"] != expected_release:
                    raise ValueError("TRANSPORT_RECOVERY_RELEASE_INVALID")
                if health["current_state"] not in {"READY", "RECOVERING", "SAFE_BLOCKED"}:
                    raise ValueError("TRANSPORT_RECOVERY_CURRENT_STATE_INVALID")
                for field in (
                    "episode_count",
                    "recovered_episode_count",
                    "terminal_episode_count",
                    "failure_attempt_count",
                    "retry_attempt_count",
                    "exhausted_invocation_count",
                    "maximum_recovery_duration_ms",
                    "event_count",
                ):
                    _integer(health[field], f"TRANSPORT_RECOVERY_COUNTER_INVALID:{field}")
                _digest(health["last_event_hash"], "TRANSPORT_RECOVERY_EVENT_HASH_INVALID")
                parse_utc(str(health["updated_at"]))
                recovery_health[name] = health
            legacy_restarts = _integer(
                infrastructure["legacy_arena_recovery_restart_count"],
                "LEGACY_ARENA_RECOVERY_RESTART_COUNT_INVALID",
            )
            operation_counters = {name: _integer(operations[name], f"OPERATION_COUNTER_INVALID:{name}") for name in OPERATION_FIELDS}
        except ValueError as error:
            blockers.add(f"SOAK_SAMPLE_COUNTER_INVALID:{index}:{error}")
            continue
        if initial_restarts is None:
            initial_restarts = dict(restarts)
        if previous_restarts is not None and any(restarts[name] < previous_restarts[name] for name in restarts):
            blockers.add(f"SOAK_RESTART_COUNTER_REGRESSION:{index}")
        if initial_restarts is not None and any(restarts[name] > initial_restarts[name] for name in restarts):
            blockers.add(f"SOAK_SERVICE_RESTART_COUNT_INCREASE:{index}")
        previous_restarts = restarts
        if initial_infrastructure_identity is None:
            initial_infrastructure_identity = infrastructure_identity
        elif infrastructure_identity != initial_infrastructure_identity:
            blockers.add(f"SOAK_INFRASTRUCTURE_IDENTITY_DRIFT:{index}")
        if infrastructure["cra_database_integrity_check"] != "ok":
            blockers.add(f"SOAK_CRA_DATABASE_INTEGRITY_NOT_OK:{index}")
        if infrastructure["cra_database_journal_mode"] != "wal":
            blockers.add(f"SOAK_CRA_DATABASE_NOT_WAL:{index}")
        if infrastructure["cra_archive_quick_check"] != "ok":
            blockers.add(f"SOAK_CRA_ARCHIVE_QUICK_CHECK_NOT_OK:{index}")
        if infrastructure["cra_archive_integrity_check"] != "ok":
            blockers.add(f"SOAK_CRA_ARCHIVE_INTEGRITY_NOT_OK:{index}")
        if infrastructure["cra_single_writer_lock"] != "ENFORCED":
            blockers.add(f"SOAK_CRA_SINGLE_WRITER_NOT_ENFORCED:{index}")
        if infrastructure["cra_backup_restore_rehearsal"] != "PASS":
            blockers.add(f"SOAK_CRA_BACKUP_RESTORE_NOT_PASS:{index}")
        if infrastructure["credential_rotation_rehearsal"] != "PASS":
            blockers.add(f"SOAK_CREDENTIAL_ROTATION_NOT_PASS:{index}")
        if infrastructure["legacy_arena_recovery_active"] is not False:
            blockers.add(f"SOAK_LEGACY_ARENA_RECOVERY_NOT_QUIESCED:{index}")
        if infrastructure["executable_command_route_present"] is not False:
            blockers.add(f"SOAK_EXECUTABLE_COMMAND_ROUTE_PRESENT:{index}")
        if any(value < minimum_credential_remaining_seconds for value in credentials.values()):
            blockers.add(f"SOAK_CREDENTIAL_REMAINING_TOO_LOW:{index}")
        if any(value < minimum_disk_free_bytes for value in disks.values()):
            blockers.add(f"SOAK_DISK_FREE_TOO_LOW:{index}")
        if any(value != 0 for value in resource_breaches.values()):
            blockers.add(f"SOAK_RESOURCE_LIMIT_BREACH:{index}")
        if previous_service_failures is not None and any(
            service_failures[name] < previous_service_failures[name] for name in SERVICE_FAILURE_FIELDS
        ):
            blockers.add(f"SOAK_SERVICE_FAILURE_COUNTER_REGRESSION:{index}")
        failed_services.update(name for name, value in service_failures.items() if value != 0 and name not in RECOVERY_FIELDS)
        recovery_counters: dict[str, tuple[int, int, int]] = {}
        for name, health in recovery_health.items():
            counters = (
                int(health["episode_count"]),
                int(health["recovered_episode_count"]),
                int(health["event_count"]),
            )
            if previous_recovery_counters is not None and any(
                current_value < previous_value
                for current_value, previous_value in zip(counters, previous_recovery_counters[name], strict=True)
            ):
                blockers.add(f"SOAK_TRANSPORT_RECOVERY_COUNTER_REGRESSION:{name.upper()}:{index}")
            recovery_counters[name] = counters
            recovery_evidence[name] = {
                "recovered_episode_count": int(health["recovered_episode_count"]),
                "maximum_recovery_duration_ms": int(health["maximum_recovery_duration_ms"]),
            }
            if int(health["terminal_episode_count"]) != 0 or health["current_state"] == "SAFE_BLOCKED":
                blockers.add(f"SOAK_TRANSPORT_RECOVERY_TERMINAL:{name.upper()}:{index}")
            if service_failures[name] > int(health["exhausted_invocation_count"]):
                blockers.add(f"SOAK_TRANSPORT_FAILURE_UNACCOUNTED:{name.upper()}:{index}")
            episode = health["current_episode"]
            if health["current_state"] == "RECOVERING":
                blockers.add(f"SOAK_TRANSPORT_RECOVERY_PENDING:{name.upper()}")
                if not isinstance(episode, Mapping) or "started_at" not in episode:
                    blockers.add(f"SOAK_TRANSPORT_RECOVERY_EPISODE_INVALID:{name.upper()}:{index}")
                elif (observed - parse_utc(str(episode["started_at"]))).total_seconds() > 60:
                    blockers.add(f"SOAK_TRANSPORT_RECOVERY_EXPIRED:{name.upper()}:{index}")
            expected_status = arena["dell_pull_status"] if name == "arena_dell_pull" else cra["projection_pull_status"]
            if expected_status != "READY" and health["current_state"] != "RECOVERING":
                blockers.add(f"SOAK_TRANSPORT_STATUS_WITHOUT_RECOVERY:{name.upper()}:{index}")
        previous_recovery_counters = recovery_counters
        previous_service_failures = service_failures
        if initial_persistent_invocations is None:
            initial_persistent_invocations = persistent_invocations
        else:
            for name, invocation_id in persistent_invocations.items():
                if invocation_id != initial_persistent_invocations[name]:
                    blockers.add(f"SOAK_PERSISTENT_SERVICE_INVOCATION_DRIFT:{name.upper()}:{index}")
        if initial_resources is None:
            initial_resources = resources
        else:
            if any(
                resources["resident_memory_bytes"][name]
                > initial_resources["resident_memory_bytes"][name] + maximum_resident_memory_growth_bytes
                for name in HOST_FIELDS
            ):
                blockers.add(f"SOAK_RESIDENT_MEMORY_GROWTH_EXCEEDED:{index}")
            if any(
                resources["open_fd_count"][name] > initial_resources["open_fd_count"][name] + maximum_open_fd_growth for name in HOST_FIELDS
            ):
                blockers.add(f"SOAK_OPEN_FD_GROWTH_EXCEEDED:{index}")
        if initial_legacy_restarts is None:
            initial_legacy_restarts = legacy_restarts
        if previous_legacy_restarts is not None and legacy_restarts < previous_legacy_restarts:
            blockers.add(f"SOAK_LEGACY_RESTART_COUNTER_REGRESSION:{index}")
        if initial_legacy_restarts is not None and legacy_restarts > initial_legacy_restarts:
            blockers.add(f"SOAK_LEGACY_RESTART_COUNT_INCREASE:{index}")
        previous_legacy_restarts = legacy_restarts
        if previous_operations is not None and any(
            operation_counters[name] < previous_operations[name]
            for name in OPERATION_FIELDS
            if name
            not in {
                "unresolved_scope_count",
                "oldest_unresolved_age_seconds",
                "cra_current_safe_blocked_duration_seconds",
                "cra_hot_database_bytes",
                "cra_hot_wal_bytes",
                "cra_hot_shm_bytes",
                "cra_hot_sqlite_total_bytes",
                "cra_archive_wal_bytes",
                "cra_archive_shm_bytes",
                "cra_archive_sqlite_total_bytes",
            }
        ):
            blockers.add(f"SOAK_OPERATION_COUNTER_REGRESSION:{index}")
        if (
            operation_counters["effect_boundary_count"] > operation_counters["effect_request_count"]
            or operation_counters["raw_outcome_unknown_count"] > operation_counters["effect_boundary_count"]
            or operation_counters["effect_scope_count"] > operation_counters["effect_request_count"]
            or operation_counters["duplicate_scope_count"] > operation_counters["effect_request_count"]
            or operation_counters["epoch_effect_boundary_count"] > operation_counters["epoch_effect_request_count"]
            or operation_counters["epoch_outcome_unknown_count"] > operation_counters["epoch_effect_boundary_count"]
            or operation_counters["epoch_duplicate_scope_count"] > operation_counters["epoch_effect_request_count"]
        ):
            blockers.add(f"SOAK_EFFECT_COUNTER_RELATION_INVALID:{index}")
        epoch_counter_baselines: dict[str, int] = {}
        for total_name, epoch_name in EPOCH_COUNTER_PAIRS.items():
            total = operation_counters[total_name]
            epoch = operation_counters[epoch_name]
            if epoch > total:
                blockers.add(f"SOAK_EPOCH_COUNTER_EXCEEDS_TOTAL:{total_name}:{index}")
            epoch_counter_baselines[total_name] = total - epoch
        if initial_epoch_counter_baselines is None:
            initial_epoch_counter_baselines = epoch_counter_baselines
        elif epoch_counter_baselines != initial_epoch_counter_baselines:
            blockers.add(f"SOAK_EPOCH_COUNTER_BASELINE_DRIFT:{index}")
        if (
            operation_counters["target_transition_count"]
            != operation_counters["safe_target_transition_count"] + operation_counters["unexplained_target_transition_count"]
        ):
            blockers.add(f"SOAK_TARGET_TRANSITION_COUNTER_CONSERVATION_INVALID:{index}")
        if operation_counters["safe_target_transition_count"] > operation_counters["epoch_effect_boundary_count"]:
            blockers.add(f"SOAK_TARGET_TRANSITION_EFFECT_ORACLE_MISMATCH:{index}")
        if previous_operations is not None and previous_target_digest is not None:
            target_changed = target_digest != previous_target_digest
            target_delta = operation_counters["target_transition_count"] - previous_operations["target_transition_count"]
            safe_delta = operation_counters["safe_target_transition_count"] - previous_operations["safe_target_transition_count"]
            unexplained_delta = (
                operation_counters["unexplained_target_transition_count"] - previous_operations["unexplained_target_transition_count"]
            )
            if target_delta != int(target_changed):
                blockers.add(f"SOAK_TARGET_TRANSITION_COUNTER_MISMATCH:{index}")
            boundary_delta = operation_counters["effect_boundary_count"] - previous_operations["effect_boundary_count"]
            duplicate_delta = operation_counters["duplicate_scope_count"] - previous_operations["duplicate_scope_count"]
            expected_safe_delta = int(
                target_changed and boundary_delta == 1 and duplicate_delta == 0 and operation_counters["unresolved_scope_count"] == 0
            )
            expected_unexplained_delta = int(target_changed) - expected_safe_delta
            if safe_delta != expected_safe_delta or unexplained_delta != expected_unexplained_delta:
                blockers.add(f"SOAK_TARGET_TRANSITION_CLASSIFICATION_MISMATCH:{index}")
        if operation_counters["unresolved_scope_count"] == 0 and operation_counters["oldest_unresolved_age_seconds"] != 0:
            blockers.add(f"SOAK_UNRESOLVED_AGE_ORACLE_MISMATCH:{index}")
        if operation_counters["cra_current_safe_blocked_duration_seconds"] > operation_counters["cra_max_safe_blocked_duration_seconds"]:
            blockers.add(f"SOAK_SAFE_BLOCKED_DURATION_ORACLE_MISMATCH:{index}")
        previous_operations = operation_counters
        previous_target_digest = target_digest
        final_unresolved_scope_count = operation_counters["unresolved_scope_count"]
        if operation_counters["epoch_duplicate_scope_count"] != 0:
            blockers.add(f"SOAK_DUPLICATE_EFFECT_SCOPE_DETECTED:{index}")
        if operation_counters["unexplained_target_transition_count"] != 0:
            blockers.add(f"SOAK_UNEXPLAINED_TARGET_TRANSITION:{index}")
        if operation_counters["legacy_arena_epoch_invocation_count"] != 0:
            blockers.add(f"SOAK_LEGACY_ARENA_INVOCATION_DETECTED:{index}")
        if operation_counters["oldest_unresolved_age_seconds"] > maximum_unresolved_scope_age_seconds:
            blockers.add("SOAK_UNRESOLVED_EFFECT_SCOPE_TOO_OLD")
        if operation_counters["cra_max_safe_blocked_duration_seconds"] > maximum_safe_blocked_duration_seconds:
            blockers.add("SOAK_CRA_SAFE_BLOCKED_DURATION_EXCEEDED")
        if operation_counters["cra_current_safe_blocked_duration_seconds"] > maximum_safe_blocked_duration_seconds:
            blockers.add("SOAK_CRA_CURRENT_SAFE_BLOCKED_TOO_LONG")
        if dell["physical_effect_count"] != operation_counters["epoch_effect_boundary_count"]:
            blockers.add(f"SOAK_DELL_EFFECT_ORACLE_MISMATCH:{index}")
        if operation_counters["cra_hot_sqlite_total_bytes"] != sum(
            operation_counters[name] for name in ("cra_hot_database_bytes", "cra_hot_wal_bytes", "cra_hot_shm_bytes")
        ):
            blockers.add(f"SOAK_CRA_HOT_SQLITE_TOTAL_INVALID:{index}")
        if operation_counters["cra_archive_sqlite_total_bytes"] != sum(
            operation_counters[name] for name in ("cra_archive_database_bytes", "cra_archive_wal_bytes", "cra_archive_shm_bytes")
        ):
            blockers.add(f"SOAK_CRA_ARCHIVE_SQLITE_TOTAL_INVALID:{index}")
        database_bytes = operation_counters["cra_hot_sqlite_total_bytes"] + operation_counters["cra_archive_sqlite_total_bytes"]
        if initial_database_bytes is None:
            initial_database_bytes = database_bytes
        elif database_bytes > initial_database_bytes + maximum_database_growth_bytes:
            blockers.add(f"SOAK_DATABASE_GROWTH_EXCEEDED:{index}")

    parity_blockers, parity_evidence = evaluate_monitoring_parity(parity_records)
    blockers.update(parity_blockers)
    if times:
        for name, started in readiness_started.items():
            if started is not None:
                maximum_readiness_outage[name] = max(maximum_readiness_outage[name], (times[-1] - started).total_seconds())
    readiness_limits = {"input_fresh": 90.0, "projection_clean": 90.0, "source_ready": 600.0}
    for name, duration_value in maximum_readiness_outage.items():
        if duration_value > readiness_limits[name]:
            blockers.add(f"SOAK_MONITORING_READINESS_EXPIRED:{name.upper()}")
        elif readiness_started[name] is not None:
            blockers.add(f"SOAK_MONITORING_READINESS_RECOVERY_PENDING:{name.upper()}")
    duration = (times[-1] - times[0]).total_seconds() if len(times) >= 2 else 0.0
    gaps = [(right - left).total_seconds() for left, right in zip(times, times[1:], strict=False)]
    maximum_gap = max(gaps, default=0.0)
    if any(gap <= 0 for gap in gaps):
        blockers.add("SOAK_SAMPLE_TIME_NOT_STRICTLY_INCREASING")
    if len(samples) >= 2 and duration < minimum_duration_seconds:
        blockers.add("SOAK_DURATION_NOT_ELIGIBLE")
    if maximum_gap > maximum_sample_gap_seconds:
        blockers.add("SOAK_SAMPLE_GAP_TOO_LARGE")
    if final_unresolved_scope_count != 0:
        blockers.add("SOAK_UNRESOLVED_EFFECT_SCOPE_AT_END")
    blockers.update(f"SOAK_SERVICE_INVOCATION_FAILURE:{name.upper()}" for name in failed_services)
    for name, values in (("OBSERVATION", observation_sequences), ("PROJECTION", projection_sequences)):
        if any(right < left for left, right in zip(values, values[1:], strict=False)):
            blockers.add(f"SOAK_{name}_SEQUENCE_REGRESSION")
        if len(values) >= 2 and values[-1] <= values[0]:
            blockers.add(f"SOAK_{name}_SEQUENCE_DID_NOT_PROGRESS")
    return _report(
        blockers,
        samples,
        duration=duration,
        maximum_gap=maximum_gap,
        parity_evidence=parity_evidence,
        recovery_evidence=recovery_evidence,
        readiness_evidence={name: int(value * 1000) for name, value in maximum_readiness_outage.items()},
    )


def _report(
    blockers: set[str],
    samples: Sequence[Mapping[str, Any]],
    *,
    duration: float,
    maximum_gap: float,
    parity_evidence: Mapping[str, int],
    recovery_evidence: Mapping[str, Mapping[str, int]],
    readiness_evidence: Mapping[str, int],
) -> dict[str, Any]:
    summarized_blockers, blocker_evidence = _summarize_blockers(blockers)
    terminal_blockers = [item for item in summarized_blockers if _terminal_blocker(item)]
    eligible = not summarized_blockers
    return {
        "schema": "cra.no_action_soak_gate.v11",
        "status": "PASS" if eligible else ("FAIL" if terminal_blockers else "NOT_YET_ELIGIBLE"),
        "terminal_failure": bool(terminal_blockers),
        "terminal_blockers": terminal_blockers,
        "eligible": eligible,
        "operational_healthy": not [item for item in summarized_blockers if _operational_blocker(item)],
        "sample_count": len(samples),
        "duration_seconds": duration,
        "maximum_sample_gap_seconds": maximum_gap,
        "blockers": summarized_blockers,
        "blocker_evidence": blocker_evidence,
        "parity_evidence": dict(parity_evidence),
        "recovery_evidence": {name: dict(value) for name, value in recovery_evidence.items()},
        "readiness_evidence_ms": dict(readiness_evidence),
    }


def _summarize_blockers(blockers: set[str]) -> tuple[list[str], dict[str, dict[str, int | None]]]:
    evidence: dict[str, dict[str, int | None]] = {}
    for blocker in sorted(blockers):
        parts = blocker.split(":")
        sample_index: int | None = None
        code = blocker
        for position, part in enumerate(parts[1:], start=1):
            if part.isdigit():
                sample_index = int(part)
                code = ":".join((*parts[:position], *parts[position + 1 :]))
                break
        item = evidence.setdefault(
            code,
            {"occurrence_count": 0, "first_sample_index": None, "last_sample_index": None},
        )
        item["occurrence_count"] = int(item["occurrence_count"] or 0) + 1
        if sample_index is not None:
            first = item["first_sample_index"]
            last = item["last_sample_index"]
            item["first_sample_index"] = sample_index if first is None else min(first, sample_index)
            item["last_sample_index"] = sample_index if last is None else max(last, sample_index)
    return sorted(evidence), {name: evidence[name] for name in sorted(evidence)}


def _operational_blocker(blocker: str) -> bool:
    eligibility_only = (
        "SOAK_DURATION_NOT_ELIGIBLE",
        "SOAK_SAMPLE_GAP_TOO_LARGE",
        "SOAK_RELEASE_IDENTITY_MISMATCH",
        "SOAK_INFRASTRUCTURE_IDENTITY_DRIFT",
        "SOAK_SERVICE_RESTART_COUNT_INCREASE",
        "SOAK_PERSISTENT_SERVICE_INVOCATION_DRIFT",
        "SOAK_RESTART_COUNTER_REGRESSION",
        "SOAK_SAMPLE_COUNT_INSUFFICIENT",
    )
    return not blocker.startswith(eligibility_only)


def _terminal_blocker(blocker: str) -> bool:
    transient = (
        "SOAK_DURATION_NOT_ELIGIBLE",
        "SOAK_SAMPLE_COUNT_INSUFFICIENT",
        "SOAK_OBSERVATION_SEQUENCE_DID_NOT_PROGRESS",
        "SOAK_PROJECTION_SEQUENCE_DID_NOT_PROGRESS",
        "SOAK_UNRESOLVED_EFFECT_SCOPE_AT_END",
        "SOAK_MONITORING_PARITY_CONVERGENCE_PENDING",
        "SOAK_MONITORING_READINESS_RECOVERY_PENDING",
        "SOAK_TRANSPORT_RECOVERY_PENDING",
    )
    return not blocker.startswith(transient)


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate immutable CRA no-action soak samples")
    parser.add_argument("--samples", type=Path, required=True)
    parser.add_argument("--arena-release", required=True)
    parser.add_argument("--cra-release", required=True)
    parser.add_argument("--dell-release", required=True)
    parser.add_argument("--minimum-duration-seconds", type=float, default=24 * 3600)
    parser.add_argument("--maximum-sample-gap-seconds", type=float, default=90)
    parser.add_argument("--minimum-disk-free-bytes", type=int, default=1024 * 1024 * 1024)
    parser.add_argument("--minimum-credential-remaining-seconds", type=int, default=3600)
    parser.add_argument("--maximum-resident-memory-growth-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--maximum-open-fd-growth", type=int, default=64)
    parser.add_argument("--maximum-unresolved-scope-age-seconds", type=int, default=60)
    parser.add_argument("--maximum-safe-blocked-duration-seconds", type=int, default=60)
    parser.add_argument("--maximum-database-growth-bytes", type=int, default=512 * 1024 * 1024)
    args = parser.parse_args()
    report = evaluate_no_action_soak(
        read_samples(args.samples),
        expected_releases={"arena": args.arena_release, "cra": args.cra_release, "dell": args.dell_release},
        minimum_duration_seconds=args.minimum_duration_seconds,
        maximum_sample_gap_seconds=args.maximum_sample_gap_seconds,
        minimum_disk_free_bytes=args.minimum_disk_free_bytes,
        minimum_credential_remaining_seconds=args.minimum_credential_remaining_seconds,
        maximum_resident_memory_growth_bytes=args.maximum_resident_memory_growth_bytes,
        maximum_open_fd_growth=args.maximum_open_fd_growth,
        maximum_unresolved_scope_age_seconds=args.maximum_unresolved_scope_age_seconds,
        maximum_safe_blocked_duration_seconds=args.maximum_safe_blocked_duration_seconds,
        maximum_database_growth_bytes=args.maximum_database_growth_bytes,
    )
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))
    if not report["eligible"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
