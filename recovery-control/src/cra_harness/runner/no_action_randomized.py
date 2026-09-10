from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import time
from collections import Counter
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from cra_dell_recovery.time import isoformat_utc
from cra_no_action_soak.gate import evaluate_no_action_soak
from monitoring_projection.live_adapter import (
    MonitoringLiveAdapter,
    MonitoringLiveAdapterConfig,
    MonitoringLiveSnapshot,
)

SCHEMA = "cra.no_action_randomized_harness.v1"
START = datetime(2026, 8, 31, tzinfo=UTC)
RELEASES = {
    "arena": "arena-cra-projection-harness",
    "cra": "cra-no-action-harness",
    "dell": "dell-observation-harness",
}

GATE_SCENARIOS = (
    "healthy",
    "single_sample_service_failure",
    "early_window",
    "sample_gap",
    "timestamp_regression",
    "release_mismatch",
    "service_failure",
    "service_failure_counter_regression",
    "restart_increase",
    "explicit_restart_invocation_drift",
    "sequence_regression",
    "command_delivery_enabled",
    "control_capability_nonzero",
    "cra_physical_effect_nonzero",
    "credential_too_low",
    "disk_too_low",
    "resident_memory_growth",
    "open_fd_growth",
    "resource_limit_breach",
    "command_route_present",
    "duplicate_effect_scope",
    "unexplained_target_transition",
    "safe_target_transition",
    "target_change_without_transition",
    "target_counter_without_change",
    "target_counter_conservation",
    "epoch_baseline_drift",
    "effect_boundary_exceeds_request",
    "effect_unknown_exceeds_boundary",
    "unresolved_age_oracle_mismatch",
    "safe_blocked_duration_oracle_mismatch",
    "nonfinite_duration_limit",
    "nonfinite_gap_limit",
    "boolean_integer_limit",
    "malformed_counter",
    "compound_release_and_service_failure",
    "compound_target_and_epoch_drift",
    "compound_resource_and_control_fault",
    "compound_counter_and_duration_oracle_fault",
    "compound_temporal_and_sequence_regression",
)

ADAPTER_SCENARIOS = (
    "coherent_snapshot",
    "transient_current_newer",
    "transient_current_reduced_after_cycle",
    "persistent_current_newer",
    "persistent_current_reduced_after_cycle",
    "build_revision_mismatch",
    "source_revision_mismatch",
    "mutation_flag_unsafe",
    "cycle_state_mismatch",
    "build_mismatch_with_temporal_race",
    "mutation_unsafe_with_temporal_race",
)


@dataclass(frozen=True)
class _HarnessConfig:
    value: dict[str, Any]


class _SequenceRepository:
    def __init__(self, snapshots: list[MonitoringLiveSnapshot]) -> None:
        self.snapshots = list(snapshots)
        self.calls = 0

    def latest(self) -> MonitoringLiveSnapshot:
        self.calls += 1
        if len(self.snapshots) > 1:
            return self.snapshots.pop(0)
        return self.snapshots[0]


class _ConsistencyHarnessAdapter(MonitoringLiveAdapter):
    """Runs only the production identity/consistency selector, without I/O sources."""

    def __init__(self, repository: _SequenceRepository, *, retry_delays: tuple[float, ...]) -> None:
        self.config = cast(
            MonitoringLiveAdapterConfig,
            _HarnessConfig(
                {
                    "expected_monitoring_build_revision": "monitoring-build-harness",
                    "expected_monitoring_source_revision": "monitoring-source-harness",
                }
            ),
        )
        self.repository = repository
        self.consistency_retry_delays = retry_delays


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _sample(
    *,
    observed_at: datetime,
    observation_sequence: int,
    projection_sequence: int,
    baseline: dict[str, int],
    case_token: str,
    index: int,
) -> dict[str, Any]:
    observed_text = isoformat_utc(observed_at)
    return {
        "schema": "cra.no_action_soak_sample.v9",
        "observed_at": observed_text,
        "release_ids": dict(RELEASES),
        "arena": {
            "dell_pull_status": "READY",
            "live_adapter_status": "READY",
            "projection_status": "READY",
            "observation_sequence": observation_sequence,
            "projection_sequence": projection_sequence,
            "control_capability_count": 0,
            "physical_effect_count": 0,
            "service_restart_count": 0,
        },
        "cra": {
            "projection_pull_status": "READY",
            "runtime_readiness": "NO_ACTION_READY",
            "policy_decision": "WOULD_AUTHORIZE",
            "policy_action": None,
            "policy_reason_code": "CRA_OPERATING_MODE_NO_ACTION",
            "policy_candidate_reason_code": "confirmed_tcp_stall",
            "policy_decision_reason_code": "CRA_OPERATING_MODE_NO_ACTION",
            "policy_decision_digest": _digest(f"policy-decision-{case_token}-{index}"),
            "policy_reason_binding": "CANDIDATE_AND_DECISION_BOUND_V1",
            "policy_blockers": ["CRA_OPERATING_MODE_NO_ACTION"],
            "monitoring_readiness": {
                "input_fresh": True,
                "parity_clean": True,
                "projection_clean": True,
                "source_ready": True,
            },
            "monitoring_parity": {
                "monitoring_cycle_id": f"cycle-{case_token}-{index}",
                "parity": {
                    "schema": "monitoring_v4.live_parity.v2",
                    "parity_policy_revision": "monitoring-v4-parity-convergence-r2",
                    "equivalent": True,
                    "accepted_difference_count": 0,
                    "unclassified_contract_difference_count": 0,
                    "input_integrity_errors": [],
                    "verified_rollouts": [],
                    "domains": {
                        "delivery": {
                            "match": True,
                            "classification": "equivalent",
                            "expected_state": "bad",
                            "actual_state": "bad",
                            "expected_source": "subsystems_status.local_delivery",
                            "actual_sources": ["runtime_delivery_watchdog"],
                            "expected_observed_at": observed_text,
                            "actual_observed_at": observed_text,
                            "normalization_error": "",
                        }
                    },
                },
            },
            "incident_state": "CONFIRMED",
            "process_cpu_seconds": float(index),
            "command_delivery_enabled": False,
            "control_capability_count": 0,
            "physical_effect_count": 0,
            "authorization_row_count": 0,
            "command_row_count": 0,
            "service_restart_count": 0,
        },
        "dell": {
            "observation_server_active": True,
            "target_snapshot_valid": True,
            "control_capability_count": 0,
            "physical_effect_count": 0,
            "service_restart_count": 0,
        },
        "infrastructure": {
            "host_boot_ids": {
                "arena": f"arena-{case_token}",
                "cra": f"cra-{case_token}",
                "dell": f"dell-{case_token}",
            },
            "release_manifest_sha256": {
                "arena": _digest(f"release-arena-{case_token}"),
                "cra": _digest(f"release-cra-{case_token}"),
                "dell": _digest(f"release-dell-{case_token}"),
            },
            "runtime_manifest_sha256": {
                "arena": _digest(f"runtime-arena-{case_token}"),
                "cra": _digest(f"runtime-cra-{case_token}"),
                "dell": _digest(f"runtime-dell-{case_token}"),
            },
            "configuration_set_sha256": {
                "arena": _digest(f"configuration-arena-{case_token}"),
                "cra": _digest(f"configuration-cra-{case_token}"),
                "dell": _digest(f"configuration-dell-{case_token}"),
            },
            "maintenance_restart_policy_sha256": {
                "arena": _digest(f"maintenance-policy-arena-{case_token}"),
                "cra": _digest(f"maintenance-policy-cra-{case_token}"),
                "dell": _digest(f"maintenance-policy-dell-{case_token}"),
            },
            "rehearsal_evidence_sha256": {
                "cra_backup_restore": _digest(f"restore-{case_token}"),
                "cra_single_writer": _digest(f"writer-{case_token}"),
                "credential_rotation": _digest(f"credential-{case_token}"),
            },
            "target_identity_sha256": _digest(f"target-{case_token}-a"),
            "cra_database_integrity_check": "ok",
            "cra_database_journal_mode": "wal",
            "cra_archive_quick_check": "ok",
            "cra_archive_integrity_check": "ok",
            "cra_single_writer_lock": "ENFORCED",
            "cra_backup_restore_rehearsal": "PASS",
            "credential_rotation_rehearsal": "PASS",
            "credential_minimum_remaining_seconds": {
                "arena_cra": baseline["credential_remaining"],
                "arena_dell": baseline["credential_remaining"] + 30,
                "dell_target_observer": baseline["credential_remaining"] + 60,
            },
            "disk_free_bytes": {
                "arena": baseline["disk_free"] + 2 * 1024**3,
                "cra": baseline["disk_free"],
                "dell": baseline["disk_free"] + 4 * 1024**3,
            },
            "resident_memory_bytes": {
                "arena": baseline["arena_memory"] + index * 1024,
                "cra": baseline["cra_memory"] + index * 512,
                "dell": baseline["dell_memory"] + index * 256,
            },
            "open_fd_count": {
                "arena": baseline["arena_fds"] + index % 3,
                "cra": baseline["cra_fds"] + index % 2,
                "dell": baseline["dell_fds"] + index % 2,
            },
            "resource_limit_breach_count": {"arena": 0, "cra": 0, "dell": 0},
            "service_failed_invocation_count": {
                "arena_dell_host_status_pull": 0,
                "arena_dell_pull": 0,
                "arena_host_status_publisher": 0,
                "arena_live_adapter": 0,
                "arena_projection": 0,
                "arena_projection_server": 0,
                "cra_arena_host_status_pull": 0,
                "cra_projection_pull": 0,
                "cra_runtime": 0,
                "cra_soak_collector": 0,
                "dell_host_status_publisher": 0,
                "dell_observation_server": 0,
            },
            "persistent_service_invocation_ids": {
                "arena_projection_server": _digest(f"invocation-arena-{case_token}")[:32],
                "cra_runtime": _digest(f"invocation-cra-{case_token}")[:32],
                "dell_observation_server": _digest(f"invocation-dell-{case_token}")[:32],
            },
            "legacy_arena_recovery_active": False,
            "legacy_arena_recovery_restart_count": 0,
            "executable_command_route_present": False,
            "transport_recovery_health": {
                name: {
                    "schema": "cra.transport_recovery_state.v1",
                    "component": name,
                    "release_id": RELEASES["arena" if name == "arena_dell_pull" else "cra"],
                    "current_state": "READY",
                    "current_episode": None,
                    "last_episode": None,
                    "episode_count": 0,
                    "recovered_episode_count": 0,
                    "terminal_episode_count": 0,
                    "failure_attempt_count": 0,
                    "retry_attempt_count": 0,
                    "exhausted_invocation_count": 0,
                    "maximum_recovery_duration_ms": 0,
                    "event_count": 0,
                    "last_event_hash": "0" * 64,
                    "updated_at": observed_text,
                }
                for name in ("arena_dell_pull", "cra_projection_pull")
            },
        },
        "operations": {
            "effect_request_count": baseline["effect_requests"],
            "effect_boundary_count": baseline["effect_boundaries"],
            "effect_scope_count": baseline["effect_scopes"],
            "raw_outcome_unknown_count": baseline["outcome_unknown"],
            "unresolved_scope_count": 0,
            "oldest_unresolved_age_seconds": 0,
            "reconciliation_count": baseline["reconciliations"],
            "duplicate_scope_count": baseline["duplicates"],
            "epoch_effect_request_count": 0,
            "epoch_effect_boundary_count": 0,
            "epoch_outcome_unknown_count": 0,
            "epoch_reconciliation_count": 0,
            "epoch_duplicate_scope_count": 0,
            "target_transition_count": 0,
            "safe_target_transition_count": 0,
            "unexplained_target_transition_count": 0,
            "legacy_arena_epoch_invocation_count": 0,
            "cra_safe_blocked_episode_count": 0,
            "cra_max_safe_blocked_duration_seconds": 0,
            "cra_current_safe_blocked_duration_seconds": 0,
            "cra_hot_database_bytes": baseline["hot_database_bytes"] + index * 1024,
            "cra_hot_wal_bytes": baseline["hot_wal_bytes"],
            "cra_hot_shm_bytes": baseline["hot_shm_bytes"],
            "cra_hot_sqlite_total_bytes": baseline["hot_database_bytes"]
            + index * 1024
            + baseline["hot_wal_bytes"]
            + baseline["hot_shm_bytes"],
            "cra_archive_database_bytes": baseline["archive_database_bytes"] + index * 512,
            "cra_archive_wal_bytes": baseline["archive_wal_bytes"],
            "cra_archive_shm_bytes": baseline["archive_shm_bytes"],
            "cra_archive_sqlite_total_bytes": baseline["archive_database_bytes"]
            + index * 512
            + baseline["archive_wal_bytes"]
            + baseline["archive_shm_bytes"],
            "cra_archive_record_count": baseline["archive_records"] + index,
            "cra_archive_checkpoint_count": baseline["archive_checkpoints"] + index // 4,
        },
    }


def _campaign(
    rng: random.Random,
    case_seed: int,
    *,
    factors: tuple[int, ...] | None = None,
) -> tuple[list[dict[str, Any]], float]:
    if factors is None:
        sample_count = rng.randint(4, 12)
        offsets = [0, *sorted(rng.sample(range(1, 86_400), sample_count - 2)), 86_400]
        requests = rng.randint(8, 200)
        boundaries = rng.randint(0, requests)
    else:
        sample_count = 4 + factors[1]
        clock_pattern = factors[3]
        interior = sample_count - 2
        if clock_pattern == 0:
            offsets = [round(index * 86_400 / (sample_count - 1)) for index in range(sample_count)]
        elif clock_pattern == 1:
            offsets = [0, *(index * (index + 1) // 2 for index in range(1, interior + 1)), 86_400]
        elif clock_pattern == 2:
            offsets = [0, *(86_400 - index * (index + 1) // 2 for index in range(interior, 0, -1)), 86_400]
        elif clock_pattern == 3:
            offsets = [0, *sorted({1 + (index * 7919) % 86_398 for index in range(1, interior + 1)}), 86_400]
        elif clock_pattern == 4:
            offsets = [0, *range(1, interior + 1), 86_400]
        else:
            offsets = [0, *range(86_399 - interior, 86_399), 86_400]
        requests = (8, 16, 32, 64, 128)[factors[5]]
        boundaries = (0, requests // 4, requests // 2, requests - 1, requests)[factors[5]]
    baseline = {
        "credential_remaining": rng.randint(7_200, 172_800),
        "disk_free": ((2 + 2 * factors[4]) if factors is not None else rng.randint(2, 16)) * 1024**3,
        "arena_memory": ((50 + 20 * factors[4]) if factors is not None else rng.randint(50, 250)) * 1024**2,
        "cra_memory": ((30 + 16 * factors[4]) if factors is not None else rng.randint(30, 150)) * 1024**2,
        "dell_memory": ((30 + 12 * factors[4]) if factors is not None else rng.randint(30, 150)) * 1024**2,
        "arena_fds": (8 + 4 * factors[4]) if factors is not None else rng.randint(8, 80),
        "cra_fds": (8 + 3 * factors[4]) if factors is not None else rng.randint(8, 80),
        "dell_fds": (8 + 2 * factors[4]) if factors is not None else rng.randint(8, 80),
        "effect_requests": requests,
        "effect_boundaries": boundaries,
        "effect_scopes": rng.randint(0, requests),
        "outcome_unknown": rng.randint(0, boundaries),
        "reconciliations": rng.randint(0, 200),
        "duplicates": rng.randint(0, max(0, requests - 1)),
        "hot_database_bytes": ((1 + 8 * factors[6]) if factors is not None else rng.randint(1, 64)) * 1024**2,
        "hot_wal_bytes": ((4 * factors[6]) if factors is not None else rng.randint(0, 48)) * 1024**2,
        "hot_shm_bytes": 32 * 1024,
        "archive_database_bytes": ((1 + 6 * factors[6]) if factors is not None else rng.randint(1, 64)) * 1024**2,
        "archive_wal_bytes": ((3 * factors[6]) if factors is not None else rng.randint(0, 48)) * 1024**2,
        "archive_shm_bytes": 32 * 1024,
        "archive_records": rng.randint(0, 10_000),
        "archive_checkpoints": rng.randint(0, 100),
    }
    case_token = f"{case_seed:016x}" if factors is None else f"{case_seed:016x}-{factors[7]}"
    observation_sequence = rng.randint(1, 1_000_000)
    projection_sequence = rng.randint(1, 1_000_000)
    samples: list[dict[str, Any]] = []
    for index, offset in enumerate(offsets):
        observation_sequence += (1, 2, 10, 100)[factors[8]] if factors is not None else rng.randint(1, 100)
        projection_sequence += (1, 3, 11, 101)[factors[9]] if factors is not None else rng.randint(1, 100)
        samples.append(
            _sample(
                observed_at=START + timedelta(seconds=offset),
                observation_sequence=observation_sequence,
                projection_sequence=projection_sequence,
                baseline=baseline,
                case_token=case_token,
                index=index,
            )
        )
    maximum_gap = max(right - left for left, right in zip(offsets, offsets[1:], strict=False)) + 0.5
    return samples, maximum_gap


def _set_from(samples: list[dict[str, Any]], index: int, path: tuple[str, ...], value: Any) -> None:
    for sample in samples[index:]:
        item: dict[str, Any] = sample
        for name in path[:-1]:
            item = cast(dict[str, Any], item[name])
        item[path[-1]] = value


def _increment_from(samples: list[dict[str, Any]], index: int, path: tuple[str, ...], amount: int = 1) -> None:
    for sample in samples[index:]:
        item: dict[str, Any] = sample
        for name in path[:-1]:
            item = cast(dict[str, Any], item[name])
        item[path[-1]] = int(item[path[-1]]) + amount


def _gate_case(
    scenario: str,
    rng: random.Random,
    case_seed: int,
    *,
    factors: tuple[int, ...] | None = None,
) -> dict[str, Any]:
    samples, maximum_gap = _campaign(rng, case_seed, factors=factors)
    index = (
        min(len(samples) - 1, 1 + round(factors[2] * (len(samples) - 2) / 5)) if factors is not None else rng.randint(1, len(samples) - 1)
    )
    expected_blocker: str | None = None
    additional_expected_blockers: tuple[str, ...] = ()
    expected_error: str | None = None
    expected_eligible = scenario in {"healthy", "safe_target_transition"}
    expected_operational_healthy: bool | None = None
    margin = (0.1, 0.5, 1.0, 10.0)[factors[10]] if factors is not None else 0.5
    limits: dict[str, Any] = {"maximum_sample_gap_seconds": maximum_gap + margin}

    if scenario == "single_sample_service_failure":
        samples = [samples[0]]
        samples[0]["infrastructure"]["service_failed_invocation_count"]["arena_live_adapter"] = 1
        expected_blocker = "SOAK_SERVICE_INVOCATION_FAILURE"
        expected_operational_healthy = False
    elif scenario == "early_window":
        samples = samples[:-1]
        expected_blocker = "SOAK_DURATION_NOT_ELIGIBLE"
        expected_operational_healthy = True
    elif scenario == "sample_gap":
        limits["maximum_sample_gap_seconds"] = maximum_gap - 0.75
        expected_blocker = "SOAK_SAMPLE_GAP_TOO_LARGE"
        expected_operational_healthy = True
    elif scenario == "timestamp_regression":
        samples[index]["observed_at"] = samples[index - 1]["observed_at"]
        expected_blocker = "SOAK_SAMPLE_TIME_NOT_STRICTLY_INCREASING"
    elif scenario == "release_mismatch":
        samples[index]["release_ids"]["arena"] = "arena-cra-projection-unexpected"
        expected_blocker = "SOAK_RELEASE_IDENTITY_MISMATCH"
        expected_operational_healthy = True
    elif scenario == "service_failure":
        _set_from(samples, index, ("infrastructure", "service_failed_invocation_count", "cra_runtime"), 1)
        expected_blocker = "SOAK_SERVICE_INVOCATION_FAILURE"
    elif scenario == "service_failure_counter_regression":
        index = min(index, len(samples) - 2)
        samples[index]["infrastructure"]["service_failed_invocation_count"]["arena_projection"] = 1
        expected_blocker = "SOAK_SERVICE_FAILURE_COUNTER_REGRESSION"
    elif scenario == "restart_increase":
        _set_from(samples, index, ("cra", "service_restart_count"), 1)
        expected_blocker = "SOAK_SERVICE_RESTART_COUNT_INCREASE"
        expected_operational_healthy = True
    elif scenario == "explicit_restart_invocation_drift":
        _set_from(
            samples,
            index,
            ("infrastructure", "persistent_service_invocation_ids", "cra_runtime"),
            _digest(f"replacement-invocation-{case_seed}")[:32],
        )
        expected_blocker = "SOAK_PERSISTENT_SERVICE_INVOCATION_DRIFT:CRA_RUNTIME"
        expected_operational_healthy = True
    elif scenario == "sequence_regression":
        _set_from(samples, index, ("arena", "projection_sequence"), 1)
        expected_blocker = "SOAK_PROJECTION_SEQUENCE_REGRESSION"
    elif scenario == "command_delivery_enabled":
        samples[index]["cra"]["command_delivery_enabled"] = True
        expected_blocker = "SOAK_COMMAND_DELIVERY_ENABLED"
    elif scenario == "control_capability_nonzero":
        samples[index]["arena"]["control_capability_count"] = 1
        expected_blocker = "SOAK_ARENA_CONTROL_CAPABILITY_NONZERO"
    elif scenario == "cra_physical_effect_nonzero":
        samples[index]["cra"]["physical_effect_count"] = 1
        expected_blocker = "SOAK_CRA_PHYSICAL_EFFECT_NONZERO"
    elif scenario == "credential_too_low":
        samples[index]["infrastructure"]["credential_minimum_remaining_seconds"]["arena_cra"] = 1
        expected_blocker = "SOAK_CREDENTIAL_REMAINING_TOO_LOW"
    elif scenario == "disk_too_low":
        samples[index]["infrastructure"]["disk_free_bytes"]["cra"] = 1
        expected_blocker = "SOAK_DISK_FREE_TOO_LOW"
    elif scenario == "resident_memory_growth":
        first = int(samples[0]["infrastructure"]["resident_memory_bytes"]["cra"])
        _set_from(samples, index, ("infrastructure", "resident_memory_bytes", "cra"), first + 256 * 1024**2 + 1)
        expected_blocker = "SOAK_RESIDENT_MEMORY_GROWTH_EXCEEDED"
    elif scenario == "open_fd_growth":
        first = int(samples[0]["infrastructure"]["open_fd_count"]["arena"])
        _set_from(samples, index, ("infrastructure", "open_fd_count", "arena"), first + 65)
        expected_blocker = "SOAK_OPEN_FD_GROWTH_EXCEEDED"
    elif scenario == "resource_limit_breach":
        samples[index]["infrastructure"]["resource_limit_breach_count"]["dell"] = 1
        expected_blocker = "SOAK_RESOURCE_LIMIT_BREACH"
    elif scenario == "command_route_present":
        samples[index]["infrastructure"]["executable_command_route_present"] = True
        expected_blocker = "SOAK_EXECUTABLE_COMMAND_ROUTE_PRESENT"
    elif scenario == "duplicate_effect_scope":
        for path in (
            ("operations", "effect_request_count"),
            ("operations", "duplicate_scope_count"),
            ("operations", "epoch_effect_request_count"),
            ("operations", "epoch_duplicate_scope_count"),
        ):
            _increment_from(samples, index, path)
        expected_blocker = "SOAK_DUPLICATE_EFFECT_SCOPE_DETECTED"
    elif scenario == "unexplained_target_transition":
        _set_from(samples, index, ("infrastructure", "target_identity_sha256"), _digest(f"target-{case_seed}-b"))
        _set_from(samples, index, ("operations", "target_transition_count"), 1)
        _set_from(samples, index, ("operations", "unexplained_target_transition_count"), 1)
        expected_blocker = "SOAK_UNEXPLAINED_TARGET_TRANSITION"
    elif scenario == "safe_target_transition":
        _set_from(samples, index, ("infrastructure", "target_identity_sha256"), _digest(f"target-{case_seed}-b"))
        for path in (
            ("operations", "effect_request_count"),
            ("operations", "effect_boundary_count"),
            ("operations", "effect_scope_count"),
            ("operations", "epoch_effect_request_count"),
            ("operations", "epoch_effect_boundary_count"),
            ("dell", "physical_effect_count"),
            ("operations", "target_transition_count"),
            ("operations", "safe_target_transition_count"),
        ):
            _increment_from(samples, index, path)
    elif scenario == "target_change_without_transition":
        _set_from(samples, index, ("infrastructure", "target_identity_sha256"), _digest(f"target-{case_seed}-b"))
        expected_blocker = "SOAK_TARGET_TRANSITION_COUNTER_MISMATCH"
    elif scenario == "target_counter_without_change":
        for path in (
            ("operations", "effect_request_count"),
            ("operations", "effect_boundary_count"),
            ("operations", "effect_scope_count"),
            ("operations", "epoch_effect_request_count"),
            ("operations", "epoch_effect_boundary_count"),
            ("dell", "physical_effect_count"),
            ("operations", "target_transition_count"),
            ("operations", "safe_target_transition_count"),
        ):
            _increment_from(samples, index, path)
        expected_blocker = "SOAK_TARGET_TRANSITION_COUNTER_MISMATCH"
    elif scenario == "target_counter_conservation":
        _set_from(samples, index, ("infrastructure", "target_identity_sha256"), _digest(f"target-{case_seed}-b"))
        _set_from(samples, index, ("operations", "target_transition_count"), 1)
        expected_blocker = "SOAK_TARGET_TRANSITION_COUNTER_CONSERVATION_INVALID"
    elif scenario == "epoch_baseline_drift":
        _increment_from(samples, index, ("operations", "effect_request_count"))
        expected_blocker = "SOAK_EPOCH_COUNTER_BASELINE_DRIFT"
    elif scenario == "effect_boundary_exceeds_request":
        for sample in samples:
            sample["operations"]["effect_boundary_count"] = sample["operations"]["effect_request_count"] + 1
        expected_blocker = "SOAK_EFFECT_COUNTER_RELATION_INVALID"
    elif scenario == "effect_unknown_exceeds_boundary":
        for sample in samples:
            sample["operations"]["raw_outcome_unknown_count"] = sample["operations"]["effect_boundary_count"] + 1
        expected_blocker = "SOAK_EFFECT_COUNTER_RELATION_INVALID"
    elif scenario == "unresolved_age_oracle_mismatch":
        for sample in samples:
            sample["operations"]["oldest_unresolved_age_seconds"] = 1
        expected_blocker = "SOAK_UNRESOLVED_AGE_ORACLE_MISMATCH"
    elif scenario == "safe_blocked_duration_oracle_mismatch":
        for sample in samples:
            sample["operations"]["cra_current_safe_blocked_duration_seconds"] = 1
        expected_blocker = "SOAK_SAFE_BLOCKED_DURATION_ORACLE_MISMATCH"
    elif scenario == "nonfinite_duration_limit":
        limits["minimum_duration_seconds"] = float("nan")
        expected_error = "SOAK_GATE_LIMIT_INVALID"
    elif scenario == "nonfinite_gap_limit":
        limits["maximum_sample_gap_seconds"] = float("inf")
        expected_error = "SOAK_GATE_LIMIT_INVALID"
    elif scenario == "boolean_integer_limit":
        limits["minimum_disk_free_bytes"] = False
        expected_error = "SOAK_GATE_LIMIT_INVALID"
    elif scenario == "malformed_counter":
        samples[index]["operations"]["effect_request_count"] = True
        expected_blocker = "SOAK_SAMPLE_COUNTER_INVALID"
    elif scenario == "compound_release_and_service_failure":
        samples[index]["release_ids"]["arena"] = "arena-cra-projection-unexpected"
        samples[index]["infrastructure"]["service_failed_invocation_count"]["cra_runtime"] = 1
        expected_blocker = "SOAK_RELEASE_IDENTITY_MISMATCH"
        additional_expected_blockers = ("SOAK_SERVICE_INVOCATION_FAILURE",)
    elif scenario == "compound_target_and_epoch_drift":
        _set_from(samples, index, ("infrastructure", "target_identity_sha256"), _digest(f"target-{case_seed}-b"))
        _increment_from(samples, index, ("operations", "effect_request_count"))
        expected_blocker = "SOAK_TARGET_TRANSITION_COUNTER_MISMATCH"
        additional_expected_blockers = ("SOAK_EPOCH_COUNTER_BASELINE_DRIFT",)
    elif scenario == "compound_resource_and_control_fault":
        first = int(samples[0]["infrastructure"]["resident_memory_bytes"]["cra"])
        _set_from(samples, index, ("infrastructure", "resident_memory_bytes", "cra"), first + 256 * 1024**2 + 1)
        samples[index]["cra"]["command_delivery_enabled"] = True
        expected_blocker = "SOAK_RESIDENT_MEMORY_GROWTH_EXCEEDED"
        additional_expected_blockers = ("SOAK_COMMAND_DELIVERY_ENABLED",)
    elif scenario == "compound_counter_and_duration_oracle_fault":
        for sample in samples:
            sample["operations"]["effect_boundary_count"] = sample["operations"]["effect_request_count"] + 1
            sample["operations"]["cra_current_safe_blocked_duration_seconds"] = 1
        expected_blocker = "SOAK_EFFECT_COUNTER_RELATION_INVALID"
        additional_expected_blockers = ("SOAK_SAFE_BLOCKED_DURATION_ORACLE_MISMATCH",)
    elif scenario == "compound_temporal_and_sequence_regression":
        samples[index]["observed_at"] = samples[index - 1]["observed_at"]
        _set_from(samples, index, ("arena", "projection_sequence"), 1)
        expected_blocker = "SOAK_SAMPLE_TIME_NOT_STRICTLY_INCREASING"
        additional_expected_blockers = ("SOAK_PROJECTION_SEQUENCE_REGRESSION",)
    elif scenario != "healthy":
        raise AssertionError(f"HARNESS_GATE_SCENARIO_UNKNOWN:{scenario}")

    report: dict[str, Any] | None = None
    actual_error: str | None = None
    try:
        report = evaluate_no_action_soak(samples, expected_releases=RELEASES, **limits)
    except ValueError as error:
        actual_error = str(error)

    if expected_error is not None:
        passed = actual_error == expected_error
    elif actual_error is not None or report is None:
        passed = False
    else:
        blockers = [str(item) for item in report["blockers"]]
        expected_blocker_prefixes = tuple(item for item in (expected_blocker, *additional_expected_blockers) if item is not None)
        blocker_match = all(any(item.startswith(prefix) for item in blockers) for prefix in expected_blocker_prefixes)
        operational_match = expected_operational_healthy is None or report["operational_healthy"] is expected_operational_healthy
        passed = blocker_match and report["eligible"] is expected_eligible and operational_match
    return {
        "scenario": scenario,
        "engine": "cra_no_action_soak.gate.evaluate_no_action_soak",
        "passed": passed,
        "classification": "PASS" if passed else "SUT_DEFECT",
        "expected": {
            "eligible": expected_eligible if expected_error is None else None,
            "blocker_prefix": expected_blocker,
            "additional_blocker_prefixes": list(additional_expected_blockers),
            "error": expected_error,
            "operational_healthy": expected_operational_healthy,
        },
        "actual": {
            "eligible": None if report is None else report["eligible"],
            "blockers": [] if report is None else report["blockers"],
            "error": actual_error,
            "operational_healthy": None if report is None else report["operational_healthy"],
        },
        "input_sample_count": len(samples),
        "simulated_seconds": (
            0.0
            if len(samples) < 2
            else (
                datetime.fromisoformat(str(samples[-1]["observed_at"]).replace("Z", "+00:00"))
                - datetime.fromisoformat(str(samples[0]["observed_at"]).replace("Z", "+00:00"))
            ).total_seconds()
        ),
    }


def _monitoring_snapshot(rng: random.Random, case_seed: int) -> MonitoringLiveSnapshot:
    completed = START + timedelta(seconds=rng.randint(1, 86_400))
    current_observed = completed - timedelta(milliseconds=rng.randint(1, 900))
    current_reduced = current_observed + timedelta(milliseconds=rng.randint(0, 1))
    state = rng.choice(("good", "bad", "unknown"))
    return MonitoringLiveSnapshot(
        cycle={
            "cycle_id": f"cycle-{case_seed:016x}",
            "completed_at": isoformat_utc(completed),
            "build_revision": "monitoring-build-harness",
            "source_revision": "monitoring-source-harness",
            "current_states_json": json.dumps({"delivery": state}, separators=(",", ":"), sort_keys=True),
            "real_delivery_enabled": 0,
            "runtime_mutation_enabled": 0,
        },
        current={
            "snapshot_id": f"snapshot-{case_seed:016x}",
            "state": state,
            "observed_at": isoformat_utc(current_observed),
            "reduced_at": isoformat_utc(current_reduced),
        },
        active_episode=None,
        latest_closed_episode=None,
        candidate=None,
        observations=(),
        component_health=(),
        database_user="harness_read_only",
    )


def _adapter_case(scenario: str, rng: random.Random, case_seed: int) -> dict[str, Any]:
    coherent = _monitoring_snapshot(rng, case_seed)
    expected_error: str | None = None
    expected_calls = 1
    snapshots = [coherent]
    retry_delays: tuple[float, ...] = ()
    cycle = dict(coherent.cycle)
    current = dict(coherent.current)
    completed = datetime.fromisoformat(str(cycle["completed_at"]).replace("Z", "+00:00"))

    if scenario == "transient_current_newer":
        current["observed_at"] = isoformat_utc(completed + timedelta(milliseconds=1))
        snapshots = [replace(coherent, current=current), coherent]
        retry_delays = (0.0,)
        expected_calls = 2
    elif scenario == "transient_current_reduced_after_cycle":
        current["reduced_at"] = isoformat_utc(completed + timedelta(milliseconds=1))
        snapshots = [replace(coherent, current=current), coherent]
        retry_delays = (0.0,)
        expected_calls = 2
    elif scenario == "persistent_current_newer":
        current["observed_at"] = isoformat_utc(completed + timedelta(milliseconds=1))
        snapshots = [replace(coherent, current=current)]
        retry_delays = (0.0, 0.0)
        expected_calls = 3
        expected_error = "LIVE_ADAPTER_DELIVERY_CURRENT_NEWER_THAN_CYCLE"
    elif scenario == "persistent_current_reduced_after_cycle":
        current["reduced_at"] = isoformat_utc(completed + timedelta(milliseconds=1))
        snapshots = [replace(coherent, current=current)]
        retry_delays = (0.0, 0.0)
        expected_calls = 3
        expected_error = "LIVE_ADAPTER_DELIVERY_CURRENT_REDUCED_AFTER_CYCLE"
    elif scenario == "build_revision_mismatch":
        cycle["build_revision"] = "unexpected-build"
        snapshots = [replace(coherent, cycle=cycle), coherent]
        retry_delays = (0.0,)
        expected_error = "LIVE_ADAPTER_MONITORING_BUILD_REVISION_MISMATCH"
    elif scenario == "source_revision_mismatch":
        cycle["source_revision"] = "unexpected-source"
        snapshots = [replace(coherent, cycle=cycle), coherent]
        retry_delays = (0.0,)
        expected_error = "LIVE_ADAPTER_MONITORING_SOURCE_REVISION_MISMATCH"
    elif scenario == "mutation_flag_unsafe":
        cycle["runtime_mutation_enabled"] = 1
        snapshots = [replace(coherent, cycle=cycle), coherent]
        retry_delays = (0.0,)
        expected_error = "LIVE_ADAPTER_MONITORING_MUTATION_FLAG_UNSAFE"
    elif scenario == "cycle_state_mismatch":
        cycle["current_states_json"] = json.dumps({"delivery": "mismatch"})
        snapshots = [replace(coherent, cycle=cycle), coherent]
        retry_delays = (0.0,)
        expected_error = "LIVE_ADAPTER_CYCLE_CURRENT_STATE_MISMATCH"
    elif scenario == "build_mismatch_with_temporal_race":
        cycle["build_revision"] = "unexpected-build"
        current["observed_at"] = isoformat_utc(completed + timedelta(milliseconds=1))
        snapshots = [replace(coherent, cycle=cycle, current=current), coherent]
        retry_delays = (0.0,)
        expected_error = "LIVE_ADAPTER_MONITORING_BUILD_REVISION_MISMATCH"
    elif scenario == "mutation_unsafe_with_temporal_race":
        cycle["runtime_mutation_enabled"] = 1
        current["observed_at"] = isoformat_utc(completed + timedelta(milliseconds=1))
        snapshots = [replace(coherent, cycle=cycle, current=current), coherent]
        retry_delays = (0.0,)
        expected_error = "LIVE_ADAPTER_MONITORING_MUTATION_FLAG_UNSAFE"
    elif scenario != "coherent_snapshot":
        raise AssertionError(f"HARNESS_ADAPTER_SCENARIO_UNKNOWN:{scenario}")

    repository = _SequenceRepository(snapshots)
    adapter = _ConsistencyHarnessAdapter(repository, retry_delays=retry_delays)
    actual_error: str | None = None
    selected: MonitoringLiveSnapshot | None = None
    try:
        selected = adapter._latest_consistent_snapshot()
    except ValueError as error:
        actual_error = str(error)
    passed = actual_error == expected_error and repository.calls == expected_calls
    if expected_error is None:
        passed = passed and selected is coherent
    return {
        "scenario": scenario,
        "engine": "monitoring_projection.live_adapter.MonitoringLiveAdapter._latest_consistent_snapshot",
        "passed": passed,
        "classification": "PASS" if passed else "SUT_DEFECT",
        "expected": {"error": expected_error, "repository_calls": expected_calls},
        "actual": {"error": actual_error, "repository_calls": repository.calls},
        "input_sample_count": expected_calls,
        "simulated_seconds": float(sum(retry_delays)),
    }


def run_randomized_no_action_harness(*, cases: int = 1_000, seed: int = 2_026_090_1) -> dict[str, Any]:
    catalogue = [("gate", name) for name in GATE_SCENARIOS] + [("adapter", name) for name in ADAPTER_SCENARIOS]
    if cases < len(catalogue):
        raise ValueError(f"HARNESS_CASE_COUNT_TOO_LOW:{len(catalogue)}")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("HARNESS_SEED_INVALID")
    rng = random.Random(seed)
    results: list[dict[str, Any]] = []
    started = time.perf_counter()
    scheduled_cases: list[tuple[str, str]] = []
    while len(scheduled_cases) < cases:
        randomized_round = list(catalogue)
        rng.shuffle(randomized_round)
        scheduled_cases.extend(randomized_round)
    for index, (engine, scenario) in enumerate(scheduled_cases[:cases]):
        case_seed = rng.getrandbits(64)
        case_rng = random.Random(case_seed)
        case_id = f"{index + 1:04d}-{_digest(f'{seed}:{case_seed}:{engine}:{scenario}')[:12]}"
        try:
            case_result = _gate_case(scenario, case_rng, case_seed) if engine == "gate" else _adapter_case(scenario, case_rng, case_seed)
        except Exception as error:  # harness defects must remain distinct from expected SUT rejections
            case_result = {
                "scenario": scenario,
                "engine": engine,
                "passed": False,
                "classification": "HARNESS_ERROR",
                "expected": {},
                "actual": {"error": f"{type(error).__name__}:{error}"},
                "input_sample_count": 0,
                "simulated_seconds": 0.0,
            }
        results.append({"case_id": case_id, "case_seed": case_seed, **case_result})
    wall_seconds = time.perf_counter() - started
    simulated_seconds = sum(float(item["simulated_seconds"]) for item in results)
    sut_defects = [item for item in results if item["classification"] == "SUT_DEFECT"]
    harness_errors = [item for item in results if item["classification"] == "HARNESS_ERROR"]
    verdict = "HARNESS_FAILURE" if harness_errors else "SUT_DEFECT" if sut_defects else "PASS"
    deterministic_payload = json.dumps(results, separators=(",", ":"), sort_keys=True).encode()
    scenario_counts = Counter(f"{item['engine']}:{item['scenario']}" for item in results)
    return {
        "schema": SCHEMA,
        "result": verdict,
        "seed": seed,
        "case_count": cases,
        "passed_case_count": sum(bool(item["passed"]) for item in results),
        "sut_defect_count": len(sut_defects),
        "harness_error_count": len(harness_errors),
        "gate_case_count": sum(item["engine"].startswith("cra_no_action_soak") for item in results),
        "adapter_case_count": sum(item["engine"].startswith("monitoring_projection") for item in results),
        "scenario_counts": dict(sorted(scenario_counts.items())),
        "scenario_count": len(catalogue),
        "minimum_scenario_variant_count": min(scenario_counts.values()),
        "maximum_scenario_variant_count": max(scenario_counts.values()),
        "coverage_matrix_complete": set(scenario_counts)
        == {f"cra_no_action_soak.gate.evaluate_no_action_soak:{name}" for name in GATE_SCENARIOS}
        | {f"monitoring_projection.live_adapter.MonitoringLiveAdapter._latest_consistent_snapshot:{name}" for name in ADAPTER_SCENARIOS},
        "case_results_sha256": hashlib.sha256(deterministic_payload).hexdigest(),
        "simulated_seconds": simulated_seconds,
        "simulated_days": round(simulated_seconds / 86_400, 6),
        "wall_duration_seconds": round(wall_seconds, 6),
        "time_compression_ratio": round(simulated_seconds / max(wall_seconds, 0.000001), 3),
        "boundary": {
            "production_state_read_count": 0,
            "credential_read_count": 0,
            "network_call_count": 0,
            "production_mutation_count": 0,
            "physical_effect_count": 0,
            "live_port_bind_count": 0,
        },
        "formal_soak_replacement": False,
        "interpretation": (
            "Deterministic randomized, compressed-time evidence against production gate and consistency-selection code; "
            "it does not replace the revision-bound 24-hour wall-clock soak."
        ),
        "mismatch_examples": [
            {
                "case_id": item["case_id"],
                "case_seed": item["case_seed"],
                "scenario": item["scenario"],
                "classification": item["classification"],
                "expected": item["expected"],
                "actual": item["actual"],
            }
            for item in results
            if not item["passed"]
        ][:100],
        "cases": results,
    }


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run isolated randomized and compressed-time CRA no-action tests")
    volume = parser.add_mutually_exclusive_group()
    volume.add_argument("--cases", type=int)
    volume.add_argument("--variants-per-scenario", type=int, default=64)
    parser.add_argument("--seed", type=int, default=2_026_090_1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.variants_per_scenario <= 0:
        raise ValueError("HARNESS_VARIANTS_PER_SCENARIO_INVALID")
    cases = args.cases or args.variants_per_scenario * (len(GATE_SCENARIOS) + len(ADAPTER_SCENARIOS))
    report = run_randomized_no_action_harness(cases=cases, seed=args.seed)
    _atomic_json(args.output, report)
    artifact_sha256 = hashlib.sha256(args.output.read_bytes()).hexdigest()
    print(
        json.dumps(
            {
                "artifact": str(args.output),
                "artifact_sha256": artifact_sha256,
                "case_results_sha256": report["case_results_sha256"],
                "case_count": report["case_count"],
                "harness_error_count": report["harness_error_count"],
                "result": report["result"],
                "simulated_days": report["simulated_days"],
                "sut_defect_count": report["sut_defect_count"],
                "wall_duration_seconds": report["wall_duration_seconds"],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0 if report["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
