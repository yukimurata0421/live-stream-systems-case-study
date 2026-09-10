from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cra_dell_recovery.time import isoformat_utc
from cra_no_action_soak.gate import read_samples
from cra_no_action_soak.sample import append_sample, compose_no_action_sample

NOW = datetime(2026, 8, 30, 9, 0, tzinfo=UTC)
RELEASES = {
    "arena": "arena-cra-projection-release-a",
    "cra": "cra-no-action-release-a",
    "dell": "dell-observation-release-a",
}


def _capture() -> dict[str, object]:
    observed = isoformat_utc(NOW - timedelta(seconds=1))
    zero = {"control_capability_count": 0, "physical_effect_count": 0}
    return {
        "schema": "cra.no_action_soak_capture.v9",
        "arena_dell_pull": {
            "schema": "monitoring_v4.dell_observation_pull_status.v1",
            "status": "READY",
            "puller_release_id": RELEASES["arena"],
            "observation_sequence": 100,
            "observed_at": observed,
            **zero,
        },
        "arena_live_adapter": {
            "schema": "monitoring_v4.cra_live_adapter_status.v2",
            "status": "READY",
            "adapter_release_id": RELEASES["arena"],
            "monitoring_cycle_id": "monitoring-cycle-a",
            "observation_sequence": 100,
            "readiness": {
                "input_fresh": True,
                "parity_clean": True,
                "projection_clean": True,
                "source_ready": True,
            },
            "parity": {
                "schema": "monitoring_v4.live_parity.v2",
                "parity_policy_revision": "monitoring-v4-parity-convergence-r2",
                "equivalent": True,
                "accepted_difference_count": 0,
                "unclassified_contract_difference_count": 0,
                "input_integrity_errors": [],
                "domains": {
                    "delivery": {
                        "match": True,
                        "classification": "equivalent",
                        "expected_state": "good",
                        "actual_state": "good",
                        "expected_source": "subsystems_status.local_delivery",
                        "actual_sources": ["runtime_delivery_watchdog"],
                        "expected_observed_at": observed,
                        "actual_observed_at": observed,
                        "normalization_error": "",
                    }
                },
            },
            "observed_at": observed,
            **zero,
        },
        "arena_projection": {
            "schema": "monitoring_v4.cra_projection_producer_status.v1",
            "status": "READY",
            "producer_release_id": RELEASES["arena"],
            "observation_sequence": 101,
            "observed_at": observed,
            **zero,
        },
        "cra_projection_pull": {
            "schema": "cra.monitoring_projection_pull_status.v1",
            "status": "READY",
            "puller_release_id": RELEASES["cra"],
            "observed_at": observed,
            **zero,
        },
        "cra_runtime": {
            "schema": "cra.runtime_status.v1",
            "runtime_release_id": RELEASES["cra"],
            "readiness": "NO_ACTION_READY",
            "policy_decision": "WOULD_AUTHORIZE",
            "policy_action": None,
            "policy_reason_code": "CRA_OPERATING_MODE_NO_ACTION",
            "policy_candidate_reason_code": "confirmed_tcp_stall",
            "policy_decision_reason_code": "CRA_OPERATING_MODE_NO_ACTION",
            "policy_decision_digest": "8" * 64,
            "policy_reason_binding": "CANDIDATE_AND_DECISION_BOUND_V1",
            "policy_blockers": ["CRA_OPERATING_MODE_NO_ACTION"],
            "monitoring_readiness": {
                "input_fresh": True,
                "parity_clean": True,
                "projection_clean": True,
                "source_ready": True,
            },
            "incident_state": "CONFIRMED",
            "process_cpu_seconds": 1.0,
            "command_delivery_enabled": False,
            "observed_at": observed,
            **zero,
        },
        "cra_database": {
            "schema": "cra.no_action_db_summary.v1",
            "runtime_release_id": RELEASES["cra"],
            "authorization_row_count": 0,
            "command_row_count": 0,
            "observed_at": observed,
        },
        "dell": {
            "schema": "cra.dell_observation_soak_summary.v1",
            "release_id": RELEASES["dell"],
            "observation_server_active": True,
            "target_snapshot_valid": True,
            "observed_at": observed,
            **zero,
        },
        "restarts": {
            "schema": "cra.no_action_restart_summary.v1",
            "arena": 0,
            "cra": 0,
            "dell": 0,
            "observed_at": observed,
        },
        "infrastructure": {
            "schema": "cra.no_action_infrastructure_summary.v4",
            "observed_at": observed,
            "host_boot_ids": {"arena": "arena-boot-a", "cra": "cra-boot-a", "dell": "dell-boot-a"},
            "release_manifest_sha256": {"arena": "a" * 64, "cra": "b" * 64, "dell": "c" * 64},
            "runtime_manifest_sha256": {"arena": "d" * 64, "cra": "e" * 64, "dell": "f" * 64},
            "configuration_set_sha256": {"arena": "1" * 64, "cra": "2" * 64, "dell": "3" * 64},
            "maintenance_restart_policy_sha256": {"arena": "9" * 64, "cra": "a" * 64, "dell": "b" * 64},
            "rehearsal_evidence_sha256": {
                "cra_backup_restore": "4" * 64,
                "cra_single_writer": "5" * 64,
                "credential_rotation": "6" * 64,
            },
            "target_identity_sha256": "7" * 64,
            "cra_database_integrity_check": "ok",
            "cra_database_journal_mode": "wal",
            "cra_archive_quick_check": "ok",
            "cra_archive_integrity_check": "ok",
            "cra_single_writer_lock": "ENFORCED",
            "cra_backup_restore_rehearsal": "PASS",
            "credential_rotation_rehearsal": "PASS",
            "credential_minimum_remaining_seconds": {
                "arena_cra": 12 * 3600,
                "arena_dell": 12 * 3600,
                "dell_target_observer": 12 * 3600,
            },
            "disk_free_bytes": {"arena": 8 * 1024**3, "cra": 4 * 1024**3, "dell": 16 * 1024**3},
            "resident_memory_bytes": {"arena": 100_000_000, "cra": 50_000_000, "dell": 40_000_000},
            "open_fd_count": {"arena": 20, "cra": 10, "dell": 10},
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
                "arena_projection_server": "a" * 32,
                "cra_runtime": "b" * 32,
                "dell_observation_server": "c" * 32,
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
                    "updated_at": observed,
                }
                for name in ("arena_dell_pull", "cra_projection_pull")
            },
        },
        "operations": {
            "schema": "cra.no_action_operation_summary.v1",
            "observed_at": observed,
            "effect_request_count": 14,
            "effect_boundary_count": 14,
            "effect_scope_count": 7,
            "raw_outcome_unknown_count": 7,
            "unresolved_scope_count": 0,
            "oldest_unresolved_age_seconds": 0,
            "reconciliation_count": 7,
            "duplicate_scope_count": 7,
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
            "cra_hot_database_bytes": 1_000_000,
            "cra_hot_wal_bytes": 50_000_000,
            "cra_hot_shm_bytes": 32_768,
            "cra_hot_sqlite_total_bytes": 51_032_768,
            "cra_archive_database_bytes": 100_000,
            "cra_archive_wal_bytes": 0,
            "cra_archive_shm_bytes": 32_768,
            "cra_archive_sqlite_total_bytes": 132_768,
            "cra_archive_record_count": 0,
            "cra_archive_checkpoint_count": 0,
        },
    }


def test_soak_sample_composes_exact_gate_input_and_appends_durably(tmp_path: Path) -> None:
    sample = compose_no_action_sample(_capture(), expected_releases=RELEASES, now=NOW)
    assert sample["arena"]["observation_sequence"] == 100
    assert sample["cra"]["command_delivery_enabled"] is False
    assert sample["cra"]["monitoring_readiness"] == _capture()["cra_runtime"]["monitoring_readiness"]
    assert sample["cra"]["policy_blockers"] == ["CRA_OPERATING_MODE_NO_ACTION"]
    assert sample["cra"]["policy_candidate_reason_code"] == "confirmed_tcp_stall"
    assert sample["cra"]["policy_decision_reason_code"] == "CRA_OPERATING_MODE_NO_ACTION"
    assert sample["dell"]["target_snapshot_valid"] is True
    assert sample["infrastructure"]["cra_single_writer_lock"] == "ENFORCED"

    output = tmp_path / "soak/samples.jsonl"
    append_sample(output, sample)
    append_sample(output, sample)
    assert read_samples(output) == [sample, sample]
    assert os.stat(output).st_mode & 0o777 == 0o600


def test_soak_sample_rejects_stale_release_mismatch_effect_and_unsafe_output(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="SOAK_CAPTURE_MAXIMUM_AGE_INVALID"):
        compose_no_action_sample(_capture(), expected_releases=RELEASES, now=NOW, maximum_input_age_seconds=True)
    with pytest.raises(ValueError, match="SOAK_CAPTURE_MAXIMUM_AGE_INVALID"):
        compose_no_action_sample(_capture(), expected_releases=RELEASES, now=NOW, maximum_input_age_seconds="10")  # type: ignore[arg-type]

    stale = _capture()
    stale["arena_projection"]["observed_at"] = isoformat_utc(NOW - timedelta(minutes=5))  # type: ignore[index]
    with pytest.raises(ValueError, match="SOAK_CAPTURE_SECTION_STALE"):
        compose_no_action_sample(stale, expected_releases=RELEASES, now=NOW)

    mismatch = _capture()
    mismatch["cra_runtime"]["runtime_release_id"] = "wrong-release"  # type: ignore[index]
    with pytest.raises(ValueError, match="SOAK_CAPTURE_CRA_RUNTIME_RELEASE_MISMATCH"):
        compose_no_action_sample(mismatch, expected_releases=RELEASES, now=NOW)

    effect = _capture()
    effect["dell"]["physical_effect_count"] = 1  # type: ignore[index]
    sample = compose_no_action_sample(effect, expected_releases=RELEASES, now=NOW)
    assert sample["dell"]["physical_effect_count"] == 1

    identity_drift = _capture()
    identity_drift["infrastructure"]["target_identity_sha256"] = "not-a-digest"  # type: ignore[index]
    with pytest.raises(ValueError, match="SOAK_CAPTURE_TARGET_IDENTITY_SHA256_INVALID"):
        compose_no_action_sample(identity_drift, expected_releases=RELEASES, now=NOW)

    malformed_readiness = _capture()
    malformed_readiness["cra_runtime"]["monitoring_readiness"]["parity_clean"] = 1  # type: ignore[index]
    with pytest.raises(ValueError, match="SOAK_CAPTURE_MONITORING_READINESS_VALUE_INVALID"):
        compose_no_action_sample(malformed_readiness, expected_releases=RELEASES, now=NOW)

    duplicate_blocker = _capture()
    duplicate_blocker["cra_runtime"]["policy_blockers"] = [  # type: ignore[index]
        "CRA_OPERATING_MODE_NO_ACTION",
        "CRA_OPERATING_MODE_NO_ACTION",
    ]
    with pytest.raises(ValueError, match="SOAK_CAPTURE_POLICY_BLOCKERS_DUPLICATE"):
        compose_no_action_sample(duplicate_blocker, expected_releases=RELEASES, now=NOW)

    output = tmp_path / "samples.jsonl"
    output.write_text(json.dumps(deepcopy(effect)), encoding="utf-8")
    os.chmod(output, 0o600)
    symlink = tmp_path / "samples-link.jsonl"
    symlink.symlink_to(output)
    with pytest.raises(OSError):
        append_sample(symlink, sample)

    unsafe = tmp_path / "unsafe.jsonl"
    unsafe.write_text("", encoding="utf-8")
    os.chmod(unsafe, 0o660)
    with pytest.raises(ValueError, match="SOAK_EVIDENCE_OUTPUT_PERMISSIONS_UNSAFE"):
        append_sample(unsafe, sample)
