from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from cra_dell_recovery.time import isoformat_utc
from cra_no_action_soak import gate as gate_module
from cra_no_action_soak.gate import evaluate_no_action_soak

START = datetime(2026, 8, 30, tzinfo=UTC)
RELEASES = {
    "arena": "arena-cra-projection-release-a",
    "cra": "cra-no-action-release-a",
    "dell": "dell-observation-release-a",
}


def _equivalent_parity(at: datetime) -> dict[str, Any]:
    observed_at = isoformat_utc(at)
    return {
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
                "expected_observed_at": observed_at,
                "actual_observed_at": observed_at,
                "normalization_error": "",
            },
            "youtube_input_quality": {
                "match": True,
                "classification": "equivalent",
                "expected_state": "good",
                "actual_state": "good",
                "expected_source": "operational_reliability_burn_status.raw_current",
                "actual_sources": ["youtube_input_quality_oauth"],
                "expected_observed_at": observed_at,
                "actual_observed_at": observed_at,
                "normalization_error": "",
            },
        },
    }


def _youtube_candidate_parity(at: datetime) -> dict[str, Any]:
    parity = _equivalent_parity(at)
    actual_at = isoformat_utc(at)
    expected_at = isoformat_utc(at - timedelta(seconds=48))
    parity["equivalent"] = False
    parity["unclassified_contract_difference_count"] = 1
    parity["domains"]["youtube_input_quality"] = {
        "match": False,
        "classification": "candidate_source_snapshot_skew_convergence",
        "expected_state": "bad",
        "actual_state": "good",
        "expected_source": "operational_reliability_burn_status.raw_current",
        "actual_sources": ["youtube_input_quality_oauth"],
        "expected_observed_at": expected_at,
        "actual_observed_at": actual_at,
        "snapshot_skew_sec": -48,
        "normalization_error": "",
        "convergence_policy_revision": "monitoring-v4-parity-convergence-r2",
        "maximum_absolute_skew_sec": 600,
        "convergence_timeout_sec": 600,
    }
    return parity


def _sample(*, at: datetime, sequence: int) -> dict[str, Any]:
    return {
        "schema": "cra.no_action_soak_sample.v9",
        "observed_at": isoformat_utc(at),
        "release_ids": RELEASES,
        "arena": {
            "dell_pull_status": "READY",
            "live_adapter_status": "READY",
            "projection_status": "READY",
            "observation_sequence": sequence,
            "projection_sequence": sequence,
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
            "policy_decision_digest": "8" * 64,
            "policy_reason_binding": "CANDIDATE_AND_DECISION_BOUND_V1",
            "policy_blockers": ["CRA_OPERATING_MODE_NO_ACTION"],
            "monitoring_readiness": {
                "input_fresh": True,
                "parity_clean": True,
                "projection_clean": True,
                "source_ready": True,
            },
            "monitoring_parity": {
                "monitoring_cycle_id": f"cycle-{sequence}",
                "parity": _equivalent_parity(at),
            },
            "incident_state": "CONFIRMED",
            "process_cpu_seconds": float(sequence),
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
                    "updated_at": isoformat_utc(at),
                }
                for name in ("arena_dell_pull", "cra_projection_pull")
            },
        },
        "operations": {
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
            "cra_archive_record_count": sequence,
            "cra_archive_checkpoint_count": 1,
        },
    }


def _eligible_samples() -> list[dict[str, Any]]:
    return [
        _sample(at=START + timedelta(seconds=offset), sequence=1_000 + index) for index, offset in enumerate(range(0, 24 * 3600 + 31, 30))
    ]


def _two_sample_window() -> list[dict[str, Any]]:
    return [
        _sample(at=START, sequence=1_000),
        _sample(at=START + timedelta(hours=24), sequence=1_001),
    ]


def test_no_action_soak_gate_accepts_full_window_with_zero_control_and_effect() -> None:
    report = evaluate_no_action_soak(
        _eligible_samples(),
        expected_releases=RELEASES,
        maximum_sample_gap_seconds=31,
    )

    assert report["eligible"] is True
    assert report["duration_seconds"] >= 24 * 3600
    assert report["blockers"] == []


def test_no_action_soak_gate_negative_controls_detect_effect_sequence_and_identity_faults() -> None:
    samples = _eligible_samples()
    samples[10]["cra"]["physical_effect_count"] = 1
    samples[20]["arena"]["projection_sequence"] = 1
    samples[30]["release_ids"] = {**RELEASES, "arena": "unexpected-release"}
    samples[40]["infrastructure"]["host_boot_ids"]["cra"] = "cra-boot-b"

    report = evaluate_no_action_soak(
        samples,
        expected_releases=RELEASES,
        maximum_sample_gap_seconds=31,
    )

    assert report["eligible"] is False
    assert any("SOAK_CRA_PHYSICAL_EFFECT_NONZERO" in blocker for blocker in report["blockers"])
    assert "SOAK_PROJECTION_SEQUENCE_REGRESSION" in report["blockers"]
    assert any("SOAK_RELEASE_IDENTITY_MISMATCH" in blocker for blocker in report["blockers"])
    assert any("SOAK_INFRASTRUCTURE_IDENTITY_DRIFT" in blocker for blocker in report["blockers"])


def test_no_action_soak_gate_never_promotes_early_or_gapped_evidence() -> None:
    early = _eligible_samples()[:10]
    report = evaluate_no_action_soak(early, expected_releases=RELEASES, maximum_sample_gap_seconds=31)
    assert report["eligible"] is False
    assert "SOAK_DURATION_NOT_ELIGIBLE" in report["blockers"]
    assert report["status"] == "NOT_YET_ELIGIBLE"
    assert report["terminal_failure"] is False

    gapped = deepcopy(_eligible_samples())
    del gapped[1:10]
    report = evaluate_no_action_soak(gapped, expected_releases=RELEASES, maximum_sample_gap_seconds=31)
    assert report["eligible"] is False
    assert "SOAK_SAMPLE_GAP_TOO_LARGE" in report["blockers"]


def _set_youtube_candidate(sample: dict[str, Any], at: datetime) -> None:
    sample["cra"]["monitoring_readiness"]["parity_clean"] = False
    sample["cra"]["monitoring_parity"]["parity"] = _youtube_candidate_parity(at)
    sample["cra"]["policy_decision"] = "BLOCKED"
    sample["cra"]["policy_reason_code"] = "MONITORING_READINESS_PARITY_CLEAN_FALSE"
    sample["cra"]["policy_decision_reason_code"] = "MONITORING_READINESS_PARITY_CLEAN_FALSE"
    sample["cra"]["policy_blockers"] = [
        "CRA_OPERATING_MODE_NO_ACTION",
        "MONITORING_READINESS_PARITY_CLEAN_FALSE",
    ]


def test_no_action_soak_gate_waits_for_and_accepts_bounded_parity_convergence() -> None:
    candidate = _sample(at=START, sequence=1_000)
    _set_youtube_candidate(candidate, START)
    proof = _sample(at=START + timedelta(seconds=60), sequence=1_001)

    pending = evaluate_no_action_soak(
        [candidate],
        expected_releases=RELEASES,
        minimum_duration_seconds=60,
        maximum_sample_gap_seconds=61,
    )
    assert pending["status"] == "NOT_YET_ELIGIBLE"
    assert pending["terminal_failure"] is False
    assert "SOAK_MONITORING_PARITY_CONVERGENCE_PENDING" in pending["blockers"]
    assert pending["parity_evidence"]["pending_difference_count"] == 1

    converged = evaluate_no_action_soak(
        [candidate, proof],
        expected_releases=RELEASES,
        minimum_duration_seconds=60,
        maximum_sample_gap_seconds=61,
    )
    assert converged["status"] == "PASS"
    assert converged["parity_evidence"]["accepted_difference_count"] == 1
    assert converged["parity_evidence"]["pending_difference_count"] == 0


def test_no_action_soak_gate_fails_closed_on_late_wrong_source_or_mutated_parity() -> None:
    candidate = _sample(at=START, sequence=1_000)
    _set_youtube_candidate(candidate, START)
    late = _sample(at=START + timedelta(seconds=601), sequence=1_001)
    report = evaluate_no_action_soak(
        [candidate, late],
        expected_releases=RELEASES,
        minimum_duration_seconds=1,
        maximum_sample_gap_seconds=700,
    )
    assert report["status"] == "FAIL"
    assert report["terminal_failure"] is True
    assert "SOAK_MONITORING_PARITY_CONVERGENCE_TIMEOUT:youtube_input_quality" in report["blockers"]

    wrong_source = deepcopy(candidate)
    wrong_source["cra"]["monitoring_parity"]["parity"]["domains"]["youtube_input_quality"]["actual_sources"] = ["unrelated_source"]
    report = evaluate_no_action_soak([wrong_source], expected_releases=RELEASES)
    assert "SOAK_MONITORING_PARITY_ACTUAL_SOURCE_MISMATCH" in report["blockers"]
    assert report["terminal_failure"] is True

    mutated = deepcopy(candidate)
    repeated = deepcopy(candidate)
    repeated["observed_at"] = isoformat_utc(START + timedelta(seconds=30))
    repeated["cra"]["process_cpu_seconds"] += 1
    repeated["cra"]["monitoring_parity"]["parity"]["domains"]["delivery"]["actual_state"] = "good"
    report = evaluate_no_action_soak(
        [mutated, repeated],
        expected_releases=RELEASES,
        minimum_duration_seconds=30,
        maximum_sample_gap_seconds=31,
    )
    assert "SOAK_MONITORING_PARITY_CYCLE_MUTATED" in report["blockers"]
    assert report["terminal_failure"] is True


def test_no_action_soak_gate_separates_candidate_identity_from_runtime_health() -> None:
    samples = _eligible_samples()
    samples[40]["infrastructure"]["host_boot_ids"]["cra"] = "cra-boot-b"
    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=31)

    assert report["eligible"] is False
    assert report["operational_healthy"] is True
    assert any("SOAK_INFRASTRUCTURE_IDENTITY_DRIFT" in blocker for blocker in report["blockers"])


def test_no_action_soak_gate_allows_fenced_effect_transition_but_rejects_duplicate_scope() -> None:
    samples = _eligible_samples()
    for sample in samples[10:]:
        sample["dell"]["physical_effect_count"] = 1
        sample["operations"]["effect_request_count"] = 15
        sample["operations"]["effect_boundary_count"] = 15
        sample["operations"]["effect_scope_count"] = 8
        sample["operations"]["epoch_effect_request_count"] = 1
        sample["operations"]["epoch_effect_boundary_count"] = 1
        sample["operations"]["target_transition_count"] = 1
        sample["operations"]["safe_target_transition_count"] = 1
        sample["infrastructure"]["target_identity_sha256"] = "8" * 64
    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=31)
    assert report["eligible"] is True

    for sample in samples[20:]:
        sample["operations"]["duplicate_scope_count"] = 8
        sample["operations"]["epoch_duplicate_scope_count"] = 1
    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=31)
    assert report["eligible"] is False
    assert any("SOAK_DUPLICATE_EFFECT_SCOPE_DETECTED" in blocker for blocker in report["blockers"])


def test_no_action_soak_gate_rejects_restart_increase_and_malformed_counter_without_crashing() -> None:
    restarted = _eligible_samples()
    restarted[10]["cra"]["service_restart_count"] = 1
    report = evaluate_no_action_soak(restarted, expected_releases=RELEASES, maximum_sample_gap_seconds=31)
    assert report["eligible"] is False
    assert any("SOAK_SERVICE_RESTART_COUNT_INCREASE" in blocker for blocker in report["blockers"])

    malformed = _eligible_samples()
    malformed[10]["arena"]["observation_sequence"] = "not-an-integer"
    report = evaluate_no_action_soak(malformed, expected_releases=RELEASES, maximum_sample_gap_seconds=31)
    assert report["eligible"] is False
    assert any("SOAK_SAMPLE_COUNTER_INVALID" in blocker for blocker in report["blockers"])


def test_no_action_soak_gate_rejects_explicit_restart_even_when_nrestarts_stays_zero() -> None:
    samples = _eligible_samples()
    for sample in samples[10:]:
        sample["infrastructure"]["persistent_service_invocation_ids"]["cra_runtime"] = "d" * 32

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=31)

    assert report["eligible"] is False
    assert report["terminal_failure"] is True
    assert report["status"] == "FAIL"
    assert "SOAK_PERSISTENT_SERVICE_INVOCATION_DRIFT:CRA_RUNTIME" in report["blockers"]
    assert all(sample["cra"]["service_restart_count"] == 0 for sample in samples)


def test_no_action_soak_gate_rejects_failed_oneshot_between_samples_and_counter_regression() -> None:
    samples = _eligible_samples()
    for sample in samples[10:20]:
        sample["infrastructure"]["service_failed_invocation_count"]["arena_live_adapter"] = 1

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=31)

    assert report["eligible"] is False
    assert report["blockers"].count("SOAK_SERVICE_INVOCATION_FAILURE:ARENA_LIVE_ADAPTER") == 1
    assert any("SOAK_SERVICE_FAILURE_COUNTER_REGRESSION" in blocker for blocker in report["blockers"])


def test_no_action_soak_gate_validates_operational_faults_in_first_sample() -> None:
    sample = _sample(at=START, sequence=1_000)
    sample["infrastructure"]["service_failed_invocation_count"]["arena_live_adapter"] = 1

    report = evaluate_no_action_soak([sample], expected_releases=RELEASES)

    assert report["eligible"] is False
    assert report["operational_healthy"] is False
    assert "SOAK_SAMPLE_COUNT_INSUFFICIENT" in report["blockers"]
    assert report["blockers"].count("SOAK_SERVICE_INVOCATION_FAILURE:ARENA_LIVE_ADAPTER") == 1


@pytest.mark.parametrize(
    "service_name",
    [
        "arena_dell_host_status_pull",
        "arena_host_status_publisher",
        "cra_arena_host_status_pull",
        "cra_soak_collector",
        "dell_host_status_publisher",
    ],
)
def test_no_action_soak_gate_observes_signed_status_pipeline_failures(service_name: str) -> None:
    sample = _sample(at=START, sequence=1_000)
    sample["infrastructure"]["service_failed_invocation_count"][service_name] = 1

    report = evaluate_no_action_soak([sample], expected_releases=RELEASES)

    reason = service_name.upper()
    assert report["operational_healthy"] is False
    assert f"SOAK_SERVICE_INVOCATION_FAILURE:{reason}" in report["blockers"]


def test_no_action_soak_gate_bounds_persistent_service_failure_blockers() -> None:
    samples = _eligible_samples()
    for sample in samples[10:]:
        sample["infrastructure"]["service_failed_invocation_count"]["cra_projection_pull"] = 1

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=31)

    service_blockers = [item for item in report["blockers"] if item.startswith("SOAK_TRANSPORT_FAILURE_UNACCOUNTED:")]
    assert service_blockers == ["SOAK_TRANSPORT_FAILURE_UNACCOUNTED:CRA_PROJECTION_PULL"]


def test_no_action_soak_gate_accepts_accounted_recovered_transport_episode() -> None:
    samples = _eligible_samples()
    for sample in samples[10:]:
        sample["infrastructure"]["service_failed_invocation_count"]["cra_projection_pull"] = 1
        health = sample["infrastructure"]["transport_recovery_health"]["cra_projection_pull"]
        health.update(
            {
                "episode_count": 1,
                "recovered_episode_count": 1,
                "failure_attempt_count": 5,
                "retry_attempt_count": 4,
                "exhausted_invocation_count": 1,
                "maximum_recovery_duration_ms": 30000,
                "event_count": 6,
                "last_event_hash": "d" * 64,
                "last_episode": {"state": "RECOVERED"},
            }
        )

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=31)

    assert report["eligible"] is True
    assert report["recovery_evidence"]["cra_projection_pull"] == {
        "recovered_episode_count": 1,
        "maximum_recovery_duration_ms": 30000,
    }


def test_no_action_soak_gate_accepts_bounded_readiness_outage_after_recovery() -> None:
    samples = _eligible_samples()
    for sample in samples[10:15]:
        sample["cra"]["monitoring_readiness"]["source_ready"] = False

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=31)

    assert report["eligible"] is True
    assert report["readiness_evidence_ms"]["source_ready"] == 150000


def test_no_action_soak_gate_waits_for_current_readiness_recovery_without_terminal_failure() -> None:
    samples = _eligible_samples()
    samples[-1]["cra"]["monitoring_readiness"]["source_ready"] = False

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=31)

    assert report["eligible"] is False
    assert report["terminal_failure"] is False
    assert "SOAK_MONITORING_READINESS_RECOVERY_PENDING:SOURCE_READY" in report["blockers"]


def test_no_action_soak_gate_bounds_all_persistent_sample_blockers_with_occurrence_evidence() -> None:
    samples = _eligible_samples()
    affected_count = len(samples) - 10
    for sample in samples[10:]:
        sample["cra"]["monitoring_readiness"]["parity_clean"] = False
        sample["infrastructure"]["cra_database_integrity_check"] = "corrupt"
        sample["infrastructure"]["credential_minimum_remaining_seconds"]["arena_cra"] = 1

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=31)

    assert report["schema"] == "cra.no_action_soak_gate.v11"
    assert report["blockers"] == [
        "SOAK_CRA_DATABASE_INTEGRITY_NOT_OK",
        "SOAK_CREDENTIAL_REMAINING_TOO_LOW",
        "SOAK_MONITORING_PARITY_READINESS_CONTRADICTION",
    ]
    for code in report["blockers"]:
        assert report["blocker_evidence"][code] == {
            "occurrence_count": affected_count,
            "first_sample_index": 10,
            "last_sample_index": len(samples) - 1,
        }


def test_no_action_soak_gate_preserves_semantic_identity_around_sample_index() -> None:
    blockers, evidence = gate_module._summarize_blockers(
        {
            "SOAK_EPOCH_COUNTER_EXCEEDS_TOTAL:effect_request_count:10",
            "SOAK_EPOCH_COUNTER_EXCEEDS_TOTAL:effect_request_count:11",
            "SOAK_EPOCH_COUNTER_EXCEEDS_TOTAL:reconciliation_count:12",
            "SOAK_SAMPLE_INVALID:13:MISSING_FIELD",
            "SOAK_SAMPLE_INVALID:14:MISSING_FIELD",
            "SOAK_SAMPLE_INVALID:15:INVALID_TYPE",
        }
    )

    assert blockers == [
        "SOAK_EPOCH_COUNTER_EXCEEDS_TOTAL:effect_request_count",
        "SOAK_EPOCH_COUNTER_EXCEEDS_TOTAL:reconciliation_count",
        "SOAK_SAMPLE_INVALID:INVALID_TYPE",
        "SOAK_SAMPLE_INVALID:MISSING_FIELD",
    ]
    assert evidence["SOAK_EPOCH_COUNTER_EXCEEDS_TOTAL:effect_request_count"] == {
        "occurrence_count": 2,
        "first_sample_index": 10,
        "last_sample_index": 11,
    }
    assert evidence["SOAK_SAMPLE_INVALID:MISSING_FIELD"] == {
        "occurrence_count": 2,
        "first_sample_index": 13,
        "last_sample_index": 14,
    }


def test_no_action_soak_gate_rejects_nonfinite_and_boolean_limits() -> None:
    samples = _two_sample_window()
    with pytest.raises(ValueError, match="SOAK_GATE_LIMIT_INVALID"):
        evaluate_no_action_soak(samples, expected_releases=RELEASES, minimum_duration_seconds=float("nan"))
    with pytest.raises(ValueError, match="SOAK_GATE_LIMIT_INVALID"):
        evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=float("inf"))
    with pytest.raises(ValueError, match="SOAK_GATE_LIMIT_INVALID"):
        evaluate_no_action_soak(samples, expected_releases=RELEASES, minimum_disk_free_bytes=False)
    with pytest.raises(ValueError, match="SOAK_EXPECTED_RELEASES_INVALID"):
        evaluate_no_action_soak(samples, expected_releases={**RELEASES, "cra": True})


def test_no_action_soak_gate_binds_target_digest_to_transition_oracle() -> None:
    samples = _two_sample_window()
    samples[1]["infrastructure"]["target_identity_sha256"] = "8" * 64

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=86_401)

    assert report["eligible"] is False
    assert any("SOAK_TARGET_TRANSITION_COUNTER_MISMATCH" in blocker for blocker in report["blockers"])
    assert any("SOAK_TARGET_TRANSITION_CLASSIFICATION_MISMATCH" in blocker for blocker in report["blockers"])


def test_no_action_soak_gate_rejects_epoch_baseline_and_counter_relation_drift() -> None:
    baseline_drift = _two_sample_window()
    baseline_drift[1]["operations"]["effect_request_count"] += 1
    report = evaluate_no_action_soak(
        baseline_drift,
        expected_releases=RELEASES,
        maximum_sample_gap_seconds=86_401,
    )
    assert any("SOAK_EPOCH_COUNTER_BASELINE_DRIFT" in blocker for blocker in report["blockers"])

    impossible_relation = _two_sample_window()
    for sample in impossible_relation:
        sample["operations"]["effect_boundary_count"] = sample["operations"]["effect_request_count"] + 1
    report = evaluate_no_action_soak(
        impossible_relation,
        expected_releases=RELEASES,
        maximum_sample_gap_seconds=86_401,
    )
    assert any("SOAK_EFFECT_COUNTER_RELATION_INVALID" in blocker for blocker in report["blockers"])


def test_no_action_soak_gate_rejects_derived_duration_oracle_inconsistency() -> None:
    samples = _two_sample_window()
    for sample in samples:
        sample["operations"]["oldest_unresolved_age_seconds"] = 1
        sample["operations"]["cra_current_safe_blocked_duration_seconds"] = 1

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=86_401)

    assert any("SOAK_UNRESOLVED_AGE_ORACLE_MISMATCH" in blocker for blocker in report["blockers"])
    assert any("SOAK_SAFE_BLOCKED_DURATION_ORACLE_MISMATCH" in blocker for blocker in report["blockers"])


def test_no_action_soak_gate_rejects_database_credential_resource_and_command_route_faults() -> None:
    samples = _eligible_samples()
    samples[10]["infrastructure"]["cra_database_integrity_check"] = "corrupt"
    samples[20]["infrastructure"]["credential_minimum_remaining_seconds"]["arena_cra"] = 30
    samples[30]["infrastructure"]["disk_free_bytes"]["cra"] = 100
    samples[40]["infrastructure"]["resident_memory_bytes"]["cra"] += 300 * 1024**2
    samples[50]["infrastructure"]["open_fd_count"]["arena"] += 65
    samples[60]["infrastructure"]["resource_limit_breach_count"]["dell"] = 1
    samples[70]["infrastructure"]["executable_command_route_present"] = True

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=31)

    assert report["eligible"] is False
    assert any("SOAK_CRA_DATABASE_INTEGRITY_NOT_OK" in blocker for blocker in report["blockers"])
    assert any("SOAK_CREDENTIAL_REMAINING_TOO_LOW" in blocker for blocker in report["blockers"])
    assert any("SOAK_DISK_FREE_TOO_LOW" in blocker for blocker in report["blockers"])
    assert any("SOAK_RESIDENT_MEMORY_GROWTH_EXCEEDED" in blocker for blocker in report["blockers"])
    assert any("SOAK_OPEN_FD_GROWTH_EXCEEDED" in blocker for blocker in report["blockers"])
    assert any("SOAK_RESOURCE_LIMIT_BREACH" in blocker for blocker in report["blockers"])
    assert any("SOAK_EXECUTABLE_COMMAND_ROUTE_PRESENT" in blocker for blocker in report["blockers"])


def test_v6_gate_rejects_policy_parity_reason_and_sqlite_total_semantic_drift() -> None:
    samples = _two_sample_window()
    samples[1]["cra"]["monitoring_readiness"]["parity_clean"] = False
    samples[1]["cra"]["policy_reason_code"] = "confirmed_tcp_stall"
    samples[1]["cra"]["policy_decision_reason_code"] = "confirmed_tcp_stall"
    samples[1]["operations"]["cra_hot_sqlite_total_bytes"] += 1

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=86_401)

    assert any("SOAK_MONITORING_PARITY_READINESS_CONTRADICTION" in blocker for blocker in report["blockers"])
    assert any("SOAK_CRA_WOULD_AUTHORIZE_SEMANTICS_INVALID" in blocker for blocker in report["blockers"])
    assert any("SOAK_CRA_HOT_SQLITE_TOTAL_INVALID" in blocker for blocker in report["blockers"])


def test_v6_gate_detects_each_reason_binding_dimension_and_nested_shape_tamper() -> None:
    samples = _two_sample_window()
    samples[1]["cra"]["policy_candidate_reason_code"] = "NO_CONFIRMED_RECOVERY_CANDIDATE"
    samples[1]["cra"]["policy_decision_digest"] = "not-a-digest"
    samples[1]["cra"]["policy_reason_binding"] = "LEGACY_UNBOUND"
    samples[1]["cra"]["monitoring_readiness"]["extra"] = True
    samples[1]["cra"]["policy_blockers"].append("CRA_OPERATING_MODE_NO_ACTION")

    report = evaluate_no_action_soak(samples, expected_releases=RELEASES, maximum_sample_gap_seconds=86_401)

    assert any("SOAK_CRA_WOULD_AUTHORIZE_SEMANTICS_INVALID" in blocker for blocker in report["blockers"])
    assert any("SOAK_CRA_DECISION_DIGEST_INVALID" in blocker for blocker in report["blockers"])
    assert any("SOAK_CRA_REASON_BINDING_INVALID" in blocker for blocker in report["blockers"])
    assert any("SOAK_CRA_MONITORING_READINESS_INVALID" in blocker for blocker in report["blockers"])
    assert any("SOAK_CRA_POLICY_BLOCKERS_DUPLICATE" in blocker for blocker in report["blockers"])


def test_v6_gate_bounds_persistent_duration_blockers_across_many_samples() -> None:
    samples = [_sample(at=START + timedelta(seconds=index), sequence=1_000 + index) for index in range(128)]
    for sample in samples:
        sample["operations"]["unresolved_scope_count"] = 1
        sample["operations"]["oldest_unresolved_age_seconds"] = 100
        sample["operations"]["cra_safe_blocked_episode_count"] = 1
        sample["operations"]["cra_max_safe_blocked_duration_seconds"] = 100
        sample["operations"]["cra_current_safe_blocked_duration_seconds"] = 100

    report = evaluate_no_action_soak(
        samples,
        expected_releases=RELEASES,
        maximum_unresolved_scope_age_seconds=1,
        maximum_safe_blocked_duration_seconds=1,
    )

    assert report["blockers"].count("SOAK_UNRESOLVED_EFFECT_SCOPE_TOO_OLD") == 1
    assert report["blockers"].count("SOAK_CRA_SAFE_BLOCKED_DURATION_EXCEEDED") == 1
    assert report["blockers"].count("SOAK_CRA_CURRENT_SAFE_BLOCKED_TOO_LONG") == 1
    assert not any(blocker.startswith("SOAK_UNRESOLVED_EFFECT_SCOPE_TOO_OLD:") for blocker in report["blockers"])
