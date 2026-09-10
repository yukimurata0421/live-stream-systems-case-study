from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import pytest

from cra_authority.heartbeat import AuthorityHeartbeatPublisher
from cra_dell_recovery.models import LocalRecoveryCandidate, MonitoringReadiness
from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.authority import AgentAuthorityLease


@dataclass
class FakeMonotonic:
    value: float = 100.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def readiness(*, fresh: bool = True, ready: bool = True) -> MonitoringReadiness:
    return MonitoringReadiness(fresh, ready, isoformat_utc(utc_now()), "fixture")


def lease_pair(environment: object) -> tuple[AuthorityHeartbeatPublisher, AgentAuthorityLease, FakeMonotonic]:
    clock = FakeMonotonic()
    publisher = AuthorityHeartbeatPublisher(
        environment.central,
        environment.central_codec,
        interval_seconds=2,
        lease_ttl_seconds=15,  # type: ignore[attr-defined]
    )
    lease = AgentAuthorityLease(
        environment.dell,  # type: ignore[attr-defined]
        environment.agent_codec,  # type: ignore[attr-defined]
        suspect_after_seconds=4,
        lease_ttl_seconds=15,
        monotonic=clock,
    )
    return publisher, lease, clock


def test_normal_heartbeat_keeps_central_active(environment: object) -> None:
    publisher, lease, clock = lease_pair(environment)
    heartbeat = publisher.build("stream-target", readiness())
    assert heartbeat is not None
    assert lease.receive(heartbeat) == "CENTRAL_ACTIVE"
    clock.advance(2)
    assert lease.tick("stream-target") == "CENTRAL_ACTIVE"


def test_delayed_heartbeat_enters_suspect_then_recovers_inside_ttl(environment: object) -> None:
    publisher, lease, clock = lease_pair(environment)
    first = publisher.build("stream-target", readiness())
    assert first is not None
    lease.receive(first)
    clock.advance(5)
    assert lease.tick("stream-target") == "CENTRAL_SUSPECT"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]
    second = publisher.build("stream-target", readiness())
    assert second is not None
    assert lease.receive(second) == "CENTRAL_ACTIVE"


def test_monitoring_stale_keeps_authority_lease_but_disables_actions(environment: object) -> None:
    publisher, lease, clock = lease_pair(environment)
    first = publisher.build("stream-target", readiness())
    assert first is not None
    lease.receive(first)
    not_ready = publisher.build("stream-target", readiness(fresh=False))
    assert not_ready is not None
    assert not_ready["decision_ready"] is False
    assert lease.receive(not_ready) == "CENTRAL_ACTIVE"
    for _ in range(7):
        clock.advance(2)
        repeated = publisher.build("stream-target", readiness(fresh=False))
        assert repeated is not None
        assert lease.receive(repeated) == "CENTRAL_ACTIVE"
        assert lease.tick("stream-target") == "CENTRAL_ACTIVE"
    assert environment.dell.fence("stream-target")["action_ready"] == 0  # type: ignore[attr-defined]
    command = environment.command("monitoring-not-ready")  # type: ignore[attr-defined]
    assert environment.service().handle_command(command)["reason_code"] == "CENTRAL_ACTION_NOT_READY"  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_stale_heartbeat_sequence_cannot_flip_action_readiness(environment: object) -> None:
    publisher, lease, _ = lease_pair(environment)
    ready = publisher.build("stream-target", readiness())
    not_ready = publisher.build("stream-target", readiness(fresh=False))
    assert ready is not None and not_ready is not None
    assert lease.receive(not_ready) == "CENTRAL_ACTIVE"
    assert environment.dell.fence("stream-target")["action_ready"] == 0  # type: ignore[attr-defined]
    assert lease.receive(ready) == "STALE_HEARTBEAT_SEQUENCE"
    assert environment.dell.fence("stream-target")["action_ready"] == 0  # type: ignore[attr-defined]

    newer_ready = publisher.build("stream-target", readiness())
    assert newer_ready is not None
    assert lease.receive(newer_ready) == "CENTRAL_ACTIVE"
    assert environment.dell.fence("stream-target")["action_ready"] == 1  # type: ignore[attr-defined]
    assert lease.receive(not_ready) == "STALE_HEARTBEAT_SEQUENCE"
    assert environment.dell.fence("stream-target")["action_ready"] == 1  # type: ignore[attr-defined]


def test_expired_heartbeat_cannot_renew_lease_or_flip_action_readiness(environment: object) -> None:
    publisher, lease, clock = lease_pair(environment)
    ready = publisher.build("stream-target", readiness())
    assert ready is not None
    assert lease.receive(ready) == "CENTRAL_ACTIVE"
    previous = dict(environment.dell.fence("stream-target"))  # type: ignore[attr-defined]
    current = utc_now()
    expired = environment.central_codec.encode(  # type: ignore[attr-defined]
        {
            **ready,
            "heartbeat_id": "expired-heartbeat",
            "heartbeat_seq": int(ready["heartbeat_seq"]) + 1,
            "decision_ready": False,
            "issued_at": isoformat_utc(current - timedelta(seconds=30)),
            "expires_at": isoformat_utc(current - timedelta(seconds=15)),
        }
    )
    clock.advance(2)

    assert lease.receive(expired) == "HEARTBEAT_EXPIRED"
    current_fence = environment.dell.fence("stream-target")  # type: ignore[attr-defined]
    assert current_fence["heartbeat_seq"] == previous["heartbeat_seq"]
    assert current_fence["action_ready"] == 1


def test_local_fallback_does_not_exit_on_heartbeat_alone(environment: object) -> None:
    publisher, lease, clock = lease_pair(environment)
    first = publisher.build("stream-target", readiness())
    assert first is not None
    lease.receive(first)
    clock.advance(16)
    assert lease.tick("stream-target") == "LOCAL_FALLBACK"
    late = publisher.build("stream-target", readiness())
    assert late is not None
    assert lease.receive(late) == "LOCAL_AUTHORITY_ACTIVE"
    assert environment.dell.fence("stream-target")["authority_state"] == "LOCAL_FALLBACK"  # type: ignore[attr-defined]


def test_restore_state_forbids_heartbeat(environment: object) -> None:
    publisher, _, _ = lease_pair(environment)
    environment.central.mark_restored("old-backup")  # type: ignore[attr-defined]
    assert publisher.build("stream-target", readiness()) is None


def test_reconciling_state_forbids_heartbeat(environment: object) -> None:
    publisher, _, _ = lease_pair(environment)
    environment.central.connection.execute(  # type: ignore[attr-defined]
        "UPDATE targets SET authority_state='RECONCILING' WHERE target_id='stream-target'"
    )
    assert publisher.build("stream-target", readiness()) is None


def test_central_unwritable_forbids_heartbeat(environment: object, monkeypatch: pytest.MonkeyPatch) -> None:
    publisher, _, _ = lease_pair(environment)
    monkeypatch.setattr(environment.central, "delivery_allowed", lambda: False)  # type: ignore[attr-defined]
    assert publisher.build("stream-target", readiness()) is None


def test_agent_restart_does_not_restore_monotonic_lease(environment: object) -> None:
    publisher, lease, _ = lease_pair(environment)
    heartbeat = publisher.build("stream-target", readiness())
    assert heartbeat is not None
    lease.receive(heartbeat)
    lease.reset_after_agent_restart("stream-target")
    assert lease.tick("stream-target") == "AGENT_STARTUP_RECONCILING"
    assert environment.dell.fence("stream-target")["last_heartbeat_received_at"] is None  # type: ignore[attr-defined]


def test_reconciliation_grace_replaces_old_epoch_monotonic_deadline(environment: object) -> None:
    publisher, lease, clock = lease_pair(environment)
    heartbeat = publisher.build("stream-target", readiness())
    assert heartbeat is not None
    lease.receive(heartbeat)
    clock.advance(16)
    assert lease.tick("stream-target") == "LOCAL_FALLBACK"
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "CENTRAL_SUSPECT", "RECONCILED_AWAITING_VALID_HEARTBEAT"
    )

    lease.arm_reconciliation_grace("stream-target")
    assert lease.tick("stream-target") == "CENTRAL_SUSPECT"
    assert environment.dell.process_lease_valid("stream-target") is False  # type: ignore[attr-defined]


def test_central_command_is_rejected_during_local_fallback(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    receipt = environment.service().handle_command(command)  # type: ignore[attr-defined]
    assert receipt["reason_code"] == "LOCAL_AUTHORITY_ACTIVE"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def candidate(environment: object, *, reason: str = "confirmed_tcp_stall", **evidence: object) -> LocalRecoveryCandidate:
    return LocalRecoveryCandidate(
        "local-action-1",
        "stream-target",
        "restart_ffmpeg",
        reason,
        environment.target,  # type: ignore[attr-defined]
        evidence,
        isoformat_utc(utc_now()),
    )


def test_local_candidate_is_rejected_while_central_active(environment: object) -> None:
    result = environment.service().handle_local_candidate(candidate(environment))  # type: ignore[attr-defined]
    assert result == "LOCAL_AUTHORITY_NOT_ACTIVE"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_allowlisted_local_candidate_uses_same_fake_adapter(environment: object) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    assert (
        service.handle_local_candidate(candidate(environment, tcp_stall_confirmed=True, distinct_sample_count=3, stall_confirm_threshold=3))
        == "EFFECT_OBSERVED"
    )
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_local_candidate_adapter_false_success_is_kept_outcome_unknown(environment: object) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    environment.adapter.after_target = environment.target  # type: ignore[attr-defined]
    result = environment.service(environment.target, environment.target).handle_local_candidate(  # type: ignore[attr-defined]
        candidate(environment, tcp_stall_confirmed=True, distinct_sample_count=3, stall_confirm_threshold=3)
    )

    assert result == "OUTCOME_UNKNOWN"
    assert (
        environment.dell.read_one(  # type: ignore[attr-defined]
            "SELECT state FROM local_actions WHERE local_action_id='local-action-1'"
        )[0]
        == "OUTCOME_UNKNOWN"
    )
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_local_candidate_exact_replay_returns_existing_state_without_second_effect(environment: object) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    value = candidate(
        environment,
        tcp_stall_confirmed=True,
        distinct_sample_count=3,
        stall_confirm_threshold=3,
    )
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]

    assert service.handle_local_candidate(value) == "EFFECT_OBSERVED"
    assert service.handle_local_candidate(value) == "EFFECT_OBSERVED"
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_local_candidate_id_conflict_is_not_treated_as_exact_replay(environment: object) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    original = candidate(
        environment,
        tcp_stall_confirmed=True,
        distinct_sample_count=3,
        stall_confirm_threshold=3,
    )
    conflict = LocalRecoveryCandidate(
        original.local_action_id,
        original.target_id,
        original.action,
        original.reason_code,
        original.target_identity,
        {"tcp_stall_confirmed": True, "distinct_sample_count": 4, "stall_confirm_threshold": 3},
        original.observed_at,
    )
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]

    assert service.handle_local_candidate(original) == "EFFECT_OBSERVED"
    assert service.handle_local_candidate(conflict) == "LOCAL_ACTION_ID_CONFLICT"
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_local_candidate_numeric_type_change_is_not_an_exact_replay(environment: object) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    original = candidate(
        environment,
        tcp_stall_confirmed=True,
        distinct_sample_count=3,
        stall_confirm_threshold=3,
    )
    conflict = LocalRecoveryCandidate(
        original.local_action_id,
        original.target_id,
        original.action,
        original.reason_code,
        original.target_identity,
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3.0, "stall_confirm_threshold": 3},
        original.observed_at,
    )
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]

    assert service.handle_local_candidate(original) == "EFFECT_OBSERVED"
    assert service.handle_local_candidate(conflict) == "LOCAL_ACTION_ID_CONFLICT"
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


@pytest.mark.parametrize("action_id", ["", "-starts-with-separator", "x" * 241, "contains space"])
def test_local_candidate_id_is_bounded_and_canonical(environment: object, action_id: str) -> None:
    value = LocalRecoveryCandidate(
        action_id,
        "stream-target",
        "restart_ffmpeg",
        "confirmed_tcp_stall",
        environment.target,  # type: ignore[attr-defined]
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
        isoformat_utc(utc_now()),
    )

    assert environment.service().handle_local_candidate(value) == "LOCAL_ACTION_ID_INVALID"  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_local_candidate_sample_count_is_bounded(environment: object) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    value = candidate(
        environment,
        tcp_stall_confirmed=True,
        distinct_sample_count=1_000_001,
        stall_confirm_threshold=3,
    )

    assert environment.service().handle_local_candidate(value) == "INSUFFICIENT_LOCAL_EVIDENCE"  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_local_candidate_unknown_target_fails_closed_without_lookup_exception(environment: object) -> None:
    value = LocalRecoveryCandidate(
        "local-action-unknown-target",
        "other-target",
        "restart_ffmpeg",
        "confirmed_tcp_stall",
        environment.target,  # type: ignore[attr-defined]
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
        isoformat_utc(utc_now()),
    )

    assert environment.service().handle_local_candidate(value) == "LOCAL_TARGET_NOT_MANAGED"  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_local_candidate_non_mapping_evidence_fails_closed(environment: object) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    value = LocalRecoveryCandidate(
        "local-action-bad-evidence",
        "stream-target",
        "restart_ffmpeg",
        "confirmed_tcp_stall",
        environment.target,  # type: ignore[attr-defined]
        ["not", "a", "mapping"],  # type: ignore[arg-type]
        isoformat_utc(utc_now()),
    )

    assert environment.service().handle_local_candidate(value) == "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_local_candidate_unknown_evidence_field_fails_closed(environment: object) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    value = candidate(
        environment,
        tcp_stall_confirmed=True,
        distinct_sample_count=3,
        stall_confirm_threshold=3,
        unversioned_semantic=True,
    )

    assert environment.service().handle_local_candidate(value) == "LOCAL_EVIDENCE_SCHEMA_UNSUPPORTED"  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "evidence",
    [
        {},
        {"tcp_stall_confirmed": False, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
        {"tcp_stall_confirmed": 1, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
        {"tcp_stall_confirmed": True},
        {"tcp_stall_confirmed": True, "distinct_sample_count": True, "stall_confirm_threshold": 3},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 2, "stall_confirm_threshold": 3},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": True},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 2},
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 4},
    ],
)
def test_local_candidate_requires_strict_confirmed_stall_evidence(
    environment: object,
    evidence: dict[str, object],
) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    value = LocalRecoveryCandidate(
        "local-action-unconfirmed",
        "stream-target",
        "restart_ffmpeg",
        "confirmed_tcp_stall",
        environment.target,  # type: ignore[attr-defined]
        evidence,
        isoformat_utc(utc_now()),
    )

    assert environment.service().handle_local_candidate(value) == "INSUFFICIENT_LOCAL_EVIDENCE"  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (timedelta(minutes=-1), "LOCAL_EVIDENCE_STALE"),
        (timedelta(minutes=1), "LOCAL_EVIDENCE_FROM_FUTURE"),
    ],
)
def test_local_candidate_rejects_stale_or_future_evidence(
    environment: object,
    offset: timedelta,
    expected: str,
) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    value = LocalRecoveryCandidate(
        "local-action-time-invalid",
        "stream-target",
        "restart_ffmpeg",
        "confirmed_tcp_stall",
        environment.target,  # type: ignore[attr-defined]
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
        isoformat_utc(utc_now() + offset),
    )

    assert environment.service().handle_local_candidate(value) == expected  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


def test_local_path_shares_persisted_hard_cooldown(environment: object) -> None:
    command = environment.command()  # type: ignore[attr-defined]
    environment.service(environment.target, environment.target).handle_command(command)  # type: ignore[attr-defined]
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    assert (
        service.handle_local_candidate(candidate(environment, tcp_stall_confirmed=True, distinct_sample_count=3, stall_confirm_threshold=3))
        == "HARD_COOLDOWN_ACTIVE"
    )
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "evidence",
    [
        {"network_down": True},
        {"remote_warning_only": True},
        {"youtube_state_only": True},
        {"low_upload_pressure": True},
        {"ffmpeg_missing": True},
        {"maintenance": True},
        {"outcome_unknown": True},
    ],
)
def test_unsafe_local_evidence_is_rejected(environment: object, evidence: dict[str, bool]) -> None:
    environment.dell.set_authority_state(  # type: ignore[attr-defined]
        "stream-target", "LOCAL_FALLBACK", "TEST_LEASE_EXPIRED"
    )
    result = environment.service().handle_local_candidate(candidate(environment, **evidence))  # type: ignore[attr-defined]
    assert result == "INSUFFICIENT_LOCAL_EVIDENCE"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]
