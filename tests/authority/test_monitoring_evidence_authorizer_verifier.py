from __future__ import annotations

import json
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_authority.authorizer import CraAuthorizer, RecoveryPolicy
from cra_authority.monitoring_evidence import MonitoringEvidenceContract
from cra_authority.reconciliation import CentralReconciler
from cra_authority.verifier import CraVerifier
from cra_dell_recovery.canonical import KeyRing, Signer
from cra_dell_recovery.effect_scope import effect_scope_id
from cra_dell_recovery.errors import CommandBlocked, SignatureValidationError
from cra_dell_recovery.models import LocalRecoveryCandidate, TargetIdentity
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now
from dell_recovery_agent.execution import FakeTargetObserver
from dell_recovery_agent.reconciliation import DellReconciliationService

ROOT = Path(__file__).resolve().parents[2]


def _signer_and_contract() -> tuple[Signer, MonitoringEvidenceContract]:
    private = Ed25519PrivateKey.from_private_bytes(bytes(range(91, 123)))
    signer = Signer("monitoring-test-key", private)
    contract = MonitoringEvidenceContract(
        ROOT / "contracts/monitoring_v4/evidence_projection.v1.schema.json",
        KeyRing({"monitoring-test-key": private.public_key()}),
        allowed_sources={"arena-monitoring-v4": "monitoring-release-a"},
    )
    return signer, contract


def _projection(
    signer: Signer,
    target: TargetIdentity,
    *,
    sequence: int,
    healthy: bool = False,
    parity_clean: bool = True,
) -> dict[str, Any]:
    now = utc_now()
    observed = now - timedelta(seconds=1)
    required = {
        "tcp_stall": "FALSE" if healthy else "CONFIRMED",
        "network_down": "FALSE",
        "ffmpeg_present": "TRUE",
        "target_stable": "TRUE",
        "maintenance": "FALSE",
        "delivery_bad": "FALSE" if healthy else "TRUE",
    }
    if healthy:
        required.update(
            {
                "stream_engine_ready": "TRUE",
                "tcp_flow_healthy": "TRUE",
                "upload_progress_healthy": "TRUE",
                "startup_gate": "TRUE",
            }
        )
    value: dict[str, Any] = {
        "schema": "monitoring_v4.evidence_projection.v1",
        "projection_id": f"projection-{sequence}",
        "target_id": "stream-target",
        "source_instance_id": "arena-monitoring-v4",
        "source_release_id": "monitoring-release-a",
        "monitoring_cycle_id": f"cycle-{sequence}",
        "observation_revision": f"revision-{sequence}",
        "observation_sequence": sequence,
        "observed_at": isoformat_utc(observed),
        "issued_at": isoformat_utc(now),
        "expires_at": isoformat_utc(now + timedelta(seconds=45)),
        "readiness": {
            "input_fresh": True,
            "parity_clean": parity_clean,
            "projection_clean": True,
            "source_ready": True,
        },
        "incident": {
            "incident_id": "incident-monitoring-a",
            "source_episode_id": "episode-monitoring-a",
            "domain": "network_transport",
            "state": "CLEAR" if healthy else "CONFIRMED",
            "reason_codes": [] if healthy else ["confirmed_tcp_stall"],
        },
        "observed_target": target.to_dict(),
        "checks": {
            name: {
                "status": status,
                "observed_at": isoformat_utc(observed),
                "evidence_ref": f"arena/evidence/{sequence}/{name}",
            }
            for name, status in required.items()
        },
        "measurements": [],
        "evidence_refs": [f"arena/cycle/{sequence}"],
        "key_id": signer.key_id,
    }
    return signer.sign(value)


def test_monitoring_projection_contains_facts_and_cra_creates_authorization(environment: object) -> None:
    signer, contract = _signer_and_contract()
    projection = contract.decode(_projection(signer, environment.target, sequence=1))  # type: ignore[attr-defined]
    authorizer = CraAuthorizer(environment.central, RecoveryPolicy("shadow-policy-v1"))  # type: ignore[attr-defined]

    decision = authorizer.evaluate(projection)
    replay = authorizer.evaluate(projection)

    assert decision.decision == "AUTHORIZED"
    assert decision.candidate_reason_code == "confirmed_tcp_stall"
    assert decision.decision_reason_code == "confirmed_tcp_stall"
    assert decision.reason_binding == "CANDIDATE_AND_DECISION_BOUND_V1"
    assert len(decision.decision_digest) == 64
    assert decision.authorization_id is not None
    assert replay == decision
    assert "action" not in projection.value
    assert "verdict" not in projection.value
    assert environment.central.read_one("SELECT count(*) FROM monitoring_evidence_projections")[0] == 1  # type: ignore[attr-defined]
    assert environment.central.read_one("SELECT count(*) FROM recovery_authorizations")[0] == 1  # type: ignore[attr-defined]
    stored = environment.central.read_one(  # type: ignore[attr-defined]
        """SELECT reason_code,candidate_reason_code,decision_reason_code,decision_digest
           FROM cra_policy_decisions WHERE decision_id=?""",
        (decision.decision_id,),
    )
    assert stored is not None
    assert stored["reason_code"] == stored["decision_reason_code"] == "confirmed_tcp_stall"
    assert stored["candidate_reason_code"] == "confirmed_tcp_stall"
    assert stored["decision_digest"] == decision.decision_digest


def test_cra_blocks_authorization_when_monitoring_parity_is_false(environment: object) -> None:
    signer, contract = _signer_and_contract()
    projection = contract.decode(_projection(signer, environment.target, sequence=1, parity_clean=False))  # type: ignore[attr-defined]

    decision = CraAuthorizer(environment.central, RecoveryPolicy("shadow-policy-v1")).evaluate(projection)  # type: ignore[attr-defined]

    assert decision.decision == "BLOCKED"
    assert decision.candidate_reason_code == "confirmed_tcp_stall"
    assert decision.decision_reason_code == "MONITORING_READINESS_PARITY_CLEAN_FALSE"
    assert "MONITORING_READINESS_PARITY_CLEAN_FALSE" in decision.blockers
    assert environment.central.read_one("SELECT count(*) FROM recovery_authorizations")[0] == 0  # type: ignore[attr-defined]


def test_cra_blocks_active_incident_when_delivery_current_is_not_bad(environment: object) -> None:
    signer, contract = _signer_and_contract()
    value = _projection(signer, environment.target, sequence=1)  # type: ignore[attr-defined]
    value["checks"]["delivery_bad"]["status"] = "FALSE"
    projection = contract.decode(signer.sign(value))

    decision = CraAuthorizer(environment.central, RecoveryPolicy("shadow-policy-v1")).evaluate(projection)  # type: ignore[attr-defined]

    assert decision.decision == "BLOCKED"
    assert "CHECK_DELIVERY_BAD_NOT_TRUE" in decision.blockers
    assert environment.central.read_one("SELECT count(*) FROM recovery_authorizations")[0] == 0  # type: ignore[attr-defined]


def test_open_incident_is_normal_no_action_not_integrity_block(environment: object) -> None:
    signer, contract = _signer_and_contract()
    value = _projection(signer, environment.target, sequence=1)  # type: ignore[attr-defined]
    value["incident"]["state"] = "OPEN"
    projection = contract.decode(signer.sign(value))

    decision = CraAuthorizer(environment.central, RecoveryPolicy("shadow-policy-v1")).evaluate(projection)  # type: ignore[attr-defined]

    assert decision.decision == "NO_ACTION"
    assert decision.candidate_reason_code == "NO_CONFIRMED_RECOVERY_CANDIDATE"
    assert decision.decision_reason_code == "INCIDENT_NOT_CONFIRMED"
    assert "INCIDENT_NOT_CONFIRMED" in decision.blockers
    assert environment.central.read_one("SELECT count(*) FROM recovery_authorizations")[0] == 0  # type: ignore[attr-defined]


def test_authorization_lifetime_starts_at_cra_decision_not_projection_issue(environment: object) -> None:
    signer, contract = _signer_and_contract()
    current = utc_now()
    value = _projection(signer, environment.target, sequence=1)  # type: ignore[attr-defined]
    value["observed_at"] = isoformat_utc(current - timedelta(seconds=6))
    value["issued_at"] = isoformat_utc(current - timedelta(seconds=5))
    value["expires_at"] = isoformat_utc(current + timedelta(seconds=45))
    for check in value["checks"].values():
        check["observed_at"] = value["observed_at"]
    projection = contract.decode(signer.sign(value), now=current)

    decision = CraAuthorizer(
        environment.central,  # type: ignore[attr-defined]
        RecoveryPolicy("shadow-policy-v1", authorization_lifetime_seconds=30),
    ).evaluate(projection)
    authorization = environment.central.read_one(  # type: ignore[attr-defined]
        "SELECT authorized_at,expires_at FROM recovery_authorizations WHERE authorization_id=?",
        (decision.authorization_id,),
    )

    assert authorization is not None
    assert parse_utc(str(authorization["authorized_at"])) > parse_utc(str(value["issued_at"]))
    lifetime = parse_utc(str(authorization["expires_at"])) - parse_utc(str(authorization["authorized_at"]))
    assert 29 <= lifetime.total_seconds() <= 30


def test_projection_tamper_and_sequence_regression_fail_closed(environment: object) -> None:
    signer, contract = _signer_and_contract()
    signed = _projection(signer, environment.target, sequence=2)  # type: ignore[attr-defined]
    tampered = {**signed, "source_release_id": "different-release"}
    with pytest.raises(SignatureValidationError):
        contract.decode(tampered)
    projection = contract.decode(signed)
    environment.central.ingest_monitoring_projection(projection.value)  # type: ignore[attr-defined]
    older = contract.decode(_projection(signer, environment.target, sequence=1))  # type: ignore[attr-defined]
    with pytest.raises(Exception, match="SEQUENCE_REGRESSION"):
        environment.central.ingest_monitoring_projection(older.value)  # type: ignore[attr-defined]


def test_projection_rejects_old_observation_even_when_envelope_ttl_is_current(environment: object) -> None:
    signer, _ = _signer_and_contract()
    current = utc_now()
    value = _projection(signer, environment.target, sequence=1)  # type: ignore[attr-defined]
    value["observed_at"] = isoformat_utc(current - timedelta(seconds=20))
    value["issued_at"] = isoformat_utc(current)
    value["expires_at"] = isoformat_utc(current + timedelta(seconds=30))
    for check in value["checks"].values():
        check["observed_at"] = value["observed_at"]
    contract = MonitoringEvidenceContract(
        ROOT / "contracts/monitoring_v4/evidence_projection.v1.schema.json",
        KeyRing({signer.key_id: signer.private_key.public_key()}),
        allowed_sources={"arena-monitoring-v4": "monitoring-release-a"},
        maximum_observation_age_seconds=5,
        maximum_check_age_seconds=30,
    )

    with pytest.raises(ValueError, match="MONITORING_EVIDENCE_OBSERVATION_TOO_OLD"):
        contract.decode(signer.sign(value), now=current)


def test_projection_rejects_stale_individual_check(environment: object) -> None:
    signer, _ = _signer_and_contract()
    current = utc_now()
    value = _projection(signer, environment.target, sequence=1)  # type: ignore[attr-defined]
    value["observed_at"] = isoformat_utc(current - timedelta(seconds=1))
    value["issued_at"] = isoformat_utc(current)
    value["expires_at"] = isoformat_utc(current + timedelta(seconds=30))
    # Bind every check to the rewritten projection time before aging only tcp_stall.
    # The helper reads a later clock and may otherwise create a different invalidity.
    for check in value["checks"].values():
        check["observed_at"] = value["observed_at"]
    value["checks"]["tcp_stall"]["observed_at"] = isoformat_utc(current - timedelta(seconds=20))
    contract = MonitoringEvidenceContract(
        ROOT / "contracts/monitoring_v4/evidence_projection.v1.schema.json",
        KeyRing({signer.key_id: signer.private_key.public_key()}),
        allowed_sources={"arena-monitoring-v4": "monitoring-release-a"},
        maximum_observation_age_seconds=30,
        maximum_check_age_seconds=5,
    )

    with pytest.raises(ValueError, match="MONITORING_CHECK_TOO_OLD:tcp_stall"):
        contract.decode(signer.sign(value), now=current)


def test_projection_allows_bounded_clock_skew_but_rejects_larger_future_issue(environment: object) -> None:
    signer, _ = _signer_and_contract()
    current = utc_now()
    value = _projection(signer, environment.target, sequence=1)  # type: ignore[attr-defined]
    value["observed_at"] = isoformat_utc(current + timedelta(seconds=1))
    value["issued_at"] = isoformat_utc(current + timedelta(seconds=1))
    value["expires_at"] = isoformat_utc(current + timedelta(seconds=30))
    for check in value["checks"].values():
        check["observed_at"] = value["observed_at"]
    contract = MonitoringEvidenceContract(
        ROOT / "contracts/monitoring_v4/evidence_projection.v1.schema.json",
        KeyRing({signer.key_id: signer.private_key.public_key()}),
        allowed_sources={"arena-monitoring-v4": "monitoring-release-a"},
        maximum_future_skew_seconds=2,
    )
    assert contract.decode(signer.sign(value), now=current).projection_id == "projection-1"
    value["observed_at"] = isoformat_utc(current + timedelta(seconds=3))
    value["issued_at"] = isoformat_utc(current + timedelta(seconds=3))
    for check in value["checks"].values():
        check["observed_at"] = value["observed_at"]

    with pytest.raises(ValueError, match="MONITORING_EVIDENCE_ISSUED_IN_FUTURE"):
        contract.decode(signer.sign(value), now=current)


def test_cra_verifier_owns_final_verdict_and_has_no_command_capability(environment: object) -> None:
    signer, contract = _signer_and_contract()
    pre = contract.decode(_projection(signer, environment.target, sequence=1))  # type: ignore[attr-defined]
    authorizer = CraAuthorizer(environment.central, RecoveryPolicy("shadow-policy-v1"))  # type: ignore[attr-defined]
    authorization = authorizer.evaluate(pre)
    command = environment.central.create_command(authorization.authorization_id, environment.central_codec, command_id="verify-command")  # type: ignore[attr-defined]
    after = TargetIdentity(
        **{
            **environment.target.to_dict(),  # type: ignore[attr-defined]
            "ffmpeg_generation": "run-b:0:4200",
            "ffmpeg_pid": 4200,
        }
    )
    environment.adapter.after_target = after  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    environment.central.deliver_command(  # type: ignore[attr-defined]
        command["command_id"],
        service.handle_command,
        verifier=environment.central_codec,  # type: ignore[attr-defined]
    )
    signed_status = service.command_status(command["command_id"])
    environment.central.record_status(signed_status, verifier=environment.central_codec)  # type: ignore[attr-defined]
    post = contract.decode(_projection(signer, after, sequence=2, healthy=True))
    environment.central.ingest_monitoring_projection(post.value)  # type: ignore[attr-defined]
    verifier = CraVerifier(environment.central)  # type: ignore[attr-defined]

    verdict = verifier.verify(
        pre=pre,
        post=post,
        execution_evidence={
            "command_id": command["command_id"],
            "local_action_id": None,
            "effect_scope_id": effect_scope_id("restart_ffmpeg", environment.target),  # type: ignore[attr-defined]
            "state": "EFFECT_OBSERVED",
            "physical_attempt_count": 1,
            "before_target": environment.target.to_dict(),  # type: ignore[attr-defined]
            "after_target": after.to_dict(),
        },
    )

    assert verdict == "RECOVERED"
    assert environment.central.read_one("SELECT status FROM commands WHERE command_id='verify-command'")[0] == "VERIFIED"  # type: ignore[attr-defined]
    incident = environment.central.read_one(  # type: ignore[attr-defined]
        "SELECT state,observation_revision,closed_at FROM incidents WHERE incident_id='incident-monitoring-a'"
    )
    assert incident is not None
    assert incident["state"] == "RECOVERED"
    assert incident["observation_revision"] == "revision-2"
    assert incident["closed_at"] is not None
    assert not hasattr(verifier, "create_command")
    assert not hasattr(verifier, "codec")
    saved = environment.central.read_one(  # type: ignore[attr-defined]
        "SELECT * FROM cra_verifier_decisions WHERE command_id='verify-command'"
    )
    assert saved is not None
    with pytest.raises(CommandBlocked, match="CRA_VERIFIER_EFFECT_SCOPE_ALREADY_FINALIZED"):
        environment.central.record_verifier_decision(  # type: ignore[attr-defined]
            {
                "verifier_decision_id": "second-final-verdict-for-same-scope",
                "command_id": saved["command_id"],
                "local_action_id": saved["local_action_id"],
                "effect_scope_id": saved["effect_scope_id"],
                "pre_projection_id": saved["pre_projection_id"],
                "post_projection_id": saved["post_projection_id"],
                "verdict": saved["verdict"],
                "reason_codes": json.loads(str(saved["reason_codes_json"])),
                "execution_evidence": json.loads(str(saved["execution_evidence_json"])),
                "decided_at": saved["decided_at"],
            }
        )


def test_verifier_rejects_after_target_not_bound_to_signed_agent_status(environment: object) -> None:
    signer, contract = _signer_and_contract()
    before = environment.target  # type: ignore[attr-defined]
    pre = contract.decode(_projection(signer, before, sequence=1))
    authorization = CraAuthorizer(environment.central, RecoveryPolicy("shadow-policy-v1")).evaluate(pre)  # type: ignore[attr-defined]
    command = environment.central.create_command(  # type: ignore[attr-defined]
        authorization.authorization_id,
        environment.central_codec,  # type: ignore[attr-defined]
        command_id="verify-signed-target-binding",
    )
    actual_after = TargetIdentity.from_dict(
        {
            **before.to_dict(),
            "ffmpeg_generation": "run-actual:0:4200",
            "ffmpeg_pid": 4200,
        }
    )
    environment.adapter.after_target = actual_after  # type: ignore[attr-defined]
    service = environment.service(before, before)  # type: ignore[attr-defined]
    environment.central.deliver_command(  # type: ignore[attr-defined]
        command["command_id"],
        service.handle_command,
        verifier=environment.central_codec,  # type: ignore[attr-defined]
    )
    environment.central.record_status(  # type: ignore[attr-defined]
        service.command_status(command["command_id"]),
        verifier=environment.central_codec,  # type: ignore[attr-defined]
    )
    forged_after = TargetIdentity.from_dict(
        {
            **before.to_dict(),
            "ffmpeg_generation": "run-forged:0:4300",
            "ffmpeg_pid": 4300,
        }
    )
    post = contract.decode(_projection(signer, forged_after, sequence=2, healthy=True))
    environment.central.ingest_monitoring_projection(post.value)  # type: ignore[attr-defined]

    with pytest.raises(CommandBlocked, match="CRA_VERIFIER_AGENT_STATUS_AFTER_TARGET_MISMATCH"):
        CraVerifier(environment.central).verify(  # type: ignore[attr-defined]
            pre=pre,
            post=post,
            execution_evidence={
                "command_id": command["command_id"],
                "local_action_id": None,
                "effect_scope_id": effect_scope_id("restart_ffmpeg", before),
                "state": "EFFECT_OBSERVED",
                "physical_attempt_count": 1,
                "before_target": before.to_dict(),
                "after_target": forged_after.to_dict(),
            },
        )
    assert environment.central.read_one("SELECT count(*) FROM cra_verifier_decisions")[0] == 0  # type: ignore[attr-defined]


def test_verifier_accepts_imported_local_journal_bound_to_projection_lineage(environment: object) -> None:
    signer, contract = _signer_and_contract()
    before = environment.target  # type: ignore[attr-defined]
    after = TargetIdentity.from_dict(
        {
            **before.to_dict(),
            "ffmpeg_generation": "run-local:0:4200",
            "ffmpeg_pid": 4200,
        }
    )
    pre = contract.decode(_projection(signer, before, sequence=1))
    environment.central.ingest_monitoring_projection(pre.value)  # type: ignore[attr-defined]
    environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "TEST_LAN_DOWN")  # type: ignore[attr-defined]
    environment.adapter.after_target = after  # type: ignore[attr-defined]
    candidate = LocalRecoveryCandidate(
        local_action_id="local-verifier-binding",
        target_id="stream-target",
        action="restart_ffmpeg",
        reason_code="confirmed_tcp_stall",
        target_identity=before,
        evidence={"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
        observed_at=isoformat_utc(utc_now()),
    )
    assert environment.service(before, before).handle_local_candidate(candidate) == "EFFECT_OBSERVED"  # type: ignore[attr-defined]
    peer = DellReconciliationService(  # type: ignore[attr-defined]
        environment.dell,
        environment.agent_codec,
        FakeTargetObserver(before),
    )
    CentralReconciler(environment.central, environment.central_codec, peer).reconcile("stream-target")  # type: ignore[attr-defined]
    post = contract.decode(_projection(signer, after, sequence=2, healthy=True))
    environment.central.ingest_monitoring_projection(post.value)  # type: ignore[attr-defined]
    terminal = environment.central.read_one(  # type: ignore[attr-defined]
        """SELECT local_action_id,effect_scope_id,record_state,payload_json
           FROM dell_journal_records ORDER BY journal_sequence DESC LIMIT 1"""
    )
    assert terminal is not None
    terminal_payload = dict(json.loads(str(terminal["payload_json"])))

    verdict = CraVerifier(environment.central).verify(  # type: ignore[attr-defined]
        pre=pre,
        post=post,
        execution_evidence={
            "command_id": None,
            "local_action_id": str(terminal["local_action_id"]),
            "effect_scope_id": str(terminal["effect_scope_id"]),
            "state": str(terminal["record_state"]),
            "physical_attempt_count": int(terminal_payload["physical_attempt_count"]),
            "before_target": terminal_payload["before_target"],
            "after_target": terminal_payload["after_target"],
        },
    )

    assert verdict == "RECOVERED"
    assert environment.central.read_one("SELECT count(*) FROM cra_verifier_decisions")[0] == 1  # type: ignore[attr-defined]
    incident = environment.central.read_one(  # type: ignore[attr-defined]
        "SELECT state,observation_revision,closed_at FROM incidents WHERE incident_id='incident-monitoring-a'"
    )
    assert incident is not None
    assert incident["state"] == "RECOVERED"
    assert incident["observation_revision"] == "revision-2"
    assert incident["closed_at"] is not None


class _CapturingVerifierStore:
    def __init__(self) -> None:
        self.value: dict[str, Any] | None = None

    def record_verifier_decision(self, value: dict[str, Any]) -> str:
        self.value = value
        return str(value["verdict"])


def _execution(before: TargetIdentity, after: TargetIdentity) -> dict[str, Any]:
    return {
        "command_id": "captured-command",
        "local_action_id": None,
        "effect_scope_id": effect_scope_id("restart_ffmpeg", before),
        "state": "EFFECT_OBSERVED",
        "physical_attempt_count": 1,
        "before_target": before.to_dict(),
        "after_target": after.to_dict(),
    }


def test_verifier_separates_explicit_failure_from_missing_evidence(environment: object) -> None:
    signer, contract = _signer_and_contract()
    before = environment.target  # type: ignore[attr-defined]
    after = TargetIdentity(
        **{
            **before.to_dict(),
            "ffmpeg_generation": "run-b:0:4200",
            "ffmpeg_pid": 4200,
        }
    )
    pre = contract.decode(_projection(signer, before, sequence=1))
    unhealthy_raw = deepcopy(_projection(signer, after, sequence=2, healthy=True))
    unhealthy_raw["checks"]["tcp_flow_healthy"]["status"] = "FALSE"
    unhealthy = contract.decode(signer.sign(unhealthy_raw))
    failed_store = _CapturingVerifierStore()

    assert CraVerifier(failed_store).verify(pre=pre, post=unhealthy, execution_evidence=_execution(before, after)) == "FAILED"
    assert failed_store.value is not None
    assert "POST_CHECK_TCP_FLOW_HEALTHY_NOT_TRUE" in failed_store.value["reason_codes"]

    missing_raw = deepcopy(_projection(signer, after, sequence=3, healthy=True))
    del missing_raw["checks"]["startup_gate"]
    missing = contract.decode(signer.sign(missing_raw))
    unknown_store = _CapturingVerifierStore()
    assert CraVerifier(unknown_store).verify(pre=pre, post=missing, execution_evidence=_execution(before, after)) == "UNKNOWN"
    assert unknown_store.value is not None
    assert "POST_CHECK_STARTUP_GATE_MISSING" in unknown_store.value["reason_codes"]


def test_verifier_treats_fresh_bound_unchanged_generation_as_failed(environment: object) -> None:
    signer, contract = _signer_and_contract()
    target = environment.target  # type: ignore[attr-defined]
    pre = contract.decode(_projection(signer, target, sequence=1))
    post = contract.decode(_projection(signer, target, sequence=2, healthy=True))
    store = _CapturingVerifierStore()

    assert CraVerifier(store).verify(pre=pre, post=post, execution_evidence=_execution(target, target)) == "FAILED"
    assert store.value is not None
    assert "FFMPEG_GENERATION_NOT_CHANGED" in store.value["reason_codes"]


def test_verifier_requires_ready_clear_same_incident_post_projection(environment: object) -> None:
    signer, contract = _signer_and_contract()
    before = environment.target  # type: ignore[attr-defined]
    after = TargetIdentity.from_dict(
        {
            **before.to_dict(),
            "ffmpeg_generation": "run-ready:0:4200",
            "ffmpeg_pid": 4200,
        }
    )
    pre = contract.decode(_projection(signer, before, sequence=1))

    not_ready = contract.decode(_projection(signer, after, sequence=2, healthy=True, parity_clean=False))
    not_ready_store = _CapturingVerifierStore()
    assert CraVerifier(not_ready_store).verify(pre=pre, post=not_ready, execution_evidence=_execution(before, after)) == "UNKNOWN"
    assert not_ready_store.value is not None
    assert "POST_READINESS_PARITY_CLEAN_FALSE" in not_ready_store.value["reason_codes"]

    still_confirmed_raw = deepcopy(_projection(signer, after, sequence=3, healthy=True))
    still_confirmed_raw["incident"] = {
        **still_confirmed_raw["incident"],
        "state": "CONFIRMED",
        "reason_codes": ["confirmed_tcp_stall"],
    }
    still_confirmed = contract.decode(signer.sign(still_confirmed_raw))
    confirmed_store = _CapturingVerifierStore()
    assert CraVerifier(confirmed_store).verify(pre=pre, post=still_confirmed, execution_evidence=_execution(before, after)) == "FAILED"
    assert confirmed_store.value is not None
    assert "POST_INCIDENT_STILL_CONFIRMED" in confirmed_store.value["reason_codes"]

    different_incident_raw = deepcopy(_projection(signer, after, sequence=4, healthy=True))
    different_incident_raw["incident"] = {
        **different_incident_raw["incident"],
        "incident_id": "different-incident",
        "source_episode_id": "different-episode",
    }
    different_incident = contract.decode(signer.sign(different_incident_raw))
    different_store = _CapturingVerifierStore()
    assert CraVerifier(different_store).verify(pre=pre, post=different_incident, execution_evidence=_execution(before, after)) == "UNKNOWN"
    assert different_store.value is not None
    assert "MONITORING_INCIDENT_CHANGED" in different_store.value["reason_codes"]
    assert "MONITORING_SOURCE_EPISODE_CHANGED" in different_store.value["reason_codes"]


def test_verifier_store_rejects_execution_state_not_present_in_central_truth(environment: object) -> None:
    signer, contract = _signer_and_contract()
    before = environment.target  # type: ignore[attr-defined]
    pre = contract.decode(_projection(signer, before, sequence=1))
    authorization = CraAuthorizer(environment.central, RecoveryPolicy("shadow-policy-v1")).evaluate(pre)  # type: ignore[attr-defined]
    command = environment.central.create_command(  # type: ignore[attr-defined]
        authorization.authorization_id,
        environment.central_codec,  # type: ignore[attr-defined]
        command_id="unexecuted-command",
    )
    after = TargetIdentity(
        **{
            **before.to_dict(),
            "ffmpeg_generation": "run-b:0:4200",
            "ffmpeg_pid": 4200,
        }
    )
    post = contract.decode(_projection(signer, after, sequence=2, healthy=True))
    environment.central.ingest_monitoring_projection(post.value)  # type: ignore[attr-defined]

    with pytest.raises(CommandBlocked, match="EXECUTION_STATE_NOT_CENTRAL_TRUTH"):
        CraVerifier(environment.central).verify(  # type: ignore[attr-defined]
            pre=pre,
            post=post,
            execution_evidence={
                **_execution(before, after),
                "command_id": command["command_id"],
            },
        )
    assert environment.central.read_one("SELECT status FROM commands WHERE command_id='unexecuted-command'")[0] == "OUTBOX_PENDING"  # type: ignore[attr-defined]
