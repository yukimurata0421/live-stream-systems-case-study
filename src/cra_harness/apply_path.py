from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_authority.authorizer import CraAuthorizer, RecoveryPolicy
from cra_authority.monitoring_evidence import MonitoringEvidenceContract, MonitoringEvidenceProjection
from cra_authority.storage import CentralStore
from cra_authority.verifier import CraVerifier
from cra_dell_recovery.canonical import KeyRing, SignedMessageCodec, Signer
from cra_dell_recovery.crash import CrashPoint, InjectedCrash, crash_at
from cra_dell_recovery.errors import CommandBlocked
from cra_dell_recovery.models import LocalRecoveryCandidate, RecoveryAuthorizationInput, TargetIdentity
from cra_dell_recovery.schema import SchemaRegistry
from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.execution import AgentService, FakeExecutionResult, FakePhysicalAdapter, FakeTargetObserver
from dell_recovery_agent.storage import DellStore


@dataclass
class _ApplyEnvironment:
    central: CentralStore
    dell: DellStore
    central_codec: SignedMessageCodec
    agent_codec: SignedMessageCodec
    monitoring_signer: Signer
    monitoring_contract: MonitoringEvidenceContract
    target: TargetIdentity

    def close(self) -> None:
        self.dell.close()
        self.central.close()


class _UnknownAdapter:
    def __init__(self) -> None:
        self.attempt_count = 0

    def restart_ffmpeg(self, expected_target: TargetIdentity) -> FakeExecutionResult:
        del expected_target
        self.attempt_count += 1
        raise RuntimeError("SYNTHETIC_RESULT_LOST_AFTER_BOUNDARY")


def _environment(project_root: Path, root: Path) -> _ApplyEnvironment:
    registry = SchemaRegistry(project_root / "contracts/cra_dell_recovery/v1")
    central_private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    agent_private = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    monitoring_private = Ed25519PrivateKey.from_private_bytes(bytes(range(91, 123)))
    central_codec = SignedMessageCodec(
        registry,
        Signer("cra-harness-key", central_private),
        KeyRing({"agent-harness-key": agent_private.public_key()}),
    )
    agent_codec = SignedMessageCodec(
        registry,
        Signer("agent-harness-key", agent_private),
        KeyRing({"cra-harness-key": central_private.public_key()}),
    )
    monitoring_signer = Signer("monitoring-harness-key", monitoring_private)
    monitoring_contract = MonitoringEvidenceContract(
        project_root / "contracts/monitoring_v4/evidence_projection.v1.schema.json",
        KeyRing({monitoring_signer.key_id: monitoring_private.public_key()}),
        allowed_sources={"arena-monitoring-v4": "monitoring-harness-release"},
    )
    central = CentralStore(root / "central.db", project_root / "migrations/central/001_initial.sql")
    central.bootstrap(
        target_id="stream-target",
        host_id="dell",
        agent_id="dell-agent",
        controller_instance_id="cra-controller",
        agent_installation_id="agent-installation-1",
    )
    dell = DellStore(root / "dell.db", project_root / "migrations/dell/001_initial.sql")
    dell.bootstrap(
        agent_id="dell-agent",
        installation_id="agent-installation-1",
        host_id="dell",
        host_boot_id="boot-a",
        target_id="stream-target",
    )
    dell.install_reconciliation(
        target_id="stream-target",
        reconciliation_id="initial-reconciliation",
        challenge_id="initial-challenge",
        nonce="initial-nonce-value-with-at-least-32-characters",
        new_epoch=1,
        session_id="session-1",
        controller_instance_id="cra-controller",
    )
    dell.connection.execute(
        """UPDATE authority_fences SET authority_state='CENTRAL_ACTIVE',action_ready=1,
           state_reason='HARNESS_AUTHORITY_READY'"""
    )
    dell.set_process_lease_valid("stream-target", True)
    target = TargetIdentity(
        host_id="dell",
        host_boot_id="boot-a",
        namespace="stream-v3",
        pod_uid="pod-a",
        container_name="stream-engine",
        container_id="containerd://a",
        ffmpeg_generation="generation-a",
        ffmpeg_pid=4100,
    )
    return _ApplyEnvironment(
        central=central,
        dell=dell,
        central_codec=central_codec,
        agent_codec=agent_codec,
        monitoring_signer=monitoring_signer,
        monitoring_contract=monitoring_contract,
        target=target,
    )


def _projection(
    environment: _ApplyEnvironment,
    *,
    sequence: int,
    target: TargetIdentity,
    healthy: bool = False,
    parity_clean: bool = True,
) -> MonitoringEvidenceProjection:
    now = utc_now()
    observed = now - timedelta(seconds=1)
    checks = {
        "tcp_stall": "FALSE" if healthy else "CONFIRMED",
        "network_down": "FALSE",
        "ffmpeg_present": "TRUE",
        "target_stable": "TRUE",
        "maintenance": "FALSE",
        "delivery_bad": "FALSE" if healthy else "TRUE",
    }
    if healthy:
        checks.update(
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
        "source_release_id": "monitoring-harness-release",
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
            "incident_id": "incident-apply-path",
            "source_episode_id": "episode-apply-path",
            "domain": "network_transport",
            "state": "CLEAR" if healthy else "CONFIRMED",
            "reason_codes": [] if healthy else ["confirmed_tcp_stall"],
        },
        "observed_target": target.to_dict(),
        "checks": {
            name: {
                "status": status,
                "observed_at": isoformat_utc(observed),
                "evidence_ref": f"harness/check/{sequence}/{name}",
            }
            for name, status in checks.items()
        },
        "measurements": [],
        "evidence_refs": [f"harness/cycle/{sequence}"],
        "key_id": environment.monitoring_signer.key_id,
    }
    return environment.monitoring_contract.decode(environment.monitoring_signer.sign(value), now=now)


def _command_from_projection(environment: _ApplyEnvironment, projection: MonitoringEvidenceProjection) -> dict[str, Any]:
    decision = CraAuthorizer(environment.central, RecoveryPolicy("shadow-policy-v1")).evaluate(projection)
    if decision.authorization_id is None:
        raise AssertionError(f"HARNESS_AUTHORIZATION_BLOCKED:{decision.blockers}")
    return environment.central.create_command(
        decision.authorization_id,
        environment.central_codec,
        command_id=f"command-{projection.sequence}",
    )


def _deliver(
    environment: _ApplyEnvironment,
    command: dict[str, Any],
    *,
    observer: FakeTargetObserver,
    adapter: FakePhysicalAdapter | _UnknownAdapter,
) -> tuple[dict[str, Any], dict[str, Any]]:
    service = AgentService(environment.dell, environment.agent_codec, observer, adapter)
    environment.central.begin_delivery(str(command["command_id"]))
    receipt = service.handle_command(command)
    environment.central.record_receipt(str(command["command_id"]), receipt, verifier=environment.central_codec)
    status = service.command_status(str(command["command_id"]))
    environment.central.record_status(status, verifier=environment.central_codec)
    return receipt, status


def _counts(environment: _ApplyEnvironment) -> dict[str, int]:
    names = (
        "recovery_authorizations",
        "commands",
        "delivery_attempts",
        "effect_scope_ledger",
        "cra_verifier_decisions",
        "effect_reconciliations",
    )
    return {
        name: int(environment.central.read_one(f"SELECT count(*) FROM {name}")[0])  # type: ignore[index]
        for name in names
    }


def _happy_path(project_root: Path, workspace: Path) -> dict[str, Any]:
    environment = _environment(project_root, workspace)
    successor = TargetIdentity.from_dict({**environment.target.to_dict(), "ffmpeg_generation": "generation-b", "ffmpeg_pid": 4200})
    adapter = FakePhysicalAdapter(after_target=successor)
    try:
        pre = _projection(environment, sequence=1, target=environment.target)
        command = _command_from_projection(environment, pre)
        _, status = _deliver(
            environment,
            command,
            observer=FakeTargetObserver(environment.target, environment.target),
            adapter=adapter,
        )
        post = _projection(environment, sequence=2, target=successor, healthy=True)
        environment.central.ingest_monitoring_projection(post.value)
        verdict = CraVerifier(environment.central).verify(
            pre=pre,
            post=post,
            execution_evidence={
                "command_id": command["command_id"],
                "local_action_id": None,
                "effect_scope_id": status["effect_scope_id"],
                "state": status["command_state"],
                "physical_attempt_count": status["attempt_count"],
                "before_target": status["before_target"],
                "after_target": status["after_target"],
            },
        )
        reconciliation = environment.central.record_effect_reconciliation(
            effect_scope_id_value=str(status["effect_scope_id"]),
            reconciled_state="EFFECT_OBSERVED",
            evidence={
                "source": "isolated-apply-path-harness",
                "verdict": verdict,
                "automatic_retry_count": 0,
                "raw_state_preserved": True,
            },
            reconciliation_id="harness-happy-reconciliation",
        )
        counts = _counts(environment)
        passed = (
            verdict == "RECOVERED"
            and reconciliation == "RECORDED"
            and adapter.attempt_count == 1
            and counts["effect_scope_ledger"] == 1
            and counts["effect_reconciliations"] == 1
        )
        return {
            "scenario": "happy_path_full_vertical_slice",
            "pass": passed,
            "verdict": verdict,
            "reconciliation": reconciliation,
            "synthetic_effect_boundary_count": adapter.attempt_count,
            "counts": counts,
        }
    finally:
        environment.close()


def _unknown_no_retry(project_root: Path, workspace: Path) -> dict[str, Any]:
    environment = _environment(project_root, workspace)
    adapter = _UnknownAdapter()
    try:
        pre = _projection(environment, sequence=1, target=environment.target)
        command = _command_from_projection(environment, pre)
        _, status = _deliver(
            environment,
            command,
            observer=FakeTargetObserver(environment.target, environment.target),
            adapter=adapter,
        )
        second = _projection(environment, sequence=2, target=environment.target)
        decision = CraAuthorizer(environment.central, RecoveryPolicy("shadow-policy-v1")).evaluate(second)
        counts = _counts(environment)
        passed = (
            status["command_state"] == "OUTCOME_UNKNOWN"
            and adapter.attempt_count == 1
            and decision.authorization_id is None
            and "CENTRAL_UNRESOLVED_COMMAND" in decision.blockers
            and counts["commands"] == 1
        )
        return {
            "scenario": "outcome_unknown_no_retry",
            "pass": passed,
            "command_state": status["command_state"],
            "second_decision": decision.decision,
            "second_blockers": list(decision.blockers),
            "synthetic_effect_boundary_count": adapter.attempt_count,
            "automatic_retry_count": 0,
            "counts": counts,
        }
    finally:
        environment.close()


def _parity_fail_closed(project_root: Path, workspace: Path) -> dict[str, Any]:
    environment = _environment(project_root, workspace)
    try:
        projection = _projection(environment, sequence=1, target=environment.target, parity_clean=False)
        decision = CraAuthorizer(environment.central, RecoveryPolicy("shadow-policy-v1")).evaluate(projection)
        counts = _counts(environment)
        return {
            "scenario": "stale_or_parity_failed_evidence",
            "pass": decision.authorization_id is None
            and "MONITORING_READINESS_PARITY_CLEAN_FALSE" in decision.blockers
            and counts["recovery_authorizations"] == 0
            and counts["commands"] == 0,
            "decision": decision.decision,
            "blockers": list(decision.blockers),
            "synthetic_effect_boundary_count": 0,
            "counts": counts,
        }
    finally:
        environment.close()


def _same_scope_different_identity(project_root: Path, workspace: Path) -> dict[str, Any]:
    environment = _environment(project_root, workspace)
    try:
        first = _command_from_projection(environment, _projection(environment, sequence=1, target=environment.target))
        environment.central.connection.execute("UPDATE commands SET status='VERIFIED' WHERE command_id=?", (first["command_id"],))
        environment.central.connection.execute("UPDATE incidents SET state='RECOVERED' WHERE incident_id='incident-apply-path'")
        now = utc_now()
        second_authorization = RecoveryAuthorizationInput(
            authorization_id="authorization-second-id",
            incident_id="incident-second-id",
            source_episode_id="episode-second-id",
            target_id="stream-target",
            action="restart_ffmpeg",
            reason_code="confirmed_tcp_stall",
            policy_revision="shadow-policy-v1",
            observation_revision="revision-second-id",
            expected_target=environment.target,
            blockers=(),
            authorized_at=isoformat_utc(now),
            expires_at=isoformat_utc(now + timedelta(seconds=30)),
        )
        environment.central.add_authorization(second_authorization)
        reason = ""
        try:
            environment.central.create_command(
                second_authorization.authorization_id,
                environment.central_codec,
                command_id="command-second-id",
            )
        except CommandBlocked as error:
            reason = str(error)
        counts = _counts(environment)
        return {
            "scenario": "different_ids_same_effect_scope",
            "pass": reason == "CENTRAL_LOGICAL_GENERATION_ALREADY_FENCED" and counts["effect_scope_ledger"] == 1,
            "reason": reason,
            "synthetic_effect_boundary_count": 0,
            "counts": counts,
        }
    finally:
        environment.close()


def _authority_race(project_root: Path, workspace: Path) -> dict[str, Any]:
    environment = _environment(project_root, workspace)
    adapter = FakePhysicalAdapter()
    try:
        command = _command_from_projection(environment, _projection(environment, sequence=1, target=environment.target))
        local = LocalRecoveryCandidate(
            local_action_id="local-race-candidate",
            target_id="stream-target",
            action="restart_ffmpeg",
            reason_code="confirmed_tcp_stall",
            target_identity=environment.target,
            evidence={"confirmed_tcp_stall": True},
            observed_at=isoformat_utc(utc_now()),
        )
        service = AgentService(
            environment.dell,
            environment.agent_codec,
            FakeTargetObserver(environment.target, environment.target),
            adapter,
        )
        local_result = service.handle_local_candidate(local)
        receipt = service.handle_command(command)
        return {
            "scenario": "cra_local_fallback_candidate_race",
            "pass": local_result == "LOCAL_AUTHORITY_NOT_ACTIVE" and receipt["disposition"] == "ACCEPTED" and adapter.attempt_count == 1,
            "local_result": local_result,
            "central_disposition": receipt["disposition"],
            "synthetic_effect_boundary_count": adapter.attempt_count,
            "effective_authority_count": 1,
        }
    finally:
        environment.close()


def _reconciling_no_effect(project_root: Path, workspace: Path) -> dict[str, Any]:
    environment = _environment(project_root, workspace)
    adapter = FakePhysicalAdapter()
    try:
        command = _command_from_projection(environment, _projection(environment, sequence=1, target=environment.target))
        environment.dell.connection.execute(
            "UPDATE authority_fences SET authority_state='RECONCILING',action_ready=0 WHERE target_id='stream-target'"
        )
        receipt = AgentService(
            environment.dell,
            environment.agent_codec,
            FakeTargetObserver(environment.target),
            adapter,
        ).handle_command(command)
        return {
            "scenario": "reconciling_has_no_effect",
            "pass": receipt["disposition"] == "REJECTED"
            and receipt["reason_code"] == "CENTRAL_AUTHORITY_NOT_ACTIVE"
            and adapter.attempt_count == 0,
            "reason": receipt["reason_code"],
            "synthetic_effect_boundary_count": adapter.attempt_count,
        }
    finally:
        environment.close()


def _pre_effect_crash_releases_only_uncommitted_reservation(project_root: Path, workspace: Path) -> dict[str, Any]:
    environment = _environment(project_root, workspace)
    try:
        projection = _projection(environment, sequence=1, target=environment.target)
        decision = CraAuthorizer(environment.central, RecoveryPolicy("shadow-policy-v1")).evaluate(projection)
        if decision.authorization_id is None:
            raise AssertionError("HARNESS_AUTHORIZATION_MISSING")
        crashed = False
        try:
            environment.central.create_command(
                decision.authorization_id,
                environment.central_codec,
                command_id="command-pre-effect-crash",
                crash=crash_at(CrashPoint.CENTRAL_BEFORE_COMMIT),
            )
        except InjectedCrash:
            crashed = True
        counts_after_crash = _counts(environment)
        retry = environment.central.create_command(
            decision.authorization_id,
            environment.central_codec,
            command_id="command-after-proven-pre-effect-crash",
        )
        counts_after_retry = _counts(environment)
        return {
            "scenario": "crash_before_effect_boundary_releases_only_uncommitted_fence",
            "pass": crashed
            and counts_after_crash["commands"] == 0
            and counts_after_crash["effect_scope_ledger"] == 0
            and retry["command_id"] == "command-after-proven-pre-effect-crash"
            and counts_after_retry["commands"] == 1
            and counts_after_retry["effect_scope_ledger"] == 1,
            "counts_after_crash": counts_after_crash,
            "counts_after_retry": counts_after_retry,
            "synthetic_effect_boundary_count": 0,
        }
    finally:
        environment.close()


def _post_effect_crash_never_retries(project_root: Path, workspace: Path) -> dict[str, Any]:
    environment = _environment(project_root, workspace)
    successor = TargetIdentity.from_dict({**environment.target.to_dict(), "ffmpeg_generation": "generation-post-crash", "ffmpeg_pid": 4300})
    adapter = FakePhysicalAdapter(after_target=successor)
    try:
        command = _command_from_projection(environment, _projection(environment, sequence=1, target=environment.target))
        service = AgentService(
            environment.dell,
            environment.agent_codec,
            FakeTargetObserver(environment.target, environment.target),
            adapter,
        )
        crashed = False
        try:
            service.handle_command(command, crash=crash_at(CrashPoint.DELL_AFTER_FAKE_EFFECT_BEFORE_STATUS))
        except InjectedCrash:
            crashed = True
        environment.dell.startup_recover("stream-target")
        status = service.command_status(str(command["command_id"]))
        duplicate = service.handle_command(command)
        return {
            "scenario": "crash_after_effect_boundary_never_retries",
            "pass": crashed
            and status["command_state"] == "OUTCOME_UNKNOWN"
            and status["attempt_count"] == 1
            and duplicate["disposition"] == "DUPLICATE"
            and adapter.attempt_count == 1,
            "command_state": status["command_state"],
            "automatic_retry_count": 0,
            "synthetic_effect_boundary_count": adapter.attempt_count,
        }
    finally:
        environment.close()


def run_apply_path_harness(project_root: Path, workspace: Path) -> dict[str, Any]:
    workspace.mkdir(parents=True, exist_ok=False)
    scenario_functions = (
        _happy_path,
        _unknown_no_retry,
        _parity_fail_closed,
        _same_scope_different_identity,
        _authority_race,
        _reconciling_no_effect,
        _pre_effect_crash_releases_only_uncommitted_reservation,
        _post_effect_crash_never_retries,
    )
    results = [function(project_root, workspace / f"scenario-{index:02d}") for index, function in enumerate(scenario_functions, 1)]
    payload = str(sorted((item["scenario"], item["pass"]) for item in results)).encode()
    return {
        "schema": "cra.apply_path_harness.v1",
        "classification": "PASS" if all(item["pass"] for item in results) else "SUT_FAILURE",
        "scenario_count": len(results),
        "passed_scenario_count": sum(bool(item["pass"]) for item in results),
        "synthetic_effect_boundary_count": sum(int(item["synthetic_effect_boundary_count"]) for item in results),
        "physical_effect_count": 0,
        "ffmpeg_signal_count": 0,
        "production_database_used": False,
        "production_network_used": False,
        "scenario_digest": hashlib.sha256(payload).hexdigest(),
        "scenarios": results,
    }
