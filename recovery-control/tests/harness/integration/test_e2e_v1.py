from __future__ import annotations

from pathlib import Path

import pytest

from cra_dell_recovery.errors import CommandBlocked
from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.recovery_verification import CHECK_NAMES, payload_hash
from cra_harness.controls.environment import HarnessEnvironment
from cra_harness.runner.e2e import E2ERunner
from cra_harness.verification_contract.fake_monitoring import FakeMonitoringObserver

ROOT = Path(__file__).resolve().parents[3]


def test_e2e_lifecycle_and_fault_combinations(tmp_path: Path) -> None:
    outcomes = E2ERunner(ROOT, tmp_path).run_all()
    assert len(outcomes) == 6
    assert {item.classification for item in outcomes} == {"PASS"}
    assert all(item.physical_attempt_count <= 1 for item in outcomes)
    assert all(item.automatic_second_restart_count == 0 for item in outcomes)
    assert all(item.production_escalation_count == 0 for item in outcomes)


def test_effect_observed_is_not_verified_without_recovery_verification(tmp_path: Path) -> None:
    outcomes = {item.scenario_id: item for item in E2ERunner(ROOT, tmp_path).run_all()}
    assert outcomes["E2E-04"].command_state == "OUTCOME_UNKNOWN"
    assert outcomes["E2E-04"].verification_verdict == "NOT_EMITTED"


def test_unknown_verification_never_triggers_a_second_restart_or_workload_escalation(tmp_path: Path) -> None:
    outcomes = E2ERunner(ROOT, tmp_path).run_all()
    unknown = [item for item in outcomes if item.verification_verdict == "UNKNOWN"]
    assert len(unknown) == 2
    assert all(item.automatic_second_restart_count == item.production_escalation_count == 0 for item in unknown)


def test_unsafe_post_effect_target_relation_suppresses_verification(tmp_path: Path) -> None:
    outcomes = {item.scenario_id: item for item in E2ERunner(ROOT, tmp_path).run_all()}
    mutation = outcomes["E2E-06"]
    assert mutation.command_state == "OUTCOME_UNKNOWN"
    assert mutation.verification_verdict == "NOT_EMITTED"
    assert mutation.ingest_result is None
    assert "RecoveryVerification suppressed" in mutation.trace


def _effect_observed_environment(tmp_path: Path) -> tuple[HarnessEnvironment, dict[str, object], dict[str, object]]:
    environment = HarnessEnvironment.create(tmp_path, ROOT)
    command = environment.command("verification-ingest")
    after = TargetIdentity.from_dict({**environment.target.to_dict(), "ffmpeg_generation": "run-ingest:1:4300", "ffmpeg_pid": 4300})
    environment.adapter.after_target = after
    service = environment.service()
    environment.central.deliver_command(
        str(command["command_id"]),
        service.handle_command,
        verifier=environment.central_codec,
    )
    status = service.command_status(str(command["command_id"]))
    environment.central.record_status(status, verifier=environment.central_codec)
    checks = {
        name: {
            "result": "PASS",
            "evidence_ref": f"evidence-ingest-{name}",
            "observed_at": "2026-08-23T04:30:10.000Z",
            "evidence_role": "supporting" if name == "external_public_signal" else "current_authoritative",
        }
        for name in CHECK_NAMES
    }
    verification = FakeMonitoringObserver().verify(
        command_id=str(command["command_id"]),
        incident_id=str(command["incident_id"]),
        target_id="stream-target",
        monitoring_cycle_id="cycle-ingest",
        command_state="EFFECT_OBSERVED",
        before_target=environment.target,
        observed_target=after,
        checks=checks,
        observed_at="2026-08-23T04:30:10.000Z",
        evidence_fresh_until="2026-08-23T04:31:10.000Z",
        evaluated_at="2026-08-23T04:30:20.000Z",
        observation_revision="observation-ingest",
    )
    return environment, command, verification


def test_recovery_verification_duplicate_is_idempotent_and_payload_conflict_is_rejected(tmp_path: Path) -> None:
    environment, _, verification = _effect_observed_environment(tmp_path)
    try:
        assert environment.central.record_recovery_verification(verification) == "ACCEPTED"
        assert environment.central.record_recovery_verification(verification) == "DUPLICATE"
        conflict = {**verification, "reason_codes": ["CONFLICTING_REPLAY"]}
        conflict["payload_sha256"] = payload_hash(conflict)
        with pytest.raises(CommandBlocked, match="VERIFICATION_PAYLOAD_CONFLICT"):
            environment.central.record_recovery_verification(conflict)
    finally:
        environment.close()


def test_old_command_verification_is_rejected_before_projection(tmp_path: Path) -> None:
    environment, _, verification = _effect_observed_environment(tmp_path)
    try:
        old = {**verification, "command_id": "command-not-current"}
        old["idempotency_key"] = ":".join((old["command_id"], old["monitoring_cycle_id"], old["observation_revision"]))
        old["payload_sha256"] = payload_hash(old)
        with pytest.raises(CommandBlocked, match="VERIFICATION_COMMAND_NOT_FOUND"):
            environment.central.record_recovery_verification(old)
        assert environment.central.connection.execute("SELECT count(*) FROM recovery_verifications").fetchone()[0] == 0
    finally:
        environment.close()
