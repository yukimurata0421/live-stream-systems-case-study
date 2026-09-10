from __future__ import annotations

import json
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cra_authority.heartbeat import AuthorityHeartbeatPublisher
from cra_dell_recovery.crash import CrashPoint, InjectedCrash, crash_at
from cra_dell_recovery.models import MonitoringReadiness, TargetIdentity
from cra_dell_recovery.recovery_verification import CHECK_NAMES, RecoveryVerificationContract
from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_harness.controls.environment import HarnessEnvironment
from cra_harness.oracles.recovery_verification import IndependentRecoveryVerificationOracle
from cra_harness.runner.compound import FakeMonotonic
from cra_harness.verification_contract.fake_monitoring import FakeMonitoringObserver
from dell_recovery_agent.authority import AgentAuthorityLease


@dataclass(frozen=True)
class E2EOutcome:
    scenario_id: str
    classification: str
    mode: str
    trace: tuple[str, ...]
    command_state: str
    verification_verdict: str
    physical_attempt_count: int
    automatic_second_restart_count: int
    production_escalation_count: int
    ingest_result: str | None
    invariant_violations: tuple[str, ...]
    timings_ms: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "profile": "end_to_end",
            "classification": self.classification,
            "mode": self.mode,
            "trace": list(self.trace),
            "command_state": self.command_state,
            "verification_verdict": self.verification_verdict,
            "physical_attempt_count": self.physical_attempt_count,
            "automatic_second_restart_count": self.automatic_second_restart_count,
            "production_escalation_count": self.production_escalation_count,
            "ingest_result": self.ingest_result,
            "invariant_violations": list(self.invariant_violations),
            "timings_ms": self.timings_ms,
        }


def _elapsed_ms(started: int) -> float:
    return round((time.perf_counter_ns() - started) / 1_000_000, 6)


def _checks(mode: str, observed_at: str) -> dict[str, dict[str, str]]:
    values = {name: "PASS" for name in CHECK_NAMES}
    if mode == "monitoring_stale":
        values = {name: "UNKNOWN" for name in CHECK_NAMES}
    roles = {name: "current_authoritative" for name in CHECK_NAMES}
    roles["external_public_signal"] = "supporting"
    roles["youtube_state"] = "current_correlated"
    return {
        name: {
            "result": values[name],
            "evidence_ref": f"evidence-e2e-{mode}-{name}",
            "observed_at": observed_at,
            "evidence_role": roles[name],
        }
        for name in CHECK_NAMES
    }


class E2ERunner:
    def __init__(self, project_root: Path, workspace: Path) -> None:
        self.project_root = project_root
        self.workspace = workspace
        registry = json.loads((project_root / "harness/scenarios/e2e_v1.json").read_text(encoding="utf-8"))
        self.scenarios: tuple[dict[str, Any], ...] = tuple(dict(item) for item in registry["scenarios"])
        self.contract = RecoveryVerificationContract(project_root / "contracts/monitoring_v4/recovery_verification.v1.schema.json")

    def run_all(self) -> list[E2EOutcome]:
        return [self.run(item) for item in self.scenarios]

    def run(self, spec: dict[str, Any]) -> E2EOutcome:
        scenario_id = str(spec["scenario_id"])
        mode = str(spec["mode"])
        environment = HarnessEnvironment.create(self.workspace / scenario_id, self.project_root)
        trace: list[str] = ["Observation", "Authorization"]
        timings: dict[str, float] = {}
        verification: dict[str, Any] | None = None
        ingest_result: str | None = None
        violations: list[str] = []
        try:
            publisher = AuthorityHeartbeatPublisher(environment.central, environment.central_codec, lease_ttl_seconds=15)
            lease = AgentAuthorityLease(environment.dell, environment.agent_codec, monotonic=FakeMonotonic())
            heartbeat = publisher.build(
                "stream-target",
                MonitoringReadiness(True, True, isoformat_utc(utc_now()), "e2e"),
            )
            if heartbeat is None:
                raise RuntimeError("E2E heartbeat not generated")
            started = time.perf_counter_ns()
            lease.receive(heartbeat)
            timings["heartbeat_processing"] = _elapsed_ms(started)

            authorization = environment.authorization(scenario_id.lower())
            environment.central.add_authorization(authorization)
            started = time.perf_counter_ns()
            command = environment.central.create_command(
                authorization.authorization_id,
                environment.central_codec,
                command_id=f"command-{scenario_id.lower()}",
            )
            timings["command_db_transaction"] = _elapsed_ms(started)
            trace.append("Command")
            changed = TargetIdentity.from_dict({**environment.target.to_dict(), "ffmpeg_generation": "run-e2e:1:4200", "ffmpeg_pid": 4200})
            if mode == "target_mutation":
                changed = TargetIdentity.from_dict(
                    {
                        **changed.to_dict(),
                        "pod_uid": "pod-unexpected",
                        "container_id": "container-unexpected",
                    }
                )
            environment.adapter.after_target = changed
            service = environment.service()
            dell_transaction_started: int | None = None
            dell_transaction_durations: list[float] = []

            def trace_dell_transaction(statement: str) -> None:
                nonlocal dell_transaction_started
                normalized = statement.strip().upper()
                if normalized.startswith("BEGIN IMMEDIATE"):
                    dell_transaction_started = time.perf_counter_ns()
                elif normalized == "COMMIT" and dell_transaction_started is not None:
                    dell_transaction_durations.append(_elapsed_ms(dell_transaction_started))
                    dell_transaction_started = None

            environment.dell.connection.set_trace_callback(trace_dell_transaction)
            if mode == "agent_crash":
                started = time.perf_counter_ns()
                with suppress(InjectedCrash):
                    environment.central.deliver_command(
                        str(command["command_id"]),
                        lambda value: service.handle_command(
                            value,
                            crash=crash_at(CrashPoint.DELL_AFTER_EXECUTION_STARTED_COMMIT),
                        ),
                        verifier=environment.central_codec,
                    )
                timings["dell_accept_transaction"] = _elapsed_ms(started)
                environment.dell.startup_recover("stream-target")
                status = service.command_status(str(command["command_id"]))
                environment.central.record_status(status, verifier=environment.central_codec)
                trace.extend(("Dell ACCEPTED", "Agent crash", "OUTCOME_UNKNOWN"))
            else:
                started = time.perf_counter_ns()
                if mode == "ack_loss":
                    with suppress(InjectedCrash):
                        environment.central.deliver_command(
                            str(command["command_id"]),
                            service.handle_command,
                            verifier=environment.central_codec,
                            crash=crash_at(CrashPoint.CENTRAL_AFTER_SEND_BEFORE_RECEIPT),
                        )
                    environment.central.deliver_command(
                        str(command["command_id"]),
                        service.handle_command,
                        verifier=environment.central_codec,
                    )
                else:
                    environment.central.deliver_command(
                        str(command["command_id"]),
                        service.handle_command,
                        verifier=environment.central_codec,
                    )
                timings["dell_accept_transaction"] = _elapsed_ms(started)
                status = service.command_status(str(command["command_id"]))
                environment.central.record_status(status, verifier=environment.central_codec)
                observed_command_state = str(status["command_state"])
                trace.extend(("Dell ACCEPTED", "Fake effect", observed_command_state))
                if observed_command_state == "EFFECT_OBSERVED":
                    observed_at = "2026-08-23T04:20:10.000Z"
                    fresh_until = "2026-08-23T04:21:10.000Z"
                    evaluated_at = "2026-08-23T04:20:20.000Z"
                    if mode in {"verification_delay", "monitoring_stale"}:
                        fresh_until = "2026-08-23T04:20:15.000Z"
                    started = time.perf_counter_ns()
                    verification = FakeMonitoringObserver().verify(
                        command_id=str(command["command_id"]),
                        incident_id=str(command["incident_id"]),
                        target_id="stream-target",
                        monitoring_cycle_id=f"cycle-{scenario_id}",
                        command_state=observed_command_state,
                        before_target=environment.target,
                        observed_target=changed,
                        checks=_checks(mode, observed_at),
                        observed_at=observed_at,
                        evidence_fresh_until=fresh_until,
                        evaluated_at=evaluated_at,
                        observation_revision=f"observation-{scenario_id}",
                    )
                    self.contract.validate(verification)
                    timings["verification"] = _elapsed_ms(started)
                    oracle_violations = IndependentRecoveryVerificationOracle.evaluate(
                        verification,
                        expected_verdict=str(spec["expected_verdict"]),
                        active_command_id=str(command["command_id"]),
                        evaluated_at=evaluated_at,
                    )
                    violations.extend(oracle_violations)
                    ingest_result = environment.central.record_recovery_verification(verification)
                    trace.append("RecoveryVerification")
                else:
                    trace.append("RecoveryVerification suppressed")
            environment.dell.connection.set_trace_callback(None)
            timings["dell_command_roundtrip"] = timings["dell_accept_transaction"]
            timings["dell_accept_transaction"] = dell_transaction_durations[0] if dell_transaction_durations else 0.0
            row = environment.central.connection.execute(
                "SELECT status FROM commands WHERE command_id=?",
                (command["command_id"],),
            ).fetchone()
            command_state = str(row["status"])
            verdict = "NOT_EMITTED" if verification is None else str(verification["verdict"])
            if command_state != spec["expected_command_state"]:
                violations.append("COMMAND_STATE_MISMATCH")
            if verdict != spec["expected_verdict"]:
                violations.append("VERDICT_MISMATCH")
            if environment.adapter.attempt_count > int(spec["max_physical_attempts"]):
                violations.append("PHYSICAL_ATTEMPT_LIMIT_EXCEEDED")
            if command_state == "VERIFIED" and verdict != "RECOVERED":
                violations.append("VERIFIED_WITHOUT_RECOVERED")
            if command_state == "EFFECT_OBSERVED":
                violations.append("EFFECT_OBSERVED_LEFT_AS_VERIFIED_EQUIVALENT")
            trace.append(command_state)
            return E2EOutcome(
                scenario_id=scenario_id,
                classification="PASS" if not violations else "SUT_FAILURE",
                mode=mode,
                trace=tuple(trace),
                command_state=command_state,
                verification_verdict=verdict,
                physical_attempt_count=environment.adapter.attempt_count,
                automatic_second_restart_count=0,
                production_escalation_count=0,
                ingest_result=ingest_result,
                invariant_violations=tuple(violations),
                timings_ms=timings,
            )
        finally:
            environment.close()
