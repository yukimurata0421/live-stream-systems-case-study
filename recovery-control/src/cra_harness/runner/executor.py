from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cra_authority.heartbeat import AuthorityHeartbeatPublisher
from cra_authority.reconciliation import CentralReconciler
from cra_authority.replay import replay_2026_08_21
from cra_authority.storage import CentralStore
from cra_dell_recovery.crash import CrashPoint, InjectedCrash, crash_at
from cra_dell_recovery.models import LocalRecoveryCandidate, MonitoringReadiness, TargetIdentity
from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_harness.controls.environment import HarnessEnvironment
from cra_harness.controls.isolation import verify_production_isolation
from cra_harness.controls.mutations import BrokenMutationAdapter
from cra_harness.injectors.faults import FaultController
from cra_harness.observers.evidence import EvidenceBundle, EvidenceCollector
from cra_harness.observers.sut import SutObserver
from cra_harness.scenarios.model import ScenarioSpec
from dell_recovery_agent.authority import AgentAuthorityLease
from dell_recovery_agent.execution import FakeTargetObserver
from dell_recovery_agent.reconciliation import DellReconciliationService


@dataclass
class _FakeMonotonic:
    value: float = 100.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@dataclass(frozen=True)
class ScenarioExecution:
    scenario: ScenarioSpec
    evidence: EvidenceBundle
    harness_errors: tuple[str, ...]
    environment_errors: tuple[str, ...]
    safety_errors: tuple[str, ...]
    injector_errors: tuple[str, ...]


class ScenarioExecutor:
    def __init__(self, project_root: Path, workspace: Path, manifest: dict[str, Any]) -> None:
        self.project_root = project_root
        self.workspace = workspace
        self.manifest = manifest

    def execute(self, run_id: str, scenario: ScenarioSpec) -> ScenarioExecution:
        root = self.workspace / scenario.scenario_id
        environment = HarnessEnvironment.create(root, self.project_root)
        collector = EvidenceCollector(run_id, scenario.scenario_id)
        protocols: list[dict[str, Any]] = []
        faults = FaultController(scenario.faults)
        harness_errors: list[str] = []
        environment_errors: list[str] = []
        process_exit: dict[str, Any] = {"exit_code": 0, "exception_type": None}
        collector.append("environment_manifest", "runner.manifest", dict(self.manifest))
        collector.append(
            "target_identity",
            "observer.target",
            {"before": environment.target.to_dict(), "after": environment.target.to_dict()},
        )
        try:
            faults.arm_all()
            faults.trigger_all()
            result = self._exercise(scenario, environment, protocols)
            collector.append("scenario_result", "observer.scenario", result)
            faults.complete_all()
        except Exception as error:  # Harness containment: never relabel plumbing exceptions as SUT failure.
            harness_errors.append(f"SCENARIO_EXECUTOR_EXCEPTION:{type(error).__name__}")
            process_exit = {"exit_code": 1, "exception_type": type(error).__name__}
            collector.append("scenario_result", "observer.scenario", {"executor_completed": False})
        for record in faults.history:
            collector.append("fault_events", "injector.lifecycle", record.to_dict())
        if not faults.history:
            collector.append("fault_events", "injector.lifecycle", {"fault_count": 0, "lifecycle": "not_requested"})
        isolation = verify_production_isolation(environment)
        collector.append("production_isolation", "control.production_isolation", isolation.to_dict())
        SutObserver().capture(
            collector,
            environment,
            protocol_messages=protocols,
            process_exit=process_exit,
        )
        safety_errors = () if isolation.safe else ("PRODUCTION_ISOLATION_VIOLATION",)
        injector_errors = tuple(f"FAULT_NOT_COMPLETED:{item}" for item in faults.untriggered)
        bundle = collector.freeze()
        environment.close()
        return ScenarioExecution(
            scenario,
            bundle,
            tuple(harness_errors),
            tuple(environment_errors),
            safety_errors,
            injector_errors,
        )

    def _exercise(
        self,
        scenario: ScenarioSpec,
        environment: HarnessEnvironment,
        protocols: list[dict[str, Any]],
    ) -> dict[str, Any]:
        scenario_id = scenario.scenario_id
        if scenario.negative_control:
            return BrokenMutationAdapter().inject(scenario_id)
        if scenario_id == "D-01-normal":
            candidate = LocalRecoveryCandidate(
                "local-central-active",
                "stream-target",
                "restart_ffmpeg",
                "confirmed_tcp_stall",
                environment.target,
                {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
                isoformat_utc(utc_now()),
            )
            local_result = environment.service().handle_local_candidate(candidate)
            local_attempt_count = environment.adapter.attempt_count
            command = environment.command()
            service = environment.service(environment.target, environment.target)
            receipt = environment.central.deliver_command(
                str(command["command_id"]),
                service.handle_command,
                verifier=environment.central_codec,
            )
            status = service.command_status(str(command["command_id"]))
            environment.central.record_status(status, verifier=environment.central_codec)
            protocols.extend([command, receipt, status])
            return {
                "reason_code": status["reason_code"],
                "terminal_state": status["command_state"],
                "central_active_local_result": local_result,
                "central_active_local_attempt_count": local_attempt_count,
            }
        if scenario_id == "D-02-duplicate":
            command = environment.command()
            service = environment.service(environment.target, environment.target)
            first = service.handle_command(command)
            duplicate = service.handle_command(command)
            protocols.extend([command, first, duplicate])
            return {"duplicate_disposition": duplicate["disposition"]}
        if scenario_id == "D-03-ack-loss":
            command = environment.command()
            service = environment.service(environment.target, environment.target)
            with suppress(InjectedCrash):
                environment.central.deliver_command(
                    str(command["command_id"]),
                    service.handle_command,
                    verifier=environment.central_codec,
                    crash=crash_at(CrashPoint.CENTRAL_AFTER_SEND_BEFORE_RECEIPT),
                )
            duplicate = environment.central.deliver_command(
                str(command["command_id"]),
                service.handle_command,
                verifier=environment.central_codec,
            )
            protocols.extend([command, duplicate])
            return {"duplicate_disposition": duplicate["disposition"]}
        if scenario_id == "D-04-central-commit-crash":
            authorization = environment.authorization()
            environment.central.add_authorization(authorization)
            with suppress(InjectedCrash):
                environment.central.create_command(
                    authorization.authorization_id,
                    environment.central_codec,
                    command_id="command-crash",
                    crash=crash_at(CrashPoint.CENTRAL_BEFORE_COMMIT),
                )
            return {"rollback_observed": environment.central.command_count() == 0}
        if scenario_id in {"D-05-dell-accepted-crash", "D-06-dell-started-crash"}:
            command = environment.command()
            service = environment.service(environment.target, environment.target)
            point = (
                CrashPoint.DELL_AFTER_ACCEPT_COMMIT
                if scenario_id == "D-05-dell-accepted-crash"
                else CrashPoint.DELL_AFTER_EXECUTION_STARTED_COMMIT
            )
            with suppress(InjectedCrash):
                service.handle_command(command, crash=crash_at(point))
            environment.dell.startup_recover("stream-target")
            state = environment.dell.connection.execute(
                "SELECT state FROM agent_commands WHERE command_id=?", (command["command_id"],)
            ).fetchone()[0]
            retry = service.handle_command(command)
            protocols.extend([command, retry])
            return {"recovered_state": state, "retry_disposition": retry["disposition"]}
        if scenario_id in {"D-07-stale-epoch", "D-08-stale-sequence", "D-09-sequence-gap"}:
            command = environment.command()
            changes: dict[str, Any]
            if scenario_id == "D-07-stale-epoch":
                changes = {"authority_epoch": 2}
            elif scenario_id == "D-08-stale-sequence":
                environment.dell.connection.execute(
                    "UPDATE authority_fences SET highest_command_seq_consumed=1 WHERE target_id='stream-target'"
                )
                changes = {"command_id": "command-stale", "message_id": "message-command-stale"}
            else:
                changes = {"command_seq": 2, "idempotency_key": "1:2:auth-1"}
            mutated = environment.central_codec.signer.sign({**command, **changes})
            receipt = environment.service().handle_command(mutated)
            protocols.extend([mutated, receipt])
            return {"reason_code": receipt["reason_code"]}
        if scenario_id == "D-10-stale-target":
            command = environment.command()
            changed = TargetIdentity(**{**environment.target.to_dict(), "ffmpeg_generation": "run-b:0:4200", "ffmpeg_pid": 4200})
            receipt = environment.service(environment.target, changed).handle_command(command)
            protocols.extend([command, receipt])
            return {"reason_code": receipt["reason_code"]}
        if scenario_id == "D-11-ledger-failure":
            command = environment.command()

            @contextmanager
            def unavailable() -> Iterator[sqlite3.Connection]:
                raise sqlite3.OperationalError("simulated SQLITE_IOERR")
                yield environment.dell.connection  # pragma: no cover

            original_write = environment.dell.write
            environment.dell.write = unavailable  # type: ignore[method-assign]
            try:
                receipt = environment.service().handle_command(command)
            finally:
                environment.dell.write = original_write  # type: ignore[method-assign]
            protocols.extend([command, receipt])
            return {"reason_code": receipt["reason_code"]}
        if scenario_id in {"D-12-heartbeat-late", "D-13-heartbeat-expiry"}:
            publisher, lease, clock = self._lease_pair(environment)
            heartbeat = publisher.build("stream-target", self._readiness())
            if heartbeat is None:
                raise RuntimeError("heartbeat fixture not created")
            lease.receive(heartbeat)
            clock.advance(5 if scenario_id == "D-12-heartbeat-late" else 16)
            interim = lease.tick("stream-target")
            if scenario_id == "D-12-heartbeat-late":
                next_heartbeat = publisher.build("stream-target", self._readiness())
                if next_heartbeat is None:
                    raise RuntimeError("recovery heartbeat not created")
                final = lease.receive(next_heartbeat)
                protocols.extend([heartbeat, next_heartbeat])
            else:
                final = interim
                protocols.append(heartbeat)
            return {"interim_state": interim, "final_state": final}
        if scenario_id == "D-14-local-fallback":
            command = environment.command()
            environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "HARNESS_LEASE_EXPIRED")
            central_receipt = environment.service().handle_command(command)
            central_attempt_count = environment.adapter.attempt_count
            candidate = LocalRecoveryCandidate(
                "local-action-1",
                "stream-target",
                "restart_ffmpeg",
                "confirmed_tcp_stall",
                environment.target,
                {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
                isoformat_utc(utc_now()),
            )
            result = environment.service(environment.target, environment.target).handle_local_candidate(candidate)
            protocols.extend([command, central_receipt])
            return {
                "local_result": result,
                "central_result": central_receipt["reason_code"],
                "central_attempt_count": central_attempt_count,
            }
        if scenario_id == "D-15-reconciliation":
            peer = DellReconciliationService(
                environment.dell,
                environment.agent_codec,
                FakeTargetObserver(environment.target),
            )
            new_epoch = CentralReconciler(environment.central, environment.central_codec, peer).reconcile("stream-target")
            return {"new_epoch": new_epoch, "dell_state": environment.dell.fence("stream-target")["authority_state"]}
        if scenario_id == "D-16-backup-restore":
            environment.command()
            backup = environment.root / "backup/central.db"
            environment.central.backup_to(backup)
            restored = CentralStore(backup, self.project_root / "migrations/central/001_initial.sql")
            try:
                with suppress(InjectedCrash):
                    restored.mark_restored("harness-backup", crash=crash_at(CrashPoint.CRA_AFTER_RESTORE))
                pending = len(restored.pending_envelopes())
                restore_state = restored.connection.execute("SELECT restore_state FROM control_plane_identity").fetchone()[0]
                publisher = AuthorityHeartbeatPublisher(
                    restored,
                    environment.central_codec,
                    interval_seconds=2,
                    lease_ttl_seconds=15,
                )
                heartbeat_generated = publisher.build("stream-target", self._readiness()) is not None
            finally:
                restored.close()
            return {
                "restored_pending_count": pending,
                "restore_state": restore_state,
                "heartbeat_generated": heartbeat_generated,
            }
        if scenario_id == "D-17-2026-replay":
            fixture = json.loads((self.project_root / "tests/fixtures/2026-08-21_replay.json").read_text(encoding="utf-8"))
            decisions = replay_2026_08_21(fixture)
            return {
                "automatic_command_count": sum(item.automatic_command_count for item in decisions),
                "unknown_count": sum(item.outcome == "UNKNOWN" for item in decisions),
            }
        if scenario_id.startswith("R-"):
            return self._random_case(scenario, environment, protocols)
        raise ValueError(f"scenario has no executor: {scenario_id}")

    @staticmethod
    def _readiness() -> MonitoringReadiness:
        return MonitoringReadiness(True, True, isoformat_utc(utc_now()), "harness")

    @staticmethod
    def _lease_pair(
        environment: HarnessEnvironment,
    ) -> tuple[AuthorityHeartbeatPublisher, AgentAuthorityLease, _FakeMonotonic]:
        clock = _FakeMonotonic()
        publisher = AuthorityHeartbeatPublisher(
            environment.central,
            environment.central_codec,
            interval_seconds=2,
            lease_ttl_seconds=15,
        )
        lease = AgentAuthorityLease(
            environment.dell,
            environment.agent_codec,
            suspect_after_seconds=4,
            lease_ttl_seconds=15,
            monotonic=clock,
        )
        return publisher, lease, clock

    @staticmethod
    def _random_case(
        scenario: ScenarioSpec,
        environment: HarnessEnvironment,
        protocols: list[dict[str, Any]],
    ) -> dict[str, Any]:
        command = environment.command()
        mutation = str(scenario.inputs["mutation"])
        delta = int(scenario.inputs["delta"])
        if mutation == "sequence":
            command_sequence = command["command_seq"]
            if not isinstance(command_sequence, int):
                raise TypeError("command_seq is not an integer")
            command = environment.central_codec.signer.sign(
                {**command, "command_seq": command_sequence + delta, "idempotency_key": f"1:{1 + delta}:auth-1"}
            )
        elif mutation == "target":
            expected_target = command["expected_target"]
            if not isinstance(expected_target, dict) or not isinstance(expected_target.get("ffmpeg_pid"), int):
                raise TypeError("expected_target is malformed")
            changed = {**expected_target, "ffmpeg_pid": expected_target["ffmpeg_pid"] + delta}
            command = environment.central_codec.signer.sign({**command, "expected_target": changed})
        receipt = environment.service(environment.target, environment.target).handle_command(command)
        protocols.extend([command, receipt])
        return {"reason_code": receipt["reason_code"], "mutation": mutation, "delta": delta}
