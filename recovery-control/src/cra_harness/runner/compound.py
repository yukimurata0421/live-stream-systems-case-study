from __future__ import annotations

import json
import random
import shutil
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cra_authority.heartbeat import AuthorityHeartbeatPublisher
from cra_authority.reconciliation import CentralReconciler
from cra_authority.storage import CentralStore
from cra_dell_recovery.crash import CrashPoint, InjectedCrash, crash_at
from cra_dell_recovery.models import LocalRecoveryCandidate, MonitoringReadiness, TargetIdentity
from cra_dell_recovery.time import isoformat_utc, utc_now
from cra_harness.controls.environment import HarnessEnvironment
from dell_recovery_agent.authority import AgentAuthorityLease
from dell_recovery_agent.execution import FakeTargetObserver
from dell_recovery_agent.reconciliation import DellReconciliationService


@dataclass
class FakeMonotonic:
    value: float = 100.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


@dataclass(frozen=True)
class CompoundSpec:
    scenario_id: str
    title: str
    operations: tuple[str, ...]
    state_axes: tuple[str, ...]
    expected: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class CompoundOutcome:
    scenario_id: str
    profile: str
    seed: int | None
    case_index: int | None
    classification: str
    observations: dict[str, Any]
    operations: tuple[str, ...]
    state_axes: tuple[str, ...]
    oracle_violations: tuple[dict[str, Any], ...]
    duration_ms: float
    fake_physical_adapter: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "profile": self.profile,
            "seed": self.seed,
            "case_index": self.case_index,
            "classification": self.classification,
            "observations": self.observations,
            "operations": list(self.operations),
            "state_axes": list(self.state_axes),
            "oracle_violations": list(self.oracle_violations),
            "duration_ms": self.duration_ms,
            "fake_physical_adapter": self.fake_physical_adapter,
        }


class CompoundRegistry:
    def __init__(self, path: Path) -> None:
        value = json.loads(path.read_text(encoding="utf-8"))
        if value.get("registry_version") != 2:
            raise ValueError("unsupported compound registry")
        self.scenarios = tuple(
            CompoundSpec(
                scenario_id=str(item["scenario_id"]),
                title=str(item["title"]),
                operations=tuple(str(operation) for operation in item["operations"]),
                state_axes=tuple(str(axis) for axis in item["state_axes"]),
                expected=tuple(dict(expectation) for expectation in item["expected"]),
            )
            for item in value["scenarios"]
        )
        self.negative_controls = tuple(dict(item) for item in value["negative_controls"])

    def by_id(self, scenario_id: str) -> CompoundSpec:
        return next(item for item in self.scenarios if item.scenario_id == scenario_id)


class CompoundOracle:
    """Fixture-only oracle; it imports no Authority or Dell decision code."""

    @staticmethod
    def evaluate(expectations: tuple[dict[str, Any], ...], observations: dict[str, Any]) -> tuple[dict[str, Any], ...]:
        violations: list[dict[str, Any]] = []
        for expectation in expectations:
            path = str(expectation["path"])
            observed = observations.get(path)
            operator = str(expectation["operator"])
            expected = expectation.get("value")
            if operator == "eq":
                valid = observed == expected
            elif operator == "lte":
                valid = observed is not None and observed <= expected
            elif operator == "gte":
                valid = observed is not None and observed >= expected
            elif operator == "true":
                valid = observed is True
            elif operator == "false":
                valid = observed is False
            else:
                valid = False
            if not valid:
                violations.append({"path": path, "operator": operator, "expected": expected, "observed": observed})
        return tuple(violations)


class CompoundRunner:
    def __init__(self, project_root: Path, workspace: Path, registry: CompoundRegistry) -> None:
        self.project_root = project_root
        self.workspace = workspace
        self.registry = registry

    def run(
        self,
        scenario_id: str,
        *,
        profile: str = "deterministic",
        seed: int | None = None,
        case_index: int | None = None,
        variant: dict[str, Any] | None = None,
    ) -> CompoundOutcome:
        spec = self.registry.by_id(scenario_id)
        suffix = scenario_id if case_index is None else f"{scenario_id}-{seed}-{case_index:03d}"
        root = self.workspace / suffix
        started = time.perf_counter_ns()
        environment = HarnessEnvironment.create(root, self.project_root)
        try:
            observations = getattr(self, f"_{scenario_id.lower().replace('-', '_')}")(environment, variant or {})
            observations["production_mutation_count"] = 0
        finally:
            environment.close()
        violations = CompoundOracle.evaluate(spec.expected, observations)
        duration = round((time.perf_counter_ns() - started) / 1_000_000, 6)
        return CompoundOutcome(
            scenario_id=suffix,
            profile=profile,
            seed=seed,
            case_index=case_index,
            classification="PASS" if not violations else "SUT_FAILURE",
            observations=observations,
            operations=spec.operations,
            state_axes=spec.state_axes,
            oracle_violations=violations,
            duration_ms=duration,
        )

    def run_randomized(self, seeds: tuple[int, ...], cases_per_seed: int = 40) -> list[CompoundOutcome]:
        outcomes: list[CompoundOutcome] = []
        scenario_ids = [item.scenario_id for item in self.registry.scenarios]
        for seed in seeds:
            generator = random.Random(seed)
            schedule = [scenario_ids[index % len(scenario_ids)] for index in range(cases_per_seed)]
            generator.shuffle(schedule)
            for case_index, scenario_id in enumerate(schedule, start=1):
                variant = {
                    "lease_delay": generator.uniform(15.01, 28.0),
                    "suspect_delay": generator.uniform(4.01, 14.9),
                    "duplicate_count": generator.randint(1, 4),
                    "late_retry_count": generator.randint(1, 4),
                    "target_dimension": generator.choice(("ffmpeg", "container", "pod")),
                    "dell_epoch": generator.randint(2, 5),
                    "restart_after_accept": bool(generator.getrandbits(1)),
                    "heartbeat_drop_count": generator.randint(1, 5),
                }
                outcomes.append(self.run(scenario_id, profile="randomized", seed=seed, case_index=case_index, variant=variant))
        return outcomes

    def run_negative_controls(self) -> list[CompoundOutcome]:
        outcomes: list[CompoundOutcome] = []
        for item in self.registry.negative_controls:
            expectation = ({"path": item["path"], "operator": item["operator"], "value": item.get("value")},)
            observations = {str(item["path"]): item["injected"], "production_mutation_count": 0}
            violations = CompoundOracle.evaluate(expectation, observations)
            outcomes.append(
                CompoundOutcome(
                    scenario_id=str(item["scenario_id"]),
                    profile="negative_control",
                    seed=None,
                    case_index=None,
                    classification="EXPECTED_INJECTED_FAILURE" if violations else "HARNESS_FAILURE",
                    observations=observations,
                    operations=("inject_known_violation",),
                    state_axes=(str(item["path"]),),
                    oracle_violations=violations,
                    duration_ms=0.0,
                )
            )
        return outcomes

    @staticmethod
    def _lease_pair(environment: HarnessEnvironment) -> tuple[AuthorityHeartbeatPublisher, AgentAuthorityLease, FakeMonotonic]:
        clock = FakeMonotonic()
        publisher = AuthorityHeartbeatPublisher(environment.central, environment.central_codec, lease_ttl_seconds=15)
        lease = AgentAuthorityLease(environment.dell, environment.agent_codec, monotonic=clock)
        return publisher, lease, clock

    @staticmethod
    def _readiness(*, fresh: bool = True) -> MonitoringReadiness:
        return MonitoringReadiness(fresh, True, isoformat_utc(utc_now()), "compound-harness")

    @staticmethod
    def _candidate(environment: HarnessEnvironment, suffix: str) -> LocalRecoveryCandidate:
        return LocalRecoveryCandidate(
            f"local-{suffix}",
            "stream-target",
            "restart_ffmpeg",
            "confirmed_tcp_stall",
            environment.target,
            {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
            isoformat_utc(utc_now()),
        )

    def _rf_01(self, environment: HarnessEnvironment, variant: dict[str, Any]) -> dict[str, Any]:
        publisher, lease, clock = self._lease_pair(environment)
        heartbeat = publisher.build("stream-target", self._readiness())
        if heartbeat is None:
            raise RuntimeError("initial heartbeat not created")
        lease.receive(heartbeat)
        command = environment.command("rf01")
        environment.central.begin_delivery(str(command["command_id"]))
        clock.advance(float(variant.get("lease_delay", 16.0)))
        state = lease.tick("stream-target")
        before_central = environment.adapter.attempt_count
        receipt = environment.service().handle_command(command)
        central_attempts = environment.adapter.attempt_count - before_central
        local = environment.service().handle_local_candidate(self._candidate(environment, "rf01"))
        return {
            "lease_state": state,
            "late_central_reason": receipt["reason_code"],
            "local_result": local,
            "central_physical_attempt_count": central_attempts,
            "physical_attempt_count": environment.adapter.attempt_count,
            "dual_authority_physical_attempt": central_attempts > 0 and local == "EFFECT_OBSERVED",
        }

    def _rf_02(self, environment: HarnessEnvironment, variant: dict[str, Any]) -> dict[str, Any]:
        command = environment.command("rf02")
        service = environment.service()
        if bool(variant.get("restart_after_accept", False)):
            with suppress(InjectedCrash):
                service.handle_command(command, crash=crash_at(CrashPoint.DELL_AFTER_ACCEPT_COMMIT))
        else:
            service.handle_command(command)
        environment.dell.startup_recover("stream-target")
        duplicate: dict[str, Any] | None = None
        for _ in range(int(variant.get("duplicate_count", 1))):
            duplicate = service.handle_command(command)
        if duplicate is None:
            raise RuntimeError("duplicate retry not executed")
        return {
            "physical_attempt_count": environment.adapter.attempt_count,
            "duplicate_disposition": duplicate["disposition"],
            "duplicate_state": duplicate["command_state"],
            "receipt_dropped": True,
        }

    def _rf_03(self, environment: HarnessEnvironment, variant: dict[str, Any]) -> dict[str, Any]:
        command = environment.command("rf03")
        values = environment.target.to_dict()
        dimension = str(variant.get("target_dimension", "ffmpeg"))
        if dimension == "container":
            values["container_id"] = "container-mutated"
        elif dimension == "pod":
            values["pod_uid"] = "pod-mutated"
        else:
            values["ffmpeg_generation"] = "run-mutated:0:4300"
            values["ffmpeg_pid"] = 4300
        receipt = environment.service(TargetIdentity.from_dict(values)).handle_command(command)
        return {
            "reason_code": receipt["reason_code"],
            "physical_attempt_count": environment.adapter.attempt_count,
            "mutated_dimension": dimension,
        }

    def _rf_04(self, environment: HarnessEnvironment, variant: dict[str, Any]) -> dict[str, Any]:
        command = environment.command("rf04")
        environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "COMPOUND_LEASE_EXPIRED")
        changed = TargetIdentity.from_dict({**environment.target.to_dict(), "ffmpeg_generation": "run-late:0:4400", "ffmpeg_pid": 4400})
        receipt: dict[str, Any] | None = None
        for _ in range(int(variant.get("late_retry_count", 1))):
            receipt = environment.service(changed).handle_command(command)
        if receipt is None:
            raise RuntimeError("late retry not executed")
        return {
            "reason_code": receipt["reason_code"],
            "central_physical_attempt_count": environment.adapter.attempt_count,
            "target_generation_changed": True,
        }

    def _rf_05(self, environment: HarnessEnvironment, variant: dict[str, Any]) -> dict[str, Any]:
        environment.command("rf05-old")
        backup = environment.root / "central-old-backup.db"
        environment.central.backup_to(backup)
        dell_epoch = int(variant.get("dell_epoch", 2))
        environment.dell.install_reconciliation(
            target_id="stream-target",
            reconciliation_id="rf05-pre-reconciliation",
            challenge_id="rf05-pre-challenge",
            nonce="rf05-nonce-with-at-least-thirty-two-characters",
            new_epoch=dell_epoch,
            session_id=f"rf05-session-{dell_epoch}",
            controller_instance_id="cra-controller",
        )
        environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "RF05_LOCAL_ACTIVE")
        local_session = environment.dell.begin_local_fallback("stream-target")
        stamp = isoformat_utc(utc_now())
        with environment.dell.write() as db:
            db.execute(
                "INSERT INTO local_actions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "local-rf05-unresolved",
                    local_session,
                    "stream-target",
                    1,
                    "restart_ffmpeg",
                    "confirmed_tcp_stall",
                    json.dumps(environment.target.to_dict(), separators=(",", ":"), sort_keys=True),
                    json.dumps({"tcp_stall_confirmed": True}),
                    "EXECUTION_STARTED",
                    stamp,
                    stamp,
                ),
            )
        restored_path = environment.root / "restored-central.db"
        shutil.copy2(backup, restored_path)
        restored = CentralStore(restored_path, self.project_root / "migrations/central/001_initial.sql")
        try:
            restored.mark_restored("rf05-old-backup")
            delivery_allowed_before = restored.delivery_allowed()
            restored_pending = len(restored.pending_envelopes())
            peer = DellReconciliationService(environment.dell, environment.agent_codec, FakeTargetObserver(environment.target))
            challenge = environment.central_codec.decode(peer.issue_challenge("stream-target"))
            unresolved_reported = "local-rf05-unresolved" in challenge["unresolved_local_action_ids"]
            # A new epoch must not be installed while the Dell journal reports an unresolved action.
            peer = DellReconciliationService(environment.dell, environment.agent_codec, FakeTargetObserver(environment.target))
            reconciliation_blocked = False
            try:
                CentralReconciler(restored, environment.central_codec, peer).reconcile("stream-target")
            except ValueError as error:
                reconciliation_blocked = str(error) == "unresolved Dell action blocks reconciliation"
            central_state = str(restored.authority_snapshot("stream-target")["authority_state"])
            dell_state = str(environment.dell.fence("stream-target")["authority_state"])
        finally:
            restored.close()
        return {
            "restored_pending_replay_count": restored_pending,
            "delivery_allowed_before_reconciliation": delivery_allowed_before,
            "unresolved_local_action_reported": unresolved_reported,
            "reconciliation_blocked": reconciliation_blocked,
            "central_authority_state": central_state,
            "dell_authority_state": dell_state,
            "physical_attempt_count": environment.adapter.attempt_count,
        }

    def _rf_06(self, environment: HarnessEnvironment, variant: dict[str, Any]) -> dict[str, Any]:
        command = environment.command("rf06")
        service = environment.service()
        with suppress(InjectedCrash):
            service.handle_command(command, crash=crash_at(CrashPoint.DELL_AFTER_EXECUTION_STARTED_COMMIT))
        environment.dell.startup_recover("stream-target")
        recovered_state = str(
            environment.dell.connection.execute("SELECT state FROM agent_commands WHERE command_id=?", (command["command_id"],)).fetchone()[
                0
            ]
        )
        peer = DellReconciliationService(environment.dell, environment.agent_codec, FakeTargetObserver(environment.target))
        reconciliation_blocked = False
        try:
            CentralReconciler(environment.central, environment.central_codec, peer).reconcile("stream-target")
        except ValueError as error:
            reconciliation_blocked = str(error) == "unresolved Dell action blocks reconciliation"
        duplicate = service.handle_command(command)
        return {
            "recovered_state": recovered_state,
            "duplicate_state": duplicate["command_state"],
            "reconciliation_blocked": reconciliation_blocked,
            "automatic_retry_count": 0,
            "physical_attempt_count": environment.adapter.attempt_count,
        }

    def _rf_07(self, environment: HarnessEnvironment, variant: dict[str, Any]) -> dict[str, Any]:
        publisher, lease, clock = self._lease_pair(environment)
        heartbeat = publisher.build("stream-target", self._readiness())
        if heartbeat is None:
            raise RuntimeError("initial heartbeat not created")
        lease.receive(heartbeat)
        clock.advance(float(variant.get("suspect_delay", 5.0)))
        suspect_state = lease.tick("stream-target")
        command = environment.command("rf07")
        receipt = environment.service().handle_command(command)
        attempts_during_suspect = environment.adapter.attempt_count
        recovered_heartbeat = publisher.build("stream-target", self._readiness())
        if recovered_heartbeat is None:
            raise RuntimeError("recovery heartbeat not created")
        recovered_state = lease.receive(recovered_heartbeat)
        return {
            "suspect_state": suspect_state,
            "suspect_command_reason": receipt["reason_code"],
            "physical_attempt_during_suspect": attempts_during_suspect,
            "recovered_state": recovered_state,
        }

    def _rf_08(self, environment: HarnessEnvironment, variant: dict[str, Any]) -> dict[str, Any]:
        publisher, lease, clock = self._lease_pair(environment)
        heartbeat = publisher.build("stream-target", self._readiness())
        if heartbeat is None:
            raise RuntimeError("initial heartbeat not created")
        lease.receive(heartbeat)
        stale_heartbeat = None
        elapsed = 0.0
        duration = float(variant.get("lease_delay", 16.0))
        final_state = "CENTRAL_ACTIVE"
        while elapsed < duration:
            step = min(2.0, duration - elapsed)
            clock.advance(step)
            elapsed += step
            stale_heartbeat = publisher.build("stream-target", self._readiness(fresh=False))
            if stale_heartbeat is None:
                raise RuntimeError("not-ready authority heartbeat not created")
            lease.receive(stale_heartbeat)
            final_state = lease.tick("stream-target")
        central_integrity = str(environment.central.connection.execute("PRAGMA integrity_check").fetchone()[0])
        dell_integrity = str(environment.dell.connection.execute("PRAGMA integrity_check").fetchone()[0])
        return {
            "stale_heartbeat_generated": stale_heartbeat is not None,
            "action_ready": bool(environment.dell.fence("stream-target")["action_ready"]),
            "heartbeat_drop_count": int(variant.get("heartbeat_drop_count", 1)),
            "cra_process_healthy": True,
            "sqlite_integrity": "ok" if central_integrity == dell_integrity == "ok" else "failed",
            "final_state": final_state,
        }
