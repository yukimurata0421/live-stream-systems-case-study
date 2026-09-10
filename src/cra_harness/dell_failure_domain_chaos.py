from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import socket
import sqlite3
import struct
import subprocess
import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_authority.reconciliation import CentralReconciler
from cra_authority.storage import CentralStore
from cra_dell_recovery.canonical import KeyRing, SignedMessageCodec, Signer
from cra_dell_recovery.errors import CommandBlocked
from cra_dell_recovery.models import LocalRecoveryCandidate, TargetIdentity
from cra_dell_recovery.schema import SchemaRegistry
from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.execution import AgentService, FakePhysicalAdapter, FakeTargetObserver
from dell_recovery_agent.reconciliation import DellReconciliationService
from dell_recovery_agent.storage import DellStore

SCHEMA = "cra.dell_failure_domain_chaos.v1"
LOOPBACK_SCENARIOS = (
    "ACCEPT_THEN_STALL",
    "PARTIAL_THEN_STALL",
    "RESET_AFTER_REQUEST",
    "CLOSE_WITHOUT_REPLY",
    "DELAYED_SUCCESS",
    "CONNECTION_REFUSED",
)
INVALID_CANDIDATE_SCENARIOS = (
    "MISSING_CONFIRMATION",
    "FALSE_CONFIRMATION",
    "INTEGER_CONFIRMATION",
    "MISSING_SAMPLE_COUNT",
    "BOOLEAN_SAMPLE_COUNT",
    "INSUFFICIENT_SAMPLE_COUNT",
    "MISSING_CONFIRM_THRESHOLD",
    "BOOLEAN_CONFIRM_THRESHOLD",
    "UNSAFE_CONFIRM_THRESHOLD",
    "COUNT_BELOW_CONFIRM_THRESHOLD",
    "TARGET_NETWORK_DOWN",
    "REMOTE_WARNING_ONLY",
    "OUTCOME_ALREADY_UNKNOWN",
    "MAINTENANCE_ACTIVE",
    "STALE_EVIDENCE",
    "FUTURE_EVIDENCE",
    "WRONG_REASON",
    "TARGET_DRIFT",
    "TARGET_UNAVAILABLE",
)


@dataclass
class _Environment:
    central: CentralStore
    dell: DellStore
    central_codec: SignedMessageCodec
    agent_codec: SignedMessageCodec
    target: TargetIdentity
    adapter: FakePhysicalAdapter

    def close(self) -> None:
        self.dell.close()
        self.central.close()


class _UnknownAdapter(FakePhysicalAdapter):
    def restart_ffmpeg(self, expected_target: TargetIdentity) -> Any:
        self.attempt_count += 1
        raise TimeoutError(f"synthetic outcome unknown for {expected_target.ffmpeg_generation}")


class _AckLossPeer:
    def __init__(self, delegate: DellReconciliationService) -> None:
        self.delegate = delegate
        self.fail_once = True

    def issue_challenge(self, target_id: str) -> dict[str, Any]:
        return self.delegate.issue_challenge(target_id)

    def journal_page(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.delegate.journal_page(payload)

    def commit(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self.delegate.commit(payload)
        if self.fail_once:
            self.fail_once = False
            raise TimeoutError("synthetic ACK loss after durable Dell commit")
        return result


class _GapPeer:
    def __init__(self, delegate: DellReconciliationService, central_codec: SignedMessageCodec, agent_codec: SignedMessageCodec) -> None:
        self.delegate = delegate
        self.central_codec = central_codec
        self.agent_codec = agent_codec

    def issue_challenge(self, target_id: str) -> dict[str, Any]:
        return self.delegate.issue_challenge(target_id)

    def journal_page(self, payload: dict[str, Any]) -> dict[str, Any]:
        page = self.central_codec.decode(self.delegate.journal_page(payload))
        records = list(page["records"])
        page["records"] = [records[0], records[-1]]
        return self.agent_codec.encode(page)

    def commit(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.delegate.commit(payload)


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _environment(project_root: Path, root: Path) -> _Environment:
    registry = SchemaRegistry(project_root / "contracts/cra_dell_recovery/v1")
    central_private = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    agent_private = Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    central_codec = SignedMessageCodec(
        registry,
        Signer("cra-test-key", central_private),
        KeyRing({"agent-test-key": agent_private.public_key()}),
    )
    agent_codec = SignedMessageCodec(
        registry,
        Signer("agent-test-key", agent_private),
        KeyRing({"cra-test-key": central_private.public_key()}),
    )
    central = CentralStore(root / "central/central.db", project_root / "migrations/central/001_initial.sql")
    central.bootstrap(
        target_id="stream-target",
        host_id="dell",
        agent_id="dell-agent",
        controller_instance_id="cra-controller",
        agent_installation_id="agent-installation-1",
    )
    dell = DellStore(root / "dell/dell.db", project_root / "migrations/dell/001_initial.sql")
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
           state_reason='VALID_HARNESS_HEARTBEAT'"""
    )
    dell.set_process_lease_valid("stream-target", True)
    return _Environment(
        central=central,
        dell=dell,
        central_codec=central_codec,
        agent_codec=agent_codec,
        target=TargetIdentity(
            host_id="dell",
            host_boot_id="boot-a",
            namespace="stream-v3",
            pod_uid="pod-a",
            container_name="stream-engine",
            container_id="containerd://a",
            ffmpeg_generation="generation-a",
            ffmpeg_pid=4100,
        ),
        adapter=FakePhysicalAdapter(),
    )


def _candidate(
    environment: _Environment,
    action_id: str,
    *,
    evidence: dict[str, Any] | None = None,
    observed_at: str | None = None,
    reason_code: str = "confirmed_tcp_stall",
    target: TargetIdentity | None = None,
) -> LocalRecoveryCandidate:
    return LocalRecoveryCandidate(
        local_action_id=action_id,
        target_id="stream-target",
        action="restart_ffmpeg",
        reason_code=reason_code,
        target_identity=target or environment.target,
        evidence=evidence
        if evidence is not None
        else {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
        observed_at=observed_at or isoformat_utc(utc_now()),
    )


def _local_service(
    environment: _Environment,
    *,
    adapter: FakePhysicalAdapter | None = None,
    observer: FakeTargetObserver | None = None,
) -> AgentService:
    return AgentService(
        environment.dell,
        environment.agent_codec,
        observer or FakeTargetObserver(environment.target, environment.target),
        adapter or environment.adapter,
    )


def _complete_local_action(environment: _Environment, action_id: str = "local-complete") -> str:
    environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "SYNTHETIC_CRA_LINK_DOWN")
    return _local_service(environment).handle_local_candidate(_candidate(environment, action_id))


def _deterministic_cases(project_root: Path, workspace: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []

    environment = _environment(project_root, workspace / "det-valid")
    try:
        first = _complete_local_action(environment)
        replay = _local_service(environment).handle_local_candidate(_candidate(environment, "local-complete"))
        other = _local_service(environment).handle_local_candidate(_candidate(environment, "local-other"))
        cases.append(
            {
                "scenario": "CRA_LINK_DOWN_TARGET_TRANSPORT_HEALTHY",
                "pass": first == replay == "EFFECT_OBSERVED" and other == "HARD_COOLDOWN_ACTIVE" and environment.adapter.attempt_count == 1,
                "first": first,
                "exact_replay": replay,
                "other_scope": other,
                "synthetic_attempt_count": environment.adapter.attempt_count,
            }
        )
    finally:
        environment.close()

    environment = _environment(project_root, workspace / "det-target-network")
    try:
        environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "SYNTHETIC_CRA_LINK_DOWN")
        result = _local_service(environment).handle_local_candidate(
            _candidate(
                environment,
                "local-network-down",
                evidence={
                    "tcp_stall_confirmed": True,
                    "distinct_sample_count": 3,
                    "stall_confirm_threshold": 3,
                    "network_down": True,
                },
            )
        )
        cases.append(
            {
                "scenario": "CRA_LINK_AND_TARGET_NETWORK_BOTH_DOWN",
                "pass": result == "INSUFFICIENT_LOCAL_EVIDENCE" and environment.adapter.attempt_count == 0,
                "result": result,
                "synthetic_attempt_count": environment.adapter.attempt_count,
            }
        )
    finally:
        environment.close()

    environment = _environment(project_root, workspace / "det-unknown")
    unknown_adapter = _UnknownAdapter()
    try:
        environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "SYNTHETIC_CRA_LINK_DOWN")
        result = _local_service(environment, adapter=unknown_adapter).handle_local_candidate(
            _candidate(environment, "local-outcome-unknown")
        )
        peer = DellReconciliationService(
            environment.dell,
            environment.agent_codec,
            FakeTargetObserver(environment.target),
        )
        blocked = ""
        try:
            CentralReconciler(environment.central, environment.central_codec, peer).reconcile("stream-target")
        except ValueError as error:
            blocked = str(error)
        cases.append(
            {
                "scenario": "OUTCOME_UNKNOWN_BLOCKS_RECONNECT",
                "pass": result == "OUTCOME_UNKNOWN"
                and blocked == "unresolved Dell action blocks reconciliation"
                and unknown_adapter.attempt_count == 1
                and environment.central.command_count() == 0,
                "result": result,
                "reconciliation_blocker": blocked,
                "synthetic_attempt_count": unknown_adapter.attempt_count,
            }
        )
    finally:
        environment.close()

    environment = _environment(project_root, workspace / "det-handback")
    try:
        first = _complete_local_action(environment)
        peer = DellReconciliationService(
            environment.dell,
            environment.agent_codec,
            FakeTargetObserver(environment.target),
        )
        first_epoch = CentralReconciler(environment.central, environment.central_codec, peer).reconcile("stream-target")
        first_rows = int(environment.central.read_one("SELECT count(*) FROM dell_journal_records")[0])  # type: ignore[index]
        second_epoch = CentralReconciler(environment.central, environment.central_codec, peer).reconcile("stream-target")
        second_rows = int(environment.central.read_one("SELECT count(*) FROM dell_journal_records")[0])  # type: ignore[index]
        cases.append(
            {
                "scenario": "RECONNECT_APPEND_ONLY_EXACTLY_ONCE",
                "pass": first == "EFFECT_OBSERVED"
                and first_epoch == 2
                and second_epoch == 3
                and first_rows == second_rows == 3
                and environment.central.command_count() == 0
                and environment.adapter.attempt_count == 1,
                "epochs": [first_epoch, second_epoch],
                "journal_rows": [first_rows, second_rows],
                "synthetic_attempt_count": environment.adapter.attempt_count,
            }
        )
    finally:
        environment.close()

    environment = _environment(project_root, workspace / "det-ack-loss")
    try:
        first = _complete_local_action(environment)
        ack_loss_peer = _AckLossPeer(
            DellReconciliationService(
                environment.dell,
                environment.agent_codec,
                FakeTargetObserver(environment.target),
            )
        )
        ack_lost = False
        try:
            CentralReconciler(environment.central, environment.central_codec, ack_loss_peer).reconcile("stream-target")
        except TimeoutError:
            ack_lost = True
        recovered_epoch = CentralReconciler(environment.central, environment.central_codec, ack_loss_peer).reconcile("stream-target")
        rows = int(environment.central.read_one("SELECT count(*) FROM dell_journal_records")[0])  # type: ignore[index]
        cases.append(
            {
                "scenario": "RECONNECT_COMMIT_ACK_LOSS",
                "pass": first == "EFFECT_OBSERVED"
                and ack_lost
                and recovered_epoch == 3
                and rows == 3
                and environment.adapter.attempt_count == 1,
                "ack_lost": ack_lost,
                "recovered_epoch": recovered_epoch,
                "journal_rows": rows,
                "synthetic_attempt_count": environment.adapter.attempt_count,
            }
        )
    finally:
        environment.close()

    environment = _environment(project_root, workspace / "det-gap")
    try:
        first = _complete_local_action(environment)
        delegate = DellReconciliationService(
            environment.dell,
            environment.agent_codec,
            FakeTargetObserver(environment.target),
        )
        gap_peer = _GapPeer(delegate, environment.central_codec, environment.agent_codec)
        blocked = ""
        try:
            CentralReconciler(environment.central, environment.central_codec, gap_peer).reconcile("stream-target")
        except CommandBlocked as error:
            blocked = str(error)
        rows = int(environment.central.read_one("SELECT count(*) FROM dell_journal_records")[0])  # type: ignore[index]
        cases.append(
            {
                "scenario": "SIGNED_JOURNAL_GAP_FAILS_CLOSED",
                "pass": first == "EFFECT_OBSERVED"
                and blocked == "DELL_JOURNAL_SEQUENCE_GAP"
                and rows == 0
                and environment.adapter.attempt_count == 1,
                "blocker": blocked,
                "journal_rows": rows,
                "synthetic_attempt_count": environment.adapter.attempt_count,
            }
        )
    finally:
        environment.close()
    return cases


def _serve_loopback(listener: socket.socket, scenario: str, errors: list[str]) -> None:
    try:
        listener.settimeout(1.0)
        connection, _ = listener.accept()
        with connection:
            connection.recv(64)
            if scenario == "ACCEPT_THEN_STALL":
                time.sleep(0.04)
            elif scenario == "PARTIAL_THEN_STALL":
                connection.sendall(b"{")
                time.sleep(0.04)
            elif scenario == "RESET_AFTER_REQUEST":
                connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            elif scenario == "DELAYED_SUCCESS":
                time.sleep(0.002)
                connection.sendall(b"OK\n")
    except BaseException as error:
        errors.append(type(error).__name__)
    finally:
        listener.close()


def _loopback_case(scenario: str) -> tuple[bool, str]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    port = int(listener.getsockname()[1])
    if scenario == "CONNECTION_REFUSED":
        listener.close()
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                return False, "UNEXPECTED_CONNECT"
        except OSError as error:
            return isinstance(error, ConnectionRefusedError) or getattr(error, "errno", None) in {61, 111}, "CONNECTION_REFUSED"
    listener.listen(1)
    errors: list[str] = []
    worker = threading.Thread(target=_serve_loopback, args=(listener, scenario, errors), daemon=True)
    worker.start()
    observed = ""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2) as client:
            client.settimeout(0.015)
            client.sendall(b"probe\n")
            if scenario == "PARTIAL_THEN_STALL":
                first = client.recv(1)
                if first != b"{":
                    observed = "PARTIAL_MISSING"
                else:
                    try:
                        client.recv(1)
                        observed = "PARTIAL_WITHOUT_STALL"
                    except TimeoutError:
                        observed = "PARTIAL_THEN_TIMEOUT"
            else:
                try:
                    value = client.recv(16)
                    observed = "SUCCESS" if value == b"OK\n" else "EOF"
                except TimeoutError:
                    observed = "TIMEOUT"
                except ConnectionResetError:
                    observed = "RESET"
    finally:
        worker.join(timeout=1.0)
    expected = {
        "ACCEPT_THEN_STALL": {"TIMEOUT"},
        "PARTIAL_THEN_STALL": {"PARTIAL_THEN_TIMEOUT"},
        "RESET_AFTER_REQUEST": {"RESET", "EOF"},
        "CLOSE_WITHOUT_REPLY": {"EOF"},
        "DELAYED_SUCCESS": {"SUCCESS"},
    }[scenario]
    return observed in expected and not errors and not worker.is_alive(), observed


def run_loopback_tcp_chaos(*, case_count: int, seed: int) -> dict[str, Any]:
    if isinstance(case_count, bool) or not isinstance(case_count, int) or not len(LOOPBACK_SCENARIOS) <= case_count <= 4096:
        raise ValueError("DELL_LOOPBACK_CASE_COUNT_INVALID")
    rng = random.Random(seed)
    scheduled = list(LOOPBACK_SCENARIOS)
    scheduled.extend(rng.choice(LOOPBACK_SCENARIOS) for _ in range(case_count - len(scheduled)))
    rng.shuffle(scheduled)
    detected: Counter[str] = Counter()
    failures: list[dict[str, Any]] = []
    for index, scenario in enumerate(scheduled):
        passed, observed = _loopback_case(scenario)
        if passed:
            detected[scenario] += 1
        elif len(failures) < 20:
            failures.append({"index": index, "scenario": scenario, "observed": observed})
    return {
        "schema": "cra.dell_loopback_tcp_chaos.v1",
        "bind_address": "127.0.0.1",
        "case_count": case_count,
        "seed": seed,
        "scenario_detection_count": {name: detected[name] for name in LOOPBACK_SCENARIOS},
        "failure_count": case_count - sum(detected.values()),
        "failures": failures,
        "production_network_used": False,
        "pass": sum(detected.values()) == case_count and all(detected[name] > 0 for name in LOOPBACK_SCENARIOS),
    }


def _invalid_variant(environment: _Environment, scenario: str, index: int) -> tuple[LocalRecoveryCandidate, FakeTargetObserver, str]:
    evidence: dict[str, Any] = {
        "tcp_stall_confirmed": True,
        "distinct_sample_count": 3,
        "stall_confirm_threshold": 3,
    }
    observed_at = isoformat_utc(utc_now())
    reason = "confirmed_tcp_stall"
    observer = FakeTargetObserver(environment.target, environment.target)
    expected = "INSUFFICIENT_LOCAL_EVIDENCE"
    if scenario == "MISSING_CONFIRMATION":
        evidence = {}
    elif scenario == "FALSE_CONFIRMATION":
        evidence = {"tcp_stall_confirmed": False, "distinct_sample_count": 3, "stall_confirm_threshold": 3}
    elif scenario == "INTEGER_CONFIRMATION":
        evidence = {"tcp_stall_confirmed": 1, "distinct_sample_count": 3, "stall_confirm_threshold": 3}
    elif scenario == "MISSING_SAMPLE_COUNT":
        evidence = {"tcp_stall_confirmed": True, "stall_confirm_threshold": 3}
    elif scenario == "BOOLEAN_SAMPLE_COUNT":
        evidence = {"tcp_stall_confirmed": True, "distinct_sample_count": True, "stall_confirm_threshold": 3}
    elif scenario == "INSUFFICIENT_SAMPLE_COUNT":
        evidence = {"tcp_stall_confirmed": True, "distinct_sample_count": 2, "stall_confirm_threshold": 3}
    elif scenario == "MISSING_CONFIRM_THRESHOLD":
        evidence = {"tcp_stall_confirmed": True, "distinct_sample_count": 3}
    elif scenario == "BOOLEAN_CONFIRM_THRESHOLD":
        evidence = {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": True}
    elif scenario == "UNSAFE_CONFIRM_THRESHOLD":
        evidence = {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 2}
    elif scenario == "COUNT_BELOW_CONFIRM_THRESHOLD":
        evidence = {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 4}
    elif scenario == "TARGET_NETWORK_DOWN":
        evidence["network_down"] = True
    elif scenario == "REMOTE_WARNING_ONLY":
        evidence["remote_warning_only"] = True
    elif scenario == "OUTCOME_ALREADY_UNKNOWN":
        evidence["outcome_unknown"] = True
    elif scenario == "MAINTENANCE_ACTIVE":
        evidence["maintenance"] = True
    elif scenario == "STALE_EVIDENCE":
        observed_at = isoformat_utc(utc_now() - timedelta(minutes=1))
        expected = "LOCAL_EVIDENCE_STALE"
    elif scenario == "FUTURE_EVIDENCE":
        observed_at = isoformat_utc(utc_now() + timedelta(minutes=1))
        expected = "LOCAL_EVIDENCE_FROM_FUTURE"
    elif scenario == "WRONG_REASON":
        reason = "network_down"
        expected = "LOCAL_ACTION_NOT_ALLOWLISTED"
    elif scenario == "TARGET_DRIFT":
        drift = TargetIdentity.from_dict({**environment.target.to_dict(), "ffmpeg_generation": "generation-b", "ffmpeg_pid": 4200})
        observer = FakeTargetObserver(drift, drift)
        expected = "STALE_TARGET"
    elif scenario == "TARGET_UNAVAILABLE":
        observer = FakeTargetObserver(None, None)
        expected = "TARGET_OBSERVATION_UNAVAILABLE"
    return (
        _candidate(
            environment,
            f"invalid-{index}",
            evidence=evidence,
            observed_at=observed_at,
            reason_code=reason,
        ),
        observer,
        expected,
    )


def run_local_lifecycle_chaos(project_root: Path, workspace: Path, *, case_count: int, seed: int) -> dict[str, Any]:
    if isinstance(case_count, bool) or not isinstance(case_count, int) or not len(INVALID_CANDIDATE_SCENARIOS) <= case_count <= 100_000:
        raise ValueError("DELL_LIFECYCLE_CASE_COUNT_INVALID")
    rng = random.Random(seed)
    scheduled = list(INVALID_CANDIDATE_SCENARIOS)
    scheduled.extend(rng.choice(INVALID_CANDIDATE_SCENARIOS) for _ in range(case_count - len(scheduled)))
    rng.shuffle(scheduled)
    environment = _environment(project_root, workspace / "randomized-lifecycle")
    environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "SYNTHETIC_CRA_LINK_DOWN")
    detected: Counter[str] = Counter()
    failures: list[dict[str, Any]] = []
    try:
        for index, scenario in enumerate(scheduled):
            candidate, observer, expected = _invalid_variant(environment, scenario, index)
            result = _local_service(environment, observer=observer).handle_local_candidate(candidate)
            if result == expected and environment.adapter.attempt_count == 0:
                detected[scenario] += 1
            elif len(failures) < 20:
                failures.append({"index": index, "scenario": scenario, "expected": expected, "actual": result})
        valid = _candidate(environment, "valid-after-invalid-storm")
        valid_result = _local_service(environment).handle_local_candidate(valid)
        exact_replays = [_local_service(environment).handle_local_candidate(valid) for _ in range(64)]
        new_scope_results = [
            _local_service(environment).handle_local_candidate(_candidate(environment, f"post-valid-{index}")) for index in range(64)
        ]
        local_rows = int(environment.dell.read_one("SELECT count(*) FROM local_actions")[0])  # type: ignore[index]
        journal_rows = int(environment.dell.read_one("SELECT count(*) FROM local_action_journal")[0])  # type: ignore[index]
        physical_attempts = int(
            environment.dell.read_one("SELECT coalesce(sum(physical_attempt_count),0) FROM effect_scope_fences")[0]  # type: ignore[index]
        )
        passed = (
            not failures
            and sum(detected.values()) == case_count
            and all(detected[name] > 0 for name in INVALID_CANDIDATE_SCENARIOS)
            and valid_result == "EFFECT_OBSERVED"
            and set(exact_replays) == {"EFFECT_OBSERVED"}
            and set(new_scope_results) == {"HARD_COOLDOWN_ACTIVE"}
            and environment.adapter.attempt_count == physical_attempts == 1
            and local_rows == 1
            and journal_rows == 3
        )
        return {
            "schema": "cra.dell_local_lifecycle_chaos.v1",
            "case_count": case_count,
            "seed": seed,
            "scenario_detection_count": {name: detected[name] for name in INVALID_CANDIDATE_SCENARIOS},
            "failure_count": case_count - sum(detected.values()),
            "failures": failures,
            "valid_result": valid_result,
            "exact_replay_count": len(exact_replays),
            "new_scope_block_count": sum(value == "HARD_COOLDOWN_ACTIVE" for value in new_scope_results),
            "synthetic_adapter_attempt_count": environment.adapter.attempt_count,
            "ledger_physical_attempt_count": physical_attempts,
            "local_action_row_count": local_rows,
            "local_journal_row_count": journal_rows,
            "false_authorization_count": len(failures),
            "pass": passed,
        }
    finally:
        environment.close()


def run_campaign(
    project_root: Path,
    output: Path,
    *,
    lifecycle_cases: int = 10_000,
    loopback_cases: int = 256,
    seed: int = 20260901,
) -> dict[str, Any]:
    started_at = isoformat_utc(utc_now())
    started = time.perf_counter()
    project_root = project_root.resolve()
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    workspace = output / "workspace"
    workspace.mkdir()
    deterministic = _deterministic_cases(project_root, workspace)
    loopback = run_loopback_tcp_chaos(case_count=loopback_cases, seed=seed)
    lifecycle = run_local_lifecycle_chaos(project_root, workspace, case_count=lifecycle_cases, seed=seed)
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    tracked_status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.splitlines()
    source_hashes = {
        str(path.relative_to(project_root)): _sha256(path)
        for path in (
            project_root / "src/cra_harness/dell_failure_domain_chaos.py",
            project_root / "src/cra_harness/runner/compound.py",
            project_root / "src/cra_harness/runner/executor.py",
            project_root / "src/dell_recovery_agent/execution.py",
            project_root / "src/dell_recovery_agent/storage.py",
            project_root / "src/dell_recovery_agent/reconciliation.py",
            project_root / "src/cra_authority/reconciliation.py",
            project_root / "migrations/dell/001_initial.sql",
            project_root / "migrations/central/001_initial.sql",
            project_root / "tests/harness/integration/test_dell_failure_domain_chaos.py",
            project_root / "tests/heartbeat/test_authority_lease.py",
            project_root / "pyproject.toml",
        )
    }
    deterministic_failures = [item for item in deterministic if not item["pass"]]
    summary = {
        "schema": SCHEMA,
        "classification": "PASS" if not deterministic_failures and loopback["pass"] and lifecycle["pass"] else "FAIL",
        "started_at": started_at,
        "finished_at": isoformat_utc(utc_now()),
        "duration_seconds": round(time.perf_counter() - started, 6),
        "host": platform.node(),
        "source_revision": revision,
        "source_worktree_dirty": bool(tracked_status),
        "source_modified_paths": sorted(line[3:] for line in tracked_status),
        "source_hashes": source_hashes,
        "sqlite_version": sqlite3.sqlite_version,
        "seed": seed,
        "case_count": len(deterministic) + loopback_cases + lifecycle_cases + 128,
        "deterministic_case_count": len(deterministic),
        "deterministic_failure_count": len(deterministic_failures),
        "loopback_case_count": loopback_cases,
        "lifecycle_case_count": lifecycle_cases,
        "replay_and_cooldown_case_count": 128,
        "false_authorization_count": lifecycle["false_authorization_count"],
        "synthetic_effect_attempt_count": sum(int(item.get("synthetic_attempt_count", 0)) for item in deterministic)
        + int(lifecycle["synthetic_adapter_attempt_count"]),
        "safety": {
            "production_network_used": False,
            "production_database_used": False,
            "production_effect_socket_used": False,
            "ffmpeg_signal_count": 0,
            "pod_mutation_count": 0,
            "deployment_mutation_count": 0,
            "host_restart_count": 0,
            "physical_effect_count": 0,
        },
    }
    _atomic_json(output / "deterministic_cases.json", deterministic)
    _atomic_json(output / "loopback_tcp_chaos.json", loopback)
    _atomic_json(output / "local_lifecycle_chaos.json", lifecycle)
    _atomic_json(output / "summary.json", summary)
    manifest = {
        "schema": "cra.dell_failure_domain_chaos_manifest.v1",
        "files": {
            name: _sha256(output / name)
            for name in ("deterministic_cases.json", "loopback_tcp_chaos.json", "local_lifecycle_chaos.json", "summary.json")
        },
    }
    _atomic_json(output / "manifest.json", manifest)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bounded Dell-local transport and lifecycle chaos")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--lifecycle-cases", type=int, default=10_000)
    parser.add_argument("--loopback-cases", type=int, default=256)
    parser.add_argument("--seed", type=int, default=20260901)
    args = parser.parse_args()
    summary = run_campaign(
        args.project_root,
        args.output,
        lifecycle_cases=args.lifecycle_cases,
        loopback_cases=args.loopback_cases,
        seed=args.seed,
    )
    print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
    if summary["classification"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
