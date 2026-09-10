from __future__ import annotations

import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from cra_dell_recovery.models import TargetIdentity
from runtime_boundary import (
    ESCALATE_RUNTIME_RECOVERY,
    RECONCILE_FFMPEG,
    RESTART_FFMPEG,
    EffectClient,
    EffectExecutorServer,
    EffectLedger,
    EffectOutcomeReconciler,
    EffectRequest,
    OutcomeUnknown,
    RuntimeSnapshotReader,
    TargetSnapshotReader,
)


def target(pid: int = 4242) -> dict[str, object]:
    return {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-1",
        "namespace": "stream-v3",
        "pod_uid": "pod-1",
        "container_name": "stream-engine",
        "container_id": "containerd://container-1",
        "ffmpeg_generation": "protocol-generation-1",
        "ffmpeg_pid": pid,
    }


def request(
    *,
    request_id: str | None = None,
    producer_id: str = "old",
    producer_generation: int = 1,
    target_identity: dict[str, object] | None = None,
    expected_generation: str = "native-1",
    expires_delta: float = 5.0,
    sequence: int = 10,
    maintenance_evidence_status: str = "AVAILABLE",
) -> dict[str, object]:
    now = datetime.now(UTC)
    identity = request_id or f"request-{uuid.uuid4()}"
    return {
        "schema_version": "runtime.effect_request.v1",
        "request_id": identity,
        "producer_id": producer_id,
        "producer_generation": producer_generation,
        "operation": "restart_ffmpeg",
        "reason": "confirmed tcp stall",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=expires_delta)).isoformat(),
        "target_identity": target_identity or target(),
        "expected_ffmpeg_generation": expected_generation,
        "idempotency_key": identity,
        "correlation_id": identity,
        "target_snapshot_id": "target-snapshot-10",
        "runtime_observation_id": "observation-10",
        "expected_executor_instance_id": "executor-1",
        "maintenance_evidence_status": maintenance_evidence_status,
        "projection_id": "projection-10" if maintenance_evidence_status == "AVAILABLE" else "",
        "projection_sequence": sequence,
    }


def runtime_identity() -> dict[str, str]:
    return {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-1",
        "namespace": "stream-v3",
        "pod_uid": "pod-1",
        "stream_engine_container_name": "stream-engine",
        "stream_engine_container_id": "containerd://container-1",
        "runtime_generation": "runtime-generation-1",
    }


def typed_request(
    *,
    intent_type: str = RECONCILE_FFMPEG,
    request_id: str | None = None,
    producer_id: str = "old",
    producer_generation: int = 1,
) -> dict[str, object]:
    now = datetime.now(UTC)
    identity = request_id or f"typed-{uuid.uuid4()}"
    restart = intent_type == RESTART_FFMPEG
    return {
        "schema_version": "runtime.typed_effect_request.v2",
        "request_id": identity,
        "producer_id": producer_id,
        "producer_generation": producer_generation,
        "intent_type": intent_type,
        "reason": "typed policy fixture",
        "failure_domain": "FFMPEG_LOCAL" if intent_type != ESCALATE_RUNTIME_RECOVERY else "RUNTIME_LOCAL",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=5)).isoformat(),
        "ffmpeg_target_identity": target() if restart else None,
        "runtime_identity": None if restart else runtime_identity(),
        "expected_ffmpeg_generation": "native-1" if restart else "",
        "idempotency_key": identity,
        "correlation_id": identity,
        "target_snapshot_id": "target-snapshot-10" if restart else "",
        "runtime_snapshot_id": "runtime-snapshot-10" if not restart else "",
        "runtime_observation_id": "observation-10",
        "expected_executor_instance_id": "executor-1",
        "maintenance_evidence_status": "AVAILABLE",
        "projection_id": "projection-10",
        "projection_sequence": 10,
    }


class Runtime:
    def __init__(self, root: Path, *, effect=None, before_effect=None) -> None:
        self.current = {
            "target_identity": target(),
            "ffmpeg_generation": "native-1",
            "target_snapshot_id": "target-snapshot-10",
            "executor_instance_id": "executor-1",
        }
        self.effects: list[str] = []
        self.ledger = EffectLedger(root / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)
        self.server = EffectExecutorServer(
            socket_path=root / "effect.sock",
            ledger=self.ledger,
            allowed_peer_uids={os.getuid()},
            current_target=lambda: self.current,
            perform_effect=effect or self.perform,
            before_effect=before_effect,
        )
        self.server.start()
        self.client = EffectClient(root / "effect.sock")

    def perform(self, value: EffectRequest) -> dict[str, object]:
        self.effects.append(value.request_id)
        return {"physical_effect_count": 1}

    def close(self) -> None:
        self.server.close()
        self.ledger.close()


def test_rt01_old_producer_and_rt02_new_shadow(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    try:
        assert runtime.client.execute(request())["ok"] is True
        rejected = runtime.client.execute(request(producer_id="new", producer_generation=2))
        assert rejected["reason"] == "PRODUCER_NOT_ACTIVE"
        assert len(runtime.effects) == 1
    finally:
        runtime.close()


def test_rt03_clean_handoff_rt04_old_rejected_and_rt16_rollback(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    try:
        decision = runtime.ledger.switch_authority(
            expected_producer_id="old",
            expected_generation=1,
            new_producer_id="new",
            new_generation=2,
        )
        assert decision.accepted
        assert runtime.client.execute(request(producer_id="old", producer_generation=1))["reason"] == "PRODUCER_NOT_ACTIVE"
        assert runtime.client.execute(request(producer_id="new", producer_generation=2))["ok"] is True
        rollback = runtime.ledger.switch_authority(
            expected_producer_id="new",
            expected_generation=2,
            new_producer_id="old",
            new_generation=3,
        )
        assert rollback.accepted
        assert runtime.ledger.authority()["producer_id"] == "old"
        assert runtime.ledger.authority()["producer_generation"] == 3
    finally:
        runtime.close()


def test_rt05_duplicate_effect_at_most_once(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    value = request(request_id="same")
    try:
        assert runtime.client.execute(value)["ok"] is True
        replay = runtime.client.execute(value)
        assert replay["replay"] is True
        assert len(runtime.effects) == 1
    finally:
        runtime.close()


def test_different_request_ids_for_same_exact_target_share_one_effect_scope(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    try:
        first = runtime.client.execute(request(request_id="scope-first"))
        second = runtime.client.execute(request(request_id="scope-second"))

        assert first["ok"] is True
        assert second["ok"] is False
        assert second["reason"] == "EFFECT_SCOPE_ALREADY_CLAIMED"
        assert second["replay"] is True
        assert runtime.effects == ["scope-first"]
        parsed = EffectRequest.from_mapping(request(request_id="scope-third"))
        scope = runtime.ledger.scope(parsed.effect_scope_id)
        assert scope is not None
        assert scope["physical_attempt_count"] == 1
    finally:
        runtime.close()


def test_outcome_unknown_blocks_new_request_id_for_same_scope(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path, effect=lambda _request: (_ for _ in ()).throw(OutcomeUnknown("uncertain")))
    try:
        first = runtime.client.execute(request(request_id="unknown-first"))
        second = runtime.client.execute(request(request_id="unknown-second"))

        assert first["state"] == "OUTCOME_UNKNOWN"
        assert second["reason"] == "EFFECT_SCOPE_ALREADY_CLAIMED"
        assert second["state"] == "OUTCOME_UNKNOWN"
        assert second["automatic_retry"] is False
    finally:
        runtime.close()


def _delayed_exit_evidence(before: dict[str, object], after: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": "runtime.delayed_exit_reconciliation_evidence.v1",
        "oracle": "DELAYED_FFMPEG_EXIT_AND_HEALTHY_SUCCESSOR",
        "observed_at": datetime.now(UTC).isoformat(),
        "physical_effect_count": 1,
        "automatic_retry_count": 0,
        "before_target": dict(before),
        "observed_target": dict(after),
        "runtime_observation_id": "observation-successor",
        "transport": {"bytes_sent": 8192, "network_down": False, "tcp_probe_ok": True},
    }


def _delayed_exit_ack_evidence(before: dict[str, object], after: dict[str, object]) -> dict[str, object]:
    now = datetime.now(UTC)
    generation = str(after["ffmpeg_generation"])
    pid = int(after["ffmpeg_pid"])
    samples = [
        {
            "observed_at": (now - timedelta(seconds=20 - index * 10)).isoformat(),
            "ffmpeg_pid": pid,
            "ffmpeg_generation": generation,
            "bytes_acked": value,
        }
        for index, value in enumerate((1000, 2000, 3500))
    ]
    return {
        "schema_version": "runtime.delayed_exit_reconciliation_evidence.v2",
        "oracle": "DELAYED_FFMPEG_EXIT_AND_SUCCESSOR_ACK_PROGRESS",
        "observed_at": now.isoformat(),
        "physical_effect_count": 1,
        "automatic_retry_count": 0,
        "before_target": dict(before),
        "observed_target": dict(after),
        "runtime_observation_id": "observation-successor-ack",
        "transport": {
            "bytes_sent": 8192,
            "network_down": False,
            "tcp_probe_ok": True,
            "ffmpeg_generation": generation,
            "ack_observation_count": 3,
            "bytes_acked_start": 1000,
            "bytes_acked_end": 3500,
            "bytes_acked_delta": 2500,
            "ack_samples": samples,
        },
    }


def test_delayed_exit_v2_requires_consecutive_ack_progress_bound_to_successor(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path, effect=lambda _request: (_ for _ in ()).throw(OutcomeUnknown("delayed exit")))
    first_value = request(request_id="delayed-ack-v2")
    first = EffectRequest.from_mapping(first_value)
    try:
        assert runtime.client.execute(first_value)["state"] == "OUTCOME_UNKNOWN"
        unresolved = runtime.client.unresolved()["unresolved_scopes"][0]
        successor = target(5252)
        successor["ffmpeg_generation"] = "protocol-generation-successor"
        runtime.current["target_identity"] = successor
        response = runtime.client.reconcile(
            {
                "schema_version": "runtime.effect_reconciliation_request.v1",
                "reconciliation_id": "reconcile-delayed-ack-v2",
                "effect_scope_id": first.effect_scope_id,
                "owner_request_id": unresolved["owner_request_id"],
                "owner_request_digest": unresolved["owner_request_digest"],
                "resolution": "EFFECT_OBSERVED",
                "evidence": _delayed_exit_ack_evidence(target(), successor),
            }
        )

        assert response["ok"] is True
        assert response["state"] == "RECONCILED_EFFECT_OBSERVED"
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("frozen_ack", "RECONCILIATION_ACK_NOT_CONSECUTIVELY_PROGRESSING"),
        ("generation_drift", "RECONCILIATION_ACK_GENERATION_MISMATCH"),
        ("aggregate_mismatch", "RECONCILIATION_ACK_AGGREGATE_INVALID"),
    ],
)
def test_delayed_exit_v2_rejects_weak_ack_oracle(tmp_path: Path, mutation: str, reason: str) -> None:
    runtime = Runtime(tmp_path, effect=lambda _request: (_ for _ in ()).throw(OutcomeUnknown("delayed exit")))
    first_value = request(request_id=f"delayed-ack-invalid-{mutation}")
    first = EffectRequest.from_mapping(first_value)
    try:
        assert runtime.client.execute(first_value)["state"] == "OUTCOME_UNKNOWN"
        unresolved = runtime.client.unresolved()["unresolved_scopes"][0]
        successor = target(5252)
        successor["ffmpeg_generation"] = "protocol-generation-successor"
        runtime.current["target_identity"] = successor
        evidence = deepcopy(_delayed_exit_ack_evidence(target(), successor))
        transport = evidence["transport"]
        assert isinstance(transport, dict)
        samples = transport["ack_samples"]
        assert isinstance(samples, list)
        if mutation == "frozen_ack":
            samples[1]["bytes_acked"] = samples[0]["bytes_acked"]
        elif mutation == "generation_drift":
            samples[1]["ffmpeg_generation"] = "other-generation"
        else:
            transport["bytes_acked_delta"] = 2499
        response = runtime.client.reconcile(
            {
                "schema_version": "runtime.effect_reconciliation_request.v1",
                "reconciliation_id": f"reconcile-delayed-ack-invalid-{mutation}",
                "effect_scope_id": first.effect_scope_id,
                "owner_request_id": unresolved["owner_request_id"],
                "owner_request_digest": unresolved["owner_request_digest"],
                "resolution": "EFFECT_OBSERVED",
                "evidence": evidence,
            }
        )

        assert response["ok"] is False
        assert response["reason"] == reason
        assert runtime.client.unresolved()["unresolved_count"] == 1
    finally:
        runtime.close()


def _retired_target_evidence(before: dict[str, object], after: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": "runtime.retired_target_reconciliation_evidence.v1",
        "oracle": "EXACT_TARGET_RETIRED_AND_HEALTHY_REPLACEMENT",
        "observed_at": datetime.now(UTC).isoformat(),
        "physical_attempt_count": 1,
        "physical_effect_outcome": "UNKNOWN",
        "automatic_retry_count": 0,
        "before_target": dict(before),
        "observed_target": dict(after),
        "runtime_observation_id": "observation-replacement",
        "transport": {"bytes_sent": 8192, "network_down": False, "tcp_probe_ok": True},
    }


def test_unresolved_query_and_delayed_exit_reconciliation_are_read_only_and_append_only(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path, effect=lambda _request: (_ for _ in ()).throw(OutcomeUnknown("delayed exit")))
    first_value = request(request_id="delayed-query-first")
    first = EffectRequest.from_mapping(first_value)
    try:
        assert runtime.client.execute(first_value)["state"] == "OUTCOME_UNKNOWN"
        queried = runtime.client.unresolved()
        assert queried["unresolved_count"] == 1
        assert queried["automatic_retry"] is False
        unresolved = queried["unresolved_scopes"][0]
        assert unresolved["owner_request_id"] == first.request_id
        assert runtime.effects == []

        successor = target(5252)
        successor["ffmpeg_generation"] = "protocol-generation-successor"
        runtime.current["target_identity"] = successor
        reconciliation = {
            "schema_version": "runtime.effect_reconciliation_request.v1",
            "reconciliation_id": "reconcile-delayed-query-first",
            "effect_scope_id": first.effect_scope_id,
            "owner_request_id": unresolved["owner_request_id"],
            "owner_request_digest": unresolved["owner_request_digest"],
            "resolution": "EFFECT_OBSERVED",
            "evidence": _delayed_exit_evidence(target(), successor),
        }
        reconciled = runtime.client.reconcile(reconciliation)
        assert reconciled["state"] == "RECONCILED_EFFECT_OBSERVED"
        assert reconciled["in_flight_count"] == 0
        assert runtime.client.unresolved()["unresolved_scopes"] == []
        assert runtime.ledger.request(first.request_id)["state"] == "OUTCOME_UNKNOWN"
        assert runtime.ledger.scope(first.effect_scope_id)["state"] == "RECONCILED_EFFECT_OBSERVED"
        assert runtime.ledger.connection.execute("SELECT count(*) FROM effect_reconciliations").fetchone()[0] == 1

        exact_replay = runtime.client.reconcile(reconciliation)
        assert exact_replay["replay"] is True
        conflicting_evidence = _delayed_exit_evidence(target(), successor)
        conflicting_evidence["transport"]["bytes_sent"] = 8193  # type: ignore[index]
        replay = runtime.client.reconcile({**reconciliation, "evidence": conflicting_evidence})
        # A fresh timestamp is different evidence and must conflict instead of
        # manufacturing a second append for the same reconciliation identity.
        assert replay["ok"] is False
        assert replay["reason"] == "RECONCILIATION_ID_CONFLICT"
        assert runtime.ledger.connection.execute("SELECT count(*) FROM effect_reconciliations").fetchone()[0] == 1
    finally:
        runtime.close()


def test_retired_exact_target_ends_global_block_without_claiming_effect_outcome(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path, effect=lambda _request: (_ for _ in ()).throw(OutcomeUnknown("delayed exit")))
    first_value = request(request_id="retired-target-first")
    first = EffectRequest.from_mapping(first_value)
    try:
        assert runtime.client.execute(first_value)["state"] == "OUTCOME_UNKNOWN"
        unresolved = runtime.client.unresolved()["unresolved_scopes"][0]
        replacement = {
            **target(6262),
            "pod_uid": "pod-2",
            "container_id": "containerd://container-2",
            "ffmpeg_generation": "protocol-generation-replacement",
        }
        runtime.current["target_identity"] = replacement
        reconciliation = {
            "schema_version": "runtime.effect_reconciliation_request.v1",
            "reconciliation_id": "retire-old-exact-target",
            "effect_scope_id": first.effect_scope_id,
            "owner_request_id": unresolved["owner_request_id"],
            "owner_request_digest": unresolved["owner_request_digest"],
            "resolution": "TARGET_RETIRED",
            "evidence": _retired_target_evidence(target(), replacement),
        }

        retired = runtime.client.reconcile(reconciliation)

        assert retired["state"] == "RETIRED_TARGET_OUTCOME_UNKNOWN"
        assert retired["in_flight_count"] == 0
        assert runtime.client.unresolved()["unresolved_scopes"] == []
        assert runtime.ledger.request(first.request_id)["state"] == "OUTCOME_UNKNOWN"
        scope = runtime.ledger.scope(first.effect_scope_id)
        assert scope is not None
        assert scope["state"] == "RETIRED_TARGET_OUTCOME_UNKNOWN"
        assert scope["physical_attempt_count"] == 1
        assert runtime.ledger.raw_unresolved_count() == 1
        assert runtime.ledger.connection.execute("SELECT count(*) FROM effect_scope_retirements").fetchone()[0] == 1
        assert runtime.ledger.connection.execute("SELECT count(*) FROM effect_reconciliations").fetchone()[0] == 0
        assert runtime.client.reconcile(reconciliation)["replay"] is True
    finally:
        runtime.close()


def test_target_retirement_is_concurrent_replay_safe_and_durable_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "ledger.sqlite3"
    runtime = Runtime(tmp_path, effect=lambda _request: (_ for _ in ()).throw(OutcomeUnknown("delayed exit")))
    first_value = request(request_id="retired-target-concurrent")
    first = EffectRequest.from_mapping(first_value)
    assert runtime.client.execute(first_value)["state"] == "OUTCOME_UNKNOWN"
    unresolved = runtime.client.unresolved()["unresolved_scopes"][0]
    runtime.close()

    evidence = _retired_target_evidence(
        target(),
        {
            **target(6262),
            "pod_uid": "pod-2",
            "container_id": "containerd://container-2",
            "ffmpeg_generation": "protocol-generation-replacement",
        },
    )
    ledgers = [EffectLedger(path, initial_producer_id="old", initial_producer_generation=1, allow_initialize=False) for _ in range(2)]

    def retire(ledger: EffectLedger) -> dict[str, object]:
        return ledger.record_target_retirement(
            reconciliation_id="retire-concurrent-exact-replay",
            effect_scope_id=first.effect_scope_id,
            owner_request_id=unresolved["owner_request_id"],
            owner_request_digest=unresolved["owner_request_digest"],
            evidence=evidence,
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(retire, ledgers))
        assert sorted(bool(result["replay"]) for result in results) == [False, True]
        assert ledgers[0].connection.execute("SELECT count(*) FROM effect_scope_retirements").fetchone()[0] == 1
    finally:
        for ledger in ledgers:
            ledger.close()

    reopened = EffectLedger(path, initial_producer_id="old", initial_producer_generation=1, allow_initialize=False)
    try:
        assert reopened.unresolved_count() == 0
        assert reopened.raw_unresolved_count() == 1
        assert reopened.request(first.request_id)["state"] == "OUTCOME_UNKNOWN"
        assert reopened.scope(first.effect_scope_id)["state"] == "RETIRED_TARGET_OUTCOME_UNKNOWN"
    finally:
        reopened.close()


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        (
            lambda value: value["evidence"].update(observed_target=target()),
            "RECONCILIATION_OBSERVED_TARGET_NOT_CURRENT",
        ),
        (
            lambda value: value["evidence"]["observed_target"].update(host_id="other-host"),
            "RECONCILIATION_OBSERVED_TARGET_NOT_CURRENT",
        ),
        (
            lambda value: value["evidence"]["transport"].update(tcp_probe_ok=False),
            "RECONCILIATION_TRANSPORT_NOT_HEALTHY",
        ),
        (
            lambda value: value["evidence"].update(physical_effect_outcome="EFFECT_OBSERVED"),
            "RECONCILIATION_ATTEMPT_COUNT_INVALID",
        ),
    ],
)
def test_retired_target_reconciliation_negative_controls_fail_closed(
    tmp_path: Path,
    mutation,
    expected: str,
) -> None:
    runtime = Runtime(tmp_path, effect=lambda _request: (_ for _ in ()).throw(OutcomeUnknown("delayed exit")))
    first_value = request(request_id="retired-target-negative")
    first = EffectRequest.from_mapping(first_value)
    try:
        runtime.client.execute(first_value)
        unresolved = runtime.client.unresolved()["unresolved_scopes"][0]
        replacement = {
            **target(6262),
            "pod_uid": "pod-2",
            "container_id": "containerd://container-2",
            "ffmpeg_generation": "protocol-generation-replacement",
        }
        runtime.current["target_identity"] = replacement
        value = {
            "schema_version": "runtime.effect_reconciliation_request.v1",
            "reconciliation_id": "retire-old-exact-target-negative",
            "effect_scope_id": first.effect_scope_id,
            "owner_request_id": unresolved["owner_request_id"],
            "owner_request_digest": unresolved["owner_request_digest"],
            "resolution": "TARGET_RETIRED",
            "evidence": _retired_target_evidence(target(), replacement),
        }
        mutation(value)

        rejected = runtime.client.reconcile(value)

        assert rejected["ok"] is False
        assert rejected["reason"] == expected
        assert runtime.ledger.unresolved_count() == 1
        assert runtime.ledger.connection.execute("SELECT count(*) FROM effect_scope_retirements").fetchone()[0] == 0
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        (lambda value: value.update(owner_request_digest="0" * 64), "RECONCILIATION_OWNER_DIGEST_MISMATCH"),
        (
            lambda value: value["evidence"]["transport"].update(bytes_sent=0),
            "RECONCILIATION_TRANSPORT_NOT_HEALTHY",
        ),
        (
            lambda value: value["evidence"]["observed_target"].update(pod_uid="different-pod"),
            "RECONCILIATION_OBSERVED_TARGET_NOT_CURRENT",
        ),
    ],
)
def test_delayed_exit_reconciliation_negative_controls_fail_closed(
    tmp_path: Path,
    mutation,
    expected: str,
) -> None:
    runtime = Runtime(tmp_path, effect=lambda _request: (_ for _ in ()).throw(OutcomeUnknown("delayed exit")))
    first_value = request(request_id="delayed-negative")
    first = EffectRequest.from_mapping(first_value)
    try:
        runtime.client.execute(first_value)
        unresolved = runtime.client.unresolved()["unresolved_scopes"][0]
        successor = target(5252)
        successor["ffmpeg_generation"] = "protocol-generation-successor"
        runtime.current["target_identity"] = successor
        value = {
            "schema_version": "runtime.effect_reconciliation_request.v1",
            "reconciliation_id": "reconcile-delayed-negative",
            "effect_scope_id": first.effect_scope_id,
            "owner_request_id": unresolved["owner_request_id"],
            "owner_request_digest": unresolved["owner_request_digest"],
            "resolution": "EFFECT_OBSERVED",
            "evidence": _delayed_exit_evidence(target(), successor),
        }
        mutation(value)
        rejected = runtime.client.reconcile(value)
        assert rejected["ok"] is False
        assert rejected["reason"] == expected
        assert runtime.ledger.unresolved_count() == 1
        assert runtime.ledger.connection.execute("SELECT count(*) FROM effect_reconciliations").fetchone()[0] == 0
    finally:
        runtime.close()


def test_new_ffmpeg_generation_creates_a_new_effect_scope(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    try:
        assert runtime.client.execute(request(request_id="generation-one"))["ok"] is True
        new_target = target(5252)
        new_target["ffmpeg_generation"] = "protocol-generation-2"
        runtime.current["target_identity"] = new_target
        runtime.current["ffmpeg_generation"] = "native-2"
        assert (
            runtime.client.execute(
                request(
                    request_id="generation-two",
                    target_identity=new_target,
                    expected_generation="native-2",
                )
            )["ok"]
            is True
        )
        assert runtime.effects == ["generation-one", "generation-two"]
    finally:
        runtime.close()


def test_same_logical_generation_with_different_pid_is_fail_closed(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    try:
        assert runtime.client.execute(request(request_id="generation-pid-first"))["ok"] is True
        changed_pid = target(5252)
        runtime.current["target_identity"] = changed_pid

        rejected = runtime.client.execute(request(request_id="generation-pid-second", target_identity=changed_pid))

        assert rejected["ok"] is False
        assert rejected["reason"] == "LOGICAL_GENERATION_PID_INVARIANT_BROKEN"
        assert rejected["automatic_retry"] is False
        assert runtime.effects == ["generation-pid-first"]
    finally:
        runtime.close()


def test_rt06_restart_before_accept_and_rt13_controller_restart(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    try:
        assert runtime.client.execute(request(request_id="after-controller-restart"))["ok"] is True
        assert runtime.ledger.unresolved_count() == 0
    finally:
        runtime.close()


def test_rt07_restart_after_accept_executes_once(tmp_path: Path) -> None:
    ledger = EffectLedger(tmp_path / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)
    value = request(request_id="accepted-before-restart")
    assert ledger.accept(EffectRequest.from_mapping(value))["accepted"] is True
    ledger.close()
    runtime = Runtime(tmp_path)
    try:
        assert runtime.client.execute(value)["ok"] is True
        assert runtime.effects == ["accepted-before-restart"]
    finally:
        runtime.close()


def test_rt08_crash_after_started_becomes_outcome_unknown(tmp_path: Path) -> None:
    path = tmp_path / "ledger.sqlite3"
    ledger = EffectLedger(path, initial_producer_id="old", initial_producer_generation=1)
    value = EffectRequest.from_mapping(request(request_id="started"))
    assert ledger.accept(value)["accepted"] is True
    assert ledger.transition("started", expected="ACCEPTED", state="EXECUTION_STARTED")
    ledger.close()
    reconstructed = EffectLedger(path, initial_producer_id="old", initial_producer_generation=1)
    try:
        assert reconstructed.request("started")["state"] == "OUTCOME_UNKNOWN"
        assert reconstructed.unresolved_count() == 1
    finally:
        reconstructed.close()


def test_rt09_target_change_rt10_generation_mismatch_and_expired_request(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    try:
        runtime.current["target_identity"] = target(5000)
        assert runtime.client.execute(request())["reason"] == "TARGET_IDENTITY_DRIFT"
        runtime.current["target_identity"] = target()
        assert runtime.client.execute(request(expected_generation="native-old"))["reason"] == "FFMPEG_GENERATION_DRIFT"
        assert runtime.client.execute(request(expires_delta=-1))["reason"] == "EXPIRY_NOT_AFTER_ISSUE"
        runtime.current = {}
        assert runtime.client.execute(request())["reason"] == "TARGET_IDENTITY_DRIFT"
        assert runtime.effects == []
    finally:
        runtime.close()


def test_request_lifetime_is_narrow_and_future_issue_is_rejected(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    try:
        assert runtime.client.execute(request(expires_delta=5.1))["reason"] == "REQUEST_LIFETIME_TOO_LONG"
        value = request()
        future = datetime.now(UTC) + timedelta(seconds=1)
        value["issued_at"] = future.isoformat()
        value["expires_at"] = (future + timedelta(seconds=5)).isoformat()
        assert runtime.client.execute(value)["reason"] == "REQUEST_ISSUED_IN_FUTURE"
        assert runtime.effects == []
    finally:
        runtime.close()


def test_production_ledger_requires_explicit_initialization(tmp_path: Path) -> None:
    path = tmp_path / "ledger.sqlite3"
    with pytest.raises(RuntimeError, match="LEDGER_MISSING_INITIALIZATION_REQUIRED"):
        EffectLedger(path, initial_producer_id="old", initial_producer_generation=1, allow_initialize=False)
    initialized = EffectLedger(path, initial_producer_id="old", initial_producer_generation=1, allow_initialize=True)
    initialized.close()
    reopened = EffectLedger(path, initial_producer_id="old", initial_producer_generation=1, allow_initialize=False)
    try:
        assert reopened.authority()["producer_id"] == "old"
    finally:
        reopened.close()


def test_executor_refuses_to_replace_non_socket_path(tmp_path: Path) -> None:
    protected = tmp_path / "effect.sock"
    protected.write_text("must survive", encoding="utf-8")
    ledger = EffectLedger(tmp_path / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)
    server = EffectExecutorServer(
        socket_path=protected,
        ledger=ledger,
        allowed_peer_uids={os.getuid()},
        current_target=lambda: {},
        perform_effect=lambda _request: {},
    )
    try:
        with pytest.raises(RuntimeError, match="EFFECT_SOCKET_PATH_NOT_SOCKET"):
            server.start()
        assert protected.read_text(encoding="utf-8") == "must survive"
    finally:
        server.close()
        ledger.close()


@pytest.mark.parametrize("status", ["STALE", "UNKNOWN"])
def test_rt11_stale_and_rt12_missing_maintenance_snapshot_do_not_change_effect_decision(
    tmp_path: Path,
    status: str,
) -> None:
    runtime = Runtime(tmp_path)
    try:
        response = runtime.client.execute(request(sequence=0, maintenance_evidence_status=status))
        assert response["ok"] is True
        assert runtime.effects
    finally:
        runtime.close()


def test_rt14_executor_restart_and_rt15_handoff_reconstruction(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    decision = runtime.ledger.switch_authority(
        expected_producer_id="old",
        expected_generation=1,
        new_producer_id="new",
        new_generation=2,
    )
    assert decision.accepted
    runtime.close()
    reconstructed = Runtime(tmp_path)
    try:
        authority = reconstructed.ledger.authority()
        assert authority["producer_id"] == "new"
        assert authority["producer_generation"] == 2
        assert reconstructed.client.execute(request(producer_id="new", producer_generation=2))["ok"] is True
    finally:
        reconstructed.close()


def test_handoff_rejected_while_unresolved(tmp_path: Path) -> None:
    ledger = EffectLedger(tmp_path / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)
    try:
        value = EffectRequest.from_mapping(request(request_id="unresolved"))
        assert ledger.accept(value)["accepted"] is True
        decision = ledger.switch_authority(
            expected_producer_id="old",
            expected_generation=1,
            new_producer_id="new",
            new_generation=2,
        )
        assert not decision.accepted
        assert decision.reason == "UNRESOLVED_EXECUTION_EXISTS"
    finally:
        ledger.close()


def test_outcome_unknown_has_no_automatic_retry(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path, effect=lambda _request: (_ for _ in ()).throw(OutcomeUnknown("uncertain")))
    value = request(request_id="unknown")
    try:
        response = runtime.client.execute(value)
        assert response["state"] == "OUTCOME_UNKNOWN"
        assert response["automatic_retry"] is False
        replay = runtime.client.execute(value)
        assert replay["state"] == "OUTCOME_UNKNOWN"
        assert runtime.ledger.request("unknown")["state"] == "OUTCOME_UNKNOWN"
    finally:
        runtime.close()


def test_delayed_child_success_at_35_seconds_reconciles_first_attempt_without_resend(tmp_path: Path) -> None:
    ledger = EffectLedger(tmp_path / "delayed.sqlite3", initial_producer_id="old", initial_producer_generation=1)
    first = EffectRequest.from_mapping(request(request_id="delayed-first"))
    assert ledger.accept(first)["accepted"] is True
    assert ledger.transition(first.request_id, expected="ACCEPTED", state="EXECUTION_STARTED")
    assert ledger.mark_effect_boundary(first.request_id)
    assert ledger.transition(first.request_id, expected="EXECUTION_STARTED", state="OUTCOME_UNKNOWN")

    class Clock:
        value = 0.0

        def now(self) -> float:
            return self.value

        def wait(self, seconds: float) -> None:
            self.value += seconds

    clock = Clock()
    successor = target(5252)
    successor["ffmpeg_generation"] = "protocol-generation-2"
    reconciler = EffectOutcomeReconciler(
        ledger,
        lambda: successor if clock.value >= 35 else target(),
        monotonic=clock.now,
        wait=clock.wait,
    )
    try:
        result = reconciler.reconcile(
            reconciliation_id="delayed-reconciliation",
            effect_scope_id=first.effect_scope_id,
            before_target=TargetIdentity.from_dict(target()),
            deadline_seconds=40,
            poll_interval_seconds=5,
        )

        assert result == "EFFECT_OBSERVED"
        assert clock.value == 35
        assert ledger.raw_unresolved_count() == 1
        assert ledger.unresolved_count() == 0
        assert ledger.scope(first.effect_scope_id)["physical_attempt_count"] == 1
        second = ledger.accept(EffectRequest.from_mapping(request(request_id="delayed-second")))
        assert second["reason"] == "EFFECT_SCOPE_ALREADY_CLAIMED"
    finally:
        ledger.close()


def test_outcome_unknown_blocks_a_new_generation_until_append_only_reconciliation(tmp_path: Path) -> None:
    ledger = EffectLedger(tmp_path / "target-wide.sqlite3", initial_producer_id="old", initial_producer_generation=1)
    first = EffectRequest.from_mapping(request(request_id="unknown-generation-a"))
    try:
        assert ledger.accept(first)["accepted"] is True
        assert ledger.transition(first.request_id, expected="ACCEPTED", state="EXECUTION_STARTED")
        assert ledger.mark_effect_boundary(first.request_id)
        assert ledger.transition(first.request_id, expected="EXECUTION_STARTED", state="OUTCOME_UNKNOWN")

        successor = target(5252)
        successor["ffmpeg_generation"] = "protocol-generation-2"
        second = EffectRequest.from_mapping(
            request(
                request_id="new-generation-b",
                target_identity=successor,
                expected_generation="native-2",
                sequence=11,
            )
        )
        blocked = ledger.accept(second)

        assert blocked["accepted"] is False
        assert blocked["reason"] == "TARGET_EFFECT_UNRESOLVED"
        assert blocked["owner_request_id"] == first.request_id
        assert blocked["state"] == "OUTCOME_UNKNOWN"
        assert ledger.request(second.request_id) is None
        assert ledger.scope(first.effect_scope_id)["physical_attempt_count"] == 1
    finally:
        ledger.close()


def test_audit_hook_failure_does_not_change_effect_decision(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path, before_effect=lambda _request: (_ for _ in ()).throw(RuntimeError("audit failed")))
    try:
        response = runtime.client.execute(request(request_id="audit-failure"))
        assert response["ok"] is True
        assert response["result"]["audit_hook_failure"] is True
        assert response["result"]["production_behavior_modified_by_audit"] is False
        assert runtime.effects == ["audit-failure"]
    finally:
        runtime.close()


def test_ledger_single_connection_access_is_serialized(tmp_path: Path) -> None:
    runtime = Runtime(tmp_path)
    try:
        values = [request(request_id=f"concurrent-{index}") for index in range(24)]

        def exercise(index: int) -> object:
            if index % 3 == 0:
                return runtime.ledger.authority()["authority_version"]
            if index % 3 == 1:
                return runtime.ledger.unresolved_count()
            return runtime.client.execute(values[index])["state"]

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(exercise, range(24)))

        assert len(results) == 24
        assert runtime.ledger.unresolved_count() == 0
        # All eight requests carry the same exact target identity.  Request IDs
        # differ, but the physical-effect fence is target-wide.
        assert len(runtime.effects) == 1
    finally:
        runtime.close()


def test_target_snapshot_reader_requires_fresh_exact_identity(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    path = tmp_path / "target.json"
    payload = {
        "schema": "cra_dell_recovery.target_snapshot.v1",
        "status": "VALID",
        "reason_code": "SNAPSHOT_CONSISTENT",
        "snapshot_id": "snapshot-1",
        "observed_at": (now - timedelta(seconds=1)).isoformat(),
        "valid_until": (now + timedelta(seconds=5)).isoformat(),
        "target_identity": target(),
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert TargetSnapshotReader(path).read(now=now).available

    payload["valid_until"] = (now - timedelta(seconds=1)).isoformat()
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert TargetSnapshotReader(path).read(now=now).reason == "TARGET_SNAPSHOT_STALE"

    payload["valid_until"] = (now + timedelta(seconds=5)).isoformat()
    del payload["target_identity"]["host_boot_id"]
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert TargetSnapshotReader(path).read(now=now).reason == "TARGET_IDENTITY_NOT_EXACT"


def test_runtime_boundary_contract_schemas_accept_protocol_examples() -> None:
    root = Path(__file__).resolve().parents[2] / "contracts" / "runtime_boundary"
    request_schema = json.loads((root / "effect_request.v1.schema.json").read_text(encoding="utf-8"))
    response_schema = json.loads((root / "effect_response.v1.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator(request_schema, format_checker=Draft202012Validator.FORMAT_CHECKER).validate(request())
    Draft202012Validator(response_schema).validate(
        {
            "schema_version": "runtime.effect_response.v1",
            "ok": False,
            "reason": "OUTCOME_UNKNOWN",
            "state": "OUTCOME_UNKNOWN",
            "request_id": "request-1",
            "correlation_id": "request-1",
            "replay": False,
            "result": None,
            "active_authority": {"producer_id": "old", "producer_generation": 1},
            "in_flight_count": 1,
            "automatic_retry": False,
        }
    )


@pytest.mark.parametrize("operation", ["kill", "signal", "systemctl", "kubectl"])
def test_arbitrary_operations_rejected(operation: str) -> None:
    value = request()
    value["operation"] = operation
    with pytest.raises(ValueError, match="OPERATION_NOT_ALLOWED"):
        EffectRequest.from_mapping(value)


def test_typed_contract_requires_identity_by_intent() -> None:
    reconcile = typed_request()
    assert EffectRequest.from_mapping(reconcile).identity_type == "RUNTIME"
    reconcile["runtime_identity"] = None
    with pytest.raises(ValueError, match="RUNTIME_IDENTITY_NOT_EXACT"):
        EffectRequest.from_mapping(reconcile)

    restart = typed_request(intent_type=RESTART_FFMPEG)
    restart["ffmpeg_target_identity"] = None
    with pytest.raises(ValueError, match="TARGET_IDENTITY_NOT_EXACT"):
        EffectRequest.from_mapping(restart)


def test_reconcile_is_durable_idempotent_and_executor_scoped(tmp_path: Path) -> None:
    effects: list[str] = []
    ledger = EffectLedger(tmp_path / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)
    current = {
        "runtime_identity": runtime_identity(),
        "executor_instance_id": "executor-1",
    }
    server = EffectExecutorServer(
        socket_path=tmp_path / "effect.sock",
        ledger=ledger,
        allowed_peer_uids={os.getuid()},
        current_target=lambda: current,
        perform_effect=lambda item: effects.append(item.request_id) or {"physical_effect_count": 1},
        allowed_intents={RESTART_FFMPEG, RECONCILE_FFMPEG},
    )
    server.start()
    client = EffectClient(tmp_path / "effect.sock")
    value = typed_request(request_id="reconcile-once")
    try:
        assert client.execute(value)["ok"] is True
        replay = client.execute(value)
        assert replay["replay"] is True
        assert effects == ["reconcile-once"]
        row = ledger.request("reconcile-once")
        assert row is not None
        assert row["intent_type"] == RECONCILE_FFMPEG
        assert row["state"] == "EFFECT_OBSERVED"

        escalation = client.execute(typed_request(intent_type=ESCALATE_RUNTIME_RECOVERY))
        assert escalation["reason"] == "INTENT_NOT_OWNED_BY_EXECUTOR"
        assert effects == ["reconcile-once"]
    finally:
        server.close()
        ledger.close()


def test_typed_started_reconstructs_to_outcome_unknown_without_retry(tmp_path: Path) -> None:
    path = tmp_path / "ledger.sqlite3"
    ledger = EffectLedger(path, initial_producer_id="old", initial_producer_generation=1)
    value = EffectRequest.from_mapping(typed_request(request_id="typed-started"))
    assert ledger.accept(value)["accepted"] is True
    assert ledger.transition("typed-started", expected="ACCEPTED", state="EXECUTION_STARTED")
    ledger.close()
    reconstructed = EffectLedger(path, initial_producer_id="old", initial_producer_generation=1)
    try:
        row = reconstructed.request("typed-started")
        assert row is not None
        assert row["state"] == "OUTCOME_UNKNOWN"
        replay = reconstructed.accept(value)
        assert replay["accepted"] is False
        assert replay["state"] == "OUTCOME_UNKNOWN"
    finally:
        reconstructed.close()


def test_runtime_snapshot_reader_accepts_runtime_when_ffmpeg_target_is_invalid(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    path = tmp_path / "target.json"
    path.write_text(
        json.dumps(
            {
                "schema": "cra_dell_recovery.target_snapshot.v1",
                "status": "INVALID",
                "reason_code": "FFMPEG_PROCESS_CARDINALITY",
                "target_identity": None,
                "runtime_status": "VALID",
                "runtime_reason_code": "RUNTIME_SNAPSHOT_CONSISTENT",
                "runtime_snapshot_id": "runtime-snapshot-1",
                "runtime_identity": runtime_identity(),
                "runtime_container_ready": True,
                "observed_at": (now - timedelta(seconds=1)).isoformat(),
                "valid_until": (now + timedelta(seconds=5)).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    decision = RuntimeSnapshotReader(path).read(now=now)
    assert decision.available
    assert decision.runtime_identity == runtime_identity()


def test_typed_schema_accepts_each_intent() -> None:
    root = Path(__file__).resolve().parents[2] / "contracts" / "runtime_boundary"
    schema = json.loads((root / "typed_effect_request.v2.schema.json").read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER)
    for intent in (RESTART_FFMPEG, RECONCILE_FFMPEG, ESCALATE_RUNTIME_RECOVERY):
        validator.validate(typed_request(intent_type=intent))
