from __future__ import annotations

import gc
import os
import sqlite3
import threading
import time
import tracemalloc
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from runtime_boundary import EffectClient, EffectExecutorServer, EffectLedger, EffectRequest


def target(pid: int, generation: str) -> dict[str, object]:
    return {
        "host_id": "isolated-dell",
        "host_boot_id": "isolated-boot",
        "namespace": "isolated",
        "pod_uid": "isolated-pod",
        "container_name": "stream-engine",
        "container_id": "containerd://isolated",
        "ffmpeg_generation": generation,
        "ffmpeg_pid": pid,
    }


def request(
    request_id: str,
    *,
    identity: dict[str, object],
    native_generation: str,
    projection_sequence: int,
    correlation_id: str | None = None,
) -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "schema_version": "runtime.effect_request.v1",
        "request_id": request_id,
        "producer_id": "isolated-controller",
        "producer_generation": 1,
        "operation": "restart_ffmpeg",
        "reason": "isolated compound fault",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=4)).isoformat(),
        "target_identity": identity,
        "expected_ffmpeg_generation": native_generation,
        "idempotency_key": request_id,
        "correlation_id": correlation_id or request_id,
        "target_snapshot_id": f"snapshot-{projection_sequence}",
        "runtime_observation_id": f"observation-{projection_sequence}",
        "expected_executor_instance_id": "isolated-executor",
        "maintenance_evidence_status": "AVAILABLE",
        "projection_id": "isolated-projection",
        "projection_sequence": projection_sequence,
    }


def current(identity: dict[str, object], native_generation: str) -> dict[str, object]:
    return {
        "target_identity": identity,
        "ffmpeg_generation": native_generation,
        "executor_instance_id": "isolated-executor",
    }


def wait_status(client: EffectClient, request_id: str, state: str) -> dict[str, Any]:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        value = client.status(request_id)
        if value.get("state") == state:
            return value
        time.sleep(0.01)
    raise AssertionError(f"{request_id} did not reach {state}")


def start_server(
    root: Path,
    ledger: EffectLedger,
    current_target: dict[str, object],
    effect,
) -> tuple[EffectExecutorServer, EffectClient]:
    server = EffectExecutorServer(
        socket_path=root / "effect.sock",
        ledger=ledger,
        allowed_peer_uids={os.getuid()},
        current_target=lambda: current_target,
        perform_effect=effect,
        execute_async=True,
        require_transport_verification=True,
    )
    server.start()
    return server, EffectClient(root / "effect.sock", timeout_seconds=0.5)


def ack_evidence(
    before: dict[str, object],
    after: dict[str, object],
    *,
    sequence: int,
) -> dict[str, object]:
    now = datetime.now(UTC)
    samples = [
        {
            "observed_at": (now - timedelta(milliseconds=20 - offset * 10)).isoformat(),
            "ffmpeg_pid": after["ffmpeg_pid"],
            "ffmpeg_generation": after["ffmpeg_generation"],
            "bytes_acked": sequence * 10_000 + offset * 1000,
        }
        for offset in range(3)
    ]
    return {
        "schema_version": "runtime.delayed_exit_reconciliation_evidence.v2",
        "oracle": "DELAYED_FFMPEG_EXIT_AND_SUCCESSOR_ACK_PROGRESS",
        "observed_at": now.isoformat(),
        "physical_effect_count": 1,
        "automatic_retry_count": 0,
        "before_target": before,
        "observed_target": after,
        "runtime_observation_id": f"successor-observation-{sequence}",
        "transport": {
            "bytes_sent": sequence * 100_000,
            "network_down": False,
            "tcp_probe_ok": True,
            "ffmpeg_generation": after["ffmpeg_generation"],
            "ack_observation_count": 3,
            "bytes_acked_start": samples[0]["bytes_acked"],
            "bytes_acked_end": samples[-1]["bytes_acked"],
            "bytes_acked_delta": 2000,
            "ack_samples": samples,
        },
    }


def test_controller_disconnect_and_restart_replay_never_duplicate_the_effect(tmp_path: Path) -> None:
    path = tmp_path / "ledger.sqlite3"
    identity = target(4100, "protocol-1")
    current_target = current(identity, "native-1")
    ledger = EffectLedger(path, initial_producer_id="isolated-controller", initial_producer_generation=1)
    entered = threading.Event()
    release = threading.Event()
    effects: list[str] = []

    def effect(value) -> dict[str, object]:
        effects.append(value.request_id)
        entered.set()
        assert release.wait(timeout=2)
        return {"physical_effect_count": 1, "exit_observed": True}

    server, client = start_server(tmp_path, ledger, current_target, effect)
    payload = request("controller-disconnect", identity=identity, native_generation="native-1", projection_sequence=1)
    try:
        assert client.execute(payload)["state"] == "EXECUTION_STARTED"
        assert entered.wait(timeout=1)
        # The requesting controller is now absent; the executor owns completion.
        release.set()
        status = wait_status(client, "controller-disconnect", "EFFECT_OBSERVED")
        assert status["effect_scope_state"] == "EFFECT_OBSERVED_AWAITING_VERIFICATION"
    finally:
        release.set()
        server.close()
        ledger.close()

    reopened = EffectLedger(path, initial_producer_id="isolated-controller", initial_producer_generation=1)
    replay_effects: list[str] = []
    server, client = start_server(
        tmp_path,
        reopened,
        current_target,
        lambda value: replay_effects.append(value.request_id) or {"physical_effect_count": 1},
    )
    try:
        replay = client.execute(payload)
        assert replay["replay"] is True
        assert replay["state"] == "EFFECT_OBSERVED"
        assert effects == ["controller-disconnect"]
        assert replay_effects == []
    finally:
        server.close()
        reopened.close()


def test_result_write_loss_after_effect_reopens_as_unknown_without_retry(tmp_path: Path) -> None:
    path = tmp_path / "ledger.sqlite3"
    identity = target(4200, "protocol-1")
    current_target = current(identity, "native-1")
    ledger = EffectLedger(path, initial_producer_id="isolated-controller", initial_producer_generation=1)
    real_transition = ledger.transition
    effects: list[str] = []

    def effect(value) -> dict[str, object]:
        effects.append(value.request_id)
        return {"physical_effect_count": 1, "exit_observed": True}

    def transition_with_terminal_write_loss(request_id: str, **kwargs):
        if kwargs.get("expected") == "EXECUTION_STARTED" and kwargs.get("state") != "EXECUTION_STARTED":
            raise sqlite3.OperationalError("synthetic result write loss")
        return real_transition(request_id, **kwargs)

    ledger.transition = transition_with_terminal_write_loss  # type: ignore[method-assign]
    server, client = start_server(tmp_path, ledger, current_target, effect)
    payload = request("result-write-loss", identity=identity, native_generation="native-1", projection_sequence=1)
    try:
        assert client.execute(payload)["state"] == "EXECUTION_STARTED"
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and effects != ["result-write-loss"]:
            time.sleep(0.01)
        assert effects == ["result-write-loss"]
        assert client.execute(payload)["replay"] is True
        assert effects == ["result-write-loss"]
        scope = ledger.unresolved_scopes()[0]
        assert scope["state"] == "EFFECT_BOUNDARY_REACHED"
        assert scope["physical_attempt_count"] == 1
    finally:
        server.close()
        ledger.close()

    reopened = EffectLedger(path, initial_producer_id="isolated-controller", initial_producer_generation=1)
    try:
        status = reopened.request_status("result-write-loss")
        assert status is not None
        assert status["state"] == "OUTCOME_UNKNOWN"
        assert status["effect_scope_state"] == "OUTCOME_UNKNOWN"
        assert status["physical_attempt_count"] == 1
    finally:
        reopened.close()


def test_sqlite_writer_lock_blocks_before_effect_and_socket_recovers(tmp_path: Path) -> None:
    path = tmp_path / "ledger.sqlite3"
    identity = target(4300, "protocol-1")
    current_target = current(identity, "native-1")
    ledger = EffectLedger(path, initial_producer_id="isolated-controller", initial_producer_generation=1)
    ledger.connection.execute("PRAGMA busy_timeout=25")
    effects: list[str] = []
    server, client = start_server(
        tmp_path,
        ledger,
        current_target,
        lambda value: effects.append(value.request_id) or {"physical_effect_count": 1, "exit_observed": True},
    )
    blocker = sqlite3.connect(path, isolation_level=None, timeout=0.1)
    blocker.execute("PRAGMA busy_timeout=25")
    blocker.execute("BEGIN IMMEDIATE")
    payload = request("sqlite-lock", identity=identity, native_generation="native-1", projection_sequence=1)
    try:
        blocked = client.execute(payload)
        assert blocked["state"] == "OUTCOME_UNKNOWN"
        assert effects == []
        assert ledger.request("sqlite-lock") is None
        blocker.execute("ROLLBACK")
        accepted = client.execute(payload)
        assert accepted["state"] == "EXECUTION_STARTED"
        wait_status(client, "sqlite-lock", "EFFECT_OBSERVED")
        assert effects == ["sqlite-lock"]
    finally:
        if blocker.in_transaction:
            blocker.execute("ROLLBACK")
        blocker.close()
        server.close()
        ledger.close()


def test_sqlite_full_blocks_before_effect_and_exact_request_can_resume(tmp_path: Path) -> None:
    path = tmp_path / "ledger.sqlite3"
    ledger = EffectLedger(path, initial_producer_id="isolated-controller", initial_producer_generation=1)
    initial_pages = int(ledger.connection.execute("PRAGMA page_count").fetchone()[0])
    ledger.connection.execute(f"PRAGMA max_page_count={initial_pages + 2}")
    full_payload: dict[str, object] | None = None
    for sequence in range(1, 32):
        identity = target(4400 + sequence, f"protocol-fill-{sequence}")
        payload = request(
            f"sqlite-fill-{sequence}",
            identity=identity,
            native_generation=f"native-fill-{sequence}",
            projection_sequence=sequence,
        )
        try:
            accepted = ledger.accept(EffectRequest.from_mapping(payload))
        except sqlite3.OperationalError as exc:
            assert "full" in str(exc).lower()
            full_payload = payload
            break
        assert accepted["accepted"] is True
        assert ledger.transition(payload["request_id"], expected="ACCEPTED", state="EXECUTION_STARTED")
        assert ledger.mark_effect_boundary(str(payload["request_id"]))
        assert ledger.transition(
            str(payload["request_id"]),
            expected="EXECUTION_STARTED",
            state="EFFECT_OBSERVED",
            result={"physical_effect_count": 1},
        )
    assert full_payload is not None
    identity = dict(full_payload["target_identity"])
    current_target = current(identity, str(full_payload["expected_ffmpeg_generation"]))
    effects: list[str] = []
    server, client = start_server(
        tmp_path,
        ledger,
        current_target,
        lambda value: effects.append(value.request_id) or {"physical_effect_count": 1, "exit_observed": True},
    )
    try:
        failed = client.execute(full_payload)
        assert failed["state"] == "OUTCOME_UNKNOWN"
        assert effects == []
        assert ledger.request(str(full_payload["request_id"])) is None
        current_pages = int(ledger.connection.execute("PRAGMA page_count").fetchone()[0])
        ledger.connection.execute(f"PRAGMA max_page_count={current_pages + 16}")
        assert client.execute(full_payload)["state"] == "EXECUTION_STARTED"
        wait_status(client, str(full_payload["request_id"]), "EFFECT_OBSERVED")
        assert effects == [full_payload["request_id"]]
    finally:
        server.close()
        ledger.close()


def test_repeated_recovery_keeps_workers_fds_memory_and_wal_bounded(tmp_path: Path) -> None:
    cycle_count = 256
    path = tmp_path / "ledger.sqlite3"
    ledger = EffectLedger(path, initial_producer_id="isolated-controller", initial_producer_generation=1)
    current_identity = target(5000, "protocol-0")
    current_target = current(current_identity, "native-0")
    effects: list[str] = []
    server, client = start_server(
        tmp_path,
        ledger,
        current_target,
        lambda value: effects.append(value.request_id) or {"physical_effect_count": 1, "exit_observed": True},
    )
    baseline_threads = len(threading.enumerate())
    baseline_fds = len(os.listdir("/proc/self/fd"))
    tracemalloc.start()
    baseline_memory = tracemalloc.get_traced_memory()[0]
    try:
        for sequence in range(1, cycle_count + 1):
            before = dict(current_identity)
            payload = request(
                f"dell-target-resource-{sequence}",
                identity=before,
                native_generation=str(current_target["ffmpeg_generation"]),
                projection_sequence=sequence,
                correlation_id=f"fra-resource-{sequence}",
            )
            assert client.execute(payload)["state"] == "EXECUTION_STARTED"
            wait_status(client, str(payload["request_id"]), "EFFECT_OBSERVED")
            correlated = client.status_by_correlation(f"fra-resource-{sequence}")
            assert correlated["ok"] is True
            assert correlated["matching_count"] == 1
            assert correlated["request_id"] == payload["request_id"]
            unresolved = client.unresolved()["unresolved_scopes"]
            assert len(unresolved) == 1
            current_identity = target(5000 + sequence, f"protocol-{sequence}")
            current_target.update(current(current_identity, f"native-{sequence}"))
            response = client.reconcile(
                {
                    "schema_version": "runtime.effect_reconciliation_request.v1",
                    "reconciliation_id": f"resource-reconcile-{sequence}",
                    "effect_scope_id": unresolved[0]["effect_scope_id"],
                    "owner_request_id": unresolved[0]["owner_request_id"],
                    "owner_request_digest": unresolved[0]["owner_request_digest"],
                    "resolution": "EFFECT_OBSERVED",
                    "evidence": ack_evidence(before, current_identity, sequence=sequence),
                }
            )
            assert response["state"] == "RECONCILED_EFFECT_OBSERVED"
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with server._workers_lock:
                if not server._workers:
                    break
            time.sleep(0.01)
        gc.collect()
        retained_memory = tracemalloc.get_traced_memory()[0] - baseline_memory
        assert effects == [f"dell-target-resource-{sequence}" for sequence in range(1, cycle_count + 1)]
        assert client.unresolved()["unresolved_count"] == 0
        with server._workers_lock:
            assert not server._workers
        assert len(threading.enumerate()) <= baseline_threads + 1
        assert len(os.listdir("/proc/self/fd")) <= baseline_fds + 3
        assert retained_memory < 8 * 1024 * 1024
        ledger.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        assert path.stat().st_size < 2 * 1024 * 1024
        wal = path.with_name(f"{path.name}-wal")
        assert not wal.exists() or wal.stat().st_size <= 4096
        rows = int(ledger.connection.execute("SELECT count(*) FROM typed_effect_requests").fetchone()[0])
        assert rows == cycle_count
    finally:
        tracemalloc.stop()
        server.close()
        ledger.close()
