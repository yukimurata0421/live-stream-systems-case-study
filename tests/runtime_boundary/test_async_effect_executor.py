from __future__ import annotations

import json
import os
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from runtime_boundary import EffectClient, EffectExecutorServer, EffectLedger, OutcomeUnknown


def target() -> dict[str, object]:
    return {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-1",
        "namespace": "stream-v3",
        "pod_uid": "pod-1",
        "container_name": "stream-engine",
        "container_id": "containerd://container-1",
        "ffmpeg_generation": "protocol-generation-1",
        "ffmpeg_pid": 4242,
    }


def request(request_id: str) -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "schema_version": "runtime.effect_request.v1",
        "request_id": request_id,
        "producer_id": "old",
        "producer_generation": 1,
        "operation": "restart_ffmpeg",
        "reason": "confirmed tcp stall",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=5)).isoformat(),
        "target_identity": target(),
        "expected_ffmpeg_generation": "native-1",
        "idempotency_key": request_id,
        "correlation_id": request_id,
        "target_snapshot_id": "target-snapshot-1",
        "runtime_observation_id": "observation-1",
        "expected_executor_instance_id": "executor-1",
        "maintenance_evidence_status": "AVAILABLE",
        "projection_id": "projection-1",
        "projection_sequence": 1,
    }


def current_target() -> dict[str, object]:
    return {
        "target_identity": target(),
        "ffmpeg_generation": "native-1",
        "executor_instance_id": "executor-1",
    }


def wait_for_state(ledger: EffectLedger, request_id: str, state: str) -> dict[str, object]:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        row = ledger.request(request_id)
        if row is not None and row["state"] == state:
            return row
        time.sleep(0.01)
    raise AssertionError(f"request {request_id} did not reach {state}")


def test_async_executor_returns_durable_acceptance_then_replays_terminal_result(tmp_path: Path) -> None:
    ledger = EffectLedger(tmp_path / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)
    entered = threading.Event()
    release = threading.Event()
    effects: list[str] = []

    def effect(value) -> dict[str, object]:
        effects.append(value.request_id)
        entered.set()
        assert release.wait(timeout=2)
        return {
            "physical_effect_count": 1,
            "effect": "SIGTERM",
            "exit_observed": True,
        }

    server = EffectExecutorServer(
        socket_path=tmp_path / "effect.sock",
        ledger=ledger,
        allowed_peer_uids={os.getuid()},
        current_target=current_target,
        perform_effect=effect,
        execute_async=True,
        require_transport_verification=True,
    )
    server.start()
    client = EffectClient(tmp_path / "effect.sock", timeout_seconds=0.5)
    payload = request("async-success")
    try:
        first = client.execute(payload)
        assert first["ok"] is True
        assert first["state"] == "EXECUTION_STARTED"
        assert first["reason"] == "EFFECT_ACCEPTED_EXECUTION_STARTED"
        assert entered.wait(timeout=0.5)

        replay_while_running = client.execute(payload)
        assert replay_while_running["replay"] is True
        assert replay_while_running["state"] == "EXECUTION_STARTED"
        assert effects == ["async-success"]

        release.set()
        row = wait_for_state(ledger, "async-success", "EFFECT_OBSERVED")
        row_result = json.loads(str(row["result_json"]))
        assert row_result["effect"] == "SIGTERM"
        terminal = client.execute(payload)
        assert terminal["state"] == "EFFECT_OBSERVED"
        assert terminal["result"]["exit_observed"] is True
        status = client.status("async-success")
        assert status["ok"] is True
        assert status["state"] == "EFFECT_OBSERVED"
        assert status["effect_scope_state"] == "EFFECT_OBSERVED_AWAITING_VERIFICATION"
        assert status["physical_attempt_count"] == 1
        assert status["result"]["exit_observed"] is True
        unresolved = client.unresolved()
        assert unresolved["unresolved_count"] == 1
        assert unresolved["unresolved_scopes"][0]["state"] == "EFFECT_OBSERVED_AWAITING_VERIFICATION"
        assert effects == ["async-success"]
    finally:
        release.set()
        server.close()
        ledger.close()


def test_async_outcome_unknown_preserves_structured_physical_result(tmp_path: Path) -> None:
    ledger = EffectLedger(tmp_path / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)

    def effect(_value):
        raise OutcomeUnknown(
            "SIGTERM_SENT_EXIT_NOT_OBSERVED",
            result={
                "physical_effect_count": 1,
                "signal_attempt_count": 1,
                "exit_observed": False,
                "process_still_running_after_dispatch": True,
            },
        )

    server = EffectExecutorServer(
        socket_path=tmp_path / "effect.sock",
        ledger=ledger,
        allowed_peer_uids={os.getuid()},
        current_target=current_target,
        perform_effect=effect,
        execute_async=True,
        require_transport_verification=True,
    )
    server.start()
    client = EffectClient(tmp_path / "effect.sock")
    payload = request("async-unknown")
    try:
        assert client.execute(payload)["state"] == "EXECUTION_STARTED"
        row = wait_for_state(ledger, "async-unknown", "OUTCOME_UNKNOWN")
        row_result = json.loads(str(row["result_json"]))
        assert row_result == {
            "reason": "SIGTERM_SENT_EXIT_NOT_OBSERVED",
            "physical_effect_count": 1,
            "signal_attempt_count": 1,
            "exit_observed": False,
            "process_still_running_after_dispatch": True,
        }
        terminal = client.execute(payload)
        assert terminal["state"] == "OUTCOME_UNKNOWN"
        assert terminal["result"] == row_result
        assert terminal["automatic_retry"] is False
        status = client.status("async-unknown")
        assert status["state"] == "OUTCOME_UNKNOWN"
        assert status["effect_scope_state"] == "OUTCOME_UNKNOWN"
        assert status["automatic_retry"] is False
    finally:
        server.close()
        ledger.close()


def test_close_does_not_silently_abandon_an_in_flight_worker(tmp_path: Path) -> None:
    ledger = EffectLedger(tmp_path / "ledger.sqlite3", initial_producer_id="old", initial_producer_generation=1)
    entered = threading.Event()
    release = threading.Event()

    def effect(_value) -> dict[str, object]:
        entered.set()
        assert release.wait(timeout=2)
        return {"physical_effect_count": 1, "effect": "SIGTERM", "exit_observed": True}

    server = EffectExecutorServer(
        socket_path=tmp_path / "effect.sock",
        ledger=ledger,
        allowed_peer_uids={os.getuid()},
        current_target=current_target,
        perform_effect=effect,
        execute_async=True,
        worker_shutdown_timeout_seconds=0.05,
    )
    server.start()
    try:
        response = EffectClient(tmp_path / "effect.sock").execute(request("close-in-flight"))
        assert response["state"] == "EXECUTION_STARTED"
        assert entered.wait(timeout=0.5)

        with pytest.raises(RuntimeError, match="EFFECT_EXECUTOR_WORKERS_DID_NOT_STOP"):
            server.close()
        assert not (tmp_path / "effect.sock").exists()
        assert ledger.request("close-in-flight")["state"] == "EXECUTION_STARTED"

        release.set()
        wait_for_state(ledger, "close-in-flight", "EFFECT_OBSERVED")
        server.close()
    finally:
        release.set()
        ledger.close()
