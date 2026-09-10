from __future__ import annotations

import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from runtime_boundary import EffectClient, EffectExecutorServer, EffectLedger


def target(pid: int = 8100) -> dict[str, object]:
    return {
        "host_id": "isolated-dell",
        "host_boot_id": "isolated-boot",
        "namespace": "isolated",
        "pod_uid": "isolated-pod",
        "container_name": "stream-engine",
        "container_id": "containerd://isolated",
        "ffmpeg_generation": f"protocol-generation-{pid}",
        "ffmpeg_pid": pid,
    }


def request(*, request_id: str, correlation_id: str, pid: int = 8100) -> dict[str, object]:
    now = datetime.now(UTC)
    return {
        "schema_version": "runtime.effect_request.v1",
        "request_id": request_id,
        "producer_id": "isolated-controller",
        "producer_generation": 1,
        "operation": "restart_ffmpeg",
        "reason": "isolated correlation lookup",
        "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=5)).isoformat(),
        "target_identity": target(pid),
        "expected_ffmpeg_generation": f"native-generation-{pid}",
        "idempotency_key": request_id,
        "correlation_id": correlation_id,
        "target_snapshot_id": f"snapshot-{pid}",
        "runtime_observation_id": f"observation-{pid}",
        "expected_executor_instance_id": "isolated-executor",
        "maintenance_evidence_status": "AVAILABLE",
        "projection_id": "isolated-projection",
        "projection_sequence": pid,
    }


def wait_terminal(client: EffectClient, request_id: str) -> None:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if client.status(request_id)["state"] == "EFFECT_OBSERVED":
            return
        time.sleep(0.01)
    raise AssertionError("effect did not reach EFFECT_OBSERVED")


def test_correlation_status_resolves_formal_request_without_replaying_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = target()
    current = {
        "target_identity": identity,
        "ffmpeg_generation": "native-generation-8100",
        "executor_instance_id": "isolated-executor",
    }
    effects: list[str] = []
    ledger = EffectLedger(
        tmp_path / "ledger.sqlite3",
        initial_producer_id="isolated-controller",
        initial_producer_generation=1,
    )
    server = EffectExecutorServer(
        socket_path=tmp_path / "effect.sock",
        ledger=ledger,
        allowed_peer_uids={os.getuid()},
        current_target=lambda: current,
        perform_effect=lambda value: effects.append(value.request_id) or {"physical_effect_count": 1, "exit_observed": True},
        execute_async=True,
        require_transport_verification=True,
    )
    server.start()
    client = EffectClient(tmp_path / "effect.sock")
    try:
        accepted = client.execute(request(request_id="dell-target-one", correlation_id="fra-incident-one"))
        assert accepted["state"] == "EXECUTION_STARTED"
        wait_terminal(client, "dell-target-one")

        first = client.status_by_correlation("fra-incident-one")
        second = client.status_by_correlation("fra-incident-one")

        assert first["ok"] is True
        assert first["matching_count"] == 1
        assert first["request_id"] == "dell-target-one"
        assert first["state"] == "EFFECT_OBSERVED"
        assert second == first
        assert effects == ["dell-target-one"]
        with monkeypatch.context() as patch:
            patch.setattr(ledger, "request_status", lambda _request_id: None)
            with pytest.raises(RuntimeError, match="CORRELATION_REQUEST_STATUS_MISSING"):
                ledger.request_status_by_correlation("fra-incident-one")
    finally:
        server.close()
        ledger.close()


def test_correlation_status_missing_and_ambiguous_are_explicit_and_read_only(tmp_path: Path) -> None:
    identity = target()
    current = {
        "target_identity": identity,
        "ffmpeg_generation": "native-generation-8100",
        "executor_instance_id": "isolated-executor",
    }
    effects: list[str] = []
    ledger = EffectLedger(
        tmp_path / "ledger.sqlite3",
        initial_producer_id="isolated-controller",
        initial_producer_generation=1,
    )
    server = EffectExecutorServer(
        socket_path=tmp_path / "effect.sock",
        ledger=ledger,
        allowed_peer_uids={os.getuid()},
        current_target=lambda: current,
        perform_effect=lambda value: effects.append(value.request_id) or {"physical_effect_count": 1, "exit_observed": True},
    )
    server.start()
    client = EffectClient(tmp_path / "effect.sock")
    try:
        missing = client.status_by_correlation("fra-missing")
        assert missing["ok"] is False
        assert missing["reason"] == "CORRELATION_NOT_FOUND"
        assert missing["matching_count"] == 0

        assert client.execute(request(request_id="dell-target-one", correlation_id="fra-shared"))["ok"] is True
        exact = client.status_by_correlation("fra-shared")
        assert exact["ok"] is True
        assert exact["automatic_retry"] is None
        ledger.connection.execute(
            """INSERT INTO typed_effect_requests(
                   request_id,idempotency_key,request_digest,producer_id,producer_generation,
                   intent_type,operation,failure_domain,correlation_id,target_snapshot_id,
                   runtime_snapshot_id,runtime_observation_id,expected_executor_instance_id,
                   maintenance_evidence_status,projection_id,projection_sequence,identity_type,
                   identity_json,ffmpeg_target_identity_json,runtime_identity_json,
                   expected_ffmpeg_generation,request_json,state,result_json,accepted_at,
                   execution_started_at,finished_at
               )
               SELECT 'dell-target-two','dell-target-two',request_digest,producer_id,producer_generation,
                      intent_type,operation,failure_domain,correlation_id,target_snapshot_id,
                      runtime_snapshot_id,runtime_observation_id,expected_executor_instance_id,
                      maintenance_evidence_status,projection_id,projection_sequence,identity_type,
                      identity_json,ffmpeg_target_identity_json,runtime_identity_json,
                      expected_ffmpeg_generation,request_json,state,result_json,accepted_at,
                      execution_started_at,finished_at
               FROM typed_effect_requests WHERE request_id='dell-target-one'"""
        )

        ambiguous = client.status_by_correlation("fra-shared")
        assert ambiguous["ok"] is False
        assert ambiguous["reason"] == "CORRELATION_AMBIGUOUS"
        assert ambiguous["matching_count"] == 2
        assert effects == ["dell-target-one"]
    finally:
        server.close()
        ledger.close()


def test_correlation_indexes_exist_on_initialized_ledger(tmp_path: Path) -> None:
    ledger = EffectLedger(
        tmp_path / "ledger.sqlite3",
        initial_producer_id="isolated-controller",
        initial_producer_generation=1,
    )
    try:
        legacy_indexes = {str(row[1]) for row in ledger.connection.execute("PRAGMA index_list(effect_requests)")}
        typed_indexes = {str(row[1]) for row in ledger.connection.execute("PRAGMA index_list(typed_effect_requests)")}
        assert "effect_requests_correlation_id_idx" in legacy_indexes
        assert "typed_effect_requests_correlation_id_idx" in typed_indexes
    finally:
        ledger.close()


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (
            {
                "schema_version": "runtime.effect_correlation_status_query.v1",
                "correlation_id": "fra-one",
                "unexpected": True,
            },
            "CORRELATION_STATUS_QUERY_FIELDS_NOT_EXACT",
        ),
        (
            {
                "schema_version": "runtime.effect_correlation_status_query.v1",
                "correlation_id": "",
            },
            "CORRELATION_STATUS_QUERY_ID_INVALID",
        ),
        (
            {
                "schema_version": "runtime.effect_correlation_status_query.v1",
                "correlation_id": "x" * 257,
            },
            "CORRELATION_STATUS_QUERY_ID_INVALID",
        ),
    ],
)
def test_correlation_status_rejects_noncanonical_queries_without_effect(
    tmp_path: Path,
    payload: dict[str, object],
    reason: str,
) -> None:
    effects: list[str] = []
    ledger = EffectLedger(
        tmp_path / "ledger.sqlite3",
        initial_producer_id="isolated-controller",
        initial_producer_generation=1,
    )
    server = EffectExecutorServer(
        socket_path=tmp_path / "effect.sock",
        ledger=ledger,
        allowed_peer_uids={os.getuid()},
        current_target=lambda: {
            "target_identity": target(),
            "ffmpeg_generation": "native-generation-8100",
            "executor_instance_id": "isolated-executor",
        },
        perform_effect=lambda value: effects.append(value.request_id),
    )
    server.start()
    try:
        response = EffectClient(tmp_path / "effect.sock").execute(payload)
        assert response["ok"] is False
        assert response["reason"] == reason
        assert effects == []
    finally:
        server.close()
        ledger.close()
