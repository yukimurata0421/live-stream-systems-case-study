"""Contract test against the real in-Pod Effect Executor (stream_recovery_control runtime_boundary).

The agent reuses that Executor unchanged, so its requests must be accepted by it, and the
Executor's own fence must hold even if a request were repeated.
"""

from __future__ import annotations

import dataclasses
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

import runtime_boundary
from cra import effect
from cra.observe import read_observation

LEGACY_SRC = Path(__file__).resolve().parents[2] / "src"  # runtime_boundary lives in this repository

TARGET = {
    "host_id": "dell-yuki",
    "host_boot_id": "boot-1",
    "namespace": "stream-v3",
    "pod_uid": "pod-1",
    "container_name": "stream-engine",
    "container_id": "containerd://container-1",
    "ffmpeg_generation": "protocol-generation-1",
    "ffmpeg_pid": 4242,
}


@pytest.fixture
def executor(tmp_path: Path):
    effects: list[str] = []
    current = {
        "target_identity": TARGET,
        "ffmpeg_generation": "native-1",
        "target_snapshot_id": "target-snapshot-10",
        "executor_instance_id": "executor-1",
    }
    ledger = runtime_boundary.EffectLedger(tmp_path / "ledger.sqlite3", initial_producer_id="cra-agent", initial_producer_generation=3)
    server = runtime_boundary.EffectExecutorServer(
        socket_path=tmp_path / "effect.sock",
        ledger=ledger,
        allowed_peer_uids={os.getuid()},
        current_target=lambda: current,
        perform_effect=lambda request: effects.append(request.request_id) or {"physical_effect_count": 1},
    )
    server.start()
    yield tmp_path / "effect.sock", effects
    server.close()
    ledger.close()


def observation(tmp_path: Path):
    import json

    path = tmp_path / "obs.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "runtime.ffmpeg_observation.v1",
                "observation_id": "observation-10",
                "executor_instance_id": "executor-1",
                "sequence": 10,
                "observed_at": datetime.now(UTC).isoformat(),
                "pod_uid": "pod-1",
                "ffmpeg_generation": "native-1",
                "ffmpeg_running": True,
                "ffmpeg_uptime_sec": 300,
                "active_producer_id": "cra-agent",
                "active_producer_generation": 3,
                "in_flight_count": 0,
                "target_snapshot_status": "VALID",
                "target_identity": TARGET,
                "target_snapshot_id": "target-snapshot-10",
                "maintenance_evidence_status": "AVAILABLE",
                "projection_id": "projection-10",
                "projection_sequence": 10,
                "tcp_metrics": {"bytes_sent": 1, "notsent": 0, "unacked": 0, "lastsnd_ms": 0},
            }
        )
    )
    obs, why = read_observation(path, max_age_sec=3)
    assert obs is not None, why
    return obs


def build(obs, producer=("cra-agent", 3)):
    return effect.build_restart_request(obs, now=datetime.now(UTC), producer_id=producer[0], producer_generation=producer[1], reason="test")


def test_agent_request_is_accepted_and_executed_once(executor, tmp_path: Path) -> None:
    socket_path, effects = executor
    obs = observation(tmp_path)
    first = effect.send(socket_path, build(obs))
    assert effect.classify(first) == "EFFECT_OBSERVED", first
    second = effect.send(socket_path, build(obs))  # same generation -> same request id -> replay
    assert second.get("replay") is True
    assert len(effects) == 1


def test_wrong_authority_is_rejected_without_effect(executor, tmp_path: Path) -> None:
    socket_path, effects = executor
    response = effect.send(socket_path, build(observation(tmp_path), producer=("independent-controller", 2)))
    assert effect.classify(response) == "REJECTED"
    assert response["reason"] == "PRODUCER_NOT_ACTIVE"
    assert effects == []


@pytest.mark.parametrize("fault", ["expired", "executor", "target"])
def test_v2_rejects_expired_or_changed_target(executor, tmp_path, fault):
    from datetime import timedelta

    socket_path, effects = executor
    request = build(observation(tmp_path))
    if fault == "expired":
        request["issued_at"] = (datetime.now(UTC) - timedelta(seconds=4)).isoformat()
        request["expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    elif fault == "executor":
        request["expected_executor_instance_id"] = "other"
    else:
        request["ffmpeg_target_identity"]["ffmpeg_pid"] += 1
    assert effect.send(socket_path, request)["state"] == "REJECTED"
    assert effects == []


@pytest.mark.parametrize("lose_reply", [False, True])
def test_minimal_agent_with_actual_legacy_async_executor(harness, tmp_path, lose_reply):
    from cra.agent import Agent
    from tests.reflex.test_reflex_agent import use_policy

    h = harness
    use_policy(h, tmp_path)
    h.pod.clock = lambda: datetime.now(UTC)
    h.config = dataclasses.replace(h.config, executor_socket=tmp_path / "effect.sock")
    target = {**TARGET, "pod_uid": "pod-a"}
    current = {"target_identity": target, "ffmpeg_generation": "gen-1", "executor_instance_id": "exec-1"}
    ledger = runtime_boundary.EffectLedger(tmp_path / "ledger.db", initial_producer_id="cra-agent", initial_producer_generation=3)
    effects, replies, statuses = [], [], []

    def send(path, request):
        assert request["schema_version"] == "runtime.typed_effect_request.v2"
        assert "expected_transport" not in request
        response = effect.send(path, request)
        replies.append(response)
        if lose_reply:
            raise TimeoutError("deliberately dropped admission response")
        return response

    def read_status(path, request_id):
        status = effect.read_status(path, request_id)
        assert "executor_instance_id" not in status, "this gate must load the unchanged old runtime"
        statuses.append(status)
        return status

    server = runtime_boundary.EffectExecutorServer(
        socket_path=h.config.executor_socket,
        ledger=ledger,
        execute_async=True,
        allowed_peer_uids={os.getuid()},
        current_target=lambda: current,
        perform_effect=lambda r: effects.append(r.request_id) or {"physical_effect_count": 1, "exit_observed": True},
    )
    h.agent = Agent(
        h.config,
        store=h.agent.store,
        prober=h.prober,
        sender=send,
        status_reader=read_status,
        clock=lambda: datetime.now(UTC),
        log=lambda _: None,
    )
    server.start()
    try:
        for _ in range(6):
            h.pod.write(stalled=True, target_identity=target)
            h.agent.step()
        h.pod.new_generation()
        h.pod.uptime = 0
        for _ in range(20):
            h.pod.write(stalled=False, target_identity=target)
            h.agent.step()
            row = h.agent.store.db.execute("SELECT * FROM actions").fetchone()
            if row["verify_result"]:
                break
            time.sleep(0.01)
        assert replies[0]["state"] == "EXECUTION_STARTED"
        assert row["state"] == "EFFECT_OBSERVED" and row["verify_result"] == "DELIVERY_RESUMED"
        assert len(effects) == len(replies) == 1 and statuses
        assert "action_reconciled" in h.kinds()
    finally:
        server.close()
        ledger.close()
