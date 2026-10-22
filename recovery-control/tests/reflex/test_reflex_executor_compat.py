"""Contract test against the in-Pod Effect Executor in `runtime_boundary`.

The agent reuses that Executor unchanged, so its requests must be accepted by it, and the
Executor's own fence must hold even if a request were repeated.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

import runtime_boundary
from cra import effect
from cra.observe import read_observation

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
