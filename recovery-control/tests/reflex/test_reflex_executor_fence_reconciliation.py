"""Against the real Executor with production's require_transport_verification=True:
a restart leaves the scope awaiting verification and blocks the next generation until the
agent's v1 reconciliation closes it."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

import runtime_boundary
from cra import effect
from cra.observe import read_observation

LEGACY_SRC = Path(__file__).resolve().parents[2] / "src"  # runtime_boundary lives in this repository


def target(generation: str, pid: int) -> dict:
    return {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-1",
        "namespace": "stream-v3",
        "pod_uid": "pod-1",
        "container_name": "stream-engine",
        "container_id": "containerd://container-1",
        "ffmpeg_generation": generation,
        "ffmpeg_pid": pid,
    }


class Runtime:
    def __init__(self, root: Path, initial_producer: tuple[str, int] = ("cra-agent", 3)) -> None:
        self.root = root
        self.effects: list[str] = []
        self.set_child("protocol-1", 4242, "native-1")
        self.ledger = runtime_boundary.EffectLedger(
            root / "ledger.sqlite3", initial_producer_id=initial_producer[0], initial_producer_generation=initial_producer[1]
        )
        self.server = runtime_boundary.EffectExecutorServer(
            socket_path=root / "effect.sock",
            ledger=self.ledger,
            allowed_peer_uids={os.getuid()},
            current_target=lambda: self.current,
            perform_effect=lambda request: self.effects.append(request.request_id) or {"physical_effect_count": 1},
            require_transport_verification=True,
        )
        self.server.start()

    @property
    def socket(self) -> Path:
        return self.root / "effect.sock"

    def set_child(self, protocol_generation: str, pid: int, native_generation: str) -> None:
        self.current = {
            "target_identity": target(protocol_generation, pid),
            "ffmpeg_generation": native_generation,
            "target_snapshot_id": f"snapshot-{native_generation}",
            "executor_instance_id": "executor-1",
        }

    def observation(self, sequence: int):
        path = self.root / "obs.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": "runtime.ffmpeg_observation.v1",
                    "observation_id": f"observation-{sequence}",
                    "executor_instance_id": "executor-1",
                    "sequence": sequence,
                    "observed_at": datetime.now(UTC).isoformat(),
                    "pod_uid": "pod-1",
                    "ffmpeg_generation": self.current["ffmpeg_generation"],
                    "ffmpeg_running": True,
                    "ffmpeg_uptime_sec": 300,
                    "active_producer_id": "cra-agent",
                    "active_producer_generation": 3,
                    "in_flight_count": self.ledger.unresolved_count(),
                    "target_snapshot_status": "VALID",
                    "target_identity": self.current["target_identity"],
                    "target_snapshot_id": self.current["target_snapshot_id"],
                    "maintenance_evidence_status": "AVAILABLE",
                    "projection_id": "projection-10",
                    "projection_sequence": 10,
                    "tcp_metrics": {
                        "bytes_sent": 1_000_000 + sequence,
                        "bytes_acked": 900_000 + sequence,
                        "notsent": 0,
                        "unacked": 0,
                        "lastsnd_ms": 10,
                    },
                }
            )
        )
        obs, why = read_observation(path, max_age_sec=3)
        assert obs is not None, why
        return obs

    def close(self) -> None:
        self.server.close()
        self.ledger.close()


@pytest.fixture
def runtime(tmp_path: Path):
    value = Runtime(tmp_path)
    yield value
    value.close()


def restart(runtime: Runtime, obs) -> dict:
    request = effect.build_restart_request(obs, now=datetime.now(UTC), producer_id="cra-agent", producer_generation=3, reason="test")
    response = effect.send(runtime.socket, request)
    response["_classified"] = effect.classify(response, expected_request_id=request["request_id"])
    return response


def test_reconciliation_releases_the_next_generation(runtime: Runtime) -> None:
    first = restart(runtime, runtime.observation(10))
    assert first["_classified"] == "EFFECT_OBSERVED", first
    scopes = effect.query_unresolved(runtime.socket)
    assert [s["state"] for s in scopes] == ["EFFECT_OBSERVED_AWAITING_VERIFICATION"]
    assert runtime.ledger.unresolved_count() == 1

    runtime.set_child("protocol-2", 5151, "native-2")  # the stream engine started a successor
    blocked = restart(runtime, runtime.observation(11))
    assert blocked["_classified"] == "REJECTED" and blocked["reason"] == "TARGET_EFFECT_UNRESOLVED", blocked

    reconciled = effect.send(runtime.socket, effect.build_reconciliation(scopes[0], runtime.observation(12), now=datetime.now(UTC)))
    assert reconciled.get("ok") is True and reconciled.get("state") == "RECONCILED_EFFECT_OBSERVED", reconciled
    assert runtime.ledger.unresolved_count() == 0
    again = effect.send(runtime.socket, effect.build_reconciliation(scopes[0], runtime.observation(12), now=datetime.now(UTC)))
    assert again.get("ok") is not True  # a closed fence is not reopened or rewritten
    assert effect.query_unresolved(runtime.socket) == []  # so the agent has nothing left to resend

    status = effect.read_status(runtime.socket, first["request_id"])
    assert status["effect_scope_state"] == "RECONCILED_EFFECT_OBSERVED", status

    second = restart(runtime, runtime.observation(13))
    assert second["_classified"] == "EFFECT_OBSERVED", second
    assert len(runtime.effects) == 2


def test_reconciliation_needs_a_distinct_successor(runtime: Runtime) -> None:
    assert restart(runtime, runtime.observation(10))["_classified"] == "EFFECT_OBSERVED"
    scope = effect.query_unresolved(runtime.socket)[0]
    response = effect.send(runtime.socket, effect.build_reconciliation(scope, runtime.observation(11), now=datetime.now(UTC)))
    assert response.get("ok") is not True  # same child: the Executor refuses to close the fence
    assert runtime.ledger.unresolved_count() == 1
