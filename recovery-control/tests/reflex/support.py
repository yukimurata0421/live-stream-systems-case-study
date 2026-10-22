"""Shared fakes for the local reflex agent tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cra.agent import Agent, CentralConfig, Config
from cra.observe import Network
from cra.store import AgentStore

T0 = datetime(2026, 10, 22, 0, 0, tzinfo=UTC)


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class FakeProber:
    def __init__(self) -> None:
        self.network = Network(gateway_ok=True, public_ok_count=2, ingest_ok=True)
        self.calls = 0

    def probe(self) -> Network:
        self.calls += 1
        return self.network


class FakeExecutor:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.response: dict[str, Any] | Exception = {"ok": True, "state": "EFFECT_OBSERVED", "reason": "EFFECT_OBSERVED"}

    def __call__(self, _socket: Path, request: dict[str, Any]) -> dict[str, Any]:
        self.requests.append(request)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class Pod:
    """Writes runtime observations the way the stream-engine publisher does."""

    def __init__(self, path: Path, clock: Clock) -> None:
        self.path = path
        self.clock = clock
        self.sequence = 0
        self.generation = 1
        self.bytes_sent = 1_000_000
        self.uptime = 120
        self.producer = ("cra-agent", 3)
        self.in_flight = 0

    def write(self, *, stalled: bool, **overrides: Any) -> None:
        self.sequence += 1
        if not stalled:
            self.bytes_sent += 500_000
        tcp = {
            "bytes_sent": self.bytes_sent,
            "notsent": 300_000 if stalled else 0,
            "unacked": 120 if stalled else 4,
            "lastsnd_ms": 9000 if stalled else 20,
        }
        value = {
            "schema_version": "runtime.ffmpeg_observation.v1",
            "observation_id": f"obs-{self.sequence}",
            "executor_instance_id": "exec-1",
            "sequence": self.sequence,
            "observed_at": self.clock().isoformat().replace("+00:00", "Z"),
            "pod_uid": "pod-a",
            "ffmpeg_generation": f"gen-{self.generation}",
            "ffmpeg_running": True,
            "ffmpeg_uptime_sec": self.uptime,
            "active_producer_id": self.producer[0],
            "active_producer_generation": self.producer[1],
            "in_flight_count": self.in_flight,
            "target_snapshot_status": "VALID",
            "target_identity": {"ffmpeg_pid": 4242, "pod_uid": "pod-a"},
            "target_snapshot_id": "snap-1",
            "tcp_metrics": tcp,
        }
        value.update(overrides)
        self.path.write_text(json.dumps(value))

    def new_generation(self) -> None:
        self.generation += 1
        self.bytes_sent = 10_000
        self.uptime = 120


class Harness:
    def __init__(self, tmp: Path, central: CentralConfig | None = None) -> None:
        self.clock = Clock()
        self.config = Config(
            agent_id="dell-test",
            db_path=tmp / "agent.db",
            observation_path=tmp / "observation.json",
            executor_socket=tmp / "effect.sock",
            producer_id="cra-agent",
            producer_generation=3,
            stop_file=tmp / "STOP",
            status_path=tmp / "status.json",
            ingest_host="192.0.2.1",
            ingest_port=1935,
            central=central,
        )
        self.pod = Pod(self.config.observation_path, self.clock)
        self.prober = FakeProber()
        self.executor = FakeExecutor()
        self.agent = self.new_agent()

    def new_agent(self, store: AgentStore | None = None) -> Agent:
        return Agent(
            self.config,
            store=store or AgentStore(self.config.db_path),
            prober=self.prober,  # type: ignore[arg-type]
            sender=self.executor,
            clock=self.clock,
            log=lambda _: None,
        )

    def tick(self, *, stalled: bool, **overrides: Any):
        self.clock.advance(2)
        self.pod.write(stalled=stalled, **overrides)
        return self.agent.step()

    def kinds(self) -> list[str]:
        return [r["kind"] for r in self.agent.store.read_audit(0, 10_000)]
