"""Shared fakes for the local reflex agent tests."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cra.agent import Agent, CentralConfig, Config
from cra.observe import Endpoint, Network
from cra.runtime_binding import PROTOCOL, SUCCESSOR_SCHEMA, TRANSPORT_SCHEMA
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
        self.endpoints: list[Endpoint] = []

    def probe(self, endpoint: Endpoint) -> Network:
        self.calls += 1
        self.endpoints.append(endpoint)
        return self.network


class FakeExecutor:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.response: dict[str, Any] | Exception = {"ok": True, "state": "EFFECT_OBSERVED", "reason": "EFFECT_OBSERVED"}

    def __call__(self, _socket: Path, request: dict[str, Any]) -> dict[str, Any]:
        self.requests.append(request)
        if isinstance(self.response, Exception):
            raise self.response
        return {"request_id": request.get("request_id"), **self.response}


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
        self.executor = None
        self.link = None
        self.bound = False

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
        value.setdefault("delivery_route", "FALLBACK")
        value.setdefault("delivery_route_generation", 1)
        value.setdefault("local_ffmpeg_pid", 4242)
        if self.bound:
            value.setdefault("cra_runtime_capabilities", {"restart_protocol": PROTOCOL, "successor": SUCCESSOR_SCHEMA})
            value.setdefault(
                "delivery_transport",
                {
                    "status": "AVAILABLE",
                    "binding": {
                        "schema": TRANSPORT_SCHEMA,
                        "executor_instance_id": value["executor_instance_id"],
                        "pod_uid": value["pod_uid"],
                        "generation": value["ffmpeg_generation"],
                        "pid": value["local_ffmpeg_pid"],
                        "start_ticks": 1000,
                        "socket_inode": 100 + self.generation,
                        "local_host": "127.0.0.2",
                        "local_port": 50000,
                        "peer_host": "127.0.0.1",
                        "peer_port": 1936,
                        "route": value["delivery_route"],
                        "route_generation": value["delivery_route_generation"],
                    },
                    "tcp_metrics": value["tcp_metrics"],
                },
            )
            value.setdefault("recovery_successor", self.link)
        self.path.write_text(json.dumps(value))

    def new_generation(self) -> None:
        before = f"gen-{self.generation}"
        self.generation += 1
        self.bytes_sent = 10_000
        self.uptime = 120
        self.link = None
        if self.executor and self.executor.requests:
            request = self.executor.requests[-1]
            if request["expected_ffmpeg_generation"] == before:
                self.link = {
                    "schema": SUCCESSOR_SCHEMA,
                    "request_id": request["request_id"],
                    "executor_instance_id": "exec-1",
                    "pod_uid": "pod-a",
                    "previous_generation": before,
                    "generation": f"gen-{self.generation}",
                    "exit_event_id": "exit-1",
                    "start_event_id": "start-1",
                }


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
            ingest_host="127.0.0.1",
            ingest_port=1936,
            ingest_endpoints={"FALLBACK": Endpoint("127.0.0.1", 1936)},
            central=central,
        )
        self.pod = Pod(self.config.observation_path, self.clock)
        self.prober = FakeProber()
        self.executor = FakeExecutor()
        self.pod.executor = self.executor
        self.agent = self.new_agent()

    def new_agent(self, store: AgentStore | None = None) -> Agent:
        return Agent(
            self.config,
            store=store or AgentStore(self.config.db_path),
            prober=self.prober,  # type: ignore[arg-type]
            sender=self.executor,
            clock=self.clock,
            log=lambda _: None,
            status_reader=lambda *_: {},
        )

    def tick(self, *, stalled: bool, **overrides: Any):
        self.clock.advance(2)
        self.pod.write(stalled=stalled, **overrides)
        return self.agent.step()

    def kinds(self) -> list[str]:
        return [r["kind"] for r in self.agent.store.read_audit(0, 10_000)]
