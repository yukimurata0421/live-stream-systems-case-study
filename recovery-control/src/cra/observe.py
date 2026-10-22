"""Inputs of the reflex loop: the stream-engine runtime observation and local network probes.

The observation is published every second by the in-Pod stream-engine Effect Executor
(schema runtime.ffmpeg_observation.v1, stream_recovery_control runtime_boundary/observation.py).
"""

from __future__ import annotations

import json
import socket
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

OBSERVATION_SCHEMA = "runtime.ffmpeg_observation.v1"


@dataclass(frozen=True)
class TcpMetrics:
    bytes_sent: int
    notsent: int
    unacked: int
    lastsnd_ms: int

    @classmethod
    def parse(cls, value: Any) -> TcpMetrics | None:
        if not isinstance(value, dict) or "bytes_sent" not in value:
            return None
        try:
            return cls(**{name: int(value.get(name) or 0) for name in ("bytes_sent", "notsent", "unacked", "lastsnd_ms")})
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True)
class Observation:
    observation_id: str
    executor_instance_id: str
    sequence: int
    observed_at: datetime
    pod_uid: str
    ffmpeg_generation: str
    ffmpeg_running: bool
    ffmpeg_uptime_sec: int
    active_producer_id: str
    active_producer_generation: int
    in_flight_count: int
    target_valid: bool
    tcp: TcpMetrics | None
    raw: dict[str, Any] = field(repr=False, compare=False)

    @property
    def generation_key(self) -> str:
        return f"{self.pod_uid}/{self.ffmpeg_generation}"


def read_observation(path: Path, *, max_age_sec: float, now: datetime | None = None) -> tuple[Observation | None, str]:
    """Return (observation, "") or (None, reason). Stale or malformed input is never used."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, "OBSERVATION_MISSING"
    except PermissionError:
        return None, "OBSERVATION_UNREADABLE"
    except (OSError, json.JSONDecodeError):
        return None, "OBSERVATION_INVALID"
    if not isinstance(raw, dict) or raw.get("schema_version") != OBSERVATION_SCHEMA:
        return None, "OBSERVATION_SCHEMA_INVALID"
    try:
        observed_at = datetime.fromisoformat(str(raw["observed_at"])).astimezone(UTC)
        age = ((now or datetime.now(UTC)) - observed_at).total_seconds()
        observation = Observation(
            observation_id=str(raw["observation_id"]),
            executor_instance_id=str(raw.get("executor_instance_id") or ""),
            sequence=int(raw["sequence"]),
            observed_at=observed_at,
            pod_uid=str(raw.get("pod_uid") or ""),
            ffmpeg_generation=str(raw.get("ffmpeg_generation") or ""),
            ffmpeg_running=raw.get("ffmpeg_running") is True,
            ffmpeg_uptime_sec=int(raw.get("ffmpeg_uptime_sec") or 0),
            active_producer_id=str(raw.get("active_producer_id") or ""),
            active_producer_generation=int(raw.get("active_producer_generation") or 0),
            in_flight_count=int(raw.get("in_flight_count") or 0),
            target_valid=raw.get("target_snapshot_status") == "VALID" and isinstance(raw.get("target_identity"), dict),
            tcp=TcpMetrics.parse(raw.get("tcp_metrics")),
            raw=raw,
        )
    except (KeyError, TypeError, ValueError):
        return None, "OBSERVATION_INVALID"
    if age < -1.0 or age > max_age_sec:
        return None, "OBSERVATION_STALE"
    return observation, ""


@dataclass(frozen=True)
class Network:
    gateway_ok: bool
    public_ok_count: int
    ingest_ok: bool

    @property
    def network_down(self) -> bool:
        return not self.gateway_ok and self.public_ok_count == 0


RunCommand = Callable[[list[str]], int]


def _run(cmd: list[str]) -> int:
    try:
        return subprocess.run(cmd, capture_output=True, timeout=3, check=False).returncode
    except (OSError, subprocess.TimeoutExpired):
        return -1


def _default_gateway() -> str:
    try:
        out = subprocess.run(["ip", "route", "show", "default"], check=False, capture_output=True, text=True, timeout=2).stdout
    except (OSError, subprocess.TimeoutExpired):
        return ""
    parts = out.split()
    return parts[parts.index("via") + 1] if "via" in parts[:-1] else ""


def _tcp_connect(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@dataclass
class Prober:
    public_hosts: list[str]
    ingest_host: str
    ingest_port: int
    run: RunCommand = _run
    gateway: Callable[[], str] = _default_gateway
    connect: Callable[[str, int], bool] = _tcp_connect

    def probe(self) -> Network:
        gw = self.gateway()

        def ping(host: str) -> bool:
            return self.run(["ping", "-c", "1", "-W", "1", host]) == 0

        return Network(
            gateway_ok=bool(gw) and ping(gw),
            public_ok_count=sum(1 for host in self.public_hosts if ping(host)),
            ingest_ok=self.connect(self.ingest_host, self.ingest_port),
        )
