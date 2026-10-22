"""Inputs of the reflex loop: the stream-engine runtime observation and local network probes.

The observation is published every second by the in-Pod stream-engine Effect Executor
(schema runtime.ffmpeg_observation.v1, stream_recovery_control runtime_boundary/observation.py).
"""

from __future__ import annotations

import json
import socket
import subprocess
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import UTC, datetime
from ipaddress import ip_address
from pathlib import Path
from typing import Any

from cra.runtime_binding import validate_transport

OBSERVATION_SCHEMA = "runtime.ffmpeg_observation.v1"


@dataclass(frozen=True)
class TcpMetrics:
    bytes_sent: int
    notsent: int
    unacked: int
    lastsnd_ms: int
    bytes_acked: int | None = None  # None when the publisher does not report it

    @classmethod
    def parse(cls, value: Any) -> TcpMetrics | None:
        if not isinstance(value, dict) or "bytes_sent" not in value:
            return None
        try:
            fields = {name: int(value.get(name) or 0) for name in ("bytes_sent", "notsent", "unacked", "lastsnd_ms")}
            acked = value.get("bytes_acked")
            return cls(**fields, bytes_acked=None if acked is None else int(acked))
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

    @property
    def binding(self) -> dict[str, Any] | None:
        sample = self.raw.get("delivery_transport")
        if not isinstance(sample, dict) or sample.get("status") != "AVAILABLE":
            return None
        try:
            value = validate_transport(sample.get("binding"))
        except (ValueError, TypeError):
            return None
        if (
            value["executor_instance_id"] != self.executor_instance_id
            or value["pod_uid"] != self.pod_uid
            or value["generation"] != self.ffmpeg_generation
            or value["pid"] != self.raw.get("local_ffmpeg_pid")
            or value["route"] != self.raw.get("delivery_route")
            or value["route_generation"] != self.raw.get("delivery_route_generation")
        ):
            return None
        return value

    @property
    def binding_status(self) -> str:
        if "delivery_transport" not in self.raw:
            return "LEGACY_UNBOUND"
        return "AVAILABLE" if self.binding else "UNAVAILABLE"

    @property
    def transport_identity(self) -> tuple[Any, ...]:
        return (
            self.executor_instance_id,
            self.generation_key,
            self.raw.get("delivery_route"),
            self.raw.get("delivery_route_generation"),
            tuple(sorted(self.binding.items())) if self.binding else self.binding_status,
        )


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
    return parse_observation(raw, max_age_sec=max_age_sec, now=now)


def parse_observation(raw: Any, *, max_age_sec: float | None, now: datetime | None = None) -> tuple[Observation | None, str]:
    """Validate one observation mapping. max_age_sec=None skips the freshness check (offline replay)."""
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
            tcp=TcpMetrics.parse((raw.get("delivery_transport") or {}).get("tcp_metrics"))
            if isinstance(raw.get("delivery_transport"), dict)
            else TcpMetrics.parse(raw.get("tcp_metrics")),
            raw=raw,
        )
    except (KeyError, TypeError, ValueError):
        return None, "OBSERVATION_INVALID"
    if max_age_sec is not None and (age < -1.0 or age > max_age_sec):
        return None, "OBSERVATION_STALE"
    return observation, ""


@dataclass(frozen=True)
class Network:
    gateway_ok: bool
    public_ok_count: int
    ingest_ok: bool
    timed_out: bool = False

    @property
    def network_down(self) -> bool:
        return not self.gateway_ok and self.public_ok_count == 0


RunCommand = Callable[[list[str]], int]


def _run(cmd: list[str]) -> int:
    try:
        return subprocess.run(cmd, capture_output=True, timeout=0.8, check=False).returncode
    except (OSError, subprocess.TimeoutExpired):
        return -1


def _default_gateway() -> str:
    try:
        out = subprocess.run(["ip", "route", "show", "default"], check=False, capture_output=True, text=True, timeout=0.2).stdout
    except (OSError, subprocess.TimeoutExpired):
        return ""
    parts = out.split()
    return parts[parts.index("via") + 1] if "via" in parts[:-1] else ""


def _tcp_connect(host: str, port: int, timeout: float = 0.8) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@dataclass(frozen=True)
class Endpoint:
    host: str
    port: int

    def __post_init__(self) -> None:
        # Literal addresses keep resolver delays outside the bounded probe path.
        ip_address(self.host)
        if not 1 <= self.port <= 65535:
            raise ValueError("invalid ingest port")


@dataclass
class Prober:
    public_hosts: list[str]
    run: RunCommand = _run
    gateway: Callable[[], str] = _default_gateway
    connect: Callable[[str, int], bool] = _tcp_connect

    def __post_init__(self) -> None:
        if len(self.public_hosts) > 4:
            raise ValueError("at most four public probes are allowed")
        for host in self.public_hosts:
            ip_address(host)

    def probe(self, endpoint: Endpoint, *, budget_sec: float = 1.0) -> Network:
        def ping(host: str) -> bool:
            return bool(host) and self.run(["ping", "-n", "-c", "1", "-W", "1", host]) == 0

        pool = ThreadPoolExecutor(max_workers=2 + len(self.public_hosts))
        gateway = pool.submit(lambda: ping(self.gateway()))
        public = [pool.submit(ping, host) for host in self.public_hosts]
        ingest = pool.submit(self.connect, endpoint.host, endpoint.port)
        futures = [gateway, *public, ingest]
        _, pending = wait(futures, timeout=budget_sec)
        pool.shutdown(wait=False, cancel_futures=True)

        def succeeded(future: Any) -> bool:
            return future.done() and not future.cancelled() and future.exception() is None and bool(future.result())

        return Network(succeeded(gateway), sum(succeeded(f) for f in public), succeeded(ingest), bool(pending))
