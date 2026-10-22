"""Shared, standard-library-only evidence contract, also vendored into the runtime candidate.

This module observes sockets and lifecycle events. It never signals, launches, or authorizes processes.
"""

from __future__ import annotations

import ipaddress
import os
import re
import subprocess
import threading
from pathlib import Path
from typing import Any

TRANSPORT_SCHEMA = "runtime.delivery_transport.v1"
SUCCESSOR_SCHEMA = "runtime.recovery_successor.v1"
PROTOCOL = "runtime.typed_effect_request.v4"
FIELDS = {
    "schema",
    "executor_instance_id",
    "pod_uid",
    "generation",
    "pid",
    "start_ticks",
    "socket_inode",
    "local_host",
    "local_port",
    "peer_host",
    "peer_port",
    "route",
    "route_generation",
}


def validate_transport(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != FIELDS or value.get("schema") != TRANSPORT_SCHEMA:
        raise ValueError("TRANSPORT_NOT_EXACT")
    if value["route"] not in {"PRIMARY", "FALLBACK"}:
        raise ValueError("TRANSPORT_ROUTE_INVALID")
    for name in ("executor_instance_id", "pod_uid", "generation"):
        if not isinstance(value[name], str) or not value[name]:
            raise ValueError("TRANSPORT_IDENTITY_MISSING")
    for name in ("pid", "start_ticks", "socket_inode", "local_port", "peer_port", "route_generation"):
        if type(value[name]) is not int or value[name] <= 0:
            raise ValueError("TRANSPORT_INTEGER_INVALID")
    if value["pid"] <= 1 or max(value["peer_port"], value["local_port"]) > 65535:
        raise ValueError("TRANSPORT_ENDPOINT_INVALID")
    for name in ("local_host", "peer_host"):
        if str(ipaddress.ip_address(value[name])) != value[name]:
            raise ValueError("TRANSPORT_ADDRESS_NOT_CANONICAL")
    return dict(value)


def _endpoint(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    address = ipaddress.ip_address(host.strip("[]"))
    return str(address), int(port)


def _counter(text: str, name: str) -> int:
    match = re.search(rf"\b{re.escape(name)}:(\d+)", text)
    return int(match[1]) if match else 0


def parse_sockets(text: str, pid: int, ports: tuple[int, ...]) -> list[dict[str, Any]]:
    """Return all matching sockets; never pick the first one as evidence of uniqueness."""
    lines = text.splitlines()
    rows = []
    for index, line in enumerate(lines):
        if "ESTAB" not in line or f"pid={pid}," not in line:
            continue
        fields = line.split()
        try:
            local_host, local_port = _endpoint(fields[3])
            peer_host, peer_port = _endpoint(fields[4])
        except (IndexError, ValueError):
            continue
        if peer_port not in ports:
            continue
        detail = lines[index + 1] if index + 1 < len(lines) else ""
        metrics = {
            name: _counter(detail, key)
            for name, key in (
                ("bytes_sent", "bytes_sent"),
                ("bytes_acked", "bytes_acked"),
                ("notsent", "notsent"),
                ("unacked", "unacked"),
                ("lastsnd_ms", "lastsnd"),
                ("rto_ms", "rto"),
            )
        }
        metrics["send_q"] = int(fields[2]) if fields[2].isdigit() else 0
        rows.append(
            {
                "local_host": local_host,
                "local_port": local_port,
                "peer_host": peer_host,
                "peer_port": peer_port,
                "socket_inode": _counter(line + " " + detail, "ino"),
                "tcp_metrics": metrics,
            }
        )
    return rows


def _start_ticks(pid: int) -> int:
    return int(Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()[19])


def collect_transport(process: dict[str, Any], executor: str, ports: tuple[int, ...], *, timeout_sec: float = 2.0) -> dict[str, Any]:
    """A bounded read-only socket sample. Preserve legacy metrics separately for the old controller."""
    result: dict[str, Any] = {"status": "UNAVAILABLE", "binding": None, "tcp_metrics": {}, "legacy_tcp_metrics": {}}
    pid = int(process.get("local_ffmpeg_pid") or 0)
    if pid <= 1:
        return result
    try:
        start = _start_ticks(pid)
        completed = subprocess.run(["ss", "-tinep"], capture_output=True, text=True, check=False, timeout=timeout_sec)
        if completed.returncode != 0:
            return result
        sockets = parse_sockets(completed.stdout, pid, ports)
        if sockets:
            result["legacy_tcp_metrics"] = sockets[0]["tcp_metrics"]
        if len(sockets) != 1:
            result["status"] = "AMBIGUOUS" if sockets else "UNAVAILABLE"
            return result
        socket = sockets[0]
        descriptors = list(Path(f"/proc/{pid}/fd").iterdir())
        if len(descriptors) > 256:
            result["reason"] = "FD_LIMIT"
            return result
        owned = False
        for descriptor in descriptors:
            try:
                owned |= os.readlink(descriptor) == f"socket:[{socket['socket_inode']}]"
            except OSError:
                continue
        if not owned or start != _start_ticks(pid):
            result["reason"] = "SOCKET_OR_PROCESS_CHANGED"
            return result
        binding = validate_transport(
            {
                "schema": TRANSPORT_SCHEMA,
                "executor_instance_id": executor,
                "pod_uid": process.get("pod_uid"),
                "generation": process.get("ffmpeg_generation"),
                "pid": pid,
                "start_ticks": start,
                "route": process.get("delivery_route"),
                "route_generation": process.get("delivery_route_generation"),
                **{k: v for k, v in socket.items() if k != "tcp_metrics"},
            }
        )
        result.update(status="AVAILABLE", binding=binding, tcp_metrics=socket["tcp_metrics"])
    except (OSError, ValueError, TypeError, IndexError, subprocess.TimeoutExpired) as exc:
        result["reason"] = type(exc).__name__
    return result


class RecoveryLinks:
    """Connect only an owner's exact exit event to its exact successor start event.

    Event IDs refer to the existing durable owner journal. A new executor starts unbound.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.exit: dict[str, Any] = {}
        self.link: dict[str, Any] | None = None

    def record(self, kind: str, event_id: str, fields: dict[str, Any], executor: str, pod_uid: str) -> None:
        with self.lock:
            if kind == "ffmpeg_exited":
                self.exit = {**fields, "event_id": event_id}
            elif kind == "ffmpeg_started":
                self.link = None
                old = self.exit
                self.exit = {}
                request = fields.get("recovery_action_id")
                if (
                    request
                    and request == old.get("recovery_action_id")
                    and event_id
                    and executor
                    and pod_uid
                    and old.get("event_id")
                    and fields.get("prior_exit_event_id") == old["event_id"]
                    and type(fields.get("replaces_ffmpeg_pid")) is int
                    and fields["replaces_ffmpeg_pid"] > 1
                    and fields["replaces_ffmpeg_pid"] == old.get("ffmpeg_pid")
                    and old.get("ffmpeg_generation")
                    and fields.get("ffmpeg_generation")
                    and old["ffmpeg_generation"] != fields["ffmpeg_generation"]
                ):
                    self.link = {
                        "schema": SUCCESSOR_SCHEMA,
                        "request_id": request,
                        "executor_instance_id": executor,
                        "pod_uid": pod_uid,
                        "previous_generation": old["ffmpeg_generation"],
                        "generation": fields["ffmpeg_generation"],
                        "exit_event_id": old["event_id"],
                        "start_event_id": event_id,
                    }

    def current(self, generation: str) -> dict[str, Any] | None:
        with self.lock:
            return dict(self.link) if self.link and self.link["generation"] == generation else None


def linked_successor(value: Any, *, request_id: str, executor: str, pod_uid: str, before: str, after: str) -> bool:
    if not isinstance(value, dict):
        return False
    expected = {
        "schema": SUCCESSOR_SCHEMA,
        "request_id": request_id,
        "executor_instance_id": executor,
        "pod_uid": pod_uid,
        "previous_generation": before,
        "generation": after,
        "exit_event_id": value.get("exit_event_id"),
        "start_event_id": value.get("start_event_id"),
    }
    return (
        value == expected
        and before != after
        and bool(before)
        and bool(after)
        and all(isinstance(value.get(k), str) and value[k] for k in expected)
    )
