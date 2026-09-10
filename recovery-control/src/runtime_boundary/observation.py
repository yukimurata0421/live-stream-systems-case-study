from __future__ import annotations

import json
import os
import re
import subprocess
import threading
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .model import ProtocolError, parse_utc
from .target import RuntimeSnapshotReader, TargetSnapshotReader


def _utc_text() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _extract_int(text: str, key: str) -> int:
    match = re.search(rf"\b{re.escape(key)}:(\d+)", text)
    return int(match.group(1)) if match else 0


def _tcp_metrics(pid: int, ports: tuple[int, ...]) -> dict[str, int | str]:
    if pid <= 1:
        return {}
    completed = subprocess.run(
        ["ss", "-tinp"],
        check=False,
        capture_output=True,
        text=True,
        timeout=2.0,
    )
    if completed.returncode != 0:
        return {}
    lines = completed.stdout.splitlines()
    token = f"pid={pid},"
    for index, line in enumerate(lines):
        if "ESTAB" not in line or token not in line:
            continue
        fields = line.split()
        peer = fields[4] if len(fields) >= 5 else ""
        if not any(peer.endswith(f":{port}") or peer.endswith(f"]:{port}") for port in ports):
            continue
        detail = lines[index + 1] if index + 1 < len(lines) else ""
        return {
            "send_q": int(fields[2]) if len(fields) >= 3 and fields[2].isdigit() else 0,
            "bytes_sent": _extract_int(detail, "bytes_sent"),
            "bytes_acked": _extract_int(detail, "bytes_acked"),
            "notsent": _extract_int(detail, "notsent"),
            "unacked": _extract_int(detail, "unacked"),
            "lastsnd_ms": _extract_int(detail, "lastsnd"),
            "rto_ms": _extract_int(detail, "rto"),
        }
    return {}


def atomic_write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(dict(payload), handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class RuntimeObservationPublisher:
    def __init__(
        self,
        *,
        path: Path,
        projection_path: Path,
        target_snapshot_path: Path,
        process_supplier: Callable[[], Mapping[str, Any]],
        producer_instance_id: str | None = None,
        interval_seconds: float = 1.0,
        rtmp_ports: tuple[int, ...] = (443,),
        recovery_evidence_cycle: Callable[[], None] | None = None,
    ) -> None:
        self.path = path
        self.projection_path = projection_path
        self.target_reader = TargetSnapshotReader(target_snapshot_path)
        self.runtime_reader = RuntimeSnapshotReader(target_snapshot_path)
        self.process_supplier = process_supplier
        self.interval_seconds = max(0.2, interval_seconds)
        self.rtmp_ports = rtmp_ports
        self.recovery_evidence_cycle = recovery_evidence_cycle
        self.instance_id = producer_instance_id or f"runtime-executor-{uuid.uuid4()}"
        self.sequence = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="runtime-observation-publisher", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def publish(self) -> dict[str, Any]:
        process = dict(self.process_supplier())
        local_pid = int(process.get("local_ffmpeg_pid") or 0)
        projection: dict[str, Any] = {}
        try:
            value = json.loads(self.projection_path.read_text(encoding="utf-8"))
            projection = value if isinstance(value, dict) else {}
        except (OSError, json.JSONDecodeError):
            projection = {}
        target_decision = self.target_reader.read()
        runtime_decision = self.runtime_reader.read()
        target = target_decision.target_identity
        maintenance_evidence_status = "UNKNOWN"
        if projection:
            if str(projection.get("schema_version") or "") != "maintenance.snapshot_projection.v1":
                maintenance_evidence_status = "INVALID"
            else:
                try:
                    maintenance_evidence_status = "AVAILABLE" if parse_utc(projection.get("fresh_until")) > datetime.now(UTC) else "STALE"
                except ProtocolError:
                    maintenance_evidence_status = "INVALID"
        self.sequence += 1
        observation = {
            "schema_version": "runtime.ffmpeg_observation.v1",
            "observation_id": f"runtime-observation-{uuid.uuid4()}",
            "producer_id": "stream-engine-effect-executor",
            "executor_instance_id": self.instance_id,
            "producer_instance_id": self.instance_id,
            "sequence": self.sequence,
            "observed_at": _utc_text(),
            "local_ffmpeg_pid": local_pid,
            "protocol_ffmpeg_pid": int(target.get("ffmpeg_pid") or 0) if target else 0,
            "ffmpeg_generation": str(process.get("ffmpeg_generation") or ""),
            "ffmpeg_uptime_sec": int(process.get("ffmpeg_uptime_sec") or 0),
            "ffmpeg_running": bool(process.get("ffmpeg_running")),
            "pod_uid": str(process.get("pod_uid") or ""),
            "pod_name": str(process.get("pod_name") or ""),
            "stream_established": bool(process.get("stream_established")),
            "active_producer_id": str(process.get("active_producer_id") or ""),
            "active_producer_generation": int(process.get("active_producer_generation") or 0),
            "authority_version": int(process.get("authority_version") or 0),
            "in_flight_count": int(process.get("in_flight_count") or 0),
            "runtime_lifecycle_state": str(process.get("runtime_lifecycle_state") or "UNKNOWN"),
            "managed_child_cardinality": str(process.get("managed_child_cardinality") or "UNKNOWN"),
            "target_identity": target,
            "target_snapshot_id": target_decision.snapshot_id,
            "target_snapshot_valid_until": target_decision.valid_until,
            "target_snapshot_reason": target_decision.reason,
            "projection_id": str(projection.get("projection_id") or ""),
            "projection_sequence": int(projection.get("projection_sequence") or 0),
            "maintenance_evidence_status": maintenance_evidence_status,
            "target_snapshot_status": "VALID" if target_decision.available else "UNKNOWN",
            "runtime_identity": runtime_decision.runtime_identity,
            "runtime_snapshot_id": runtime_decision.snapshot_id,
            "runtime_snapshot_valid_until": runtime_decision.valid_until,
            "runtime_snapshot_reason": runtime_decision.reason,
            "runtime_snapshot_status": "VALID" if runtime_decision.available else "UNKNOWN",
            "runtime_container_ready": runtime_decision.container_ready,
            "tcp_metrics": _tcp_metrics(local_pid, self.rtmp_ports),
            "production_behavior_modified": False,
        }
        atomic_write(self.path, observation)
        if self.recovery_evidence_cycle is not None:
            with suppress(Exception):
                self.recovery_evidence_cycle()
        return observation

    def _run(self) -> None:
        while not self._stop.is_set():
            with suppress(BaseException):
                self.publish()
            self._stop.wait(self.interval_seconds)
