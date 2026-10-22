"""Client of the in-Pod stream-engine Effect Executor (runtime_boundary EffectExecutorServer).

Request format follows stream_v3 watchers/fast_recovery_core/effect_contract.py
(runtime.typed_effect_request.v2). The request_id and idempotency_key are derived from the
FFmpeg generation, so a crashed or restarted agent can never mint a second request for the
same generation; the Executor's own effect-scope fence is a second, independent guard.
"""

from __future__ import annotations

import hashlib
import json
import socket
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from cra.observe import Observation

MAX_MESSAGE = 65536


def request_id_for(generation_key: str) -> str:
    return "cra-" + hashlib.sha256(generation_key.encode()).hexdigest()[:40]


def _utc(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_restart_request(obs: Observation, *, now: datetime, producer_id: str, producer_generation: int, reason: str) -> dict[str, Any]:
    raw = obs.raw
    request_id = request_id_for(obs.generation_key)
    return {
        "schema_version": "runtime.typed_effect_request.v2",
        "request_id": request_id,
        "producer_id": producer_id,
        "producer_generation": producer_generation,
        "intent_type": "RESTART_FFMPEG",
        "reason": reason[:512],
        "failure_domain": "DELIVERY_PATH",
        "issued_at": _utc(now),
        "expires_at": _utc(now + timedelta(seconds=3)),
        "ffmpeg_target_identity": raw.get("target_identity"),
        "runtime_identity": None,
        "expected_ffmpeg_generation": obs.ffmpeg_generation,
        "idempotency_key": request_id,
        "correlation_id": request_id,
        "target_snapshot_id": str(raw.get("target_snapshot_id") or ""),
        "runtime_snapshot_id": "",
        "runtime_observation_id": obs.observation_id,
        "expected_executor_instance_id": obs.executor_instance_id,
        "maintenance_evidence_status": str(raw.get("maintenance_evidence_status") or "UNKNOWN"),
        "projection_id": str(raw.get("projection_id") or ""),
        "projection_sequence": int(raw.get("projection_sequence") or 0),
    }


def send(socket_path: Path, request: dict[str, Any], *, timeout_sec: float = 5.0) -> dict[str, Any]:
    payload = json.dumps(request, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    if len(payload) > MAX_MESSAGE:
        raise ValueError("REQUEST_TOO_LARGE")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(timeout_sec)
        conn.connect(str(socket_path))
        conn.sendall(payload)
        response = bytearray()
        while b"\n" not in response and len(response) <= MAX_MESSAGE:
            chunk = conn.recv(4096)
            if not chunk:
                break
            response.extend(chunk)
    value = json.loads(bytes(response).split(b"\n", 1)[0])
    if not isinstance(value, dict):
        raise TypeError("RESPONSE_INVALID")
    return value


def classify(response: dict[str, Any]) -> str:
    """Map an Executor response to the agent's action state."""
    state = str(response.get("state") or "")
    if response.get("ok") is True and state == "EFFECT_OBSERVED":
        return "EFFECT_OBSERVED"
    if response.get("ok") is False and state in {"REJECTED", "EFFECT_FAILED"}:
        return state  # Executor guarantees no signal was sent for these.
    return "OUTCOME_UNKNOWN"
