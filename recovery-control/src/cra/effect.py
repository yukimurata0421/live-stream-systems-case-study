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
    value = {
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
    return value


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


def read_status(socket_path: Path, request_id: str) -> dict[str, Any]:
    return send(socket_path, {"schema_version": "runtime.effect_request_status_query.v1", "request_id": request_id}, timeout_sec=0.5)


def query_unresolved(socket_path: Path) -> list[dict[str, Any]]:
    """Executor-owned unresolved fences (read-only)."""
    response = send(socket_path, {"schema_version": "runtime.effect_unresolved_query.v1"}, timeout_sec=0.5)
    scopes = response.get("unresolved_scopes")
    if response.get("ok") is not True or not isinstance(scopes, list):
        raise ValueError("UNRESOLVED_RESPONSE_INVALID")
    return [scope for scope in scopes if isinstance(scope, dict)]


def build_reconciliation(scope: dict[str, Any], obs: Observation, *, now: datetime) -> dict[str, Any]:
    """Close the agent's own EFFECT_OBSERVED_AWAITING_VERIFICATION fence after verified recovery.

    Uses the Executor's existing v1 evidence (the same one the legacy controller sends): the
    fenced target, a distinct current successor, sending progress and a healthy local network.
    The caller has already checked the network; this cannot request an effect.
    """
    tcp = obs.tcp
    stable = {
        "effect_scope_id": scope["effect_scope_id"],
        "owner_request_id": scope["owner_request_id"],
        "observed_ffmpeg_generation": obs.ffmpeg_generation,
        "observed_ffmpeg_pid": (obs.raw.get("target_identity") or {}).get("ffmpeg_pid"),
    }
    digest = hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {
        "schema_version": "runtime.effect_reconciliation_request.v1",
        "reconciliation_id": f"cra-reconcile-{digest[:40]}",
        "effect_scope_id": scope["effect_scope_id"],
        "owner_request_id": scope["owner_request_id"],
        "owner_request_digest": scope["owner_request_digest"],
        "resolution": "EFFECT_OBSERVED",
        "evidence": {
            "schema_version": "runtime.delayed_exit_reconciliation_evidence.v1",
            "oracle": "DELAYED_FFMPEG_EXIT_AND_HEALTHY_SUCCESSOR",
            "observed_at": _utc(now),
            "physical_effect_count": 1,
            "automatic_retry_count": 0,
            "before_target": scope["identity"],
            "observed_target": obs.raw.get("target_identity"),
            "runtime_observation_id": obs.observation_id,
            "transport": {"bytes_sent": tcp.bytes_sent if tcp else 0, "network_down": False, "tcp_probe_ok": True},
        },
    }


def classify(response: dict[str, Any], *, expected_request_id: str | None = None) -> str:
    """Map an Executor response to the agent's action state."""
    state = str(response.get("state") or "")
    if expected_request_id is not None and response.get("request_id") != expected_request_id:
        return "OUTCOME_UNKNOWN"
    if response.get("ok") is True and state == "EFFECT_OBSERVED":
        return "EFFECT_OBSERVED"
    if response.get("ok") is False and state in {"REJECTED", "EFFECT_FAILED"}:
        return state  # Executor guarantees no signal was sent for these.
    if response.get("ok") is False and response.get("reason") == "TARGET_EFFECT_UNRESOLVED":
        # The ledger refuses before recording the request: another request's fence is still open.
        # (A request already recorded under this id is answered from its own record instead.)
        return "REJECTED"
    return "OUTCOME_UNKNOWN"
