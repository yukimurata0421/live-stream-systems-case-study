from __future__ import annotations

import hashlib
import json
import os
import socket
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

RESTART_FFMPEG = "RESTART_FFMPEG"
RECONCILE_FFMPEG = "RECONCILE_FFMPEG"
ESCALATE_RUNTIME_RECOVERY = "ESCALATE_RUNTIME_RECOVERY"


def _request_effect_executor(socket_path: Path, payload: dict[str, Any], *, timeout_seconds: float) -> dict[str, Any]:
    """Use the narrow Unix protocol without importing executor-side packages."""

    raw = (json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n").encode()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout_seconds)
        client.connect(str(socket_path))
        client.sendall(raw)
        response = bytearray()
        while b"\n" not in response:
            chunk = client.recv(65536)
            if not chunk:
                break
            response.extend(chunk)
            if len(response) > 1_048_576:
                raise ValueError("RESPONSE_TOO_LARGE")
    value = json.loads(bytes(response).split(b"\n", 1)[0])
    if not isinstance(value, dict):
        raise ValueError("RESPONSE_INVALID")
    return value


def read_runtime_observation(path: Path, *, max_age_seconds: float = 3.0) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(value, dict) or str(value.get("schema_version") or "") != "runtime.ffmpeg_observation.v1":
        return {}
    try:
        observed = datetime.fromisoformat(str(value.get("observed_at") or "").replace("Z", "+00:00"))
    except ValueError:
        return {}
    age = (datetime.now(UTC) - observed.astimezone(UTC)).total_seconds()
    if age < -1.0 or age > max_age_seconds:
        return {}
    return value


def execute_effect_request(
    *,
    socket_path: Path,
    runtime_observation_path: Path,
    producer_id: str,
    producer_generation: int,
    reason: str,
    correlation_id: str,
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    return execute_typed_effect_request(
        socket_path=socket_path,
        runtime_observation_path=runtime_observation_path,
        producer_id=producer_id,
        producer_generation=producer_generation,
        intent_type=RESTART_FFMPEG,
        failure_domain="DELIVERY_PATH",
        reason=reason,
        correlation_id=correlation_id,
        timeout_seconds=timeout_seconds,
    )


def _stable_request_id(intent_type: str, identity: dict[str, Any]) -> str:
    canonical = json.dumps(
        {"intent_type": intent_type, "exact_identity": identity},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"dell-target-{hashlib.sha256(canonical).hexdigest()}"


def execute_typed_effect_request(
    *,
    socket_path: Path,
    runtime_observation_path: Path,
    producer_id: str,
    producer_generation: int,
    intent_type: str,
    failure_domain: str,
    reason: str,
    correlation_id: str,
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    observation = read_runtime_observation(runtime_observation_path)
    if not observation:
        return {"ok": False, "state": "REJECTED", "reason": "RUNTIME_OBSERVATION_UNAVAILABLE"}
    if intent_type not in {RESTART_FFMPEG, RECONCILE_FFMPEG, ESCALATE_RUNTIME_RECOVERY}:
        return {"ok": False, "state": "REJECTED", "reason": "INTENT_NOT_SUPPORTED"}
    target = observation.get("target_identity")
    runtime_identity = observation.get("runtime_identity")
    generation = str(observation.get("ffmpeg_generation") or "")
    executor_instance_id = str(observation.get("executor_instance_id") or "")
    target_snapshot_id = str(observation.get("target_snapshot_id") or "")
    runtime_snapshot_id = str(observation.get("runtime_snapshot_id") or "")
    observation_id = str(observation.get("observation_id") or "")
    if not executor_instance_id or not observation_id:
        return {"ok": False, "state": "REJECTED", "reason": "EXECUTOR_IDENTITY_UNAVAILABLE"}
    if intent_type == RESTART_FFMPEG:
        if str(observation.get("target_snapshot_status") or "") != "VALID" or not isinstance(target, dict):
            return {"ok": False, "state": "REJECTED", "reason": "TARGET_IDENTITY_UNAVAILABLE"}
        if not generation or not target_snapshot_id or not bool(observation.get("ffmpeg_running")):
            return {"ok": False, "state": "REJECTED", "reason": "FFMPEG_GENERATION_UNAVAILABLE"}
        fence_identity = dict(target)
    else:
        if str(observation.get("runtime_snapshot_status") or "") != "VALID" or not isinstance(runtime_identity, dict):
            return {"ok": False, "state": "REJECTED", "reason": "RUNTIME_IDENTITY_UNAVAILABLE"}
        if not runtime_snapshot_id:
            return {"ok": False, "state": "REJECTED", "reason": "RUNTIME_SNAPSHOT_ID_UNAVAILABLE"}
        fence_identity = dict(runtime_identity)
    now = datetime.now(UTC)
    operation_id = _stable_request_id(intent_type, fence_identity)
    request = {
        "schema_version": "runtime.typed_effect_request.v2",
        "request_id": operation_id,
        "producer_id": producer_id,
        "producer_generation": producer_generation,
        "intent_type": intent_type,
        "reason": reason[:512],
        "failure_domain": failure_domain,
        "issued_at": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "expires_at": (now + timedelta(seconds=5)).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "ffmpeg_target_identity": target if intent_type == RESTART_FFMPEG else None,
        "runtime_identity": runtime_identity if intent_type != RESTART_FFMPEG else None,
        "expected_ffmpeg_generation": generation if intent_type == RESTART_FFMPEG else "",
        "idempotency_key": operation_id,
        "correlation_id": correlation_id or operation_id,
        "target_snapshot_id": target_snapshot_id if intent_type == RESTART_FFMPEG else "",
        "runtime_snapshot_id": runtime_snapshot_id if intent_type != RESTART_FFMPEG else "",
        "runtime_observation_id": observation_id,
        "expected_executor_instance_id": executor_instance_id,
        "maintenance_evidence_status": str(observation.get("maintenance_evidence_status") or "UNKNOWN"),
        "projection_id": str(observation.get("projection_id") or ""),
        "projection_sequence": int(observation.get("projection_sequence") or 0),
    }
    try:
        return _request_effect_executor(socket_path, request, timeout_seconds=timeout_seconds)
    except TimeoutError:
        # This is a replay/query of the same durable request, never a new
        # physical attempt.  Both the stable idempotency key and the executor's
        # exact-target fence converge the response after a transport timeout.
        time.sleep(min(0.25, max(0.01, timeout_seconds / 4)))
        return _request_effect_executor(socket_path, request, timeout_seconds=max(2.0, timeout_seconds))


def unresolved_effect_scopes(*, socket_path: Path, timeout_seconds: float = 2.0) -> dict[str, Any]:
    """Read executor-owned unresolved scopes without proposing a new action."""

    response = _request_effect_executor(
        socket_path,
        {"schema_version": "runtime.effect_unresolved_query.v1"},
        timeout_seconds=timeout_seconds,
    )
    scopes = response.get("unresolved_scopes")
    count = response.get("unresolved_count")
    if (
        response.get("schema_version") != "runtime.effect_unresolved_response.v1"
        or response.get("ok") is not True
        or not isinstance(scopes, list)
        or isinstance(count, bool)
        or not isinstance(count, int)
        or count != len(scopes)
        or not all(isinstance(item, dict) for item in scopes)
    ):
        raise ValueError("UNRESOLVED_EFFECT_RESPONSE_INVALID")
    return response


def effect_request_status(
    *,
    socket_path: Path,
    request_id: str,
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    """Read one durable request result without replaying or proposing an effect."""

    response = _request_effect_executor(
        socket_path,
        {
            "schema_version": "runtime.effect_request_status_query.v1",
            "request_id": request_id,
        },
        timeout_seconds=timeout_seconds,
    )
    if (
        response.get("schema_version") != "runtime.effect_request_status_response.v1"
        or not isinstance(response.get("ok"), bool)
        or str(response.get("request_id") or "") != request_id
        or not isinstance(response.get("state"), str)
        or not isinstance(response.get("effect_scope_state"), str)
        or isinstance(response.get("physical_attempt_count"), bool)
        or not isinstance(response.get("physical_attempt_count"), int)
    ):
        raise ValueError("EFFECT_REQUEST_STATUS_RESPONSE_INVALID")
    return response


def effect_request_status_by_correlation(
    *,
    socket_path: Path,
    correlation_id: str,
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    """Resolve a controller action ID to exactly one durable request."""

    response = _request_effect_executor(
        socket_path,
        {
            "schema_version": "runtime.effect_correlation_status_query.v1",
            "correlation_id": correlation_id,
        },
        timeout_seconds=timeout_seconds,
    )
    matching_count = response.get("matching_count")
    if (
        response.get("schema_version") != "runtime.effect_correlation_status_response.v1"
        or not isinstance(response.get("ok"), bool)
        or str(response.get("correlation_id") or "") != correlation_id
        or isinstance(matching_count, bool)
        or not isinstance(matching_count, int)
        or matching_count < 0
        or not isinstance(response.get("request_id"), str)
        or not isinstance(response.get("state"), str)
        or not isinstance(response.get("effect_scope_state"), str)
        or isinstance(response.get("physical_attempt_count"), bool)
        or not isinstance(response.get("physical_attempt_count"), int)
        or (response.get("ok") is True and (matching_count != 1 or not response.get("request_id")))
        or (response.get("ok") is False and matching_count == 1)
    ):
        raise ValueError("EFFECT_CORRELATION_STATUS_RESPONSE_INVALID")
    return response


def reconcile_delayed_effect(
    *,
    socket_path: Path,
    unresolved_scope: dict[str, Any],
    runtime_observation: dict[str, Any],
    transport: dict[str, Any],
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    """Append a delayed-exit or retired-target conclusion; never invoke an effect."""

    before = unresolved_scope.get("identity")
    observed = runtime_observation.get("target_identity")
    if not isinstance(before, dict) or not isinstance(observed, dict):
        raise TypeError("RECONCILIATION_TARGET_IDENTITY_INVALID")
    if runtime_observation.get("target_snapshot_status") != "VALID":
        raise ValueError("RECONCILIATION_TARGET_SNAPSHOT_NOT_VALID")
    scope_id = str(unresolved_scope.get("effect_scope_id") or "")
    owner_request_id = str(unresolved_scope.get("owner_request_id") or "")
    owner_request_digest = str(unresolved_scope.get("owner_request_digest") or "")
    observation_id = str(runtime_observation.get("observation_id") or "")
    if not all((scope_id, owner_request_id, owner_request_digest, observation_id)):
        raise ValueError("RECONCILIATION_IDENTITY_MISSING")
    same_runtime = all(
        before.get(name) == observed.get(name)
        for name in ("host_id", "host_boot_id", "namespace", "pod_uid", "container_name", "container_id")
    )
    target_retired = all(before.get(name) == observed.get(name) for name in ("host_id", "namespace", "container_name")) and any(
        before.get(name) != observed.get(name) for name in ("host_boot_id", "pod_uid", "container_id")
    )
    if same_runtime:
        if before.get("ffmpeg_generation") == observed.get("ffmpeg_generation") or before.get("ffmpeg_pid") == observed.get("ffmpeg_pid"):
            raise ValueError("RECONCILIATION_SUCCESSOR_NOT_DISTINCT")
        resolution = "EFFECT_OBSERVED"
    elif target_retired:
        resolution = "TARGET_RETIRED"
    else:
        raise ValueError("RECONCILIATION_TARGET_RELATION_UNSAFE")
    stable = {
        "effect_scope_id": scope_id,
        "owner_request_id": owner_request_id,
        "resolution": resolution,
        "observed_host_boot_id": observed.get("host_boot_id"),
        "observed_pod_uid": observed.get("pod_uid"),
        "observed_container_id": observed.get("container_id"),
        "observed_ffmpeg_generation": observed.get("ffmpeg_generation"),
        "observed_ffmpeg_pid": observed.get("ffmpeg_pid"),
    }
    reconciliation_id = (
        f"dell-auto-reconcile-{hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}"
    )
    common_evidence = {
        "observed_at": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "automatic_retry_count": 0,
        "before_target": before,
        "observed_target": observed,
        "runtime_observation_id": observation_id,
    }
    if resolution == "EFFECT_OBSERVED":
        evidence = {
            "schema_version": "runtime.delayed_exit_reconciliation_evidence.v2",
            "oracle": "DELAYED_FFMPEG_EXIT_AND_SUCCESSOR_ACK_PROGRESS",
            "physical_effect_count": 1,
            **common_evidence,
            "transport": {
                "bytes_sent": transport.get("bytes_sent"),
                "network_down": transport.get("network_down"),
                "tcp_probe_ok": transport.get("tcp_probe_ok"),
                "ffmpeg_generation": transport.get("ffmpeg_generation"),
                "ack_observation_count": transport.get("ack_observation_count"),
                "bytes_acked_start": transport.get("bytes_acked_start"),
                "bytes_acked_end": transport.get("bytes_acked_end"),
                "bytes_acked_delta": transport.get("bytes_acked_delta"),
                "ack_samples": transport.get("ack_samples"),
            },
        }
    else:
        evidence = {
            "schema_version": "runtime.retired_target_reconciliation_evidence.v1",
            "oracle": "EXACT_TARGET_RETIRED_AND_HEALTHY_REPLACEMENT",
            "physical_attempt_count": 1,
            "physical_effect_outcome": "UNKNOWN",
            **common_evidence,
            "transport": {
                "bytes_sent": transport.get("bytes_sent"),
                "network_down": transport.get("network_down"),
                "tcp_probe_ok": transport.get("tcp_probe_ok"),
            },
        }
    request = {
        "schema_version": "runtime.effect_reconciliation_request.v1",
        "reconciliation_id": reconciliation_id,
        "effect_scope_id": scope_id,
        "owner_request_id": owner_request_id,
        "owner_request_digest": owner_request_digest,
        "resolution": resolution,
        "evidence": evidence,
    }
    return _request_effect_executor(socket_path, request, timeout_seconds=timeout_seconds)


def configured_paths() -> dict[str, Path]:
    return {
        "socket": Path(os.environ["FR_EFFECT_EXECUTOR_SOCKET"]),
        "runtime_observation": Path(os.environ["FR_RUNTIME_OBSERVATION_FILE"]),
        "escalation_socket": Path(os.environ.get("FR_RUNTIME_RECOVERY_SOCKET", "/run/stream-v3-control/runtime-recovery.sock")),
        "escalation_observation": Path(
            os.environ.get(
                "FR_RUNTIME_RECOVERY_OBSERVATION_FILE",
                "/run/stream-v3-control/runtime-recovery-observation.json",
            )
        ),
    }
