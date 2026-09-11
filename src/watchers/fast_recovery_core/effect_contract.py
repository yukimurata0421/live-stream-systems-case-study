from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

RESTART_FFMPEG = "RESTART_FFMPEG"
RECONCILE_FFMPEG = "RECONCILE_FFMPEG"
ESCALATE_RUNTIME_RECOVERY = "ESCALATE_RUNTIME_RECOVERY"


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
    from runtime_boundary import EffectClient

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
    else:
        if str(observation.get("runtime_snapshot_status") or "") != "VALID" or not isinstance(
            runtime_identity, dict
        ):
            return {"ok": False, "state": "REJECTED", "reason": "RUNTIME_IDENTITY_UNAVAILABLE"}
        if not runtime_snapshot_id:
            return {"ok": False, "state": "REJECTED", "reason": "RUNTIME_SNAPSHOT_ID_UNAVAILABLE"}
    now = datetime.now(UTC)
    operation_id = correlation_id or f"fra-{uuid.uuid4()}"
    request = {
        "schema_version": "runtime.typed_effect_request.v2",
        "request_id": operation_id,
        "producer_id": producer_id,
        "producer_generation": producer_generation,
        "intent_type": intent_type,
        "reason": reason[:512],
        "failure_domain": failure_domain,
        "issued_at": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "expires_at": (now + timedelta(seconds=3)).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "ffmpeg_target_identity": target if intent_type == RESTART_FFMPEG else None,
        "runtime_identity": runtime_identity if intent_type != RESTART_FFMPEG else None,
        "expected_ffmpeg_generation": generation if intent_type == RESTART_FFMPEG else "",
        "idempotency_key": operation_id,
        "correlation_id": operation_id,
        "target_snapshot_id": target_snapshot_id if intent_type == RESTART_FFMPEG else "",
        "runtime_snapshot_id": runtime_snapshot_id if intent_type != RESTART_FFMPEG else "",
        "runtime_observation_id": observation_id,
        "expected_executor_instance_id": executor_instance_id,
        "maintenance_evidence_status": str(observation.get("maintenance_evidence_status") or "UNKNOWN"),
        "projection_id": str(observation.get("projection_id") or ""),
        "projection_sequence": int(observation.get("projection_sequence") or 0),
    }
    return EffectClient(socket_path, timeout_seconds=timeout_seconds).execute(request)


def unresolved_effect_scopes(*, socket_path: Path, timeout_seconds: float = 2.0) -> dict[str, Any]:
    """Read executor-owned unresolved scopes without proposing a new action."""

    from runtime_boundary import EffectClient

    response = EffectClient(socket_path, timeout_seconds=timeout_seconds).unresolved()
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


def reconcile_delayed_effect(
    *,
    socket_path: Path,
    unresolved_scope: dict[str, Any],
    runtime_observation: dict[str, Any],
    transport: dict[str, Any],
    timeout_seconds: float = 2.0,
) -> dict[str, Any]:
    """Append a delayed-exit conclusion; this path cannot invoke an effect."""

    from runtime_boundary import EffectClient

    before = unresolved_scope.get("identity")
    observed = runtime_observation.get("target_identity")
    if not isinstance(before, dict) or not isinstance(observed, dict):
        raise TypeError("RECONCILIATION_TARGET_IDENTITY_INVALID")
    scope_id = str(unresolved_scope.get("effect_scope_id") or "")
    owner_request_id = str(unresolved_scope.get("owner_request_id") or "")
    owner_request_digest = str(unresolved_scope.get("owner_request_digest") or "")
    observation_id = str(runtime_observation.get("observation_id") or "")
    if not all((scope_id, owner_request_id, owner_request_digest, observation_id)):
        raise ValueError("RECONCILIATION_IDENTITY_MISSING")
    stable = {
        "effect_scope_id": scope_id,
        "owner_request_id": owner_request_id,
        "observed_ffmpeg_generation": observed.get("ffmpeg_generation"),
        "observed_ffmpeg_pid": observed.get("ffmpeg_pid"),
    }
    reconciliation_id = f"dell-auto-reconcile-{hashlib.sha256(json.dumps(stable, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}"
    request = {
        "schema_version": "runtime.effect_reconciliation_request.v1",
        "reconciliation_id": reconciliation_id,
        "effect_scope_id": scope_id,
        "owner_request_id": owner_request_id,
        "owner_request_digest": owner_request_digest,
        "resolution": "EFFECT_OBSERVED",
        "evidence": {
            "schema_version": "runtime.delayed_exit_reconciliation_evidence.v1",
            "oracle": "DELAYED_FFMPEG_EXIT_AND_HEALTHY_SUCCESSOR",
            "observed_at": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "physical_effect_count": 1,
            "automatic_retry_count": 0,
            "before_target": before,
            "observed_target": observed,
            "runtime_observation_id": observation_id,
            "transport": {
                "bytes_sent": transport.get("bytes_sent"),
                "network_down": transport.get("network_down"),
                "tcp_probe_ok": transport.get("tcp_probe_ok"),
            },
        },
    }
    return EffectClient(socket_path, timeout_seconds=timeout_seconds).reconcile(request)


def configured_paths() -> dict[str, Path]:
    return {
        "socket": Path(os.environ["FR_EFFECT_EXECUTOR_SOCKET"]),
        "runtime_observation": Path(os.environ["FR_RUNTIME_OBSERVATION_FILE"]),
        "escalation_socket": Path(
            os.environ.get("FR_RUNTIME_RECOVERY_SOCKET", "/run/stream-v3-control/runtime-recovery.sock")
        ),
        "escalation_observation": Path(
            os.environ.get(
                "FR_RUNTIME_RECOVERY_OBSERVATION_FILE",
                "/run/stream-v3-control/runtime-recovery-observation.json",
            )
        ),
    }
