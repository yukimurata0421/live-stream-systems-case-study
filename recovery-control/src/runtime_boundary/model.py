from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from cra_dell_recovery.effect_scope import effect_scope_id, logical_generation_scope_id

MAX_EFFECT_REQUEST_LIFETIME_SECONDS = 5.0

RESTART_FFMPEG = "RESTART_FFMPEG"
RECONCILE_FFMPEG = "RECONCILE_FFMPEG"
ESCALATE_RUNTIME_RECOVERY = "ESCALATE_RUNTIME_RECOVERY"
EXECUTOR_OWNED_INTENTS = frozenset({RESTART_FFMPEG, RECONCILE_FFMPEG})
ALL_EFFECT_INTENTS = frozenset({*EXECUTOR_OWNED_INTENTS, ESCALATE_RUNTIME_RECOVERY})

EXACT_TARGET_FIELDS = (
    "host_id",
    "host_boot_id",
    "namespace",
    "pod_uid",
    "container_name",
    "container_id",
    "ffmpeg_generation",
    "ffmpeg_pid",
)

EXACT_RUNTIME_FIELDS = (
    "host_id",
    "host_boot_id",
    "namespace",
    "pod_uid",
    "stream_engine_container_name",
    "stream_engine_container_id",
    "runtime_generation",
)


class ProtocolError(ValueError):
    pass


def parse_utc(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProtocolError("INVALID_TIMESTAMP") from exc
    if parsed.tzinfo is None:
        raise ProtocolError("TIMESTAMP_TZ_REQUIRED")
    return parsed.astimezone(UTC)


def validate_target(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(EXACT_TARGET_FIELDS):
        raise ProtocolError("TARGET_IDENTITY_NOT_EXACT")
    target = {field: value[field] for field in EXACT_TARGET_FIELDS}
    for field in EXACT_TARGET_FIELDS:
        if field == "ffmpeg_pid":
            try:
                target[field] = int(target[field])
            except (TypeError, ValueError) as exc:
                raise ProtocolError("TARGET_PID_INVALID") from exc
            if int(target[field]) <= 1:
                raise ProtocolError("TARGET_PID_INVALID")
        elif not str(target[field] or "").strip():
            raise ProtocolError(f"TARGET_{field.upper()}_MISSING")
        else:
            target[field] = str(target[field])
    if target["container_name"] != "stream-engine":
        raise ProtocolError("TARGET_CONTAINER_NOT_STREAM_ENGINE")
    return target


def validate_runtime_identity(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != set(EXACT_RUNTIME_FIELDS):
        raise ProtocolError("RUNTIME_IDENTITY_NOT_EXACT")
    identity: dict[str, str] = {}
    for field in EXACT_RUNTIME_FIELDS:
        normalized = str(value[field] or "").strip()
        if not normalized:
            raise ProtocolError(f"RUNTIME_{field.upper()}_MISSING")
        identity[field] = normalized
    if identity["stream_engine_container_name"] != "stream-engine":
        raise ProtocolError("RUNTIME_CONTAINER_NOT_STREAM_ENGINE")
    if not identity["stream_engine_container_id"].startswith("containerd://"):
        raise ProtocolError("RUNTIME_CONTAINER_ID_INVALID")
    return identity


def _required_text(value: Mapping[str, Any], name: str) -> str:
    result = str(value.get(name) or "").strip()
    if not result:
        raise ProtocolError("REQUIRED_IDENTITY_MISSING")
    return result


@dataclass(frozen=True)
class EffectRequest:
    schema_version: str
    request_id: str
    producer_id: str
    producer_generation: int
    intent_type: str
    reason: str
    failure_domain: str
    issued_at: str
    expires_at: str
    ffmpeg_target_identity: dict[str, Any] | None
    runtime_identity: dict[str, str] | None
    expected_ffmpeg_generation: str
    idempotency_key: str
    correlation_id: str
    target_snapshot_id: str
    runtime_snapshot_id: str
    runtime_observation_id: str
    expected_executor_instance_id: str
    maintenance_evidence_status: str
    projection_id: str
    projection_sequence: int

    @property
    def operation(self) -> str:
        return {
            RESTART_FFMPEG: "restart_ffmpeg",
            RECONCILE_FFMPEG: "reconcile_ffmpeg",
            ESCALATE_RUNTIME_RECOVERY: "escalate_runtime_recovery",
        }[self.intent_type]

    @property
    def target_identity(self) -> dict[str, Any]:
        """Compatibility view for the v1 restart-only protocol."""

        return self.ffmpeg_target_identity or {}

    @property
    def identity_type(self) -> str:
        return "FFMPEG_TARGET" if self.intent_type == RESTART_FFMPEG else "RUNTIME"

    @property
    def fence_identity(self) -> Mapping[str, Any]:
        if self.intent_type == RESTART_FFMPEG:
            return self.ffmpeg_target_identity or {}
        return self.runtime_identity or {}

    @property
    def effect_scope_id(self) -> str:
        if self.intent_type != RESTART_FFMPEG or self.ffmpeg_target_identity is None:
            encoded = json.dumps(
                {"intent_type": self.intent_type, "identity": self.fence_identity},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            return hashlib.sha256(encoded).hexdigest()
        return effect_scope_id("restart_ffmpeg", self.ffmpeg_target_identity)

    @property
    def logical_generation_scope_id(self) -> str | None:
        """PID-independent physical fence for restart requests only."""

        if self.intent_type != RESTART_FFMPEG or self.ffmpeg_target_identity is None:
            return None
        return logical_generation_scope_id("restart_ffmpeg", self.ffmpeg_target_identity)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> EffectRequest:
        schema_version = str(value.get("schema_version") or "")
        if schema_version == "runtime.effect_request.v1":
            return cls._from_v1(value)
        if schema_version != "runtime.typed_effect_request.v2":
            raise ProtocolError("SCHEMA_VERSION_UNSUPPORTED")
        return cls._from_v2(value)

    @classmethod
    def _common(cls, value: Mapping[str, Any]) -> dict[str, Any]:
        issued_at = str(value.get("issued_at") or "")
        expires_at = str(value.get("expires_at") or "")
        issued = parse_utc(issued_at)
        expires = parse_utc(expires_at)
        if expires <= issued:
            raise ProtocolError("EXPIRY_NOT_AFTER_ISSUE")
        if (expires - issued).total_seconds() > MAX_EFFECT_REQUEST_LIFETIME_SECONDS:
            raise ProtocolError("REQUEST_LIFETIME_TOO_LONG")
        try:
            generation = int(value.get("producer_generation") or 0)
            sequence = int(value.get("projection_sequence") or 0)
        except (TypeError, ValueError) as exc:
            raise ProtocolError("GENERATION_INVALID") from exc
        if generation <= 0 or sequence < 0:
            raise ProtocolError("GENERATION_INVALID")
        reason = str(value.get("reason") or "").strip()
        if not reason or len(reason) > 512 or "\n" in reason or "\r" in reason:
            raise ProtocolError("REASON_INVALID")
        evidence_status = str(value.get("maintenance_evidence_status") or "").strip()
        if evidence_status not in {"AVAILABLE", "UNKNOWN", "STALE", "INVALID"}:
            raise ProtocolError("MAINTENANCE_EVIDENCE_STATUS_INVALID")
        projection_id = str(value.get("projection_id") or "").strip()
        if evidence_status == "AVAILABLE" and (not projection_id or sequence <= 0):
            raise ProtocolError("MAINTENANCE_EVIDENCE_IDENTITY_MISSING")
        return {
            "request_id": _required_text(value, "request_id"),
            "producer_id": _required_text(value, "producer_id"),
            "producer_generation": generation,
            "reason": reason,
            "issued_at": issued_at,
            "expires_at": expires_at,
            "idempotency_key": _required_text(value, "idempotency_key"),
            "correlation_id": _required_text(value, "correlation_id"),
            "expected_executor_instance_id": _required_text(value, "expected_executor_instance_id"),
            "maintenance_evidence_status": evidence_status,
            "projection_id": projection_id,
            "projection_sequence": sequence,
        }

    @classmethod
    def _from_v1(cls, value: Mapping[str, Any]) -> EffectRequest:
        if str(value.get("operation") or "") != "restart_ffmpeg":
            raise ProtocolError("OPERATION_NOT_ALLOWED")
        common = cls._common(value)
        generation = _required_text(value, "expected_ffmpeg_generation")
        target = validate_target(value.get("target_identity"))
        return cls(
            schema_version="runtime.effect_request.v1",
            intent_type=RESTART_FFMPEG,
            failure_domain="FFMPEG_LOCAL",
            ffmpeg_target_identity=target,
            runtime_identity=None,
            expected_ffmpeg_generation=generation,
            target_snapshot_id=_required_text(value, "target_snapshot_id"),
            runtime_snapshot_id="",
            runtime_observation_id=_required_text(value, "runtime_observation_id"),
            **common,
        )

    @classmethod
    def _from_v2(cls, value: Mapping[str, Any]) -> EffectRequest:
        common = cls._common(value)
        intent = str(value.get("intent_type") or "").strip()
        if intent not in ALL_EFFECT_INTENTS:
            raise ProtocolError("INTENT_NOT_ALLOWED")
        failure_domain = str(value.get("failure_domain") or "").strip()
        if not failure_domain:
            raise ProtocolError("FAILURE_DOMAIN_MISSING")
        ffmpeg_target: dict[str, Any] | None = None
        runtime_identity: dict[str, str] | None = None
        expected_generation = str(value.get("expected_ffmpeg_generation") or "").strip()
        target_snapshot_id = str(value.get("target_snapshot_id") or "").strip()
        runtime_snapshot_id = str(value.get("runtime_snapshot_id") or "").strip()
        runtime_observation_id = str(value.get("runtime_observation_id") or "").strip()
        if intent == RESTART_FFMPEG:
            ffmpeg_target = validate_target(value.get("ffmpeg_target_identity"))
            if value.get("runtime_identity") is not None and value.get("runtime_identity") != "":
                raise ProtocolError("RESTART_RUNTIME_IDENTITY_FORBIDDEN")
            if not expected_generation or not target_snapshot_id or not runtime_observation_id:
                raise ProtocolError("RESTART_IDENTITY_MISSING")
        else:
            runtime_identity = validate_runtime_identity(value.get("runtime_identity"))
            if value.get("ffmpeg_target_identity") is not None and value.get("ffmpeg_target_identity") != "":
                raise ProtocolError("RUNTIME_INTENT_FFMPEG_TARGET_FORBIDDEN")
            if expected_generation or not runtime_snapshot_id:
                raise ProtocolError("RUNTIME_INTENT_IDENTITY_INVALID")
            if intent == RECONCILE_FFMPEG and not runtime_observation_id:
                raise ProtocolError("RECONCILE_OBSERVATION_ID_MISSING")
        return cls(
            schema_version="runtime.typed_effect_request.v2",
            intent_type=intent,
            failure_domain=failure_domain,
            ffmpeg_target_identity=ffmpeg_target,
            runtime_identity=runtime_identity,
            expected_ffmpeg_generation=expected_generation,
            target_snapshot_id=target_snapshot_id,
            runtime_snapshot_id=runtime_snapshot_id,
            runtime_observation_id=runtime_observation_id,
            **common,
        )

    def canonical(self) -> dict[str, Any]:
        return {
            "schema_version": "runtime.typed_effect_request.v2",
            "request_id": self.request_id,
            "producer_id": self.producer_id,
            "producer_generation": self.producer_generation,
            "intent_type": self.intent_type,
            "reason": self.reason,
            "failure_domain": self.failure_domain,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "ffmpeg_target_identity": self.ffmpeg_target_identity,
            "runtime_identity": self.runtime_identity,
            "expected_ffmpeg_generation": self.expected_ffmpeg_generation,
            "idempotency_key": self.idempotency_key,
            "correlation_id": self.correlation_id,
            "target_snapshot_id": self.target_snapshot_id,
            "runtime_snapshot_id": self.runtime_snapshot_id,
            "runtime_observation_id": self.runtime_observation_id,
            "expected_executor_instance_id": self.expected_executor_instance_id,
            "maintenance_evidence_status": self.maintenance_evidence_status,
            "projection_id": self.projection_id,
            "projection_sequence": self.projection_sequence,
        }

    def digest(self) -> str:
        encoded = json.dumps(self.canonical(), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()
