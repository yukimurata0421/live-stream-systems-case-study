from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, ClassVar, Mapping

from ._decode import contract_object, integer, object_value, text
from .ids import json_copy, payload_sha256, stable_id, validate_id
from .time import parse_utc, require_not_before


OBSERVATION_STATES = frozenset({"good", "bad", "unknown", "not_applicable"})
EVIDENCE_ROLES = frozenset(
    {"current_authoritative", "current_correlated", "supporting", "diagnostic", "historical", "formal"}
)
DOMAIN_ORDER = (
    "youtube_lifecycle",
    "youtube_input_quality",
    "delivery",
    "rendering",
    "audio",
    "viewer_external",
    "monitoring_platform",
    "network_transport",
    "runtime_resource",
    "adsb_source",
    "api_quota",
    "recovery_policy",
    "notification_delivery",
    "control_loop",
)
DOMAINS = frozenset(DOMAIN_ORDER)
_NAME_RE = re.compile(r"^[a-z][a-z0-9_.-]{1,95}$")


def _name(value: str, *, field: str, allowed: frozenset[str] | None = None) -> str:
    if not isinstance(value, str) or not _NAME_RE.fullmatch(value):
        raise ValueError(f"invalid {field}: {value!r}")
    if allowed is not None and value not in allowed:
        raise ValueError(f"unsupported {field}: {value!r}")
    return value


@dataclass(frozen=True)
class ObservationEnvelope:
    SCHEMA: ClassVar[str] = "monitoring_v4.observation.v1"

    observation_id: str
    domain: str
    source: str
    source_event_id: str
    source_generation: str
    evidence_role: str
    status: str
    reason_code: str
    observed_at: str
    received_at: str
    freshness_limit_sec: int
    producer_revision: str
    payload_sha256: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        validate_id(self.observation_id, field="observation_id")
        _name(self.domain, field="domain", allowed=DOMAINS)
        _name(self.source, field="source")
        if not isinstance(self.source_event_id, str) or not self.source_event_id.strip():
            raise ValueError("source_event_id is required")
        if len(self.source_event_id) > 300:
            raise ValueError("source_event_id exceeds 300 characters")
        if not self.source_generation or len(self.source_generation) > 200:
            raise ValueError("source_generation must contain 1-200 characters")
        _name(self.evidence_role, field="evidence_role", allowed=EVIDENCE_ROLES)
        _name(self.status, field="status", allowed=OBSERVATION_STATES)
        _name(self.reason_code, field="reason_code")
        parse_utc(self.observed_at, field="observed_at")
        parse_utc(self.received_at, field="received_at")
        require_not_before(
            self.received_at,
            self.observed_at,
            later_field="received_at",
            earlier_field="observed_at",
        )
        if type(self.freshness_limit_sec) is not int or self.freshness_limit_sec <= 0:
            raise ValueError("freshness_limit_sec must be positive")
        if not self.producer_revision or len(self.producer_revision) > 200:
            raise ValueError("producer_revision must contain 1-200 characters")
        if not isinstance(self.payload, Mapping):
            raise ValueError("payload must be an object")
        copied = json_copy(self.payload)
        if not re.fullmatch(r"[0-9a-f]{64}", self.payload_sha256):
            raise ValueError("payload_sha256 must be a lowercase SHA-256 digest")
        if payload_sha256(copied) != self.payload_sha256:
            raise ValueError("payload_sha256 does not match payload")
        object.__setattr__(self, "payload", copied)

    @classmethod
    def create(
        cls,
        *,
        domain: str,
        source: str,
        source_event_id: str,
        source_generation: str,
        evidence_role: str,
        status: str,
        reason_code: str,
        observed_at: str,
        received_at: str,
        freshness_limit_sec: int,
        producer_revision: str,
        payload: Mapping[str, Any],
    ) -> "ObservationEnvelope":
        copied = json_copy(payload)
        ident = stable_id(
            "obs",
            cls.SCHEMA,
            domain,
            source,
            source_event_id,
            source_generation,
            evidence_role,
            status,
            reason_code,
            observed_at,
            freshness_limit_sec,
            producer_revision,
            copied,
        )
        return cls(
            ident,
            domain,
            source,
            source_event_id,
            source_generation,
            evidence_role,
            status,
            reason_code,
            observed_at,
            received_at,
            freshness_limit_sec,
            producer_revision,
            payload_sha256(copied),
            copied,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "observation_id": self.observation_id,
            "domain": self.domain,
            "source": self.source,
            "source_event_id": self.source_event_id,
            "source_generation": self.source_generation,
            "evidence_role": self.evidence_role,
            "status": self.status,
            "reason_code": self.reason_code,
            "observed_at": self.observed_at,
            "received_at": self.received_at,
            "freshness_limit_sec": self.freshness_limit_sec,
            "producer_revision": self.producer_revision,
            "payload_sha256": self.payload_sha256,
            "payload": json_copy(self.payload),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ObservationEnvelope":
        raw = contract_object(
            value,
            schema=cls.SCHEMA,
            fields=frozenset(
                {
                    "observation_id",
                    "domain",
                    "source",
                    "source_event_id",
                    "source_generation",
                    "evidence_role",
                    "status",
                    "reason_code",
                    "observed_at",
                    "received_at",
                    "freshness_limit_sec",
                    "producer_revision",
                    "payload_sha256",
                    "payload",
                }
            ),
        )
        return cls(
            observation_id=text(raw, "observation_id"),
            domain=text(raw, "domain"),
            source=text(raw, "source"),
            source_event_id=text(raw, "source_event_id"),
            source_generation=text(raw, "source_generation"),
            evidence_role=text(raw, "evidence_role"),
            status=text(raw, "status"),
            reason_code=text(raw, "reason_code"),
            observed_at=text(raw, "observed_at"),
            received_at=text(raw, "received_at"),
            freshness_limit_sec=integer(raw, "freshness_limit_sec"),
            producer_revision=text(raw, "producer_revision"),
            payload_sha256=text(raw, "payload_sha256"),
            payload=object_value(raw, "payload"),
        )


@dataclass(frozen=True)
class ObservationRejection:
    SCHEMA: ClassVar[str] = "monitoring_v4.observation_rejection.v1"

    rejection_id: str
    source: str
    reason_code: str
    detail: str
    received_at: str
    payload_sha256: str = ""

    def __post_init__(self) -> None:
        validate_id(self.rejection_id, field="rejection_id")
        _name(self.source, field="source")
        _name(self.reason_code, field="reason_code")
        parse_utc(self.received_at, field="received_at")
        if len(self.detail) > 500:
            raise ValueError("rejection detail exceeds 500 characters")
        if self.payload_sha256 and not re.fullmatch(r"[0-9a-f]{64}", self.payload_sha256):
            raise ValueError("payload_sha256 must be a lowercase SHA-256 digest")

    @classmethod
    def create(
        cls, *, source: str, reason_code: str, detail: str, received_at: str, payload_sha256: str = ""
    ) -> "ObservationRejection":
        ident = stable_id("rej", source, reason_code, detail, received_at, payload_sha256)
        return cls(ident, source, reason_code, detail[:500], received_at, payload_sha256)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "rejection_id": self.rejection_id,
            "source": self.source,
            "reason_code": self.reason_code,
            "detail": self.detail,
            "received_at": self.received_at,
            "payload_sha256": self.payload_sha256,
        }
