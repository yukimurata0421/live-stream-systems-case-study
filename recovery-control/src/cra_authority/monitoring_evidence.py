from __future__ import annotations

import json
import math
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from jsonschema import Draft202012Validator, FormatChecker

from cra_authority.json_input import load_object
from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.time import parse_utc

SCHEMA = "monitoring_v4.evidence_projection.v1"
FORBIDDEN_CONTROL_FIELDS = frozenset(
    {
        "action",
        "authorized_action",
        "authorization",
        "authorization_status",
        "command_id",
        "cooldown",
        "recovery_budget",
        "verdict",
    }
)


def reject_control_semantics(value: object) -> None:
    """Reject control-plane meaning anywhere in a Monitoring-owned payload."""

    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).lower() in FORBIDDEN_CONTROL_FIELDS:
                raise ValueError(f"MONITORING_CONTROL_FIELD_FORBIDDEN:{key}")
            reject_control_semantics(item)
    elif isinstance(value, list):
        for item in value:
            reject_control_semantics(item)


@dataclass(frozen=True)
class MonitoringEvidenceProjection:
    value: dict[str, Any]

    @property
    def projection_id(self) -> str:
        return str(self.value["projection_id"])

    @property
    def target_id(self) -> str:
        return str(self.value["target_id"])

    @property
    def sequence(self) -> int:
        return int(self.value["observation_sequence"])

    @property
    def incident(self) -> dict[str, Any]:
        return dict(self.value["incident"])

    @property
    def readiness(self) -> dict[str, bool]:
        return {str(key): bool(item) for key, item in dict(self.value["readiness"]).items()}

    @property
    def checks(self) -> dict[str, dict[str, str]]:
        return {str(key): dict(item) for key, item in dict(self.value["checks"]).items()}

    @property
    def observed_target(self) -> TargetIdentity:
        return TargetIdentity.from_dict(dict(self.value["observed_target"]))

    @property
    def is_fresh_now(self) -> bool:
        now = datetime.now(UTC)
        return parse_utc(str(self.value["issued_at"])) <= now < parse_utc(str(self.value["expires_at"]))


class MonitoringEvidenceContract:
    def __init__(
        self,
        schema_path: Path,
        verifier: KeyRing,
        *,
        allowed_sources: Mapping[str, str],
        maximum_ttl_seconds: float = 60.0,
        maximum_observation_age_seconds: float = 30.0,
        maximum_check_age_seconds: float = 30.0,
        maximum_future_skew_seconds: float = 2.0,
    ) -> None:
        self.validator = Draft202012Validator(
            json.loads(schema_path.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        )
        self.verifier = verifier
        self.allowed_sources = dict(allowed_sources)
        self.maximum_ttl_seconds = maximum_ttl_seconds
        self.maximum_observation_age_seconds = maximum_observation_age_seconds
        self.maximum_check_age_seconds = maximum_check_age_seconds
        self.maximum_future_skew_seconds = maximum_future_skew_seconds
        if any(
            isinstance(bound, bool) or not isinstance(bound, (int, float)) or not math.isfinite(bound) or bound <= 0
            for bound in (
                maximum_ttl_seconds,
                maximum_observation_age_seconds,
                maximum_check_age_seconds,
                maximum_future_skew_seconds,
            )
        ):
            raise ValueError("MONITORING_EVIDENCE_TIME_BOUND_INVALID")

    def decode(self, value: Mapping[str, Any], *, now: datetime | None = None) -> MonitoringEvidenceProjection:
        projection = self.verify_integrity(value)
        current = (now or datetime.now(UTC)).astimezone(UTC)
        observed = parse_utc(str(projection.value["observed_at"]))
        issued = parse_utc(str(projection.value["issued_at"]))
        expires = parse_utc(str(projection.value["expires_at"]))
        future_limit = current + timedelta(seconds=self.maximum_future_skew_seconds)
        if issued > future_limit:
            raise ValueError("MONITORING_EVIDENCE_ISSUED_IN_FUTURE")
        if observed > future_limit:
            raise ValueError("MONITORING_EVIDENCE_OBSERVED_IN_FUTURE")
        if expires <= current:
            raise ValueError("MONITORING_EVIDENCE_EXPIRED")
        if (current - observed).total_seconds() > self.maximum_observation_age_seconds:
            raise ValueError("MONITORING_EVIDENCE_OBSERVATION_TOO_OLD")
        for name, check in projection.checks.items():
            checked_at = parse_utc(str(check["observed_at"]))
            if checked_at > future_limit:
                raise ValueError(f"MONITORING_CHECK_OBSERVED_IN_FUTURE:{name}")
            if (current - checked_at).total_seconds() > self.maximum_check_age_seconds:
                raise ValueError(f"MONITORING_CHECK_TOO_OLD:{name}")
        return projection

    def verify_integrity(self, value: Mapping[str, Any]) -> MonitoringEvidenceProjection:
        """Verify contract, origin, signature and intrinsic time ordering.

        This intentionally does not assert wall-clock freshness. It exists for
        monotonic replay checks against an older, already accepted inbox file.
        Authorization callers must continue to use :meth:`decode`.
        """

        payload = dict(value)
        self.validator.validate(payload)
        reject_control_semantics(payload)
        self.verifier.verify(payload)
        source = str(payload["source_instance_id"])
        if self.allowed_sources.get(source) != str(payload["source_release_id"]):
            raise ValueError("MONITORING_SOURCE_OR_RELEASE_NOT_ALLOWED")
        observed = parse_utc(str(payload["observed_at"]))
        issued = parse_utc(str(payload["issued_at"]))
        expires = parse_utc(str(payload["expires_at"]))
        if not observed <= issued < expires:
            raise ValueError("MONITORING_EVIDENCE_TIME_ORDER_INVALID")
        if (expires - issued).total_seconds() > self.maximum_ttl_seconds:
            raise ValueError("MONITORING_EVIDENCE_TTL_TOO_LONG")
        for name, check in dict(payload["checks"]).items():
            checked_at = parse_utc(str(dict(check)["observed_at"]))
            if checked_at > observed:
                raise ValueError(f"MONITORING_CHECK_NEWER_THAN_PROJECTION:{name}")
        return MonitoringEvidenceProjection(payload)


class MonitoringEvidenceSource(Protocol):
    def latest(self) -> MonitoringEvidenceProjection: ...


class SignedProjectionFileSource:
    """Atomic-file inbox adapter; transport and signature remain separate gates."""

    def __init__(self, path: Path, contract: MonitoringEvidenceContract, *, maximum_bytes: int = 2 * 1024 * 1024) -> None:
        self.path = path
        self.contract = contract
        self.maximum_bytes = maximum_bytes

    def latest(self) -> MonitoringEvidenceProjection:
        descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("MONITORING_EVIDENCE_NOT_REGULAR_FILE")
            if stat.S_IMODE(metadata.st_mode) & 0o022:
                raise ValueError("MONITORING_EVIDENCE_PERMISSIONS_UNSAFE")
            if metadata.st_size > self.maximum_bytes:
                raise ValueError("MONITORING_EVIDENCE_TOO_LARGE")
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = -1
                payload = handle.read(self.maximum_bytes + 1)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        if len(payload) > self.maximum_bytes:
            raise ValueError("MONITORING_EVIDENCE_TOO_LARGE")
        raw = load_object(payload, maximum_bytes=self.maximum_bytes)
        return self.contract.decode(raw)
