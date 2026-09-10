from __future__ import annotations

import json
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from .canonical import KeyRing
from .models import TargetIdentity
from .time import parse_utc

SCHEMA = "cra_dell_recovery.observation_bundle.v1"
OBSERVATION_PATH = "/v1/dell-observations/latest"


@dataclass(frozen=True)
class DellObservationBundle:
    value: dict[str, Any]

    @property
    def source_instance_id(self) -> str:
        return str(self.value["source_instance_id"])

    @property
    def source_release_id(self) -> str:
        return str(self.value["source_release_id"])

    @property
    def sequence(self) -> int:
        return int(self.value["observation_sequence"])

    @property
    def observed_at(self) -> datetime:
        return parse_utc(str(self.value["observed_at"]))

    @property
    def valid_until(self) -> datetime:
        return parse_utc(str(self.value["valid_until"]))

    @property
    def target(self) -> TargetIdentity:
        return TargetIdentity.from_dict(dict(dict(self.value["target_snapshot"])["target_identity"]))

    @property
    def checks(self) -> dict[str, dict[str, str]]:
        return {str(name): dict(item) for name, item in dict(self.value["checks"]).items()}


class DellObservationContract:
    """Signature, source, target and intrinsic time gate for Dell read-only facts."""

    def __init__(
        self,
        schema_path: Path,
        verifier: KeyRing,
        *,
        allowed_sources: Mapping[str, str],
        expected_host_id: str,
        expected_namespace: str,
        expected_container_name: str = "stream-engine",
        maximum_ttl_seconds: float = 30.0,
    ) -> None:
        self.validator = Draft202012Validator(
            json.loads(schema_path.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        )
        self.verifier = verifier
        self.allowed_sources = dict(allowed_sources)
        self.expected_host_id = expected_host_id
        self.expected_namespace = expected_namespace
        self.expected_container_name = expected_container_name
        self.maximum_ttl_seconds = maximum_ttl_seconds

    def verify_integrity(self, value: Mapping[str, Any]) -> DellObservationBundle:
        payload = dict(value)
        self.validator.validate(payload)
        self.verifier.verify(payload)
        source = str(payload["source_instance_id"])
        if self.allowed_sources.get(source) != str(payload["source_release_id"]):
            raise ValueError("DELL_OBSERVATION_SOURCE_OR_RELEASE_NOT_ALLOWED")
        observed = parse_utc(str(payload["observed_at"]))
        valid_until = parse_utc(str(payload["valid_until"]))
        if observed >= valid_until:
            raise ValueError("DELL_OBSERVATION_TIME_ORDER_INVALID")
        if (valid_until - observed).total_seconds() > self.maximum_ttl_seconds:
            raise ValueError("DELL_OBSERVATION_TTL_TOO_LONG")
        target_snapshot = dict(payload["target_snapshot"])
        target = TargetIdentity.from_dict(dict(target_snapshot["target_identity"]))
        if (
            target.host_id != self.expected_host_id
            or target.namespace != self.expected_namespace
            or target.container_name != self.expected_container_name
        ):
            raise ValueError("DELL_OBSERVATION_TARGET_IDENTITY_MISMATCH")
        if parse_utc(str(target_snapshot["observed_at"])) > observed:
            raise ValueError("DELL_OBSERVATION_TARGET_NEWER_THAN_BUNDLE")
        if parse_utc(str(target_snapshot["valid_until"])) < valid_until:
            raise ValueError("DELL_OBSERVATION_OUTLIVES_TARGET")
        for name, check in dict(payload["checks"]).items():
            if parse_utc(str(dict(check)["observed_at"])) > observed:
                raise ValueError(f"DELL_OBSERVATION_CHECK_NEWER_THAN_BUNDLE:{name}")
        return DellObservationBundle(payload)

    def decode(self, value: Mapping[str, Any], *, now: datetime | None = None) -> DellObservationBundle:
        bundle = self.verify_integrity(value)
        current = (now or datetime.now(UTC)).astimezone(UTC)
        if bundle.observed_at > current:
            raise ValueError("DELL_OBSERVATION_IN_FUTURE")
        if bundle.valid_until <= current:
            raise ValueError("DELL_OBSERVATION_EXPIRED")
        return bundle


class SignedDellObservationFileSource:
    """Reads an atomically replaced observation inbox without following symlinks."""

    def __init__(self, path: Path, contract: DellObservationContract, *, maximum_bytes: int = 2 * 1024 * 1024) -> None:
        self.path = path
        self.contract = contract
        self.maximum_bytes = maximum_bytes

    def latest(self, *, now: datetime | None = None) -> DellObservationBundle:
        descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("DELL_OBSERVATION_NOT_REGULAR_FILE")
            if stat.S_IMODE(metadata.st_mode) & 0o022:
                raise ValueError("DELL_OBSERVATION_PERMISSIONS_UNSAFE")
            if metadata.st_size > self.maximum_bytes:
                raise ValueError("DELL_OBSERVATION_TOO_LARGE")
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = -1
                raw = handle.read(self.maximum_bytes + 1)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        if len(raw) > self.maximum_bytes:
            raise ValueError("DELL_OBSERVATION_TOO_LARGE")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("DELL_OBSERVATION_NOT_OBJECT")
        return self.contract.decode(value, now=now)
