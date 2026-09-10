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

from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.time import parse_utc

SCHEMA = "cra.no_action_host_status.v1"
STATUS_PATH = "/v1/no-action-soak-status/latest"
SERVER_RESOURCE_SCHEMA = "cra.no_action_server_resource_status.v1"


def read_regular_bytes(
    path: Path,
    *,
    maximum_bytes: int = 2 * 1024 * 1024,
    secret: bool = False,
    expected_owner_uid: int | None = None,
) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if expected_owner_uid is not None and metadata.st_uid != expected_owner_uid:
            raise ValueError("HOST_STATUS_INPUT_OWNER_MISMATCH")
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("HOST_STATUS_INPUT_NOT_REGULAR_FILE")
        if stat.S_IMODE(metadata.st_mode) & (0o077 if secret else 0o022):
            raise ValueError("HOST_STATUS_INPUT_PERMISSIONS_UNSAFE")
        if metadata.st_size < 1 or metadata.st_size > maximum_bytes:
            raise ValueError("HOST_STATUS_INPUT_SIZE_INVALID")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > maximum_bytes:
        raise ValueError("HOST_STATUS_INPUT_TOO_LARGE")
    return raw


def read_object(path: Path, *, maximum_bytes: int = 2 * 1024 * 1024, secret: bool = False) -> dict[str, Any]:
    value = json.loads(read_regular_bytes(path, maximum_bytes=maximum_bytes, secret=secret))
    if not isinstance(value, dict):
        raise ValueError("HOST_STATUS_INPUT_NOT_OBJECT")
    return dict(value)


def atomic_write_json(path: Path, value: Mapping[str, Any], *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(dict(value), handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def write_server_resource_status(
    path: Path,
    *,
    role: str,
    host_id: str,
    release_id: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    if role not in {"dell", "arena"}:
        raise ValueError("SERVER_RESOURCE_STATUS_ROLE_INVALID")
    status = Path("/proc/self/status").read_text(encoding="utf-8")
    rss_kib: int | None = None
    for line in status.splitlines():
        fields = line.split()
        if len(fields) == 3 and fields[0] == "VmRSS:" and fields[1].isdigit() and fields[2] == "kB":
            rss_kib = int(fields[1])
            break
    if rss_kib is None:
        raise ValueError("SERVER_RESOURCE_STATUS_RSS_MISSING")
    value = {
        "schema": SERVER_RESOURCE_SCHEMA,
        "role": role,
        "host_id": host_id,
        "release_id": release_id,
        "observed_at": (now or datetime.now(UTC)).astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "resident_memory_bytes": rss_kib * 1024,
        "open_fd_count": sum(1 for _ in Path("/proc/self/fd").iterdir()),
        "control_capability_count": 0,
        "physical_effect_count": 0,
    }
    atomic_write_json(path, value, mode=0o640)
    return value


def read_server_resource_status(
    path: Path,
    *,
    expected_role: str,
    expected_host_id: str,
    expected_release_id: str,
    maximum_age_seconds: float,
    maximum_future_skew_seconds: float = 1.0,
    now: datetime | None = None,
) -> tuple[int, int]:
    value = read_object(path, maximum_bytes=64 * 1024)
    if set(value) != {
        "schema",
        "role",
        "host_id",
        "release_id",
        "observed_at",
        "resident_memory_bytes",
        "open_fd_count",
        "control_capability_count",
        "physical_effect_count",
    }:
        raise ValueError("SERVER_RESOURCE_STATUS_FIELDS_INVALID")
    if value["schema"] != SERVER_RESOURCE_SCHEMA:
        raise ValueError("SERVER_RESOURCE_STATUS_SCHEMA_INVALID")
    if value["role"] != expected_role or value["host_id"] != expected_host_id:
        raise ValueError("SERVER_RESOURCE_STATUS_HOST_MISMATCH")
    if value["release_id"] != expected_release_id:
        raise ValueError("SERVER_RESOURCE_STATUS_RELEASE_MISMATCH")
    if value["control_capability_count"] != 0 or value["physical_effect_count"] != 0:
        raise ValueError("SERVER_RESOURCE_STATUS_CAPABILITY_PRESENT")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    observed = parse_utc(str(value["observed_at"]))
    future_skew = (observed - current).total_seconds()
    if future_skew > maximum_future_skew_seconds:
        raise ValueError("SERVER_RESOURCE_STATUS_FROM_FUTURE")
    if (current - observed).total_seconds() > maximum_age_seconds:
        raise ValueError("SERVER_RESOURCE_STATUS_STALE")
    resources: list[int] = []
    for name in ("resident_memory_bytes", "open_fd_count"):
        item = value[name]
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise ValueError("SERVER_RESOURCE_STATUS_VALUE_INVALID")
        resources.append(item)
    return resources[0], resources[1]


@dataclass(frozen=True)
class HostStatusBundle:
    value: dict[str, Any]

    @property
    def sequence(self) -> int:
        return int(self.value["sequence"])

    @property
    def observed_at(self) -> datetime:
        return parse_utc(str(self.value["observed_at"]))

    @property
    def valid_until(self) -> datetime:
        return parse_utc(str(self.value["valid_until"]))


class HostStatusContract:
    """Signature, role, host, release and intrinsic-time gate for soak-only host facts."""

    def __init__(
        self,
        schema_path: Path,
        verifier: KeyRing,
        *,
        key_id: str,
        expected_role: str,
        expected_host_id: str,
        expected_release_id: str,
        maximum_ttl_seconds: float = 30.0,
    ) -> None:
        if expected_role not in {"dell", "arena"}:
            raise ValueError("HOST_STATUS_ROLE_INVALID")
        if not 1.0 <= maximum_ttl_seconds <= 60.0:
            raise ValueError("HOST_STATUS_TTL_OUT_OF_RANGE")
        self.validator = Draft202012Validator(
            json.loads(schema_path.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        )
        self.verifier = verifier
        self.key_id = key_id
        self.expected_role = expected_role
        self.expected_host_id = expected_host_id
        self.expected_release_id = expected_release_id
        self.maximum_ttl_seconds = maximum_ttl_seconds

    def verify_integrity(self, value: Mapping[str, Any]) -> HostStatusBundle:
        payload = dict(value)
        self.validator.validate(payload)
        self.verifier.verify(payload)
        if payload.get("schema") != SCHEMA:
            raise ValueError("HOST_STATUS_SCHEMA_UNSUPPORTED")
        if payload.get("key_id") != self.key_id:
            raise ValueError("HOST_STATUS_KEY_ID_MISMATCH")
        if payload.get("role") != self.expected_role:
            raise ValueError("HOST_STATUS_ROLE_MISMATCH")
        if payload.get("host_id") != self.expected_host_id:
            raise ValueError("HOST_STATUS_HOST_ID_MISMATCH")
        if payload.get("release_id") != self.expected_release_id:
            raise ValueError("HOST_STATUS_RELEASE_ID_MISMATCH")
        observed = parse_utc(str(payload["observed_at"]))
        valid_until = parse_utc(str(payload["valid_until"]))
        if observed >= valid_until:
            raise ValueError("HOST_STATUS_TIME_ORDER_INVALID")
        if (valid_until - observed).total_seconds() > self.maximum_ttl_seconds:
            raise ValueError("HOST_STATUS_TTL_TOO_LONG")
        return HostStatusBundle(payload)

    def decode(self, value: Mapping[str, Any], *, now: datetime | None = None) -> HostStatusBundle:
        bundle = self.verify_integrity(value)
        current = (now or datetime.now(UTC)).astimezone(UTC)
        if bundle.observed_at > current:
            raise ValueError("HOST_STATUS_OBSERVED_IN_FUTURE")
        if bundle.valid_until <= current:
            raise ValueError("HOST_STATUS_EXPIRED")
        return bundle


class SignedHostStatusFileSource:
    def __init__(self, path: Path, contract: HostStatusContract, *, maximum_bytes: int = 2 * 1024 * 1024) -> None:
        self.path = path
        self.contract = contract
        self.maximum_bytes = maximum_bytes

    def latest(self, *, now: datetime | None = None) -> HostStatusBundle:
        return self.contract.decode(read_object(self.path, maximum_bytes=self.maximum_bytes), now=now)


def served_host_status_bytes(
    path: Path,
    *,
    expected_role: str,
    expected_host_id: str,
    expected_release_id: str,
    maximum_bytes: int,
    now: datetime | None = None,
) -> bytes:
    raw = read_regular_bytes(path, maximum_bytes=maximum_bytes)
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        raise ValueError("HOST_STATUS_FILE_SCHEMA_INVALID")
    if value.get("role") != expected_role:
        raise ValueError("HOST_STATUS_FILE_ROLE_MISMATCH")
    if value.get("host_id") != expected_host_id:
        raise ValueError("HOST_STATUS_FILE_HOST_MISMATCH")
    if value.get("release_id") != expected_release_id:
        raise ValueError("HOST_STATUS_FILE_RELEASE_MISMATCH")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    if parse_utc(str(value.get("observed_at", ""))) > current:
        raise ValueError("HOST_STATUS_FILE_OBSERVED_IN_FUTURE")
    if parse_utc(str(value.get("valid_until", ""))) <= current:
        raise ValueError("HOST_STATUS_FILE_EXPIRED")
    return raw
