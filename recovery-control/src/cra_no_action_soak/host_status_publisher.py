from __future__ import annotations

import argparse
import grp
import hashlib
import json
import math
import os
import pwd
import re
import sqlite3
import stat
import subprocess
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from jsonschema import Draft202012Validator, FormatChecker

from cra_dell_recovery.canonical import KeyRing, Signer, canonical_json
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.time import isoformat_utc, parse_utc

from .host_status import SCHEMA, HostStatusContract, atomic_write_json, read_object, read_regular_bytes, read_server_resource_status

CONFIG_SCHEMA = "cra.no_action_host_status_publisher.v1"
FAILURE_MESSAGE_ID = "d9b373ed55a64feb8242e02dbe79a49c"
INVOCATION_ID = re.compile(r"^[0-9a-f]{32}$")
RESOURCE_FAILURE = re.compile(r"oom-kill|out of memory|failed with result ['\"]?resources|memory limit", re.IGNORECASE)
SIGNING_KEY_CREDENTIAL = "dell-host-status-signing-key.pem"
SIGNING_KEY_OVERRIDE_ENV = "CRA_HOST_STATUS_SIGNING_KEY_FILE"
# The arena pull can legitimately spend several retries waiting for the next
# Dell status whose remaining validity clears its handoff floor. Keep the
# publisher inside its 15-second systemd timeout while allowing that pull to
# finish after the historical 8-second refresh window.
UPSTREAM_STATUS_REFRESH_DELAYS_SECONDS = (0.5,) * 24
PUBLISH_COMPLETION_MARGIN_SECONDS = 1.0
JOURNAL_COUNTER_SCHEMA = "cra.no_action_host_status_journal_counters.v1"
JOURNAL_CURSOR_PREFIX = "-- cursor: "
ROLE_SERVICES = {
    "dell": frozenset(
        {
            "dell_host_status_publisher",
            "dell_observation_server",
        }
    ),
    "arena": frozenset(
        {
            "arena_dell_host_status_pull",
            "arena_dell_pull",
            "arena_host_status_publisher",
            "arena_live_adapter",
            "arena_projection",
            "arena_projection_server",
        }
    ),
}
ROLE_CREDENTIALS = {
    "dell": frozenset({"dell_target_observer"}),
    "arena": frozenset({"arena_dell"}),
}
DELL_INPUTS = frozenset(
    {
        "target_snapshot_file",
        "observation_file",
        "observation_schema_file",
        "effect_database",
        "server_resource_status_file",
    }
)
ARENA_INPUTS = frozenset(
    {
        "dell_host_status_file",
        "dell_host_status_schema_file",
        "dell_key_id",
        "dell_public_key_file",
        "dell_host_id",
        "dell_release_id",
        "dell_pull_status_file",
        "live_adapter_status_file",
        "projection_status_file",
        "transport_recovery_state_file",
        "legacy_recovery_unit",
        "server_resource_status_file",
    }
)


def _required_path(value: object, code: str) -> Path:
    if not isinstance(value, str) or not value.startswith("/"):
        raise ValueError(code)
    return Path(value)


def _private_key(path: Path) -> Ed25519PrivateKey:
    raw = read_regular_bytes(path, maximum_bytes=64 * 1024, secret=True)
    value = serialization.load_pem_private_key(raw, password=None)
    if not isinstance(value, Ed25519PrivateKey):
        raise ValueError("HOST_STATUS_SIGNING_KEY_NOT_ED25519")
    return value


def _signing_private_key_path(value: dict[str, Any]) -> Path:
    configured = Path(str(value["signing_private_key_file"]))
    override = os.environ.get(SIGNING_KEY_OVERRIDE_ENV)
    if override is None:
        return configured
    if value.get("role") != "dell":
        raise ValueError("HOST_STATUS_SIGNING_KEY_OVERRIDE_ROLE_INVALID")
    credentials_directory = os.environ.get("CREDENTIALS_DIRECTORY")
    if not credentials_directory or not credentials_directory.startswith("/"):
        raise ValueError("HOST_STATUS_CREDENTIALS_DIRECTORY_INVALID")
    expected = Path(credentials_directory) / SIGNING_KEY_CREDENTIAL
    override_path = _required_path(override, "HOST_STATUS_SIGNING_KEY_OVERRIDE_INVALID")
    if override_path != expected:
        raise ValueError("HOST_STATUS_SIGNING_KEY_OVERRIDE_MISMATCH")
    return override_path


def _public_key(path: Path) -> Ed25519PublicKey:
    raw = read_regular_bytes(path, maximum_bytes=64 * 1024)
    value = serialization.load_pem_public_key(raw)
    if not isinstance(value, Ed25519PublicKey):
        raise ValueError("HOST_STATUS_VERIFICATION_KEY_NOT_ED25519")
    return value


class PublisherConfig:
    def __init__(self, value: dict[str, Any], path: Path) -> None:
        self.value = value
        self.path = path

    @classmethod
    def load(cls, path: Path) -> PublisherConfig:
        value = read_object(path, maximum_bytes=64 * 1024)
        required = {
            "schema",
            "role",
            "host_id",
            "host_contract_file",
            "release_id",
            "release_manifest_file",
            "runtime_manifest_file",
            "configuration_directory",
            "maintenance_restart_policy_file",
            "disk_path",
            "service_units",
            "persistent_service_name",
            "credential_files",
            "signing_private_key_file",
            "key_id",
            "host_status_schema_file",
            "output_file",
            "output_owner",
            "output_group",
            "maximum_ttl_seconds",
            "maximum_input_age_seconds",
            "role_inputs",
        }
        optional = {"minimum_remaining_validity_seconds"}
        if not required.issubset(value) or set(value) - required - optional or value.get("schema") != CONFIG_SCHEMA:
            raise ValueError("HOST_STATUS_PUBLISHER_CONFIG_FIELDS_INVALID")
        role = value.get("role")
        if role not in ROLE_SERVICES:
            raise ValueError("HOST_STATUS_PUBLISHER_ROLE_INVALID")
        for name in ("host_id", "release_id", "persistent_service_name", "key_id", "output_owner", "output_group"):
            if not isinstance(value.get(name), str) or not str(value[name]).strip():
                raise ValueError(f"HOST_STATUS_PUBLISHER_{name.upper()}_INVALID")
        require_runtime_release(str(value["release_id"]), "HOST_STATUS_PUBLISHER_RUNTIME_RELEASE_MISMATCH")
        for name in (
            "host_contract_file",
            "release_manifest_file",
            "runtime_manifest_file",
            "configuration_directory",
            "maintenance_restart_policy_file",
            "disk_path",
            "signing_private_key_file",
            "host_status_schema_file",
            "output_file",
        ):
            _required_path(value[name], f"HOST_STATUS_PUBLISHER_{name.upper()}_INVALID")
        services = value.get("service_units")
        if not isinstance(services, dict) or set(services) != ROLE_SERVICES[str(role)]:
            raise ValueError("HOST_STATUS_PUBLISHER_SERVICE_FIELDS_INVALID")
        if value["persistent_service_name"] not in services:
            raise ValueError("HOST_STATUS_PUBLISHER_PERSISTENT_SERVICE_INVALID")
        if not all(isinstance(unit, str) and unit.endswith(".service") and unit for unit in services.values()):
            raise ValueError("HOST_STATUS_PUBLISHER_UNIT_INVALID")
        credentials = value.get("credential_files")
        if not isinstance(credentials, dict) or set(credentials) != ROLE_CREDENTIALS[str(role)]:
            raise ValueError("HOST_STATUS_PUBLISHER_CREDENTIAL_FIELDS_INVALID")
        for item in credentials.values():
            _required_path(item, "HOST_STATUS_PUBLISHER_CREDENTIAL_PATH_INVALID")
        role_inputs = value.get("role_inputs")
        expected_inputs = DELL_INPUTS if role == "dell" else ARENA_INPUTS
        if not isinstance(role_inputs, dict) or set(role_inputs) != expected_inputs:
            raise ValueError("HOST_STATUS_PUBLISHER_ROLE_INPUT_FIELDS_INVALID")
        path_fields = (
            DELL_INPUTS
            if role == "dell"
            else ARENA_INPUTS
            - {
                "dell_key_id",
                "dell_host_id",
                "dell_release_id",
                "legacy_recovery_unit",
            }
        )
        for name in path_fields:
            _required_path(role_inputs[name], f"HOST_STATUS_PUBLISHER_ROLE_PATH_INVALID:{name}")
        for name in ("maximum_ttl_seconds", "maximum_input_age_seconds"):
            number = value[name]
            if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(float(number)):
                raise ValueError(f"HOST_STATUS_PUBLISHER_{name.upper()}_INVALID")
        if not 5 <= float(value["maximum_ttl_seconds"]) <= 60:
            raise ValueError("HOST_STATUS_PUBLISHER_TTL_OUT_OF_RANGE")
        if not 5 <= float(value["maximum_input_age_seconds"]) <= 300:
            raise ValueError("HOST_STATUS_PUBLISHER_INPUT_AGE_OUT_OF_RANGE")
        minimum_remaining = value.get("minimum_remaining_validity_seconds", 0)
        if (
            isinstance(minimum_remaining, bool)
            or not isinstance(minimum_remaining, (int, float))
            or not math.isfinite(float(minimum_remaining))
            or not 0 <= float(minimum_remaining) <= float(value["maximum_ttl_seconds"]) - PUBLISH_COMPLETION_MARGIN_SECONDS
        ):
            raise ValueError("HOST_STATUS_PUBLISHER_MINIMUM_REMAINING_VALIDITY_INVALID")
        contract = read_object(Path(value["host_contract_file"]), maximum_bytes=64 * 1024)
        if contract.get("host_id") != value["host_id"]:
            raise ValueError("HOST_STATUS_PUBLISHER_HOST_CONTRACT_MISMATCH")
        manifest = read_object(Path(value["release_manifest_file"]), maximum_bytes=2 * 1024 * 1024)
        if manifest.get("release_id") != value["release_id"] or manifest.get("component") != role:
            raise ValueError("HOST_STATUS_PUBLISHER_RELEASE_MANIFEST_MISMATCH")
        return cls(value, path)


def _run(arguments: list[str], *, allowed_returncodes: frozenset[int] = frozenset({0})) -> str:
    result = subprocess.run(arguments, check=False, capture_output=True, text=True, timeout=15)
    if result.returncode not in allowed_returncodes:
        raise subprocess.CalledProcessError(result.returncode, arguments, output=result.stdout, stderr=result.stderr)
    return result.stdout.strip()


def _systemctl(unit: str, field: str) -> str:
    return _run(["systemctl", "show", "--value", "-p", field, unit])


def _nonnegative_integer(raw: str, code: str) -> int:
    if not raw.isdigit():
        raise ValueError(code)
    return int(raw)


def _service_snapshot(units: dict[str, str]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for name, unit in units.items():
        invocation = _systemctl(unit, "InvocationID")
        if invocation and INVOCATION_ID.fullmatch(invocation) is None:
            raise ValueError(f"HOST_STATUS_SERVICE_INVOCATION_INVALID:{name}")
        active = _systemctl(unit, "ActiveState")
        sub = _systemctl(unit, "SubState")
        if not active or not sub:
            raise ValueError(f"HOST_STATUS_SERVICE_STATE_INVALID:{name}")
        result[name] = {
            "unit": unit,
            "active_state": active,
            "sub_state": sub,
            "restart_count": _nonnegative_integer(
                _systemctl(unit, "NRestarts"),
                f"HOST_STATUS_SERVICE_RESTART_COUNT_INVALID:{name}",
            ),
            "invocation_id": invocation or None,
        }
    return result


def _journal(unit: str, *matches: str) -> list[dict[str, Any]]:
    arguments = ["journalctl", "--quiet", "-b", "-u", unit, *matches, "--no-pager", "-o", "json"]
    raw = _run(arguments)
    values: list[dict[str, Any]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError("HOST_STATUS_JOURNAL_ROW_INVALID")
        values.append(dict(value))
    return values


def _service_failure_counts(units: dict[str, str]) -> dict[str, int]:
    return {name: len(_journal(unit, f"MESSAGE_ID={FAILURE_MESSAGE_ID}")) for name, unit in units.items()}


def _resource_breach_count(units: dict[str, str]) -> int:
    count = 0
    for unit in units.values():
        count += sum(bool(RESOURCE_FAILURE.search(str(row.get("MESSAGE", "")))) for row in _journal(unit))
    return count


def _invocation_count(unit: str) -> int:
    identities = {
        str(row.get("_SYSTEMD_INVOCATION_ID"))
        for row in _journal(unit)
        if INVOCATION_ID.fullmatch(str(row.get("_SYSTEMD_INVOCATION_ID", ""))) is not None
    }
    return len(identities)


def _journal_counter_state_path(output_file: Path) -> Path:
    return output_file.with_name("host-status-journal-counters.json")


def _counter_map(value: object, names: frozenset[str], code: str) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != names:
        raise ValueError(code)
    result: dict[str, int] = {}
    for name, raw in value.items():
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise ValueError(code)
        result[str(name)] = raw
    return result


def _journal_cursor(raw: str) -> tuple[list[dict[str, Any]], str | None]:
    rows: list[dict[str, Any]] = []
    cursor: str | None = None
    for line in raw.splitlines():
        if line.startswith(JOURNAL_CURSOR_PREFIX):
            candidate = line.removeprefix(JOURNAL_CURSOR_PREFIX).strip()
            if not candidate:
                raise ValueError("HOST_STATUS_JOURNAL_CURSOR_INVALID")
            cursor = candidate
            continue
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError("HOST_STATUS_JOURNAL_ROW_INVALID")
        rows.append(dict(value))
    return rows, cursor


def _journal_counters(
    units: dict[str, str],
    *,
    legacy_service_unit: str | None,
    state_path: Path,
    boot_id: str,
    owner: str,
    group: str,
) -> tuple[dict[str, int], int, int]:
    names = frozenset(units)
    failures = {name: 0 for name in names}
    resource_breaches = {name: 0 for name in names}
    legacy_invocation_count = 0
    legacy_last_invocation_id: str | None = None
    cursor: str | None = None
    if state_path.exists():
        state = read_object(state_path, maximum_bytes=512 * 1024)
        if (
            set(state)
            != {
                "schema",
                "boot_id",
                "cursor",
                "service_units",
                "legacy_service_unit",
                "service_failed_invocation_count",
                "resource_limit_breach_count",
                "legacy_invocation_count",
                "legacy_last_invocation_id",
            }
            or state.get("schema") != JOURNAL_COUNTER_SCHEMA
        ):
            raise ValueError("HOST_STATUS_JOURNAL_COUNTER_STATE_INVALID")
        if state.get("boot_id") == boot_id:
            if state.get("service_units") != units or state.get("legacy_service_unit") != legacy_service_unit:
                raise ValueError("HOST_STATUS_JOURNAL_COUNTER_SCOPE_MISMATCH")
            cursor_value = state.get("cursor")
            if not isinstance(cursor_value, str) or not cursor_value:
                raise ValueError("HOST_STATUS_JOURNAL_COUNTER_CURSOR_INVALID")
            cursor = cursor_value
            failures = _counter_map(
                state.get("service_failed_invocation_count"),
                names,
                "HOST_STATUS_JOURNAL_FAILURE_COUNTER_INVALID",
            )
            resource_breaches = _counter_map(
                state.get("resource_limit_breach_count"),
                names,
                "HOST_STATUS_JOURNAL_RESOURCE_COUNTER_INVALID",
            )
            raw_invocation_count = state.get("legacy_invocation_count")
            if isinstance(raw_invocation_count, bool) or not isinstance(raw_invocation_count, int) or raw_invocation_count < 0:
                raise ValueError("HOST_STATUS_JOURNAL_INVOCATION_STATE_INVALID")
            raw_last_invocation_id = state.get("legacy_last_invocation_id")
            if raw_last_invocation_id is not None and (
                not isinstance(raw_last_invocation_id, str) or INVOCATION_ID.fullmatch(raw_last_invocation_id) is None
            ):
                raise ValueError("HOST_STATUS_JOURNAL_INVOCATION_STATE_INVALID")
            legacy_invocation_count = raw_invocation_count
            legacy_last_invocation_id = raw_last_invocation_id

    unit_to_name = {unit: name for name, unit in units.items()}
    selected_units = sorted(set(unit_to_name) | ({legacy_service_unit} if legacy_service_unit else set()))
    arguments = ["journalctl", "--quiet", "-b"]
    for unit in selected_units:
        arguments.extend(("-u", unit))
    if cursor is not None:
        arguments.append(f"--after-cursor={cursor}")
    arguments.extend(("--no-pager", "--show-cursor", "-o", "json"))
    rows, next_cursor = _journal_cursor(_run(arguments))
    if next_cursor is None:
        _, next_cursor = _journal_cursor(_run(["journalctl", "--quiet", "-b", "-n", "0", "--no-pager", "--show-cursor", "-o", "json"]))
    if next_cursor is None:
        raise ValueError("HOST_STATUS_JOURNAL_CURSOR_MISSING")

    for row in rows:
        row_unit = next(
            (candidate for candidate in (str(row.get("_SYSTEMD_UNIT", "")), str(row.get("UNIT", ""))) if candidate in selected_units),
            None,
        )
        if row_unit is None:
            continue
        name = unit_to_name.get(row_unit)
        if name is not None:
            if row.get("MESSAGE_ID") == FAILURE_MESSAGE_ID:
                failures[name] += 1
            if RESOURCE_FAILURE.search(str(row.get("MESSAGE", ""))):
                resource_breaches[name] += 1
        if row_unit == legacy_service_unit:
            invocation = str(row.get("_SYSTEMD_INVOCATION_ID", ""))
            # A systemd service cannot run overlapping invocations.  Keeping
            # only the last ordered journal ID therefore deduplicates rows
            # without retaining an unbounded set for the lifetime of a boot.
            if INVOCATION_ID.fullmatch(invocation) is not None and invocation != legacy_last_invocation_id:
                legacy_invocation_count += 1
                legacy_last_invocation_id = invocation

    state = {
        "schema": JOURNAL_COUNTER_SCHEMA,
        "boot_id": boot_id,
        "cursor": next_cursor,
        "service_units": units,
        "legacy_service_unit": legacy_service_unit,
        "service_failed_invocation_count": failures,
        "resource_limit_breach_count": resource_breaches,
        "legacy_invocation_count": legacy_invocation_count,
        "legacy_last_invocation_id": legacy_last_invocation_id,
    }
    _write_owned(state_path, state, owner=owner, group=group)
    return failures, sum(resource_breaches.values()), legacy_invocation_count


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(read_regular_bytes(path, maximum_bytes=8 * 1024 * 1024)).hexdigest()


def _configuration_digest(path: Path) -> str:
    metadata = path.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or path.is_symlink() or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise ValueError("HOST_STATUS_CONFIGURATION_DIRECTORY_UNSAFE")
    items: list[bytes] = []
    for item in sorted(path.iterdir(), key=lambda candidate: candidate.name):
        if not item.is_file() or item.is_symlink():
            continue
        digest = hashlib.sha256(read_regular_bytes(item)).hexdigest()
        items.append(f"{digest}  {item}\n".encode())
    if not items:
        raise ValueError("HOST_STATUS_CONFIGURATION_DIRECTORY_EMPTY")
    return hashlib.sha256(b"".join(items)).hexdigest()


def _certificate_remaining(path: Path, current: datetime) -> int:
    certificate = x509.load_pem_x509_certificate(read_regular_bytes(path, maximum_bytes=64 * 1024))
    expires = certificate.not_valid_after_utc.astimezone(UTC)
    return max(0, int((expires - current).total_seconds()))


def _effect_counters(path: Path, current: datetime) -> dict[str, int]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, isolation_level=None, timeout=5.0)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA trusted_schema=OFF")

        def scalar(sql: str) -> int:
            row = connection.execute(sql).fetchone()
            if row is None or isinstance(row[0], bool) or not isinstance(row[0], int) or row[0] < 0:
                raise ValueError("HOST_STATUS_EFFECT_COUNTER_INVALID")
            return int(row[0])

        values = {
            "effect_request_count": scalar("SELECT count(*) FROM typed_effect_requests"),
            "effect_boundary_count": scalar("SELECT count(*) FROM typed_effect_requests WHERE state!='ACCEPTED'"),
            "effect_scope_count": scalar("SELECT count(*) FROM effect_scope_fences"),
            "raw_outcome_unknown_count": scalar("SELECT count(*) FROM typed_effect_requests WHERE state='OUTCOME_UNKNOWN'"),
            "unresolved_scope_count": scalar(
                "SELECT count(*) FROM effect_scope_fences WHERE state IN "
                "('ACCEPTED','EXECUTION_STARTED','EFFECT_BOUNDARY_REACHED','OUTCOME_UNKNOWN')"
            ),
            "reconciliation_count": scalar("SELECT count(*) FROM effect_reconciliations"),
            "duplicate_scope_count": scalar(
                "SELECT coalesce(sum(count_per_scope-1),0) FROM ("
                "SELECT count(*) AS count_per_scope FROM typed_effect_requests "
                "WHERE intent_type='RESTART_FFMPEG' GROUP BY identity_json HAVING count(*) > 1)"
            ),
        }
        row = connection.execute(
            "SELECT coalesce(min(r.finished_at),'') FROM effect_scope_fences f "
            "JOIN typed_effect_requests r ON r.request_id=f.owner_request_id "
            "WHERE f.state IN ('ACCEPTED','EXECUTION_STARTED','EFFECT_BOUNDARY_REACHED','OUTCOME_UNKNOWN')"
        ).fetchone()
        oldest = "" if row is None else str(row[0])
        values["oldest_unresolved_age_seconds"] = max(0, math.ceil((current - parse_utc(oldest)).total_seconds())) if oldest else 0
        return values
    finally:
        connection.close()


def _fresh_status(
    path: Path,
    current: datetime | None,
    maximum_age_seconds: float,
) -> dict[str, Any]:
    value = read_object(path)
    validation_current = (current or datetime.now(UTC)).astimezone(UTC)
    observed = parse_utc(str(value.get("observed_at", "")))
    if observed > validation_current or (validation_current - observed).total_seconds() > maximum_age_seconds:
        raise ValueError(f"HOST_STATUS_COMPONENT_STALE:{path.name}")
    return value


def _dell_payload(config: PublisherConfig, current: datetime | None) -> tuple[dict[str, Any], datetime]:
    value = config.value
    role_inputs = dict(value["role_inputs"])
    target = read_object(Path(role_inputs["target_snapshot_file"]), maximum_bytes=256 * 1024)
    observation = read_object(Path(role_inputs["observation_file"]))
    Draft202012Validator(
        json.loads(Path(role_inputs["observation_schema_file"]).read_text(encoding="utf-8")),
        format_checker=FormatChecker(),
    ).validate(observation)
    private_key = _private_key(_signing_private_key_path(value))
    KeyRing({str(value["key_id"]): private_key.public_key()}).verify(observation)
    validation_current = (current or datetime.now(UTC)).astimezone(UTC)
    target_valid_until = parse_utc(str(target.get("valid_until", "")))
    observation_valid_until = parse_utc(str(observation.get("valid_until", "")))
    if (
        target.get("status") != "VALID"
        or target_valid_until <= validation_current
        or observation.get("schema") != "cra_dell_recovery.observation_bundle.v1"
        or observation.get("source_release_id") != value["release_id"]
        or observation_valid_until <= validation_current
    ):
        raise ValueError("HOST_STATUS_DELL_OBSERVATION_NOT_CURRENT")
    target_identity = target.get("target_identity")
    observation_target = dict(observation.get("target_snapshot") or {}).get("target_identity")
    if not isinstance(target_identity, dict) or observation_target != target_identity:
        raise ValueError("HOST_STATUS_DELL_TARGET_MISMATCH")
    target_digest = hashlib.sha256(canonical_json(target_identity)).hexdigest()
    effects = _effect_counters(Path(role_inputs["effect_database"]), validation_current)
    return (
        {
            "observation": {
                name: observation[name]
                for name in (
                    "schema",
                    "source_release_id",
                    "observation_id",
                    "observation_revision",
                    "observation_sequence",
                    "observed_at",
                    "valid_until",
                    "payload_sha256",
                )
            },
            "target_snapshot_valid": True,
            "target_identity_sha256": target_digest,
            "control_capability_count": 0,
            "physical_effect_count": effects["effect_boundary_count"],
            "effect_counters": effects,
        },
        min(target_valid_until, observation_valid_until),
    )


def _arena_payload(
    config: PublisherConfig,
    current: datetime | None,
    *,
    legacy_invocation_count: int | None = None,
) -> tuple[dict[str, Any], datetime]:
    value = config.value
    role_inputs = dict(value["role_inputs"])
    dell_contract = HostStatusContract(
        Path(role_inputs["dell_host_status_schema_file"]),
        KeyRing({str(role_inputs["dell_key_id"]): _public_key(Path(role_inputs["dell_public_key_file"]))}),
        key_id=str(role_inputs["dell_key_id"]),
        expected_role="dell",
        expected_host_id=str(role_inputs["dell_host_id"]),
        expected_release_id=str(role_inputs["dell_release_id"]),
        maximum_ttl_seconds=60.0,
    )
    dell = dell_contract.decode(read_object(Path(role_inputs["dell_host_status_file"])), now=current)
    maximum_age = float(value["maximum_input_age_seconds"])
    statuses = {
        "dell_pull_status": _fresh_status(Path(role_inputs["dell_pull_status_file"]), current, maximum_age),
        "live_adapter_status": _fresh_status(Path(role_inputs["live_adapter_status_file"]), current, maximum_age),
        "projection_status": _fresh_status(Path(role_inputs["projection_status_file"]), current, maximum_age),
    }
    recovery = read_object(Path(role_inputs["transport_recovery_state_file"]), maximum_bytes=512 * 1024)
    validation_current = (current or datetime.now(UTC)).astimezone(UTC)
    recovery_updated = parse_utc(str(recovery.get("updated_at", "")))
    if recovery_updated > validation_current or (validation_current - recovery_updated).total_seconds() > maximum_age:
        raise ValueError("HOST_STATUS_ARENA_RECOVERY_STATE_STALE")
    legacy_unit = str(role_inputs["legacy_recovery_unit"])
    legacy_active = (
        _run(
            ["systemctl", "is-active", legacy_unit],
            allowed_returncodes=frozenset({0, 3}),
        )
        == "active"
    )
    restart_count = _nonnegative_integer(
        _systemctl(legacy_unit.removesuffix(".timer") + ".service", "NRestarts"),
        "HOST_STATUS_LEGACY_RESTART_COUNT_INVALID",
    )
    invocation_count = (
        _invocation_count(legacy_unit.removesuffix(".timer") + ".service") if legacy_invocation_count is None else legacy_invocation_count
    )
    return (
        {
            "dell_host_status": dell.value,
            **statuses,
            "transport_recovery_health": recovery,
            "legacy_arena_recovery_active": legacy_active,
            "legacy_arena_recovery_restart_count": restart_count,
            "legacy_arena_recovery_invocation_count": invocation_count,
        },
        dell.valid_until,
    )


def _role_payload(
    config: PublisherConfig,
    current: datetime | None,
    *,
    legacy_invocation_count: int | None = None,
    minimum_remaining_seconds: float = 0.0,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    wait: Callable[[float], None] = time.sleep,
) -> tuple[dict[str, Any], datetime]:
    delays = () if current is not None or config.value["role"] == "dell" else UPSTREAM_STATUS_REFRESH_DELAYS_SECONDS
    for attempt in range(len(delays) + 1):
        try:
            if config.value["role"] == "dell":
                payload, valid_until = _dell_payload(config, current)
            else:
                payload, valid_until = (
                    _arena_payload(config, current)
                    if legacy_invocation_count is None
                    else _arena_payload(
                        config,
                        current,
                        legacy_invocation_count=legacy_invocation_count,
                    )
                )
            validation_current = (current or clock()).astimezone(UTC)
            if minimum_remaining_seconds > 0 and (valid_until - validation_current).total_seconds() < minimum_remaining_seconds:
                raise ValueError("HOST_STATUS_INSUFFICIENT_VALIDITY")
            return payload, valid_until
        except ValueError as error:
            if str(error) not in {"HOST_STATUS_EXPIRED", "HOST_STATUS_INSUFFICIENT_VALIDITY"} or attempt == len(delays):
                raise
        wait(delays[attempt])
    raise AssertionError("unreachable")


def _write_owned(path: Path, value: dict[str, Any], *, owner: str, group: str) -> None:
    atomic_write_json(path, value, mode=0o640)
    user = pwd.getpwnam(owner)
    group_value = grp.getgrnam(group)
    os.chown(path, user.pw_uid, group_value.gr_gid, follow_symlinks=False)
    os.chmod(path, 0o640, follow_symlinks=False)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish(config: PublisherConfig, *, now: datetime | None = None) -> dict[str, Any]:
    fixed_current = now.astimezone(UTC) if now is not None else None
    value = config.value
    services = _service_snapshot(dict(value["service_units"]))
    persistent = services[str(value["persistent_service_name"])]
    if persistent["active_state"] != "active" or persistent["invocation_id"] is None:
        raise ValueError("HOST_STATUS_PERSISTENT_SERVICE_NOT_ACTIVE")
    role_inputs = dict(value["role_inputs"])
    rss, fds = read_server_resource_status(
        Path(role_inputs["server_resource_status_file"]),
        expected_role=str(value["role"]),
        expected_host_id=str(value["host_id"]),
        expected_release_id=str(value["release_id"]),
        maximum_age_seconds=float(value["maximum_input_age_seconds"]),
        now=fixed_current,
    )
    disk = os.statvfs(str(value["disk_path"]))
    host_boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    if not host_boot_id:
        raise ValueError("HOST_STATUS_BOOT_ID_MISSING")
    identity = {
        "release_manifest_sha256": _sha256_file(Path(value["release_manifest_file"])),
        "runtime_manifest_sha256": _sha256_file(Path(value["runtime_manifest_file"])),
        "configuration_set_sha256": _configuration_digest(Path(value["configuration_directory"])),
        "maintenance_restart_policy_sha256": _sha256_file(Path(value["maintenance_restart_policy_file"])),
    }
    output_file = Path(value["output_file"])
    legacy_service_unit = None
    if value["role"] == "arena":
        legacy_service_unit = str(role_inputs["legacy_recovery_unit"]).removesuffix(".timer") + ".service"
    service_failures, resource_breach_count, legacy_invocation_count = _journal_counters(
        dict(value["service_units"]),
        legacy_service_unit=legacy_service_unit,
        state_path=_journal_counter_state_path(output_file),
        boot_id=host_boot_id,
        owner=str(value["output_owner"]),
        group=str(value["output_group"]),
    )
    minimum_remaining = float(value.get("minimum_remaining_validity_seconds", 0))
    acquisition_minimum = minimum_remaining + PUBLISH_COMPLETION_MARGIN_SECONDS if minimum_remaining > 0 else 0
    payload, upstream_valid_until = _role_payload(
        config,
        fixed_current,
        legacy_invocation_count=legacy_invocation_count if value["role"] == "arena" else None,
        minimum_remaining_seconds=acquisition_minimum,
    )
    current = fixed_current or datetime.now(UTC)
    ttl_valid_until = current + timedelta(seconds=float(value["maximum_ttl_seconds"]))
    valid_until = min(ttl_valid_until, upstream_valid_until)
    if valid_until <= current:
        raise ValueError("HOST_STATUS_HAS_NO_VALIDITY_WINDOW")
    if (valid_until - current).total_seconds() < minimum_remaining:
        raise ValueError("HOST_STATUS_HAS_INSUFFICIENT_VALIDITY_WINDOW")
    bundle: dict[str, Any] = {
        "schema": SCHEMA,
        "role": value["role"],
        "host_id": value["host_id"],
        "release_id": value["release_id"],
        "sequence": int(current.timestamp() * 1_000_000),
        "observed_at": isoformat_utc(current),
        "valid_until": isoformat_utc(valid_until),
        "host_boot_id": host_boot_id,
        "identity": identity,
        "resources": {
            "disk_free_bytes": disk.f_bavail * disk.f_frsize,
            "resident_memory_bytes": rss,
            "open_fd_count": fds,
            "resource_limit_breach_count": resource_breach_count,
        },
        "services": services,
        "service_failed_invocation_count": service_failures,
        "credential_minimum_remaining_seconds": {
            name: _certificate_remaining(Path(path), current) for name, path in dict(value["credential_files"]).items()
        },
        "payload": payload,
        "key_id": value["key_id"],
    }
    signer = Signer(str(value["key_id"]), _private_key(_signing_private_key_path(value)))
    signed = signer.sign(bundle)
    Draft202012Validator(
        json.loads(Path(value["host_status_schema_file"]).read_text(encoding="utf-8")),
        format_checker=FormatChecker(),
    ).validate(signed)
    completion_current = fixed_current or datetime.now(UTC)
    if valid_until <= completion_current or (valid_until - completion_current).total_seconds() < minimum_remaining:
        raise ValueError("HOST_STATUS_VALIDITY_HEADROOM_ERODED")
    _write_owned(
        output_file,
        signed,
        owner=str(value["output_owner"]),
        group=str(value["output_group"]),
    )
    return signed


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish one signed local-host NO_ACTION soak status")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args()
    config = PublisherConfig.load(args.config)
    if args.check_config:
        _private_key(_signing_private_key_path(config.value))
        print(json.dumps({"config": "VALID", "role": config.value["role"], "schema": config.value["schema"]}, sort_keys=True))
        return
    result = publish(config)
    print(
        json.dumps(
            {
                "schema": result["schema"],
                "role": result["role"],
                "release_id": result["release_id"],
                "sequence": result["sequence"],
                "observed_at": result["observed_at"],
                "valid_until": result["valid_until"],
                "control_capability_count": 0,
                "physical_effect_count": 0,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
