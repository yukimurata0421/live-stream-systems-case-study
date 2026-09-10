from __future__ import annotations

import argparse
import grp
import hashlib
import json
import math
import os
import pwd
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from cra_dell_recovery.canonical import KeyRing, canonical_json
from cra_dell_recovery.errors import SignatureValidationError, UnknownKeyError
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.time import isoformat_utc, parse_utc

from .host_status import atomic_write_json, read_object, read_server_resource_status
from .host_status_publisher import (
    _certificate_remaining,
    _configuration_digest,
    _effect_counters,
    _private_key,
    _sha256_file,
    _signing_private_key_path,
    _systemctl,
)
from .resilient_host_status import (
    STATE_SCHEMA,
    ComponentStatus,
    ExactTransitionBinding,
    build_resilient_status,
    record_publisher_failure,
)

CONFIG_SCHEMA = "cra.resilient_host_status_publisher.v3"
ROLE_SERVICES = frozenset(
    {
        "k3s_service",
        "target_snapshot_producer",
        "dell_observation_server",
        "dell_resilient_host_status_publisher",
    }
)
ROLE_INPUTS = frozenset(
    {
        "target_snapshot_file",
        "target_snapshot_schema_file",
        "observation_file",
        "observation_schema_file",
        "effect_database",
        "server_resource_status_file",
    }
)


class PublisherConfig:
    def __init__(self, value: dict[str, Any], path: Path) -> None:
        self.value = value
        self.path = path

    @classmethod
    def load(cls, path: Path) -> PublisherConfig:
        value = read_object(path, maximum_bytes=128 * 1024)
        required = {
            "schema",
            "role",
            "host_id",
            "target_source_host_id",
            "host_contract_file",
            "release_id",
            "release_manifest_file",
            "runtime_manifest_file",
            "configuration_directory",
            "maintenance_restart_policy_file",
            "disk_path",
            "service_units",
            "credential_files",
            "signing_private_key_file",
            "key_id",
            "resilient_host_status_schema_file",
            "output_file",
            "state_file",
            "transition_journal_file",
            "output_owner",
            "output_group",
            "reporter_lease_seconds",
            "last_good_retention_seconds",
            "component_validity_seconds",
            "maximum_input_age_seconds",
            "minimum_disk_free_bytes",
            "minimum_credential_remaining_seconds",
            "clock_uncertainty_bound_ms",
            "maximum_clock_tracking_age_seconds",
            "clock_tracking_file",
            "role_inputs",
        }
        if set(value) != required or value.get("schema") != CONFIG_SCHEMA or value.get("role") != "dell":
            raise ValueError("RESILIENT_STATUS_PUBLISHER_CONFIG_FIELDS_INVALID")
        for name in (
            "host_id",
            "target_source_host_id",
            "release_id",
            "key_id",
            "output_owner",
            "output_group",
        ):
            if not isinstance(value.get(name), str) or not str(value[name]).strip():
                raise ValueError(f"RESILIENT_STATUS_PUBLISHER_{name.upper()}_INVALID")
        require_runtime_release(str(value["release_id"]), "RESILIENT_STATUS_PUBLISHER_RUNTIME_RELEASE_MISMATCH")
        for name in (
            "host_contract_file",
            "release_manifest_file",
            "runtime_manifest_file",
            "configuration_directory",
            "maintenance_restart_policy_file",
            "disk_path",
            "signing_private_key_file",
            "resilient_host_status_schema_file",
            "output_file",
            "state_file",
            "transition_journal_file",
            "clock_tracking_file",
        ):
            if not isinstance(value.get(name), str) or not str(value[name]).startswith("/"):
                raise ValueError(f"RESILIENT_STATUS_PUBLISHER_{name.upper()}_INVALID")
        services = value.get("service_units")
        if not isinstance(services, dict) or set(services) != ROLE_SERVICES:
            raise ValueError("RESILIENT_STATUS_PUBLISHER_SERVICE_FIELDS_INVALID")
        if not all(isinstance(unit, str) and unit.endswith(".service") for unit in services.values()):
            raise ValueError("RESILIENT_STATUS_PUBLISHER_SERVICE_UNIT_INVALID")
        role_inputs = value.get("role_inputs")
        if not isinstance(role_inputs, dict) or set(role_inputs) != ROLE_INPUTS:
            raise ValueError("RESILIENT_STATUS_PUBLISHER_ROLE_INPUTS_INVALID")
        if not all(isinstance(item, str) and item.startswith("/") for item in role_inputs.values()):
            raise ValueError("RESILIENT_STATUS_PUBLISHER_ROLE_INPUT_PATH_INVALID")
        credentials = value.get("credential_files")
        if not isinstance(credentials, dict) or set(credentials) != {"dell_target_observer"}:
            raise ValueError("RESILIENT_STATUS_PUBLISHER_CREDENTIAL_FIELDS_INVALID")
        if not all(isinstance(item, str) and item.startswith("/") for item in credentials.values()):
            raise ValueError("RESILIENT_STATUS_PUBLISHER_CREDENTIAL_PATH_INVALID")
        for name, lower, upper in (
            ("reporter_lease_seconds", 15, 300),
            ("last_good_retention_seconds", 60, 86400),
            ("component_validity_seconds", 5, 60),
            ("maximum_input_age_seconds", 5, 300),
            ("minimum_disk_free_bytes", 64 * 1024 * 1024, 1024 * 1024 * 1024 * 1024),
            ("minimum_credential_remaining_seconds", 60, 366 * 86400),
            ("clock_uncertainty_bound_ms", 1, 5000),
            ("maximum_clock_tracking_age_seconds", 1, 60),
        ):
            raw = value[name]
            if (
                isinstance(raw, bool)
                or not isinstance(raw, (int, float))
                or not math.isfinite(float(raw))
                or not lower <= float(raw) <= upper
            ):
                raise ValueError(f"RESILIENT_STATUS_PUBLISHER_{name.upper()}_INVALID")
        if float(value["maximum_clock_tracking_age_seconds"]) > float(value["component_validity_seconds"]):
            raise ValueError("RESILIENT_STATUS_PUBLISHER_CLOCK_TRACKING_BUDGET_INVALID")
        contract = read_object(Path(value["host_contract_file"]), maximum_bytes=64 * 1024)
        if contract.get("host_id") != value["host_id"]:
            raise ValueError("RESILIENT_STATUS_PUBLISHER_HOST_CONTRACT_MISMATCH")
        manifest = read_object(Path(value["release_manifest_file"]), maximum_bytes=2 * 1024 * 1024)
        if manifest.get("release_id") != value["release_id"] or manifest.get("component") != "dell":
            raise ValueError("RESILIENT_STATUS_PUBLISHER_RELEASE_MANIFEST_MISMATCH")
        return cls(value, path)


def _digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _component_from_error(name: str, error: BaseException) -> ComponentStatus:
    state = "MISSING" if isinstance(error, FileNotFoundError) else "ERROR"
    code = str(error)
    if not code.isupper() or not code.replace("_", "").isalnum():
        code = f"{name.upper()}_UNAVAILABLE"
    return ComponentStatus(name, state, code[:128])


def _target_component(
    path: Path,
    schema_path: Path,
    *,
    expected_source_host_id: str,
    current: datetime | None,
) -> tuple[list[ComponentStatus], dict[str, Any] | None, datetime | None, datetime | None]:
    try:
        value = read_object(path, maximum_bytes=256 * 1024)
        Draft202012Validator(
            json.loads(schema_path.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        ).validate(value)
        validation_current = (current or datetime.now(UTC)).astimezone(UTC)
        if value.get("schema") != "cra_dell_recovery.target_snapshot.v1":
            raise ValueError("TARGET_SNAPSHOT_SCHEMA_INVALID")
        observed = parse_utc(str(value.get("observed_at", "")))
        valid_until = parse_utc(str(value.get("valid_until", "")))
        target = value.get("target_identity")
        if value.get("status") != "VALID" or not isinstance(target, dict):
            return (
                [
                    ComponentStatus("target_snapshot", "INVALID", "TARGET_SNAPSHOT_INVALID", observed, valid_until),
                    ComponentStatus("runtime_identity", "INVALID", "RUNTIME_IDENTITY_INVALID"),
                    ComponentStatus("ffmpeg_identity", "INVALID", "FFMPEG_IDENTITY_INVALID"),
                ],
                None,
                observed,
                valid_until,
            )
        target_value = dict(target)
        if target_value.get("host_id") != expected_source_host_id:
            raise ValueError("TARGET_SNAPSHOT_SOURCE_HOST_MISMATCH")
        digest = _digest(target_value)
        runtime = value.get("runtime_identity")
        runtime_valid = (
            value.get("runtime_status") == "VALID" and value.get("runtime_container_ready") is True and isinstance(runtime, dict)
        )
        runtime_digest = _digest(dict(runtime)) if isinstance(runtime, dict) else None
        if valid_until <= validation_current:
            return (
                [
                    ComponentStatus("target_snapshot", "STALE", "TARGET_SNAPSHOT_STALE", observed, valid_until, digest),
                    ComponentStatus("runtime_identity", "STALE", "RUNTIME_IDENTITY_STALE", observed, valid_until, runtime_digest),
                    ComponentStatus("ffmpeg_identity", "STALE", "FFMPEG_IDENTITY_STALE", observed, valid_until, digest),
                ],
                target_value,
                observed,
                valid_until,
            )
        return (
            [
                ComponentStatus("target_snapshot", "FRESH", "TARGET_SNAPSHOT_FRESH", observed, valid_until, digest),
                ComponentStatus(
                    "runtime_identity",
                    "FRESH" if runtime_valid else "INVALID",
                    "RUNTIME_IDENTITY_FRESH" if runtime_valid else "RUNTIME_IDENTITY_INVALID",
                    observed if runtime_valid else None,
                    valid_until if runtime_valid else None,
                    runtime_digest,
                ),
                ComponentStatus("ffmpeg_identity", "FRESH", "FFMPEG_IDENTITY_FRESH", observed, valid_until, digest),
            ],
            target_value,
            observed,
            valid_until,
        )
    except (OSError, ValueError, ValidationError, json.JSONDecodeError, KeyError, TypeError) as error:
        return (
            [
                _component_from_error("target_snapshot", error),
                ComponentStatus("runtime_identity", "ERROR", "RUNTIME_IDENTITY_UNAVAILABLE"),
                ComponentStatus("ffmpeg_identity", "ERROR", "FFMPEG_IDENTITY_UNAVAILABLE"),
            ],
            None,
            None,
            None,
        )


def _observation_component(
    path: Path,
    schema_path: Path,
    *,
    public_key: Any,
    key_id: str,
    expected_release_id: str,
    expected_target: dict[str, Any] | None,
    current: datetime | None,
) -> tuple[ComponentStatus, dict[str, Any] | None, datetime | None, datetime | None]:
    try:
        value = read_object(path)
        Draft202012Validator(
            json.loads(schema_path.read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        ).validate(value)
        KeyRing({key_id: public_key}).verify(value)
        validation_current = (current or datetime.now(UTC)).astimezone(UTC)
        if value.get("schema") != "cra_dell_recovery.observation_bundle.v1":
            raise ValueError("DELL_OBSERVATION_SCHEMA_INVALID")
        if value.get("source_release_id") != expected_release_id:
            raise ValueError("DELL_OBSERVATION_RELEASE_MISMATCH")
        observed = parse_utc(str(value["observed_at"]))
        valid_until = parse_utc(str(value["valid_until"]))
        observation_target = dict(value.get("target_snapshot") or {}).get("target_identity")
        if expected_target is not None and observation_target != expected_target:
            raise ValueError("DELL_OBSERVATION_TARGET_MISMATCH")
        digest = str(value["payload_sha256"])
        if valid_until <= validation_current:
            component = ComponentStatus("dell_observation", "STALE", "DELL_OBSERVATION_STALE", observed, valid_until, digest)
            return component, value, observed, valid_until
        component = ComponentStatus("dell_observation", "FRESH", "DELL_OBSERVATION_FRESH", observed, valid_until, digest)
        return component, value, observed, valid_until
    except (
        OSError,
        ValueError,
        ValidationError,
        SignatureValidationError,
        UnknownKeyError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
    ) as error:
        return _component_from_error("dell_observation", error), None, None, None


def _service_component(name: str, unit: str, *, current: datetime, validity_seconds: float) -> ComponentStatus:
    try:
        active = _systemctl(unit, "ActiveState")
        sub = _systemctl(unit, "SubState")
        invocation = _systemctl(unit, "InvocationID")
        restart_count = _systemctl(unit, "NRestarts")
        identity = _digest(
            {
                "unit": unit,
                "active_state": active,
                "sub_state": sub,
                "invocation_id": invocation,
                "restart_count": restart_count,
            }
        )
        expected_active = name != "dell_resilient_host_status_publisher"
        healthy = active == ("active" if expected_active else "activating")
        return ComponentStatus(
            name,
            "FRESH" if healthy else "INVALID",
            f"{name.upper()}_ACTIVE" if healthy else f"{name.upper()}_NOT_ACTIVE",
            current,
            current + timedelta(seconds=validity_seconds),
            identity,
        )
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        return _component_from_error(name, error)


def _effect_component(
    path: Path,
    *,
    current: datetime,
    validity_seconds: float,
) -> tuple[ComponentStatus, dict[str, int] | None]:
    try:
        counters = _effect_counters(path, current)
        return (
            ComponentStatus(
                "effect_evidence",
                "FRESH",
                "EFFECT_EVIDENCE_FRESH",
                current,
                current + timedelta(seconds=validity_seconds),
                _digest(counters),
            ),
            counters,
        )
    except (OSError, ValueError, sqlite3.Error) as error:
        return _component_from_error("effect_evidence", error), None


def _chronyc_uncertainty(stdout: str) -> tuple[int, str]:
    fields = [field.strip() for field in stdout.strip().split(",")]
    if len(fields) != 14:
        raise ValueError("CLOCK_TRACKING_FIELD_COUNT_INVALID")
    try:
        stratum = int(fields[2])
        reference_time = float(fields[3])
        system_time_offset = float(fields[4])
        root_delay = float(fields[10])
        root_dispersion = float(fields[11])
        update_interval = float(fields[12])
    except ValueError as error:
        raise ValueError("CLOCK_TRACKING_VALUE_INVALID") from error
    measurements = (
        reference_time,
        system_time_offset,
        root_delay,
        root_dispersion,
        update_interval,
    )
    if (
        not all(math.isfinite(value) for value in measurements)
        or not 1 <= stratum <= 15
        or reference_time <= 0
        or root_delay < 0
        or root_dispersion < 0
        or update_interval <= 0
        or fields[13] != "Normal"
    ):
        raise ValueError("CLOCK_TRACKING_UNSYNCHRONIZED")
    # chrony exposes a signed system offset and non-negative network/root error
    # terms.  This deliberately uses the conservative half-RTT bound rather
    # than treating NTPSynchronized=yes as zero uncertainty.
    uncertainty_seconds = abs(system_time_offset) + (root_delay / 2.0) + root_dispersion
    uncertainty_ms = math.ceil(uncertainty_seconds * 1000.0)
    measurement_digest = _digest(
        {
            "stratum": stratum,
            "reference_time": reference_time,
            "system_time_offset": system_time_offset,
            "root_delay": root_delay,
            "root_dispersion": root_dispersion,
            "update_interval": update_interval,
            "leap_status": fields[13],
        }
    )
    return uncertainty_ms, measurement_digest


def _clock_status(
    *,
    current: datetime | None,
    uncertainty_bound_ms: int,
    tracking_file: Path | None = None,
    maximum_tracking_age_seconds: float = 15.0,
) -> tuple[str, int | None, ComponentStatus]:
    try:
        if tracking_file is None:
            timedatectl = subprocess.run(
                ["timedatectl", "show", "--value", "-p", "NTPSynchronized"],
                check=True,
                capture_output=True,
                text=True,
                timeout=2,
            )
            if timedatectl.stdout.strip().lower() != "yes":
                raise ValueError("CLOCK_SYSTEMD_NOT_SYNCHRONIZED")
            tracking_csv = subprocess.run(
                ["chronyc", "-c", "tracking"],
                check=True,
                capture_output=True,
                text=True,
                timeout=2,
            ).stdout
            validation_current = (current or datetime.now(UTC)).astimezone(UTC)
            observed = validation_current
            valid_until = validation_current + timedelta(seconds=10)
        else:
            fact = read_object(tracking_file, maximum_bytes=64 * 1024)
            validation_current = (current or datetime.now(UTC)).astimezone(UTC)
            if set(fact) != {"schema", "observed_at", "ntp_synchronized", "tracking_csv"}:
                raise ValueError("CLOCK_TRACKING_FACT_FIELDS_INVALID")
            if fact.get("schema") != "cra.clock_tracking_fact.v1" or fact.get("ntp_synchronized") is not True:
                raise ValueError("CLOCK_TRACKING_FACT_UNSYNCHRONIZED")
            observed = parse_utc(str(fact.get("observed_at", "")))
            valid_until = observed + timedelta(seconds=maximum_tracking_age_seconds)
            if observed > validation_current or valid_until <= validation_current:
                raise ValueError("CLOCK_TRACKING_FACT_STALE")
            tracking_csv = str(fact.get("tracking_csv", ""))
        measured_uncertainty_ms, measurement_digest = _chronyc_uncertainty(tracking_csv)
        synchronized = measured_uncertainty_ms <= uncertainty_bound_ms
        return (
            "SYNCED" if synchronized else "UNCERTAIN",
            measured_uncertainty_ms,
            ComponentStatus(
                "clock",
                "FRESH" if synchronized else "INVALID",
                "CLOCK_UNCERTAINTY_WITHIN_BOUND" if synchronized else "CLOCK_UNCERTAINTY_EXCEEDS_BOUND",
                observed,
                valid_until,
                measurement_digest,
            ),
        )
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        return "UNCERTAIN", None, _component_from_error("clock", error)


def _disk_component(
    path: Path,
    *,
    minimum_free_bytes: int,
    current: datetime,
    validity_seconds: float,
) -> ComponentStatus:
    try:
        status = os.statvfs(path)
        free_bytes = status.f_bavail * status.f_frsize
        identity = _digest({"path": str(path), "free_bytes": free_bytes, "free_inodes": status.f_favail})
        healthy = free_bytes >= minimum_free_bytes and status.f_favail > 0
        return ComponentStatus(
            "local_storage",
            "FRESH" if healthy else "INVALID",
            "LOCAL_STORAGE_FRESH" if healthy else "LOCAL_STORAGE_EXHAUSTED",
            current,
            current + timedelta(seconds=validity_seconds),
            identity,
        )
    except OSError as error:
        return _component_from_error("local_storage", error)


def _credential_component(
    paths: dict[str, str],
    *,
    minimum_remaining_seconds: int,
    current: datetime,
    validity_seconds: float,
) -> ComponentStatus:
    try:
        remaining = {name: _certificate_remaining(Path(path), current) for name, path in paths.items()}
        healthy = all(value >= minimum_remaining_seconds for value in remaining.values())
        return ComponentStatus(
            "credentials",
            "FRESH" if healthy else "INVALID",
            "CREDENTIALS_FRESH" if healthy else "CREDENTIALS_EXPIRING",
            current,
            current + timedelta(seconds=validity_seconds),
            _digest(remaining),
        )
    except (OSError, ValueError) as error:
        return _component_from_error("credentials", error)


def _resource_component(
    path: Path,
    *,
    role: str,
    host_id: str,
    release_id: str,
    maximum_age_seconds: float,
    current: datetime | None,
    validity_seconds: float,
) -> ComponentStatus:
    try:
        resident_memory, open_fds = read_server_resource_status(
            path,
            expected_role=role,
            expected_host_id=host_id,
            expected_release_id=release_id,
            maximum_age_seconds=maximum_age_seconds,
            maximum_future_skew_seconds=0.0,
            now=current,
        )
        validation_current = (current or datetime.now(UTC)).astimezone(UTC)
        return ComponentStatus(
            "publisher_resources",
            "FRESH",
            "PUBLISHER_RESOURCES_FRESH",
            validation_current,
            validation_current + timedelta(seconds=validity_seconds),
            _digest({"resident_memory_bytes": resident_memory, "open_fd_count": open_fds}),
        )
    except (OSError, ValueError) as error:
        return _component_from_error("publisher_resources", error)


def _transition_context(state_file: Path) -> tuple[str | None, list[tuple[str | None, str | None]]]:
    if not state_file.exists():
        return None, []
    state = read_object(state_file, maximum_bytes=512 * 1024)
    if state.get("schema") != STATE_SCHEMA:
        raise ValueError("RESILIENT_STATUS_STATE_SCHEMA_INVALID")
    value = state.get("last_target_identity_sha256")
    episodes = state.get("open_episodes")
    if not isinstance(episodes, list):
        raise ValueError("RESILIENT_STATUS_STATE_OPEN_EPISODES_INVALID")
    pending: list[tuple[str | None, str | None]] = []
    for episode in episodes:
        if not isinstance(episode, dict):
            raise ValueError("RESILIENT_STATUS_STATE_OPEN_EPISODE_INVALID")
        before = episode.get("before_target_identity_sha256")
        after = episode.get("after_target_identity_sha256")
        pending.append((str(before) if isinstance(before, str) else None, str(after) if isinstance(after, str) else None))
    return (str(value) if isinstance(value, str) else None), pending


def _exact_transition_binding(
    database: Path,
    *,
    before_digest: str | None,
    after_target: dict[str, Any] | None,
) -> ExactTransitionBinding | None:
    if before_digest is None or after_target is None:
        return None
    after_digest = _digest(after_target)
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, isolation_level=None, timeout=5.0)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA trusted_schema=OFF")
        rows = connection.execute(
            "SELECT effect_scope_id,owner_request_id,state,physical_attempt_count,identity_json,result_json "
            "FROM effect_scope_fences WHERE physical_attempt_count=1 ORDER BY updated_at DESC LIMIT 128"
        ).fetchall()
        matches: list[ExactTransitionBinding] = []
        for row in rows:
            if str(row["state"]) != "RECONCILED_EFFECT_OBSERVED":
                continue
            try:
                before = dict(json.loads(str(row["identity_json"])))
                result = dict(json.loads(str(row["result_json"])))
                observed_target = result.get("observed_target", result.get("after_target"))
                if not isinstance(observed_target, dict):
                    continue
                if _digest(before) != before_digest or _digest(dict(observed_target)) != after_digest:
                    continue
                if int(result.get("physical_effect_count", -1)) != 1:
                    continue
            except (json.JSONDecodeError, TypeError, ValueError):
                continue
            matches.append(
                ExactTransitionBinding(
                    str(row["owner_request_id"]),
                    str(row["effect_scope_id"]),
                    before_digest,
                    after_digest,
                )
            )
        if len(matches) != 1:
            return None
        return matches[0]
    finally:
        connection.close()


def _write_owned(path: Path, value: dict[str, Any], *, owner: str, group: str) -> None:
    atomic_write_json(path, value, mode=0o640)
    user = pwd.getpwnam(owner)
    group_value = grp.getgrnam(group)
    os.chown(path, user.pw_uid, group_value.gr_gid, follow_symlinks=False)
    os.chmod(path, 0o640, follow_symlinks=False)


def _source_window(
    observed_values: list[datetime],
    valid_until_values: list[datetime],
) -> tuple[datetime | None, datetime | None, bool]:
    observed = max(observed_values, default=None)
    valid_until = min(valid_until_values, default=None)
    coherent = observed is None or valid_until is None or valid_until > observed
    if not coherent:
        return None, None, False
    return observed, valid_until, True


def publish(config: PublisherConfig, *, now: datetime | None = None) -> dict[str, Any]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    fixed_current = now is not None
    value = config.value
    role_inputs = dict(value["role_inputs"])
    private_key = _private_key(_signing_private_key_path(value))
    target_components, target, target_observed, target_valid_until = _target_component(
        Path(role_inputs["target_snapshot_file"]),
        Path(role_inputs["target_snapshot_schema_file"]),
        expected_source_host_id=str(value["target_source_host_id"]),
        current=current if fixed_current else None,
    )
    observation_component, observation, observation_observed, observation_valid_until = _observation_component(
        Path(role_inputs["observation_file"]),
        Path(role_inputs["observation_schema_file"]),
        public_key=private_key.public_key(),
        key_id=str(value["key_id"]),
        expected_release_id=str(value["release_id"]),
        expected_target=target,
        current=current if fixed_current else None,
    )
    validity_seconds = float(value["component_validity_seconds"])
    effect_component, effects = _effect_component(
        Path(role_inputs["effect_database"]),
        current=current,
        validity_seconds=validity_seconds,
    )
    clock_state, uncertainty, clock_component = _clock_status(
        current=current if fixed_current else None,
        uncertainty_bound_ms=int(value["clock_uncertainty_bound_ms"]),
        tracking_file=Path(value["clock_tracking_file"]),
        maximum_tracking_age_seconds=float(value["maximum_clock_tracking_age_seconds"]),
    )
    service_components = [
        _service_component(name, unit, current=current, validity_seconds=validity_seconds)
        for name, unit in dict(value["service_units"]).items()
    ]
    disk_component = _disk_component(
        Path(value["disk_path"]),
        minimum_free_bytes=int(value["minimum_disk_free_bytes"]),
        current=current,
        validity_seconds=validity_seconds,
    )
    credential_component = _credential_component(
        dict(value["credential_files"]),
        minimum_remaining_seconds=int(value["minimum_credential_remaining_seconds"]),
        current=current,
        validity_seconds=validity_seconds,
    )
    resource_component = _resource_component(
        Path(role_inputs["server_resource_status_file"]),
        role="dell",
        host_id=str(value["host_id"]),
        release_id=str(value["release_id"]),
        maximum_age_seconds=float(value["maximum_input_age_seconds"]),
        current=current if fixed_current else None,
        validity_seconds=validity_seconds,
    )
    components = [
        *target_components,
        observation_component,
        effect_component,
        clock_component,
        disk_component,
        credential_component,
        resource_component,
        *service_components,
    ]
    target_digest = _digest(target) if target is not None else None
    observation_digest = str(observation["payload_sha256"]) if observation is not None else None
    source_observed_values = [item for item in (target_observed, observation_observed) if item is not None]
    source_valid_values = [item for item in (target_valid_until, observation_valid_until) if item is not None]
    source_observed, source_valid_until, source_window_coherent = _source_window(
        source_observed_values,
        source_valid_values,
    )
    if not source_window_coherent:
        target_digest = None
        observation_digest = None
    previous_digest, pending_transitions = _transition_context(Path(value["state_file"]))
    binding = None
    binding_before = previous_digest
    if previous_digest == target_digest:
        binding_before = next(
            (before for before, after in pending_transitions if before is not None and after == target_digest),
            None,
        )
    if source_window_coherent and binding_before is not None and target is not None:
        try:
            binding = _exact_transition_binding(
                Path(role_inputs["effect_database"]),
                before_digest=binding_before,
                after_target=target,
            )
        except (OSError, sqlite3.Error, ValueError):
            binding = None
    identity = {
        "release_manifest_sha256": _sha256_file(Path(value["release_manifest_file"])),
        "runtime_manifest_sha256": _sha256_file(Path(value["runtime_manifest_file"])),
        "configuration_set_sha256": _configuration_digest(Path(value["configuration_directory"])),
        "maintenance_restart_policy_sha256": _sha256_file(Path(value["maintenance_restart_policy_file"])),
    }
    host_boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    report_current = current if fixed_current else datetime.now(UTC)
    signed = build_resilient_status(
        state_file=Path(value["state_file"]),
        transition_journal=Path(value["transition_journal_file"]),
        schema_file=Path(value["resilient_host_status_schema_file"]),
        private_key=private_key,
        key_id=str(value["key_id"]),
        role="dell",
        host_id=str(value["host_id"]),
        release_id=str(value["release_id"]),
        host_boot_id=host_boot_id,
        identity=identity,
        components=components,
        source_observed_at=source_observed,
        source_valid_until=source_valid_until,
        target_identity_sha256=target_digest,
        source_payload_sha256=observation_digest,
        origin_host_id=str(value["host_id"]) if target is not None and source_window_coherent else None,
        physical_effect_count=effects["effect_boundary_count"] if effects is not None else None,
        exact_transition_binding=binding,
        explicit_reason_codes=("SOURCE_TIME_WINDOW_DISJOINT",) if not source_window_coherent else (),
        reporter_lease_seconds=float(value["reporter_lease_seconds"]),
        last_good_retention_seconds=float(value["last_good_retention_seconds"]),
        clock_state=clock_state,
        clock_uncertainty_ms=uncertainty,
        now=report_current,
    )
    _write_owned(
        Path(value["output_file"]),
        signed,
        owner=str(value["output_owner"]),
        group=str(value["output_group"]),
    )
    return signed


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish fail-operational signed Dell host status v3")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args()
    config = PublisherConfig.load(args.config)
    if args.check_config:
        _private_key(_signing_private_key_path(config.value))
        print(json.dumps({"schema": config.value["schema"], "config": "VALID"}, sort_keys=True))
        return
    try:
        result = publish(config)
        print(
            json.dumps(
                {
                    "schema": result["schema"],
                    "state": result["state"],
                    "producer_sequence": result["producer_sequence"],
                    "observed_at": result["observed_at"],
                    "reason_codes": result["reason_codes"],
                    "nonfresh_components": [component["name"] for component in result["components"] if component["state"] != "FRESH"],
                    "control_capability_count": 0,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    except Exception as error:
        failure_record_error_class = None
        try:
            record_publisher_failure(
                state_file=Path(config.value["state_file"]),
                host_id=str(config.value["host_id"]),
                host_boot_id=Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip(),
            )
        except Exception as record_error:
            failure_record_error_class = type(record_error).__name__
        print(
            json.dumps(
                {
                    "schema": "cra.resilient_host_status_publisher_failure.v1",
                    "status": "SAFE_BLOCKED",
                    "error_class": type(error).__name__,
                    "observed_at": isoformat_utc(datetime.now(UTC)),
                    "control_capability_count": 0,
                    "physical_effect_count": 0,
                    "failure_record_error_class": failure_record_error_class,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        raise


if __name__ == "__main__":
    main()
