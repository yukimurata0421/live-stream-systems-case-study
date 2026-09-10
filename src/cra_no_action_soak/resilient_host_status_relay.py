from __future__ import annotations

import argparse
import grp
import json
import math
import os
import pwd
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from jsonschema.exceptions import ValidationError

from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.errors import SignatureValidationError, UnknownKeyError
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.time import isoformat_utc, parse_utc

from .host_status import atomic_write_json, read_object
from .host_status_publisher import (
    _configuration_digest,
    _private_key,
    _public_key,
    _sha256_file,
    _signing_private_key_path,
    _systemctl,
)
from .resilient_host_status import (
    ComponentStatus,
    ResilientHostStatusContract,
    build_resilient_status,
    evidence_valid_until,
    record_publisher_failure,
)
from .resilient_host_status_publisher import (
    _clock_status,
    _component_from_error,
    _credential_component,
    _digest,
    _disk_component,
    _resource_component,
)

CONFIG_SCHEMA = "cra.resilient_host_status_relay_publisher.v4"
SERVICE_NAMES = frozenset(
    {
        "arena_resilient_status_publisher",
        "arena_projection_server",
    }
)
PIPELINE_STATUS_NAMES = frozenset({"arena_live_adapter", "arena_projection"})
PIPELINE_STATUS_CONTRACTS = {
    "arena_live_adapter": ("monitoring_v4.cra_live_adapter_status.v2", "adapter_release_id"),
    "arena_projection": ("monitoring_v4.cra_projection_producer_status.v1", "producer_release_id"),
}


class RelayConfig:
    def __init__(self, value: dict[str, Any], path: Path) -> None:
        self.value = value
        self.path = path

    @classmethod
    def load(cls, path: Path) -> RelayConfig:
        value = read_object(path, maximum_bytes=128 * 1024)
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
            "pipeline_status_files",
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
            "communication_unreachable_seconds",
            "minimum_upstream_evidence_seconds",
            "upstream_refresh_delays_seconds",
            "minimum_disk_free_bytes",
            "minimum_credential_remaining_seconds",
            "clock_uncertainty_bound_ms",
            "maximum_clock_tracking_age_seconds",
            "clock_tracking_file",
            "dell_status_file",
            "dell_pull_status_file",
            "dell_key_id",
            "dell_public_key_file",
            "dell_host_id",
            "dell_release_id",
            "server_resource_status_file",
        }
        if set(value) != required or value.get("schema") != CONFIG_SCHEMA or value.get("role") != "arena":
            raise ValueError("RESILIENT_STATUS_RELAY_CONFIG_FIELDS_INVALID")
        for name in (
            "host_id",
            "release_id",
            "key_id",
            "dell_key_id",
            "dell_host_id",
            "dell_release_id",
            "output_owner",
            "output_group",
        ):
            if not isinstance(value.get(name), str) or not str(value[name]).strip():
                raise ValueError(f"RESILIENT_STATUS_RELAY_{name.upper()}_INVALID")
        require_runtime_release(str(value["release_id"]), "RESILIENT_STATUS_RELAY_RUNTIME_RELEASE_MISMATCH")
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
            "dell_status_file",
            "dell_pull_status_file",
            "dell_public_key_file",
            "server_resource_status_file",
            "clock_tracking_file",
        ):
            if not isinstance(value.get(name), str) or not str(value[name]).startswith("/"):
                raise ValueError(f"RESILIENT_STATUS_RELAY_{name.upper()}_INVALID")
        services = value.get("service_units")
        if not isinstance(services, dict) or set(services) != SERVICE_NAMES:
            raise ValueError("RESILIENT_STATUS_RELAY_SERVICE_FIELDS_INVALID")
        if not all(isinstance(unit, str) and unit.endswith(".service") for unit in services.values()):
            raise ValueError("RESILIENT_STATUS_RELAY_SERVICE_UNIT_INVALID")
        pipeline_status_files = value.get("pipeline_status_files")
        if not isinstance(pipeline_status_files, dict) or set(pipeline_status_files) != PIPELINE_STATUS_NAMES:
            raise ValueError("RESILIENT_STATUS_RELAY_PIPELINE_STATUS_FIELDS_INVALID")
        if not all(isinstance(item, str) and item.startswith("/") for item in pipeline_status_files.values()):
            raise ValueError("RESILIENT_STATUS_RELAY_PIPELINE_STATUS_PATH_INVALID")
        credentials = value.get("credential_files")
        if not isinstance(credentials, dict) or set(credentials) != {"arena_dell", "arena_cra"}:
            raise ValueError("RESILIENT_STATUS_RELAY_CREDENTIAL_FIELDS_INVALID")
        if not all(isinstance(item, str) and item.startswith("/") for item in credentials.values()):
            raise ValueError("RESILIENT_STATUS_RELAY_CREDENTIAL_PATH_INVALID")
        for name, lower, upper in (
            ("reporter_lease_seconds", 15, 300),
            ("last_good_retention_seconds", 60, 86400),
            ("component_validity_seconds", 5, 60),
            ("maximum_input_age_seconds", 5, 300),
            ("communication_unreachable_seconds", 15, 600),
            ("minimum_upstream_evidence_seconds", 1, 30),
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
                raise ValueError(f"RESILIENT_STATUS_RELAY_{name.upper()}_INVALID")
        if float(value["maximum_clock_tracking_age_seconds"]) > float(value["component_validity_seconds"]):
            raise ValueError("RESILIENT_STATUS_RELAY_CLOCK_TRACKING_BUDGET_INVALID")
        refresh_delays = value["upstream_refresh_delays_seconds"]
        if (
            not isinstance(refresh_delays, list)
            or not 1 <= len(refresh_delays) <= 16
            or any(
                isinstance(delay, bool)
                or not isinstance(delay, (int, float))
                or not math.isfinite(float(delay))
                or not 0.05 <= float(delay) <= 2
                for delay in refresh_delays
            )
            or sum(float(delay) for delay in refresh_delays) > 8
        ):
            raise ValueError("RESILIENT_STATUS_RELAY_UPSTREAM_REFRESH_DELAYS_INVALID")
        contract = read_object(Path(value["host_contract_file"]), maximum_bytes=64 * 1024)
        if contract.get("host_id") != value["host_id"]:
            raise ValueError("RESILIENT_STATUS_RELAY_HOST_CONTRACT_MISMATCH")
        manifest = read_object(Path(value["release_manifest_file"]), maximum_bytes=2 * 1024 * 1024)
        if manifest.get("release_id") != value["release_id"] or manifest.get("component") != "arena":
            raise ValueError("RESILIENT_STATUS_RELAY_RELEASE_MANIFEST_MISMATCH")
        return cls(value, path)


def _service_component(
    name: str,
    unit: str,
    *,
    current: datetime,
    validity_seconds: float,
) -> ComponentStatus:
    try:
        active = _systemctl(unit, "ActiveState")
        sub = _systemctl(unit, "SubState")
        invocation = _systemctl(unit, "InvocationID")
        restarts = _systemctl(unit, "NRestarts")
        expected = "activating" if name == "arena_resilient_status_publisher" else "active"
        healthy = active == expected
        identity = {
            "unit": unit,
            "active_state": active,
            "sub_state": sub,
            "invocation_id": invocation,
            "restart_count": restarts,
        }
        return ComponentStatus(
            name,
            "FRESH" if healthy else "INVALID",
            f"{name.upper()}_ACTIVE" if healthy else f"{name.upper()}_NOT_ACTIVE",
            current,
            current + timedelta(seconds=validity_seconds),
            _digest(identity),
        )
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        return _component_from_error(name, error)


def _pipeline_status_component(
    name: str,
    path: Path,
    *,
    release_id: str,
    current: datetime | None,
    maximum_age_seconds: float,
) -> ComponentStatus:
    try:
        expected_schema, release_field = PIPELINE_STATUS_CONTRACTS[name]
        value = read_object(path, maximum_bytes=64 * 1024)
        validation_current = (current or datetime.now(UTC)).astimezone(UTC)
        if value.get("schema") != expected_schema:
            raise ValueError(f"{name.upper()}_STATUS_SCHEMA_INVALID")
        if value.get("status") != "READY":
            raise ValueError(f"{name.upper()}_STATUS_NOT_READY")
        if value.get(release_field) != release_id:
            raise ValueError(f"{name.upper()}_STATUS_RELEASE_MISMATCH")
        for field in ("control_capability_count", "physical_effect_count"):
            raw = value.get(field)
            if isinstance(raw, bool) or not isinstance(raw, int) or raw != 0:
                raise ValueError(f"{name.upper()}_{field.upper()}_INVALID")
        observed_at = parse_utc(str(value["observed_at"]))
        age = (validation_current - observed_at).total_seconds()
        if age < 0:
            raise ValueError(f"{name.upper()}_STATUS_FROM_FUTURE")
        valid_until = observed_at + timedelta(seconds=maximum_age_seconds)
        if validation_current > valid_until:
            return ComponentStatus(
                name,
                "STALE",
                f"{name.upper()}_STATUS_STALE",
                observed_at,
                valid_until,
                _digest(value),
            )
        return ComponentStatus(
            name,
            "FRESH",
            f"{name.upper()}_STATUS_FRESH",
            observed_at,
            valid_until,
            _digest(value),
        )
    except (KeyError, OSError, TypeError, ValueError) as error:
        return _component_from_error(name, error)


def _upstream_components(
    value: dict[str, Any],
    *,
    current: datetime | None,
) -> tuple[list[ComponentStatus], dict[str, Any] | None, str | None]:
    schema = Path(value["resilient_host_status_schema_file"])
    contract = ResilientHostStatusContract(
        schema,
        KeyRing({str(value["dell_key_id"]): _public_key(Path(value["dell_public_key_file"]))}),
        key_id=str(value["dell_key_id"]),
        expected_role="dell",
        expected_host_id=str(value["dell_host_id"]),
        expected_release_id=str(value["dell_release_id"]),
        maximum_report_lease_seconds=300,
        maximum_clock_skew_seconds=5,
    )
    maximum_age = float(value["maximum_input_age_seconds"])
    pull_component: ComponentStatus
    last_contact: datetime | None = None
    pull_status: dict[str, Any] = {}
    try:
        pull_status = read_object(Path(value["dell_pull_status_file"]), maximum_bytes=128 * 1024)
        pull_current = (current or datetime.now(UTC)).astimezone(UTC)
        last_contact = parse_utc(str(pull_status.get("observed_at", "")))
        time_valid = last_contact <= pull_current
        fresh = time_valid and (pull_current - last_contact).total_seconds() <= maximum_age
        ready = pull_status.get("schema") == "cra.resilient_host_status_pull_status.v2" and pull_status.get("status") == "READY" and fresh
        if not time_valid:
            pull_component = ComponentStatus("dell_status_pull", "INVALID", "DELL_STATUS_PULL_FUTURE")
        else:
            pull_component = ComponentStatus(
                "dell_status_pull",
                "FRESH" if ready else ("STALE" if not fresh else "INVALID"),
                "DELL_STATUS_PULL_FRESH" if ready else ("DELL_STATUS_PULL_STALE" if not fresh else "DELL_STATUS_PULL_BLOCKED"),
                last_contact,
                last_contact + timedelta(seconds=maximum_age),
            )
    except (
        OSError,
        ValueError,
        ValidationError,
        SignatureValidationError,
        UnknownKeyError,
        json.JSONDecodeError,
    ) as error:
        pull_component = _component_from_error("dell_status_pull", error)
    try:
        upstream_value = read_object(Path(value["dell_status_file"]))
        upstream_current = (current or datetime.now(UTC)).astimezone(UTC)
        upstream = contract.decode(upstream_value, now=upstream_current)
        if pull_component.state != "FRESH":
            raise ValueError("DELL_STATUS_PULL_NOT_CURRENT")
        if (
            pull_status.get("source_producer_instance_id") != upstream["producer_instance_id"]
            or pull_status.get("source_producer_sequence") != upstream["producer_sequence"]
        ):
            raise ValueError("DELL_STATUS_PULL_BINDING_MISMATCH")
        upstream_component = ComponentStatus(
            "dell_signed_status",
            "FRESH",
            "DELL_SIGNED_STATUS_FRESH",
            parse_utc(str(upstream["observed_at"])),
            parse_utc(str(upstream["report_lease_until"])),
            str(upstream["payload_sha256"]),
        )
        return [pull_component, upstream_component], upstream, None
    except (
        OSError,
        ValueError,
        ValidationError,
        SignatureValidationError,
        UnknownKeyError,
        json.JSONDecodeError,
    ) as error:
        upstream_component = _component_from_error("dell_signed_status", error)
        failure_current = (current or datetime.now(UTC)).astimezone(UTC)
        age = None if last_contact is None or last_contact > failure_current else (failure_current - last_contact).total_seconds()
        state = "SOURCE_UNREACHABLE" if age is None or age > float(value["communication_unreachable_seconds"]) else "COMMUNICATION_DEGRADED"
        return [pull_component, upstream_component], None, state


def _write_owned(path: Path, value: dict[str, Any], *, owner: str, group: str) -> None:
    atomic_write_json(path, value, mode=0o640)
    user = pwd.getpwnam(owner)
    group_value = grp.getgrnam(group)
    os.chown(path, user.pw_uid, group_value.gr_gid, follow_symlinks=False)
    os.chmod(path, 0o640, follow_symlinks=False)


def _upstream_evidence_remaining(value: dict[str, Any] | None, *, current: datetime) -> float | None:
    if value is None:
        return None
    return (evidence_valid_until(value) - current).total_seconds()


def _insufficient_upstream_components(components: list[ComponentStatus]) -> list[ComponentStatus]:
    return [
        ComponentStatus(
            component.name,
            "ERROR",
            "DELL_STATUS_INSUFFICIENT_EVIDENCE_VALIDITY",
            identity_sha256=component.identity_sha256,
        )
        if component.name == "dell_signed_status"
        else component
        for component in components
    ]


def publish(config: RelayConfig, *, now: datetime | None = None) -> dict[str, Any]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    fixed_current = now is not None
    value = config.value
    validity_seconds = float(value["component_validity_seconds"])
    upstream_components, upstream, communication_state = _upstream_components(
        value,
        current=current if fixed_current else None,
    )
    minimum_upstream_remaining = float(value["minimum_upstream_evidence_seconds"])
    upstream_checked_at = current if fixed_current else datetime.now(UTC)
    remaining = _upstream_evidence_remaining(upstream, current=upstream_checked_at)
    if not fixed_current:
        for delay in (float(item) for item in value["upstream_refresh_delays_seconds"]):
            if remaining is not None and remaining >= minimum_upstream_remaining:
                break
            time.sleep(delay)
            upstream_components, upstream, communication_state = _upstream_components(value, current=None)
            upstream_checked_at = datetime.now(UTC)
            remaining = _upstream_evidence_remaining(upstream, current=upstream_checked_at)
    if upstream is not None and (remaining is None or remaining < minimum_upstream_remaining):
        upstream_components = _insufficient_upstream_components(upstream_components)
        upstream = None
        communication_state = "COMMUNICATION_DEGRADED"
    local_current = current if fixed_current else datetime.now(UTC)
    service_components = [
        _service_component(name, unit, current=local_current, validity_seconds=validity_seconds)
        for name, unit in dict(value["service_units"]).items()
    ]
    pipeline_components = [
        _pipeline_status_component(
            name,
            Path(path),
            release_id=str(value["release_id"]),
            current=current if fixed_current else None,
            maximum_age_seconds=float(value["maximum_input_age_seconds"]),
        )
        for name, path in dict(value["pipeline_status_files"]).items()
    ]
    clock_state, uncertainty, clock_component = _clock_status(
        current=current if fixed_current else None,
        uncertainty_bound_ms=int(value["clock_uncertainty_bound_ms"]),
        tracking_file=Path(value["clock_tracking_file"]),
        maximum_tracking_age_seconds=float(value["maximum_clock_tracking_age_seconds"]),
    )
    disk_component = _disk_component(
        Path(value["disk_path"]),
        minimum_free_bytes=int(value["minimum_disk_free_bytes"]),
        current=local_current,
        validity_seconds=validity_seconds,
    )
    credential_component = _credential_component(
        dict(value["credential_files"]),
        minimum_remaining_seconds=int(value["minimum_credential_remaining_seconds"]),
        current=local_current,
        validity_seconds=validity_seconds,
    )
    resource_component = _resource_component(
        Path(value["server_resource_status_file"]),
        role="arena",
        host_id=str(value["host_id"]),
        release_id=str(value["release_id"]),
        maximum_age_seconds=float(value["maximum_input_age_seconds"]),
        current=current if fixed_current else None,
        validity_seconds=validity_seconds,
    )
    components = [
        *upstream_components,
        clock_component,
        disk_component,
        credential_component,
        resource_component,
        *pipeline_components,
        *service_components,
    ]
    source = dict(upstream["current"]) if upstream is not None else {}
    explicit_state = communication_state
    explicit_reasons: tuple[str, ...] = ()
    if upstream is not None and upstream["state"] != "READY":
        explicit_state = upstream["state"] if upstream["state"] in {"RECOVERING", "DEGRADED_OBSERVABILITY"} else "DEGRADED_SOURCE"
        explicit_reasons = (f"DELL_{upstream['state']}",)
    identity = {
        "release_manifest_sha256": _sha256_file(Path(value["release_manifest_file"])),
        "runtime_manifest_sha256": _sha256_file(Path(value["runtime_manifest_file"])),
        "configuration_set_sha256": _configuration_digest(Path(value["configuration_directory"])),
        "maintenance_restart_policy_sha256": _sha256_file(Path(value["maintenance_restart_policy_file"])),
    }
    private_key = _private_key(_signing_private_key_path(value))
    host_boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    report_current = current if fixed_current else datetime.now(UTC)
    signed = build_resilient_status(
        state_file=Path(value["state_file"]),
        transition_journal=Path(value["transition_journal_file"]),
        schema_file=Path(value["resilient_host_status_schema_file"]),
        private_key=private_key,
        key_id=str(value["key_id"]),
        role="arena",
        host_id=str(value["host_id"]),
        release_id=str(value["release_id"]),
        host_boot_id=host_boot_id,
        identity=identity,
        components=components,
        source_observed_at=parse_utc(str(source["source_observed_at"])) if source.get("source_observed_at") else None,
        source_valid_until=parse_utc(str(source["source_valid_until"])) if source.get("source_valid_until") else None,
        target_identity_sha256=str(source["target_identity_sha256"]) if source.get("target_identity_sha256") else None,
        source_payload_sha256=str(source["source_payload_sha256"]) if source.get("source_payload_sha256") else None,
        origin_host_id=str(source["origin_host_id"]) if source.get("origin_host_id") else None,
        physical_effect_count=(
            int(upstream["physical_effect_count"]) if upstream and upstream["physical_effect_count"] is not None else None
        ),
        upstream_status=upstream,
        track_target_transitions=False,
        explicit_state=explicit_state,
        explicit_reason_codes=explicit_reasons,
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
    parser = argparse.ArgumentParser(description="Publish fail-operational signed arena relay status v3")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args()
    config = RelayConfig.load(args.config)
    if args.check_config:
        _private_key(_signing_private_key_path(config.value))
        print(json.dumps({"schema": CONFIG_SCHEMA, "config": "VALID"}, sort_keys=True))
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
                    "schema": "cra.resilient_host_status_relay_failure.v1",
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
