from __future__ import annotations

import fnmatch
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from .systemd import HOST_ROLE, validate_systemd_snapshot


CONTRACT_SCHEMA = "monitoring_v4.live_unit_contract.v1"
EXPECTED_FIELDS = ("load_state", "active_state", "sub_state", "unit_file_state")
ALLOWED_DISPOSITIONS = frozenset(
    {
        "ported_v4_shadow",
        "retained_v3",
        "runtime_observer_outside_v4",
        "publisher_outside_v4",
        "platform_dependency",
        "disabled_legacy",
    }
)
COLLECTION_ERROR_CODE = re.compile(
    r"^collection_failed:(?P<role>[A-Za-z0-9_.-]{1,96}):"
    r"(?P<error>[A-Za-z][A-Za-z0-9_]{0,95})$"
)
MAX_COLLECTION_ERRORS = 64


def _strings(value: object, *, field: str, nonempty: bool = True) -> list[str]:
    if not isinstance(value, list) or (nonempty and not value):
        raise ValueError(f"{field} must be a{' non-empty' if nonempty else ''} list")
    if any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{field} entries must be non-empty strings")
    if len(set(value)) != len(value):
        raise ValueError(f"{field} contains duplicate values")
    return list(value)


def load_live_unit_contract(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read live unit contract: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema") != CONTRACT_SCHEMA:
        raise ValueError("unsupported live unit contract schema")
    hosts = payload.get("hosts")
    if not isinstance(hosts, list) or not hosts:
        raise ValueError("live unit contract hosts must be a non-empty list")
    seen_hosts: set[str] = set()
    for host_index, host in enumerate(hosts):
        if not isinstance(host, dict):
            raise ValueError(f"live unit contract host {host_index} is not an object")
        role = host.get("host_role")
        if (
            not isinstance(role, str)
            or not HOST_ROLE.fullmatch(role)
            or role in seen_hosts
        ):
            raise ValueError(f"live unit contract host {host_index} has invalid or duplicate role")
        seen_hosts.add(role)
        patterns = _strings(host.get("patterns"), field=f"host {role} patterns")
        groups = host.get("groups")
        if not isinstance(groups, list) or not groups:
            raise ValueError(f"host {role} groups must be a non-empty list")
        seen_groups: set[str] = set()
        seen_units: set[str] = set()
        for group_index, group in enumerate(groups):
            if not isinstance(group, dict):
                raise ValueError(f"host {role} group {group_index} is not an object")
            group_id = group.get("id")
            if not isinstance(group_id, str) or not group_id or group_id in seen_groups:
                raise ValueError(f"host {role} has invalid or duplicate group id")
            seen_groups.add(group_id)
            units = _strings(group.get("units"), field=f"group {group_id} units")
            overlap = seen_units.intersection(units)
            if overlap:
                raise ValueError(f"host {role} units occur in multiple groups: {sorted(overlap)}")
            seen_units.update(units)
            if any(not any(fnmatch.fnmatchcase(unit, pattern) for pattern in patterns) for unit in units):
                raise ValueError(f"group {group_id} contains a unit outside host patterns")
            expected = group.get("expected")
            if not isinstance(expected, dict):
                raise ValueError(f"group {group_id} expected state is required")
            for field in EXPECTED_FIELDS:
                _strings(expected.get(field), field=f"group {group_id} expected.{field}")
            disposition = group.get("disposition")
            if disposition not in ALLOWED_DISPOSITIONS:
                raise ValueError(f"group {group_id} has invalid disposition")
            if not isinstance(group.get("owner"), str) or not group["owner"].strip():
                raise ValueError(f"group {group_id} owner is required")
            source = group.get("source")
            if not isinstance(source, dict):
                raise ValueError(f"group {group_id} source is required")
            repository = source.get("repository")
            if repository not in {"stream_v3", "stream_v4", "platform"}:
                raise ValueError(f"group {group_id} source repository is invalid")
            directories = source.get("directories", [])
            if repository != "platform":
                _strings(directories, field=f"group {group_id} source directories")
            elif directories not in ([], None):
                _strings(directories, field=f"group {group_id} source directories", nonempty=False)
    return payload


def _source_path(
    unit: str,
    source: Mapping[str, Any],
    repositories: Mapping[str, Path],
) -> str | None:
    repository = str(source.get("repository"))
    if repository == "platform":
        return "platform-managed"
    root = repositories.get(repository)
    if root is None:
        return None
    for raw in source.get("directories", []):
        directory = Path(str(raw))
        if directory.is_absolute() or ".." in directory.parts:
            continue
        candidate = Path(root) / directory / unit
        if candidate.is_file():
            return f"{repository}:{candidate.relative_to(root)}"
    return None


def evaluate_live_unit_contract(
    contract: Mapping[str, Any],
    snapshots: Sequence[Mapping[str, Any]],
    *,
    repositories: Mapping[str, Path],
    collection_errors: Sequence[str] = (),
) -> dict[str, Any]:
    """Compare exact discovered unit surfaces with explicit ownership/state policy."""

    if contract.get("schema") != CONTRACT_SCHEMA:
        raise ValueError("unsupported live unit contract schema")
    contract_roles = {str(item["host_role"]) for item in contract["hosts"]}
    if len(collection_errors) > MAX_COLLECTION_ERRORS:
        raise ValueError("too many systemd collection errors")
    snapshot_errors: list[str] = []
    for code in collection_errors:
        if not isinstance(code, str):
            raise ValueError("systemd collection error codes must be strings")
        match = COLLECTION_ERROR_CODE.fullmatch(code)
        if match is None:
            raise ValueError("invalid systemd collection error code")
        if match.group("role") not in contract_roles:
            raise ValueError("systemd collection error references an unknown host role")
        snapshot_errors.append(code)

    normalized_snapshots: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(snapshots):
        try:
            snapshot = validate_systemd_snapshot(dict(raw))
        except (TypeError, ValueError) as exc:
            snapshot_errors.append(f"invalid_snapshot:{index}:{type(exc).__name__}")
            continue
        role = snapshot["host_role"]
        if role in normalized_snapshots:
            snapshot_errors.append(f"duplicate_snapshot:{role}")
            continue
        normalized_snapshots[role] = snapshot

    hosts: list[dict[str, Any]] = []
    all_source_missing: list[dict[str, str]] = []
    required_roles: list[str] = []
    for host in contract["hosts"]:
        role = str(host["host_role"])
        required = host.get("required_for_r0") is not False
        if required:
            required_roles.append(role)
        groups = host["groups"]
        expected_by_unit: dict[str, tuple[Mapping[str, Any], Mapping[str, Any]]] = {}
        group_summary: list[dict[str, Any]] = []
        source_missing: list[dict[str, str]] = []
        for group in groups:
            for unit in group["units"]:
                expected_by_unit[str(unit)] = (group, group["expected"])
                source_path = _source_path(str(unit), group["source"], repositories)
                if source_path is None:
                    missing = {"host_role": role, "unit": str(unit), "group": str(group["id"])}
                    source_missing.append(missing)
                    all_source_missing.append(missing)
            group_summary.append(
                {
                    "id": group["id"],
                    "unit_count": len(group["units"]),
                    "disposition": group["disposition"],
                    "owner": group["owner"],
                }
            )

        snapshot = normalized_snapshots.get(role)
        missing_units: list[str] = []
        unexpected_units: list[str] = []
        state_mismatches: list[dict[str, Any]] = []
        if snapshot is not None:
            observed = {item["name"]: item for item in snapshot["units"]}
            expected_names = set(expected_by_unit)
            observed_names = set(observed)
            missing_units = sorted(expected_names - observed_names)
            unexpected_units = sorted(observed_names - expected_names)
            for unit in sorted(expected_names & observed_names):
                group, expected = expected_by_unit[unit]
                actual = observed[unit]
                for field in EXPECTED_FIELDS:
                    allowed = list(expected[field])
                    if actual[field] not in allowed:
                        state_mismatches.append(
                            {
                                "unit": unit,
                                "group": group["id"],
                                "field": field,
                                "actual": actual[field],
                                "allowed": allowed,
                            }
                        )
        runtime_verified = snapshot is not None
        static_ok = not source_missing
        runtime_ok = runtime_verified and not missing_units and not unexpected_units and not state_mismatches
        hosts.append(
            {
                "host_role": role,
                "required_for_r0": required,
                "patterns": list(host["patterns"]),
                "expected_unit_count": len(expected_by_unit),
                "observed_unit_count": snapshot["unit_count"] if snapshot else 0,
                "captured_at_utc": snapshot.get("captured_at_utc", "") if snapshot else "",
                "runtime_verified": runtime_verified,
                "missing_units": missing_units,
                "unexpected_units": unexpected_units,
                "state_mismatches": state_mismatches,
                "source_missing": source_missing,
                "groups": group_summary,
                "static_ok": static_ok,
                "runtime_ok": runtime_ok,
                "ok": static_ok and runtime_ok,
            }
        )

    unknown_snapshot_roles = sorted(set(normalized_snapshots) - contract_roles)
    required_hosts = [item for item in hosts if item["required_for_r0"]]
    runtime_complete = bool(required_hosts) and all(item["runtime_ok"] for item in required_hosts)
    static_ok = not all_source_missing
    return {
        "schema": "monitoring_v4.live_unit_contract_report.v1",
        "host_count": len(hosts),
        "required_host_count": len(required_hosts),
        "verified_required_host_count": sum(item["runtime_verified"] for item in required_hosts),
        "static_ok": static_ok,
        "runtime_complete": runtime_complete,
        "evidence_complete": static_ok and runtime_complete and not snapshot_errors and not unknown_snapshot_roles,
        "snapshot_errors": snapshot_errors,
        "unknown_snapshot_roles": unknown_snapshot_roles,
        "source_missing": all_source_missing,
        "hosts": hosts,
    }
