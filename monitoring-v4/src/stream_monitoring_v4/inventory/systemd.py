from __future__ import annotations

import fnmatch
import json
import re
import subprocess
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SNAPSHOT_SCHEMA = "monitoring_v4.systemd_unit_snapshot.v1"
UNIT_NAME = re.compile(r"^[A-Za-z0-9_.@:-]+\.(?:service|timer)$")
STATE_VALUE = re.compile(r"^[A-Za-z0-9_.@:-]{1,96}$")
HOST_ROLE = re.compile(r"^[A-Za-z0-9_.-]{1,96}$")
ERROR_TYPE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,95}$")
RunCommand = Callable[[Sequence[str], float], subprocess.CompletedProcess[str]]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _run(command: Sequence[str], timeout_sec: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        text=True,
        encoding="utf-8",
        errors="replace",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_sec,
        check=False,
    )


def collection_failure_code(host_role: str, error: BaseException) -> str:
    """Return a bounded failure code without leaking an endpoint or stderr.

    Collection errors frequently contain SSH aliases, addresses, or a remote
    command's stderr.  Inventory artifacts only need the failed role and the
    exception class; raw diagnostics must be inspected through a separate,
    access-controlled troubleshooting path when they are needed.
    """

    if not isinstance(host_role, str) or not HOST_ROLE.fullmatch(host_role):
        raise ValueError("collection failure host_role is invalid")
    error_type = type(error).__name__
    if not ERROR_TYPE.fullmatch(error_type):
        error_type = "Exception"
    return f"collection_failed:{host_role}:{error_type}"


def parse_unit_file_names(text: str, patterns: Sequence[str]) -> list[str]:
    """Return the exact installed service/timer names selected by the contract."""

    selected: set[str] = set()
    for raw in text.splitlines():
        fields = raw.split()
        if not fields:
            continue
        name = fields[0]
        if not UNIT_NAME.fullmatch(name):
            continue
        if any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns):
            selected.add(name)
    return sorted(selected)


def parse_show_blocks(text: str) -> dict[str, dict[str, str]]:
    """Parse ``systemctl show`` key/value blocks without trusting field order."""

    result: dict[str, dict[str, str]] = {}
    current: dict[str, str] = {}

    def commit() -> None:
        nonlocal current
        name = current.get("Id", "")
        if name:
            if not UNIT_NAME.fullmatch(name):
                raise ValueError(f"invalid systemd unit id: {name!r}")
            if name in result:
                raise ValueError(f"duplicate systemd unit block: {name}")
            result[name] = current
        current = {}

    for raw in [*text.splitlines(), ""]:
        if not raw.strip():
            commit()
            continue
        key, separator, value = raw.partition("=")
        if not separator or not key:
            raise ValueError("invalid systemctl show output")
        current[key] = value.strip()
    return result


def _validated_state(value: str, *, field: str, unit: str) -> str:
    if not STATE_VALUE.fullmatch(value):
        raise ValueError(f"invalid {field} for {unit}: {value!r}")
    return value


def collect_systemd_snapshot(
    host_role: str,
    patterns: Sequence[str],
    *,
    command_prefix: Sequence[str] = (),
    timeout_sec: float = 30.0,
    run_command: RunCommand = _run,
) -> dict[str, Any]:
    """Collect a secret-free, read-only snapshot of the selected unit surface.

    ``command_prefix`` is an argv prefix, not a shell fragment.  It permits a
    central audit to use e.g. ``("ssh", "arena-server")`` while keeping the
    remote endpoint out of the resulting artifact.
    """

    if not isinstance(host_role, str) or not HOST_ROLE.fullmatch(host_role):
        raise ValueError("host_role is required and must be a bounded identifier")
    normalized_patterns = tuple(str(item) for item in patterns)
    if not normalized_patterns or any(not item or len(item) > 160 for item in normalized_patterns):
        raise ValueError("at least one bounded systemd unit pattern is required")
    prefix = tuple(str(item) for item in command_prefix)
    list_command = (
        *prefix,
        "systemctl",
        "list-unit-files",
        "--no-legend",
        "--no-pager",
        "--type=service",
        "--type=timer",
    )
    listed = run_command(list_command, timeout_sec)
    if listed.returncode != 0:
        detail = (listed.stderr or listed.stdout or "systemctl list-unit-files failed").strip()
        raise RuntimeError(detail[-500:])
    names = parse_unit_file_names(listed.stdout, normalized_patterns)
    units: list[dict[str, str]] = []
    if names:
        show_command = (
            *prefix,
            "systemctl",
            "show",
            *names,
            "--no-pager",
            "--property=Id,LoadState,ActiveState,SubState,UnitFileState",
        )
        shown = run_command(show_command, timeout_sec)
        if shown.returncode != 0:
            detail = (shown.stderr or shown.stdout or "systemctl show failed").strip()
            raise RuntimeError(detail[-500:])
        by_name = parse_show_blocks(shown.stdout)
        missing = sorted(set(names) - set(by_name))
        if missing:
            raise RuntimeError(f"systemctl show omitted units: {','.join(missing)}")
        for name in names:
            item = by_name[name]
            units.append(
                {
                    "name": name,
                    "load_state": _validated_state(
                        item.get("LoadState", ""), field="load_state", unit=name
                    ),
                    "active_state": _validated_state(
                        item.get("ActiveState", ""), field="active_state", unit=name
                    ),
                    "sub_state": _validated_state(
                        item.get("SubState", ""), field="sub_state", unit=name
                    ),
                    "unit_file_state": _validated_state(
                        item.get("UnitFileState", ""), field="unit_file_state", unit=name
                    ),
                }
            )
    return {
        "schema": SNAPSHOT_SCHEMA,
        "captured_at_utc": _utc_now(),
        "host_role": host_role,
        "patterns": list(normalized_patterns),
        "unit_count": len(units),
        "units": units,
    }


def validate_systemd_snapshot(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("schema") != SNAPSHOT_SCHEMA:
        raise ValueError("unsupported systemd snapshot schema")
    host_role = payload.get("host_role")
    if not isinstance(host_role, str) or not HOST_ROLE.fullmatch(host_role):
        raise ValueError("systemd snapshot host_role is invalid")
    units = payload.get("units")
    if not isinstance(units, list):
        raise ValueError("systemd snapshot units must be a list")
    seen: set[str] = set()
    normalized: list[dict[str, str]] = []
    for index, raw in enumerate(units):
        if not isinstance(raw, dict):
            raise ValueError(f"systemd snapshot unit {index} is not an object")
        name = raw.get("name")
        if not isinstance(name, str) or not UNIT_NAME.fullmatch(name):
            raise ValueError(f"systemd snapshot unit {index} has invalid name")
        if name in seen:
            raise ValueError(f"duplicate systemd snapshot unit: {name}")
        seen.add(name)
        normalized.append(
            {
                "name": name,
                "load_state": _validated_state(
                    str(raw.get("load_state", "")), field="load_state", unit=name
                ),
                "active_state": _validated_state(
                    str(raw.get("active_state", "")), field="active_state", unit=name
                ),
                "sub_state": _validated_state(
                    str(raw.get("sub_state", "")), field="sub_state", unit=name
                ),
                "unit_file_state": _validated_state(
                    str(raw.get("unit_file_state", "")), field="unit_file_state", unit=name
                ),
            }
        )
    declared_count = payload.get("unit_count")
    if not isinstance(declared_count, int) or declared_count != len(normalized):
        raise ValueError("systemd snapshot unit_count does not match units")
    patterns = payload.get("patterns")
    if not isinstance(patterns, list) or any(not isinstance(item, str) for item in patterns):
        raise ValueError("systemd snapshot patterns are invalid")
    return {
        "schema": SNAPSHOT_SCHEMA,
        "captured_at_utc": str(payload.get("captured_at_utc", ""))[:64],
        "host_role": host_role,
        "patterns": list(patterns),
        "unit_count": len(normalized),
        "units": sorted(normalized, key=lambda item: item["name"]),
    }


def load_systemd_snapshot(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read systemd snapshot: {exc}") from exc
    return validate_systemd_snapshot(payload)
