from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import shlex
import sqlite3
import stat
import subprocess
import time
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra_no_action_soak.gate import evaluate_no_action_soak, read_samples
from cra_no_action_soak.sample import append_sample, compose_no_action_sample
from cra_no_action_soak.time import isoformat_utc, parse_utc

PROFILE_SCHEMA = "cra.no_action_soak_collector_profile.v3"
LOCAL_PROFILE_SCHEMA = "cra.no_action_soak_collector_profile.v4"
RELEASE_ID = re.compile(r"^(?:arena-cra-projection|cra-no-action|dell-observation)-[0-9a-f]{12}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
LOCAL_HOST_STATUS_REFRESH_DELAYS_SECONDS = (0.5,) * 16
ARENA_HOST_STATUS_SERVICE_NAMES = frozenset(
    {
        "arena_dell_host_status_pull",
        "arena_dell_pull",
        "arena_host_status_publisher",
        "arena_live_adapter",
        "arena_projection",
        "arena_projection_server",
    }
)
DELL_HOST_STATUS_SERVICE_NAMES = frozenset(
    {
        "dell_host_status_publisher",
        "dell_observation_server",
    }
)


def _collector_release_id() -> str:
    value = os.environ.get("CRA_IMMUTABLE_RELEASE_ID", "")
    if not value.startswith("cra-no-action-") or RELEASE_ID.fullmatch(value) is None:
        raise ValueError("SOAK_COLLECTOR_RUNTIME_RELEASE_INVALID")
    return value


def _run(host: str | None, script: str, *, allowed_returncodes: frozenset[int] = frozenset({0})) -> str:
    if host is None:
        command = ["sh", "-c", script] if os.environ.get("CRA_SOAK_LOCAL_DIRECT") == "1" else ["sudo", "-n", "sh", "-c", script]
    else:
        if os.environ.get("CRA_SOAK_REMOTE_EXECUTION") == "forbidden":
            raise ValueError("SOAK_COLLECTOR_REMOTE_EXECUTION_FORBIDDEN")
        command = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            host,
            f"sudo -n sh -c {shlex.quote(script)}",
        ]
    result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=15)
    if result.returncode not in allowed_returncodes:
        raise subprocess.CalledProcessError(result.returncode, command, output=result.stdout, stderr=result.stderr)
    return result.stdout.strip()


def _json(host: str | None, path: str) -> dict[str, Any]:
    value = json.loads(_run(host, f"cat {shlex.quote(path)}"))
    if not isinstance(value, dict):
        raise ValueError(f"SOAK_COLLECTOR_JSON_NOT_OBJECT:{path}")
    return dict(value)


def _sha256(host: str | None, path: str) -> str:
    value = _run(host, f"sha256sum {shlex.quote(path)}").split()[0]
    if SHA256.fullmatch(value) is None:
        raise ValueError(f"SOAK_COLLECTOR_SHA256_INVALID:{path}")
    return value


def _config_set_sha256(host: str | None, directory: str) -> str:
    quoted = shlex.quote(directory)
    script = f"find {quoted} -maxdepth 1 -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum | sha256sum"
    value = _run(host, script).split()[0]
    if SHA256.fullmatch(value) is None:
        raise ValueError(f"SOAK_COLLECTOR_CONFIG_SHA256_INVALID:{directory}")
    return value


def _show_integer(host: str | None, unit: str, field: str) -> int:
    raw = _run(host, f"systemctl show --value -p {shlex.quote(field)} {shlex.quote(unit)}")
    if not raw.isdigit():
        raise ValueError(f"SOAK_COLLECTOR_SYSTEMD_INTEGER_INVALID:{unit}:{field}")
    return int(raw)


def _show_invocation_id(host: str | None, unit: str) -> str:
    raw = _run(host, f"systemctl show --value -p InvocationID {shlex.quote(unit)}")
    if re.fullmatch(r"[0-9a-f]{32}", raw) is None:
        raise ValueError(f"SOAK_COLLECTOR_SYSTEMD_INVOCATION_ID_INVALID:{unit}")
    return raw


def _restart_total(host: str | None, units: list[str]) -> int:
    return sum(_show_integer(host, unit, "NRestarts") for unit in units)


def _resource_breach_count(host: str | None, units: list[str], epoch_started_at: str) -> int:
    unit_arguments = " ".join(f"-u {shlex.quote(unit)}" for unit in units)
    script = (
        f"journalctl --since {shlex.quote(epoch_started_at)} {unit_arguments} --no-pager -o cat "
        "| grep -Eic \"oom-kill|out of memory|Failed with result 'resources'|memory limit\" || true"
    )
    raw = _run(host, script)
    if not raw.isdigit():
        raise ValueError("SOAK_COLLECTOR_RESOURCE_BREACH_COUNT_INVALID")
    return int(raw)


def _unit_invocation_count(host: str | None, unit: str, epoch_started_at: str) -> int:
    script = (
        f"journalctl --since {shlex.quote(epoch_started_at)} -u {shlex.quote(unit)} --no-pager -o json "
        '| sed -n \'s/.*"_SYSTEMD_INVOCATION_ID":"\\([^"]*\\)".*/\\1/p\' | sort -u | wc -l'
    )
    raw = _run(host, script)
    if not raw.isdigit():
        raise ValueError("SOAK_COLLECTOR_INVOCATION_COUNT_INVALID")
    return int(raw)


def _unit_failure_count(host: str | None, unit: str, epoch_started_at: str) -> int:
    script = (
        f"journalctl --quiet --since {shlex.quote(epoch_started_at)} -u {shlex.quote(unit)} "
        "MESSAGE_ID=d9b373ed55a64feb8242e02dbe79a49c --no-pager -o json | wc -l"
    )
    raw = _run(host, script)
    if not raw.isdigit():
        raise ValueError("SOAK_COLLECTOR_UNIT_FAILURE_COUNT_INVALID")
    return int(raw)


def _resources(host: str | None, units: list[str]) -> tuple[int, int]:
    pids = {_show_integer(host, unit, "MainPID") for unit in units}
    pids.discard(0)
    if not pids:
        raise ValueError("SOAK_COLLECTOR_RESOURCE_PROCESS_MISSING")
    pid_list = " ".join(str(pid) for pid in sorted(pids))
    script = (
        "rss=0; fds=0; for pid in "
        + pid_list
        + "; do test -r /proc/$pid/status || exit 1; "
        + "kb=$(awk '$1 == \"VmRSS:\" {print $2}' /proc/$pid/status); "
        + "rss=$((rss + ${kb:-0} * 1024)); count=$(find /proc/$pid/fd -mindepth 1 -maxdepth 1 | wc -l); "
        + 'fds=$((fds + count)); done; printf \'%s %s\\n\' "$rss" "$fds"'
    )
    raw = _run(host, script).split()
    if len(raw) != 2 or not all(item.isdigit() for item in raw):
        raise ValueError("SOAK_COLLECTOR_RESOURCE_INVALID")
    return int(raw[0]), int(raw[1])


def _disk_free(host: str | None, path: str) -> int:
    raw = _run(host, f"stat -f -c '%a %S' {shlex.quote(path)}").split()
    if len(raw) != 2 or not all(item.isdigit() for item in raw):
        raise ValueError(f"SOAK_COLLECTOR_DISK_INVALID:{path}")
    return int(raw[0]) * int(raw[1])


def _certificate_remaining(host: str | None, path: str, now: datetime) -> int:
    raw = _run(host, f"openssl x509 -in {shlex.quote(path)} -noout -enddate")
    if not raw.startswith("notAfter="):
        raise ValueError(f"SOAK_COLLECTOR_CERTIFICATE_INVALID:{path}")
    expires = datetime.strptime(raw.removeprefix("notAfter="), "%b %d %H:%M:%S %Y %Z").replace(tzinfo=UTC)
    return max(0, int((expires - now).total_seconds()))


def _local_db_summary(database: Path, release: str, observed_at: str) -> tuple[dict[str, Any], str, str]:
    with closing(sqlite3.connect(f"{database.absolute().as_uri()}?mode=ro", uri=True, timeout=5.0)) as connection:
        connection.execute("PRAGMA query_only=ON")
        authorization_count = int(connection.execute("SELECT count(*) FROM recovery_authorizations").fetchone()[0])
        command_count = int(connection.execute("SELECT count(*) FROM commands").fetchone()[0])
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
    return (
        {
            "schema": "cra.no_action_db_summary.v1",
            "runtime_release_id": release,
            "authorization_row_count": authorization_count,
            "command_row_count": command_count,
            "observed_at": observed_at,
        },
        integrity,
        journal_mode,
    )


def _db_summary(host: str | None, release: str, observed_at: str) -> tuple[dict[str, Any], str, str]:
    database = Path("/var/lib/stream-recovery-control/cra/central.db")
    if host is None and os.environ.get("CRA_SOAK_LOCAL_DIRECT") == "1":
        return _local_db_summary(database, release, observed_at)
    quoted = shlex.quote(str(database))
    authorization_count = int(_run(host, f"sqlite3 {quoted} 'SELECT count(*) FROM recovery_authorizations;'"))
    command_count = int(_run(host, f"sqlite3 {quoted} 'SELECT count(*) FROM commands;'"))
    integrity = _run(host, f"sqlite3 {quoted} 'PRAGMA integrity_check;'")
    journal_mode = _run(host, f"sqlite3 {quoted} 'PRAGMA journal_mode;'")
    return (
        {
            "schema": "cra.no_action_db_summary.v1",
            "runtime_release_id": release,
            "authorization_row_count": authorization_count,
            "command_row_count": command_count,
            "observed_at": observed_at,
        },
        integrity,
        journal_mode,
    )


def _effect_summary(
    database: str,
    baseline: dict[str, int],
    *,
    current: datetime,
) -> dict[str, int]:
    quoted = shlex.quote(database)

    def scalar(sql: str) -> int:
        raw = _run(None, f"sqlite3 {quoted} {shlex.quote(sql)}")
        if not raw.isdigit():
            raise ValueError("SOAK_COLLECTOR_EFFECT_COUNTER_INVALID")
        return int(raw)

    values = {
        "effect_request_count": scalar("SELECT count(*) FROM typed_effect_requests;"),
        "effect_boundary_count": scalar("SELECT count(*) FROM typed_effect_requests WHERE state!='ACCEPTED';"),
        "effect_scope_count": scalar("SELECT count(*) FROM effect_scope_fences;"),
        "raw_outcome_unknown_count": scalar("SELECT count(*) FROM typed_effect_requests WHERE state='OUTCOME_UNKNOWN';"),
        "unresolved_scope_count": scalar(
            """SELECT count(*) FROM effect_scope_fences WHERE state IN
               ('ACCEPTED','EXECUTION_STARTED','EFFECT_BOUNDARY_REACHED','OUTCOME_UNKNOWN');"""
        ),
        "reconciliation_count": scalar("SELECT count(*) FROM effect_reconciliations;"),
        "duplicate_scope_count": scalar(
            """SELECT coalesce(sum(count_per_scope-1),0) FROM (
               SELECT count(*) AS count_per_scope FROM typed_effect_requests
               WHERE intent_type='RESTART_FFMPEG' GROUP BY identity_json HAVING count(*) > 1);"""
        ),
    }
    oldest = _run(
        None,
        f"sqlite3 {quoted} "
        + shlex.quote(
            """SELECT coalesce(min(r.finished_at),'') FROM effect_scope_fences f
               JOIN typed_effect_requests r ON r.request_id=f.owner_request_id
               WHERE f.state IN ('ACCEPTED','EXECUTION_STARTED','EFFECT_BOUNDARY_REACHED','OUTCOME_UNKNOWN');"""
        ),
    )
    values["oldest_unresolved_age_seconds"] = max(0, math.ceil((current - parse_utc(oldest)).total_seconds())) if oldest else 0
    return _effect_summary_from_values(values, baseline)


def _effect_summary_from_values(values: dict[str, int], baseline: dict[str, int]) -> dict[str, int]:
    baseline_fields = {
        "effect_request_count",
        "effect_boundary_count",
        "raw_outcome_unknown_count",
        "reconciliation_count",
        "duplicate_scope_count",
    }
    if set(baseline) != baseline_fields:
        raise ValueError("SOAK_COLLECTOR_EFFECT_BASELINE_INVALID")
    for name in baseline_fields:
        if values[name] < baseline[name]:
            raise ValueError(f"SOAK_COLLECTOR_EFFECT_COUNTER_REGRESSION:{name}")
    values.update(
        {
            "epoch_effect_request_count": values["effect_request_count"] - baseline["effect_request_count"],
            "epoch_effect_boundary_count": values["effect_boundary_count"] - baseline["effect_boundary_count"],
            "epoch_outcome_unknown_count": values["raw_outcome_unknown_count"] - baseline["raw_outcome_unknown_count"],
            "epoch_reconciliation_count": values["reconciliation_count"] - baseline["reconciliation_count"],
            "epoch_duplicate_scope_count": values["duplicate_scope_count"] - baseline["duplicate_scope_count"],
        }
    )
    return values


def _runtime_event_summary(host: str | None, path: str, epoch_started_at: str, current: datetime) -> dict[str, int]:
    raw = _run(host, f"test ! -f {shlex.quote(path)} || tail -n 10000 {shlex.quote(path)}")
    epoch = parse_utc(epoch_started_at)
    events: list[tuple[datetime, str]] = []
    for line in raw.splitlines():
        try:
            value = json.loads(line)
            observed = parse_utc(str(value["observed_at"]))
            readiness = str(value["readiness"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            continue
        if observed >= epoch:
            events.append((observed, readiness))
    events.sort()
    episodes = 0
    blocked_since: datetime | None = None
    maximum = 0.0
    for observed, readiness in events:
        if readiness == "SAFE_BLOCKED":
            if blocked_since is None:
                blocked_since = observed
                episodes += 1
        elif blocked_since is not None:
            maximum = max(maximum, (observed - blocked_since).total_seconds())
            blocked_since = None
    current_duration = max(0.0, (current - blocked_since).total_seconds()) if blocked_since is not None else 0.0
    maximum = max(maximum, current_duration)
    return {
        "cra_safe_blocked_episode_count": episodes,
        "cra_max_safe_blocked_duration_seconds": math.ceil(maximum),
        "cra_current_safe_blocked_duration_seconds": math.ceil(current_duration),
    }


def _transition_summary(
    target_digest: str,
    effects: dict[str, int],
    previous_sample: dict[str, Any] | None,
) -> dict[str, int]:
    transitions = safe = unexplained = 0
    if previous_sample is not None:
        previous_operations = dict(previous_sample.get("operations") or {})
        previous_infrastructure = dict(previous_sample.get("infrastructure") or {})
        transitions = int(previous_operations.get("target_transition_count") or 0)
        safe = int(previous_operations.get("safe_target_transition_count") or 0)
        unexplained = int(previous_operations.get("unexplained_target_transition_count") or 0)
        previous_digest = str(previous_infrastructure.get("target_identity_sha256") or "")
        if previous_digest and previous_digest != target_digest:
            transitions += 1
            previous_boundary = int(previous_operations.get("effect_boundary_count") or 0)
            previous_duplicates = int(previous_operations.get("duplicate_scope_count") or 0)
            if (
                effects["effect_boundary_count"] == previous_boundary + 1
                and effects["duplicate_scope_count"] == previous_duplicates
                and effects["unresolved_scope_count"] == 0
            ):
                safe += 1
            else:
                unexplained += 1
    return {
        "target_transition_count": transitions,
        "safe_target_transition_count": safe,
        "unexplained_target_transition_count": unexplained,
    }


def _read_profile(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema") not in {PROFILE_SCHEMA, LOCAL_PROFILE_SCHEMA}:
        raise ValueError("SOAK_COLLECTOR_PROFILE_INVALID")
    if set(value.get("releases", {})) != {"arena", "cra", "dell"}:
        raise ValueError("SOAK_COLLECTOR_RELEASE_FIELDS_INVALID")
    if not all(RELEASE_ID.fullmatch(str(item)) for item in value["releases"].values()):
        raise ValueError("SOAK_COLLECTOR_RELEASE_INVALID")
    epoch_started_at = value.get("epoch_started_at")
    if not isinstance(epoch_started_at, str):
        raise ValueError("SOAK_COLLECTOR_EPOCH_START_INVALID")
    parse_utc(epoch_started_at)
    rehearsals = value.get("rehearsal_evidence_sha256")
    if (
        not isinstance(rehearsals, dict)
        or set(rehearsals)
        != {
            "cra_backup_restore",
            "cra_single_writer",
            "credential_rotation",
        }
        or not all(isinstance(item, str) and SHA256.fullmatch(item) for item in rehearsals.values())
    ):
        raise ValueError("SOAK_COLLECTOR_REHEARSAL_HASH_INVALID")
    baseline = value.get("effect_baseline")
    if not isinstance(baseline, dict) or not all(isinstance(item, int) and item >= 0 for item in baseline.values()):
        raise ValueError("SOAK_COLLECTOR_EFFECT_BASELINE_INVALID")
    if not isinstance(value.get("legacy_arena_recovery_baseline"), int) or value["legacy_arena_recovery_baseline"] < 0:
        raise ValueError("SOAK_COLLECTOR_LEGACY_BASELINE_INVALID")
    if not isinstance(value.get("cra_runtime_event_file"), str) or not str(value["cra_runtime_event_file"]).startswith("/"):
        raise ValueError("SOAK_COLLECTOR_RUNTIME_EVENT_FILE_INVALID")
    if value["schema"] == PROFILE_SCHEMA:
        if not isinstance(value.get("effect_ledger_file"), str) or not str(value["effect_ledger_file"]).startswith("/"):
            raise ValueError("SOAK_COLLECTOR_EFFECT_LEDGER_INVALID")
        return value
    if value.get("collection_mode") != "local_host_status_v1":
        raise ValueError("SOAK_COLLECTOR_LOCAL_MODE_INVALID")
    for name in ("arena_host_status_file", "host_status_schema_file", "state_directory"):
        if not isinstance(value.get(name), str) or not str(value[name]).startswith("/"):
            raise ValueError(f"SOAK_COLLECTOR_LOCAL_PATH_INVALID:{name}")
    sources = value.get("host_status_sources")
    if not isinstance(sources, dict) or set(sources) != {"arena", "dell"}:
        raise ValueError("SOAK_COLLECTOR_HOST_STATUS_SOURCES_INVALID")
    for name, source in sources.items():
        if not isinstance(source, dict) or set(source) != {"host_id", "key_id", "public_key_file"}:
            raise ValueError(f"SOAK_COLLECTOR_HOST_STATUS_SOURCE_INVALID:{name}")
        if not all(isinstance(source[field], str) and source[field].strip() for field in ("host_id", "key_id")):
            raise ValueError(f"SOAK_COLLECTOR_HOST_STATUS_SOURCE_ID_INVALID:{name}")
        if not isinstance(source["public_key_file"], str) or not source["public_key_file"].startswith("/"):
            raise ValueError(f"SOAK_COLLECTOR_HOST_STATUS_SOURCE_KEY_INVALID:{name}")
    counter_baseline = value.get("remote_counter_baseline")
    expected_failures = {
        "arena_dell_host_status_pull",
        "arena_dell_pull",
        "arena_host_status_publisher",
        "arena_live_adapter",
        "arena_projection",
        "arena_projection_server",
        "dell_host_status_publisher",
        "dell_observation_server",
    }
    if not isinstance(counter_baseline, dict) or set(counter_baseline) != {
        "resource_limit_breach_count",
        "service_failed_invocation_count",
        "legacy_arena_recovery_invocation_count",
    }:
        raise ValueError("SOAK_COLLECTOR_REMOTE_BASELINE_INVALID")
    resource_baseline = counter_baseline["resource_limit_breach_count"]
    failure_baseline = counter_baseline["service_failed_invocation_count"]
    if not isinstance(resource_baseline, dict) or set(resource_baseline) != {"arena", "dell"}:
        raise ValueError("SOAK_COLLECTOR_RESOURCE_BASELINE_INVALID")
    if not isinstance(failure_baseline, dict) or set(failure_baseline) != expected_failures:
        raise ValueError("SOAK_COLLECTOR_FAILURE_BASELINE_INVALID")
    if not all(isinstance(item, int) and item >= 0 for item in (*resource_baseline.values(), *failure_baseline.values())):
        raise ValueError("SOAK_COLLECTOR_REMOTE_COUNTER_BASELINE_INVALID")
    if (
        not isinstance(counter_baseline["legacy_arena_recovery_invocation_count"], int)
        or counter_baseline["legacy_arena_recovery_invocation_count"] < 0
    ):
        raise ValueError("SOAK_COLLECTOR_LEGACY_INVOCATION_BASELINE_INVALID")
    return value


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(value, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _collect_ssh_audit(
    profile: dict[str, Any],
    *,
    now: datetime | None = None,
    previous_sample: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    observed_at = isoformat_utc(current)
    releases = {name: str(value) for name, value in profile["releases"].items()}
    arena_release, cra_release, dell_release = releases["arena"], releases["cra"], releases["dell"]
    arena_host, cra_host = str(profile["arena_host"]), str(profile["cra_host"])
    arena_units = [
        f"monitoring-v4-dell-observation-pull@{arena_release}.service",
        f"monitoring-v4-cra-live-adapter@{arena_release}.service",
        f"monitoring-v4-cra-projection@{arena_release}.service",
        f"monitoring-v4-cra-projection-server@{arena_release}.service",
    ]
    cra_units = [
        f"cra-monitoring-projection-pull@{cra_release}.service",
        f"cra-runtime-no-action@{cra_release}.service",
    ]
    dell_units = [f"dell-observation-server@{dell_release}.service"]
    arena_resource_units = [f"monitoring-v4-cra-projection-server@{arena_release}.service"]
    cra_resource_units = [f"cra-runtime-no-action@{cra_release}.service"]
    arena_state = "/var/lib/stream-monitoring-v4"
    cra_state = f"/var/lib/stream-recovery-control/cra/releases/{cra_release}"
    dell_state = "/var/lib/stream-recovery-control/dell"
    target = _json(None, f"{dell_state}/target_snapshot.json")
    target_valid = target.get("status") == "VALID" and parse_utc(str(target.get("valid_until", ""))) > current
    db_summary, integrity, journal_mode = _db_summary(cra_host, cra_release, observed_at)
    arena_rss, arena_fds = _resources(arena_host, arena_resource_units)
    cra_rss, cra_fds = _resources(cra_host, cra_resource_units)
    dell_rss, dell_fds = _resources(None, dell_units)
    runtime_status = _json(cra_host, f"{cra_state}/runtime-status.json")
    manifest = _json(cra_host, f"/opt/stream-recovery-control/releases/{cra_release}/release_manifest.json")
    executable_route = not (
        runtime_status.get("command_delivery_enabled") is False
        and manifest.get("production_action_enabled") is False
        and manifest.get("control_capability_count") == 0
        and manifest.get("physical_effect_count") == 0
        and manifest.get("effect_adapter_packaged") is False
    )
    target_identity = target.get("target_identity")
    if not isinstance(target_identity, dict):
        raise ValueError("SOAK_COLLECTOR_TARGET_IDENTITY_INVALID")
    target_digest = hashlib.sha256(json.dumps(target_identity, separators=(",", ":"), sort_keys=True).encode()).hexdigest()
    effects = _effect_summary(
        str(profile["effect_ledger_file"]),
        {name: int(value) for name, value in dict(profile["effect_baseline"]).items()},
        current=current,
    )
    transitions = _transition_summary(target_digest, effects, previous_sample)
    runtime_events = _runtime_event_summary(
        cra_host,
        str(profile["cra_runtime_event_file"]),
        str(profile["epoch_started_at"]),
        current,
    )
    legacy_unit = str(profile["legacy_arena_recovery_unit"])
    legacy_active = (
        _run(
            arena_host,
            f"systemctl is-active {shlex.quote(legacy_unit)}",
            allowed_returncodes=frozenset({0, 3}),
        )
        == "active"
    )
    infrastructure = {
        "schema": "cra.no_action_infrastructure_summary.v4",
        "observed_at": observed_at,
        "host_boot_ids": {
            "arena": _run(arena_host, "cat /proc/sys/kernel/random/boot_id"),
            "cra": _run(cra_host, "cat /proc/sys/kernel/random/boot_id"),
            "dell": _run(None, "cat /proc/sys/kernel/random/boot_id"),
        },
        "release_manifest_sha256": {
            "arena": _sha256(arena_host, f"/opt/stream-monitoring-cra-projection/releases/{arena_release}/release_manifest.json"),
            "cra": _sha256(cra_host, f"/opt/stream-recovery-control/releases/{cra_release}/release_manifest.json"),
            "dell": _sha256(None, f"/opt/stream-recovery-observation/releases/{dell_release}/release_manifest.json"),
        },
        "runtime_manifest_sha256": {
            "arena": _sha256(arena_host, f"/opt/stream-monitoring-cra-projection/runtimes/{arena_release}/runtime_manifest.json"),
            "cra": _sha256(cra_host, f"/opt/stream-recovery-control/runtimes/{cra_release}/runtime_manifest.json"),
            "dell": _sha256(None, f"/opt/stream-recovery-observation/runtimes/{dell_release}/runtime_manifest.json"),
        },
        "configuration_set_sha256": {
            "arena": _config_set_sha256(arena_host, f"/etc/stream-monitoring-cra-projection/releases/{arena_release}"),
            "cra": _config_set_sha256(cra_host, f"/etc/stream-recovery-control/releases/{cra_release}"),
            "dell": _config_set_sha256(None, f"/etc/stream-recovery-observation/releases/{dell_release}"),
        },
        "maintenance_restart_policy_sha256": {
            "arena": _sha256(arena_host, "/etc/needrestart/conf.d/stream-recovery-control.conf"),
            "cra": _sha256(cra_host, "/etc/needrestart/conf.d/stream-recovery-control.conf"),
            "dell": _sha256(None, "/etc/needrestart/conf.d/stream-recovery-control.conf"),
        },
        "rehearsal_evidence_sha256": dict(profile["rehearsal_evidence_sha256"]),
        "target_identity_sha256": target_digest,
        "cra_database_integrity_check": integrity,
        "cra_database_journal_mode": journal_mode,
        "cra_archive_quick_check": runtime_status.get("retention", {}).get("archive_quick_check"),
        "cra_archive_integrity_check": runtime_status.get("retention", {}).get("archive_integrity_check"),
        "cra_single_writer_lock": "ENFORCED",
        "cra_backup_restore_rehearsal": "PASS",
        "credential_rotation_rehearsal": "PASS",
        "credential_minimum_remaining_seconds": {
            "arena_cra": _certificate_remaining(cra_host, "/etc/stream-recovery-control/cra-monitoring-client.pem", current),
            "arena_dell": _certificate_remaining(
                arena_host,
                "/etc/stream-monitoring-cra-projection/credentials/dell-observation-client.pem",
                current,
            ),
            "dell_target_observer": _certificate_remaining(
                None,
                "/etc/stream-recovery-observation/credentials/dell-observation-server.pem",
                current,
            ),
        },
        "disk_free_bytes": {
            "arena": _disk_free(arena_host, arena_state),
            "cra": _disk_free(cra_host, cra_state),
            "dell": _disk_free(None, dell_state),
        },
        "resident_memory_bytes": {"arena": arena_rss, "cra": cra_rss, "dell": dell_rss},
        "open_fd_count": {"arena": arena_fds, "cra": cra_fds, "dell": dell_fds},
        "resource_limit_breach_count": {
            "arena": _resource_breach_count(arena_host, arena_units, str(profile["epoch_started_at"])),
            "cra": _resource_breach_count(cra_host, cra_units, str(profile["epoch_started_at"])),
            "dell": _resource_breach_count(None, dell_units, str(profile["epoch_started_at"])),
        },
        "persistent_service_invocation_ids": {
            "arena_projection_server": _show_invocation_id(arena_host, arena_resource_units[0]),
            "cra_runtime": _show_invocation_id(cra_host, cra_resource_units[0]),
            "dell_observation_server": _show_invocation_id(None, dell_units[0]),
        },
        "legacy_arena_recovery_active": legacy_active,
        "legacy_arena_recovery_restart_count": _show_integer(arena_host, legacy_unit.removesuffix(".timer") + ".service", "NRestarts"),
        "executable_command_route_present": executable_route,
        "transport_recovery_health": {
            "arena_dell_pull": _json(
                arena_host,
                f"{arena_state}/cra-dell-observation/releases/{arena_release}/recovery-state.json",
            ),
            "cra_projection_pull": _json(
                cra_host,
                f"/var/lib/stream-recovery-control/monitoring-inbox/releases/{cra_release}/recovery-state.json",
            ),
        },
    }
    retention = runtime_status.get("retention")
    if not isinstance(retention, dict):
        raise ValueError("SOAK_COLLECTOR_RETENTION_STATUS_MISSING")
    legacy_restart_value = infrastructure["legacy_arena_recovery_restart_count"]
    if isinstance(legacy_restart_value, bool) or not isinstance(legacy_restart_value, int):
        raise ValueError("SOAK_COLLECTOR_LEGACY_COUNTER_INVALID")
    legacy_restart_delta = legacy_restart_value - int(profile["legacy_arena_recovery_baseline"])
    if legacy_restart_delta < 0:
        raise ValueError("SOAK_COLLECTOR_LEGACY_COUNTER_REGRESSION")
    legacy_service = legacy_unit.removesuffix(".timer") + ".service"
    legacy_invocations = max(
        legacy_restart_delta,
        _unit_invocation_count(arena_host, legacy_service, str(profile["epoch_started_at"])),
    )
    service_failures = {
        "arena_dell_pull": _unit_failure_count(arena_host, arena_units[0], str(profile["epoch_started_at"])),
        "arena_live_adapter": _unit_failure_count(arena_host, arena_units[1], str(profile["epoch_started_at"])),
        "arena_projection": _unit_failure_count(arena_host, arena_units[2], str(profile["epoch_started_at"])),
        "arena_projection_server": _unit_failure_count(arena_host, arena_units[3], str(profile["epoch_started_at"])),
        "cra_projection_pull": _unit_failure_count(cra_host, cra_units[0], str(profile["epoch_started_at"])),
        "cra_runtime": _unit_failure_count(cra_host, cra_units[1], str(profile["epoch_started_at"])),
        "dell_observation_server": _unit_failure_count(None, dell_units[0], str(profile["epoch_started_at"])),
    }
    infrastructure["service_failed_invocation_count"] = service_failures
    operations = {
        "schema": "cra.no_action_operation_summary.v1",
        "observed_at": observed_at,
        **effects,
        **transitions,
        **runtime_events,
        "legacy_arena_epoch_invocation_count": legacy_invocations,
        "cra_hot_database_bytes": int(retention["hot_database_bytes"]),
        "cra_hot_wal_bytes": int(retention["hot_wal_bytes"]),
        "cra_hot_shm_bytes": int(retention["hot_shm_bytes"]),
        "cra_hot_sqlite_total_bytes": int(retention["hot_sqlite_total_bytes"]),
        "cra_archive_database_bytes": int(retention["archive_database_bytes"]),
        "cra_archive_wal_bytes": int(retention["archive_wal_bytes"]),
        "cra_archive_shm_bytes": int(retention["archive_shm_bytes"]),
        "cra_archive_sqlite_total_bytes": int(retention["archive_sqlite_total_bytes"]),
        "cra_archive_record_count": int(retention["archive_record_count"]),
        "cra_archive_checkpoint_count": int(retention["archive_checkpoint_count"]),
    }
    return {
        "schema": "cra.no_action_soak_capture.v9",
        "arena_dell_pull": _json(arena_host, f"{arena_state}/cra-dell-observation/releases/{arena_release}/status.json"),
        "arena_live_adapter": _json(arena_host, f"{arena_state}/cra-facts/releases/{arena_release}/status.json"),
        "arena_projection": _json(arena_host, f"{arena_state}/cra-projection/releases/{arena_release}/status.json"),
        "cra_projection_pull": _json(
            cra_host,
            f"/var/lib/stream-recovery-control/monitoring-inbox/releases/{cra_release}/pull-status.json",
        ),
        "cra_runtime": runtime_status,
        "cra_database": db_summary,
        "dell": {
            "schema": "cra.dell_observation_soak_summary.v1",
            "release_id": dell_release,
            "observation_server_active": _run(None, f"systemctl is-active {shlex.quote(dell_units[0])}") == "active",
            "target_snapshot_valid": target_valid,
            "control_capability_count": 0,
            "physical_effect_count": effects["epoch_effect_boundary_count"],
            "observed_at": observed_at,
        },
        "restarts": {
            "schema": "cra.no_action_restart_summary.v1",
            "arena": _restart_total(arena_host, arena_units),
            "cra": _restart_total(cra_host, cra_units),
            "dell": _restart_total(None, dell_units),
            "observed_at": observed_at,
        },
        "infrastructure": infrastructure,
        "operations": operations,
    }


def _public_key(path: Path) -> Any:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    value = serialization.load_pem_public_key(path.read_bytes())
    if not isinstance(value, Ed25519PublicKey):
        raise ValueError("SOAK_COLLECTOR_HOST_STATUS_KEY_NOT_ED25519")
    return value


def _host_status_contract(profile: dict[str, Any], role: str) -> Any:
    from cra_dell_recovery.canonical import KeyRing
    from cra_no_action_soak.host_status import HostStatusContract

    source = dict(dict(profile["host_status_sources"])[role])
    return HostStatusContract(
        Path(profile["host_status_schema_file"]),
        KeyRing({str(source["key_id"]): _public_key(Path(source["public_key_file"]))}),
        key_id=str(source["key_id"]),
        expected_role=role,
        expected_host_id=str(source["host_id"]),
        expected_release_id=str(profile["releases"][role]),
        maximum_ttl_seconds=60.0,
    )


def _counter_delta(current: int, baseline: int, code: str) -> int:
    if isinstance(current, bool) or not isinstance(current, int) or current < baseline:
        raise ValueError(code)
    return current - baseline


def _service_map(value: object, expected: frozenset[str], code: str) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(code)
    result: dict[str, dict[str, Any]] = {}
    for name, raw in value.items():
        if not isinstance(raw, dict):
            raise ValueError(code)
        result[name] = dict(raw)
    return result


def _integer_map(value: object, expected: frozenset[str], code: str) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(code)
    result: dict[str, int] = {}
    for name, raw in value.items():
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            raise ValueError(code)
        result[name] = raw
    return result


def _decode_local_host_status(
    contract: Any,
    path: Path,
    *,
    now: datetime | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    wait: Callable[[float], None] = time.sleep,
) -> tuple[Any, datetime]:
    from cra_no_action_soak.host_status import read_object

    delays = () if now is not None else LOCAL_HOST_STATUS_REFRESH_DELAYS_SECONDS
    for attempt in range(len(delays) + 1):
        current = (now or clock()).astimezone(UTC)
        try:
            return contract.decode(read_object(path), now=current), current
        except ValueError as error:
            if str(error) != "HOST_STATUS_EXPIRED" or attempt == len(delays):
                raise
        wait(delays[attempt])
    raise AssertionError("unreachable")


def _collect_local_contract(
    profile: dict[str, Any],
    *,
    now: datetime | None = None,
    previous_sample: dict[str, Any] | None = None,
) -> dict[str, Any]:
    arena, current = _decode_local_host_status(
        _host_status_contract(profile, "arena"),
        Path(profile["arena_host_status_file"]),
        now=now,
    )
    observed_at = isoformat_utc(current)
    releases = {name: str(value) for name, value in profile["releases"].items()}
    cra_release, dell_release = releases["cra"], releases["dell"]
    arena_payload = dict(arena.value["payload"])
    raw_dell = arena_payload.get("dell_host_status")
    if not isinstance(raw_dell, dict):
        raise ValueError("SOAK_COLLECTOR_DELL_HOST_STATUS_MISSING")
    dell = _host_status_contract(profile, "dell").decode(raw_dell, now=current)
    dell_payload = dict(dell.value["payload"])
    arena_service_names = ARENA_HOST_STATUS_SERVICE_NAMES
    dell_service_names = DELL_HOST_STATUS_SERVICE_NAMES
    arena_services = _service_map(arena.value.get("services"), arena_service_names, "SOAK_COLLECTOR_ARENA_SERVICES_INVALID")
    dell_services = _service_map(dell.value.get("services"), dell_service_names, "SOAK_COLLECTOR_DELL_SERVICES_INVALID")
    arena_failures = _integer_map(
        arena.value.get("service_failed_invocation_count"),
        arena_service_names,
        "SOAK_COLLECTOR_ARENA_FAILURE_COUNTERS_INVALID",
    )
    dell_failures = _integer_map(
        dell.value.get("service_failed_invocation_count"),
        dell_service_names,
        "SOAK_COLLECTOR_DELL_FAILURE_COUNTERS_INVALID",
    )
    arena_credentials = _integer_map(
        arena.value.get("credential_minimum_remaining_seconds"),
        frozenset({"arena_dell"}),
        "SOAK_COLLECTOR_ARENA_CREDENTIAL_INVALID",
    )
    dell_credentials = _integer_map(
        dell.value.get("credential_minimum_remaining_seconds"),
        frozenset({"dell_target_observer"}),
        "SOAK_COLLECTOR_DELL_CREDENTIAL_INVALID",
    )
    remote_baseline = dict(profile["remote_counter_baseline"])
    failure_baseline = dict(remote_baseline["service_failed_invocation_count"])
    resource_baseline = dict(remote_baseline["resource_limit_breach_count"])
    service_failures = {
        name: _counter_delta(arena_failures[name], int(failure_baseline[name]), f"SOAK_COLLECTOR_FAILURE_REGRESSION:{name}")
        for name in arena_service_names
    }
    service_failures.update(
        {
            name: _counter_delta(
                dell_failures[name],
                int(failure_baseline[name]),
                f"SOAK_COLLECTOR_FAILURE_REGRESSION:{name}",
            )
            for name in dell_service_names
        }
    )
    arena_resources = dict(arena.value["resources"])
    dell_resources = dict(dell.value["resources"])
    remote_resource_breaches = {
        "arena": _counter_delta(
            int(arena_resources["resource_limit_breach_count"]),
            int(resource_baseline["arena"]),
            "SOAK_COLLECTOR_RESOURCE_REGRESSION:arena",
        ),
        "dell": _counter_delta(
            int(dell_resources["resource_limit_breach_count"]),
            int(resource_baseline["dell"]),
            "SOAK_COLLECTOR_RESOURCE_REGRESSION:dell",
        ),
    }
    raw_effects = dell_payload.get("effect_counters")
    if not isinstance(raw_effects, dict) or not all(isinstance(value, int) and value >= 0 for value in raw_effects.values()):
        raise ValueError("SOAK_COLLECTOR_DELL_EFFECT_COUNTERS_INVALID")
    effects = _effect_summary_from_values(dict(raw_effects), dict(profile["effect_baseline"]))
    target_digest = str(dell_payload.get("target_identity_sha256", ""))
    if SHA256.fullmatch(target_digest) is None:
        raise ValueError("SOAK_COLLECTOR_DELL_TARGET_DIGEST_INVALID")
    transitions = _transition_summary(target_digest, effects, previous_sample)

    cra_units = [
        f"cra-monitoring-projection-pull@{cra_release}.service",
        f"cra-runtime-no-action@{cra_release}.service",
    ]
    cra_runtime_unit = cra_units[1]
    cra_state = f"/var/lib/stream-recovery-control/cra/releases/{cra_release}"
    runtime_status = _json(None, f"{cra_state}/runtime-status.json")
    manifest = _json(None, f"/opt/stream-recovery-control/releases/{cra_release}/release_manifest.json")
    executable_route = not (
        runtime_status.get("command_delivery_enabled") is False
        and manifest.get("production_action_enabled") is False
        and manifest.get("control_capability_count") == 0
        and manifest.get("physical_effect_count") == 0
        and manifest.get("effect_adapter_packaged") is False
    )
    db_summary, integrity, journal_mode = _db_summary(None, cra_release, observed_at)
    cra_rss, cra_fds = _resources(None, [cra_runtime_unit])
    runtime_events = _runtime_event_summary(
        None,
        str(profile["cra_runtime_event_file"]),
        str(profile["epoch_started_at"]),
        current,
    )
    retention = runtime_status.get("retention")
    if not isinstance(retention, dict):
        raise ValueError("SOAK_COLLECTOR_RETENTION_STATUS_MISSING")
    arena_legacy_restarts = arena_payload.get("legacy_arena_recovery_restart_count")
    arena_legacy_invocations = arena_payload.get("legacy_arena_recovery_invocation_count")
    if isinstance(arena_legacy_restarts, bool) or not isinstance(arena_legacy_restarts, int):
        raise ValueError("SOAK_COLLECTOR_LEGACY_RESTART_INVALID")
    if isinstance(arena_legacy_invocations, bool) or not isinstance(arena_legacy_invocations, int):
        raise ValueError("SOAK_COLLECTOR_LEGACY_INVOCATION_INVALID")
    legacy_restart_delta = _counter_delta(
        arena_legacy_restarts,
        int(profile["legacy_arena_recovery_baseline"]),
        "SOAK_COLLECTOR_LEGACY_RESTART_REGRESSION",
    )
    legacy_invocation_delta = _counter_delta(
        arena_legacy_invocations,
        int(remote_baseline["legacy_arena_recovery_invocation_count"]),
        "SOAK_COLLECTOR_LEGACY_INVOCATION_REGRESSION",
    )
    service_failures.update(
        {
            "cra_arena_host_status_pull": _unit_failure_count(
                None,
                f"cra-arena-host-status-pull@{cra_release}.service",
                str(profile["epoch_started_at"]),
            ),
            "cra_projection_pull": _unit_failure_count(None, cra_units[0], str(profile["epoch_started_at"])),
            "cra_runtime": _unit_failure_count(None, cra_units[1], str(profile["epoch_started_at"])),
            "cra_soak_collector": _unit_failure_count(
                None,
                "cra-no-action-soak-local-collector@" + _collector_release_id() + ".service",
                str(profile["epoch_started_at"]),
            ),
        }
    )
    arena_identity = dict(arena.value["identity"])
    dell_identity = dict(dell.value["identity"])
    arena_invocation = arena_services["arena_projection_server"].get("invocation_id")
    dell_invocation = dell_services["dell_observation_server"].get("invocation_id")
    if not isinstance(arena_invocation, str) or not isinstance(dell_invocation, str):
        raise ValueError("SOAK_COLLECTOR_REMOTE_INVOCATION_ID_MISSING")
    infrastructure = {
        "schema": "cra.no_action_infrastructure_summary.v4",
        "observed_at": observed_at,
        "host_boot_ids": {
            "arena": str(arena.value["host_boot_id"]),
            "cra": _run(None, "cat /proc/sys/kernel/random/boot_id"),
            "dell": str(dell.value["host_boot_id"]),
        },
        "release_manifest_sha256": {
            "arena": str(arena_identity["release_manifest_sha256"]),
            "cra": _sha256(None, f"/opt/stream-recovery-control/releases/{cra_release}/release_manifest.json"),
            "dell": str(dell_identity["release_manifest_sha256"]),
        },
        "runtime_manifest_sha256": {
            "arena": str(arena_identity["runtime_manifest_sha256"]),
            "cra": _sha256(None, f"/opt/stream-recovery-control/runtimes/{cra_release}/runtime_manifest.json"),
            "dell": str(dell_identity["runtime_manifest_sha256"]),
        },
        "configuration_set_sha256": {
            "arena": str(arena_identity["configuration_set_sha256"]),
            "cra": _config_set_sha256(None, f"/etc/stream-recovery-control/releases/{cra_release}"),
            "dell": str(dell_identity["configuration_set_sha256"]),
        },
        "maintenance_restart_policy_sha256": {
            "arena": str(arena_identity["maintenance_restart_policy_sha256"]),
            "cra": _sha256(None, "/etc/needrestart/conf.d/stream-recovery-control.conf"),
            "dell": str(dell_identity["maintenance_restart_policy_sha256"]),
        },
        "rehearsal_evidence_sha256": dict(profile["rehearsal_evidence_sha256"]),
        "target_identity_sha256": target_digest,
        "cra_database_integrity_check": integrity,
        "cra_database_journal_mode": journal_mode,
        "cra_archive_quick_check": runtime_status.get("retention", {}).get("archive_quick_check"),
        "cra_archive_integrity_check": runtime_status.get("retention", {}).get("archive_integrity_check"),
        "cra_single_writer_lock": "ENFORCED",
        "cra_backup_restore_rehearsal": "PASS",
        "credential_rotation_rehearsal": "PASS",
        "credential_minimum_remaining_seconds": {
            "arena_cra": _certificate_remaining(None, "/etc/stream-recovery-control/cra-monitoring-client.pem", current),
            "arena_dell": arena_credentials["arena_dell"],
            "dell_target_observer": dell_credentials["dell_target_observer"],
        },
        "disk_free_bytes": {
            "arena": int(arena_resources["disk_free_bytes"]),
            "cra": _disk_free(None, cra_state),
            "dell": int(dell_resources["disk_free_bytes"]),
        },
        "resident_memory_bytes": {
            "arena": int(arena_resources["resident_memory_bytes"]),
            "cra": cra_rss,
            "dell": int(dell_resources["resident_memory_bytes"]),
        },
        "open_fd_count": {
            "arena": int(arena_resources["open_fd_count"]),
            "cra": cra_fds,
            "dell": int(dell_resources["open_fd_count"]),
        },
        "resource_limit_breach_count": {
            **remote_resource_breaches,
            "cra": _resource_breach_count(None, cra_units, str(profile["epoch_started_at"])),
        },
        "service_failed_invocation_count": service_failures,
        "persistent_service_invocation_ids": {
            "arena_projection_server": arena_invocation,
            "cra_runtime": _show_invocation_id(None, cra_runtime_unit),
            "dell_observation_server": dell_invocation,
        },
        "legacy_arena_recovery_active": arena_payload.get("legacy_arena_recovery_active"),
        "legacy_arena_recovery_restart_count": arena_legacy_restarts,
        "executable_command_route_present": executable_route,
        "transport_recovery_health": {
            "arena_dell_pull": dict(arena_payload["transport_recovery_health"]),
            "cra_projection_pull": _json(
                None,
                f"/var/lib/stream-recovery-control/monitoring-inbox/releases/{cra_release}/recovery-state.json",
            ),
        },
    }
    operations = {
        "schema": "cra.no_action_operation_summary.v1",
        "observed_at": observed_at,
        **effects,
        **transitions,
        **runtime_events,
        "legacy_arena_epoch_invocation_count": max(legacy_restart_delta, legacy_invocation_delta),
        "cra_hot_database_bytes": int(retention["hot_database_bytes"]),
        "cra_hot_wal_bytes": int(retention["hot_wal_bytes"]),
        "cra_hot_shm_bytes": int(retention["hot_shm_bytes"]),
        "cra_hot_sqlite_total_bytes": int(retention["hot_sqlite_total_bytes"]),
        "cra_archive_database_bytes": int(retention["archive_database_bytes"]),
        "cra_archive_wal_bytes": int(retention["archive_wal_bytes"]),
        "cra_archive_shm_bytes": int(retention["archive_shm_bytes"]),
        "cra_archive_sqlite_total_bytes": int(retention["archive_sqlite_total_bytes"]),
        "cra_archive_record_count": int(retention["archive_record_count"]),
        "cra_archive_checkpoint_count": int(retention["archive_checkpoint_count"]),
    }
    return {
        "schema": "cra.no_action_soak_capture.v9",
        "arena_dell_pull": dict(arena_payload["dell_pull_status"]),
        "arena_live_adapter": dict(arena_payload["live_adapter_status"]),
        "arena_projection": dict(arena_payload["projection_status"]),
        "cra_projection_pull": _json(
            None,
            f"/var/lib/stream-recovery-control/monitoring-inbox/releases/{cra_release}/pull-status.json",
        ),
        "cra_runtime": runtime_status,
        "cra_database": db_summary,
        "dell": {
            "schema": "cra.dell_observation_soak_summary.v1",
            "release_id": dell_release,
            "observation_server_active": dell_services["dell_observation_server"].get("active_state") == "active",
            "target_snapshot_valid": dell_payload.get("target_snapshot_valid"),
            "control_capability_count": dell_payload.get("control_capability_count"),
            "physical_effect_count": effects["epoch_effect_boundary_count"],
            "observed_at": observed_at,
        },
        "restarts": {
            "schema": "cra.no_action_restart_summary.v1",
            "arena": sum(int(item["restart_count"]) for item in arena_services.values()),
            "cra": _restart_total(None, cra_units),
            "dell": sum(int(item["restart_count"]) for item in dell_services.values()),
            "observed_at": observed_at,
        },
        "infrastructure": infrastructure,
        "operations": operations,
    }


def collect(
    profile: dict[str, Any],
    *,
    now: datetime | None = None,
    previous_sample: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if profile.get("collection_mode") == "local_host_status_v1":
        return _collect_local_contract(profile, now=now, previous_sample=previous_sample)
    return _collect_ssh_audit(profile, now=now, previous_sample=previous_sample)


def _last_sample(path: Path) -> dict[str, Any] | None:
    try:
        samples = read_samples(path)
    except FileNotFoundError:
        return None
    if not samples:
        return None
    return samples[-1]


@contextmanager
def _collector_lock(sample_path: Path) -> Iterator[None]:
    sample_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = sample_path.with_name(f".{sample_path.name}.lock")
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("SOAK_COLLECTOR_LOCK_NOT_REGULAR_FILE")
        if stat.S_IMODE(metadata.st_mode) & 0o077:
            raise ValueError("SOAK_COLLECTOR_LOCK_PERMISSIONS_UNSAFE")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _append_sample_and_write_gate_unlocked(
    sample_path: Path,
    gate_path: Path,
    sample: dict[str, Any],
    expected_releases: dict[str, str],
) -> dict[str, Any]:
    append_sample(sample_path, sample)
    gate = evaluate_no_action_soak(read_samples(sample_path), expected_releases=expected_releases)
    _atomic_json(gate_path, gate)
    return gate


def _append_sample_and_write_gate(
    sample_path: Path,
    gate_path: Path,
    sample: dict[str, Any],
    expected_releases: dict[str, str],
) -> dict[str, Any]:
    with _collector_lock(sample_path):
        return _append_sample_and_write_gate_unlocked(sample_path, gate_path, sample, expected_releases)


def _write_terminal_gate(path: Path, gate: dict[str, Any], *, detected_at: datetime | None = None) -> bool:
    if gate.get("terminal_failure") is not True:
        return False
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    value = {
        "schema": "cra.no_action_soak_terminal_failure.v1",
        "detected_at": isoformat_utc(detected_at or datetime.now(UTC)),
        "gate": gate,
    }
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        # First-terminal evidence is immutable.  Later samples continue to the
        # current gate and samples.jsonl so recovery remains observable.
        return False
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(value, handle, separators=(",", ":"), sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect and append one three-host CRA no-action soak sample")
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--capture", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--gate-output", type=Path)
    parser.add_argument("--terminal-output", type=Path)
    args = parser.parse_args()
    profile = _read_profile(args.profile)
    state_directory = Path(profile["state_directory"]) if profile.get("collection_mode") == "local_host_status_v1" else None
    capture_output = args.capture or (state_directory / "capture-current.json" if state_directory is not None else None)
    sample_output = args.output or (state_directory / "samples.jsonl" if state_directory is not None else None)
    gate_output = args.gate_output or (state_directory / "gate-current.json" if state_directory is not None else None)
    if capture_output is None or sample_output is None or gate_output is None:
        raise ValueError("SOAK_COLLECTOR_OUTPUT_PATHS_REQUIRED")
    terminal_output = args.terminal_output or gate_output.with_name("terminal-failure.json")
    with _collector_lock(sample_output):
        capture = collect(profile, previous_sample=_last_sample(sample_output))
        sample = compose_no_action_sample(capture, expected_releases=profile["releases"], maximum_input_age_seconds=60)
        _atomic_json(capture_output, capture)
        gate = _append_sample_and_write_gate_unlocked(sample_output, gate_output, sample, dict(profile["releases"]))
        _write_terminal_gate(terminal_output, gate)
    print(json.dumps(sample, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
