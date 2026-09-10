from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import re
import signal
import stat
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from cra_authority.authorizer import CraAuthorizer, RecoveryPolicy
from cra_authority.json_input import load_object
from cra_authority.monitoring_evidence import MonitoringEvidenceContract, SignedProjectionFileSource
from cra_authority.no_action_store import NoActionCentralStore
from cra_authority.retention import SignedNoActionArchive, load_archive_private_key
from cra_dell_recovery.canonical import KeyRing
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.time import isoformat_utc, utc_now


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


def _required_string(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _required_path(value: Any, *, field: str) -> Path:
    path = Path(_required_string(value, field=field))
    if not path.is_absolute():
        raise ValueError(f"{field} must be an absolute path")
    return path


def _finite_number(value: Any, *, field: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be a JSON number")
    parsed = float(value)
    if not math.isfinite(parsed) or not minimum <= parsed <= maximum:
        raise ValueError(f"{field} is outside the safe range")
    return parsed


def _bounded_integer(value: Any, *, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be a JSON integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{field} is outside the safe range")
    return int(value)


def _secure_read(path: Path, *, maximum_bytes: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("CRA_RUNTIME_INPUT_NOT_REGULAR_FILE")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise ValueError("CRA_RUNTIME_INPUT_PERMISSIONS_UNSAFE")
        if metadata.st_size > maximum_bytes:
            raise ValueError("CRA_RUNTIME_INPUT_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > maximum_bytes:
        raise ValueError("CRA_RUNTIME_INPUT_TOO_LARGE")
    return raw


def _public_key(path: Path) -> Ed25519PublicKey:
    value = serialization.load_pem_public_key(_secure_read(path, maximum_bytes=64 * 1024))
    if not isinstance(value, Ed25519PublicKey):
        raise ValueError("monitoring verification key must be Ed25519")
    return value


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "ab") as handle:
            descriptor = -1
            handle.write(json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _error_reason_code(error: Exception) -> str:
    raw = str(error).strip()
    if re.fullmatch(r"[A-Z][A-Z0-9_:.-]{0,159}", raw):
        detail = raw
    else:
        detail = re.sub(r"[^A-Z0-9]+", "_", raw.upper()).strip("_")[:120] or "NO_DETAIL"
    error_class = re.sub(r"[^A-Z0-9]+", "_", type(error).__name__.upper()).strip("_")
    return f"{error_class}:{detail}"[:200]


@dataclass(frozen=True)
class RuntimeConfig:
    value: dict[str, Any]
    path: Path

    @classmethod
    def load(cls, path: Path) -> RuntimeConfig:
        raw = load_object(_secure_read(path, maximum_bytes=64 * 1024), maximum_bytes=64 * 1024)
        if not isinstance(raw, dict):
            raise ValueError("CRA config must be a JSON object")
        required = {
            "schema",
            "operating_mode",
            "runtime_release_id",
            "database",
            "migration",
            "target_id",
            "lock_file",
            "status_file",
            "cycle_interval_seconds",
            "monitoring",
            "policy",
        }
        optional_v2 = {"event_file", "heartbeat_interval_seconds", "retention"}
        if frozenset(raw) not in {frozenset(required), frozenset(required | optional_v2)}:
            raise ValueError(f"CRA config fields must be exact: {sorted(required)}")
        if raw["schema"] not in {"cra.runtime.v1", "cra.runtime.v2"}:
            raise ValueError("unsupported CRA runtime config schema")
        if raw["schema"] == "cra.runtime.v1" and set(raw) != required:
            raise ValueError("cra.runtime.v1 does not accept v2 fields")
        if raw["schema"] == "cra.runtime.v2" and set(raw) != required | optional_v2:
            raise ValueError("cra.runtime.v2 fields are not exact")
        if raw["operating_mode"] != "NO_ACTION":
            raise ValueError("this release is fail-closed to NO_ACTION")
        _required_string(raw["runtime_release_id"], field="runtime_release_id")
        require_runtime_release(raw["runtime_release_id"], "CRA_RUNTIME_RELEASE_MISMATCH")
        path_fields = ["database", "migration", "lock_file", "status_file"]
        if raw["schema"] == "cra.runtime.v2":
            path_fields.append("event_file")
        paths = {field: _required_path(raw[field], field=field) for field in path_fields}
        _required_string(raw["target_id"], field="target_id")
        for state_field in ("database", "lock_file"):
            if "releases" in paths[state_field].parts:
                raise ValueError(f"CRA_RELEASE_SCOPED_STATE_FORBIDDEN:{state_field}")
        _finite_number(raw["cycle_interval_seconds"], field="cycle_interval_seconds", minimum=0.1, maximum=300)
        if raw["schema"] == "cra.runtime.v2":
            _finite_number(raw["heartbeat_interval_seconds"], field="heartbeat_interval_seconds", minimum=10, maximum=3600)
            retention = raw["retention"]
            if not isinstance(retention, dict) or set(retention) != {
                "archive_database",
                "archive_signing_private_key",
                "archive_key_id",
                "retain_decision_count",
                "compact_interval_seconds",
            }:
                raise ValueError("retention config fields are not exact")
            _required_path(retention["archive_database"], field="retention.archive_database")
            _required_path(retention["archive_signing_private_key"], field="retention.archive_signing_private_key")
            _required_string(retention["archive_key_id"], field="retention.archive_key_id")
            _bounded_integer(
                retention["retain_decision_count"],
                field="retention.retain_decision_count",
                minimum=2,
                maximum=10_000_000,
            )
            _finite_number(
                retention["compact_interval_seconds"],
                field="retention.compact_interval_seconds",
                minimum=10,
                maximum=86400,
            )
        monitoring = raw["monitoring"]
        if not isinstance(monitoring, dict) or set(monitoring) != {
            "projection_file",
            "schema_file",
            "source_instance_id",
            "source_release_id",
            "key_id",
            "public_key_file",
            "maximum_ttl_seconds",
            "maximum_check_age_seconds",
        }:
            raise ValueError("monitoring config fields are not exact")
        for field in ("projection_file", "schema_file", "public_key_file"):
            _required_path(monitoring[field], field=f"monitoring.{field}")
        for field in ("source_instance_id", "source_release_id", "key_id"):
            _required_string(monitoring[field], field=f"monitoring.{field}")
        _finite_number(monitoring["maximum_ttl_seconds"], field="monitoring.maximum_ttl_seconds", minimum=1, maximum=60)
        _finite_number(
            monitoring["maximum_check_age_seconds"],
            field="monitoring.maximum_check_age_seconds",
            minimum=1,
            maximum=300,
        )
        policy = raw["policy"]
        if not isinstance(policy, dict) or set(policy) != {
            "revision",
            "authorization_lifetime_seconds",
            "minimum_action_interval_seconds",
            "hourly_action_limit",
            "daily_action_limit",
        }:
            raise ValueError("policy config fields are not exact")
        _required_string(policy["revision"], field="policy.revision")
        _bounded_integer(
            policy["authorization_lifetime_seconds"],
            field="policy.authorization_lifetime_seconds",
            minimum=1,
            maximum=300,
        )
        _bounded_integer(
            policy["minimum_action_interval_seconds"],
            field="policy.minimum_action_interval_seconds",
            minimum=1,
            maximum=604800,
        )
        hourly = _bounded_integer(policy["hourly_action_limit"], field="policy.hourly_action_limit", minimum=1, maximum=10000)
        daily = _bounded_integer(policy["daily_action_limit"], field="policy.daily_action_limit", minimum=1, maximum=100000)
        if daily < hourly:
            raise ValueError("policy.daily_action_limit must be greater than or equal to hourly_action_limit")
        return cls(raw, path)


class SingletonLock:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            os.close(descriptor)
            raise ValueError("CRA_SINGLE_WRITER_LOCK_NOT_REGULAR")
        os.fchmod(descriptor, 0o600)
        self._handle = os.fdopen(descriptor, "a+", encoding="utf-8")

    def __enter__(self) -> SingletonLock:
        locked = False
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
            self._handle.seek(0)
            self._handle.truncate()
            self._handle.write(f"{os.getpid()}\n")
            self._handle.flush()
            os.fsync(self._handle.fileno())
            return self
        except BaseException as exc:
            if locked:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            if isinstance(exc, BlockingIOError):
                raise RuntimeError("CRA_SINGLE_WRITER_ALREADY_RUNNING") from exc
            raise

    def __exit__(self, *_: object) -> None:
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()


class CraNoActionRuntime:
    def __init__(self, config: RuntimeConfig) -> None:
        value = config.value
        self.config = config
        self.store = NoActionCentralStore(Path(value["database"]), Path(value["migration"]))
        target = self.store.read_one(
            "SELECT target_id,authority_state FROM targets WHERE target_id=?",
            (str(value["target_id"]),),
        )
        if target is None:
            raise ValueError("CRA target is not provisioned; run cra-provision first")
        self.authority_state = str(target["authority_state"])
        monitoring = dict(value["monitoring"])
        contract = MonitoringEvidenceContract(
            Path(monitoring["schema_file"]),
            KeyRing({str(monitoring["key_id"]): _public_key(Path(monitoring["public_key_file"]))}),
            allowed_sources={str(monitoring["source_instance_id"]): str(monitoring["source_release_id"])},
            maximum_ttl_seconds=float(monitoring["maximum_ttl_seconds"]),
            maximum_check_age_seconds=float(monitoring["maximum_check_age_seconds"]),
        )
        self.source = SignedProjectionFileSource(Path(monitoring["projection_file"]), contract)
        policy = dict(value["policy"])
        self.authorizer = CraAuthorizer(
            self.store,
            RecoveryPolicy(
                revision=str(policy["revision"]),
                authorization_lifetime_seconds=int(policy["authorization_lifetime_seconds"]),
                minimum_action_interval_seconds=int(policy["minimum_action_interval_seconds"]),
                hourly_action_limit=int(policy["hourly_action_limit"]),
                daily_action_limit=int(policy["daily_action_limit"]),
                materialize_authorization=False,
            ),
        )
        self.archive: SignedNoActionArchive | None = None
        self._last_compaction = 0.0
        self._last_archive_integrity_check = 0.0
        self._last_event_at = 0.0
        self._last_event_fingerprint = ""
        self._event_sequence = 0
        if value["schema"] == "cra.runtime.v2":
            retention = dict(value["retention"])
            self.archive = SignedNoActionArchive(
                Path(retention["archive_database"]),
                key_id=str(retention["archive_key_id"]),
                private_key=load_archive_private_key(Path(retention["archive_signing_private_key"])),
            )

    def close(self) -> None:
        if self.archive is not None:
            self.archive.close()
        self.store.close()

    def run_once(self) -> dict[str, Any]:
        base: dict[str, Any] = {
            "schema": "cra.runtime_status.v1",
            "component": "cra-authority",
            "operating_mode": "NO_ACTION",
            "runtime_release_id": self.config.value["runtime_release_id"],
            "target_id": self.config.value["target_id"],
            "authority_state": self.authority_state,
            "reconciliation_required": self.authority_state != "CENTRAL_ACTIVE",
            "observed_at": isoformat_utc(utc_now()),
            "sqlite_version": self.store.status.version,
            "sqlite_production_gate": self.store.status.production_gate,
            "control_capability_count": 0,
            "physical_effect_count": 0,
            "command_delivery_enabled": False,
        }
        retention_status = self._retention_status()
        if retention_status is not None:
            base["retention"] = retention_status
        try:
            projection = self.source.latest()
            high = self.store.read_one(
                "SELECT max(observation_sequence) FROM monitoring_evidence_projections WHERE source_instance_id=?",
                (str(projection.value["source_instance_id"]),),
            )
            # An old exact duplicate remains valid historical evidence, but it
            # must not replace the latest current decision after inbox rollback.
            if high is not None and high[0] is not None and projection.sequence < int(high[0]):
                raise ValueError("MONITORING_PROJECTION_SEQUENCE_REGRESSION")
            decision = self.authorizer.evaluate(projection)
            # SQLite waits and historical-decision replay do not extend source
            # validity. Recheck before presenting a decision as current.
            completed_at = utc_now()
            self.source.contract.decode(projection.value, now=completed_at)
            base["observed_at"] = isoformat_utc(completed_at)
            would_authorize = decision.blockers == ("CRA_OPERATING_MODE_NO_ACTION",)
            status = {
                **base,
                "readiness": "NO_ACTION_READY" if self.store.status.production_gate == "PASS" else "SAFE_BLOCKED_SQLITE_VERSION",
                "projection_id": projection.projection_id,
                "projection_sequence": projection.sequence,
                "policy_decision": "WOULD_AUTHORIZE" if would_authorize else decision.decision,
                "policy_blockers": list(decision.blockers),
                "policy_action": decision.action,
                "policy_candidate_reason_code": decision.candidate_reason_code,
                "policy_decision_reason_code": decision.decision_reason_code,
                "policy_decision_digest": decision.decision_digest,
                "policy_reason_binding": decision.reason_binding,
                "policy_reason_code": decision.decision_reason_code,
                "monitoring_readiness": projection.readiness,
                "incident_state": str(projection.incident["state"]),
            }
        except Exception as exc:
            reason_code = _error_reason_code(exc)
            base["observed_at"] = isoformat_utc(utc_now())
            status = {
                **base,
                "readiness": "SAFE_BLOCKED",
                "error_class": type(exc).__name__,
                "error_reason_code": reason_code,
                "policy_decision": "NO_ACTION",
                "policy_blockers": ["MONITORING_EVIDENCE_UNAVAILABLE_OR_INVALID", reason_code],
                "policy_action": None,
                "policy_candidate_reason_code": "NO_CONFIRMED_RECOVERY_CANDIDATE",
                "policy_decision_reason_code": reason_code,
                "policy_decision_digest": "",
                "policy_reason_binding": "UNAVAILABLE",
                "policy_reason_code": reason_code,
                "monitoring_readiness": {
                    "input_fresh": False,
                    "parity_clean": False,
                    "projection_clean": False,
                    "source_ready": False,
                },
                "incident_state": "UNKNOWN",
            }
        status["process_cpu_seconds"] = round(time.process_time(), 6)
        _atomic_json(Path(self.config.value["status_file"]), status)
        self._emit_event(status)
        return status

    def _retention_status(self) -> dict[str, Any] | None:
        # Maintenance can block on SQLite/filesystem work. Finish it before
        # acquiring the projection whose freshness the status will assert.
        if self.archive is not None:
            retention = dict(self.config.value["retention"])
            current_monotonic = time.monotonic()
            if current_monotonic - self._last_compaction >= float(retention["compact_interval_seconds"]):
                self.store.compact_no_action_evidence(
                    self.archive,
                    retain_decision_count=int(retention["retain_decision_count"]),
                )
                self._last_compaction = current_monotonic
            full_integrity = current_monotonic - self._last_archive_integrity_check >= 3600
            archive_status = self.archive.status(full_integrity=full_integrity)
            if archive_status["archive_integrity_check_mode"] == "FULL":
                self._last_archive_integrity_check = current_monotonic
            hot_database = Path(self.config.value["database"])
            hot_database_bytes = _file_size(hot_database)
            hot_wal_bytes = _file_size(hot_database.with_name(f"{hot_database.name}-wal"))
            hot_shm_bytes = _file_size(hot_database.with_name(f"{hot_database.name}-shm"))
            return {
                **archive_status,
                "hot_database_bytes": hot_database_bytes,
                "hot_wal_bytes": hot_wal_bytes,
                "hot_shm_bytes": hot_shm_bytes,
                "hot_sqlite_total_bytes": hot_database_bytes + hot_wal_bytes + hot_shm_bytes,
            }
        return None

    def _emit_event(self, status: dict[str, Any]) -> None:
        if self.config.value["schema"] == "cra.runtime.v1":
            print(json.dumps(status, separators=(",", ":"), sort_keys=True), flush=True)
            return
        state = {
            "readiness": status.get("readiness"),
            "policy_decision": status.get("policy_decision"),
            "policy_blockers": status.get("policy_blockers"),
            "projection_id": status.get("projection_id"),
            "error_reason_code": status.get("error_reason_code"),
        }
        fingerprint = json.dumps(state, separators=(",", ":"), sort_keys=True)
        current = time.monotonic()
        heartbeat = float(self.config.value["heartbeat_interval_seconds"])
        if fingerprint == self._last_event_fingerprint and current - self._last_event_at < heartbeat:
            return
        self._event_sequence += 1
        event = {
            "schema": "cra.runtime_event.v1",
            "sequence": self._event_sequence,
            "event_type": "STATE_CHANGED" if fingerprint != self._last_event_fingerprint else "HEARTBEAT",
            "observed_at": status["observed_at"],
            **state,
        }
        _append_jsonl(Path(self.config.value["event_file"]), event)
        print(json.dumps(event, separators=(",", ":"), sort_keys=True), flush=True)
        self._last_event_fingerprint = fingerprint
        self._last_event_at = current


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="CRA responsibility-split no-action runtime")
    value.add_argument("--config", type=Path, required=True)
    value.add_argument("--once", action="store_true")
    value.add_argument("--check-config", action="store_true")
    return value


def main() -> None:
    args = parser().parse_args()
    config = RuntimeConfig.load(args.config)
    if args.check_config:
        print(json.dumps({"config": "VALID", "operating_mode": config.value["operating_mode"]}, sort_keys=True))
        return
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    with SingletonLock(Path(config.value["lock_file"])):
        runtime = CraNoActionRuntime(config)
        try:
            while True:
                runtime.run_once()
                if args.once:
                    return
                stopped.wait(float(config.value["cycle_interval_seconds"]))
                if stopped.is_set():
                    return
        finally:
            runtime.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"readiness": "SAFE_BLOCKED", "error_class": type(error).__name__}, sort_keys=True), file=sys.stderr)
        raise
