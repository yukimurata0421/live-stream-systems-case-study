from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import stat
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from jsonschema import Draft202012Validator, FormatChecker

from cra_authority.monitoring_evidence import reject_control_semantics
from cra_dell_recovery.canonical import Signer, canonical_json
from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.release_identity import require_runtime_release
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now

CONFIG_SCHEMA = "monitoring_v4.cra_projection_producer_config.v1"
FACT_SCHEMA = "monitoring_v4.cra_fact_bundle.v1"
PROJECTION_SCHEMA = "monitoring_v4.evidence_projection.v1"
TARGET_SNAPSHOT_SCHEMA = "cra_dell_recovery.target_snapshot.v1"
SAFE_ERROR = re.compile(r"^[A-Z][A-Z0-9_:.-]{0,199}$")


def _read_json(path: Path, *, maximum_bytes: int) -> dict[str, Any]:
    raw = _read_regular_bytes(path, maximum_bytes=maximum_bytes, secret=False)
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("PROJECTION_INPUT_NOT_OBJECT")
    return dict(value)


def _read_regular_bytes(path: Path, *, maximum_bytes: int, secret: bool) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("PROJECTION_INPUT_NOT_REGULAR_FILE")
        if stat.S_IMODE(metadata.st_mode) & (0o077 if secret else 0o022):
            raise ValueError("PROJECTION_INPUT_WRITABLE_BY_UNTRUSTED_PRINCIPAL")
        if metadata.st_size > maximum_bytes:
            raise ValueError("PROJECTION_INPUT_TOO_LARGE")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            raw = handle.read(maximum_bytes + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > maximum_bytes:
        raise ValueError("PROJECTION_INPUT_TOO_LARGE")
    return raw


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


def _validator(path: Path) -> Draft202012Validator:
    return Draft202012Validator(
        json.loads(path.read_text(encoding="utf-8")),
        format_checker=FormatChecker(),
    )


def _private_key(path: Path) -> Ed25519PrivateKey:
    try:
        raw = _read_regular_bytes(path, maximum_bytes=64 * 1024, secret=True)
    except ValueError as error:
        if str(error) == "PROJECTION_INPUT_WRITABLE_BY_UNTRUSTED_PRINCIPAL":
            raise ValueError("MONITORING_SIGNING_KEY_PERMISSIONS_UNSAFE") from error
        raise
    value = serialization.load_pem_private_key(raw, password=None)
    if not isinstance(value, Ed25519PrivateKey):
        raise ValueError("MONITORING_SIGNING_KEY_NOT_ED25519")
    return value


@dataclass(frozen=True)
class ProjectionProducerConfig:
    value: dict[str, Any]
    path: Path

    @classmethod
    def load(cls, path: Path) -> ProjectionProducerConfig:
        value = _read_json(path, maximum_bytes=64 * 1024)
        required = {
            "schema",
            "producer_release_id",
            "source_instance_id",
            "source_release_id",
            "target_id",
            "expected_target",
            "fact_bundle_file",
            "target_snapshot_file",
            "fact_bundle_schema_file",
            "target_snapshot_schema_file",
            "projection_schema_file",
            "private_key_file",
            "key_id",
            "output_file",
            "status_file",
            "state_database",
            "projection_ttl_seconds",
            "maximum_fact_age_seconds",
            "maximum_target_age_seconds",
            "maximum_observation_skew_seconds",
        }
        if set(value) != required:
            raise ValueError("PROJECTION_CONFIG_FIELDS_NOT_EXACT")
        if value["schema"] != CONFIG_SCHEMA:
            raise ValueError("PROJECTION_CONFIG_SCHEMA_UNSUPPORTED")
        for name in (
            "producer_release_id",
            "source_instance_id",
            "source_release_id",
            "target_id",
            "key_id",
        ):
            if not str(value[name]).strip():
                raise ValueError(f"PROJECTION_CONFIG_{name.upper()}_MISSING")
        require_runtime_release(value["producer_release_id"], "PROJECTION_RUNTIME_RELEASE_MISMATCH")
        require_runtime_release(value["source_release_id"], "PROJECTION_SOURCE_RELEASE_MISMATCH")
        expected = value["expected_target"]
        if not isinstance(expected, dict) or set(expected) != {"host_id", "namespace", "container_name"}:
            raise ValueError("PROJECTION_EXPECTED_TARGET_FIELDS_NOT_EXACT")
        if expected["container_name"] != "stream-engine" or not expected["host_id"] or not expected["namespace"]:
            raise ValueError("PROJECTION_EXPECTED_TARGET_INVALID")
        for name, lower, upper in (
            ("projection_ttl_seconds", 1.0, 60.0),
            ("maximum_fact_age_seconds", 1.0, 300.0),
            ("maximum_target_age_seconds", 1.0, 60.0),
            ("maximum_observation_skew_seconds", 0.0, 300.0),
        ):
            if not lower <= float(value[name]) <= upper:
                raise ValueError(f"PROJECTION_CONFIG_{name.upper()}_OUT_OF_RANGE")
        return cls(value, path)


class ProjectionSequenceStore:
    """Durable monotonic source sequence and exact replay store."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.connection = sqlite3.connect(path, isolation_level=None, timeout=5.0)
        if str(self.connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower() != "wal":
            raise RuntimeError("PROJECTION_STATE_WAL_UNAVAILABLE")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute("PRAGMA trusted_schema=OFF")
        self.connection.execute(
            """CREATE TABLE IF NOT EXISTS projection_sequence_state(
                   source_instance_id TEXT PRIMARY KEY,
                   observation_sequence INTEGER NOT NULL CHECK(observation_sequence > 0),
                   input_sha256 TEXT NOT NULL,
                   projection_id TEXT NOT NULL,
                   projection_json TEXT NOT NULL CHECK(json_valid(projection_json)),
                   recorded_at TEXT NOT NULL
               ) STRICT"""
        )
        os.chmod(path, 0o600)
        if str(self.connection.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
            raise RuntimeError("PROJECTION_STATE_INTEGRITY_CHECK_FAILED")

    def record_or_replay(
        self,
        *,
        source_instance_id: str,
        sequence: int,
        input_sha256: str,
        projection: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                """SELECT observation_sequence,input_sha256,projection_json
                   FROM projection_sequence_state WHERE source_instance_id=?""",
                (source_instance_id,),
            ).fetchone()
            if row is not None:
                previous_sequence = int(row[0])
                if sequence < previous_sequence:
                    raise ValueError("MONITORING_FACT_SEQUENCE_REGRESSION")
                if sequence == previous_sequence:
                    if str(row[1]) != input_sha256:
                        raise ValueError("MONITORING_FACT_SEQUENCE_CONFLICT")
                    saved = json.loads(str(row[2]))
                    if not isinstance(saved, dict):
                        raise RuntimeError("PROJECTION_STATE_REPLAY_NOT_OBJECT")
                    self.connection.execute("COMMIT")
                    return dict(saved), True
            encoded = json.dumps(projection, separators=(",", ":"), sort_keys=True)
            self.connection.execute(
                """INSERT INTO projection_sequence_state VALUES (?,?,?,?,?,?)
                   ON CONFLICT(source_instance_id) DO UPDATE SET
                     observation_sequence=excluded.observation_sequence,
                     input_sha256=excluded.input_sha256,
                     projection_id=excluded.projection_id,
                     projection_json=excluded.projection_json,
                     recorded_at=excluded.recorded_at""",
                (
                    source_instance_id,
                    sequence,
                    input_sha256,
                    projection["projection_id"],
                    encoded,
                    isoformat_utc(utc_now()),
                ),
            )
            self.connection.execute("COMMIT")
            return projection, False
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def close(self) -> None:
        self.connection.close()


class MonitoringProjectionProducer:
    """Signs facts and a fresh read-only Dell identity; it owns no control API."""

    def __init__(self, config: ProjectionProducerConfig) -> None:
        self.config = config
        value = config.value
        self.fact_validator = _validator(Path(value["fact_bundle_schema_file"]))
        self.target_validator = _validator(Path(value["target_snapshot_schema_file"]))
        self.projection_validator = _validator(Path(value["projection_schema_file"]))
        self.signer = Signer(str(value["key_id"]), _private_key(Path(value["private_key_file"])))
        self.state = ProjectionSequenceStore(Path(value["state_database"]))

    def close(self) -> None:
        self.state.close()

    def produce(self, *, now: datetime | None = None) -> dict[str, Any]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        value = self.config.value
        facts = _read_json(Path(value["fact_bundle_file"]), maximum_bytes=2 * 1024 * 1024)
        target_snapshot = _read_json(Path(value["target_snapshot_file"]), maximum_bytes=256 * 1024)
        self.fact_validator.validate(facts)
        self.target_validator.validate(target_snapshot)
        reject_control_semantics(facts)
        self._validate_facts(facts, current)
        target = self._validate_target_snapshot(target_snapshot, current)
        target_reference = f"dell/target/{target_snapshot['snapshot_id']}"
        if target_reference not in facts["evidence_refs"]:
            raise ValueError("MONITORING_FACT_TARGET_SNAPSHOT_REFERENCE_MISMATCH")

        fact_observed = parse_utc(str(facts["observed_at"]))
        target_observed = parse_utc(str(target_snapshot["observed_at"]))
        skew = abs((fact_observed - target_observed).total_seconds())
        if skew > float(value["maximum_observation_skew_seconds"]):
            raise ValueError("MONITORING_TARGET_OBSERVATION_SKEW_TOO_LARGE")
        issued = current
        expires = min(
            current + timedelta(seconds=float(value["projection_ttl_seconds"])),
            parse_utc(str(facts["valid_until"])),
            parse_utc(str(target_snapshot["valid_until"])),
        )
        if expires <= issued:
            raise ValueError("MONITORING_PROJECTION_HAS_NO_VALIDITY_WINDOW")

        input_digest = hashlib.sha256(
            canonical_json(
                {
                    "facts": facts,
                    "target_snapshot_id": target_snapshot["snapshot_id"],
                    "target_identity": target.to_dict(),
                }
            )
        ).hexdigest()
        projection_id = f"projection-{input_digest[:40]}"
        references = sorted(
            {
                *(str(item) for item in facts["evidence_refs"]),
                target_reference,
            }
        )
        projection: dict[str, Any] = {
            "schema": PROJECTION_SCHEMA,
            "projection_id": projection_id,
            "target_id": facts["target_id"],
            "source_instance_id": facts["source_instance_id"],
            "source_release_id": facts["source_release_id"],
            "monitoring_cycle_id": facts["monitoring_cycle_id"],
            "observation_revision": facts["observation_revision"],
            "observation_sequence": facts["observation_sequence"],
            "observed_at": isoformat_utc(max(fact_observed, target_observed)),
            "issued_at": isoformat_utc(issued),
            "expires_at": isoformat_utc(expires),
            "readiness": facts["readiness"],
            "incident": facts["incident"],
            "observed_target": target.to_dict(),
            "checks": facts["checks"],
            "measurements": facts["measurements"],
            "evidence_refs": references,
            "key_id": self.signer.key_id,
        }
        signed = self.signer.sign(projection)
        self.projection_validator.validate(signed)
        saved, replayed = self.state.record_or_replay(
            source_instance_id=str(facts["source_instance_id"]),
            sequence=int(facts["observation_sequence"]),
            input_sha256=input_digest,
            projection=signed,
        )
        _atomic_json(Path(value["output_file"]), saved)
        status = {
            "schema": "monitoring_v4.cra_projection_producer_status.v1",
            "status": "READY",
            "producer_release_id": value["producer_release_id"],
            "projection_id": saved["projection_id"],
            "observation_sequence": saved["observation_sequence"],
            "replayed": replayed,
            "observed_at": isoformat_utc(current),
            "control_capability_count": 0,
            "physical_effect_count": 0,
        }
        _atomic_json(Path(value["status_file"]), status)
        return saved

    def _validate_facts(self, facts: dict[str, Any], current: datetime) -> None:
        value = self.config.value
        if facts["schema"] != FACT_SCHEMA:
            raise ValueError("MONITORING_FACT_SCHEMA_UNSUPPORTED")
        for name in ("source_instance_id", "source_release_id", "target_id"):
            if str(facts[name]) != str(value[name]):
                raise ValueError(f"MONITORING_FACT_{name.upper()}_MISMATCH")
        observed = parse_utc(str(facts["observed_at"]))
        valid_until = parse_utc(str(facts["valid_until"]))
        if observed > current:
            raise ValueError("MONITORING_FACT_OBSERVED_IN_FUTURE")
        if valid_until <= current:
            raise ValueError("MONITORING_FACT_EXPIRED")
        if (current - observed).total_seconds() > float(value["maximum_fact_age_seconds"]):
            raise ValueError("MONITORING_FACT_TOO_OLD")
        for name, check in dict(facts["checks"]).items():
            if parse_utc(str(dict(check)["observed_at"])) > observed:
                raise ValueError(f"MONITORING_FACT_CHECK_NEWER_THAN_BUNDLE:{name}")

    def _validate_target_snapshot(self, snapshot: dict[str, Any], current: datetime) -> TargetIdentity:
        value = self.config.value
        if snapshot["schema"] != TARGET_SNAPSHOT_SCHEMA or snapshot["status"] != "VALID":
            raise ValueError("DELL_TARGET_SNAPSHOT_NOT_VALID")
        observed = parse_utc(str(snapshot["observed_at"]))
        expires = parse_utc(str(snapshot["valid_until"]))
        if observed > current:
            raise ValueError("DELL_TARGET_SNAPSHOT_OBSERVED_IN_FUTURE")
        if expires <= current:
            raise ValueError("DELL_TARGET_SNAPSHOT_EXPIRED")
        if (current - observed).total_seconds() > float(value["maximum_target_age_seconds"]):
            raise ValueError("DELL_TARGET_SNAPSHOT_TOO_OLD")
        target = TargetIdentity.from_dict(dict(snapshot["target_identity"]))
        expected = dict(value["expected_target"])
        if (
            target.host_id != expected["host_id"]
            or target.namespace != expected["namespace"]
            or target.container_name != expected["container_name"]
        ):
            raise ValueError("DELL_TARGET_SNAPSHOT_IDENTITY_MISMATCH")
        return target


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="Monitoring v4 facts-only CRA projection producer")
    value.add_argument("--config", type=Path, required=True)
    value.add_argument("--check-config", action="store_true")
    return value


def _error_code(error: BaseException) -> str:
    detail = str(error)
    return detail if SAFE_ERROR.fullmatch(detail) else "PROJECTION_INPUT_INVALID"


def main() -> None:
    args = parser().parse_args()
    config = ProjectionProducerConfig.load(args.config)
    if args.check_config:
        _private_key(Path(config.value["private_key_file"]))
        print(json.dumps({"config": "VALID", "schema": config.value["schema"]}, sort_keys=True))
        return
    producer: MonitoringProjectionProducer | None = None
    try:
        producer = MonitoringProjectionProducer(config)
        projection = producer.produce()
        print(json.dumps(projection, separators=(",", ":"), sort_keys=True))
    except Exception as error:
        status = {
            "schema": "monitoring_v4.cra_projection_producer_status.v1",
            "status": "SAFE_BLOCKED",
            "producer_release_id": config.value["producer_release_id"],
            "observed_at": isoformat_utc(utc_now()),
            "error_class": type(error).__name__,
            "error_code": _error_code(error),
            "control_capability_count": 0,
            "physical_effect_count": 0,
        }
        _atomic_json(Path(config.value["status_file"]), status)
        print(json.dumps(status, separators=(",", ":"), sort_keys=True), file=sys.stderr)
        raise
    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    main()
