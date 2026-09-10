from __future__ import annotations

import fcntl
import hashlib
import json
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


def _utc(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _now_text(now: datetime) -> str:
    return now.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical_sha256(value: Mapping[str, Any]) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _read_object(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return None
    return dict(value) if isinstance(value, Mapping) else None


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


@dataclass(frozen=True)
class ProjectionDecision:
    available: bool
    reason_code: str
    snapshot: Mapping[str, Any] | None
    projection_id: str = ""
    projection_sequence: int = 0

    def audit_snapshot(self) -> Mapping[str, Any] | None:
        if not self.available or self.snapshot is None:
            return None
        return self.snapshot


class ProjectionProjector:
    """Publish a digest-bound envelope from an already-local Dell cache.

    No network or mutation adapter is used.  A producer-owned sequence file is
    advanced before publication, so a crash can create a harmless sequence gap
    but cannot silently reuse a published sequence.
    """

    def __init__(
        self,
        *,
        source_path: Path,
        output_path: Path,
        sequence_path: Path,
        producer_id: str,
        producer_instance_id: str,
        expected_source_producer_id: str,
        ttl_seconds: float = 5.0,
    ) -> None:
        self.source_path = source_path
        self.output_path = output_path
        self.sequence_path = sequence_path
        self.producer_id = producer_id
        self.producer_instance_id = producer_instance_id
        self.expected_source_producer_id = expected_source_producer_id
        self.ttl_seconds = max(0.1, float(ttl_seconds))

    def _next_sequence(self) -> int:
        self.sequence_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self.sequence_path.with_suffix(self.sequence_path.suffix + ".lock")
        with lock_path.open("a+", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            state = _read_object(self.sequence_path) or {}
            sequence = int(state.get("last_reserved_sequence") or 0) + 1
            atomic_write_json(
                self.sequence_path,
                {
                    "schema_version": "maintenance.snapshot_projection_sequence.v1",
                    "producer_id": self.producer_id,
                    "producer_instance_id": self.producer_instance_id,
                    "last_reserved_sequence": sequence,
                },
            )
            return sequence

    def publish(self, *, now: datetime | None = None) -> dict[str, Any]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        snapshot = _read_object(self.source_path)
        if snapshot is None:
            raise ValueError("SOURCE_SNAPSHOT_MISSING_OR_CORRUPT")
        if str(snapshot.get("schema_version") or "") != "maintenance.audit_state_snapshot.v2":
            raise ValueError("SOURCE_SCHEMA_UNSUPPORTED")
        if str(snapshot.get("producer_id") or "") != self.expected_source_producer_id:
            raise ValueError("SOURCE_PRODUCER_MISMATCH")
        if snapshot.get("production_behavior_modified") is not False or int(snapshot.get("physical_effect_count") or 0) != 0:
            raise ValueError("SOURCE_AUDIT_ONLY_BOUNDARY_VIOLATION")
        source_observed = _utc(snapshot.get("observed_at"))
        source_fresh_until = _utc(snapshot.get("fresh_until"))
        if source_observed is None or source_fresh_until is None or source_observed > current or source_fresh_until <= current:
            raise ValueError("SOURCE_SNAPSHOT_STALE_OR_FUTURE")
        sequence = self._next_sequence()
        projection_fresh_until = min(source_fresh_until, current + timedelta(seconds=self.ttl_seconds))
        envelope = {
            "schema_version": "maintenance.snapshot_projection.v1",
            "projection_id": f"maintenance-projection-{uuid.uuid4()}",
            "projection_sequence": sequence,
            "producer_id": self.producer_id,
            "producer_instance_id": self.producer_instance_id,
            "published_at": _now_text(current),
            "fresh_until": _now_text(projection_fresh_until),
            "source_snapshot_id": str(snapshot.get("snapshot_id") or ""),
            "source_producer_id": str(snapshot.get("producer_id") or ""),
            "source_producer_instance_id": str(snapshot.get("producer_instance_id") or ""),
            "source_observed_at": str(snapshot.get("observed_at") or ""),
            "source_fresh_until": str(snapshot.get("fresh_until") or ""),
            "maintenance_generation": int(snapshot.get("maintenance_generation") or 0),
            "target_snapshot_id": str(snapshot.get("target_snapshot_id") or ""),
            "target_snapshot_status": str(snapshot.get("target_snapshot_status") or "MISSING"),
            "payload": snapshot,
            "payload_sha256": canonical_sha256(snapshot),
            "production_behavior_modified": False,
            "physical_effect_count": 0,
        }
        atomic_write_json(self.output_path, envelope)
        os.chmod(self.output_path, 0o640)
        return envelope


class ProjectionReader:
    """Validate only a local projection and preserve replay high water."""

    def __init__(
        self,
        *,
        path: Path,
        expected_producer_id: str,
        expected_source_producer_id: str,
        high_water_path: Path | None = None,
    ) -> None:
        self.path = path
        self.expected_producer_id = expected_producer_id
        self.expected_source_producer_id = expected_source_producer_id
        self.high_water_path = high_water_path

    @staticmethod
    def mount_contract(*, read_only: bool, consumer_has_write_credential: bool) -> ProjectionDecision:
        if not read_only or consumer_has_write_credential:
            return ProjectionDecision(False, "PROJECTION_WRITABLE_BY_CONSUMER", None)
        return ProjectionDecision(True, "READ_ONLY_PROJECTION_BOUNDARY", {})

    def read(
        self,
        *,
        now: datetime | None = None,
        expected_target_identity: Mapping[str, Any] | None = None,
        expected_maintenance_generation: int | None = None,
    ) -> ProjectionDecision:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        envelope = _read_object(self.path)
        if envelope is None:
            return ProjectionDecision(False, "PROJECTION_MISSING_OR_CORRUPT", None)
        projection_id = str(envelope.get("projection_id") or "")
        try:
            sequence = int(envelope.get("projection_sequence") or 0)
        except (TypeError, ValueError):
            sequence = 0

        def reject(reason: str) -> ProjectionDecision:
            return ProjectionDecision(False, reason, None, projection_id, sequence)

        if str(envelope.get("schema_version") or "") != "maintenance.snapshot_projection.v1":
            return reject("PROJECTION_SCHEMA_UNSUPPORTED")
        if not projection_id or sequence <= 0:
            return reject("PROJECTION_IDENTITY_INVALID")
        if str(envelope.get("producer_id") or "") != self.expected_producer_id:
            return reject("PROJECTION_PRODUCER_MISMATCH")
        if str(envelope.get("source_producer_id") or "") != self.expected_source_producer_id:
            return reject("SOURCE_PRODUCER_MISMATCH")
        published_at = _utc(envelope.get("published_at"))
        fresh_until = _utc(envelope.get("fresh_until"))
        if published_at is None or fresh_until is None or published_at > current or fresh_until <= current:
            return reject("PROJECTION_STALE_OR_FUTURE")
        payload = envelope.get("payload")
        if not isinstance(payload, Mapping):
            return reject("PROJECTION_PAYLOAD_MISSING")
        if canonical_sha256(payload) != str(envelope.get("payload_sha256") or ""):
            return reject("PROJECTION_INTEGRITY_FAILURE")
        if str(payload.get("snapshot_id") or "") != str(envelope.get("source_snapshot_id") or ""):
            return reject("SOURCE_SNAPSHOT_ID_MISMATCH")
        if str(payload.get("producer_id") or "") != self.expected_source_producer_id:
            return reject("SOURCE_PAYLOAD_PRODUCER_MISMATCH")
        source_fresh_until = _utc(payload.get("fresh_until"))
        if source_fresh_until is None or source_fresh_until <= current:
            return reject("SOURCE_SNAPSHOT_STALE")
        generation = int(payload.get("maintenance_generation") or 0)
        if generation != int(envelope.get("maintenance_generation") or 0):
            return reject("MAINTENANCE_GENERATION_ENVELOPE_MISMATCH")
        if expected_maintenance_generation is not None and generation != expected_maintenance_generation:
            return reject("MAINTENANCE_GENERATION_MISMATCH")
        if str(payload.get("target_snapshot_status") or "MISSING") != "VALID":
            return reject("TARGET_SNAPSHOT_NOT_VALID")
        target = payload.get("target_identity")
        if not isinstance(target, Mapping):
            return reject("TARGET_IDENTITY_MISSING")
        if expected_target_identity is not None and dict(target) != dict(expected_target_identity):
            return reject("TARGET_IDENTITY_MISMATCH")

        if self.high_water_path is not None:
            high_water = _read_object(self.high_water_path) or {}
            previous_instance = str(high_water.get("producer_instance_id") or "")
            current_instance = str(envelope.get("producer_instance_id") or "")
            previous_sequence = int(high_water.get("projection_sequence") or 0)
            previous_generation = int(high_water.get("maintenance_generation") or 0)
            if previous_instance == current_instance and sequence < previous_sequence:
                return reject("PROJECTION_SEQUENCE_REGRESSION")
            if generation < previous_generation:
                return reject("MAINTENANCE_GENERATION_REGRESSION")
            atomic_write_json(
                self.high_water_path,
                {
                    "schema_version": "maintenance.snapshot_projection_high_water.v1",
                    "producer_id": self.expected_producer_id,
                    "producer_instance_id": current_instance,
                    "projection_sequence": sequence,
                    "maintenance_generation": generation,
                    "projection_id": projection_id,
                },
            )
        return ProjectionDecision(True, "PROJECTION_VALID", dict(payload), projection_id, sequence)
