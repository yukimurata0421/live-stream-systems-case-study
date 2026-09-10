from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from maintenance_audit import target_identity_complete
from maintenance_shadow.store import MaintenanceShadowStore, ShadowState


def _parse_utc(value: object) -> datetime | None:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def read_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
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


def classify_target_snapshot(snapshot: Mapping[str, Any] | None, *, now: datetime | None = None) -> tuple[str, dict[str, Any] | None, str]:
    if not isinstance(snapshot, Mapping):
        return "MISSING", None, "TARGET_SNAPSHOT_MISSING"
    target = snapshot.get("target_identity")
    if not isinstance(target, Mapping) or not target_identity_complete(target):
        return "UNSTABLE", None, "TARGET_IDENTITY_INCOMPLETE"
    observed = _parse_utc(snapshot.get("observed_at"))
    valid_until = _parse_utc(snapshot.get("valid_until"))
    current = now or datetime.now(UTC)
    if observed is None or valid_until is None:
        return "UNSTABLE", dict(target), "TARGET_TIME_PROOF_MISSING"
    if observed > current:
        return "UNSTABLE", dict(target), "TARGET_OBSERVED_IN_FUTURE"
    if current > valid_until:
        return "STALE", dict(target), "TARGET_SNAPSHOT_EXPIRED"
    status = str(snapshot.get("status") or "VALID")
    if status != "VALID":
        return "UNSTABLE", dict(target), f"TARGET_SOURCE_{status}"
    age = max(0.0, (current - observed).total_seconds())
    return "VALID", dict(target), f"TARGET_VALID_AGE_{age:.3f}s"


class ShadowSnapshotProducer:
    def __init__(
        self,
        store: MaintenanceShadowStore,
        *,
        target_snapshot_path: Path | None,
        authority_projection_path: Path | None,
        output_path: Path,
        ttl_seconds: float = 10.0,
    ) -> None:
        self.store = store
        self.target_snapshot_path = target_snapshot_path
        self.authority_projection_path = authority_projection_path
        self.output_path = output_path
        self.ttl_seconds = max(0.1, ttl_seconds)

    def build(self) -> dict[str, Any]:
        now = datetime.now(UTC)
        observed_at = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        fresh_until = (now + timedelta(seconds=self.ttl_seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        state = self.store.state()
        identity = self.store.identity()
        proof = self.store.startup_proof()
        raw_target = read_json(self.target_snapshot_path)
        target_status, source_target, target_reason = classify_target_snapshot(raw_target, now=now)
        authority = read_json(self.authority_projection_path) or {}
        maintenance_state = str(state["maintenance_state"])
        available = bool(state["startup_reconciled"]) and maintenance_state != ShadowState.STARTUP_RECONCILING.value
        if maintenance_state == ShadowState.INACTIVE.value and not proof.positive_inactive_proof:
            available = False
        target_observed = _parse_utc(raw_target.get("observed_at")) if raw_target else None
        target_age = (now - target_observed).total_seconds() if target_observed else None
        snapshot = {
            "schema_version": "maintenance.audit_state_snapshot.v2",
            "available": available,
            "producer_id": str(identity["producer_id"]),
            "producer_instance_id": str(identity["coordinator_instance_id"]),
            "snapshot_id": f"maintenance-snapshot-{uuid.uuid4()}",
            "observed_at": observed_at,
            "observed_at_wall": observed_at,
            "observed_at_monotonic_ns": time.monotonic_ns(),
            "observed_at_monotonic": time.monotonic(),
            "fresh_until": fresh_until,
            "maintenance_state": maintenance_state,
            "maintenance_id": str(state["maintenance_id"] or ""),
            "maintenance_generation": int(state["maintenance_generation"]),
            "authority_epoch": int(authority.get("authority_epoch") or 0),
            "authority_session_id": str(authority.get("authority_session_id") or ""),
            "transaction_state": maintenance_state,
            "authorization_id": "",
            "authorization_state": "NONE",
            "source_target_identity": source_target,
            "target_identity": source_target,
            "target_snapshot_id": str(raw_target.get("snapshot_id") or "") if raw_target else "",
            "target_snapshot_status": target_status,
            "target_snapshot_reason": target_reason,
            "target_snapshot_age_seconds": target_age,
            "reconciliation_state": str(state["reconciliation_state"]),
            "authorizations": self.store.authorization_projections(),
            "proof": proof.to_dict(),
            "physical_effect_count": self.store.physical_effect_count(),
            "production_behavior_modified": False,
        }
        return snapshot

    def publish(self) -> dict[str, Any]:
        snapshot = self.build()
        atomic_write_json(self.output_path, snapshot)
        return snapshot
