from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import jsonschema

from maintenance_shadow.snapshot import atomic_write_json


class ShadowMaintenanceSnapshotCache:
    """Atomic audit-state cache. This component cannot execute a mutation."""

    def __init__(self, path: Path, schema_path: Path | None = None) -> None:
        self.path = path
        self._validator: jsonschema.Draft202012Validator | None = None
        if schema_path is not None:
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            self._validator = jsonschema.Draft202012Validator(schema)

    def accept(self, value: Mapping[str, Any]) -> dict[str, Any]:
        if self._validator is not None:
            self._validator.validate(dict(value))
        if str(value.get("schema_version") or "") != "maintenance.audit_state_snapshot.v2":
            raise ValueError("unsupported maintenance snapshot schema")
        if value.get("production_behavior_modified") is not False or int(value.get("physical_effect_count") or 0) != 0:
            raise ValueError("snapshot violates audit-only boundary")
        for field in ("snapshot_id", "producer_id", "producer_instance_id", "observed_at", "fresh_until"):
            if not str(value.get(field) or ""):
                raise ValueError(f"maintenance snapshot missing {field}")
        now = datetime.now(UTC)
        try:
            observed_at = datetime.fromisoformat(str(value["observed_at"]).replace("Z", "+00:00")).astimezone(UTC)
            fresh_until = datetime.fromisoformat(str(value["fresh_until"]).replace("Z", "+00:00")).astimezone(UTC)
        except ValueError as exc:
            raise ValueError("invalid maintenance snapshot time") from exc
        if observed_at > now or fresh_until <= now or fresh_until <= observed_at:
            raise ValueError("maintenance snapshot is stale or future-dated")
        current: Mapping[str, Any] | None = None
        if self.path.exists():
            try:
                decoded = json.loads(self.path.read_text(encoding="utf-8"))
                current = decoded if isinstance(decoded, Mapping) else None
            except (OSError, ValueError):
                current = None
        if current is not None and str(current.get("producer_id") or "") == str(value.get("producer_id") or ""):
            current_observed = datetime.fromisoformat(str(current["observed_at"]).replace("Z", "+00:00")).astimezone(UTC)
            current_generation = int(current.get("maintenance_generation") or 0)
            candidate_generation = int(value.get("maintenance_generation") or 0)
            if candidate_generation < current_generation or observed_at <= current_observed:
                raise ValueError("maintenance snapshot replay or generation regression")
        atomic_write_json(self.path, value)
        return {
            "accepted": True,
            "snapshot_id": str(value["snapshot_id"]),
            "cache_path_configured": True,
            "physical_effect_count": 0,
            "production_behavior_modified": False,
        }
