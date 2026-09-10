from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .model import ProtocolError, parse_utc, validate_runtime_identity, validate_target


@dataclass(frozen=True)
class TargetSnapshotDecision:
    available: bool
    reason: str
    snapshot_id: str
    target_identity: dict[str, Any] | None
    observed_at: str
    valid_until: str


class TargetSnapshotReader:
    """Read exact target truth independently from maintenance evidence."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def read(self, *, now: datetime | None = None) -> TargetSnapshotDecision:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return self._unavailable("TARGET_SNAPSHOT_MISSING")
        except (OSError, json.JSONDecodeError):
            return self._unavailable("TARGET_SNAPSHOT_UNREADABLE")
        if not isinstance(value, dict) or str(value.get("schema") or "") != "cra_dell_recovery.target_snapshot.v1":
            return self._unavailable("TARGET_SNAPSHOT_SCHEMA_INVALID")
        if str(value.get("status") or "") != "VALID":
            return self._unavailable(str(value.get("reason_code") or "TARGET_SNAPSHOT_NOT_VALID"))
        try:
            observed_at = str(value.get("observed_at") or "")
            valid_until = str(value.get("valid_until") or "")
            observed = parse_utc(observed_at)
            expiry = parse_utc(valid_until)
            target = validate_target(value.get("target_identity"))
        except ProtocolError as exc:
            return self._unavailable(str(exc))
        if observed > current:
            return self._unavailable("TARGET_SNAPSHOT_FROM_FUTURE")
        if expiry <= current:
            return self._unavailable("TARGET_SNAPSHOT_STALE")
        snapshot_id = str(value.get("snapshot_id") or "").strip()
        if not snapshot_id:
            return self._unavailable("TARGET_SNAPSHOT_ID_MISSING")
        return TargetSnapshotDecision(True, "TARGET_SNAPSHOT_VALID", snapshot_id, target, observed_at, valid_until)

    @staticmethod
    def _unavailable(reason: str) -> TargetSnapshotDecision:
        return TargetSnapshotDecision(False, reason, "", None, "", "")


@dataclass(frozen=True)
class RuntimeSnapshotDecision:
    available: bool
    reason: str
    snapshot_id: str
    runtime_identity: dict[str, str] | None
    observed_at: str
    valid_until: str
    container_ready: bool | None


class RuntimeSnapshotReader:
    """Read owner-runtime identity even when no FFmpeg child exists."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def read(self, *, now: datetime | None = None) -> RuntimeSnapshotDecision:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return self._unavailable("RUNTIME_SNAPSHOT_MISSING")
        except (OSError, json.JSONDecodeError):
            return self._unavailable("RUNTIME_SNAPSHOT_UNREADABLE")
        if not isinstance(value, dict) or str(value.get("schema") or "") != "cra_dell_recovery.target_snapshot.v1":
            return self._unavailable("RUNTIME_SNAPSHOT_SCHEMA_INVALID")
        if str(value.get("runtime_status") or "") != "VALID":
            return self._unavailable(str(value.get("runtime_reason_code") or "RUNTIME_SNAPSHOT_NOT_VALID"))
        try:
            observed_at = str(value.get("observed_at") or "")
            valid_until = str(value.get("valid_until") or "")
            observed = parse_utc(observed_at)
            expiry = parse_utc(valid_until)
            identity = validate_runtime_identity(value.get("runtime_identity"))
        except ProtocolError as exc:
            return self._unavailable(str(exc))
        if observed > current:
            return self._unavailable("RUNTIME_SNAPSHOT_FROM_FUTURE")
        if expiry <= current:
            return self._unavailable("RUNTIME_SNAPSHOT_STALE")
        snapshot_id = str(value.get("runtime_snapshot_id") or value.get("snapshot_id") or "").strip()
        if not snapshot_id:
            return self._unavailable("RUNTIME_SNAPSHOT_ID_MISSING")
        ready = value.get("runtime_container_ready")
        return RuntimeSnapshotDecision(
            True,
            "RUNTIME_SNAPSHOT_VALID",
            snapshot_id,
            identity,
            observed_at,
            valid_until,
            ready if isinstance(ready, bool) else None,
        )

    @staticmethod
    def _unavailable(reason: str) -> RuntimeSnapshotDecision:
        return RuntimeSnapshotDecision(False, reason, "", None, "", "", None)
