from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.time import parse_utc, utc_now


class FileTargetObserver:
    """Read-only shadow adapter; it never signals a process or mutates a workload."""

    def __init__(self, snapshot_path: Path) -> None:
        self.snapshot_path = snapshot_path
        self.last_reason_code = "TARGET_OBSERVATION_UNAVAILABLE"
        self.last_snapshot: dict[str, Any] | None = None

    def observe(self) -> TargetIdentity | None:
        try:
            value = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
            if not isinstance(value, dict):
                raise ValueError("target snapshot must be an object")
            self.last_snapshot = dict(value)
            if value.get("schema") == "cra_dell_recovery.target_snapshot.v1":
                if value.get("status") != "VALID":
                    self.last_reason_code = str(value.get("reason_code") or "TARGET_OBSERVATION_UNAVAILABLE")
                    return None
                if parse_utc(str(value["valid_until"])) <= utc_now():
                    self.last_reason_code = "STALE_TARGET"
                    return None
                value = value["target_identity"]
            self.last_reason_code = "TARGET_OBSERVATION_AVAILABLE"
            return TargetIdentity.from_dict(dict(value))
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            self.last_reason_code = "TARGET_OBSERVATION_UNAVAILABLE"
            return None
