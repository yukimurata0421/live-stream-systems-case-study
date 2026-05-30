from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LegacyComponent:
    """A migrated /stream execution unit owned by one stream_v2 subsystem."""

    subsystem: str
    name: str
    path: Path
    kind: str
    role: str
    destructive: bool = False
    url_risk: str = "none"
    service_unit: str = ""
    default_command: tuple[str, ...] = ()
    shared_with: tuple[str, ...] = ()
    notes: str = ""

    @property
    def exists(self) -> bool:
        return self.path.exists()

    def to_dict(self) -> dict[str, Any]:
        resolved = ""
        try:
            resolved = str(self.path.resolve())
        except FileNotFoundError:
            resolved = ""
        return {
            "subsystem": self.subsystem,
            "name": self.name,
            "kind": self.kind,
            "role": self.role,
            "path": str(self.path),
            "resolved_path": resolved,
            "exists": self.exists,
            "destructive": self.destructive,
            "url_risk": self.url_risk,
            "service_unit": self.service_unit,
            "default_command": list(self.default_command),
            "shared_with": list(self.shared_with),
            "notes": self.notes,
        }
