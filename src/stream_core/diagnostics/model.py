from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class CheckResult:
    name: str
    category: str
    severity: str
    ok: bool
    summary: str
    detail: str = ""
    path: str = ""
    fatal: bool = False
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "category": self.category,
            "severity": self.severity,
            "ok": self.ok,
            "fatal": self.fatal,
            "summary": self.summary,
        }
        if self.detail:
            payload["detail"] = self.detail
        if self.path:
            payload["path"] = self.path
        if self.data:
            payload["data"] = self.data
        return payload


def status_prefix(result: CheckResult) -> str:
    if result.severity == "ok":
        return "ok"
    if result.severity == "info":
        return "info"
    if result.severity == "warn":
        return "warn"
    return "ng"


def payload_from_results(results: list[CheckResult]) -> dict[str, Any]:
    return {
        "ok": all(item.ok for item in results),
        "fatal_count": sum(1 for item in results if item.fatal),
        "warn_count": sum(1 for item in results if item.severity == "warn"),
        "fail_count": sum(1 for item in results if item.severity == "fail"),
        "checks": [item.to_dict() for item in results],
    }
