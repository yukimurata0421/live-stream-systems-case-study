from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal

try:
    from .identity import TargetIdentity
except ImportError:
    from identity import TargetIdentity  # type: ignore


class SourceKind(str, Enum):
    DATA_API = "data_api"
    OAUTH = "oauth"
    WATCH_PAGE = "watch_page"
    RESOLVER = "resolver"
    INGEST_LOCAL = "ingest_local"
    API_COST = "api_cost"


LiveVerdict = Literal[
    "live",
    "ended",
    "unknown",
    "not_live",
    "unplayable",
    "login_required",
    "degraded",
    "inactive",
]


@dataclass(frozen=True)
class EvidenceRecord:
    source: SourceKind
    verdict: LiveVerdict
    target: TargetIdentity
    observed_at: float
    target_epoch: int
    restart_epoch: int
    ttl_sec: float
    raw: dict

    def is_fresh(self, now_ts: float, current_target_epoch: int, current_restart_epoch: int) -> bool:
        return (
            self.target_epoch >= current_target_epoch
            and self.restart_epoch >= current_restart_epoch
            and (now_ts - self.observed_at) <= self.ttl_sec
        )

    def to_dict(self) -> dict:
        return {
            "source": self.source.value,
            "verdict": self.verdict,
            "target": self.target.to_dict(),
            "observed_at": self.observed_at,
            "target_epoch": self.target_epoch,
            "restart_epoch": self.restart_epoch,
            "ttl_sec": self.ttl_sec,
            "raw": self.raw,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "EvidenceRecord":
        return cls(
            source=SourceKind(str(payload.get("source", "unknown"))),
            verdict=str(payload.get("verdict", "unknown") or "unknown"),  # type: ignore[arg-type]
            target=TargetIdentity.from_dict(payload.get("target")),
            observed_at=float(payload.get("observed_at", 0) or 0),
            target_epoch=int(payload.get("target_epoch", 0) or 0),
            restart_epoch=int(payload.get("restart_epoch", 0) or 0),
            ttl_sec=float(payload.get("ttl_sec", 0) or 0),
            raw=dict(payload.get("raw", {}) if isinstance(payload.get("raw", {}), dict) else {}),
        )
