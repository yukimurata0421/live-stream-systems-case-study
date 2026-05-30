from __future__ import annotations

from dataclasses import dataclass

try:
    from evidence.sources import SourceKind
except ImportError:
    from ..evidence.sources import SourceKind  # type: ignore


@dataclass(frozen=True)
class Policy:
    ttl: dict[SourceKind, float]
    budget_release_reconfirm_sec: float = 300.0
    min_restart_interval_sec: float = 300.0

    @classmethod
    def from_values(
        cls,
        *,
        data_api_ttl_sec: int,
        oauth_ttl_sec: int,
        watch_ttl_sec: int,
        resolver_ttl_sec: int,
        ingest_ttl_sec: int,
        api_cost_ttl_sec: int,
        budget_release_reconfirm_sec: int,
        min_restart_interval_sec: int,
    ) -> "Policy":
        return cls(
            ttl={
                SourceKind.DATA_API: float(data_api_ttl_sec),
                SourceKind.OAUTH: float(oauth_ttl_sec),
                SourceKind.WATCH_PAGE: float(watch_ttl_sec),
                SourceKind.RESOLVER: float(resolver_ttl_sec),
                SourceKind.INGEST_LOCAL: float(ingest_ttl_sec),
                SourceKind.API_COST: float(api_cost_ttl_sec),
            },
            budget_release_reconfirm_sec=float(budget_release_reconfirm_sec),
            min_restart_interval_sec=float(min_restart_interval_sec),
        )
