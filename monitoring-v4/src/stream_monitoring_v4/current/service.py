from __future__ import annotations

from collections.abc import Iterable, Mapping

from stream_contracts.monitoring_v4.current import DomainCurrent

from stream_monitoring_v4.domains.reducer import reduce_domain
from stream_monitoring_v4.domains.source_policy import DomainPolicy
from stream_monitoring_v4.storage.ports import CurrentReducerRepository


class CurrentReducerService:
    def __init__(
        self,
        repository: CurrentReducerRepository,
        policies: Mapping[str, DomainPolicy],
    ) -> None:
        self.repository = repository
        self.policies = dict(policies)

    def reduce(self, domains: Iterable[str], *, now_ts: int) -> list[DomainCurrent]:
        snapshots = [
            reduce_domain(
                self.repository.latest_observations(domain),
                self.policies[domain],
                now_ts=now_ts,
            )
            for domain in domains
        ]
        canonical: list[DomainCurrent] = []
        reused = 0
        with self.repository.transaction() as connection:
            for snapshot in snapshots:
                if self.repository.save_current(snapshot, connection=connection):
                    canonical.append(snapshot)
                    continue
                selected = self.repository.current(snapshot.domain, connection=connection)
                if selected is None:
                    raise RuntimeError(f"domain current disappeared for {snapshot.domain}")
                canonical.append(selected)
                reused += 1
        self.repository.set_component_health(
            "current_reducer",
            "good",
            f"domains={len(canonical)} canonical_reused={reused}",
            now_ts=now_ts,
        )
        return canonical
