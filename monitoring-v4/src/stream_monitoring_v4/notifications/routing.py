from __future__ import annotations

from dataclasses import dataclass

from stream_contracts.monitoring_v4.incident import IncidentEpisode, IncidentTransition
from stream_contracts.monitoring_v4.time import unix_ts


ROUTE_POLICY_REVISION = "monitoring-v4-route-policy-r5.1"


@dataclass(frozen=True)
class RoutePolicy:
    revision: str = ROUTE_POLICY_REVISION
    discord_route: str = "discord"
    slack_route: str = "slack"
    slack_after_sec: int = 1800

    def __post_init__(self) -> None:
        if not self.revision:
            raise ValueError("route policy revision is required")

    def routes(self, transition: IncidentTransition, episode: IncidentEpisode) -> tuple[str, ...]:
        routes = [self.discord_route]
        duration = max(0, unix_ts(transition.occurred_at) - unix_ts(episode.opened_at))
        routing_severity = episode.severity if transition.phase == "recovered" else transition.severity
        if routing_severity == "critical" or (
            routing_severity == "warning" and duration >= max(0, self.slack_after_sec)
        ):
            routes.append(self.slack_route)
        return tuple(dict.fromkeys(routes))
