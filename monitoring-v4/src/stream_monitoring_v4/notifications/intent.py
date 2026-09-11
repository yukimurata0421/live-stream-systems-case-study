from __future__ import annotations

from stream_contracts.monitoring_v4.incident import IncidentEpisode, IncidentTransition
from stream_contracts.monitoring_v4.notification import NotificationIntent

from .renderer import TEMPLATE_REVISION, render
from .routing import RoutePolicy


def build_notification_intents(
    transition: IncidentTransition,
    episode: IncidentEpisode,
    route_policy: RoutePolicy,
) -> tuple[NotificationIntent, ...]:
    subject, content = render(transition, episode)
    return tuple(
        NotificationIntent.create(
            transition_id=transition.transition_id,
            episode_id=transition.episode_id,
            route=route,
            phase=transition.phase,
            severity=transition.severity,
            created_at=transition.occurred_at,
            not_before=transition.occurred_at,
            subject=subject,
            content=content,
            route_policy_revision=route_policy.revision,
            template_revision=TEMPLATE_REVISION,
            dedupe_parts=(transition.transition_id,),
        )
        for route in route_policy.routes(transition, episode)
    )
