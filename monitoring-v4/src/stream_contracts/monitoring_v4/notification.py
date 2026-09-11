from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Sequence

from .ids import stable_id, validate_id
from .incident import SEVERITIES, TRANSITION_PHASES
from .observation import _name
from .time import parse_utc, require_not_before


@dataclass(frozen=True)
class NotificationIntent:
    SCHEMA: ClassVar[str] = "monitoring_v4.notification_intent.v1"

    intent_id: str
    transition_id: str
    episode_id: str
    route: str
    phase: str
    severity: str
    created_at: str
    not_before: str
    subject: str
    content: str
    dedupe_key: str
    route_policy_revision: str
    template_revision: str

    def __post_init__(self) -> None:
        validate_id(self.intent_id, field="intent_id")
        validate_id(self.transition_id, field="transition_id")
        validate_id(self.episode_id, field="episode_id")
        _name(self.route, field="route")
        _name(self.phase, field="transition_phase", allowed=TRANSITION_PHASES)
        _name(self.severity, field="severity", allowed=SEVERITIES)
        parse_utc(self.created_at, field="created_at")
        parse_utc(self.not_before, field="not_before")
        require_not_before(
            self.not_before,
            self.created_at,
            later_field="not_before",
            earlier_field="created_at",
        )
        if not self.subject or len(self.subject) > 200:
            raise ValueError("subject must contain 1-200 characters")
        if not self.content or len(self.content) > 4000:
            raise ValueError("content must contain 1-4000 characters")
        if not self.dedupe_key or len(self.dedupe_key) > 300:
            raise ValueError("dedupe_key must contain 1-300 characters")
        if not self.route_policy_revision or len(self.route_policy_revision) > 200:
            raise ValueError("route_policy_revision must contain 1-200 characters")
        if not self.template_revision or len(self.template_revision) > 200:
            raise ValueError("template_revision must contain 1-200 characters")

    @classmethod
    def create(
        cls,
        *,
        transition_id: str,
        episode_id: str,
        route: str,
        phase: str,
        severity: str,
        created_at: str,
        not_before: str,
        subject: str,
        content: str,
        route_policy_revision: str,
        template_revision: str,
        dedupe_parts: Sequence[str] = (),
    ) -> "NotificationIntent":
        dedupe_key = "|".join((route, phase, episode_id, *dedupe_parts))
        ident = stable_id(
            "ntf",
            transition_id,
            route,
            dedupe_key,
            route_policy_revision,
            template_revision,
        )
        return cls(
            ident,
            transition_id,
            episode_id,
            route,
            phase,
            severity,
            created_at,
            not_before,
            subject,
            content,
            dedupe_key,
            route_policy_revision,
            template_revision,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "intent_id": self.intent_id,
            "transition_id": self.transition_id,
            "episode_id": self.episode_id,
            "route": self.route,
            "phase": self.phase,
            "severity": self.severity,
            "created_at": self.created_at,
            "not_before": self.not_before,
            "subject": self.subject,
            "content": self.content,
            "dedupe_key": self.dedupe_key,
            "route_policy_revision": self.route_policy_revision,
            "template_revision": self.template_revision,
        }
