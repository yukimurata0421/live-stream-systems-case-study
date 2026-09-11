from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


INCIDENT_POLICY_REVISION = "monitoring-v4-incident-policy-r5.1"


@dataclass(frozen=True)
class IncidentPolicy:
    domain: str
    summary: str
    revision: str = INCIDENT_POLICY_REVISION
    bad_severity: str = "warning"
    unknown_severity: str = "warning"
    bad_min_samples: int = 1
    bad_min_duration_sec: int = 0
    unknown_min_samples: int = 2
    unknown_min_duration_sec: int = 300
    repeat_sec: int = 600
    bad_repeat_sec: int | None = None
    unknown_repeat_sec: int | None = None
    reason_repeat_sec: tuple[tuple[str, int], ...] = ()
    actionable_unknown_reason_codes: tuple[str, ...] | None = None
    suppress_during_maintenance: bool = True

    def __post_init__(self) -> None:
        if not self.revision:
            raise ValueError("incident policy revision is required")
        if self.bad_min_samples < 1 or self.unknown_min_samples < 1:
            raise ValueError("incident sample thresholds must be positive")
        if self.bad_min_duration_sec < 0 or self.unknown_min_duration_sec < 0:
            raise ValueError("incident duration thresholds must be non-negative")
        if self.repeat_sec < 60:
            raise ValueError("repeat_sec must be at least 60 seconds")
        for value in (self.bad_repeat_sec, self.unknown_repeat_sec):
            if value is not None and value < 60:
                raise ValueError("state repeat interval must be at least 60 seconds")
        for reason, interval in self.reason_repeat_sec:
            if not reason or interval < 60:
                raise ValueError("reason repeat policy is invalid")
        if self.actionable_unknown_reason_codes is not None and any(
            not reason for reason in self.actionable_unknown_reason_codes
        ):
            raise ValueError("actionable unknown reason codes must be non-empty")

    def threshold(self, state: str) -> tuple[int, int, str]:
        if state == "bad":
            return self.bad_min_samples, self.bad_min_duration_sec, self.bad_severity
        if state == "unknown":
            return self.unknown_min_samples, self.unknown_min_duration_sec, self.unknown_severity
        raise ValueError(f"no incident threshold for state={state!r}")

    def repeat_interval(self, state: str, reason_codes: Sequence[str]) -> int:
        by_reason = dict(self.reason_repeat_sec)
        matching = [by_reason[reason] for reason in reason_codes if reason in by_reason]
        if matching:
            return max(matching)
        if state == "bad" and self.bad_repeat_sec is not None:
            return self.bad_repeat_sec
        if state == "unknown" and self.unknown_repeat_sec is not None:
            return self.unknown_repeat_sec
        return self.repeat_sec

    def is_actionable(self, state: str, reason_codes: Sequence[str]) -> bool:
        if state == "bad":
            return True
        if state != "unknown":
            return False
        if self.actionable_unknown_reason_codes is None:
            return True
        allowed = set(self.actionable_unknown_reason_codes)
        return any(reason in allowed for reason in reason_codes)


DEFAULT_INCIDENT_POLICIES: dict[str, IncidentPolicy] = {
    "youtube_lifecycle": IncidentPolicy(
        "youtube_lifecycle",
        "YouTube lifecycle current state is outside the accepted contract",
        unknown_min_samples=2,
        unknown_min_duration_sec=300,
        repeat_sec=600,
        reason_repeat_sec=(("source_disagreement", 1800),),
    ),
    "youtube_input_quality": IncidentPolicy(
        "youtube_input_quality",
        "YouTube current input quality warning is active",
        bad_severity="warning",
        unknown_min_samples=2,
        unknown_min_duration_sec=300,
        repeat_sec=600,
        actionable_unknown_reason_codes=(
            "measurement_coverage_missing",
            "measurement_freshness_stale",
        ),
    ),
    "delivery": IncidentPolicy(
        "delivery",
        "stream delivery current state is degraded",
        bad_severity="critical",
        unknown_min_samples=2,
        unknown_min_duration_sec=300,
        repeat_sec=600,
        reason_repeat_sec=(("source_disagreement", 1800),),
    ),
    "rendering": IncidentPolicy("rendering", "rendering current state is degraded", repeat_sec=600),
    "audio": IncidentPolicy("audio", "audio current state is degraded", repeat_sec=600),
    "viewer_external": IncidentPolicy(
        "viewer_external",
        "supporting viewer or external evidence is degraded",
        unknown_min_samples=2,
        unknown_min_duration_sec=300,
        repeat_sec=900,
        bad_repeat_sec=300,
        unknown_repeat_sec=900,
    ),
    "monitoring_platform": IncidentPolicy(
        "monitoring_platform",
        "Monitoring v4 self-health is degraded",
        repeat_sec=600,
    ),
    "network_transport": IncidentPolicy(
        "network_transport",
        "network and RTMPS reachability evidence is degraded",
        bad_severity="critical",
        bad_min_samples=2,
        bad_min_duration_sec=180,
        repeat_sec=600,
    ),
    "runtime_resource": IncidentPolicy(
        "runtime_resource",
        "runtime resource guardrail is degraded",
        bad_min_samples=2,
        bad_min_duration_sec=300,
        repeat_sec=600,
    ),
    "adsb_source": IncidentPolicy(
        "adsb_source",
        "ADS-B source freshness is degraded",
        bad_min_samples=2,
        bad_min_duration_sec=180,
        repeat_sec=600,
    ),
    "api_quota": IncidentPolicy(
        "api_quota",
        "YouTube API quota or coverage evidence is degraded",
        bad_min_samples=2,
        bad_min_duration_sec=300,
        repeat_sec=600,
    ),
    "recovery_policy": IncidentPolicy(
        "recovery_policy",
        "a non-noop recovery action is pending",
        bad_min_samples=2,
        bad_min_duration_sec=300,
        repeat_sec=600,
    ),
    "notification_delivery": IncidentPolicy(
        "notification_delivery",
        "notification delivery outbox is pending or malformed",
        bad_min_samples=2,
        bad_min_duration_sec=300,
        repeat_sec=600,
    ),
    "control_loop": IncidentPolicy(
        "control_loop",
        "the V3 control-loop task matrix is incomplete, stale, or failed",
        bad_min_samples=2,
        bad_min_duration_sec=180,
        repeat_sec=600,
    ),
}
