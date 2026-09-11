from __future__ import annotations

from dataclasses import dataclass


SOURCE_POLICY_REVISION = "monitoring-v4-source-policy-r5.1"


@dataclass(frozen=True)
class SourceRule:
    source: str
    priority: int
    ttl_sec: int
    roles: tuple[str, ...] = ("current_authoritative", "current_correlated")
    producer_id: str = ""
    current_authority: bool = True
    diagnostic_kind: str = "correlated_non_authoritative"

    def __post_init__(self) -> None:
        if self.priority < 0:
            raise ValueError("source priority must be non-negative")
        if self.ttl_sec <= 0:
            raise ValueError("source ttl_sec must be positive")
        if not isinstance(self.producer_id, str):
            raise ValueError("producer_id must be a string")
        if self.producer_id and not self.producer_id.strip():
            raise ValueError("producer_id must be non-blank when provided")
        if not isinstance(self.current_authority, bool):
            raise ValueError("current_authority must be boolean")
        if not isinstance(self.diagnostic_kind, str) or not self.diagnostic_kind.strip():
            raise ValueError("diagnostic_kind must be a non-blank string")

    @property
    def producer_key(self) -> str:
        return self.producer_id or self.source


@dataclass(frozen=True)
class DomainPolicy:
    domain: str
    sources: tuple[SourceRule, ...]
    revision: str = SOURCE_POLICY_REVISION
    default_ttl_sec: int = 300
    allowed_roles: tuple[str, ...] = ("current_authoritative", "current_correlated")

    def rule(self, source: str) -> SourceRule | None:
        return next((item for item in self.sources if item.source == source), None)

    def __post_init__(self) -> None:
        if not self.revision:
            raise ValueError("source policy revision is required")


DEFAULT_POLICIES: dict[str, DomainPolicy] = {
    "youtube_lifecycle": DomainPolicy(
        "youtube_lifecycle",
        (
            SourceRule("youtube_watchdog", 100, 180),
            SourceRule("youtube_video_resolver", 80, 90),
            SourceRule(
                "youtube_api_direct_lifecycle",
                90,
                300,
                producer_id="youtube_watchdog",
                current_authority=False,
                diagnostic_kind="same_producer_api_probe",
            ),
            SourceRule("youtube_public", 30, 300, ("supporting",)),
        ),
    ),
    "youtube_input_quality": DomainPolicy(
        "youtube_input_quality",
        (
            SourceRule(
                "youtube_input_quality_oauth",
                100,
                600,
                producer_id="youtube_oauth_watchdog",
            ),
            SourceRule(
                "youtube_input_quality_prometheus",
                80,
                360,
                producer_id="youtube_oauth_watchdog",
                current_authority=False,
                diagnostic_kind="same_producer_projection",
            ),
            SourceRule(
                "youtube_input_quality_rolling",
                20,
                3600,
                ("historical",),
                producer_id="youtube_oauth_watchdog",
                current_authority=False,
                diagnostic_kind="same_producer_projection",
            ),
            SourceRule(
                "youtube_input_quality_api_direct",
                90,
                300,
                producer_id="youtube_oauth_watchdog",
                current_authority=False,
                diagnostic_kind="same_producer_api_probe",
            ),
        ),
    ),
    "delivery": DomainPolicy(
        "delivery",
        (
            SourceRule("runtime_delivery_watchdog", 100, 180),
            SourceRule("rtmps_tcp", 100, 60),
            SourceRule("viewer_delivery", 30, 300, ("supporting",)),
        ),
    ),
    "rendering": DomainPolicy(
        "rendering",
        (SourceRule("map_runtime", 100, 180), SourceRule("viewer_frame", 40, 900, ("supporting",))),
    ),
    "audio": DomainPolicy(
        "audio",
        (
            SourceRule("audio_watchdog", 110, 180),
            SourceRule("legacy_subsystem_audio", 100, 180),
            SourceRule("viewer_audio", 40, 900, ("supporting",)),
        ),
    ),
    "viewer_external": DomainPolicy(
        "viewer_external",
        (
            SourceRule("viewer_synthetic", 100, 900, ("supporting",)),
            SourceRule("external_blackbox", 80, 900, ("supporting",)),
        ),
        allowed_roles=("supporting",),
    ),
    "monitoring_platform": DomainPolicy(
        "monitoring_platform",
        (SourceRule("monitoring_self", 100, 180),),
    ),
    "network_transport": DomainPolicy(
        "network_transport",
        (SourceRule("network_observer", 100, 180),),
    ),
    "runtime_resource": DomainPolicy(
        "runtime_resource",
        (
            SourceRule("memory_status", 110, 180),
            SourceRule(
                "resource_memory",
                80,
                1200,
                ("current_correlated",),
                current_authority=False,
                diagnostic_kind="resource_diagnostic",
            ),
        ),
    ),
    "adsb_source": DomainPolicy(
        "adsb_source",
        (SourceRule("adsb_freshness", 100, 180),),
    ),
    "api_quota": DomainPolicy(
        "api_quota",
        (SourceRule("youtube_api_quota", 100, 900),),
    ),
    "recovery_policy": DomainPolicy(
        "recovery_policy",
        (SourceRule("recovery_plan", 100, 180),),
    ),
    "notification_delivery": DomainPolicy(
        "notification_delivery",
        (SourceRule("notification_delivery", 100, 180),),
    ),
    "control_loop": DomainPolicy(
        "control_loop",
        (SourceRule("control_loop_state", 100, 180),),
    ),
}
