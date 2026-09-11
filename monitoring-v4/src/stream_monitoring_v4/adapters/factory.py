from __future__ import annotations

from pathlib import Path

from stream_contracts.monitoring_v4.observation import DOMAIN_ORDER

from .audio import LegacySubsystemAudioAdapter
from .base import ReadOnlyAdapter
from .external import ExternalBlackboxAdapter
from .lifecycle import RuntimeLifecycleAdapter
from .monitoring import MonitoringSelfAdapter
from .operations import (
    AdsbSourceAdapter,
    ControlLoopAdapter,
    MemoryStatusAdapter,
    NetworkTransportAdapter,
    NotificationDeliveryAdapter,
    RecoveryPolicyAdapter,
    RuntimeResourceAdapter,
    YouTubeApiQuotaAdapter,
)
from .reliability import InputQualityProjectionAdapter
from .rendering import MapRuntimeAdapter
from .viewer import ViewerSyntheticAdapter
from .youtube import YouTubeStateAdapter
from .youtube_api import YouTubeApiEvidenceAdapter


SHADOW_DOMAINS = DOMAIN_ORDER


def state_file_adapters(
    state_root: Path,
    *,
    youtube_api_state_file: Path | None = None,
) -> tuple[ReadOnlyAdapter, ...]:
    root = Path(state_root)
    adapters: tuple[ReadOnlyAdapter, ...] = (
        YouTubeStateAdapter(
            watchdog_stats_file=root / "youtube_watchdog_stats.json",
            resolver_state_file=root / "youtube_video_id_resolver_state.json",
        ),
        MapRuntimeAdapter(state_file=root / "map_runtime_status.json"),
        LegacySubsystemAudioAdapter(state_file=root / "subsystems_status.json"),
        ViewerSyntheticAdapter(state_file=root / "viewer_synthetic_status.json"),
        ExternalBlackboxAdapter(state_file=root / "external_blackbox_status.json"),
        MonitoringSelfAdapter(state_file=root / "monitoring_watchdog_state.json"),
        InputQualityProjectionAdapter(
            burn_status_file=root / "operational_reliability_burn_status.json"
        ),
        RuntimeLifecycleAdapter(state_file=root / "runtime_lifecycle_events.json"),
        NetworkTransportAdapter(state_file=root / "network_observer.json"),
        RuntimeResourceAdapter(state_file=root / "resource_memory.json"),
        MemoryStatusAdapter(state_file=root / "memory_status.json"),
        AdsbSourceAdapter(state_file=root / "adsb_freshness_state.json"),
        YouTubeApiQuotaAdapter(state_file=root / "youtube_api_quota_state.json"),
        RecoveryPolicyAdapter(state_file=root / "recovery_action_plan.json"),
        NotificationDeliveryAdapter(
            state_file=root / "notification_state.json",
            outbox_file=root / "notification_outbox_status.json",
        ),
        ControlLoopAdapter(state_file=root / "control_loop_state.json"),
    )
    if youtube_api_state_file is not None:
        adapters += (YouTubeApiEvidenceAdapter(state_file=youtube_api_state_file),)
    return adapters
