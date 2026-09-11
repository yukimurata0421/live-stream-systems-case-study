from __future__ import annotations

from dataclasses import dataclass


COLLECTOR_ADAPTERS = (
    "youtube_state_files",
    "map_runtime_state",
    "legacy_subsystem_audio_state",
    "viewer_synthetic_state",
    "external_blackbox_state",
    "monitoring_self_state",
    "input_quality_projection_state",
    "runtime_lifecycle_state",
    "youtube_api_direct_state",
)


@dataclass(frozen=True)
class SourceCoveragePolicy:
    cadence_sec: int
    basis: str

    def __post_init__(self) -> None:
        if self.cadence_sec <= 0:
            raise ValueError("source coverage cadence must be positive")
        if not self.basis.strip():
            raise ValueError("source coverage basis is required")


# These are evidence-coverage buckets, not claims that every producer is a
# wall-clock timer. The arena control loop schedules a task's next run after
# completion and runs tasks serially; cached remote probes therefore update
# more slowly than the loop that writes their containing file. Each bucket is
# at or inside the v4 freshness boundary and includes the deployed producer's
# declared interval, execution time, and timer accuracy.
SOURCE_CADENCES = {
    "youtube_watchdog": SourceCoveragePolicy(
        180,
        "120s remote-probe cache plus serial 45s monitor scheduling; lifecycle freshness 180s",
    ),
    "youtube_input_quality_oauth": SourceCoveragePolicy(
        180,
        "120s OAuth cache plus serial 45s monitor scheduling; input-quality freshness 600s",
    ),
    "runtime_delivery_watchdog": SourceCoveragePolicy(
        120,
        "45s monitor file updates sampled by a 60s shadow; delivery freshness 180s",
    ),
    "youtube_video_resolver": SourceCoveragePolicy(
        60,
        "5s resolver task sampled by a 60s shadow; resolver freshness 90s",
    ),
    "map_runtime": SourceCoveragePolicy(
        120,
        "60s serial arena task plus execution delay; rendering freshness 180s",
    ),
    "legacy_subsystem_audio": SourceCoveragePolicy(
        120,
        "60s serial arena task plus execution delay; audio freshness 180s",
    ),
    "viewer_synthetic": SourceCoveragePolicy(
        360,
        "300s serial arena task plus execution delay; viewer freshness 900s",
    ),
    "external_blackbox": SourceCoveragePolicy(
        360,
        "300s inactive timer plus 30s accuracy and import duration; external freshness 900s",
    ),
    "monitoring_self": SourceCoveragePolicy(
        120,
        "60s timer plus 15s accuracy; monitoring freshness 180s",
    ),
    "youtube_api_direct_lifecycle": SourceCoveragePolicy(
        180,
        "120s arena direct API collector plus bounded request duration; evidence freshness 300s",
    ),
    "youtube_input_quality_api_direct": SourceCoveragePolicy(
        180,
        "120s arena direct API collector plus bounded request duration; evidence freshness 300s",
    ),
}

PARITY_CONVERGENCE_GRACE_SEC = 120
MINIMUM_EQUIVALENT_CYCLE_PCT = 99.9
MAXIMUM_ACCEPTED_DIFFERENCE_PCT = 0.1
