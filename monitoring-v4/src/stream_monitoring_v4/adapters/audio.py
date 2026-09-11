from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from stream_contracts.monitoring_v4.observation import ObservationEnvelope, ObservationRejection

from .base import AdapterBatch
from .snapshot_support import bounded_strings, read_source, source_timestamp, status_word


PRODUCER_REVISION = "stream-monitoring-v4-r2.3"
AUDIO_EVIDENCE_ALLOWLIST = frozenset(
    {
        "now_playing_fresh",
        "play_history_recent",
        "pulse_route_ok",
        "audio_energy_ok",
        "pulse_source_present",
    }
)


def legacy_audio_status(payload: Mapping[str, Any]) -> str:
    music = payload.get("music")
    if not isinstance(music, Mapping):
        return "unknown"
    return status_word(music.get("state"))


def legacy_audio_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = payload.get("music")
    music = raw if isinstance(raw, Mapping) else {}
    result: dict[str, Any] = {
        "origin": "subsystems_status.music",
        "state": str(music.get("state", ""))[:32],
        "confidence": str(music.get("confidence", ""))[:32],
        "evidence": [
            item
            for item in bounded_strings(music.get("evidence"))
            if item in AUDIO_EVIDENCE_ALLOWLIST
        ],
    }
    for key in (
        "audio_fail_count",
        "pulse_source_missing_count",
        "pulse_route_ok",
        "play_history_recent",
        "track_transition_within_grace",
        "bucket_boundary_within_grace",
    ):
        value = music.get(key)
        if isinstance(value, (int, float, bool)):
            result[key] = value
    if isinstance(music.get("now_playing_status"), str):
        result["now_playing_status"] = str(music["now_playing_status"])[:32]
    return result


class LegacySubsystemAudioAdapter:
    """Temporary read-only bridge for the v3 subsystem audio projection."""

    source = "legacy_subsystem_audio_state"

    def __init__(self, *, state_file: Path, deadline_sec: float = 2.0) -> None:
        self.state_file = Path(state_file)
        self.deadline_sec = max(0.01, float(deadline_sec))

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        rejections: list[ObservationRejection] = []
        snapshot = read_source(
            self.state_file,
            source="legacy_subsystem_audio",
            received_at=received_at,
            rejections=rejections,
        )
        if snapshot is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        receipt = snapshot.received_at
        observed_at = source_timestamp(
            snapshot,
            ("ts_utc",),
            source="legacy_subsystem_audio",
            received_at=receipt,
            rejections=rejections,
        )
        if observed_at is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        status = legacy_audio_status(snapshot.payload)
        item = ObservationEnvelope.create(
            domain="audio",
            source="legacy_subsystem_audio",
            source_event_id=snapshot.sha256,
            source_generation=snapshot.sha256,
            evidence_role="current_authoritative",
            status=status,
            reason_code=f"legacy_audio_{status}",
            observed_at=observed_at,
            received_at=receipt,
            freshness_limit_sec=180,
            producer_revision=PRODUCER_REVISION,
            payload=legacy_audio_payload(snapshot.payload),
        )
        return AdapterBatch(self.source, (item,), tuple(rejections))
