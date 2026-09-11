from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from stream_contracts.monitoring_v4.observation import ObservationEnvelope, ObservationRejection

from .base import AdapterBatch
from .snapshot_support import bounded_strings, read_source, source_timestamp, status_word


PRODUCER_REVISION = "stream-monitoring-v4-r2.5"
MAP_CONDITIONS = (
    "asset_identity",
    "browser_contract",
    "deployment_ready",
    "pod_topology_ready",
    "precipitation_data_ok",
    "precipitation_fetcher_health",
    "precipitation_generation_integrity",
    "precipitation_render_applied",
    "precipitation_status",
    "precipitation_validtime_match",
    "render_heartbeat",
    "runtime_readiness",
    "semantic_visual_contract",
)
MAP_REASON_ALLOWLIST = frozenset(MAP_CONDITIONS)


def map_runtime_status(payload: Mapping[str, Any]) -> str:
    if payload.get("delivery_critical_ok") is False:
        return "bad"
    if payload.get("weather_ok") is False:
        return "bad"
    return status_word(payload.get("status"))


def map_runtime_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw_conditions = payload.get("conditions")
    conditions = raw_conditions if isinstance(raw_conditions, Mapping) else {}
    return {
        "schema": str(payload.get("schema", ""))[:96],
        "status": str(payload.get("status", ""))[:32],
        "delivery_critical_ok": payload.get("delivery_critical_ok")
        if isinstance(payload.get("delivery_critical_ok"), bool)
        else None,
        "weather_ok": payload.get("weather_ok") if isinstance(payload.get("weather_ok"), bool) else None,
        "conditions": {
            key: conditions[key]
            for key in MAP_CONDITIONS
            if key in conditions and isinstance(conditions[key], bool)
        },
        "critical_reasons": [
            item
            for item in bounded_strings(payload.get("critical_reasons"))
            if item in MAP_REASON_ALLOWLIST
        ],
        "weather_reasons": [
            item
            for item in bounded_strings(payload.get("weather_reasons"))
            if item in MAP_REASON_ALLOWLIST
        ],
        "probe_error_count": len(payload.get("probe_errors", []))
        if isinstance(payload.get("probe_errors"), list)
        else 0,
    }


class MapRuntimeAdapter:
    source = "map_runtime_state"

    def __init__(self, *, state_file: Path, deadline_sec: float = 2.0) -> None:
        self.state_file = Path(state_file)
        self.deadline_sec = max(0.01, float(deadline_sec))

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        rejections: list[ObservationRejection] = []
        snapshot = read_source(
            self.state_file,
            source="map_runtime",
            received_at=received_at,
            rejections=rejections,
        )
        if snapshot is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        receipt = snapshot.received_at
        observed_at = source_timestamp(
            snapshot,
            ("checked_at_utc",),
            source="map_runtime",
            received_at=receipt,
            rejections=rejections,
        )
        if observed_at is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        status = map_runtime_status(snapshot.payload)
        item = ObservationEnvelope.create(
            domain="rendering",
            source="map_runtime",
            source_event_id=snapshot.sha256,
            source_generation=snapshot.sha256,
            evidence_role="current_authoritative",
            status=status,
            reason_code=f"map_runtime_{status}",
            observed_at=observed_at,
            received_at=receipt,
            freshness_limit_sec=180,
            producer_revision=PRODUCER_REVISION,
            payload=map_runtime_payload(snapshot.payload),
        )
        return AdapterBatch(self.source, (item,), tuple(rejections))
