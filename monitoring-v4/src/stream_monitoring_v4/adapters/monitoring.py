from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from stream_contracts.monitoring_v4.observation import ObservationEnvelope, ObservationRejection

from .base import AdapterBatch
from .snapshot_support import read_source, source_timestamp


PRODUCER_REVISION = "stream-monitoring-v4-r2.3"
CHECK_ALLOWLIST = (
    "arena_k3s_prometheus_ready",
    "grafana_prometheus_proxy_contract_present",
    "prometheus_ready",
    "public_grafana_health",
    "stream_v3_exporter_cache_fresh",
    "stream_v3_exporter_refresh_success",
    "stream_v3_exporter_up",
    "stream_v3_health_snapshot_fresh",
    "stream_v3_metrics_contract_present",
    "stream_v3_objective_snapshot_fresh",
    "stream_v3_target_up",
)


def monitoring_status(payload: Mapping[str, Any]) -> str:
    raw = payload.get("checks")
    checks = raw if isinstance(raw, Mapping) else {}
    values = [checks[name].get("ok") for name in CHECK_ALLOWLIST if isinstance(checks.get(name), Mapping)]
    if not values or any(value is not True and value is not False for value in values):
        return "unknown"
    if any(value is False for value in values):
        return "bad"
    return "good" if len(values) == len(CHECK_ALLOWLIST) else "unknown"


def monitoring_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = payload.get("checks")
    checks = raw if isinstance(raw, Mapping) else {}
    known = {
        name: checks[name].get("ok")
        for name in CHECK_ALLOWLIST
        if isinstance(checks.get(name), Mapping) and isinstance(checks[name].get("ok"), bool)
    }
    return {
        "expected_check_count": len(CHECK_ALLOWLIST),
        "observed_check_count": len(known),
        "ok_count": sum(value is True for value in known.values()),
        "bad_count": sum(value is False for value in known.values()),
        "failed_checks": sorted(name for name, value in known.items() if value is False),
        "missing_checks": sorted(name for name in CHECK_ALLOWLIST if name not in known),
        "repair_enabled": payload.get("repair_enabled") is True,
    }


class MonitoringSelfAdapter:
    source = "monitoring_self_state"

    def __init__(self, *, state_file: Path, deadline_sec: float = 2.0) -> None:
        self.state_file = Path(state_file)
        self.deadline_sec = max(0.01, float(deadline_sec))

    def collect(self, *, received_at: str | None = None) -> AdapterBatch:
        rejections: list[ObservationRejection] = []
        snapshot = read_source(
            self.state_file,
            source="monitoring_self",
            received_at=received_at,
            rejections=rejections,
        )
        if snapshot is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        receipt = snapshot.received_at
        observed_at = source_timestamp(
            snapshot,
            ("updated_at_utc",),
            source="monitoring_self",
            received_at=receipt,
            rejections=rejections,
        )
        if observed_at is None:
            return AdapterBatch(self.source, rejections=tuple(rejections))
        status = monitoring_status(snapshot.payload)
        item = ObservationEnvelope.create(
            domain="monitoring_platform",
            source="monitoring_self",
            source_event_id=snapshot.sha256,
            source_generation=snapshot.sha256,
            evidence_role="current_authoritative",
            status=status,
            reason_code=f"monitoring_self_{status}",
            observed_at=observed_at,
            received_at=receipt,
            freshness_limit_sec=180,
            producer_revision=PRODUCER_REVISION,
            payload=monitoring_payload(snapshot.payload),
        )
        return AdapterBatch(self.source, (item,), tuple(rejections))
