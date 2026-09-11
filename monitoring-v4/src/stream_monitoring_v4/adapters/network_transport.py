from __future__ import annotations

from typing import Any, Mapping

from ._operational_base import OperationalSnapshotAdapter, mapping


def network_status(payload: Mapping[str, Any]) -> str:
    status = str(mapping(payload.get("classification")).get("status", "")).strip().lower()
    if status == "ok":
        return "good"
    if status in {"degraded", "incident_candidate", "route_change_observed"}:
        return "bad"
    return "unknown"


class NetworkTransportAdapter(OperationalSnapshotAdapter):
    domain = "network_transport"
    source = "network_observer"
    timestamp_keys = ("ts_utc",)
    freshness_limit_sec = 180

    def status(self, payload: Mapping[str, Any]) -> str:
        return network_status(payload)

    def safe_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        classification = mapping(payload.get("classification"))
        signals = mapping(classification.get("signals"))
        return {
            "status": str(classification.get("status", ""))[:32],
            "cause_layer": str(classification.get("cause_layer", ""))[:64],
            "cause": str(classification.get("cause", ""))[:96],
            "affected_path": str(classification.get("affected_path", ""))[:64],
            "signals": {
                str(name)[:64]: value
                for name, value in signals.items()
                if isinstance(value, bool)
            },
        }
