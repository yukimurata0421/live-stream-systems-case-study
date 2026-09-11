from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from stream_contracts.monitoring_v4.runtime_evidence import RuntimeRolloutProjection

from stream_monitoring_v4.adapters.json_file import SnapshotReadError, read_json_snapshot
from stream_monitoring_v4.adapters.monitoring import monitoring_status
from stream_monitoring_v4.adapters.operations import (
    adsb_status,
    control_status,
    network_status,
    memory_status,
    notification_status,
    quota_status,
    recovery_status,
)
from stream_monitoring_v4.adapters.rendering import map_runtime_status
from stream_monitoring_v4.adapters.snapshot_support import status_word
from stream_monitoring_v4.adapters.viewer import viewer_status

from .live_parity import live_parity_report


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _read(path: Path) -> tuple[Mapping[str, Any], str]:
    try:
        snapshot = read_json_snapshot(path)
    except SnapshotReadError as exc:
        return {}, exc.reason_code
    return snapshot.payload, ""


def _input_quality_current(payload: Mapping[str, Any]) -> str:
    feedback_root = _mapping(payload.get("fast_feedback"))
    feedback = _mapping(feedback_root.get("youtube_input_quality"))
    current = _mapping(feedback.get("raw_current"))
    if current.get("available") is not True or current.get("eligible") is not True:
        return "unknown"
    classification = str(current.get("classification", "")).strip().lower()
    if classification == "good":
        return "good"
    if classification.startswith("bad_"):
        return "bad"
    return "unknown"


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def normalized_v3_current(state_root: Path) -> dict[str, dict[str, str]]:
    """Normalize current v3 artifacts without importing or executing v3 code."""

    root = Path(state_root)
    subsystems, subsystems_error = _read(root / "subsystems_status.json")
    map_state, map_error = _read(root / "map_runtime_status.json")
    viewer, viewer_error = _read(root / "viewer_synthetic_status.json")
    monitoring, monitoring_error = _read(root / "monitoring_watchdog_state.json")
    burn, burn_error = _read(root / "operational_reliability_burn_status.json")
    input_quality = _mapping(_mapping(burn.get("fast_feedback")).get("youtube_input_quality"))
    input_quality_raw = _mapping(input_quality.get("raw_current"))
    network, network_error = _read(root / "network_observer.json")
    memory, memory_error = _read(root / "memory_status.json")
    adsb, adsb_error = _read(root / "adsb_freshness_state.json")
    quota, quota_error = _read(root / "youtube_api_quota_state.json")
    recovery, recovery_error = _read(root / "recovery_action_plan.json")
    notification, notification_error = _read(root / "notification_state.json")
    outbox, outbox_error = _read(root / "notification_outbox_status.json")
    control, control_error = _read(root / "control_loop_state.json")

    def subsystem(name: str) -> str:
        return status_word(_mapping(subsystems.get(name)).get("state"))

    return {
        "youtube_lifecycle": {
            "state": subsystem("youtube_lifecycle") if not subsystems_error else "unknown",
            "source": "subsystems_status.youtube_lifecycle",
            "observed_at": _text(subsystems.get("ts_utc")),
            "normalization_error": subsystems_error,
        },
        "youtube_input_quality": {
            "state": _input_quality_current(burn) if not burn_error else "unknown",
            "source": "operational_reliability_burn_status.raw_current",
            "observed_at": _text(input_quality_raw.get("ts_utc")),
            "normalization_error": burn_error,
        },
        "delivery": {
            "state": subsystem("local_delivery") if not subsystems_error else "unknown",
            "source": "subsystems_status.local_delivery",
            "observed_at": _text(subsystems.get("ts_utc")),
            "normalization_error": subsystems_error,
        },
        "rendering": {
            "state": map_runtime_status(map_state) if not map_error else "unknown",
            "source": "map_runtime_status",
            "observed_at": _text(map_state.get("checked_at_utc")),
            "normalization_error": map_error,
        },
        "audio": {
            "state": subsystem("music") if not subsystems_error else "unknown",
            "source": "subsystems_status.music",
            "observed_at": _text(subsystems.get("ts_utc")),
            "normalization_error": subsystems_error,
        },
        "viewer_external": {
            "state": viewer_status(viewer) if not viewer_error else "unknown",
            "source": "viewer_synthetic_status",
            "observed_at": _text(viewer.get("checked_at_utc")),
            "normalization_error": viewer_error,
        },
        "monitoring_platform": {
            "state": monitoring_status(monitoring) if not monitoring_error else "unknown",
            "source": "monitoring_watchdog_state",
            "observed_at": _text(monitoring.get("updated_at_utc")),
            "normalization_error": monitoring_error,
        },
        "network_transport": {
            "state": network_status(network) if not network_error else "unknown",
            "source": "network_observer.classification",
            "observed_at": _text(network.get("ts_utc")),
            "normalization_error": network_error,
        },
        "runtime_resource": {
            "state": memory_status(memory) if not memory_error else "unknown",
            "source": "memory_status.overall",
            "observed_at": _text(memory.get("generated_at_utc")),
            "normalization_error": memory_error,
        },
        "adsb_source": {
            "state": adsb_status(adsb) if not adsb_error else "unknown",
            "source": "adsb_freshness_state",
            "observed_at": _text(adsb.get("ts_utc")),
            "normalization_error": adsb_error,
        },
        "api_quota": {
            "state": quota_status(quota) if not quota_error else "unknown",
            "source": "youtube_api_cost.open_day",
            "observed_at": _text(_mapping(quota.get("window")).get("effective_end_utc")),
            "normalization_error": quota_error,
        },
        "recovery_policy": {
            "state": recovery_status(recovery) if not recovery_error else "unknown",
            "source": "recovery_action_plan",
            "observed_at": _text(recovery.get("ts_utc")),
            "normalization_error": recovery_error,
        },
        "notification_delivery": {
            "state": notification_status(notification, outbox)
            if not notification_error and not outbox_error
            else "unknown",
            "source": "stream_notify_state+outbox",
            "observed_at": _text(notification.get("updated_ts_utc")),
            "normalization_error": notification_error or outbox_error,
        },
        "control_loop": {
            "state": control_status(control) if not control_error else "unknown",
            "source": "v3_control_state",
            "observed_at": _text(control.get("updated_at_utc")),
            "normalization_error": control_error,
        },
    }


def runtime_rollout_projection(state_root: Path) -> RuntimeRolloutProjection | None:
    payload, error = _read(Path(state_root) / "runtime_rollout_evidence.json")
    if error:
        return None
    try:
        return RuntimeRolloutProjection.from_dict(payload)
    except (TypeError, ValueError):
        return None
