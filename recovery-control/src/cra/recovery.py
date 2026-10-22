"""Delivery observations, independent of execution attribution or runtime extensions."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cra.observe import Observation


def delivery_sample(obs: Observation) -> dict[str, Any]:
    return {
        "generation_key": obs.generation_key,
        "executor_instance_id": obs.executor_instance_id,
        "delivery_route": obs.raw.get("delivery_route"),
        "delivery_route_generation": obs.raw.get("delivery_route_generation"),
        "sequence": obs.sequence,
        "observed_at": obs.observed_at.isoformat(),
        "uptime_sec": obs.ffmpeg_uptime_sec,
        "ffmpeg_running": obs.ffmpeg_running,
        "tcp": vars(obs.tcp) if obs.tcp else None,
    }


def after_request(sample: dict[str, Any], response: dict[str, Any]) -> bool:
    try:
        pod, _, generation = sample["generation_key"].partition("/")
        elapsed = (
            datetime.fromisoformat(sample["observed_at"]) - datetime.fromisoformat(response["agent_request_issued_at"])
        ).total_seconds()
        return (
            pod == response["agent_pod_uid"]
            and sample["executor_instance_id"] == response["agent_executor_instance_id"]
            and bool(generation)
            and generation != response["agent_previous_generation"]
            and 0 <= sample["uptime_sec"] <= elapsed <= response["agent_verify_window_sec"]
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def sending_progress(first: dict[str, Any], second: dict[str, Any], *, max_gap_sec: float, lastsnd_ms: int) -> bool:
    try:
        gap = (datetime.fromisoformat(second["observed_at"]) - datetime.fromisoformat(first["observed_at"])).total_seconds()
        return (
            all(first[k] == second[k] for k in ("generation_key", "executor_instance_id", "delivery_route", "delivery_route_generation"))
            and first["ffmpeg_running"] is True
            and second["ffmpeg_running"] is True
            and second["sequence"] > first["sequence"]
            and 0 < gap <= max_gap_sec
            and 0 <= first["tcp"]["bytes_sent"] < second["tcp"]["bytes_sent"]
            and 0 <= second["tcp"]["lastsnd_ms"] < lastsnd_ms
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return False
