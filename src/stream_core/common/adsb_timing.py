"""Separate source-clock lag at collection from snapshot retention."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any
from . import observation_time

ADSB_SOURCE_SAMPLE_MAX_AGE_SEC = 180
ADSB_SOURCE_HEARTBEAT_MAX_AGE_SEC = 30


def _epoch(value: Any) -> float | None:
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        if isinstance(value, (int, float)):
            result = float(value)
        elif isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            result = parsed.timestamp()
        else:
            return None
    except (ValueError, TypeError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def adsb_source_timing(payload: dict[str, Any], *, now: float, reading=None, source_clock_model=None) -> dict[str, Any]:
    result = {"available": False, "sample_age": None, "heartbeat_age": None,
              "heartbeat_age_at_sample": None, "sample_fresh": False,
              "heartbeat_fresh": False, "capture_fresh": False}
    if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now):
        return result
    sample_value = payload.get("sample_ts")
    causal_age = None
    reference = observation_time.capture() if reading is None else reading
    if "observation_time" in payload:
        qualified = observation_time.qualify(payload["observation_time"], now=reference,
            max_age=ADSB_SOURCE_SAMPLE_MAX_AGE_SEC, source="adsb_overlay_probe")
        result["qualification"] = qualified.reason
        result["utc_status"] = qualified.utc_status
        if not qualified.valid:
            return result
        causal_age = qualified.age_upper
        sample_value = payload["observation_time"]["observed_at_utc"]
    if sample_value is None:
        sample_value = payload.get("ts_utc")
    sample = _epoch(sample_value)
    if sample is None or not 0 <= sample or causal_age is None and sample > now:
        return result
    sample_age = now - sample if causal_age is None else causal_age
    result["capture_fresh"] = sample_age <= ADSB_SOURCE_SAMPLE_MAX_AGE_SEC
    source = _epoch(payload.get("source_now"))
    if source is None or source < 0:
        return result
    heartbeat_age_at_sample = sample - source
    if causal_age is not None:
        bounds = observation_time.source_lag_bounds_at_collection(payload, now=reference, model=source_clock_model)
        result["source_lag_bounds_sec"] = bounds
        if bounds is None:
            result["qualification"] = "upstream_clock_unqualified_or_future"
            return result
        if bounds[0] <= ADSB_SOURCE_HEARTBEAT_MAX_AGE_SEC < bounds[1]:
            result["qualification"] = "upstream_age_uncertain"
            return result
        if type(payload.get("source_heartbeat_ok")) is not bool:
            result["qualification"] = "upstream_heartbeat_unknown"
            return result
        heartbeat_age_at_sample = bounds[1]
    elif source > sample:
        result["qualification"] = "upstream_clock_unqualified_or_future"
        return result
    result.update(available=True, sample_age=sample_age, heartbeat_age=now - source,
                  heartbeat_age_at_sample=heartbeat_age_at_sample,
                  sample_fresh=result["capture_fresh"],
                  heartbeat_fresh=heartbeat_age_at_sample <= ADSB_SOURCE_HEARTBEAT_MAX_AGE_SEC
                  and payload.get("source_heartbeat_ok", True) is True)
    return result


def adsb_freshness_signals(payload: dict[str, Any], now: datetime, *, reading=None, source_clock_model=None) -> dict[str, Any]:
    timing = adsb_source_timing(payload, now=now.timestamp(), reading=reading, source_clock_model=source_clock_model)
    fresh = timing["capture_fresh"]
    heartbeat_ok = timing["heartbeat_fresh"] if fresh and timing["available"] else None
    status = str(payload.get("status") or payload.get("judgment") or "").strip().lower()
    explicit_failure = (status in {"stale", "failed", "fail", "unhealthy"}
                        or payload.get("source_heartbeat_ok", True) is not True
                        or any(payload.get(key) is True for key in
                               ["stale", "messages_stalled", "aircraft_json_stale"]))
    if "observation_time" in payload and not timing["available"]:
        explicit_failure = False
    stale = fresh and (heartbeat_ok is False or explicit_failure)
    activity = str(payload.get("activity_state") or "unknown")
    if activity not in {"active", "quiet", "unknown"}:
        activity = "unknown"
    return {"fresh": fresh, "ok": fresh and heartbeat_ok is True and not explicit_failure,
            "stale": stale, "heartbeat_ok": heartbeat_ok, "activity_state": activity}
