from __future__ import annotations


PULSE_HEALTH_DEFAULT = {
    "dj_missing_count": 0,
    "capture_missing_count": 0,
    "dj_latency_high_count": 0,
    "capture_latency_high_count": 0,
}


def int_value(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def normalize_health_state(data: dict | None) -> dict[str, int]:
    source = data if isinstance(data, dict) else {}
    normalized: dict[str, int] = {}
    for key, default in PULSE_HEALTH_DEFAULT.items():
        normalized[key] = int_value(source.get(key, default), default)
    return normalized


def update_health_state(
    state: dict[str, int],
    metrics: dict,
    *,
    dj_latency_crit_usec: int,
    capture_latency_crit_usec: int,
) -> dict[str, int]:
    current = normalize_health_state(state)
    dj_present = bool(metrics.get("dj_sink_input_present", False))
    capture_present = bool(metrics.get("capture_source_output_present", False))
    dj_buf = int_value(metrics.get("dj_buffer_latency_usec", -1), -1)
    capture_buf = int_value(metrics.get("capture_buffer_latency_usec", -1), -1)

    return {
        "dj_missing_count": current["dj_missing_count"] + 1 if not dj_present else 0,
        "capture_missing_count": current["capture_missing_count"] + 1 if not capture_present else 0,
        "dj_latency_high_count": (
            current["dj_latency_high_count"] + 1 if dj_present and dj_buf >= dj_latency_crit_usec else 0
        ),
        "capture_latency_high_count": (
            current["capture_latency_high_count"] + 1
            if capture_present and capture_buf >= capture_latency_crit_usec
            else 0
        ),
    }


def anomaly_decision(
    state: dict[str, int],
    metrics: dict,
    *,
    threshold: int,
    dj_latency_crit_usec: int,
    capture_latency_crit_usec: int,
) -> dict | None:
    if state["dj_missing_count"] >= threshold:
        return {
            "case": "dj_sink_input_missing",
            "count": state["dj_missing_count"],
            "component": "dj",
            "reason": "pulse route missing: AutoDJ sink-input not found",
            "extra_stream_reason": (
                "pulse route unstable: repeated AutoDJ sink-input missing"
                if state["dj_missing_count"] >= threshold * 2
                else ""
            ),
            "event_fields": {},
        }
    if state["capture_missing_count"] >= threshold:
        return {
            "case": "stream_capture_source_output_missing",
            "count": state["capture_missing_count"],
            "component": "stream",
            "reason": "pulse capture route missing: stream source-output not found",
            "extra_stream_reason": "",
            "event_fields": {},
        }
    if state["dj_latency_high_count"] >= threshold:
        dj_buf = int_value(metrics.get("dj_buffer_latency_usec", -1), -1)
        return {
            "case": "dj_buffer_latency_high",
            "count": state["dj_latency_high_count"],
            "component": "dj",
            "reason": f"pulse dj buffer latency high ({dj_buf} usec)",
            "extra_stream_reason": "",
            "event_fields": {
                "observed_dj_buffer_latency_usec": dj_buf,
                "threshold_usec": dj_latency_crit_usec,
            },
        }
    if state["capture_latency_high_count"] >= threshold:
        capture_buf = int_value(metrics.get("capture_buffer_latency_usec", -1), -1)
        return {
            "case": "stream_capture_buffer_latency_high",
            "count": state["capture_latency_high_count"],
            "component": "stream",
            "reason": f"pulse capture buffer latency high ({capture_buf} usec)",
            "extra_stream_reason": "",
            "event_fields": {
                "observed_capture_buffer_latency_usec": capture_buf,
                "threshold_usec": capture_latency_crit_usec,
            },
        }
    return None


def warning_due(
    metrics: dict,
    *,
    dj_latency_warn_usec: int,
    capture_latency_warn_usec: int,
) -> bool:
    dj_present = bool(metrics.get("dj_sink_input_present", False))
    capture_present = bool(metrics.get("capture_source_output_present", False))
    dj_buf = int_value(metrics.get("dj_buffer_latency_usec", -1), -1)
    capture_buf = int_value(metrics.get("capture_buffer_latency_usec", -1), -1)
    return (dj_present and dj_buf >= dj_latency_warn_usec) or (
        capture_present and capture_buf >= capture_latency_warn_usec
    )
