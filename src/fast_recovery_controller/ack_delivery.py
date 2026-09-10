"""Source-timed ACK delivery measurement with a measurement-first action gate."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

STATE_KEY = "ack_delivery_measurement_v1"
SCHEMA_VERSION = "stream_v3.ack_delivery_measurement.v1"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_TARGET_FIELDS = (
    "host_id",
    "host_boot_id",
    "namespace",
    "pod_uid",
    "container_name",
    "container_id",
    "ffmpeg_generation",
)
_PROFILE_FIELDS = ("video_bitrate", "video_maxrate", "video_bufsize", "audio_bitrate")
_MAX_OBSERVATION_ROWS = 128


def _parse_observed_us(value: object) -> int:
    if not isinstance(value, str) or not value:
        raise ValueError("ACK_RATE_SOURCE_TIME_MISSING")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("ACK_RATE_SOURCE_TIME_INVALID") from exc
    if parsed.tzinfo is None:
        raise ValueError("ACK_RATE_SOURCE_TIME_NAIVE")
    delta = parsed.astimezone(UTC) - _EPOCH
    observed_us = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    if observed_us <= 0:
        raise ValueError("ACK_RATE_SOURCE_TIME_INVALID")
    return observed_us


def _strict_int(value: object, *, minimum: int, reason: str) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(reason)
    return value


def _digest(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _normalized_profile(profile: Mapping[str, object]) -> dict[str, str]:
    return {name: str(profile.get(name) or "") for name in _PROFILE_FIELDS}


def _source_point(observation: Mapping[str, Any], *, now_ts: int, source_max_age_sec: int) -> dict[str, Any]:
    if observation.get("schema_version") != "runtime.ffmpeg_observation.v1":
        raise ValueError("ACK_RATE_SOURCE_SCHEMA_INVALID")
    observed_at = observation.get("observed_at")
    observed_us = _parse_observed_us(observed_at)
    age_us = now_ts * 1_000_000 - observed_us
    if age_us < -1_000_000 or age_us > source_max_age_sec * 1_000_000:
        raise ValueError("ACK_RATE_SOURCE_STALE_OR_FUTURE")

    producer = observation.get("producer_instance_id")
    source_generation = observation.get("ffmpeg_generation")
    if not isinstance(producer, str) or not producer or not isinstance(source_generation, str) or not source_generation:
        raise ValueError("ACK_RATE_SOURCE_IDENTITY_MISSING")
    sequence = _strict_int(observation.get("sequence"), minimum=1, reason="ACK_RATE_SOURCE_SEQUENCE_INVALID")
    protocol_pid = _strict_int(
        observation.get("protocol_ffmpeg_pid"),
        minimum=2,
        reason="ACK_RATE_PROTOCOL_PID_INVALID",
    )

    target_raw = observation.get("target_identity")
    if not isinstance(target_raw, Mapping):
        raise ValueError("ACK_RATE_TARGET_IDENTITY_MISSING")
    target: dict[str, object] = {}
    for name in _TARGET_FIELDS:
        value = target_raw.get(name)
        if not isinstance(value, str) or not value:
            raise ValueError("ACK_RATE_TARGET_IDENTITY_INVALID")
        target[name] = value
    target_pid = _strict_int(target_raw.get("ffmpeg_pid"), minimum=2, reason="ACK_RATE_TARGET_PID_INVALID")
    if target_pid != protocol_pid:
        raise ValueError("ACK_RATE_TARGET_PID_MISMATCH")

    metrics_raw = observation.get("tcp_metrics")
    if not isinstance(metrics_raw, Mapping):
        raise ValueError("ACK_RATE_TCP_METRICS_MISSING")
    metrics = {
        name: _strict_int(metrics_raw.get(name), minimum=0, reason=f"ACK_RATE_{name.upper()}_INVALID")
        for name in ("bytes_acked", "send_q", "notsent", "unacked", "lastsnd_ms", "rto_ms")
    }
    identity_sha256 = _digest(
        {
            "producer_instance_id": producer,
            "source_ffmpeg_generation": source_generation,
            "protocol_ffmpeg_pid": protocol_pid,
            "target_identity": {**target, "ffmpeg_pid": target_pid},
        }
    )
    return {
        "identity_sha256": identity_sha256,
        "observed_at": observed_at,
        "observed_us": observed_us,
        "sequence": sequence,
        "bytes_acked": metrics["bytes_acked"],
        "ffmpeg_pid": protocol_pid,
        **{name: metrics[name] for name in ("send_q", "notsent", "unacked", "lastsnd_ms", "rto_ms")},
    }


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 6)
    interpolated = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    return round(interpolated, 6)


def _distribution(values: list[float]) -> dict[str, int | float | None]:
    median = _percentile(values, 0.5)
    deviations = [] if median is None else [abs(value - median) for value in values]
    return {
        "count": len(values),
        "minimum_mbps": None if not values else round(min(values), 6),
        "p01_mbps": _percentile(values, 0.01),
        "p05_mbps": _percentile(values, 0.05),
        "median_mbps": median,
        "p95_mbps": _percentile(values, 0.95),
        "maximum_mbps": None if not values else round(max(values), 6),
        "mad_mbps": _percentile(deviations, 0.5),
    }


def _valid_history(raw: object, *, current_us: int, history_sec: int, history_sample_sec: int) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    cutoff_us = current_us - history_sec * 1_000_000
    result: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        observed_us = item.get("observed_us")
        mbps = item.get("ack_mbps")
        healthy = item.get("healthy")
        if (
            type(observed_us) is int
            and cutoff_us <= observed_us <= current_us
            and isinstance(mbps, (int, float))
            and not isinstance(mbps, bool)
            and math.isfinite(float(mbps))
            and float(mbps) >= 0
            and isinstance(healthy, bool)
        ):
            result.append(
                {
                    "observed_us": observed_us,
                    "ack_mbps": round(float(mbps), 6),
                    "healthy": healthy,
                }
            )
    result.sort(key=lambda item: int(item["observed_us"]))
    maximum_rows = max(2, history_sec // max(1, history_sample_sec) + 2)
    return result[-maximum_rows:]


def _valid_observations(raw: object, *, identity_sha256: str) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    result: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict) or item.get("identity_sha256") != identity_sha256:
            continue
        if not all(
            type(item.get(name)) is int and int(item[name]) >= minimum
            for name, minimum in (
                ("observed_us", 1),
                ("sequence", 1),
                ("bytes_acked", 0),
                ("ffmpeg_pid", 2),
                ("send_q", 0),
                ("notsent", 0),
                ("unacked", 0),
                ("lastsnd_ms", 0),
                ("rto_ms", 0),
            )
        ):
            continue
        if not isinstance(item.get("observed_at"), str):
            continue
        result.append(dict(item))
    result.sort(key=lambda item: int(item["observed_us"]))
    return result[-_MAX_OBSERVATION_ROWS:]


def _stored_nonnegative_int(value: object) -> int:
    return value if type(value) is int and value >= 0 else 0


def _iso_from_observed_us(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000, tz=UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _statistics(
    history: list[dict[str, Any]],
    *,
    history_sec: int,
    history_sample_sec: int,
    minimum_healthy_samples: int,
) -> dict[str, Any]:
    all_values = [float(item["ack_mbps"]) for item in history]
    healthy_rows = [item for item in history if item["healthy"] is True]
    healthy_values = [float(item["ack_mbps"]) for item in healthy_rows]
    history_span_sec = 0 if len(history) < 2 else (int(history[-1]["observed_us"]) - int(history[0]["observed_us"])) / 1_000_000
    healthy_span_sec = (
        0 if len(healthy_rows) < 2 else (int(healthy_rows[-1]["observed_us"]) - int(healthy_rows[0]["observed_us"])) / 1_000_000
    )
    required_span_sec = max(0, history_sec - 2 * history_sample_sec)
    expected_sample_count = max(1, history_sec // max(1, history_sample_sec))
    return {
        "history_sample_count": len(history),
        "healthy_sample_count": len(healthy_rows),
        "expected_sample_count": expected_sample_count,
        "history_coverage_ratio": round(min(1.0, len(history) / expected_sample_count), 6),
        "healthy_coverage_ratio": round(min(1.0, len(healthy_rows) / expected_sample_count), 6),
        "history_start_observed_at": None if not history else _iso_from_observed_us(int(history[0]["observed_us"])),
        "history_end_observed_at": None if not history else _iso_from_observed_us(int(history[-1]["observed_us"])),
        "healthy_start_observed_at": None if not healthy_rows else _iso_from_observed_us(int(healthy_rows[0]["observed_us"])),
        "healthy_end_observed_at": None if not healthy_rows else _iso_from_observed_us(int(healthy_rows[-1]["observed_us"])),
        "history_span_sec": round(history_span_sec, 3),
        "healthy_span_sec": round(healthy_span_sec, 3),
        "required_span_sec": required_span_sec,
        "minimum_healthy_samples": minimum_healthy_samples,
        "baseline_ready": len(healthy_rows) >= minimum_healthy_samples and healthy_span_sec >= required_span_sec,
        "all": _distribution(all_values),
        "healthy": _distribution(healthy_values),
    }


def _public(data: dict[str, Any], *, history_sample_emitted: bool) -> dict[str, Any]:
    return {key: value for key, value in data.items() if key not in {"observations", "history", "last_history_observed_us"}} | {
        "history_sample_emitted": history_sample_emitted
    }


def observe_ack_delivery(
    controller_state: dict[str, Any],
    *,
    now_ts: int,
    runtime_observation: Mapping[str, Any],
    stream_profile: Mapping[str, object],
    measurement_enabled: bool,
    action_enabled: bool,
    restart_threshold_mbps: float | None,
    pending_effect: bool,
    window_sec: int = 60,
    history_sec: int = 86400,
    history_sample_sec: int = 60,
    minimum_healthy_samples: int = 720,
    restart_confirmations: int = 2,
    source_max_age_sec: int = 30,
    max_window_slack_sec: int = 30,
    queue_notsent_threshold: int = 524288,
    queue_unacked_threshold: int = 256,
    queue_lastsnd_threshold_ms: int = 1000,
) -> dict[str, Any]:
    """Update bounded state and return a history-free measurement report.

    No threshold is inferred here. Until an operator supplies a threshold after
    reviewing a complete baseline, the output remains measurement-only.
    """

    normalized_profile = _normalized_profile(stream_profile)
    profile_sha256 = _digest(normalized_profile)
    raw = controller_state.get(STATE_KEY)
    data = dict(raw) if isinstance(raw, dict) and raw.get("schema_version") == SCHEMA_VERSION else {}
    previous_threshold = data.get("configured_restart_threshold_mbps")
    threshold_valid = restart_threshold_mbps is None or (
        isinstance(restart_threshold_mbps, (int, float))
        and not isinstance(restart_threshold_mbps, bool)
        and math.isfinite(float(restart_threshold_mbps))
        and float(restart_threshold_mbps) > 0
    )
    stored_threshold = float(restart_threshold_mbps) if threshold_valid and restart_threshold_mbps is not None else None
    if data.get("profile_sha256") not in {None, profile_sha256}:
        data = {"reset_reason": "STREAM_PROFILE_CHANGED"}
    elif previous_threshold != stored_threshold:
        data["shadow_low_streak"] = 0
    data.update(
        {
            "schema_version": SCHEMA_VERSION,
            "profile_sha256": profile_sha256,
            "stream_profile": normalized_profile,
            "measurement_enabled": measurement_enabled,
            "action_enabled": action_enabled,
            "configured_restart_threshold_mbps": stored_threshold,
            "window_sec": window_sec,
            "history_sec": history_sec,
            "history_sample_sec": history_sample_sec,
            "restart_confirmations": restart_confirmations,
        }
    )

    config_valid = (
        type(now_ts) is int
        and now_ts > 0
        and type(measurement_enabled) is bool
        and type(action_enabled) is bool
        and type(pending_effect) is bool
        and type(window_sec) is int
        and 10 <= window_sec <= 600
        and type(history_sec) is int
        and window_sec <= history_sec <= 7 * 86400
        and type(history_sample_sec) is int
        and 10 <= history_sample_sec <= history_sec
        and type(minimum_healthy_samples) is int
        and minimum_healthy_samples > 0
        and type(restart_confirmations) is int
        and 1 <= restart_confirmations <= 12
        and type(source_max_age_sec) is int
        and 1 <= source_max_age_sec <= 300
        and type(max_window_slack_sec) is int
        and 1 <= max_window_slack_sec <= window_sec
        and all(
            type(value) is int and value >= 0 for value in (queue_notsent_threshold, queue_unacked_threshold, queue_lastsnd_threshold_ms)
        )
        and threshold_valid
    )
    if not config_valid or not measurement_enabled:
        safe_history_sec = history_sec if type(history_sec) is int and history_sec >= 0 else 0
        safe_history_sample_sec = history_sample_sec if type(history_sample_sec) is int and history_sample_sec >= 0 else 0
        safe_minimum_samples = minimum_healthy_samples if type(minimum_healthy_samples) is int and minimum_healthy_samples > 0 else 1
        data.update(
            {
                "status": "DISABLED" if config_valid else "UNKNOWN",
                "reason_code": "ACK_RATE_MEASUREMENT_DISABLED" if config_valid else "ACK_RATE_CONFIG_INVALID",
                "observations": [],
                "history": [],
                "statistics": _statistics(
                    [],
                    history_sec=safe_history_sec,
                    history_sample_sec=safe_history_sample_sec,
                    minimum_healthy_samples=safe_minimum_samples,
                ),
                "shadow_low_streak": 0,
                "shadow_candidate_confirmed": False,
                "restart_candidate_confirmed": False,
                "latest": None,
            }
        )
        controller_state[STATE_KEY] = data
        return _public(data, history_sample_emitted=False)

    try:
        current = _source_point(runtime_observation, now_ts=now_ts, source_max_age_sec=source_max_age_sec)
    except (TypeError, ValueError, OverflowError) as exc:
        history = _valid_history(
            data.get("history"),
            current_us=now_ts * 1_000_000,
            history_sec=history_sec,
            history_sample_sec=history_sample_sec,
        )
        data.update(
            {
                "status": "UNKNOWN",
                "reason_code": str(exc),
                "observations": [],
                "history": history,
                "statistics": _statistics(
                    history,
                    history_sec=history_sec,
                    history_sample_sec=history_sample_sec,
                    minimum_healthy_samples=minimum_healthy_samples,
                ),
                "shadow_low_streak": 0,
                "shadow_candidate_confirmed": False,
                "restart_candidate_confirmed": False,
                "latest": None,
            }
        )
        controller_state[STATE_KEY] = data
        return _public(data, history_sample_emitted=False)

    same_identity = data.get("current_identity_sha256") == current["identity_sha256"]
    observations = _valid_observations(data.get("observations"), identity_sha256=str(current["identity_sha256"]))
    if not same_identity:
        observations = []
        data["shadow_low_streak"] = 0
        data["current_identity_sha256"] = current["identity_sha256"]

    if observations:
        previous = observations[-1]
        duplicate = (
            current["sequence"] == previous.get("sequence")
            and current["observed_us"] == previous.get("observed_us")
            and current["bytes_acked"] == previous.get("bytes_acked")
        )
        if duplicate:
            data.update(
                {
                    "status": "UNKNOWN",
                    "reason_code": "ACK_RATE_DUPLICATE_SOURCE_SAMPLE",
                    "shadow_candidate_confirmed": False,
                    "restart_candidate_confirmed": False,
                }
            )
            controller_state[STATE_KEY] = data
            return _public(data, history_sample_emitted=False)
        discontinuity = (
            int(current["sequence"]) <= int(previous.get("sequence", 0))
            or int(current["observed_us"]) <= int(previous.get("observed_us", 0))
            or int(current["bytes_acked"]) < int(previous.get("bytes_acked", 0))
        )
        if discontinuity:
            observations = []
            data["shadow_low_streak"] = 0

    observations.append(current)
    maximum_age_us = (window_sec + max_window_slack_sec) * 1_000_000
    observations = [item for item in observations if 0 <= int(current["observed_us"]) - int(item.get("observed_us", 0)) <= maximum_age_us]
    observations.sort(key=lambda item: int(item["observed_us"]))
    data["observations"] = observations

    baselines = [
        item
        for item in observations[:-1]
        if window_sec * 1_000_000 <= int(current["observed_us"]) - int(item["observed_us"]) <= maximum_age_us
    ]
    if not baselines:
        history = _valid_history(
            data.get("history"),
            current_us=int(current["observed_us"]),
            history_sec=history_sec,
            history_sample_sec=history_sample_sec,
        )
        data.update(
            {
                "status": "WARMING",
                "reason_code": "ACK_RATE_WINDOW_INCOMPLETE",
                "history": history,
                "statistics": _statistics(
                    history,
                    history_sec=history_sec,
                    history_sample_sec=history_sample_sec,
                    minimum_healthy_samples=minimum_healthy_samples,
                ),
                "shadow_candidate_confirmed": False,
                "restart_candidate_confirmed": False,
                "latest": None,
            }
        )
        controller_state[STATE_KEY] = data
        return _public(data, history_sample_emitted=False)

    baseline = max(baselines, key=lambda item: int(item["observed_us"]))
    elapsed_us = int(current["observed_us"]) - int(baseline["observed_us"])
    ack_delta = int(current["bytes_acked"]) - int(baseline["bytes_acked"])
    ack_mbps = round(ack_delta * 8 / elapsed_us, 6)
    queue_pressure = (
        int(current["notsent"]) >= queue_notsent_threshold
        or int(current["unacked"]) >= queue_unacked_threshold
        or int(current["lastsnd_ms"]) >= queue_lastsnd_threshold_ms
    )
    healthy = ack_mbps > 0 and not queue_pressure and not pending_effect
    latest = {
        "source_observed_at": current["observed_at"],
        "source_sequence": current["sequence"],
        "identity_sha256": current["identity_sha256"],
        "ffmpeg_pid": current["ffmpeg_pid"],
        "sample_interval_sec": round(elapsed_us / 1_000_000, 6),
        "bytes_acked_delta": ack_delta,
        "ack_mbps": ack_mbps,
        "send_q": current["send_q"],
        "notsent": current["notsent"],
        "unacked": current["unacked"],
        "lastsnd_ms": current["lastsnd_ms"],
        "rto_ms": current["rto_ms"],
        "queue_pressure": queue_pressure,
        "pending_effect": pending_effect,
        "healthy_baseline_eligible": healthy,
    }

    history = _valid_history(
        data.get("history"),
        current_us=int(current["observed_us"]),
        history_sec=history_sec,
        history_sample_sec=history_sample_sec,
    )
    last_history_us = _stored_nonnegative_int(data.get("last_history_observed_us"))
    history_sample_emitted = last_history_us <= 0 or int(current["observed_us"]) - last_history_us >= history_sample_sec * 1_000_000
    if history_sample_emitted:
        history.append(
            {
                "observed_us": current["observed_us"],
                "ack_mbps": ack_mbps,
                "healthy": healthy,
            }
        )
        data["last_history_observed_us"] = current["observed_us"]
    history = _valid_history(
        history,
        current_us=int(current["observed_us"]),
        history_sec=history_sec,
        history_sample_sec=history_sample_sec,
    )
    statistics = _statistics(
        history,
        history_sec=history_sec,
        history_sample_sec=history_sample_sec,
        minimum_healthy_samples=minimum_healthy_samples,
    )

    threshold_set = stored_threshold is not None
    below_threshold = stored_threshold is not None and ack_mbps < stored_threshold
    healthy_median = statistics["healthy"]["median_mbps"]
    threshold_to_healthy_median_ratio = (
        None
        if stored_threshold is None or not isinstance(healthy_median, (int, float)) or healthy_median <= 0
        else round(stored_threshold / float(healthy_median), 6)
    )
    shadow_condition = bool(statistics["baseline_ready"] and below_threshold and queue_pressure and not pending_effect)
    shadow_low_streak = _stored_nonnegative_int(data.get("shadow_low_streak")) + 1 if shadow_condition else 0
    shadow_confirmed = shadow_condition and shadow_low_streak >= restart_confirmations
    restart_confirmed = shadow_confirmed and action_enabled
    if restart_confirmed:
        reason_code = "ACK_RATE_RESTART_CANDIDATE_CONFIRMED"
    elif shadow_confirmed:
        reason_code = "ACK_RATE_SHADOW_CANDIDATE_CONFIRMED"
    elif not threshold_set:
        reason_code = "ACK_RATE_MEASURE_ONLY_THRESHOLD_UNSET"
    elif not statistics["baseline_ready"]:
        reason_code = "ACK_RATE_BASELINE_BUILDING"
    elif below_threshold and not queue_pressure:
        reason_code = "ACK_RATE_LOW_WITHOUT_QUEUE_PRESSURE"
    elif shadow_condition:
        reason_code = "ACK_RATE_LOW_CONFIRMING"
    else:
        reason_code = "ACK_RATE_OBSERVED"
    data.update(
        {
            "status": "VALID",
            "reason_code": reason_code,
            "history": history,
            "statistics": statistics,
            "latest": latest,
            "below_configured_threshold": below_threshold,
            "configured_threshold_to_healthy_median_ratio": threshold_to_healthy_median_ratio,
            "shadow_low_streak": shadow_low_streak,
            "shadow_candidate_confirmed": shadow_confirmed,
            "restart_candidate_confirmed": restart_confirmed,
        }
    )
    controller_state[STATE_KEY] = data
    return _public(data, history_sample_emitted=history_sample_emitted)
