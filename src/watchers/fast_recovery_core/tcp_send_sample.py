"""Source-timed upload observations; never used for recovery decisions."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

STATE_KEY = "tcp_send_sample_source_v2"
TIME_SOURCE = "runtime_observation.observed_at"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def bind_runtime_metrics(
    observation: dict[str, Any], *, consumer_monotonic_ns: int
) -> dict[str, Any]:
    """Keep the counter and its source metadata from the same atomic snapshot."""
    metrics = observation.get("tcp_metrics")
    if not isinstance(metrics, dict) or not metrics:
        return {}
    return {
        **metrics,
        "sample_observed_at": observation.get("observed_at"),
        "sample_producer_instance_id": observation.get("producer_instance_id"),
        "sample_ffmpeg_generation": observation.get("ffmpeg_generation"),
        "sample_source_sequence": observation.get("sequence"),
        "sample_consumer_monotonic_ns": consumer_monotonic_ns,
    }


def _positive_int(value: Any) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError("invalid positive integer")
    return value


def _snapshot(
    ffmpeg_pid: int, bytes_sent: int, metrics: dict[str, Any]
) -> dict[str, Any]:
    observed_at = metrics["sample_observed_at"]
    if not isinstance(observed_at, str):
        raise TypeError("missing source timestamp")
    parsed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("source timestamp must include timezone")
    delta = parsed.astimezone(timezone.utc) - _EPOCH
    observed_us = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    _positive_int(observed_us)
    producer = metrics["sample_producer_instance_id"]
    generation = metrics["sample_ffmpeg_generation"]
    if not all(
        isinstance(value, str) and value.strip() for value in (producer, generation)
    ):
        raise ValueError("missing source identity")
    if (
        type(bytes_sent) is not int
        or bytes_sent < 0
        or metrics.get("bytes_sent") != bytes_sent
    ):
        raise ValueError("counter mismatch")
    # runtime.ffmpeg_observation.v1 currently omits conn. Its counter identity
    # is producer + FFmpeg generation + PID, not a promised socket identity.
    # Retain endpoint fencing when a producer actually supplies it; never
    # invent connection evidence or reject valid v1 observations for its absence.
    connection = None
    if metrics.get("conn"):
        parts = str(metrics["conn"]).split()
        if len(parts) < 5 or parts[0] != "ESTAB":
            raise ValueError("invalid connection identity")
        connection = hashlib.sha256("\0".join(parts[3:5]).encode()).hexdigest()
    return {
        "identity": [producer, generation, _positive_int(ffmpeg_pid), connection],
        "observed_at": observed_at,
        "observed_us": observed_us,
        "sequence": _positive_int(metrics["sample_source_sequence"]),
        "consumer_monotonic_ns": _positive_int(metrics["sample_consumer_monotonic_ns"]),
        "bytes_sent": bytes_sent,
    }


def source_timed_sample(
    state: dict[str, Any],
    *,
    ffmpeg_pid: int,
    bytes_sent: int,
    metrics: dict[str, Any],
    interval_seconds: int,
) -> dict[str, Any] | None:
    """Return one auditable rate, or no sample when continuity is unproven.

    observed_at is the producer's pre-ss timestamp, not an exact packet time.
    Consumer monotonic time is only a clock-discontinuity guard, never the rate
    denominator. Missing source metadata must not fall back to consumer time.
    """
    try:
        current = _snapshot(ffmpeg_pid, bytes_sent, metrics)
    except (KeyError, TypeError, ValueError, OverflowError):
        state.pop(STATE_KEY, None)
        return None
    if interval_seconds <= 0:
        return None
    saved = state.get(STATE_KEY)
    baseline = saved.get("baseline") if isinstance(saved, dict) else None
    seen = saved.get("seen") if isinstance(saved, dict) else None
    required = set(current)
    if not all(
        isinstance(item, dict)
        and required <= item.keys()
        and all(
            type(item[key]) is int and item[key] > 0
            for key in (
                "observed_us",
                "sequence",
                "consumer_monotonic_ns",
            )
        )
        and type(item["bytes_sent"]) is int
        and item["bytes_sent"] >= 0
        for item in (baseline, seen)
    ):
        state[STATE_KEY] = {"baseline": current, "seen": current}
        return None
    try:
        same_identity = current["identity"] == baseline["identity"] == seen["identity"]
        sequence_delta = current["sequence"] - seen["sequence"]
        elapsed_us = current["observed_us"] - baseline["observed_us"]
        seen_elapsed_us = current["observed_us"] - seen["observed_us"]
        monotonic_us = (
            current["consumer_monotonic_ns"] - seen["consumer_monotonic_ns"]
        ) / 1000
        counter_regressed = current["bytes_sent"] < seen["bytes_sent"]
    except (KeyError, TypeError, ValueError):
        state[STATE_KEY] = {"baseline": current, "seen": current}
        return None
    if same_identity and sequence_delta <= 0:
        # Do not move a valid baseline backwards for a duplicate/reordered read.
        return None
    if (
        not same_identity
        or counter_regressed
        or seen_elapsed_us <= 0
        or monotonic_us <= 0
        or abs(seen_elapsed_us - monotonic_us) > 6_000_000
        or seen["bytes_sent"] < baseline["bytes_sent"]
        or seen["observed_us"] < baseline["observed_us"]
    ):
        state[STATE_KEY] = {"baseline": current, "seen": current}
        return None
    saved["seen"] = current
    if elapsed_us < interval_seconds * 1_000_000:
        return None
    bytes_delta = current["bytes_sent"] - baseline["bytes_sent"]
    payload = {
        "ffmpeg_pid": ffmpeg_pid,
        "sample_interval_sec": elapsed_us / 1_000_000,
        "bytes_sent_delta": bytes_delta,
        "bytes_sent": bytes_sent,
        "mbps": round(bytes_delta * 8 / elapsed_us, 3),
        "sample_schema_version": "ffmpeg_tcp_send_sample.v2",
        "sample_time_source": TIME_SOURCE,
        "sample_start_observed_at": baseline["observed_at"],
        "sample_end_observed_at": current["observed_at"],
        "sample_source_sequence_start": baseline["sequence"],
        "sample_source_sequence_end": current["sequence"],
        "sample_producer_instance_id": current["identity"][0],
        "sample_ffmpeg_generation": current["identity"][1],
        "sample_connection_id": current["identity"][3],
        "sample_identity_scope": (
            "socket_endpoints" if current["identity"][3] else "ffmpeg_generation"
        ),
        **{
            name: metrics.get(name, 0)
            for name in (
                "bytes_acked",
                "send_q",
                "notsent",
                "unacked",
                "lastsnd_ms",
                "rto_ms",
            )
        },
        "conn": str(metrics.get("conn") or ""),
    }
    saved["baseline"] = current
    return payload
