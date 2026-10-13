"""Persist high-cadence TCP observations without owning recovery authority."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import signal
import stat
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

CONFIG_SCHEMA = "stream_v3.tcp_stall_collector_config.v1"
MANIFEST_SCHEMA = "stream_v3.tcp_stall_collector_epoch.v1"
SAMPLE_SCHEMA = "stream_v3.tcp_stall_raw_sample.v1"
STATUS_SCHEMA = "stream_v3.tcp_stall_collector_status.v1"
SOURCE_SCHEMA = "runtime.ffmpeg_observation.v1"
DETECTOR_STATE_SCHEMA = "stream_v3.rtmps_episode_detector_state.v2"
EPISODE_SNAPSHOT_SCHEMA = "stream_v3.rtmps_episode_snapshot.v2"
EPISODE_SCHEMA = "stream_v3_rtmps_episode/v2"
_PROFILE_FIELDS = ("video_bitrate", "video_maxrate", "video_bufsize", "audio_bitrate")
_METRIC_FIELDS = ("bytes_acked", "bytes_sent", "send_q", "notsent", "unacked", "lastsnd_ms", "rto_ms")
_TARGET_FIELDS = (
    "host_id",
    "host_boot_id",
    "namespace",
    "pod_uid",
    "container_name",
    "container_id",
    "ffmpeg_generation",
)
_RUNTIME_FIELDS = (
    "host_id",
    "host_boot_id",
    "namespace",
    "pod_uid",
    "runtime_generation",
    "stream_engine_container_id",
    "stream_engine_container_name",
)
_EPISODE_RUNTIME_FIELDS = (
    "producer_instance_id",
    "runtime_generation",
    "host_boot_id",
    "pod_uid",
    "stream_engine_container_id",
)
_MAX_SOURCE_BYTES = 1_048_576


class CollectorError(RuntimeError):
    """A fail-closed collector error."""


class DuplicateKeyError(ValueError):
    """JSON object contains a duplicate key."""


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise DuplicateKeyError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def strict_json_bytes(raw: bytes) -> Any:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("JSON_NOT_UTF8") from exc
    return json.loads(text, parse_constant=_reject_constant, object_pairs_hook=_unique_object)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _utc_now(value_ns: int | None = None) -> str:
    seconds = (time.time_ns() if value_ns is None else value_ns) / 1_000_000_000
    return datetime.fromtimestamp(seconds, tz=UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc_us(value: object) -> int:
    if not isinstance(value, str) or not value:
        raise ValueError("SOURCE_TIME_MISSING")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("SOURCE_TIME_INVALID") from exc
    if parsed.tzinfo is None:
        raise ValueError("SOURCE_TIME_NAIVE")
    return int(parsed.astimezone(UTC).timestamp() * 1_000_000)


def _strict_int(value: object, *, minimum: int, reason: str) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(reason)
    return value


def _strict_text(value: object, *, reason: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(reason)
    return value


@dataclass(frozen=True)
class CollectorConfig:
    collector_id: str
    release_id: str
    source_file: Path
    expected_host_id: str
    expected_namespace: str
    expected_container_name: str
    expected_producer_id: str
    interval_seconds: float
    max_adjacent_interval_seconds: float
    stall_confirmation_seconds: float
    replacement_correlation_seconds: float
    source_max_age_seconds: int
    future_tolerance_seconds: int
    max_output_bytes: int
    stream_profile: dict[str, str]
    config_sha256: str


def load_config(path: Path) -> CollectorConfig:
    raw_bytes = path.read_bytes()
    raw = strict_json_bytes(raw_bytes)
    if not isinstance(raw, dict):
        raise CollectorError("CONFIG_NOT_OBJECT")
    expected = {
        "schema",
        "collector_id",
        "release_id",
        "source_file",
        "expected_host_id",
        "expected_namespace",
        "expected_container_name",
        "expected_producer_id",
        "interval_seconds",
        "max_adjacent_interval_seconds",
        "stall_confirmation_seconds",
        "replacement_correlation_seconds",
        "source_max_age_seconds",
        "future_tolerance_seconds",
        "max_output_bytes",
        "stream_profile",
    }
    if set(raw) != expected or raw.get("schema") != CONFIG_SCHEMA:
        raise CollectorError("CONFIG_SCHEMA_INVALID")
    interval = raw.get("interval_seconds")
    if not isinstance(interval, (int, float)) or isinstance(interval, bool) or not math.isfinite(float(interval)):
        raise CollectorError("CONFIG_INTERVAL_INVALID")
    if not 1 <= float(interval) <= 60:
        raise CollectorError("CONFIG_INTERVAL_OUT_OF_RANGE")
    maximum_adjacent = raw.get("max_adjacent_interval_seconds")
    if (
        not isinstance(maximum_adjacent, (int, float))
        or isinstance(maximum_adjacent, bool)
        or not math.isfinite(float(maximum_adjacent))
        or not float(interval) <= float(maximum_adjacent) <= 120
    ):
        raise CollectorError("CONFIG_MAX_ADJACENT_INVALID")
    confirmation = raw.get("stall_confirmation_seconds")
    if (
        not isinstance(confirmation, (int, float))
        or isinstance(confirmation, bool)
        or not math.isfinite(float(confirmation))
        or not float(interval) <= float(confirmation) <= 60
    ):
        raise CollectorError("CONFIG_STALL_CONFIRMATION_INVALID")
    correlation = raw.get("replacement_correlation_seconds")
    if (
        not isinstance(correlation, (int, float))
        or isinstance(correlation, bool)
        or not math.isfinite(float(correlation))
        or not float(confirmation) <= float(correlation) <= 120
    ):
        raise CollectorError("CONFIG_REPLACEMENT_CORRELATION_INVALID")
    max_age = _strict_int(raw.get("source_max_age_seconds"), minimum=1, reason="CONFIG_MAX_AGE_INVALID")
    future = _strict_int(raw.get("future_tolerance_seconds"), minimum=0, reason="CONFIG_FUTURE_TOLERANCE_INVALID")
    maximum = _strict_int(raw.get("max_output_bytes"), minimum=1_048_576, reason="CONFIG_MAX_OUTPUT_INVALID")
    if max_age > 300 or future > 30 or maximum > 10 * 1024**3:
        raise CollectorError("CONFIG_LIMIT_OUT_OF_RANGE")
    profile = raw.get("stream_profile")
    if not isinstance(profile, dict) or set(profile) != set(_PROFILE_FIELDS):
        raise CollectorError("CONFIG_PROFILE_INVALID")
    normalized_profile = {name: _strict_text(profile.get(name), reason="CONFIG_PROFILE_INVALID") for name in _PROFILE_FIELDS}
    return CollectorConfig(
        collector_id=_strict_text(raw.get("collector_id"), reason="CONFIG_COLLECTOR_ID_INVALID"),
        release_id=_strict_text(raw.get("release_id"), reason="CONFIG_RELEASE_ID_INVALID"),
        source_file=Path(_strict_text(raw.get("source_file"), reason="CONFIG_SOURCE_INVALID")),
        expected_host_id=_strict_text(raw.get("expected_host_id"), reason="CONFIG_HOST_INVALID"),
        expected_namespace=_strict_text(raw.get("expected_namespace"), reason="CONFIG_NAMESPACE_INVALID"),
        expected_container_name=_strict_text(raw.get("expected_container_name"), reason="CONFIG_CONTAINER_INVALID"),
        expected_producer_id=_strict_text(raw.get("expected_producer_id"), reason="CONFIG_PRODUCER_INVALID"),
        interval_seconds=float(interval),
        max_adjacent_interval_seconds=float(maximum_adjacent),
        stall_confirmation_seconds=float(confirmation),
        replacement_correlation_seconds=float(correlation),
        source_max_age_seconds=max_age,
        future_tolerance_seconds=future,
        max_output_bytes=maximum,
        stream_profile=normalized_profile,
        config_sha256=hashlib.sha256(raw_bytes).hexdigest(),
    )


def _read_regular_file(path: Path) -> tuple[bytes, os.stat_result]:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise CollectorError("SOURCE_NOT_REGULAR_FILE")
        if metadata.st_size > _MAX_SOURCE_BYTES:
            raise CollectorError("SOURCE_TOO_LARGE")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, min(65_536, _MAX_SOURCE_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > _MAX_SOURCE_BYTES:
                raise CollectorError("SOURCE_TOO_LARGE")
        return b"".join(chunks), metadata
    finally:
        os.close(fd)


def _target_projection(raw: object, *, config: CollectorConfig) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("TARGET_IDENTITY_MISSING")
    result: dict[str, Any] = {}
    for name in _TARGET_FIELDS:
        result[name] = _strict_text(raw.get(name), reason="TARGET_IDENTITY_INVALID")
    result["ffmpeg_pid"] = _strict_int(raw.get("ffmpeg_pid"), minimum=2, reason="TARGET_PID_INVALID")
    if result["host_id"] != config.expected_host_id:
        raise ValueError("TARGET_HOST_MISMATCH")
    if result["namespace"] != config.expected_namespace:
        raise ValueError("TARGET_NAMESPACE_MISMATCH")
    if result["container_name"] != config.expected_container_name:
        raise ValueError("TARGET_CONTAINER_MISMATCH")
    return result


def _runtime_projection(raw: object, *, config: CollectorConfig) -> dict[str, str]:
    if not isinstance(raw, dict):
        raise ValueError("RUNTIME_IDENTITY_MISSING")
    result = {name: _strict_text(raw.get(name), reason="RUNTIME_IDENTITY_INVALID") for name in _RUNTIME_FIELDS}
    if result["host_id"] != config.expected_host_id:
        raise ValueError("RUNTIME_HOST_MISMATCH")
    if result["namespace"] != config.expected_namespace:
        raise ValueError("RUNTIME_NAMESPACE_MISMATCH")
    if result["stream_engine_container_name"] != config.expected_container_name:
        raise ValueError("RUNTIME_CONTAINER_MISMATCH")
    return result


def _metric_projection(raw: object) -> dict[str, int]:
    if not isinstance(raw, dict):
        raise ValueError("TCP_METRICS_MISSING")
    return {name: _strict_int(raw.get(name), minimum=0, reason=f"TCP_{name.upper()}_INVALID") for name in _METRIC_FIELDS}


def project_source(raw: object, *, now_us: int, config: CollectorConfig) -> tuple[dict[str, Any], str, str, bool]:
    """Return allowlisted source, source status, reason and rate eligibility."""
    if not isinstance(raw, dict) or raw.get("schema_version") != SOURCE_SCHEMA:
        raise ValueError("SOURCE_SCHEMA_INVALID")
    observed_at = _strict_text(raw.get("observed_at"), reason="SOURCE_TIME_MISSING")
    observed_us = _parse_utc_us(observed_at)
    age_us = now_us - observed_us
    source_status = "VALID"
    reason = "SOURCE_VALID"
    if age_us < -config.future_tolerance_seconds * 1_000_000:
        source_status, reason = "FUTURE", "SOURCE_TIME_FUTURE"
    elif age_us > config.source_max_age_seconds * 1_000_000:
        source_status, reason = "STALE", "SOURCE_TOO_OLD"
    sequence = _strict_int(raw.get("sequence"), minimum=1, reason="SOURCE_SEQUENCE_INVALID")
    producer_id = _strict_text(raw.get("producer_id"), reason="SOURCE_PRODUCER_INVALID")
    if producer_id != config.expected_producer_id:
        raise ValueError("SOURCE_PRODUCER_MISMATCH")
    producer_instance_id = _strict_text(raw.get("producer_instance_id"), reason="SOURCE_PRODUCER_INVALID")
    source_generation = _strict_text(raw.get("ffmpeg_generation"), reason="SOURCE_GENERATION_INVALID")
    ffmpeg_running = raw.get("ffmpeg_running")
    if type(ffmpeg_running) is not bool:
        raise ValueError("SOURCE_RUNNING_STATE_INVALID")
    production_behavior_modified = raw.get("production_behavior_modified")
    if type(production_behavior_modified) is not bool:
        raise ValueError("SOURCE_PRODUCTION_BEHAVIOR_INVALID")
    in_flight_count = _strict_int(raw.get("in_flight_count"), minimum=0, reason="SOURCE_IN_FLIGHT_INVALID")
    projection: dict[str, Any] = {
        "schema_version": SOURCE_SCHEMA,
        "observation_id": str(raw.get("observation_id") or ""),
        "observed_at": observed_at,
        "observed_us": observed_us,
        "sequence": sequence,
        "producer_id": producer_id,
        "producer_instance_id": producer_instance_id,
        "source_ffmpeg_generation": source_generation,
        "ffmpeg_running": ffmpeg_running,
        "ffmpeg_uptime_sec": raw.get("ffmpeg_uptime_sec") if type(raw.get("ffmpeg_uptime_sec")) is int else None,
        "in_flight_count": in_flight_count,
        "production_behavior_modified": production_behavior_modified,
        "runtime_lifecycle_state": str(raw.get("runtime_lifecycle_state") or ""),
        "runtime_snapshot_id": str(raw.get("runtime_snapshot_id") or ""),
        "runtime_snapshot_status": str(raw.get("runtime_snapshot_status") or ""),
        "target_snapshot_id": str(raw.get("target_snapshot_id") or ""),
        "target_snapshot_status": str(raw.get("target_snapshot_status") or ""),
        "stream_established": raw.get("stream_established") if type(raw.get("stream_established")) is bool else None,
    }
    rate_eligible = False
    try:
        target = _target_projection(raw.get("target_identity"), config=config)
        runtime = _runtime_projection(raw.get("runtime_identity"), config=config)
        metrics = _metric_projection(raw.get("tcp_metrics"))
        protocol_pid = _strict_int(raw.get("protocol_ffmpeg_pid"), minimum=2, reason="PROTOCOL_PID_INVALID")
        if target["ffmpeg_pid"] != protocol_pid:
            raise ValueError("TARGET_PID_MISMATCH")
        if (
            target["host_id"] != runtime["host_id"]
            or target["host_boot_id"] != runtime["host_boot_id"]
            or target["namespace"] != runtime["namespace"]
            or target["pod_uid"] != runtime["pod_uid"]
            or target["container_name"] != runtime["stream_engine_container_name"]
            or target["container_id"] != runtime["stream_engine_container_id"]
        ):
            raise ValueError("TARGET_RUNTIME_IDENTITY_MISMATCH")
        projection["protocol_ffmpeg_pid"] = protocol_pid
        projection["target_identity"] = target
        projection["runtime_identity"] = runtime
        projection["tcp_metrics"] = metrics
        projection["identity_sha256"] = _sha256(
            {
                "producer_instance_id": producer_instance_id,
                "source_ffmpeg_generation": source_generation,
                "protocol_ffmpeg_pid": protocol_pid,
                "target_identity": target,
                "runtime_identity": runtime,
            }
        )
        rate_eligible = bool(
            source_status == "VALID"
            and ffmpeg_running
            and raw.get("stream_established") is True
            and raw.get("runtime_snapshot_status") == "VALID"
            and raw.get("target_snapshot_status") == "VALID"
        )
        if source_status == "VALID" and not rate_eligible:
            reason = "SOURCE_NOT_RATE_ELIGIBLE"
    except ValueError as exc:
        projection["rate_projection_error"] = str(exc)
        if source_status == "VALID":
            reason = "SOURCE_NOT_RATE_ELIGIBLE"
    projection["source_age_sec"] = round(age_us / 1_000_000, 6)
    return projection, source_status, reason, rate_eligible


def classify_adjacent(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
    *,
    max_elapsed_seconds: float = 15,
) -> tuple[str, dict[str, Any] | None]:
    if previous is None:
        return "BASELINE", None
    previous_metrics = previous["tcp_metrics"]
    current_metrics = current["tcp_metrics"]
    duplicate = (
        current["identity_sha256"] == previous["identity_sha256"]
        and current["sequence"] == previous["sequence"]
        and current["observed_us"] == previous["observed_us"]
        and current_metrics == previous_metrics
    )
    if duplicate:
        return "DUPLICATE_SOURCE", None
    if current["identity_sha256"] != previous["identity_sha256"]:
        return "IDENTITY_CHANGED", None
    if current["sequence"] <= previous["sequence"]:
        return "SEQUENCE_REGRESSION", None
    elapsed_us = current["observed_us"] - previous["observed_us"]
    if elapsed_us <= 0:
        return "SOURCE_TIME_REGRESSION", None
    if elapsed_us > max_elapsed_seconds * 1_000_000:
        return "COLLECTOR_GAP", None
    ack_delta = current_metrics["bytes_acked"] - previous_metrics["bytes_acked"]
    sent_delta = current_metrics["bytes_sent"] - previous_metrics["bytes_sent"]
    if ack_delta < 0 or sent_delta < 0:
        return "COUNTER_REGRESSION", None
    adjacent = {
        "elapsed_sec": round(elapsed_us / 1_000_000, 6),
        "sequence_delta": current["sequence"] - previous["sequence"],
        "bytes_acked_delta": ack_delta,
        "bytes_sent_delta": sent_delta,
        "ack_mbps": round(ack_delta * 8 / elapsed_us, 6),
        "send_mbps": round(sent_delta * 8 / elapsed_us, 6),
    }
    return "CONTINUOUS", adjacent


def _target_source_hash(source: dict[str, Any]) -> str:
    """Match cra_no_action_soak.recovery_facts.source_hash for scalar identity fields."""
    return _sha256({"raw": source["target_identity"]})


def _same_runtime(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return all(
        left.get(name) == right.get(name)
        for name in (
            "producer_instance_id",
            "runtime_generation",
            "host_boot_id",
            "pod_uid",
            "stream_engine_container_id",
        )
    )


def _runtime_capsule(source: dict[str, Any]) -> dict[str, str]:
    runtime = source["runtime_identity"]
    return {
        "producer_instance_id": source["producer_instance_id"],
        "runtime_generation": runtime["runtime_generation"],
        "host_boot_id": runtime["host_boot_id"],
        "pod_uid": runtime["pod_uid"],
        "stream_engine_container_id": runtime["stream_engine_container_id"],
    }


def _stall_signal(source: dict[str, Any], adjacent: dict[str, Any] | None) -> bool:
    """A zero ACK interval needs an independently advancing source and pressure or zero writes."""
    if adjacent is None or adjacent.get("bytes_acked_delta") != 0:
        return False
    metrics = source["tcp_metrics"]
    return bool(
        adjacent.get("bytes_sent_delta") == 0
        or metrics["send_q"] > 0
        or metrics["notsent"] > 0
        or metrics["unacked"] > 0
        or metrics["lastsnd_ms"] >= 5_000
        or metrics["rto_ms"] >= 1_000
    )


def _episode_reason_code(reason: str) -> str:
    """Keep durable classifications bounded even when an exception message is free-form."""
    return reason if re.fullmatch(r"[A-Z][A-Z0-9_]{1,63}", reason) else "SOURCE_INVALID"


def _atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = _canonical_bytes(value) + b"\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(temporary, flags, 0o640)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise CollectorError("ATOMIC_WRITE_FAILED")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _append_jsonl(path: Path, value: object, *, maximum_bytes: int) -> int:
    payload = _canonical_bytes(value) + b"\n"
    current_size = path.stat().st_size if path.exists() else 0
    if current_size + len(payload) > maximum_bytes:
        raise CollectorError("OUTPUT_LIMIT_REACHED")
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o640)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise CollectorError("APPEND_FAILED")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)
    return current_size + len(payload)


def _read_last_row(path: Path) -> dict[str, Any] | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    with path.open("rb") as stream:
        size = stream.seek(0, os.SEEK_END)
        start = max(0, size - _MAX_SOURCE_BYTES)
        stream.seek(start)
        tail = stream.read()
    lines = tail.splitlines()
    if start > 0 and lines:
        lines = lines[1:]
    if not lines:
        raise CollectorError("SAMPLE_TAIL_UNREADABLE")
    value = strict_json_bytes(lines[-1])
    if not isinstance(value, dict) or value.get("schema") != SAMPLE_SCHEMA:
        raise CollectorError("SAMPLE_TAIL_INVALID")
    return value


def _valid_detector_candidate(value: object) -> bool:
    if value is None:
        return True
    fields = {
        "runtime",
        "identity_sha256",
        "target_sha256",
        "detection_basis",
        "trigger_reason_code",
        "started_at",
        "detected_at",
        "first_collector_sequence",
        "last_collector_sequence",
        "zero_ack_interval_count",
    }
    if not isinstance(value, dict) or set(value) != fields:
        return False
    runtime = value.get("runtime")
    if (
        not isinstance(runtime, dict)
        or set(runtime) != set(_EPISODE_RUNTIME_FIELDS)
        or any(not isinstance(runtime.get(field), str) or not runtime[field] for field in _EPISODE_RUNTIME_FIELDS)
        or any(
            not isinstance(value.get(field), str) or re.fullmatch(r"[0-9a-f]{64}", value[field]) is None
            for field in ("identity_sha256", "target_sha256")
        )
        or any(type(value.get(field)) is not int or value[field] < 1 for field in ("first_collector_sequence", "last_collector_sequence"))
        or type(value.get("zero_ack_interval_count")) is not int
        or value["zero_ack_interval_count"] < 0
        or value["last_collector_sequence"] < value["first_collector_sequence"]
    ):
        return False
    basis = value.get("detection_basis")
    reason = value.get("trigger_reason_code")
    if (
        basis not in {"ACK_STALL", "SESSION_CONTINUITY_LOSS"}
        or not isinstance(reason, str)
        or re.fullmatch(r"[A-Z][A-Z0-9_]{1,63}", reason) is None
        or basis == "ACK_STALL"
        and (value["zero_ack_interval_count"] < 1 or reason != "ZERO_ACK_WITH_SEND_PRESSURE")
        or basis == "SESSION_CONTINUITY_LOSS"
        and (value["zero_ack_interval_count"] != 0 or reason == "SOURCE_VALID")
    ):
        return False
    try:
        return _parse_utc_us(value["started_at"]) <= _parse_utc_us(value["detected_at"])
    except ValueError:
        return False


def _valid_detector_episode(value: object) -> bool:
    if value is None:
        return True
    required = {
        "schema",
        "episode_id",
        "sequence",
        *_EPISODE_RUNTIME_FIELDS,
        "state",
        "classification",
        "detection_basis",
        "trigger_reason_code",
        "started_at",
        "detected_at",
        "last_observed_failure_at",
        "recovered_at",
        "target_sha256",
        "replacement_target_sha256",
        "first_collector_sequence",
        "last_collector_sequence",
        "zero_ack_interval_count",
    }
    if not isinstance(value, dict) or set(value) != required or value.get("schema") != EPISODE_SCHEMA:
        return False
    if (
        any(not isinstance(value.get(field), str) or not value[field] for field in _EPISODE_RUNTIME_FIELDS)
        or type(value.get("sequence")) is not int
        or value["sequence"] < 1
        or any(type(value.get(field)) is not int or value[field] < 1 for field in ("first_collector_sequence", "last_collector_sequence"))
        or type(value.get("zero_ack_interval_count")) is not int
        or value["zero_ack_interval_count"] < 0
        or value["last_collector_sequence"] < value["first_collector_sequence"]
        or not isinstance(value.get("target_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", value["target_sha256"]) is None
    ):
        return False
    state = value.get("state")
    classification = value.get("classification")
    basis = value.get("detection_basis")
    reason = value.get("trigger_reason_code")
    replacement = value.get("replacement_target_sha256")
    recovered_at = value.get("recovered_at")
    if (
        basis not in {"ACK_STALL", "SESSION_CONTINUITY_LOSS"}
        or not isinstance(reason, str)
        or re.fullmatch(r"[A-Z][A-Z0-9_]{1,63}", reason) is None
        or basis == "ACK_STALL"
        and (value["zero_ack_interval_count"] < 1 or reason != "ZERO_ACK_WITH_SEND_PRESSURE")
        or basis == "SESSION_CONTINUITY_LOSS"
        and (value["zero_ack_interval_count"] != 0 or reason == "SOURCE_VALID")
    ):
        return False
    if state == "ACTIVE":
        expected_active = "RTMPS_ACK_STALL" if basis == "ACK_STALL" else "FFMPEG_SESSION_LOSS"
        if classification != expected_active or replacement is not None or recovered_at is not None:
            return False
    elif state == "RECOVERED":
        if (
            classification
            not in {
                "RTMPS_ACK_STALL_FFMPEG_REPLACED",
                "RTMPS_ACK_STALL_RECOVERED_SAME_TARGET",
                "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED",
            }
            or not isinstance(replacement, str)
            or re.fullmatch(r"[0-9a-f]{64}", replacement) is None
            or not isinstance(recovered_at, str)
            or classification in {"RTMPS_ACK_STALL_FFMPEG_REPLACED", "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED"}
            and replacement == value["target_sha256"]
            or classification == "RTMPS_ACK_STALL_RECOVERED_SAME_TARGET"
            and replacement != value["target_sha256"]
            or basis == "SESSION_CONTINUITY_LOSS"
            and classification != "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED"
            or basis == "ACK_STALL"
            and classification == "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED"
        ):
            return False
    else:
        return False
    try:
        started_us = _parse_utc_us(value.get("started_at"))
        detected_us = _parse_utc_us(value.get("detected_at"))
        failed_us = _parse_utc_us(value.get("last_observed_failure_at"))
        recovered_us = _parse_utc_us(recovered_at) if recovered_at is not None else None
    except ValueError:
        return False
    expected_id = hashlib.sha256(
        f"{value['host_boot_id']}:{value['sequence']}:{value['started_at']}:{value['target_sha256']}".encode()
    ).hexdigest()[:32]
    return bool(
        isinstance(value.get("episode_id"), str)
        and re.fullmatch(r"[0-9a-f]{32}", value["episode_id"]) is not None
        and value["episode_id"] == expected_id
        and started_us <= detected_us <= failed_us
        and (recovered_us is None or failed_us <= recovered_us)
    )


def _detector_state_consistent(candidate: object, episode: object, episode_sequence: int) -> bool:
    if episode is None:
        return episode_sequence == 0
    if not isinstance(episode, dict) or episode.get("sequence") != episode_sequence:
        return False
    if episode.get("state") == "RECOVERED":
        if candidate is None:
            return True
        if not isinstance(candidate, dict):
            return False
        try:
            return bool(
                candidate["first_collector_sequence"] > episode["last_collector_sequence"]
                and _parse_utc_us(candidate["started_at"]) >= _parse_utc_us(episode["recovered_at"])
                and _parse_utc_us(candidate["detected_at"]) > _parse_utc_us(episode["recovered_at"])
            )
        except (KeyError, TypeError, ValueError):
            return False
    if not isinstance(candidate, dict) or not isinstance(candidate.get("runtime"), dict):
        return False
    return bool(
        all(episode.get(field) == candidate["runtime"].get(field) for field in _EPISODE_RUNTIME_FIELDS)
        and episode.get("target_sha256") == candidate.get("target_sha256")
        and episode.get("started_at") == candidate.get("started_at")
        and episode.get("detected_at") == candidate.get("detected_at")
        and episode.get("first_collector_sequence") == candidate.get("first_collector_sequence")
        and episode.get("detection_basis") == candidate.get("detection_basis")
        and episode.get("trigger_reason_code") == candidate.get("trigger_reason_code")
    )


class Collector:
    def __init__(self, config: CollectorConfig, *, epoch_id: str, output_dir: Path) -> None:
        if not epoch_id or any(
            character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for character in epoch_id
        ):
            raise CollectorError("EPOCH_ID_INVALID")
        if config.release_id != epoch_id:
            raise CollectorError("RELEASE_EPOCH_MISMATCH")
        self.config = config
        self.epoch_id = epoch_id
        self.output_dir = output_dir
        self.samples_path = output_dir / "samples.jsonl"
        self.status_path = output_dir / "status.json"
        self.manifest_path = output_dir / "epoch_manifest.json"
        self.detector_state_path = output_dir / "detector_state.json"
        self.episode_snapshot_path = output_dir / "rtmps_episode.json"
        self.sequence = 0
        self.previous_rate_point: dict[str, Any] | None = None
        self.episode_sequence = 0
        self.candidate: dict[str, Any] | None = None
        self.latest_episode: dict[str, Any] | None = None
        self.counts = {"total": 0, "valid_source": 0, "rate_eligible": 0, "continuous": 0, "duplicate": 0, "censored": 0, "invalid": 0}
        self._initialize()

    def _initialize(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        profile_sha256 = _sha256(self.config.stream_profile)
        immutable = {
            "schema": MANIFEST_SCHEMA,
            "collector_id": self.config.collector_id,
            "release_id": self.config.release_id,
            "epoch_id": self.epoch_id,
            "config_sha256": self.config.config_sha256,
            "source_file": str(self.config.source_file),
            "source_schema": SOURCE_SCHEMA,
            "expected_host_id": self.config.expected_host_id,
            "expected_namespace": self.config.expected_namespace,
            "expected_container_name": self.config.expected_container_name,
            "expected_producer_id": self.config.expected_producer_id,
            "interval_seconds": self.config.interval_seconds,
            "max_adjacent_interval_seconds": self.config.max_adjacent_interval_seconds,
            "stall_confirmation_seconds": self.config.stall_confirmation_seconds,
            "replacement_correlation_seconds": self.config.replacement_correlation_seconds,
            "source_max_age_seconds": self.config.source_max_age_seconds,
            "future_tolerance_seconds": self.config.future_tolerance_seconds,
            "max_output_bytes": self.config.max_output_bytes,
            "stream_profile": self.config.stream_profile,
            "stream_profile_sha256": profile_sha256,
            "authority_boundary": {
                "network_access": False,
                "recovery_action_access": False,
                "effect_socket_access": False,
                "controller_state_access": False,
                "source_producer_is_independent": False,
                "source_producer_note": (
                    "runtime observation is produced by the stream-engine effect executor; this collector is permission-separated"
                ),
            },
        }
        if self.manifest_path.exists():
            existing = strict_json_bytes(self.manifest_path.read_bytes())
            if not isinstance(existing, dict) or {key: existing.get(key) for key in immutable} != immutable:
                raise CollectorError("EPOCH_MANIFEST_MISMATCH")
        else:
            _atomic_json(self.manifest_path, {**immutable, "created_at": _utc_now()})
        last = _read_last_row(self.samples_path)
        if last is not None:
            sequence = last.get("collector_sequence")
            if type(sequence) is not int or sequence < 1 or last.get("epoch_id") != self.epoch_id:
                raise CollectorError("SAMPLE_TAIL_IDENTITY_INVALID")
            self.sequence = sequence
            if (
                last.get("rate_eligible") is True
                and last.get("source_status") == "VALID"
                and last.get("censor_reason") != "RECOVERY_EFFECT_IN_FLIGHT"
                and isinstance(last.get("source"), dict)
            ):
                self.previous_rate_point = last["source"]
        if self.status_path.exists():
            try:
                saved = strict_json_bytes(self.status_path.read_bytes())
                if isinstance(saved, dict) and saved.get("epoch_id") == self.epoch_id and isinstance(saved.get("counts"), dict):
                    for name in self.counts:
                        value = saved["counts"].get(name)
                        if type(value) is int and value >= 0:
                            self.counts[name] = value
            except (OSError, ValueError):
                pass
        if self.detector_state_path.exists():
            saved = strict_json_bytes(self.detector_state_path.read_bytes())
            if (
                not isinstance(saved, dict)
                or set(saved) != {"schema", "release_id", "epoch_id", "episode_sequence", "candidate", "latest_episode"}
                or saved.get("schema") != DETECTOR_STATE_SCHEMA
                or saved.get("release_id") != self.config.release_id
                or saved.get("epoch_id") != self.epoch_id
                or type(saved.get("episode_sequence")) is not int
                or saved["episode_sequence"] < 0
                or not _valid_detector_candidate(saved.get("candidate"))
                or not _valid_detector_episode(saved.get("latest_episode"))
                or not _detector_state_consistent(
                    saved.get("candidate"),
                    saved.get("latest_episode"),
                    saved["episode_sequence"],
                )
            ):
                raise CollectorError("DETECTOR_STATE_INVALID")
            self.episode_sequence = saved["episode_sequence"]
            self.candidate = saved["candidate"]
            self.latest_episode = saved["latest_episode"]

    @staticmethod
    def _episode_id(host_boot_id: str, sequence: int, started_at: str, target_sha256: str) -> str:
        return hashlib.sha256(f"{host_boot_id}:{sequence}:{started_at}:{target_sha256}".encode()).hexdigest()[:32]

    def _start_candidate(
        self,
        source: dict[str, Any],
        *,
        collector_sequence: int,
        detection_basis: str = "ACK_STALL",
        trigger_reason_code: str = "ZERO_ACK_WITH_SEND_PRESSURE",
        detected_at: str | None = None,
    ) -> None:
        observed_at = detected_at or source["observed_at"]
        self.candidate = {
            "runtime": _runtime_capsule(source),
            "identity_sha256": source["identity_sha256"],
            "target_sha256": _target_source_hash(source),
            "detection_basis": detection_basis,
            "trigger_reason_code": trigger_reason_code,
            "started_at": observed_at,
            "detected_at": observed_at,
            "first_collector_sequence": collector_sequence,
            "last_collector_sequence": collector_sequence,
            "zero_ack_interval_count": 1 if detection_basis == "ACK_STALL" else 0,
        }

    def _confirm_candidate(self, source: dict[str, Any]) -> None:
        assert self.candidate is not None
        self.episode_sequence += 1
        started_at = self.candidate["started_at"]
        target_sha256 = self.candidate["target_sha256"]
        self.latest_episode = {
            "schema": EPISODE_SCHEMA,
            "episode_id": self._episode_id(
                self.candidate["runtime"]["host_boot_id"],
                self.episode_sequence,
                started_at,
                target_sha256,
            ),
            "sequence": self.episode_sequence,
            **self.candidate["runtime"],
            "state": "ACTIVE",
            "classification": ("RTMPS_ACK_STALL" if self.candidate["detection_basis"] == "ACK_STALL" else "FFMPEG_SESSION_LOSS"),
            "detection_basis": self.candidate["detection_basis"],
            "trigger_reason_code": self.candidate["trigger_reason_code"],
            "started_at": started_at,
            "detected_at": self.candidate["detected_at"],
            "last_observed_failure_at": (
                source["observed_at"] if self.candidate["detection_basis"] == "ACK_STALL" else self.candidate["detected_at"]
            ),
            "recovered_at": None,
            "target_sha256": target_sha256,
            "replacement_target_sha256": None,
            "first_collector_sequence": self.candidate["first_collector_sequence"],
            "last_collector_sequence": self.sequence,
            "zero_ack_interval_count": self.candidate["zero_ack_interval_count"],
        }

    def _recover_episode(self, source: dict[str, Any], *, classification: str) -> None:
        assert self.latest_episode is not None
        self.latest_episode.update(
            state="RECOVERED",
            classification=classification,
            recovered_at=source["observed_at"],
            replacement_target_sha256=_target_source_hash(source),
            last_collector_sequence=self.sequence,
        )
        self.candidate = None

    def _advance_episode_detector(
        self,
        source: dict[str, Any] | None,
        *,
        rate_eligible: bool,
        continuity: str,
        adjacent: dict[str, Any] | None,
        collector_observed_at: str,
        previous_rate_point: dict[str, Any] | None,
        source_status: str,
        reason_code: str,
    ) -> None:
        active = self.latest_episode if self.latest_episode and self.latest_episode.get("state") == "ACTIVE" else None
        if active is not None and self.candidate is None:
            raise CollectorError("DETECTOR_STATE_INVALID")
        # The correlation deadline applies only before an episode is confirmed.
        # Once ACTIVE, the candidate is the durable identity anchor until the
        # episode is recovered; expiring it would make persisted state invalid.
        if self.candidate is not None and active is None:
            elapsed = (_parse_utc_us(collector_observed_at) - _parse_utc_us(self.candidate["detected_at"])) / 1_000_000
            if elapsed > self.config.replacement_correlation_seconds:
                self.candidate = None
        if source is None or not rate_eligible:
            previous_rate_point_age_seconds = (
                (_parse_utc_us(collector_observed_at) - _parse_utc_us(previous_rate_point["observed_at"])) / 1_000_000
                if previous_rate_point is not None
                else None
            )
            session_loss_trigger = (
                source_status == "VALID"
                and reason_code == "SOURCE_NOT_RATE_ELIGIBLE"
                or source_status == "INVALID"
                and reason_code == "SOURCE_GENERATION_INVALID"
            )
            if (
                self.candidate is None
                and previous_rate_point is not None
                and previous_rate_point_age_seconds is not None
                and 0 <= previous_rate_point_age_seconds <= self.config.max_adjacent_interval_seconds
                and previous_rate_point.get("production_behavior_modified") is False
                and previous_rate_point.get("in_flight_count") == 0
                and session_loss_trigger
                and (
                    source is None
                    or source.get("production_behavior_modified") is False
                    and source.get("in_flight_count") == 0
                )
            ):
                self._start_candidate(
                    previous_rate_point,
                    collector_sequence=self.sequence,
                    detection_basis="SESSION_CONTINUITY_LOSS",
                    trigger_reason_code=_episode_reason_code(reason_code if reason_code != "SOURCE_VALID" else "SOURCE_NOT_RATE_ELIGIBLE"),
                    detected_at=collector_observed_at,
                )
            elif self.candidate is not None and self.candidate["detection_basis"] == "SESSION_CONTINUITY_LOSS":
                self.candidate["last_collector_sequence"] = self.sequence
            return
        runtime = _runtime_capsule(source)
        if active is not None:
            # The detector-state guard above proves this for the complete
            # synchronous transition.  Keep one reachable invariant check so
            # branch qualification does not require an impossible mutation of
            # ``self.candidate`` midway through this method.
            candidate = cast(dict[str, Any], self.candidate)
            if not _same_runtime(active, runtime):
                return
            if source["identity_sha256"] != candidate["identity_sha256"]:
                classification = (
                    "RTMPS_ACK_STALL_FFMPEG_REPLACED"
                    if candidate["detection_basis"] == "ACK_STALL"
                    else "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED"
                )
                self._recover_episode(source, classification=classification)
            elif adjacent is not None and adjacent.get("bytes_acked_delta", 0) > 0:
                self._recover_episode(source, classification="RTMPS_ACK_STALL_RECOVERED_SAME_TARGET")
            elif _stall_signal(source, adjacent):
                active["last_observed_failure_at"] = source["observed_at"]
                active["last_collector_sequence"] = self.sequence
                active["zero_ack_interval_count"] += 1
            return

        if self.candidate is not None:
            if not _same_runtime(self.candidate["runtime"], runtime):
                self.candidate = None
                return
            if source["identity_sha256"] != self.candidate["identity_sha256"]:
                self._confirm_candidate(source)
                classification = (
                    "RTMPS_ACK_STALL_FFMPEG_REPLACED"
                    if self.candidate["detection_basis"] == "ACK_STALL"
                    else "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED"
                )
                self._recover_episode(source, classification=classification)
                return
            if self.candidate["detection_basis"] == "SESSION_CONTINUITY_LOSS":
                self.candidate = None
                return
            if _stall_signal(source, adjacent):
                self.candidate["last_collector_sequence"] = self.sequence
                self.candidate["zero_ack_interval_count"] += 1
                elapsed = (_parse_utc_us(source["observed_at"]) - _parse_utc_us(self.candidate["detected_at"])) / 1_000_000
                if elapsed >= self.config.stall_confirmation_seconds:
                    self._confirm_candidate(source)
                return
            if adjacent is not None and adjacent.get("bytes_acked_delta", 0) > 0:
                self.candidate = None
                return
        if continuity == "CONTINUOUS" and _stall_signal(source, adjacent):
            self._start_candidate(source, collector_sequence=self.sequence)

    def _detector_state_payload(self) -> dict[str, Any]:
        if (
            not _valid_detector_candidate(self.candidate)
            or not _valid_detector_episode(self.latest_episode)
            or not _detector_state_consistent(self.candidate, self.latest_episode, self.episode_sequence)
        ):
            raise CollectorError("DETECTOR_STATE_INVALID")
        return {
            "schema": DETECTOR_STATE_SCHEMA,
            "release_id": self.config.release_id,
            "epoch_id": self.epoch_id,
            "episode_sequence": self.episode_sequence,
            "candidate": self.candidate,
            "latest_episode": self.latest_episode,
        }

    def _persist_episode_state(self, row: dict[str, Any]) -> None:
        _atomic_json(self.detector_state_path, self._detector_state_payload())
        _atomic_json(
            self.episode_snapshot_path,
            {
                "schema": EPISODE_SNAPSHOT_SCHEMA,
                "collector_id": self.config.collector_id,
                "release_id": self.config.release_id,
                "epoch_id": self.epoch_id,
                "observed_at": row["collector_observed_at"],
                "collector_sequence": self.sequence,
                "source_status": row["source_status"],
                "source_observed_at": row["source"]["observed_at"] if isinstance(row.get("source"), dict) else None,
                "reason_code": row["reason_code"],
                "episode_sequence": self.episode_sequence,
                "episode": self.latest_episode,
            },
        )

    def collect_once(self, *, wall_time_ns: int | None = None, monotonic_ns: int | None = None) -> dict[str, Any]:
        wall_ns = time.time_ns() if wall_time_ns is None else wall_time_ns
        mono_ns = time.monotonic_ns() if monotonic_ns is None else monotonic_ns
        now_us = wall_ns // 1000
        source: dict[str, Any] | None = None
        source_status = "INVALID"
        reason = "SOURCE_READ_ERROR"
        source_sha256: str | None = None
        source_mtime_ns: int | None = None
        rate_eligible = False
        try:
            raw_bytes, metadata = _read_regular_file(self.config.source_file)
            source_sha256 = hashlib.sha256(raw_bytes).hexdigest()
            source_mtime_ns = metadata.st_mtime_ns
            raw = strict_json_bytes(raw_bytes)
            source, source_status, reason, rate_eligible = project_source(raw, now_us=now_us, config=self.config)
        except FileNotFoundError:
            source_status, reason = "UNAVAILABLE", "SOURCE_FILE_MISSING"
        except PermissionError:
            source_status, reason = "UNAVAILABLE", "SOURCE_PERMISSION_DENIED"
        except (CollectorError, DuplicateKeyError, OSError, TypeError, ValueError) as exc:
            source_status = "INVALID"
            reason = str(exc) or type(exc).__name__

        continuity = "NOT_RATE_ELIGIBLE"
        adjacent: dict[str, Any] | None = None
        previous_rate_point = self.previous_rate_point
        if rate_eligible and source is not None:
            continuity, adjacent = classify_adjacent(
                self.previous_rate_point,
                source,
                max_elapsed_seconds=self.config.max_adjacent_interval_seconds,
            )
            if continuity != "DUPLICATE_SOURCE":
                self.previous_rate_point = source
        else:
            self.previous_rate_point = None

        censor_reason: str | None = None
        if source_status != "VALID":
            censor_reason = reason
        elif source is not None and source["production_behavior_modified"]:
            censor_reason = "PRODUCTION_BEHAVIOR_MODIFIED"
        elif not rate_eligible:
            censor_reason = "SOURCE_NOT_RATE_ELIGIBLE"
        elif source is not None and source["in_flight_count"] > 0:
            censor_reason = "RECOVERY_EFFECT_IN_FLIGHT"
        elif continuity != "CONTINUOUS":
            censor_reason = continuity
        analysis_eligible = censor_reason is None
        if censor_reason in {"RECOVERY_EFFECT_IN_FLIGHT", "PRODUCTION_BEHAVIOR_MODIFIED"}:
            # Never derive a later rate across an intervention boundary.
            self.previous_rate_point = None

        self.sequence += 1
        collector_observed_at = _utc_now(wall_ns)
        row = {
            "schema": SAMPLE_SCHEMA,
            "collector_id": self.config.collector_id,
            "release_id": self.config.release_id,
            "epoch_id": self.epoch_id,
            "collector_sequence": self.sequence,
            "collector_observed_at": collector_observed_at,
            "collector_monotonic_ns": mono_ns,
            "config_sha256": self.config.config_sha256,
            "stream_profile_sha256": _sha256(self.config.stream_profile),
            "source_path": str(self.config.source_file),
            "source_file_mtime_ns": source_mtime_ns,
            "source_sha256": source_sha256,
            "source_status": source_status,
            "reason_code": reason,
            "rate_eligible": rate_eligible,
            "continuity": continuity,
            "adjacent_rate": adjacent,
            "analysis_eligible": analysis_eligible,
            "censor_reason": censor_reason,
            "source": source,
        }
        self._advance_episode_detector(
            source,
            rate_eligible=rate_eligible,
            continuity=continuity,
            adjacent=adjacent,
            collector_observed_at=collector_observed_at,
            previous_rate_point=previous_rate_point,
            source_status=source_status,
            reason_code=reason,
        )
        # A detector failure must not leave an uncommitted raw tail behind.
        self._detector_state_payload()
        new_size = _append_jsonl(self.samples_path, row, maximum_bytes=self.config.max_output_bytes)
        self.counts["total"] += 1
        if source_status == "VALID":
            self.counts["valid_source"] += 1
        else:
            self.counts["invalid"] += 1
        if rate_eligible:
            self.counts["rate_eligible"] += 1
        if continuity == "CONTINUOUS":
            self.counts["continuous"] += 1
        if continuity == "DUPLICATE_SOURCE":
            self.counts["duplicate"] += 1
        if censor_reason is not None:
            self.counts["censored"] += 1
        self._persist_episode_state(row)
        _atomic_json(
            self.status_path,
            {
                "schema": STATUS_SCHEMA,
                "collector_id": self.config.collector_id,
                "release_id": self.config.release_id,
                "epoch_id": self.epoch_id,
                "status": "RUNNING",
                "last_sample_at": row["collector_observed_at"],
                "last_collector_sequence": self.sequence,
                "last_source_status": source_status,
                "last_reason_code": reason,
                "last_continuity": continuity,
                "last_analysis_eligible": analysis_eligible,
                "samples_bytes": new_size,
                "max_output_bytes": self.config.max_output_bytes,
                "counts": self.counts,
                "episode_sequence": self.episode_sequence,
                "episode_state": self.latest_episode.get("state") if self.latest_episode else "NONE",
            },
        )
        return row

    def mark_stopped(self) -> None:
        if not self.status_path.exists():
            return
        try:
            status = strict_json_bytes(self.status_path.read_bytes())
        except (OSError, ValueError):
            return
        if isinstance(status, dict) and status.get("epoch_id") == self.epoch_id:
            status["status"] = "STOPPED"
            status["stopped_at"] = _utc_now()
            _atomic_json(self.status_path, status)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--epoch-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    collector = Collector(load_config(args.config), epoch_id=args.epoch_id, output_dir=args.output_dir)
    if args.once:
        collector.collect_once()
        return 0
    stopped = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        while not stopped.is_set():
            started = time.monotonic()
            collector.collect_once()
            stopped.wait(max(0.0, collector.config.interval_seconds - (time.monotonic() - started)))
    finally:
        collector.mark_stopped()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
