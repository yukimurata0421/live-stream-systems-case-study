from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from fast_recovery_controller import ack_delivery

START = datetime(2026, 9, 3, tzinfo=UTC)
PROFILE = {
    "video_bitrate": "3400k",
    "video_maxrate": "3400k",
    "video_bufsize": "6800k",
    "audio_bitrate": "192k",
}


def runtime_observation(
    seconds: int,
    bytes_acked: int,
    *,
    sequence: int,
    pid: int = 200,
    generation: str = "ffmpeg-generation-1",
    producer: str = "runtime-producer-1",
    notsent: int = 0,
    unacked: int = 0,
    lastsnd_ms: int = 5,
) -> dict[str, Any]:
    observed_at = START + timedelta(seconds=seconds)
    return {
        "schema_version": "runtime.ffmpeg_observation.v1",
        "observed_at": observed_at.isoformat(timespec="microseconds"),
        "producer_instance_id": producer,
        "sequence": sequence,
        "protocol_ffmpeg_pid": pid,
        "ffmpeg_generation": f"native-{generation}",
        "target_identity": {
            "host_id": "isolated-dell",
            "host_boot_id": "isolated-boot",
            "namespace": "isolated",
            "pod_uid": "isolated-pod",
            "container_name": "stream-engine",
            "container_id": "containerd://isolated",
            "ffmpeg_generation": generation,
            "ffmpeg_pid": pid,
        },
        "tcp_metrics": {
            "bytes_acked": bytes_acked,
            "send_q": notsent,
            "notsent": notsent,
            "unacked": unacked,
            "lastsnd_ms": lastsnd_ms,
            "rto_ms": 250,
        },
    }


def observe(
    state: dict[str, Any],
    seconds: int,
    bytes_acked: int,
    *,
    sequence: int,
    threshold: float | None = None,
    action_enabled: bool = False,
    pending_effect: bool = False,
    profile: dict[str, str] | None = None,
    **observation_overrides: Any,
) -> dict[str, Any]:
    return ack_delivery.observe_ack_delivery(
        state,
        now_ts=int(START.timestamp()) + seconds,
        runtime_observation=runtime_observation(
            seconds,
            bytes_acked,
            sequence=sequence,
            **observation_overrides,
        ),
        stream_profile=profile or PROFILE,
        measurement_enabled=True,
        action_enabled=action_enabled,
        restart_threshold_mbps=threshold,
        pending_effect=pending_effect,
    )


def collect_healthy_day(state: dict[str, Any], *, mbps: float = 4.8) -> tuple[int, int, dict[str, Any]]:
    bytes_acked = 1_000_000
    bytes_per_minute = round(mbps * 60 * 1_000_000 / 8)
    report: dict[str, Any] = {}
    for minute in range(24 * 60 + 1):
        seconds = minute * 60
        report = observe(
            state,
            seconds,
            bytes_acked,
            sequence=minute + 1,
        )
        bytes_acked += bytes_per_minute
    return 24 * 60 * 60, bytes_acked - bytes_per_minute, report


def test_24h_source_timed_baseline_is_measurement_only_without_threshold() -> None:
    state: dict[str, Any] = {}

    _, _, report = collect_healthy_day(state)

    statistics = report["statistics"]
    assert report["status"] == "VALID"
    assert report["reason_code"] == "ACK_RATE_MEASURE_ONLY_THRESHOLD_UNSET"
    assert statistics["baseline_ready"] is True
    assert statistics["history_sample_count"] == 1440
    assert statistics["healthy_sample_count"] == 1440
    assert statistics["expected_sample_count"] == 1440
    assert statistics["history_coverage_ratio"] == 1.0
    assert statistics["healthy_coverage_ratio"] == 1.0
    assert statistics["history_start_observed_at"] == "2026-09-03T00:01:00.000000Z"
    assert statistics["history_end_observed_at"] == "2026-09-04T00:00:00.000000Z"
    assert statistics["healthy_start_observed_at"] == "2026-09-03T00:01:00.000000Z"
    assert statistics["healthy_end_observed_at"] == "2026-09-04T00:00:00.000000Z"
    assert statistics["healthy"]["median_mbps"] == 4.8
    assert statistics["healthy"]["mad_mbps"] == 0.0
    assert report["configured_restart_threshold_mbps"] is None
    assert report["configured_threshold_to_healthy_median_ratio"] is None
    assert report["stream_profile"] == PROFILE
    assert report["restart_candidate_confirmed"] is False
    assert "history" not in report
    assert len(state[ack_delivery.STATE_KEY]["history"]) == 1440


def test_measured_threshold_stays_shadow_then_can_arm_after_two_low_windows() -> None:
    state: dict[str, Any] = {}
    seconds, bytes_acked, baseline = collect_healthy_day(state)
    threshold = float(baseline["statistics"]["healthy"]["median_mbps"]) * 0.1
    low_bytes_per_ten_seconds = round(0.1 * 10 * 1_000_000 / 8)
    report: dict[str, Any] = {}

    for offset in range(10, 71, 10):
        bytes_acked += low_bytes_per_ten_seconds
        report = observe(
            state,
            seconds + offset,
            bytes_acked,
            sequence=1441 + offset // 10,
            threshold=threshold,
            notsent=600_000,
            lastsnd_ms=2_000,
        )

    assert report["latest"]["ack_mbps"] == 0.1
    assert report["latest"]["queue_pressure"] is True
    assert report["reason_code"] == "ACK_RATE_SHADOW_CANDIDATE_CONFIRMED"
    assert report["shadow_low_streak"] == 2
    assert report["shadow_candidate_confirmed"] is True
    assert report["restart_candidate_confirmed"] is False

    bytes_acked += low_bytes_per_ten_seconds
    armed = observe(
        state,
        seconds + 80,
        bytes_acked,
        sequence=1449,
        threshold=threshold,
        action_enabled=True,
        notsent=600_000,
        lastsnd_ms=2_000,
    )
    assert armed["reason_code"] == "ACK_RATE_RESTART_CANDIDATE_CONFIRMED"
    assert armed["configured_threshold_to_healthy_median_ratio"] == 0.1
    assert armed["restart_candidate_confirmed"] is True


def test_low_rate_without_pressure_and_pending_effect_never_arm_restart() -> None:
    state: dict[str, Any] = {}
    seconds, bytes_acked, baseline = collect_healthy_day(state)
    threshold = float(baseline["statistics"]["healthy"]["p05_mbps"]) * 0.1
    low_step = round(0.1 * 10 * 1_000_000 / 8)

    for offset in range(10, 81, 10):
        bytes_acked += low_step
        report = observe(
            state,
            seconds + offset,
            bytes_acked,
            sequence=1441 + offset // 10,
            threshold=threshold,
            action_enabled=True,
        )
    assert report["reason_code"] == "ACK_RATE_LOW_WITHOUT_QUEUE_PRESSURE"
    assert report["restart_candidate_confirmed"] is False

    for offset in range(90, 111, 10):
        bytes_acked += low_step
        report = observe(
            state,
            seconds + offset,
            bytes_acked,
            sequence=1441 + offset // 10,
            threshold=threshold,
            action_enabled=True,
            pending_effect=True,
            notsent=600_000,
        )
    assert report["latest"]["pending_effect"] is True
    assert report["restart_candidate_confirmed"] is False
    assert report["shadow_low_streak"] == 0


def test_identity_counter_and_profile_changes_rebaseline_without_false_zero() -> None:
    state: dict[str, Any] = {}
    self_first = observe(state, 0, 1_000_000, sequence=1)
    duplicate = observe(state, 0, 1_000_000, sequence=1)
    assert self_first["status"] == "WARMING"
    assert duplicate["reason_code"] == "ACK_RATE_DUPLICATE_SOURCE_SAMPLE"
    assert duplicate["restart_candidate_confirmed"] is False

    changed = observe(
        state,
        60,
        100,
        sequence=2,
        generation="ffmpeg-generation-2",
    )
    assert changed["reason_code"] == "ACK_RATE_WINDOW_INCOMPLETE"
    assert changed["latest"] is None

    regressed = observe(
        state,
        120,
        50,
        sequence=3,
        generation="ffmpeg-generation-2",
    )
    assert regressed["reason_code"] == "ACK_RATE_WINDOW_INCOMPLETE"
    assert regressed["restart_candidate_confirmed"] is False

    profile_changed = observe(
        state,
        180,
        1_000,
        sequence=4,
        generation="ffmpeg-generation-2",
        profile={**PROFILE, "video_bitrate": "2500k"},
    )
    internal = state[ack_delivery.STATE_KEY]
    assert profile_changed["reason_code"] == "ACK_RATE_WINDOW_INCOMPLETE"
    assert internal["reset_reason"] == "STREAM_PROFILE_CHANGED"
    assert internal["history"] == []


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (lambda value: value.update(schema_version="runtime.ffmpeg_observation.invalid"), "ACK_RATE_SOURCE_SCHEMA_INVALID"),
        (lambda value: value.pop("observed_at"), "ACK_RATE_SOURCE_TIME_MISSING"),
        (lambda value: value.update(observed_at="bad"), "ACK_RATE_SOURCE_TIME_INVALID"),
        (lambda value: value.update(observed_at="2026-09-03T00:00:00"), "ACK_RATE_SOURCE_TIME_NAIVE"),
        (lambda value: value.update(observed_at="1970-01-01T00:00:00Z"), "ACK_RATE_SOURCE_TIME_INVALID"),
        (lambda value: value.update(observed_at="2026-09-02T23:00:00Z"), "ACK_RATE_SOURCE_STALE_OR_FUTURE"),
        (lambda value: value.update(producer_instance_id=""), "ACK_RATE_SOURCE_IDENTITY_MISSING"),
        (lambda value: value.update(sequence=True), "ACK_RATE_SOURCE_SEQUENCE_INVALID"),
        (lambda value: value.update(protocol_ffmpeg_pid=1), "ACK_RATE_PROTOCOL_PID_INVALID"),
        (lambda value: value.update(target_identity=None), "ACK_RATE_TARGET_IDENTITY_MISSING"),
        (lambda value: value["target_identity"].update(host_id=""), "ACK_RATE_TARGET_IDENTITY_INVALID"),
        (lambda value: value["target_identity"].update(ffmpeg_pid=201), "ACK_RATE_TARGET_PID_MISMATCH"),
        (lambda value: value.update(tcp_metrics=None), "ACK_RATE_TCP_METRICS_MISSING"),
        (lambda value: value.update(tcp_metrics={}), "ACK_RATE_BYTES_ACKED_INVALID"),
    ],
)
def test_invalid_source_is_unknown_and_never_treated_as_zero(mutation: Any, reason: str) -> None:
    state: dict[str, Any] = {}
    value = runtime_observation(0, 1_000_000, sequence=1)
    mutation(value)

    report = ack_delivery.observe_ack_delivery(
        state,
        now_ts=int(START.timestamp()),
        runtime_observation=value,
        stream_profile=PROFILE,
        measurement_enabled=True,
        action_enabled=True,
        restart_threshold_mbps=0.5,
        pending_effect=False,
    )

    assert report["status"] == "UNKNOWN"
    assert report["reason_code"] == reason
    assert report["latest"] is None
    assert report["restart_candidate_confirmed"] is False


def test_disabled_or_invalid_configuration_fails_closed() -> None:
    state: dict[str, Any] = {}
    observation = runtime_observation(0, 1_000_000, sequence=1)

    disabled = ack_delivery.observe_ack_delivery(
        state,
        now_ts=int(START.timestamp()),
        runtime_observation=observation,
        stream_profile=PROFILE,
        measurement_enabled=False,
        action_enabled=False,
        restart_threshold_mbps=None,
        pending_effect=False,
    )
    invalid = ack_delivery.observe_ack_delivery(
        state,
        now_ts=int(START.timestamp()),
        runtime_observation=observation,
        stream_profile=PROFILE,
        measurement_enabled=True,
        action_enabled=True,
        restart_threshold_mbps=float("nan"),
        pending_effect=False,
    )

    assert disabled["reason_code"] == "ACK_RATE_MEASUREMENT_DISABLED"
    assert invalid["reason_code"] == "ACK_RATE_CONFIG_INVALID"
    assert invalid["restart_candidate_confirmed"] is False


def test_corrupt_persisted_rows_are_discarded_before_measurement() -> None:
    state: dict[str, Any] = {}
    observe(state, 0, 1_000_000, sequence=1)
    internal = state[ack_delivery.STATE_KEY]
    identity = internal["current_identity_sha256"]
    malformed_integer_row = {
        "identity_sha256": identity,
        "observed_at": START.isoformat(),
    }
    malformed_time_row = {
        "identity_sha256": identity,
        "observed_at": 123,
        "observed_us": int(START.timestamp() * 1_000_000),
        "sequence": 1,
        "bytes_acked": 1_000_000,
        "ffmpeg_pid": 200,
        "send_q": 0,
        "notsent": 0,
        "unacked": 0,
        "lastsnd_ms": 5,
        "rto_ms": 250,
    }
    internal["observations"] = [malformed_integer_row, malformed_time_row, *internal["observations"]]
    internal["history"] = [None, {"observed_us": "bad", "ack_mbps": 4.8, "healthy": True}]

    report = observe(state, 10, 7_000_000, sequence=2)

    assert report["status"] == "WARMING"
    cleaned = state[ack_delivery.STATE_KEY]
    assert len(cleaned["observations"]) == 2
    assert cleaned["history"] == []


def test_configured_threshold_waits_for_complete_baseline() -> None:
    state: dict[str, Any] = {}
    observe(state, 0, 1_000_000, sequence=1, threshold=0.5, action_enabled=True)

    report = observe(state, 60, 37_000_000, sequence=2, threshold=0.5, action_enabled=True)

    assert report["status"] == "VALID"
    assert report["reason_code"] == "ACK_RATE_BASELINE_BUILDING"
    assert report["statistics"]["baseline_ready"] is False
    assert report["restart_candidate_confirmed"] is False
