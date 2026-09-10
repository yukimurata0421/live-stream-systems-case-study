from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tools.analyze_tcp_stall import (
    DerivedSample,
    RawSample,
    StallEvent,
    add_causal_baseline,
    condition_runs,
    derive_samples,
    event_rule_runs,
    percentile,
    run_analysis,
    run_synthetic_suite,
    sanitized_sample_rows,
    threshold_evaluation,
)


def raw_sample(seconds: int, *, pid: int = 100, acked: int, sent: int | None = None) -> RawSample:
    return RawSample(
        ts=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds),
        pid=pid,
        bytes_acked=acked,
        bytes_sent=acked if sent is None else sent,
        send_q=0,
        notsent=0,
        unacked=0,
        lastsnd_ms=0,
        rto_ms=200,
        source_epoch="pid_bound_event_time",
    )


def derived_sample(
    seconds: int,
    rate: float,
    *,
    label: str = "NORMAL_CANDIDATE",
    pid: int = 100,
    delta_rate: float | None = None,
    queue_pressure: bool = False,
) -> DerivedSample:
    return DerivedSample(
        ts=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds),
        pid=pid,
        source_epoch="pid_bound_event_time",
        interval_sec=60.0,
        bytes_acked_delta=int(rate * 1_000_000 * 60 / 8),
        bytes_per_10s=rate * 1_000_000 * 10 / 8,
        bytes_per_60s=rate * 1_000_000 * 60 / 8,
        rate_mbps=rate,
        sent_rate_mbps=rate,
        delta_rate_mbps=delta_rate,
        negative_drop_mbps=None,
        relative_drop=None,
        rolling_3m_mbps=rate,
        rolling_5m_mbps=rate,
        send_q=0,
        notsent=0,
        unacked=0,
        lastsnd_ms=0,
        rto_ms=200,
        queue_pressure=queue_pressure,
        label=label,
    )


def test_percentile_uses_type_7_linear_interpolation() -> None:
    assert percentile([0.0, 10.0], 25.0) == pytest.approx(2.5)
    assert percentile([0.0, 10.0], 50.0) == pytest.approx(5.0)
    assert percentile([0.0, 10.0], 75.0) == pytest.approx(7.5)


def test_derive_samples_uses_ack_progression_and_resets_on_identity_or_counter_change() -> None:
    samples = [
        raw_sample(0, acked=1_000),
        raw_sample(60, acked=61_000),
        raw_sample(120, acked=121_000),
        raw_sample(180, pid=200, acked=10),
        raw_sample(240, pid=200, acked=60_010),
        raw_sample(300, pid=200, acked=20),
        raw_sample(360, pid=200, acked=60_020),
    ]
    derived, discontinuities = derive_samples(samples)
    assert [sample.bytes_acked_delta for sample in derived] == [60_000, 60_000, 60_000, 60_000]
    assert discontinuities == {"initial_warmup": 1, "pid_change": 1, "counter_regression": 1}


def test_condition_runs_breaks_on_label_identity_and_time_gap() -> None:
    samples = [
        derived_sample(60, 0.1),
        derived_sample(120, 0.1),
        derived_sample(180, 0.1, label="KNOWN_TCP_STALL"),
        derived_sample(240, 0.1),
        derived_sample(400, 0.1),
        derived_sample(460, 0.1, pid=200),
    ]
    runs = condition_runs(samples, lambda sample: sample.rate_mbps < 1.0, label="NORMAL_CANDIDATE")
    assert [len(run) for run in runs] == [2, 1, 1, 1]


def test_stall_event_window_is_explicit() -> None:
    anchor = datetime(2026, 1, 1, 12, tzinfo=UTC)
    event = StallEvent("event", 100, anchor, anchor + timedelta(minutes=2))
    assert event.window_start == anchor - timedelta(minutes=10)
    assert event.window_end == anchor + timedelta(minutes=7)


def test_event_replay_is_bound_to_exact_ffmpeg_pid() -> None:
    anchor = datetime(2026, 1, 1, 12, tzinfo=UTC)
    event = StallEvent("event", 100, anchor, anchor)
    samples = [
        derived_sample(12 * 3600, 0.1, pid=100),
        derived_sample(12 * 3600 + 60, 0.1, pid=200),
    ]
    runs = event_rule_runs(event, samples, lambda sample: sample.rate_mbps < 1.0)
    assert [[sample.pid for sample in run] for run in runs] == [[100]]


def test_strict_event_replay_excludes_lookback_chance_match() -> None:
    anchor = datetime(2026, 1, 1, 12, tzinfo=UTC)
    event = StallEvent("event", 100, anchor, anchor)
    samples = [
        derived_sample(12 * 3600 - 5 * 60, 0.1),
        derived_sample(12 * 3600 + 60, 0.2),
    ]
    broad = event_rule_runs(event, samples, lambda sample: sample.rate_mbps < 1.0)
    strict = event_rule_runs(event, samples, lambda sample: sample.rate_mbps < 1.0, strict_window=True)
    assert [sample.ts for run in broad for sample in run] == [sample.ts for sample in samples]
    assert [sample.ts for run in strict for sample in run] == [samples[1].ts]


def test_unknown_queue_pressure_is_counted_in_background_not_normal_false_rate() -> None:
    samples = [
        derived_sample(60, 5.0, delta_rate=0.1),
        derived_sample(120, 4.9, delta_rate=-0.1),
        derived_sample(180, 0.1, label="UNKNOWN", delta_rate=-4.8, queue_pressure=True),
        derived_sample(240, 5.1, delta_rate=5.0),
    ]
    _, rows, _ = threshold_evaluation(samples, [], [5.0, 4.9, 5.1], [0.1, -0.1, 5.0])
    row = next(
        item for item in rows if item["percentile"] == 0.1 and item["rule"] == "rate_plus_queue_pressure" and item["persistence_n"] == 1
    )
    assert row["normal_false_runs"] == 0
    assert row["background_candidate_runs"] == 1


def test_seven_day_causal_baseline_cannot_be_ready_from_short_span() -> None:
    samples = [derived_sample(index * 60, 4.8 + (index % 3) * 0.01) for index in range(10_081)]
    summary = add_causal_baseline(samples, window_days=7)
    ready = [sample for sample in samples if sample.baseline_state == "READY"]
    assert ready
    assert ready[0].ts >= samples[0].ts + timedelta(days=7, minutes=-3)
    assert summary["minimum_span_days"] == pytest.approx(7 - 180 / 86_400)


def test_offline_states_stop_short_of_unproven_tcp_stall() -> None:
    rows = sanitized_sample_rows(
        [
            derived_sample(60, 4.8),
            derived_sample(120, 0.2),
            derived_sample(180, 0.1, label="UNKNOWN", queue_pressure=True),
        ],
        normal_rates=[4.7, 4.8, 4.9],
        anomaly_threshold_mbps=1.0,
    )
    assert [row["candidate_state"] for row in rows] == ["NORMAL", "DEGRADED", "STRONG_DEGRADATION"]
    assert all(row["candidate_state"] != "STALL_CANDIDATE" for row in rows)


def test_offline_run_writes_sanitized_immutable_artifact_set(tmp_path: Path) -> None:
    primary = tmp_path / "primary.jsonl"
    labels = tmp_path / "labels.jsonl"
    rows = []
    acknowledged = 1_000
    for index in range(14):
        if index:
            acknowledged += (60_000_000 + (index % 4) * 1_000_000) // 8
        rows.append(
            {
                "kind": "tcp_send_sample",
                "ts_utc": (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=index * 60)).isoformat(),
                "ffmpeg_pid": 123,
                "bytes_acked": acknowledged,
                "bytes_sent": acknowledged,
                "send_q": 0,
                "notsent": 0,
                "unacked": 0,
                "lastsnd_ms": 0,
                "rto_ms": 200,
                "conn": "sensitive endpoint must never be exported",
            }
        )
    primary.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    labels.write_text("", encoding="utf-8")
    output = tmp_path / "output"
    args = argparse.Namespace(
        primary=str(primary),
        label_events=[str(labels)],
        stream_engine_events=None,
        ffmpeg_stderr=None,
        rtmps_observations=None,
        analysis_end="2026-01-01T00:20:00Z",
        repository_head="fixture",
        output=str(output),
    )
    analysis = run_analysis(args)
    assert analysis["production_connection"] == "NONE"
    assert analysis["production_logic_mutated"] is False
    with (output / "tcp-stall-derived-samples.csv").open(encoding="utf-8", newline="") as handle:
        exported = list(csv.DictReader(handle))
    assert exported
    assert "pid" not in exported[0]
    assert "conn" not in exported[0]
    assert {row["candidate_state"] for row in exported} <= {"NORMAL", "DEGRADED", "STRONG_DEGRADATION"}
    assert all(row["normal_empirical_percentile_rank"] for row in exported)
    assert "sensitive endpoint" not in (output / "tcp-stall-analysis.json").read_text(encoding="utf-8")
    with pytest.raises(FileExistsError):
        run_analysis(args)


def test_synthetic_suite_covers_declared_scenarios_without_effects() -> None:
    rows = run_synthetic_suite()
    assert {row["scenario"] for row in rows} == {
        "stable_bitrate",
        "minor_jitter",
        "single_low_outlier",
        "slow_degradation",
        "sudden_drop",
        "sustained_near_zero",
        "bitrate_step_change",
        "long_degraded_period",
        "recovery",
    }
    assert all(row["pass"] for row in rows)
    assert all(row["physical_effect_count"] == 0 for row in rows)
