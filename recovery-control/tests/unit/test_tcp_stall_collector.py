from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cra_no_action_soak.recovery_facts import source_hash
from tcp_stall_collector.service import Collector, CollectorConfig, CollectorError, classify_adjacent, project_source, strict_json_bytes


def config(tmp_path: Path, source: Path | None = None) -> CollectorConfig:
    return CollectorConfig(
        collector_id="collector-test",
        release_id="epoch-1",
        source_file=source or tmp_path / "source.json",
        expected_host_id="delivery-host",
        expected_namespace="stream-v3",
        expected_container_name="stream-engine",
        expected_producer_id="stream-engine-effect-executor",
        interval_seconds=10,
        max_adjacent_interval_seconds=15,
        stall_confirmation_seconds=10,
        replacement_correlation_seconds=45,
        source_max_age_seconds=30,
        future_tolerance_seconds=2,
        max_output_bytes=2_000_000,
        stream_profile={"video_bitrate": "3400k", "video_maxrate": "3400k", "video_bufsize": "6800k", "audio_bitrate": "192k"},
        config_sha256="a" * 64,
    )


def source(*, observed_at: str = "2026-09-12T00:00:00Z", sequence: int = 10, acked: int = 1_000_000, sent: int = 1_100_000) -> dict:
    return {
        "schema_version": "runtime.ffmpeg_observation.v1",
        "observation_id": "observation-10",
        "observed_at": observed_at,
        "sequence": sequence,
        "producer_id": "stream-engine-effect-executor",
        "producer_instance_id": "executor-1",
        "ffmpeg_generation": "source-generation-1",
        "ffmpeg_running": True,
        "ffmpeg_uptime_sec": 100,
        "in_flight_count": 0,
        "production_behavior_modified": False,
        "runtime_lifecycle_state": "FFMPEG_RUNNING",
        "runtime_snapshot_id": "runtime-1",
        "runtime_snapshot_status": "VALID",
        "target_snapshot_id": "target-1",
        "target_snapshot_status": "VALID",
        "stream_established": True,
        "protocol_ffmpeg_pid": 1234,
        "runtime_identity": {
            "host_id": "delivery-host",
            "host_boot_id": "boot-1",
            "namespace": "stream-v3",
            "pod_uid": "pod-1",
            "runtime_generation": "runtime-1",
            "stream_engine_container_id": "containerd://one",
            "stream_engine_container_name": "stream-engine",
        },
        "target_identity": {
            "host_id": "delivery-host",
            "host_boot_id": "boot-1",
            "namespace": "stream-v3",
            "pod_uid": "pod-1",
            "container_name": "stream-engine",
            "container_id": "containerd://one",
            "ffmpeg_generation": "ffmpeg-1",
            "ffmpeg_pid": 1234,
        },
        "tcp_metrics": {
            "bytes_acked": acked,
            "bytes_sent": sent,
            "send_q": 0,
            "notsent": 0,
            "unacked": 0,
            "lastsnd_ms": 5,
            "rto_ms": 210,
        },
    }


def _collect_at(collector: Collector, source_path: Path, offset_seconds: int, raw: dict | str | None) -> dict:
    if raw is None:
        source_path.unlink(missing_ok=True)
    elif isinstance(raw, str):
        source_path.write_text(raw, encoding="utf-8")
    else:
        observed_at = datetime(2026, 9, 12, tzinfo=UTC) + timedelta(seconds=offset_seconds)
        raw["observed_at"] = observed_at.isoformat().replace("+00:00", "Z")
        source_path.write_text(json.dumps(raw), encoding="utf-8")
    return collector.collect_once(
        wall_time_ns=(1_789_171_200 + offset_seconds) * 1_000_000_000,
        monotonic_ns=offset_seconds + 1,
    )


def test_strict_json_rejects_duplicate_and_nonfinite() -> None:
    with pytest.raises(ValueError):
        strict_json_bytes(b'{"x":1,"x":2}')
    with pytest.raises(ValueError):
        strict_json_bytes(b'{"x":NaN}')


def test_adjacent_rate_uses_source_time_and_ack_counter(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    first, status, _, eligible = project_source(source(), now_us=1_789_171_200_000_000, config=cfg)
    second, _, _, _ = project_source(
        source(observed_at="2026-09-12T00:00:10Z", sequence=11, acked=7_000_000, sent=7_400_000),
        now_us=1_789_171_210_000_000,
        config=cfg,
    )
    continuity, adjacent = classify_adjacent(first, second)
    assert status == "VALID" and eligible is True
    assert continuity == "CONTINUOUS"
    assert adjacent == {
        "elapsed_sec": 10.0,
        "sequence_delta": 1,
        "bytes_acked_delta": 6_000_000,
        "bytes_sent_delta": 6_300_000,
        "ack_mbps": 4.8,
        "send_mbps": 5.04,
    }


def test_duplicate_does_not_synthesize_zero_rate(tmp_path: Path) -> None:
    point, _, _, _ = project_source(source(), now_us=1_789_171_200_000_000, config=config(tmp_path))
    continuity, adjacent = classify_adjacent(point, point)
    assert continuity == "DUPLICATE_SOURCE"
    assert adjacent is None


def test_long_collector_gap_does_not_become_a_rate(tmp_path: Path) -> None:
    cfg = config(tmp_path)
    first, _, _, _ = project_source(source(), now_us=1_789_171_200_000_000, config=cfg)
    later, _, _, _ = project_source(
        source(observed_at="2026-09-12T00:00:20Z", sequence=20, acked=13_000_000),
        now_us=1_789_171_220_000_000,
        config=cfg,
    )
    assert classify_adjacent(first, later, max_elapsed_seconds=15) == ("COLLECTOR_GAP", None)


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"sequence": 9}, "SEQUENCE_REGRESSION"),
        ({"observed_at": "2026-09-11T23:59:59Z", "sequence": 11}, "SOURCE_TIME_REGRESSION"),
        ({"sequence": 11, "acked": 999_999}, "COUNTER_REGRESSION"),
    ],
)
def test_discontinuities_are_not_rates(tmp_path: Path, change: dict, reason: str) -> None:
    cfg = config(tmp_path)
    first, _, _, _ = project_source(source(), now_us=1_789_171_200_000_000, config=cfg)
    current_raw = source(
        observed_at=change.get("observed_at", "2026-09-12T00:00:10Z"),
        sequence=change.get("sequence", 11),
        acked=change.get("acked", 7_000_000),
    )
    current, _, _, _ = project_source(current_raw, now_us=1_789_171_210_000_000, config=cfg)
    assert classify_adjacent(first, current) == (reason, None)


def test_collector_persists_invalid_and_recovers_without_bridging_gap(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    cfg = config(tmp_path, source_path)
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    collector = Collector(cfg, epoch_id="epoch-1", output_dir=output)
    first = collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    source_path.write_text("not-json", encoding="utf-8")
    invalid = collector.collect_once(wall_time_ns=1_789_171_205_000_000_000, monotonic_ns=2)
    source_path.write_text(json.dumps(source(observed_at="2026-09-12T00:00:10Z", sequence=11)), encoding="utf-8")
    recovered = collector.collect_once(wall_time_ns=1_789_171_210_000_000_000, monotonic_ns=3)
    assert first["continuity"] == "BASELINE"
    assert invalid["source_status"] == "INVALID" and invalid["analysis_eligible"] is False
    assert recovered["continuity"] == "BASELINE" and recovered["adjacent_rate"] is None
    assert len((output / "samples.jsonl").read_text().splitlines()) == 3


def test_restart_reconstructs_sequence_and_previous_point(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    cfg = config(tmp_path, source_path)
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    first = Collector(cfg, epoch_id="epoch-1", output_dir=output)
    first.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    source_path.write_text(json.dumps(source(observed_at="2026-09-12T00:00:10Z", sequence=11, acked=7_000_000)), encoding="utf-8")
    second = Collector(cfg, epoch_id="epoch-1", output_dir=output)
    row = second.collect_once(wall_time_ns=1_789_171_210_000_000_000, monotonic_ns=2)
    assert row["collector_sequence"] == 2
    assert row["continuity"] == "CONTINUOUS"


def test_release_and_epoch_must_match(tmp_path: Path) -> None:
    with pytest.raises(CollectorError, match="RELEASE_EPOCH_MISMATCH"):
        Collector(config(tmp_path), epoch_id="other-epoch", output_dir=tmp_path / "output")


def test_intervention_is_censored_and_next_sample_starts_new_baseline(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    cfg = config(tmp_path, source_path)
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    collector = Collector(cfg, epoch_id="epoch-1", output_dir=output)
    collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    intervention = source(observed_at="2026-09-12T00:00:10Z", sequence=11, acked=7_000_000)
    intervention["in_flight_count"] = 1
    source_path.write_text(json.dumps(intervention), encoding="utf-8")
    censored = collector.collect_once(wall_time_ns=1_789_171_210_000_000_000, monotonic_ns=2)
    source_path.write_text(
        json.dumps(source(observed_at="2026-09-12T00:00:20Z", sequence=12, acked=13_000_000)),
        encoding="utf-8",
    )
    after = collector.collect_once(wall_time_ns=1_789_171_220_000_000_000, monotonic_ns=3)
    assert censored["continuity"] == "CONTINUOUS"
    assert censored["censor_reason"] == "RECOVERY_EFFECT_IN_FLIGHT"
    assert censored["analysis_eligible"] is False
    assert after["continuity"] == "BASELINE"
    assert after["adjacent_rate"] is None


def test_mixed_runtime_target_identity_is_not_rate_eligible(tmp_path: Path) -> None:
    raw = source()
    raw["runtime_identity"]["pod_uid"] = "other-pod"
    projected, status, reason, eligible = project_source(raw, now_us=1_789_171_200_000_000, config=config(tmp_path))
    assert status == "VALID"
    assert reason == "SOURCE_NOT_RATE_ELIGIBLE"
    assert eligible is False
    assert projected["rate_projection_error"] == "TARGET_RUNTIME_IDENTITY_MISMATCH"


def test_modified_production_behavior_is_censored(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    raw = source()
    raw["production_behavior_modified"] = True
    source_path.write_text(json.dumps(raw), encoding="utf-8")
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    row = collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    assert row["rate_eligible"] is True
    assert row["analysis_eligible"] is False
    assert row["censor_reason"] == "PRODUCTION_BEHAVIOR_MODIFIED"


def test_unestablished_stream_is_not_rate_or_episode_eligible(tmp_path: Path) -> None:
    raw = source()
    raw["stream_established"] = False
    projected, status, reason, eligible = project_source(
        raw,
        now_us=1_789_171_200_000_000,
        config=config(tmp_path),
    )
    assert projected["stream_established"] is False
    assert status == "VALID"
    assert reason == "SOURCE_NOT_RATE_ELIGIBLE"
    assert eligible is False


def test_output_limit_fails_closed(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    cfg = config(tmp_path, source_path)
    cfg = CollectorConfig(**{**cfg.__dict__, "max_output_bytes": 1_048_576})
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    collector = Collector(cfg, epoch_id="epoch-1", output_dir=tmp_path / "output")
    collector.samples_path.write_bytes(b"x" * 1_048_000)
    with pytest.raises(CollectorError, match="OUTPUT_LIMIT_REACHED"):
        collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)


def test_single_flat_ack_interval_is_only_a_candidate(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    source_path.write_text(
        json.dumps(source(observed_at="2026-09-12T00:00:10Z", sequence=11, acked=1_000_000, sent=1_100_000)),
        encoding="utf-8",
    )
    collector.collect_once(wall_time_ns=1_789_171_210_000_000_000, monotonic_ns=2)
    snapshot = json.loads(collector.episode_snapshot_path.read_text())
    assert snapshot["episode"] is None
    assert collector.candidate is not None

    source_path.write_text(
        json.dumps(source(observed_at="2026-09-12T00:00:20Z", sequence=12, acked=7_000_000, sent=7_100_000)),
        encoding="utf-8",
    )
    collector.collect_once(wall_time_ns=1_789_171_220_000_000_000, monotonic_ns=3)
    assert collector.candidate is None
    assert json.loads(collector.episode_snapshot_path.read_text())["episode"] is None


def test_flat_ack_then_same_runtime_ffmpeg_replacement_is_durable_recovered_episode(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=output)
    first = source()
    source_path.write_text(json.dumps(first), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    stalled = source(observed_at="2026-09-12T00:00:10Z", sequence=11, acked=1_000_000, sent=1_100_000)
    stalled["tcp_metrics"]["send_q"] = 400_000
    source_path.write_text(json.dumps(stalled), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_210_000_000_000, monotonic_ns=2)
    replacement = source(observed_at="2026-09-12T00:00:20Z", sequence=12, acked=50_000, sent=60_000)
    replacement.update(ffmpeg_generation="source-generation-2", protocol_ffmpeg_pid=2345)
    replacement["target_identity"].update(ffmpeg_generation="ffmpeg-2", ffmpeg_pid=2345)
    source_path.write_text(json.dumps(replacement), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_220_000_000_000, monotonic_ns=3)

    snapshot = json.loads(collector.episode_snapshot_path.read_text())
    episode = snapshot["episode"]
    assert episode["state"] == "RECOVERED"
    assert episode["classification"] == "RTMPS_ACK_STALL_FFMPEG_REPLACED"
    assert episode["started_at"] == "2026-09-12T00:00:10Z"
    assert episode["recovered_at"] == "2026-09-12T00:00:20Z"
    assert episode["target_sha256"] == source_hash(first["target_identity"])
    assert episode["replacement_target_sha256"] == source_hash(replacement["target_identity"])

    restored = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=output)
    assert restored.latest_episode == episode
    assert restored.episode_sequence == 1

    state = json.loads(restored.detector_state_path.read_text())
    inconsistent = json.loads(json.dumps(state))
    inconsistent["latest_episode"]["state"] = "ACTIVE"
    restored.detector_state_path.write_text(json.dumps(inconsistent), encoding="utf-8")
    with pytest.raises(CollectorError, match="DETECTOR_STATE_INVALID"):
        Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=output)

    corrupt_digest = json.loads(json.dumps(state))
    corrupt_digest["latest_episode"]["target_sha256"] = "c" * 64
    restored.detector_state_path.write_text(json.dumps(corrupt_digest), encoding="utf-8")
    with pytest.raises(CollectorError, match="DETECTOR_STATE_INVALID"):
        Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=output)


def test_recovered_episode_allows_a_second_session_loss_across_collector_restart(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    cfg = config(tmp_path, source_path)
    collector = Collector(cfg, epoch_id="epoch-1", output_dir=output)
    _collect_at(collector, source_path, 0, source(sequence=10))
    unavailable = source(sequence=11)
    unavailable["stream_established"] = False
    _collect_at(collector, source_path, 5, unavailable)
    successor = source(sequence=12, acked=50_000, sent=60_000)
    successor.update(ffmpeg_generation="source-generation-2", protocol_ffmpeg_pid=2345)
    successor["target_identity"].update(ffmpeg_generation="ffmpeg-2", ffmpeg_pid=2345)
    _collect_at(collector, source_path, 10, successor)
    first = json.loads(collector.episode_snapshot_path.read_text())["episode"]
    assert first["state"] == "RECOVERED" and first["sequence"] == 1

    successor["sequence"] = 13
    successor["tcp_metrics"].update(bytes_acked=100_000, bytes_sent=110_000)
    _collect_at(collector, source_path, 15, successor)
    unavailable = source(sequence=14)
    unavailable["stream_established"] = False
    _collect_at(collector, source_path, 20, unavailable)
    state = json.loads(collector.detector_state_path.read_text())
    assert state["candidate"] is not None
    assert state["latest_episode"] == first
    collector = Collector(cfg, epoch_id="epoch-1", output_dir=output)

    third = source(sequence=15, acked=50_000, sent=60_000)
    third.update(ffmpeg_generation="source-generation-3", protocol_ffmpeg_pid=3456)
    third["target_identity"].update(ffmpeg_generation="ffmpeg-3", ffmpeg_pid=3456)
    _collect_at(collector, source_path, 25, third)
    second = json.loads(collector.episode_snapshot_path.read_text())["episode"]
    assert second["state"] == "RECOVERED" and second["sequence"] == 2
    assert second["target_sha256"] == source_hash(successor["target_identity"])
    assert second["replacement_target_sha256"] == source_hash(third["target_identity"])
    assert second["episode_id"] != first["episode_id"]
    assert len(collector.samples_path.read_text().splitlines()) == 6


def test_recovered_episode_rejects_candidate_that_predates_its_recovery(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=output)
    _collect_at(collector, source_path, 0, source(sequence=10))
    unavailable = source(sequence=11)
    unavailable["stream_established"] = False
    _collect_at(collector, source_path, 5, unavailable)
    successor = source(sequence=12, acked=50_000, sent=60_000)
    successor.update(ffmpeg_generation="source-generation-2", protocol_ffmpeg_pid=2345)
    successor["target_identity"].update(ffmpeg_generation="ffmpeg-2", ffmpeg_pid=2345)
    _collect_at(collector, source_path, 10, successor)
    committed = collector.samples_path.read_bytes()
    collector._start_candidate(
        collector.previous_rate_point, collector_sequence=2,
        detection_basis="SESSION_CONTINUITY_LOSS", detected_at="2026-09-12T00:00:05Z",
    )
    with pytest.raises(CollectorError, match="DETECTOR_STATE_INVALID"):
        collector._detector_state_payload()
    assert collector.samples_path.read_bytes() == committed


def test_two_flat_ack_intervals_confirm_active_then_ack_progress_recovers_same_target(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    for offset, sequence, acked, sent in (
        (0, 10, 1_000_000, 1_100_000),
        (10, 11, 1_000_000, 1_100_000),
        (20, 12, 1_000_000, 1_100_000),
    ):
        raw = source(observed_at=f"2026-09-12T00:00:{offset:02d}Z", sequence=sequence, acked=acked, sent=sent)
        raw["tcp_metrics"]["notsent"] = 100_000 if offset else 0
        source_path.write_text(json.dumps(raw), encoding="utf-8")
        collector.collect_once(
            wall_time_ns=(1_789_171_200 + offset) * 1_000_000_000,
            monotonic_ns=offset + 1,
        )
    assert collector.latest_episode is not None
    assert collector.latest_episode["state"] == "ACTIVE"

    state = json.loads(collector.detector_state_path.read_text())
    inconsistent_basis = json.loads(json.dumps(state))
    inconsistent_basis["candidate"].update(
        detection_basis="SESSION_CONTINUITY_LOSS",
        trigger_reason_code="SOURCE_GENERATION_INVALID",
        zero_ack_interval_count=0,
    )
    collector.detector_state_path.write_text(json.dumps(inconsistent_basis), encoding="utf-8")
    with pytest.raises(CollectorError, match="DETECTOR_STATE_INVALID"):
        Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    collector.detector_state_path.write_text(json.dumps(state), encoding="utf-8")

    source_path.write_text(
        json.dumps(source(observed_at="2026-09-12T00:00:30Z", sequence=13, acked=7_000_000, sent=7_100_000)),
        encoding="utf-8",
    )
    collector.collect_once(wall_time_ns=1_789_171_230_000_000_000, monotonic_ns=31)
    assert collector.latest_episode["state"] == "RECOVERED"
    assert collector.latest_episode["classification"] == "RTMPS_ACK_STALL_RECOVERED_SAME_TARGET"


_ACTIVE_EPISODE_MATRIX = json.loads(
    (Path(__file__).resolve().parents[2] / "harness/scenarios/rtmps_failure_sequences_v1.json").read_text(encoding="utf-8")
)["active_episode_matrix"]
_ACTIVE_EPISODE_CASES = [
    pytest.param(elapsed, intermediate, restart, id=f"t{elapsed}-{intermediate}-r{int(restart)}")
    for elapsed in _ACTIVE_EPISODE_MATRIX["elapsed_seconds"]
    for intermediate in _ACTIVE_EPISODE_MATRIX["intermediate"]
    for restart in _ACTIVE_EPISODE_MATRIX["restart_before_recovery"]
]
assert len(_ACTIVE_EPISODE_CASES) == _ACTIVE_EPISODE_MATRIX["required_case_count"]


@pytest.mark.parametrize("elapsed_seconds,intermediate,restart_before_recovery", _ACTIVE_EPISODE_CASES)
def test_confirmed_episode_survives_candidate_deadline_and_collector_restart(
    tmp_path: Path,
    elapsed_seconds: int,
    intermediate: str,
    restart_before_recovery: bool,
) -> None:
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    cfg = config(tmp_path, source_path)
    collector = Collector(cfg, epoch_id="epoch-1", output_dir=output)
    _collect_at(collector, source_path, 0, source(sequence=10))
    for offset, sequence in ((5, 11), (15, 12)):
        stalled = source(sequence=sequence)
        stalled["tcp_metrics"]["send_q"] = 400_000
        _collect_at(collector, source_path, offset, stalled)
    assert collector.latest_episode is not None
    assert collector.latest_episode["state"] == "ACTIVE"

    if intermediate == "stalled":
        unavailable: dict | str | None = source(sequence=13)
        unavailable["tcp_metrics"]["send_q"] = 400_000
    elif intermediate == "not_rate_eligible":
        unavailable: dict | str | None = source(sequence=13)
        unavailable["stream_established"] = False
    elif intermediate == "invalid_source":
        unavailable = "not-json"
    else:
        unavailable = None
    _collect_at(collector, source_path, 5 + elapsed_seconds, unavailable)

    state = json.loads(collector.detector_state_path.read_text(encoding="utf-8"))
    assert (state["latest_episode"]["state"] == "ACTIVE") is _ACTIVE_EPISODE_MATRIX["expected_active_before_recovery"]
    assert state["candidate"] is not None
    assert state["candidate"]["target_sha256"] == state["latest_episode"]["target_sha256"]
    if restart_before_recovery:
        collector = Collector(cfg, epoch_id="epoch-1", output_dir=output)

    replacement = source(sequence=14, acked=50_000, sent=60_000)
    replacement.update(ffmpeg_generation="source-generation-2", protocol_ffmpeg_pid=2345)
    replacement["target_identity"].update(ffmpeg_generation="ffmpeg-2", ffmpeg_pid=2345)
    _collect_at(collector, source_path, 10 + elapsed_seconds, replacement)
    assert collector.latest_episode is not None
    assert collector.latest_episode["state"] == "RECOVERED"
    assert collector.latest_episode["classification"] == _ACTIVE_EPISODE_MATRIX["expected_classification"]
    assert Collector(cfg, epoch_id="epoch-1", output_dir=output).candidate is None


def test_invalid_active_state_fails_before_appending_an_uncommitted_sample(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=output)
    _collect_at(collector, source_path, 0, source(sequence=10))
    for offset, sequence in ((5, 11), (15, 12)):
        stalled = source(sequence=sequence)
        stalled["tcp_metrics"]["send_q"] = 400_000
        _collect_at(collector, source_path, offset, stalled)
    assert collector.latest_episode is not None
    assert collector.latest_episode["state"] == "ACTIVE"
    committed_bytes = collector.samples_path.read_bytes()
    committed_state = collector.detector_state_path.read_bytes()

    # An internal transition defect must fail closed before it changes either
    # append-only history or the durable detector state.
    collector.candidate = None
    with pytest.raises(CollectorError, match="DETECTOR_STATE_INVALID"):
        _collect_at(collector, source_path, 20, source(sequence=13))
    assert collector.samples_path.read_bytes() == committed_bytes
    assert collector.detector_state_path.read_bytes() == committed_state


def test_active_episode_keeps_stall_evidence_but_does_not_cross_runtime_drift(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    cfg = config(tmp_path, source_path)
    collector = Collector(cfg, epoch_id="epoch-1", output_dir=output)
    _collect_at(collector, source_path, 0, source(sequence=10))
    for offset, sequence in ((5, 11), (15, 12), (20, 13)):
        stalled = source(sequence=sequence)
        stalled["tcp_metrics"]["send_q"] = 400_000
        _collect_at(collector, source_path, offset, stalled)
    assert collector.latest_episode is not None
    assert collector.latest_episode["state"] == "ACTIVE"
    assert collector.latest_episode["zero_ack_interval_count"] == 3

    other_runtime = source(sequence=14, acked=50_000, sent=60_000)
    other_runtime["runtime_identity"].update(pod_uid="pod-2", stream_engine_container_id="containerd://two")
    other_runtime["target_identity"].update(pod_uid="pod-2", container_id="containerd://two")
    _collect_at(collector, source_path, 25, other_runtime)
    assert collector.latest_episode["state"] == "ACTIVE"
    assert collector.latest_episode["recovered_at"] is None
    assert Collector(cfg, epoch_id="epoch-1", output_dir=output).latest_episode["state"] == "ACTIVE"


def test_session_loss_candidate_updates_across_repeated_non_rate_source(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    _collect_at(collector, source_path, 0, source(sequence=10))
    for offset, sequence in ((5, 11), (10, 12)):
        unavailable = source(sequence=sequence)
        unavailable["stream_established"] = False
        _collect_at(collector, source_path, offset, unavailable)
    assert collector.candidate is not None
    assert collector.candidate["detection_basis"] == "SESSION_CONTINUITY_LOSS"
    assert collector.candidate["last_collector_sequence"] == 3

    replacement = source(sequence=13, acked=50_000, sent=60_000)
    replacement.update(ffmpeg_generation="source-generation-2", protocol_ffmpeg_pid=2345)
    replacement["target_identity"].update(ffmpeg_generation="ffmpeg-2", ffmpeg_pid=2345)
    _collect_at(collector, source_path, 15, replacement)
    assert collector.latest_episode is not None
    assert collector.latest_episode["classification"] == "FFMPEG_SESSION_LOSS_FFMPEG_REPLACED"


def test_ack_candidate_requires_confirmation_and_pressure_before_promotion(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    _collect_at(collector, source_path, 0, source(sequence=10))
    for offset, sequence in ((5, 11), (10, 12)):
        stalled = source(sequence=sequence)
        stalled["tcp_metrics"]["send_q"] = 400_000
        _collect_at(collector, source_path, offset, stalled)
    assert collector.candidate is not None
    assert collector.latest_episode is None
    assert collector.candidate["zero_ack_interval_count"] == 2

    _collect_at(collector, source_path, 15, source(sequence=13, sent=1_200_000))
    assert collector.candidate is not None
    assert collector.latest_episode is None
    _collect_at(collector, source_path, 20, source(sequence=14, acked=7_000_000, sent=7_100_000))
    assert collector.candidate is None
    assert collector.latest_episode is None


def test_five_second_long_stall_and_session_gap_survives_mid_episode_restart(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    cfg = config(tmp_path, source_path)
    collector = Collector(cfg, epoch_id="epoch-1", output_dir=output)
    _collect_at(collector, source_path, 0, source(sequence=10))

    for offset in range(5, 161, 5):
        current = source(sequence=10 + offset // 5)
        if offset < 65:
            current["tcp_metrics"]["send_q"] = 400_000
        else:
            current["stream_established"] = False
        _collect_at(collector, source_path, offset, current)
        if offset >= 15:
            state = json.loads(collector.detector_state_path.read_text(encoding="utf-8"))
            assert state["candidate"] is not None
            assert state["latest_episode"]["state"] == "ACTIVE"
        if offset == 80:
            collector = Collector(cfg, epoch_id="epoch-1", output_dir=output)

    replacement = source(sequence=43, acked=50_000, sent=60_000)
    replacement.update(ffmpeg_generation="source-generation-2", protocol_ffmpeg_pid=2345)
    replacement["target_identity"].update(ffmpeg_generation="ffmpeg-2", ffmpeg_pid=2345)
    _collect_at(collector, source_path, 165, replacement)
    assert collector.latest_episode is not None
    assert collector.latest_episode["state"] == "RECOVERED"
    assert collector.latest_episode["classification"] == "RTMPS_ACK_STALL_FFMPEG_REPLACED"
    assert len(collector.samples_path.read_text(encoding="utf-8").splitlines()) == 34


def test_candidate_does_not_cross_pod_or_container_runtime_identity(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    stalled = source(observed_at="2026-09-12T00:00:10Z", sequence=11, acked=1_000_000, sent=1_100_000)
    source_path.write_text(json.dumps(stalled), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_210_000_000_000, monotonic_ns=2)
    replacement = source(observed_at="2026-09-12T00:00:20Z", sequence=12, acked=50_000, sent=60_000)
    replacement["runtime_identity"].update(pod_uid="pod-2", stream_engine_container_id="containerd://two")
    replacement["target_identity"].update(pod_uid="pod-2", container_id="containerd://two")
    source_path.write_text(json.dumps(replacement), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_220_000_000_000, monotonic_ns=3)
    assert collector.candidate is None
    assert collector.latest_episode is None


def test_candidate_expires_before_unrelated_late_ffmpeg_replacement(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=output)
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    stalled = source(observed_at="2026-09-12T00:00:10Z", sequence=11)
    stalled["tcp_metrics"]["send_q"] = 400_000
    source_path.write_text(json.dumps(stalled), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_210_000_000_000, monotonic_ns=2)
    source_path.write_text("invalid", encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_256_000_000_000, monotonic_ns=3)
    assert collector.candidate is None

    replacement = source(observed_at="2026-09-12T00:01:10Z", sequence=12, acked=50_000, sent=60_000)
    replacement.update(ffmpeg_generation="source-generation-2", protocol_ffmpeg_pid=2345)
    replacement["target_identity"].update(ffmpeg_generation="ffmpeg-2", ffmpeg_pid=2345)
    source_path.write_text(json.dumps(replacement), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_270_000_000_000, monotonic_ns=4)
    assert collector.latest_episode is None


def test_historical_20260912_eof_sequence_detects_session_loss_without_flat_ack(tmp_path: Path) -> None:
    catalog = json.loads(
        (Path(__file__).resolve().parents[2] / "harness/scenarios/rtmps_failure_sequences_v1.json").read_text(encoding="utf-8")
    )
    case = next(item for item in catalog["cases"] if item["id"] == "historical_20260912_eof_without_flat_ack")
    source_path = tmp_path / "source.json"
    output = tmp_path / "output"
    cfg = config(tmp_path, source_path)
    collector = Collector(cfg, epoch_id="epoch-1", output_dir=output)

    first = source()
    source_path.write_text(json.dumps(first), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    broken = source(observed_at="2026-09-12T00:00:05Z", sequence=11)
    broken["ffmpeg_generation"] = None
    source_path.write_text(json.dumps(broken), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_205_000_000_000, monotonic_ns=2)

    # The candidate is durable across a collector process restart while the
    # stream-engine runtime, Pod and container remain the same.
    collector = Collector(cfg, epoch_id="epoch-1", output_dir=output)
    replacement = source(observed_at="2026-09-12T00:00:15Z", sequence=12, acked=50_000, sent=60_000)
    replacement.update(ffmpeg_generation="source-generation-2", protocol_ffmpeg_pid=2345)
    replacement["target_identity"].update(ffmpeg_generation="ffmpeg-2", ffmpeg_pid=2345)
    source_path.write_text(json.dumps(replacement), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_215_000_000_000, monotonic_ns=3)

    episode = json.loads(collector.episode_snapshot_path.read_text())["episode"]
    assert episode["state"] == "RECOVERED"
    assert episode["classification"] == case["episode"]["classification"]
    assert episode["detection_basis"] == "SESSION_CONTINUITY_LOSS"
    assert episode["trigger_reason_code"] == "SOURCE_GENERATION_INVALID"
    assert episode["zero_ack_interval_count"] == 0
    assert episode["target_sha256"] == source_hash(first["target_identity"])
    assert episode["replacement_target_sha256"] == source_hash(replacement["target_identity"])


def test_session_loss_same_target_return_does_not_create_episode(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    broken = source(observed_at="2026-09-12T00:00:05Z", sequence=11)
    broken["ffmpeg_generation"] = None
    source_path.write_text(json.dumps(broken), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_205_000_000_000, monotonic_ns=2)
    assert collector.candidate is not None
    source_path.write_text(json.dumps(source(observed_at="2026-09-12T00:00:10Z", sequence=11)), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_210_000_000_000, monotonic_ns=3)
    assert collector.candidate is None
    assert collector.latest_episode is None


def test_session_loss_candidate_does_not_cross_runtime_replacement(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    broken = source(observed_at="2026-09-12T00:00:05Z", sequence=11)
    broken["ffmpeg_generation"] = None
    source_path.write_text(json.dumps(broken), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_205_000_000_000, monotonic_ns=2)
    assert collector.candidate is not None
    replacement = source(observed_at="2026-09-12T00:00:10Z", sequence=12, acked=50_000, sent=60_000)
    replacement.update(ffmpeg_generation="source-generation-2", protocol_ffmpeg_pid=2345)
    replacement["runtime_identity"].update(pod_uid="pod-2", stream_engine_container_id="containerd://two")
    replacement["target_identity"].update(pod_uid="pod-2", container_id="containerd://two", ffmpeg_generation="ffmpeg-2", ffmpeg_pid=2345)
    source_path.write_text(json.dumps(replacement), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_210_000_000_000, monotonic_ns=3)
    assert collector.candidate is None
    assert collector.latest_episode is None


def test_session_loss_candidate_expires_before_late_replacement(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)
    broken = source(observed_at="2026-09-12T00:00:05Z", sequence=11)
    broken["ffmpeg_generation"] = None
    source_path.write_text(json.dumps(broken), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_205_000_000_000, monotonic_ns=2)
    assert collector.candidate is not None
    collector.collect_once(wall_time_ns=1_789_171_251_000_000_000, monotonic_ns=3)
    assert collector.candidate is None
    replacement = source(observed_at="2026-09-12T00:01:00Z", sequence=12, acked=50_000, sent=60_000)
    replacement.update(ffmpeg_generation="source-generation-2", protocol_ffmpeg_pid=2345)
    replacement["target_identity"].update(ffmpeg_generation="ffmpeg-2", ffmpeg_pid=2345)
    source_path.write_text(json.dumps(replacement), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_260_000_000_000, monotonic_ns=4)
    assert collector.latest_episode is None


@pytest.mark.parametrize("untrusted_sample", ["missing", "invalid", "stale", "future"])
def test_observation_source_failure_does_not_become_session_loss_episode(tmp_path: Path, untrusted_sample: str) -> None:
    source_path = tmp_path / "source.json"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)

    if untrusted_sample == "missing":
        source_path.unlink()
    elif untrusted_sample == "invalid":
        source_path.write_text("not-json", encoding="utf-8")
    else:
        observed_at = "2026-09-11T23:59:00Z" if untrusted_sample == "stale" else "2026-09-12T00:00:10Z"
        source_path.write_text(json.dumps(source(observed_at=observed_at, sequence=11)), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_205_000_000_000, monotonic_ns=2)
    assert collector.candidate is None

    replacement = source(observed_at="2026-09-12T00:00:10Z", sequence=12, acked=50_000, sent=60_000)
    replacement.update(ffmpeg_generation="source-generation-2", protocol_ffmpeg_pid=2345)
    replacement["target_identity"].update(ffmpeg_generation="ffmpeg-2", ffmpeg_pid=2345)
    source_path.write_text(json.dumps(replacement), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_210_000_000_000, monotonic_ns=3)
    assert collector.latest_episode is None


def test_session_loss_candidate_is_not_started_during_observed_intervention(tmp_path: Path) -> None:
    source_path = tmp_path / "source.json"
    collector = Collector(config(tmp_path, source_path), epoch_id="epoch-1", output_dir=tmp_path / "output")
    source_path.write_text(json.dumps(source()), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_200_000_000_000, monotonic_ns=1)

    intervention = source(observed_at="2026-09-12T00:00:05Z", sequence=11)
    intervention.update(stream_established=False, in_flight_count=1, production_behavior_modified=True)
    source_path.write_text(json.dumps(intervention), encoding="utf-8")
    collector.collect_once(wall_time_ns=1_789_171_205_000_000_000, monotonic_ns=2)
    assert collector.candidate is None
