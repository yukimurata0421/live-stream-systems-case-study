"""Exercise the first host adapter, real HTTP entry and bounded projection."""
import copy
import importlib.util
import json
from pathlib import Path
import select
import subprocess
import sys
from urllib.error import HTTPError
from urllib.request import ProxyHandler, Request, build_opener
from unittest.mock import patch

import pytest

from stream_core.common import observation_time, upload_observation, upload_samples
from stream_core.common.observation_contract import Observation, TargetState, Validity
from stream_core.common import observation_projection as projection
from test_independent_upload_measurement import (
    measured, SOURCE, RECEIVER, source_clock, receiver_clock, sampler, counter, select as select_source, accept,
)

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "ops/scripts/stream_v3_observation_exporter.py"
spec = importlib.util.spec_from_file_location("host_observation_exporter", SCRIPT)
exporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exporter)


def fixture_source(folder, *, row=None, at=60, identity=SOURCE):
    row = measured(at) if row is None else row
    upload_samples.append_sample(folder / "samples.jsonl", row)
    status = {"schema": "stream.upload_sampler_status.v1", **identity,
        "source_host": "encoder-host", "source_mono": row["source_mono"],
        "measurement_epoch": row["measurement_epoch"], "status": "READY"}
    upload_samples.write_json(folder / "status.json", status)
    return row, status


def read(folder, t=60, offset=0, identity=SOURCE):
    return upload_observation.collect(folder, host="encoder-host", identity=identity,
                                      capture=lambda: source_clock(t, offset))


@pytest.mark.parametrize("state", [TargetState.OK, TargetState.FAIL])
def test_missing_evidence_cannot_assert_target_health(state):
    with pytest.raises(ValueError):
        Observation("upload", "encoder_tcp_send", Validity.UNAVAILABLE,
                    "missing", target_state=state)


def test_expiration_clears_a_previously_known_target_state():
    fact = Observation.measured(source="probe", scope="receiver", age_upper_seconds=181,
                                max_age_seconds=180, target_state=TargetState.OK)
    assert fact.validity == Validity.STALE and fact.target_state == TargetState.UNKNOWN


@pytest.mark.parametrize("age", [True, -1, float("nan"), float("inf")])
def test_invalid_age_is_never_fresh(age):
    with pytest.raises(ValueError):
        Observation.measured(source="probe", scope="receiver", age_upper_seconds=age, max_age_seconds=180)


@pytest.mark.parametrize("offset", [-7200, 0, 7200])
def test_host_adapter_freshness_survives_utc_offset_and_step(tmp_path, offset):
    row, _ = fixture_source(tmp_path)
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    observation = read(tmp_path, 90, offset)
    assert observation.observation.validity == Validity.VALID
    assert observation.observation.age_upper_seconds == pytest.approx(30.1)
    assert observation.mbps == row["mbps"]
    assert observation.observation.target_state == TargetState.UNKNOWN
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


def test_stopped_sampler_does_not_renew_the_last_measurement(tmp_path):
    fixture_source(tmp_path)
    recent = read(tmp_path, 90)
    assert recent.observation.validity == Validity.VALID and not recent.collector_available
    # Recopying identical files changes filesystem times, not evidence age.
    for path in tmp_path.iterdir():
        path.write_bytes(path.read_bytes())
    expired = read(tmp_path, 241)
    assert expired.observation.validity == Validity.STALE and expired.mbps is None
    text = exporter.render(expired, host="encoder-host", source_hash="a"*64, config_hash="b"*64)
    assert 'validity="STALE"} 1' in text and "stream_v3_observation_upload_mbps{" not in text


def test_real_zero_measurement_and_missing_source_are_distinct(tmp_path):
    state = sampler.Sampler({}, identity=SOURCE, host="encoder-host")
    state.step(counter(0, delta=0), source_clock(0))
    fixture_source(tmp_path, row=state.step(counter(60, delta=0), source_clock(60)))
    assert read(tmp_path).mbps == 0
    (tmp_path / "samples.jsonl").unlink()
    absent = read(tmp_path)
    assert absent.mbps is None and absent.observation.validity == Validity.UNAVAILABLE


@pytest.mark.parametrize("at", [61, 90, 241])
@pytest.mark.parametrize("offset", [-7200, 0, 7200])
def test_direct_and_operated_replica_agree_for_the_same_interval(tmp_path, at, offset):
    direct = tmp_path / "local";direct.mkdir()
    replica = tmp_path / "replica";replica.mkdir()
    row, _ = fixture_source(direct, row=measured(offset=offset))
    accept(select_source(replica), row, offset=offset)
    candidate = read(direct, at, offset)
    with patch.object(upload_samples.clock, "identity", return_value=RECEIVER), patch.object(
            upload_samples.clock, "capture", return_value=receiver_clock(at, -offset)):
        previous = upload_samples.latest(replica, receiver_clock(at, -offset)["wall"])
    assert previous["_mbps"] == row["mbps"]
    assert (candidate.observation.validity == Validity.VALID) == (previous["_source_age"] <= 180)
    # The replica adds the bounded round trip; direct collection omits that hop.
    assert 0 <= previous["_source_age"] - candidate.observation.age_upper_seconds < .1
    if candidate.mbps is not None:
        assert candidate.mbps == previous["_mbps"]


def test_epoch_change_waits_for_new_interval_then_recovers(tmp_path):
    row, status = fixture_source(tmp_path)
    status.update(measurement_epoch="new-generation", status="WAITING")
    upload_samples.write_json(tmp_path / "status.json", status)
    first = read(tmp_path)
    assert first.mbps is None and first.observation.reason == "baseline_pending"
    following = measured(120)
    following["measurement_epoch"] = "new-generation"
    upload_samples.append_sample(tmp_path / "samples.jsonl", following)
    status.update(source_mono=following["source_mono"], status="READY")
    upload_samples.write_json(tmp_path / "status.json", status)
    assert read(tmp_path, 120).observation.validity == Validity.VALID


@pytest.mark.parametrize("change", ["boot", "machine", "host", "future", "boolean"])
def test_wrong_source_identity_or_time_cannot_be_reported_fresh(tmp_path, change):
    row, status = fixture_source(tmp_path)
    key = {"boot": "boot_id", "machine": "machine_id_sha256", "host": "source_host"}.get(change)
    if key:
        status[key] = "other"
    elif change == "future":
        status["source_mono"] += 60
    else:
        status["source_mono"] = True
    upload_samples.write_json(tmp_path / "status.json", status)
    assert read(tmp_path).observation.validity == Validity.UNAVAILABLE


def test_source_read_failure_and_symlink_do_not_return_zero_or_cached_success(tmp_path):
    fixture_source(tmp_path)
    assert read(tmp_path).mbps is not None
    original = tmp_path / "samples.jsonl"
    moved = tmp_path / "original"
    original.rename(moved);original.symlink_to(moved)
    assert read(tmp_path).mbps is None


def test_metrics_have_bounded_labels_and_no_process_or_sample_identity(tmp_path):
    row, _ = fixture_source(tmp_path)
    text = exporter.render(read(tmp_path), host="encoder-host", source_hash="d"*64, config_hash="e"*64)
    for private in [row["sample_id"], row["source_machine"], row["measurement_epoch"], row["connection_id"]]:
        assert private not in text
    assert 'state="UNKNOWN"} 1' in text
    assert 'activity="UNSPECIFIED",' in text


def test_real_read_only_http_entry_survives_source_loss_without_false_health(tmp_path):
    identity = upload_samples.clock.identity();now = upload_samples.clock.capture()
    row = measured()
    row.update(source_machine=identity["machine_id_sha256"], source_boot=identity["boot_id"], source_mono=now["mono"]-2)
    fixture_source(tmp_path, row=row, identity=identity)
    original = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    process = subprocess.Popen([sys.executable, "-I", "-B", str(SCRIPT), "--directory", str(tmp_path),
        "--source-host", "encoder-host", "--machine", identity["machine_id_sha256"], "--port", "0"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert select.select([process.stdout], [], [], 5)[0], "exporter did not start"
        started = json.loads(process.stdout.readline())
        url = f'http://127.0.0.1:{started["listen"][1]}'
        opener = build_opener(ProxyHandler({}))
        with opener.open(url + "/metrics", timeout=2) as response:
            assert response.status == 200 and response.headers["Cache-Control"] == "no-store"
            assert 'validity="VALID"} 1' in response.read().decode()
        assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == original
        (tmp_path / "samples.jsonl").unlink()
        with opener.open(url + "/metrics", timeout=2) as response:
            assert response.status == 200
            text = response.read().decode()
            assert 'validity="UNAVAILABLE"} 1' in text and "stream_v3_observation_upload_mbps{" not in text
        with pytest.raises(HTTPError) as error:
            opener.open(url + "/control", timeout=2)
        assert error.value.code == 404
        with pytest.raises(HTTPError) as error:
            opener.open(Request(url + "/metrics", data=b"{}"), timeout=2)
        assert error.value.code == 501
    finally:
        process.terminate()
        process.wait(timeout=5)


@pytest.mark.parametrize("extra", [["--machine", "wrong"], ["--listen", "0.0.0.0"], ["--listen", "8.8.8.8"]])
def test_cli_refuses_unbound_host_or_public_listener(tmp_path, extra):
    result = subprocess.run([sys.executable, "-I", "-B", str(SCRIPT), "--directory", str(tmp_path),
        "--source-host", "encoder-host", "--machine", upload_samples.clock.identity()["machine_id_sha256"],
        "--once", *extra], capture_output=True, timeout=5)
    assert result.returncode != 0 and not result.stdout


def query_payload(*, fresh=1, age=10, rate=0, stamp=1000):
    labels = {"job": projection.JOB, "instance": "127.0.0.1:9120", "host": "encoder-host",
              "source": "upload", "scope": "encoder_tcp_send"}
    return {"status": "success", "data": {"resultType": "vector", "result": [
        {"metric": {**labels, "__name__": key}, "value": [stamp, str(value)]}
        for key, value in [(projection.FRESH, fresh), (projection.AGE, age), (projection.RATE, rate)]]}}


def project(payload, now=1000, elapsed=.1):
    return projection.project(payload, now_wall=now, elapsed_seconds=elapsed,
                              instance="127.0.0.1:9120", host="encoder-host")


def test_projection_preserves_zero_and_leaves_delivery_unknown():
    result = project(query_payload())
    assert result["upload_mbps"] == 0
    assert result["observation"]["target_state"] == "UNKNOWN"
    assert result["valid_for_seconds"] == pytest.approx(29.9)


@pytest.mark.parametrize("change", ["down", "missing", "duplicate", "host", "nan", "future", "old", "mixed", "age", "delay"])
def test_projection_never_renews_old_or_ambiguous_prometheus_evidence(change):
    payload = query_payload();rows = payload["data"]["result"]
    elapsed = .1
    if change == "down": rows[0]["value"][1] = "0"
    if change == "missing": rows.pop()
    if change == "duplicate": rows.append(copy.deepcopy(rows[0]))
    if change == "host": rows[0]["metric"]["host"] = "other"
    if change == "nan": rows[2]["value"][1] = "NaN"
    if change == "future": rows[0]["value"][0] += 60
    if change == "old":
        for row in rows: row["value"][0] -= 60
    if change == "mixed": rows[0]["value"][0] -= 1
    if change == "age": rows[1]["value"][1] = "180"
    if change == "delay": elapsed = 4
    result = project(payload, elapsed=elapsed)
    assert result["upload_mbps"] is None and result["valid_for_seconds"] == 0


def test_saved_projection_has_boot_bound_expiry_even_after_copy_or_utc_step():
    result = project(query_payload())
    original = observation_time.Reading(1800000000, 500, "monitor", "boot", "b"*64)
    result = observation_time.attach(result, "observation_projection", now=original)
    for offset in [-7200, 0, 7200]:
        assert projection.still_valid(copy.deepcopy(result), now=observation_time.Reading(
            original.wall+offset, 510, "monitor", "boot", "b"*64))
    assert not projection.still_valid(result, now=observation_time.Reading(
        original.wall, 531, "monitor", "boot", "b"*64))
    assert not projection.still_valid(result, now=observation_time.Reading(
        original.wall, 510, "monitor", "new-boot", "b"*64))


def test_projection_lifetime_is_limited_by_the_original_sample():
    result = project(query_payload(age=179), elapsed=.25)
    assert result["valid_for_seconds"] == .75
