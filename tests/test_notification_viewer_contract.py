"""Notification decisions consume the actual viewer producer's result schema."""
import importlib.util
import json
from pathlib import Path

import pytest

from stream_core.common import observation_time as timing
from stream_core.common import viewer_evidence as viewer
from stream_core.notifications import observation

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("viewer_notification_producer", ROOT / "ops/scripts/stream_v3_viewer_synthetic_probe.py")
producer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(producer)


def reading(t=0):
    return timing.Reading(1_800_000_000. + t, 1000. + t, "fixture", "boot-1", "a" * 64)


def produced(kind="healthy", version=2, t=0):
    capture = {"frame_ok": True, "black_detected": kind == "black", "capture_sha256": "picture"}
    previous = {"consecutive_probe_failures": 1, "consecutive_visual_failures": 1}
    if kind == "frozen":
        previous["capture_sha256"] = "picture"
    result = producer.evaluate_result(previous, checked_at_utc="2027-01-15T08:00:00Z", video_id="fixture",
        api_live_state="live", capture={} if kind == "probe_failed" else capture,
        error="capture_failed" if kind == "probe_failed" else "", duration_sec=1.)
    result["schema"] = f"stream_v3.viewer_synthetic.v{version}"
    return timing.attach(result, "viewer_synthetic", now=reading(t))


@pytest.mark.parametrize("version", [1, 2])
@pytest.mark.parametrize("kind,probe,visual", [("healthy", True, True), ("black", True, False),
                                               ("frozen", True, False), ("probe_failed", False, None)])
def test_actual_producer_schema_keeps_capture_failure_and_visual_failure_distinct(tmp_path, version, kind, probe, visual):
    payload = produced(kind, version)
    assert "probe_ok" not in payload
    assert viewer.probe_state(payload) is probe and viewer.visual_state(payload) is visual
    assert observation.complete("viewer_synthetic", payload)
    assert observation.normal_for("viewer:synthetic_missing_or_stale", "viewer_synthetic", payload, {})
    assert observation.normal_for("viewer:synthetic_probe_failed", "viewer_synthetic", payload, {}) is probe
    assert observation.normal_for("viewer:visual_failure", "viewer_synthetic", payload, {}) is (visual is True)
    (tmp_path / "viewer_synthetic_status.json").write_text(json.dumps(payload))
    ident = "viewer:synthetic_probe_failed" if kind == "probe_failed" else "viewer:visual_failure"
    items = [] if kind == "healthy" else [{"id": ident, "severity": "warning"}]
    rows = observation.collect(tmp_path, items, {}, now=reading())
    sample, verdict = observation.evidence(rows.observations, ident, now=reading())
    assert sample.valid and verdict == ("GOOD" if kind == "healthy" else "BAD")


@pytest.mark.parametrize("field,value", [("consecutive_probe_failures", None), ("consecutive_probe_failures", True),
    ("consecutive_probe_failures", -1), ("consecutive_probe_failures", 0.), ("schema", "unrecognized"),
    ("frame_ok", 1), ("probe_ok", "true")])
def test_partial_or_malformed_capture_evidence_stays_unknown(field, value):
    payload = {**produced(), field: value}
    assert viewer.probe_state(payload) is None
    assert not observation.complete("viewer_synthetic", payload)


def test_explicit_capture_failure_cannot_be_overridden_by_a_healthy_status():
    payload = {**produced(), "probe_ok": False}
    assert viewer.probe_state(payload) is False and viewer.visual_state(payload) is None


def test_a_captured_frame_without_visual_evidence_cannot_confirm_visual_recovery():
    payload = produced()
    for field in ("black_detected", "freeze_detected", "consecutive_visual_failures"):
        payload.pop(field)
    assert viewer.probe_state(payload) is True and viewer.visual_state(payload) is None
    assert not observation.normal_for("viewer:visual_failure", "viewer_synthetic", payload, {})


@pytest.mark.parametrize("ident", ["viewer:synthetic_missing_or_stale", "viewer:synthetic_probe_failed", "viewer:visual_failure"])
def test_two_actual_normal_producer_observations_at_300_second_cadence_close_the_episode(tmp_path, ident):
    incident = {"id": ident, "severity": "warning"}
    active = {ident: {"last_incident": incident}}
    state = {}
    for t, expected_open in [(300, True), (600, False)]:
        (tmp_path / "viewer_synthetic_status.json").write_text(json.dumps(produced(t=t)))
        rows = observation.collect(tmp_path, [], {}, now=reading(t))
        result = observation.reconcile([], active, state, rows.observations, now=reading(t), config={})
        assert (ident in {item["id"] for item in result}) is expected_open
    assert ident in state["observation_recoveries"]
