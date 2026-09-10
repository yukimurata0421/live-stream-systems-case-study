from __future__ import annotations

import copy
from datetime import UTC, datetime, timedelta

import pytest

from tools.monitor_bounded_termination_external import assess


def healthy() -> dict:
    stamp = "2026-09-03T22:00:00+00:00"
    return {
        "remote_now": stamp,
        "boot_id": "47493c1e-ec47-4a1e-b4b5-3afb0c75631c",
        "release": "/opt/stream-v3/releases/example-monitoring-release",
        "service": "active",
        "youtube_watchdog_stats.json": {
            "ts_utc": stamp,
            "oauth_checked_ts_utc": stamp,
            "status": "ok",
            "healthy": True,
            "api_live_state": "live",
            "action": "none",
            "oauth_probe_ok": True,
            "oauth_stream_status": "active",
            "oauth_stream_health_status": "good",
        },
        "viewer_synthetic_status.json": {
            "checked_at_utc": stamp,
            "status": "healthy",
            "frame_ok": True,
            "black_detected": False,
            "freeze_detected": False,
            "consecutive_probe_failures": 0,
            "consecutive_visual_failures": 0,
        },
    }


def test_fresh_healthy_raw_evidence_passes_without_mutation() -> None:
    raw = healthy()
    before = copy.deepcopy(raw)
    assert assess(raw, datetime.fromisoformat(raw["remote_now"]))[0] == []
    assert raw == before


@pytest.mark.parametrize("status,health", [("inactive", "noData"), ("active", "noData"), (None, None)])
def test_aggregate_healthy_cannot_mask_unhealthy_raw_ingest(status: str | None, health: str | None) -> None:
    raw = healthy()
    raw["youtube_watchdog_stats.json"].update(oauth_stream_status=status, oauth_stream_health_status=health)
    assert "YOUTUBE_INGEST_NOT_HEALTHY" in assess(raw, datetime.fromisoformat(raw["remote_now"]))[0]


@pytest.mark.parametrize(
    "component,key,seconds,reason",
    [
        ("youtube_watchdog_stats.json", "ts_utc", 121, "YOUTUBE_STALE"),
        ("youtube_watchdog_stats.json", "oauth_checked_ts_utc", 241, "OAUTH_STALE"),
        ("viewer_synthetic_status.json", "checked_at_utc", 361, "VIEWER_STALE"),
        ("viewer_synthetic_status.json", "checked_at_utc", -2, "VIEWER_STALE"),
    ],
)
def test_stale_or_future_source_is_not_healthy(component: str, key: str, seconds: int, reason: str) -> None:
    raw = healthy()
    now = datetime(2026, 9, 3, 22, tzinfo=UTC)
    raw[component][key] = (now - timedelta(seconds=seconds)).isoformat()
    assert reason in assess(raw, now)[0]


def test_frozen_viewer_fails_even_if_reported_status_is_healthy() -> None:
    raw = healthy()
    raw["viewer_synthetic_status.json"]["freeze_detected"] = True
    assert "VIEWER_NOT_HEALTHY" in assess(raw, datetime.fromisoformat(raw["remote_now"]))[0]


@pytest.mark.parametrize("stamp", [None, "", "invalid", "2026-09-03T22:00:00"])
def test_missing_or_malformed_oauth_time_is_evidence_failure_not_monitor_exception(stamp: str | None) -> None:
    raw = healthy()
    raw["youtube_watchdog_stats.json"]["oauth_checked_ts_utc"] = stamp
    reasons, ages = assess(raw, datetime.fromisoformat(raw["remote_now"]))
    assert "OAUTH_TIMESTAMP_INVALID" in reasons
    assert ages["oauth"] is None
