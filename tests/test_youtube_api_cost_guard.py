from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_watchdog_config  # type: ignore
import youtube_api_cost_guard  # type: ignore


class YouTubeApiCostGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.snapshot = Path(self._tmpdir.name) / "open_day_latest.json"
        self._env = mock.patch.dict(
            os.environ,
            {
                "YTW_API_COST_BURN_RATE_ENABLE": "1",
                "YTW_API_COST_BURN_RATE_LATEST_FILE": str(self.snapshot),
                "YTW_API_COST_BURN_RATE_THRESHOLD_UNITS_PER_DAY": "9000",
                "YTW_API_COST_BURN_RATE_MIN_ELAPSED_SEC": "300",
                "YTW_API_COST_BURN_RATE_MAX_AGE_SEC": "3600",
            },
            clear=False,
        )
        self._env.start()
        importlib.reload(youtube_watchdog_config)
        importlib.reload(youtube_api_cost_guard)

    def tearDown(self) -> None:
        self._env.stop()
        self._tmpdir.cleanup()

    def _write_snapshot(self, *, units: int, start: datetime, end: datetime, open_day: bool = True) -> None:
        payload = {
            "status": "ok",
            "target_day": "2026-05-05",
            "window": {
                "tz": "America/Los_Angeles",
                "open_day": open_day,
                "start_utc": start.isoformat().replace("+00:00", "Z"),
                "end_utc": (start + timedelta(days=1)).isoformat().replace("+00:00", "Z"),
                "effective_end_utc": end.isoformat().replace("+00:00", "Z"),
                "lag_sec": 120,
            },
            "totals": {"calls": 100, "units": units, "quota_exceeded_events": 0},
        }
        self.snapshot.write_text(json.dumps(payload), encoding="utf-8")

    def test_activates_when_projected_units_exceed_threshold(self) -> None:
        start = datetime(2026, 5, 5, 7, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(hours=2)
        self._write_snapshot(units=1000, start=start, end=end)
        now_ts = int((end + timedelta(minutes=5)).timestamp())
        status = youtube_api_cost_guard.load_api_cost_burn_rate_status(now_ts)
        self.assertTrue(status.active)
        self.assertGreaterEqual(status.projected_units_per_day, 9000)

    def test_ignores_non_open_day_snapshot(self) -> None:
        start = datetime(2026, 5, 4, 7, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(hours=6)
        self._write_snapshot(units=1000, start=start, end=end, open_day=False)
        now_ts = int((end + timedelta(minutes=5)).timestamp())
        status = youtube_api_cost_guard.load_api_cost_burn_rate_status(now_ts)
        self.assertTrue(status.active)
        self.assertIn("fail-closed active", status.reason)

    def test_non_open_day_snapshot_is_not_active_when_fail_closed_disabled(self) -> None:
        with mock.patch.dict(os.environ, {"YTW_API_COST_BURN_RATE_FAIL_CLOSED": "0"}, clear=False):
            importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_api_cost_guard)
            start = datetime(2026, 5, 4, 7, 0, 0, tzinfo=timezone.utc)
            end = start + timedelta(hours=6)
            self._write_snapshot(units=1000, start=start, end=end, open_day=False)
            now_ts = int((end + timedelta(minutes=5)).timestamp())
            status = mod.load_api_cost_burn_rate_status(now_ts)
            self.assertFalse(status.active)
            self.assertIn("not open-day", status.reason)

    def test_future_effective_end_is_treated_as_degraded(self) -> None:
        start = datetime(2026, 5, 5, 7, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(hours=2)
        self._write_snapshot(units=1000, start=start, end=end)
        now_ts = int((end - timedelta(minutes=5)).timestamp())
        status = youtube_api_cost_guard.load_api_cost_burn_rate_status(now_ts)
        self.assertTrue(status.active)
        self.assertIn("from future", status.reason)


if __name__ == "__main__":
    unittest.main()
