from __future__ import annotations

import importlib
import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_watchdog  # type: ignore


class YouTubeWatchdogCacheFreshnessTests(unittest.TestCase):
    def test_oauth_cache_uses_checked_ts_not_stats_ts(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        payload = {
            "ts_utc": "2026-05-05T09:59:00Z",
            "oauth_checked_ts_utc": "2026-05-05T09:30:00Z",
            "oauth_enabled": True,
            "oauth_configured": True,
            "oauth_probe_ok": True,
            "oauth_healthy": True,
            "oauth_reason": "old but appears fresh by ts_utc",
        }
        now_ts = mod.parse_iso_ts("2026-05-05T10:00:00Z")
        cached = mod.oauth_from_stats_cache(payload, now_ts, max_age_sec=300)
        self.assertIsNone(cached)

    def test_data_api_cache_uses_checked_ts_not_stats_ts(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        payload = {
            "ts_utc": "2026-05-05T09:59:00Z",
            "data_api_checked_ts_utc": "2026-05-05T09:30:00Z",
            "video_id": "VID123",
            "api_ok": True,
            "api_reason": "old api state",
            "api_live_state": "live",
        }
        now_ts = mod.parse_iso_ts("2026-05-05T10:00:00Z")
        reused, *_ = mod.data_api_from_stats_cache(
            payload,
            now_ts=now_ts,
            max_age_sec=300,
            selected_video_id="VID123",
        )
        self.assertFalse(reused)

    def test_cache_falls_back_to_stats_ts_when_checked_ts_missing(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        payload = {
            "ts_utc": "2026-05-05T09:59:00Z",
            "oauth_enabled": True,
            "oauth_configured": True,
            "oauth_probe_ok": True,
            "oauth_healthy": True,
            "oauth_reason": "legacy payload",
        }
        now_ts = mod.parse_iso_ts("2026-05-05T10:00:00Z")
        cached = mod.oauth_from_stats_cache(payload, now_ts, max_age_sec=300)
        self.assertIsNotNone(cached)

    def test_oauth_cache_requires_oauth_fields(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        payload = {
            "ts_utc": "2026-05-05T09:59:00Z",
            "status": "startup_grace",
            "healthy": True,
        }
        now_ts = mod.parse_iso_ts("2026-05-05T10:00:00Z")
        cached = mod.oauth_from_stats_cache(payload, now_ts, max_age_sec=300)
        self.assertIsNone(cached)


if __name__ == "__main__":
    unittest.main()
