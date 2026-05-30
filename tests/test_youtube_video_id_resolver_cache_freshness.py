from __future__ import annotations

import importlib
import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_video_id_resolver  # type: ignore


class YouTubeVideoIdResolverCacheFreshnessTests(unittest.TestCase):
    def test_oauth_cache_uses_checked_ts_not_stats_ts(self) -> None:
        mod = importlib.reload(youtube_video_id_resolver)
        payload = {
            "ts_utc": "2026-05-05T09:59:00Z",
            "oauth_checked_ts_utc": "2026-05-05T09:30:00Z",
            "oauth_probe_ok": True,
            "oauth_healthy": True,
            "oauth_reason": "old oauth",
        }
        now_ts = mod.parse_iso_ts("2026-05-05T10:00:00Z")
        cached = mod.oauth_from_watchdog_stats_cache(payload, now_ts, max_age_sec=300)
        self.assertIsNone(cached)

    def test_data_api_cache_uses_checked_ts_not_stats_ts(self) -> None:
        mod = importlib.reload(youtube_video_id_resolver)
        payload = {
            "ts_utc": "2026-05-05T09:59:00Z",
            "data_api_checked_ts_utc": "2026-05-05T09:30:00Z",
            "video_id": "VID123",
            "api_reason": "old data api",
            "api_live_state": "live",
        }
        now_ts = mod.parse_iso_ts("2026-05-05T10:00:00Z")
        reused, _, _ = mod.data_api_from_watchdog_stats_cache(
            payload,
            now_ts=now_ts,
            max_age_sec=300,
            selected_video_id="VID123",
        )
        self.assertFalse(reused)


if __name__ == "__main__":
    unittest.main()
