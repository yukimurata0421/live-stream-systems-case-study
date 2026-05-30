from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_watchdog_config  # type: ignore


class YouTubeWatchdogConfigTests(unittest.TestCase):
    def test_state_root_overrides_token_path(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "STREAM_RUNTIME_STATE_DIR": "/tmp/ytw-state",
            },
            clear=False,
        ):
            mod = importlib.reload(youtube_watchdog_config)
            self.assertEqual(mod.OAUTH_TOKEN_STATE_FILE, "/tmp/ytw-state/youtube_oauth_token_state.json")
            self.assertEqual(mod.QUOTA_STATE_FILE, "/tmp/ytw-state/youtube_quota_state.json")
            self.assertEqual(mod.STATE_FILE, "/tmp/ytw-state/youtube_watchdog_state.json")
            self.assertEqual(mod.LOG_FILE, "/tmp/ytw-state/logs/youtube_watchdog.jsonl")
            self.assertEqual(mod.API_CALL_LOG_FILE, "/tmp/ytw-state/logs/youtube_api_calls.jsonl")
            self.assertEqual(mod.STATS_FILE, "/tmp/ytw-state/youtube_watchdog_stats.json")
            self.assertEqual(
                mod.VIDEO_RESOLVER_STATE_FILE,
                "/tmp/ytw-state/youtube_video_id_resolver_state.json",
            )

    def test_default_token_path_uses_repo_local_v2_state_root(self) -> None:
        with mock.patch.dict(os.environ, {"HOME": "/home/testuser"}, clear=False):
            os.environ.pop("STREAM_RUNTIME_STATE_DIR", None)
            os.environ.pop("YTW_OAUTH_TOKEN_STATE_FILE", None)
            mod = importlib.reload(youtube_watchdog_config)
            self.assertEqual(
                Path(mod.OAUTH_TOKEN_STATE_FILE),
                (ROOT / ".state" / "adsb-streamnew-v2" / "youtube_oauth_token_state.json").resolve(),
            )

    def test_startup_grace_default_and_override(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("YTW_STARTUP_GRACE_SEC", None)
            os.environ.pop("YTW_OK_LOG_EVERY_SEC", None)
            os.environ.pop("YTW_VIDEO_RESOLVER_NORMAL_INTERVAL_SEC", None)
            os.environ.pop("YTW_VIDEO_RESOLVER_UNHEALTHY_INTERVAL_SEC", None)
            os.environ.pop("YTW_VIDEO_RESOLVER_FAST_ENTER_STREAK", None)
            os.environ.pop("YTW_VIDEO_RESOLVER_FAST_EXIT_STREAK", None)
            os.environ.pop("YTW_VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC", None)
            os.environ.pop("YTW_VIDEO_RESOLVER_FAST_SEARCH_WINDOW_SEC", None)
            os.environ.pop("YTW_VIDEO_RESOLVER_FAST_SEARCH_MAX_CALLS", None)
            os.environ.pop("YTW_VIDEO_RESOLVER_REQUIRE_INGEST_FOR_SEARCH", None)
            os.environ.pop("YTW_VIDEO_RESOLVER_FAST_REMOTE_PROBE", None)
            os.environ.pop("YTW_VIDEO_RESOLVER_REFRESH_WATCHDOG_STATS_FAST_MODE", None)
            os.environ.pop("YTW_VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC", None)
            os.environ.pop("YTW_VIDEO_ID_CONFIGURED_FALLBACK_BOOT_SEC", None)
            os.environ.pop("YTW_QUOTA_EXHAUSTED_COOLDOWN_SEC", None)
            os.environ.pop("YTW_QUOTA_RESET_MARGIN_SEC", None)
            os.environ.pop("YTW_API_COST_BURN_RATE_ENABLE", None)
            os.environ.pop("YTW_API_COST_BURN_RATE_THRESHOLD_UNITS_PER_DAY", None)
            os.environ.pop("YTW_API_COST_BURN_RATE_CLOCK_SKEW_ALLOW_SEC", None)
            os.environ.pop("YTW_API_COST_BURN_RATE_OAUTH_MIN_INTERVAL_SEC", None)
            os.environ.pop("YTW_API_COST_BURN_RATE_DATA_API_MIN_INTERVAL_SEC", None)
            os.environ.pop("YTW_API_COST_BURN_RATE_FAIL_CLOSED", None)
            mod = importlib.reload(youtube_watchdog_config)
            self.assertEqual(mod.STARTUP_GRACE_SEC, 30)
            self.assertEqual(mod.OK_LOG_EVERY_SEC, 300)
            self.assertEqual(mod.VIDEO_RESOLVER_NORMAL_INTERVAL_SEC, 45)
            self.assertEqual(mod.VIDEO_RESOLVER_UNHEALTHY_INTERVAL_SEC, 5)
            self.assertEqual(mod.VIDEO_RESOLVER_FAST_ENTER_STREAK, 2)
            self.assertEqual(mod.VIDEO_RESOLVER_FAST_EXIT_STREAK, 3)
            self.assertEqual(mod.VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC, 5)
            self.assertEqual(mod.VIDEO_RESOLVER_FAST_SEARCH_WINDOW_SEC, 180)
            self.assertEqual(mod.VIDEO_RESOLVER_FAST_SEARCH_MAX_CALLS, 36)
            self.assertTrue(mod.VIDEO_RESOLVER_REQUIRE_INGEST_FOR_SEARCH)
            self.assertFalse(mod.VIDEO_RESOLVER_FAST_REMOTE_PROBE)
            self.assertFalse(mod.VIDEO_RESOLVER_REFRESH_WATCHDOG_STATS_FAST_MODE)
            self.assertEqual(mod.VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC, 15)
            self.assertEqual(mod.VIDEO_ID_CONFIGURED_FALLBACK_BOOT_SEC, 120)
            self.assertEqual(mod.QUOTA_EXHAUSTED_COOLDOWN_SEC, 21600)
            self.assertEqual(mod.QUOTA_RESET_MARGIN_SEC, 300)
            self.assertTrue(mod.API_COST_BURN_RATE_ENABLE)
            self.assertEqual(mod.API_COST_BURN_RATE_THRESHOLD_UNITS_PER_DAY, 9000)
            self.assertEqual(mod.API_COST_BURN_RATE_CLOCK_SKEW_ALLOW_SEC, 120)
            self.assertTrue(mod.API_COST_BURN_RATE_FAIL_CLOSED)
            self.assertEqual(mod.API_COST_BURN_RATE_OAUTH_MIN_INTERVAL_SEC, 600)
            self.assertEqual(mod.API_COST_BURN_RATE_DATA_API_MIN_INTERVAL_SEC, 600)
        with mock.patch.dict(os.environ, {"YTW_STARTUP_GRACE_SEC": "45"}, clear=False):
            mod = importlib.reload(youtube_watchdog_config)
            self.assertEqual(mod.STARTUP_GRACE_SEC, 45)
        with mock.patch.dict(os.environ, {"YTW_OK_LOG_EVERY_SEC": "600"}, clear=False):
            mod = importlib.reload(youtube_watchdog_config)
            self.assertEqual(mod.OK_LOG_EVERY_SEC, 600)
        with mock.patch.dict(os.environ, {"YTW_QUOTA_EXHAUSTED_COOLDOWN_SEC": "7200"}, clear=False):
            mod = importlib.reload(youtube_watchdog_config)
            self.assertEqual(mod.QUOTA_EXHAUSTED_COOLDOWN_SEC, 7200)
        with mock.patch.dict(os.environ, {"YTW_QUOTA_RESET_MARGIN_SEC": "600"}, clear=False):
            mod = importlib.reload(youtube_watchdog_config)
            self.assertEqual(mod.QUOTA_RESET_MARGIN_SEC, 600)
        with mock.patch.dict(
            os.environ,
            {
                "YTW_API_COST_BURN_RATE_ENABLE": "0",
                "YTW_API_COST_BURN_RATE_THRESHOLD_UNITS_PER_DAY": "8000",
                "YTW_API_COST_BURN_RATE_CLOCK_SKEW_ALLOW_SEC": "30",
                "YTW_API_COST_BURN_RATE_FAIL_CLOSED": "0",
                "YTW_API_COST_BURN_RATE_OAUTH_MIN_INTERVAL_SEC": "900",
                "YTW_API_COST_BURN_RATE_DATA_API_MIN_INTERVAL_SEC": "1200",
            },
            clear=False,
        ):
            mod = importlib.reload(youtube_watchdog_config)
            self.assertFalse(mod.API_COST_BURN_RATE_ENABLE)
            self.assertEqual(mod.API_COST_BURN_RATE_THRESHOLD_UNITS_PER_DAY, 8000)
            self.assertEqual(mod.API_COST_BURN_RATE_CLOCK_SKEW_ALLOW_SEC, 30)
            self.assertFalse(mod.API_COST_BURN_RATE_FAIL_CLOSED)
            self.assertEqual(mod.API_COST_BURN_RATE_OAUTH_MIN_INTERVAL_SEC, 900)
            self.assertEqual(mod.API_COST_BURN_RATE_DATA_API_MIN_INTERVAL_SEC, 1200)
        with mock.patch.dict(
            os.environ,
            {
                "YTW_VIDEO_RESOLVER_NORMAL_INTERVAL_SEC": "90",
                "YTW_VIDEO_RESOLVER_UNHEALTHY_INTERVAL_SEC": "5",
                "YTW_VIDEO_RESOLVER_FAST_ENTER_STREAK": "4",
                "YTW_VIDEO_RESOLVER_FAST_EXIT_STREAK": "6",
                "YTW_VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC": "7",
                "YTW_VIDEO_RESOLVER_FAST_SEARCH_WINDOW_SEC": "480",
                "YTW_VIDEO_RESOLVER_FAST_SEARCH_MAX_CALLS": "15",
                "YTW_VIDEO_RESOLVER_REQUIRE_INGEST_FOR_SEARCH": "0",
                "YTW_VIDEO_RESOLVER_FAST_REMOTE_PROBE": "1",
                "YTW_VIDEO_RESOLVER_REFRESH_WATCHDOG_STATS_FAST_MODE": "1",
                "YTW_VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC": "30",
                "YTW_VIDEO_ID_CONFIGURED_FALLBACK_BOOT_SEC": "240",
            },
            clear=False,
        ):
            mod = importlib.reload(youtube_watchdog_config)
            self.assertEqual(mod.VIDEO_RESOLVER_NORMAL_INTERVAL_SEC, 90)
            self.assertEqual(mod.VIDEO_RESOLVER_UNHEALTHY_INTERVAL_SEC, 5)
            self.assertEqual(mod.VIDEO_RESOLVER_FAST_ENTER_STREAK, 4)
            self.assertEqual(mod.VIDEO_RESOLVER_FAST_EXIT_STREAK, 6)
            self.assertEqual(mod.VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC, 7)
            self.assertEqual(mod.VIDEO_RESOLVER_FAST_SEARCH_WINDOW_SEC, 480)
            self.assertEqual(mod.VIDEO_RESOLVER_FAST_SEARCH_MAX_CALLS, 15)
            self.assertFalse(mod.VIDEO_RESOLVER_REQUIRE_INGEST_FOR_SEARCH)
            self.assertTrue(mod.VIDEO_RESOLVER_FAST_REMOTE_PROBE)
            self.assertTrue(mod.VIDEO_RESOLVER_REFRESH_WATCHDOG_STATS_FAST_MODE)
            self.assertEqual(mod.VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC, 30)
            self.assertEqual(mod.VIDEO_ID_CONFIGURED_FALLBACK_BOOT_SEC, 240)


if __name__ == "__main__":
    unittest.main()
