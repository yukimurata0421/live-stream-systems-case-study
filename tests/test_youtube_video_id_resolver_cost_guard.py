from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_watchdog_config  # type: ignore
import youtube_video_id_resolver  # type: ignore


class YouTubeVideoIdResolverCostGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._env = mock.patch.dict(
            os.environ,
            {
                "STREAM_RUNTIME_STATE_DIR": self._tmpdir.name,
                "YTW_VIDEO_ID": "VID_CONFIG",
                "YTW_API_KEY": "APIKEY",
                "YTW_CHANNEL_ID": "UC123",
                "YTW_CHANNEL_LIVE_URL": "",
            },
            clear=False,
        )
        self._env.start()
        importlib.reload(youtube_watchdog_config)
        importlib.reload(youtube_video_id_resolver)

    def tearDown(self) -> None:
        self._env.stop()
        self._tmpdir.cleanup()

    def test_guard_defers_oauth_probe_and_data_api_check_in_normal_mode(self) -> None:
        mod = importlib.reload(youtube_video_id_resolver)
        captured: dict = {}

        def save_hook(payload: dict) -> None:
            captured.clear()
            captured.update(payload)

        guard_state = mock.Mock(
            active=True,
            reason="projected units/day 11000>=9000",
            projected_units_per_day=11000,
            threshold_units_per_day=9000,
        )

        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
            with mock.patch.object(mod, "load_api_cost_burn_rate_status", return_value=guard_state):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                        with mock.patch.object(mod, "load_video_resolver_state", return_value={}):
                            with mock.patch.object(mod, "quota_guard_status", return_value=(False, "inactive", {})):
                                with mock.patch.object(
                                    mod,
                                    "resolve_video_id_from_live_page",
                                    return_value=("", "missing live page"),
                                ):
                                    with mock.patch.object(mod, "resolve_live_video_id", return_value=("", "gated")):
                                        with mock.patch.object(mod, "probe_with_oauth") as oauth_mock:
                                            with mock.patch.object(mod, "check_data_api") as api_mock:
                                                with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                    self.assertEqual(mod.main(), 0)
        oauth_mock.assert_not_called()
        api_mock.assert_not_called()
        self.assertEqual(captured.get("api_live_state"), "deferred")
        self.assertIn("api cost burn guard", str(captured.get("api_reason", "")))
        self.assertTrue(bool(captured.get("api_cost_burn_rate_active")))

    def test_guard_defers_oauth_probe_and_data_api_check_in_fast_mode(self) -> None:
        mod = importlib.reload(youtube_video_id_resolver)
        captured: dict = {}

        def save_hook(payload: dict) -> None:
            captured.clear()
            captured.update(payload)

        guard_state = mock.Mock(
            active=True,
            reason="projected units/day 11000>=9000",
            projected_units_per_day=11000,
            threshold_units_per_day=9000,
        )

        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
            with mock.patch.object(mod, "load_api_cost_burn_rate_status", return_value=guard_state):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(True, "runtime tcp disconnected")):
                    with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                        with mock.patch.object(
                            mod,
                            "load_video_resolver_state",
                            return_value={
                                "fast_mode_active": True,
                                "fast_mode_bad_streak": 3,
                                "fast_mode_good_streak": 0,
                            },
                        ):
                            with mock.patch.object(mod, "quota_guard_status", return_value=(False, "inactive", {})):
                                with mock.patch.object(
                                    mod,
                                    "resolve_video_id_from_live_page",
                                    return_value=("", "missing live page"),
                                ):
                                    with mock.patch.object(mod, "resolve_live_video_id", return_value=("", "gated")):
                                        with mock.patch.object(mod, "probe_with_oauth") as oauth_mock:
                                            with mock.patch.object(mod, "check_data_api") as api_mock:
                                                with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                    self.assertEqual(mod.main(), 0)
        oauth_mock.assert_not_called()
        api_mock.assert_not_called()
        self.assertEqual(captured.get("api_live_state"), "deferred")
        self.assertIn("api cost burn guard", str(captured.get("api_reason", "")))

    def test_guard_still_uses_fresh_watchdog_oauth_cache_before_defer(self) -> None:
        mod = importlib.reload(youtube_video_id_resolver)
        captured: dict = {}

        def save_hook(payload: dict) -> None:
            captured.clear()
            captured.update(payload)

        guard_state = mock.Mock(
            active=True,
            reason="projected units/day 11000>=9000",
            projected_units_per_day=11000,
            threshold_units_per_day=9000,
        )
        stats = {
            "ts_utc": "2026-05-05T09:30:00Z",
            "oauth_checked_ts_utc": "2026-05-05T09:30:00Z",
            "oauth_enabled": True,
            "oauth_configured": True,
            "oauth_probe_ok": True,
            "oauth_healthy": True,
            "oauth_reason": "oauth cached",
            "oauth_video_id": "VID_OAUTH_CACHE",
            "oauth_broadcast_id": "BCAST1",
            "oauth_life_cycle_status": "live",
            "oauth_bound_stream_id": "STREAM1",
            "oauth_stream_status": "active",
            "oauth_stream_health_status": "good",
            "oauth_stream_health_issues": 0,
        }

        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
            with mock.patch.object(mod, "load_api_cost_burn_rate_status", return_value=guard_state):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(mod, "load_watchdog_stats", return_value=stats):
                        with mock.patch.object(mod, "load_video_resolver_state", return_value={}):
                            with mock.patch.object(mod, "quota_guard_status", return_value=(False, "inactive", {})):
                                with mock.patch.object(
                                    mod,
                                    "resolve_video_id_from_live_page",
                                    return_value=("", "missing live page"),
                                ):
                                    with mock.patch.object(mod, "resolve_live_video_id", return_value=("", "gated")):
                                        with mock.patch.object(mod, "probe_with_oauth") as oauth_mock:
                                            with mock.patch.object(mod, "check_data_api") as api_mock:
                                                with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                    self.assertEqual(mod.main(), 0)
        oauth_mock.assert_not_called()
        api_mock.assert_not_called()
        self.assertEqual(captured.get("source"), "oauth")


if __name__ == "__main__":
    unittest.main()
