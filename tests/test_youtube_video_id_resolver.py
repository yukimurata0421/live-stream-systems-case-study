from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_watchdog_config  # type: ignore
import youtube_watchdog_state  # type: ignore
import youtube_api  # type: ignore
import youtube_api_cost_guard  # type: ignore
import youtube_video_id_resolver  # type: ignore


class YouTubeVideoIdResolverTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._env_patcher = mock.patch.dict(
            os.environ,
            {
                "STREAM_RUNTIME_STATE_DIR": self._tmpdir.name,
                "YTW_QUOTA_STATE_FILE": str(Path(self._tmpdir.name) / "youtube_quota_state.json"),
                "YTW_API_COST_BURN_RATE_ENABLE": "0",
            },
            clear=False,
        )
        self._env_patcher.start()
        importlib.reload(youtube_watchdog_config)
        importlib.reload(youtube_watchdog_state)
        importlib.reload(youtube_api)
        importlib.reload(youtube_api_cost_guard)
        importlib.reload(youtube_video_id_resolver)

    def tearDown(self) -> None:
        self._env_patcher.stop()
        self._tmpdir.cleanup()

    def test_skips_when_interval_not_elapsed_in_fast_mode(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "load_video_resolver_state", return_value={"last_attempt_ts": 100}):
                    with mock.patch.object(mod.time, "time", return_value=103):
                        with mock.patch.object(mod, "resolve_fast_mode", return_value=(True, "runtime tcp disconnected")):
                            with mock.patch.object(mod, "probe_with_oauth") as probe:
                                self.assertEqual(mod.main(), 0)
                                probe.assert_not_called()
            self.assertEqual(cfg.VIDEO_RESOLVER_UNHEALTHY_INTERVAL_SEC, 5)
            self.assertEqual(cfg.VIDEO_RESOLVER_NORMAL_INTERVAL_SEC, 45)

    def test_resolve_fast_mode_prefers_runtime_tcp_signal(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            mod = importlib.reload(youtube_video_id_resolver)
            with mock.patch.object(mod, "detect_ingest_connected_now", return_value=(False, "runtime tcp disconnected")):
                fast_mode, reason = mod.resolve_fast_mode({})
                self.assertTrue(fast_mode)
                self.assertIn("runtime tcp disconnected", reason)

            with mock.patch.object(mod, "detect_ingest_connected_now", return_value=(True, "runtime tcp connected")):
                fast_mode, reason = mod.resolve_fast_mode({})
                self.assertFalse(fast_mode)
                self.assertIn("runtime tcp connected", reason)

    def test_resolve_fast_mode_falls_back_to_stats(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            mod = importlib.reload(youtube_video_id_resolver)
            with mock.patch.object(mod, "detect_ingest_connected_now", return_value=(None, "ffmpeg child pid unavailable")):
                fast_mode, reason = mod.resolve_fast_mode({"ingest_connected": False})
                self.assertTrue(fast_mode)
                self.assertIn("stats fallback", reason)

            with mock.patch.object(mod, "detect_ingest_connected_now", return_value=(None, "ffmpeg child pid unavailable")):
                fast_mode, reason = mod.resolve_fast_mode({"ingest_connected": True})
                self.assertFalse(fast_mode)
                self.assertIn("stats fallback", reason)

    def test_fast_mode_remote_refresh_updates_watchdog_stats_payload(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_VIDEO_RESOLVER_REFRESH_WATCHDOG_STATS_FAST_MODE": "1",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=True,
                healthy=True,
                reason="oauth live",
                mode="shadow",
                life_cycle_status="live",
                broadcast_id="BID123",
                video_id="VID123",
                channel_id="UC123",
                bound_stream_id="STREAM123",
                stream_status="active",
                stream_health_status="good",
                remote_checked=True,
            )
            captured: dict = {}

            with mock.patch.object(mod, "update_stats", side_effect=lambda payload: captured.update(payload)):
                refreshed = mod.refresh_watchdog_stats_from_fast_mode(
                    fast_mode=True,
                    local_runtime={
                        "stream_active": True,
                        "ffmpeg_pid": 222,
                        "ffmpeg_uptime_sec": 25,
                        "ingest_connected": True,
                        "ingest_connection": "ESTAB ...",
                        "local_ok": True,
                    },
                    selected_video_id="VID123",
                    selected_source="oauth",
                    search_reason="resolved from oauth",
                    oauth=oauth,
                    api_checked=True,
                    api_ok=True,
                    api_reason="data api says live",
                    api_live_state="live",
                    data_api_checked_ts_utc="2026-05-07T23:00:00Z",
                    fast_mode_reason="runtime tcp disconnected; mode=fast",
                    recovery_episode_id="",
                    quota_guard_active=False,
                    quota_guard_reason="quota state inactive",
                    api_cost_guard=SimpleNamespace(
                        active=False,
                        reason="disabled",
                        projected_units_per_day=0,
                        threshold_units_per_day=9000,
                    ),
                )

            self.assertTrue(refreshed)
            self.assertEqual(captured.get("status"), "ok")
            self.assertEqual(captured.get("judgment"), "ok")
            self.assertEqual(captured.get("health_source"), "resolver_fast_remote_refresh")
            self.assertEqual(captured.get("remote_source"), "data_api_oauth")
            self.assertEqual(captured.get("remote_probe_source"), "data_api_oauth")
            self.assertEqual(captured.get("remote_sample_source"), "data_api_oauth")
            self.assertEqual(captured.get("remote_probe_ts_utc"), captured.get("oauth_checked_ts_utc"))
            self.assertRegex(str(captured.get("remote_sample_id", "")), r"^rps-[0-9a-f]{16}$")
            self.assertEqual(captured.get("recovery_episode_id"), "")
            self.assertEqual(captured.get("ffmpeg_generation"), "stream_pid=0:ffmpeg_pid=222")
            self.assertEqual(captured.get("remote_status"), "ok")
            self.assertTrue(captured.get("local_ok"))
            self.assertTrue(captured.get("oauth_ok"))
            self.assertTrue(captured.get("api_ok"))

    def test_fast_mode_remote_refresh_does_not_warn_on_oauth_failure_when_api_live(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_VIDEO_RESOLVER_REFRESH_WATCHDOG_STATS_FAST_MODE": "1",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="oauth transient error",
                mode="shadow",
                remote_checked=True,
            )
            captured: dict = {}

            with mock.patch.object(mod, "update_stats", side_effect=lambda payload: captured.update(payload)):
                refreshed = mod.refresh_watchdog_stats_from_fast_mode(
                    fast_mode=True,
                    local_runtime={
                        "stream_active": True,
                        "ffmpeg_pid": 222,
                        "ffmpeg_uptime_sec": 25,
                        "ingest_connected": True,
                        "ingest_connection": "ESTAB ...",
                        "local_ok": True,
                    },
                    selected_video_id="VID123",
                    selected_source="resolver_cache",
                    search_reason="resolved from cache",
                    oauth=oauth,
                    api_checked=True,
                    api_ok=True,
                    api_reason="data api says live",
                    api_live_state="live",
                    data_api_checked_ts_utc="2026-05-07T23:00:00Z",
                    fast_mode_reason="runtime tcp disconnected; mode=fast",
                    recovery_episode_id="fast-1777938250",
                    quota_guard_active=False,
                    quota_guard_reason="quota state inactive",
                    api_cost_guard=SimpleNamespace(
                        active=False,
                        reason="disabled",
                        projected_units_per_day=0,
                        threshold_units_per_day=9000,
                    ),
                )

            self.assertTrue(refreshed)
            self.assertEqual(captured.get("remote_status"), "ok")
            self.assertEqual(captured.get("status"), "ok")
            self.assertTrue(captured.get("api_ok"))
            self.assertFalse(captured.get("oauth_ok"))

    def test_candidate_new_url_is_held_during_url_preservation_window(self) -> None:
        mod = importlib.reload(youtube_video_id_resolver)
        selected, source, details = mod.choose_video_candidate(
            [("NEWVID", "channel_live_page"), ("OLDVID", "resolver_cache")],
            expected_video_id="OLDVID",
            url_preservation_active=True,
        )
        self.assertEqual((selected, source), ("OLDVID", "resolver_cache"))
        self.assertEqual(details["selected_candidate_policy"], "preserve_expected_url")
        self.assertFalse(details["candidate_new_url_found"])

    def test_candidate_new_url_is_logged_but_not_promoted_without_expected_confirmation(self) -> None:
        mod = importlib.reload(youtube_video_id_resolver)
        selected, source, details = mod.choose_video_candidate(
            [("NEWVID", "channel_live_page")],
            expected_video_id="OLDVID",
            url_preservation_active=True,
        )
        self.assertEqual((selected, source), ("OLDVID", "expected_url_preserved"))
        self.assertTrue(details["candidate_new_url_found"])
        self.assertEqual(details["candidate_new_video_id"], "NEWVID")
        self.assertEqual(details["candidate_new_video_source"], "channel_live_page")

    def test_candidate_new_url_is_promoted_after_url_preservation_window(self) -> None:
        mod = importlib.reload(youtube_video_id_resolver)
        selected, source, details = mod.choose_video_candidate(
            [("NEWVID", "channel_live_page")],
            expected_video_id="OLDVID",
            url_preservation_active=False,
        )
        self.assertEqual((selected, source), ("NEWVID", "channel_live_page"))
        self.assertTrue(details["candidate_new_url_found"])
        self.assertEqual(details["selected_candidate_policy"], "new_url_allowed_after_window")

    def test_oauth_video_id_is_preferred(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=True,
                healthy=True,
                reason="ok",
                mode="shadow",
                life_cycle_status="live",
                broadcast_id="BID123",
                video_id="VID_OAUTH",
                channel_id="UC123",
                bound_stream_id="STREAM123",
                stream_status="active",
                stream_health_status="good",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(True, "runtime tcp disconnected")):
                    with mock.patch.object(mod, "load_video_resolver_state", return_value={}):
                        with mock.patch.object(mod.time, "time", return_value=200):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id", return_value=("VID_API", "ok")):
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("VID_LIVEPAGE", "ok"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            self.assertEqual(captured.get("video_id"), "VID_OAUTH")
            self.assertEqual(captured.get("source"), "oauth")

    def test_live_page_redirect_is_tried_before_data_api_and_can_skip_it(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_CHANNEL_LIVE_URL": "https://www.youtube.com/channel/UC123/live",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="oauth unavailable",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}
            call_order: list[str] = []

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            def live_page_side_effect(_url: str, **_kwargs: object) -> tuple[str, str]:
                call_order.append("live_page")
                return "VID_LIVEPAGE", "resolved from channel live redirect"

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(True, "runtime tcp disconnected")):
                    with mock.patch.object(mod, "load_video_resolver_state", return_value={}):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_video_id_from_live_page", side_effect=live_page_side_effect):
                                    with mock.patch.object(mod, "resolve_live_video_id") as search_mock:
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            self.assertEqual(call_order, ["live_page"])
            search_mock.assert_not_called()
            self.assertEqual(captured.get("source"), "channel_live_page")
            self.assertIn("skipped: channel live page resolved", str(captured.get("data_api_search_reason", "")))

    def test_live_page_html_is_weak_and_data_api_search_can_override_it(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_CHANNEL_LIVE_URL": "https://www.youtube.com/channel/UC123/live",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="oauth unavailable",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(mod, "load_video_resolver_state", return_value={}):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(
                                    mod,
                                    "resolve_video_id_from_live_page",
                                    return_value=("VID_HTML", "resolved from channel live html"),
                                ):
                                    with mock.patch.object(
                                        mod,
                                        "resolve_live_video_id",
                                        return_value=("VID_SEARCH", "live search resolved video id"),
                                    ) as search_mock:
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_called_once()
            self.assertEqual(captured.get("video_id"), "VID_SEARCH")
            self.assertEqual(captured.get("source"), "data_api_search")

    def test_quota_guard_bypasses_oauth_and_data_api(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_CHANNEL_LIVE_URL": "https://www.youtube.com/channel/UC123/live",
            },
            clear=False,
        ):
            importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(True, "runtime tcp disconnected")):
                    with mock.patch.object(mod, "load_video_resolver_state", return_value={}):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "quota_guard_status", return_value=(True, "quota guard active", {})):
                                with mock.patch.object(mod, "probe_with_oauth") as oauth_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("VID_LIVEPAGE", "resolved from channel live redirect"),
                                    ):
                                        with mock.patch.object(mod, "resolve_live_video_id") as search_mock:
                                            with mock.patch.object(mod, "check_data_api") as check_mock:
                                                with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                    self.assertEqual(mod.main(), 0)

            oauth_mock.assert_not_called()
            search_mock.assert_not_called()
            check_mock.assert_not_called()
            self.assertEqual(captured.get("source"), "channel_live_page")
            self.assertEqual(captured.get("api_live_state"), "quota_exhausted")
            self.assertTrue(bool(captured.get("quota_guard_active")))

    def test_data_api_search_is_gated_in_normal_mode_with_known_video(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "VID_CONFIG",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC": "0",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(
                mod,
                "load_watchdog_stats",
                return_value={
                    "ts_utc": "2026-05-04T23:45:00Z",
                    "ingest_connected": True,
                    "api_live_state": "live",
                    "oauth_life_cycle_status": "live",
                },
            ):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(mod, "load_video_resolver_state", return_value={}):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id") as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_not_called()
            self.assertEqual(captured.get("source"), "configured")
            self.assertIn("data api search gated", str(captured.get("data_api_search_reason", "")))

    def test_data_api_search_is_suppressed_in_fast_mode_when_runtime_cache_is_resolved(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC": "0",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_260,
                            "video_id": "VID_CACHE",
                            "source": "resolver_cache",
                            "resolved_ts": 1_777_938_295,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(
                                    mod,
                                    "resolve_live_video_id",
                                    return_value=("VID_API", "live search resolved video id"),
                                ) as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_not_called()
            self.assertEqual(captured.get("source"), "resolver_cache")
            self.assertIn("video id already resolved", str(captured.get("data_api_search_reason", "")))

    def test_data_api_search_is_gated_by_api_cost_burn_guard(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC": "0",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="oauth unavailable",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            guard_state = mock.Mock(
                active=True,
                reason="projected units/day 10000>=9000",
                projected_units_per_day=10000,
                threshold_units_per_day=9000,
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(mod, "load_video_resolver_state", return_value={}):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "quota_guard_status", return_value=(False, "inactive", {})):
                                with mock.patch.object(mod, "load_api_cost_burn_rate_status", return_value=guard_state):
                                    with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                        with mock.patch.object(
                                            mod,
                                            "resolve_video_id_from_live_page",
                                            return_value=("", "skipped"),
                                        ):
                                            with mock.patch.object(mod, "resolve_live_video_id") as search_mock:
                                                with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                                    with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                        self.assertEqual(mod.main(), 0)

            search_mock.assert_not_called()
            self.assertIn("api cost burn guard active", str(captured.get("data_api_search_reason", "")))

    def test_configured_video_id_does_not_suppress_fast_search(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "VID_CONFIG",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC": "0",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_260,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(
                                    mod,
                                    "resolve_live_video_id",
                                    return_value=("VID_API", "live search resolved video id"),
                                ) as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_called_once()
            self.assertEqual(captured.get("source"), "data_api_search")

    def test_data_api_search_runs_in_fast_mode_after_ingest_reconnect_when_unresolved(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC": "0",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_260,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(
                                    mod,
                                    "resolve_live_video_id",
                                    return_value=("VID_API", "live search resolved video id"),
                                ) as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_called_once()
            self.assertEqual(captured.get("source"), "data_api_search")
            self.assertEqual(captured.get("video_id"), "VID_API")

    def test_data_api_search_is_blocked_while_ingest_disconnected(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(True, "runtime tcp disconnected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_260,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id") as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_not_called()
            self.assertIn("ingest not ready for search.list", str(captured.get("data_api_search_reason", "")))

    def test_ingest_ready_memory_allows_search_after_transient_ss_miss(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_INGEST_READY_MEMORY_SEC": "30",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            now_ts = 1_777_938_300
            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(True, "runtime tcp disconnected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_260,
                            "ingest_ready_last_true_ts": now_ts - 10,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=now_ts):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(
                                    mod,
                                    "resolve_live_video_id",
                                    return_value=("VID_API", "live search resolved video id"),
                                ) as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_called_once()
            self.assertTrue(bool(captured.get("ingest_ready_for_search")))
            self.assertIn("using ingest ready memory", str(captured.get("ingest_ready_reason", "")))

    def test_data_api_search_runs_when_remote_ended_while_ingest_connected(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "VID_CONFIG",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC": "0",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(
                mod,
                "load_watchdog_stats",
                return_value={
                    "ts_utc": "2026-05-04T23:45:00Z",
                    "ingest_connected": True,
                    "video_id": "VID_CONFIG",
                    "api_live_state": "ended",
                    "oauth_life_cycle_status": "complete",
                    "oauth_video_id": "VID_CONFIG",
                    "watch_reason": "watch page live marker inconclusive (treated as unknown)",
                },
            ):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(mod, "load_video_resolver_state", return_value={}):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(
                                    mod,
                                    "resolve_live_video_id",
                                    return_value=("VID_API", "live search resolved video id"),
                                ) as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_called_once()
            self.assertEqual(captured.get("source"), "data_api_search")
            self.assertIn("live search resolved", str(captured.get("data_api_search_reason", "")))

    def test_data_api_search_is_time_throttled_and_falls_back(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC": "5",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_260,
                            "last_data_api_search_ts": 1_777_938_298,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id") as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_not_called()
            self.assertEqual(captured.get("source"), "none")
            self.assertIn("throttled by time", str(captured.get("data_api_search_reason", "")))
            self.assertEqual(captured.get("last_data_api_search_ts"), 1_777_938_298)

    def test_data_api_search_stops_after_fast_window_elapsed(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_FAST_SEARCH_WINDOW_SEC": "180",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_000,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_181):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id") as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_not_called()
            self.assertEqual(captured.get("source"), "none")
            self.assertIn("fast window elapsed", str(captured.get("data_api_search_reason", "")))
            self.assertEqual(captured.get("fast_search_window_start_ts"), 1_777_938_000)

    def test_data_api_search_stops_after_fast_window_max_calls(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_FAST_SEARCH_WINDOW_SEC": "180",
                "YTW_VIDEO_RESOLVER_FAST_SEARCH_MAX_CALLS": "10",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_250,
                            "fast_search_episode_calls": 10,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id") as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_not_called()
            self.assertIn("max calls reached", str(captured.get("data_api_search_reason", "")))

    def test_fast_search_counter_increments_only_on_actual_search_call(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC": "5",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_260,
                            "fast_search_episode_calls": 5,
                            "last_data_api_search_ts": 1_777_938_298,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id") as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_not_called()
            self.assertEqual(int(captured.get("fast_search_episode_calls", 0)), 5)

    def test_fast_search_window_starts_after_ingest_reconnect(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC": "5",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 0,
                            "fast_search_episode_calls": 0,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id", return_value=("", "live search failed")) as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_called_once()
            self.assertEqual(int(captured.get("fast_search_window_start_ts", 0)), 1_777_938_300)
            self.assertEqual(int(captured.get("fast_search_episode_calls", 0)), 1)

    def test_search_counter_increments_on_attempt_even_when_search_fails(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC": "5",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_260,
                            "fast_search_episode_calls": 5,
                            "last_data_api_search_ts": 1_777_938_200,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(
                                    mod,
                                    "resolve_live_video_id",
                                    return_value=("", "live search http 500"),
                                ) as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_called_once()
            self.assertEqual(int(captured.get("fast_search_episode_calls", 0)), 6)
            self.assertEqual(int(captured.get("last_data_api_search_ts", 0)), 1_777_938_300)
            self.assertEqual(str(captured.get("data_api_search_reason", "")), "live search http 500")

    def test_fast_search_window_not_reset_by_ingest_flapping_in_same_episode(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_LIVE_URL": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC": "5",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_260,
                            "fast_search_episode_calls": 5,
                            "last_data_api_search_ts": 1_777_938_200,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(
                                    mod,
                                    "resolve_live_video_id",
                                    return_value=("", "live search failed"),
                                ) as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api") as api_check_mock:
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_called_once()
            api_check_mock.assert_not_called()
            self.assertEqual(int(captured.get("fast_search_window_start_ts", 0)), 1_777_938_260)
            self.assertEqual(int(captured.get("fast_search_episode_calls", 0)), 6)
            self.assertTrue(bool(captured.get("fast_mode_active")))

    def test_fast_search_episode_calls_not_reset_by_ingest_flapping_memory(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_LIVE_URL": "",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_INGEST_READY_MEMORY_SEC": "30",
                "YTW_VIDEO_RESOLVER_SEARCH_MIN_INTERVAL_SEC": "5",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(True, "runtime tcp disconnected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": True,
                            "fast_mode_bad_streak": 3,
                            "fast_mode_good_streak": 0,
                            "ingest_ready_last_true_ts": 1_777_938_290,
                            "fast_search_window_start_ts": 1_777_938_260,
                            "fast_search_episode_calls": 5,
                            "last_data_api_search_ts": 1_777_938_200,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(
                                    mod,
                                    "resolve_live_video_id",
                                    return_value=("", "live search failed"),
                                ) as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api") as api_check_mock:
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_called_once()
            api_check_mock.assert_not_called()
            self.assertEqual(int(captured.get("fast_search_window_start_ts", 0)), 1_777_938_260)
            self.assertEqual(int(captured.get("fast_search_episode_calls", 0)), 6)
            self.assertTrue(bool(captured.get("ingest_ready_for_search")))
            self.assertIn("using ingest ready memory", str(captured.get("ingest_ready_reason", "")))

    def test_fast_search_counter_resets_on_new_episode(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "",
                "YTW_CHANNEL_LIVE_URL": "",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(True, "runtime tcp disconnected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": False,
                            "fast_mode_bad_streak": 1,
                            "fast_mode_good_streak": 0,
                            "fast_search_window_start_ts": 1_777_938_000,
                            "fast_search_episode_calls": 10,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id") as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_not_called()
            self.assertEqual(int(captured.get("fast_search_episode_calls", -1)), 0)

    def test_fast_mode_hysteresis_enters_on_2_and_exits_on_3(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            mod = importlib.reload(youtube_video_id_resolver)

            state: dict = {}
            mode1, reason1, bad1, good1 = mod.apply_fast_mode_hysteresis(state, True, "raw_bad_1")
            self.assertFalse(mode1)
            self.assertEqual(bad1, 1)
            self.assertEqual(good1, 0)
            self.assertIn("hysteresis=hold", reason1)

            state.update(
                {
                    "fast_mode_active": mode1,
                    "fast_mode_bad_streak": bad1,
                    "fast_mode_good_streak": good1,
                }
            )
            mode2, reason2, bad2, good2 = mod.apply_fast_mode_hysteresis(state, True, "raw_bad_2")
            self.assertTrue(mode2)
            self.assertEqual(bad2, 2)
            self.assertEqual(good2, 0)
            self.assertIn("hysteresis=enter_fast", reason2)

            state.update(
                {
                    "fast_mode_active": mode2,
                    "fast_mode_bad_streak": bad2,
                    "fast_mode_good_streak": good2,
                }
            )
            mode3, reason3, bad3, good3 = mod.apply_fast_mode_hysteresis(state, False, "raw_good_1")
            self.assertTrue(mode3)
            self.assertEqual(bad3, 0)
            self.assertEqual(good3, 1)
            self.assertIn("hysteresis=hold", reason3)

            state.update(
                {
                    "fast_mode_active": mode3,
                    "fast_mode_bad_streak": bad3,
                    "fast_mode_good_streak": good3,
                }
            )
            mode4, _reason4, bad4, good4 = mod.apply_fast_mode_hysteresis(state, False, "raw_good_2")
            self.assertTrue(mode4)
            self.assertEqual(good4, 2)
            self.assertEqual(bad4, 0)

            state.update(
                {
                    "fast_mode_active": mode4,
                    "fast_mode_bad_streak": bad4,
                    "fast_mode_good_streak": good4,
                }
            )
            mode5, reason5, bad5, good5 = mod.apply_fast_mode_hysteresis(state, False, "raw_good_3")
            self.assertFalse(mode5)
            self.assertEqual(good5, 3)
            self.assertEqual(bad5, 0)
            self.assertIn("hysteresis=exit_normal", reason5)

    def test_target_interval_uses_fast_interval_in_fast_mode(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_VIDEO_ID": "",
                "YTW_LIVE_URL": "",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="oauth unavailable",
                mode="shadow",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(True, "runtime tcp disconnected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "fast_mode_active": False,
                            "fast_mode_bad_streak": 1,
                            "fast_mode_good_streak": 0,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_500):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id", return_value=("", "n/a")):
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api") as api_mock:
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            api_mock.assert_not_called()
            self.assertEqual(captured.get("target_interval_sec"), 5)
            self.assertTrue(bool(captured.get("search_cadence_active")))

    def test_remote_ended_needs_confirm_window_before_search(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "VID_CONFIG",
                "YTW_CHANNEL_LIVE_URL": "",
                "YTW_VIDEO_RESOLVER_REMOTE_ENDED_CONFIRM_SEC": "15",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(
                mod,
                "load_watchdog_stats",
                return_value={
                    "ts_utc": "2026-05-04T23:45:00Z",
                    "ingest_connected": True,
                    "video_id": "VID_CONFIG",
                    "api_live_state": "ended",
                    "oauth_life_cycle_status": "complete",
                    "oauth_video_id": "VID_CONFIG",
                    "watch_reason": "watch page live marker inconclusive (treated as unknown)",
                },
            ):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(mod, "load_video_resolver_state", return_value={}):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_300):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id") as search_mock:
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            search_mock.assert_not_called()
            self.assertFalse(bool(captured.get("remote_ended_confirmed")))
            self.assertIn("waiting confirm", str(captured.get("remote_ended_reason", "")))

    def test_remote_ended_requires_matching_api_and_oauth_video_id(self) -> None:
        mod = importlib.reload(youtube_video_id_resolver)
        now_ts = 1_777_938_300
        stats = {
            "ts_utc": "2026-05-04T23:45:00Z",
            "ingest_connected": True,
            "video_id": "VID_FROM_LIVE_PAGE",
            "api_live_state": "ended",
            "oauth_life_cycle_status": "complete",
            "oauth_video_id": "VID_FROM_OAUTH",
            "watch_reason": "watch page live marker inconclusive (treated as unknown)",
        }
        raw, reason = mod.has_recent_remote_ended(stats, now_ts)
        self.assertFalse(raw)
        self.assertIn("id mismatch", reason)

        stats["oauth_video_id"] = "VID_FROM_LIVE_PAGE"
        raw, reason = mod.has_recent_remote_ended(stats, now_ts)
        self.assertTrue(raw)
        self.assertIn("remote ended while ingest connected", reason)

    def test_configured_fallback_is_single_use_boot_seed(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "YTW_CHANNEL_ID": "UC123",
                "YTW_API_KEY": "APIKEY",
                "YTW_VIDEO_ID": "VID_CONFIG",
                "YTW_CHANNEL_LIVE_URL": "",
            },
            clear=False,
        ):
            mod_cfg = importlib.reload(youtube_watchdog_config)
            mod = importlib.reload(youtube_video_id_resolver)
            oauth = mod_cfg.OAuthProbeResult(
                enabled=True,
                configured=True,
                probe_ok=False,
                healthy=False,
                reason="quota",
                mode="shadow",
            )
            api_check = mod_cfg.DataApiCheckResult(
                checked=True,
                api_ok=True,
                live_state="live",
                reason="data api says live",
            )
            captured: dict = {}

            def save_hook(payload: dict) -> None:
                captured.clear()
                captured.update(payload)

            with mock.patch.object(mod, "load_watchdog_stats", return_value={}):
                with mock.patch.object(mod, "resolve_fast_mode", return_value=(False, "runtime tcp connected")):
                    with mock.patch.object(
                        mod,
                        "load_video_resolver_state",
                        return_value={
                            "startup_anchor_ts": 1_777_938_200,
                            "configured_fallback_uses": 1,
                            "resolved_ts": 0,
                        },
                    ):
                        with mock.patch.object(mod.time, "time", return_value=1_777_938_220):
                            with mock.patch.object(mod, "probe_with_oauth", return_value=oauth):
                                with mock.patch.object(mod, "resolve_live_video_id", return_value=("", "n/a")):
                                    with mock.patch.object(
                                        mod,
                                        "resolve_video_id_from_live_page",
                                        return_value=("", "skipped"),
                                    ):
                                        with mock.patch.object(mod, "check_data_api", return_value=api_check):
                                            with mock.patch.object(mod, "save_video_resolver_state", side_effect=save_hook):
                                                self.assertEqual(mod.main(), 0)

            self.assertNotEqual(captured.get("source"), "configured")
            self.assertIn("already used", str(captured.get("configured_fallback_reason", "")))

if __name__ == "__main__":
    unittest.main()
