from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_watchdog  # type: ignore
import youtube_watchdog_config  # type: ignore


class YouTubeWatchdogCheckedTimestampTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._env = mock.patch.dict(
            os.environ,
            {
                "STREAM_RUNTIME_STATE_DIR": self._tmpdir.name,
                "YTW_VIDEO_ID": "",
                "YTW_API_KEY": "",
                "YTW_CHANNEL_ID": "",
                "YTW_ENFORCE_RESTART": "0",
                "YTW_STARTUP_GRACE_SEC": "0",
            },
            clear=False,
        )
        self._env.start()
        importlib.reload(youtube_watchdog_config)
        importlib.reload(youtube_watchdog)

    def tearDown(self) -> None:
        self._env.stop()
        self._tmpdir.cleanup()

    def _common_runtime_mocks(self, mod):
        return [
            mock.patch.object(mod, "is_service_active", return_value=True),
            mock.patch.object(mod, "get_main_pid", return_value=100),
            mock.patch.object(mod, "get_child_ffmpeg_pid", return_value=200),
            mock.patch.object(mod, "get_process_elapsed_sec", return_value=1200),
            mock.patch.object(
                mod,
                "ffmpeg_has_ingest_connection_any",
                return_value=(True, "ESTAB ..."),
            ),
            mock.patch.object(mod, "parse_ingest_ports", return_value=[1935]),
            mock.patch.object(
                mod,
                "load_state",
                return_value={"fail_count": 0, "degraded_public_count": 0},
            ),
            mock.patch.object(mod, "save_state"),
            mock.patch.object(mod, "load_video_resolver_state", return_value={}),
            mock.patch.object(
                mod,
                "check_public_watch_page_verdict",
                return_value=mock.Mock(
                    ok_for_availability=True, reason="watch ok", verdict="live"
                ),
            ),
            mock.patch.object(
                mod, "quota_guard_status", return_value=(False, "inactive", {})
            ),
            mock.patch.object(
                mod,
                "load_api_cost_burn_rate_status",
                return_value=mock.Mock(
                    active=False,
                    reason="inactive",
                    projected_units_per_day=0,
                    threshold_units_per_day=9000,
                ),
            ),
            mock.patch.object(mod, "log"),
        ]

    def test_does_not_promote_legacy_wrapper_time_when_remote_check_is_unavailable(
        self,
    ) -> None:
        mod = importlib.reload(youtube_watchdog)
        legacy_ts = "2026-05-05T09:30:00Z"
        last_stats = {
            "ts_utc": legacy_ts,
            "oauth_probe_ok": True,
            "oauth_healthy": True,
            "oauth_reason": "legacy oauth",
            "oauth_video_id": "VID123",
            "video_id": "VID123",
            "api_ok": True,
            "api_reason": "legacy api",
            "api_live_state": "live",
        }

        payloads: list[dict] = []

        def capture(payload: dict) -> None:
            payloads.append(dict(payload))

        patchers = self._common_runtime_mocks(mod)
        patchers.extend(
            [
                mock.patch.object(
                    mod, "load_last_watchdog_stats", return_value=last_stats
                ),
                mock.patch.object(
                    mod,
                    "probe_with_oauth",
                    return_value=youtube_watchdog_config.OAuthProbeResult(
                        enabled=True,
                        configured=False,
                        probe_ok=False,
                        healthy=False,
                        reason="not configured",
                        mode="shadow",
                        remote_checked=False,
                    ),
                ),
                mock.patch.object(
                    mod,
                    "check_data_api",
                    return_value=youtube_watchdog_config.DataApiCheckResult(
                        checked=False,
                        api_ok=False,
                        live_state="skipped",
                        reason="no probe",
                    ),
                ),
                mock.patch.object(mod, "write_stats", side_effect=capture),
                mock.patch.object(mod.time, "time", return_value=1_777_938_300),
            ]
        )
        with ExitStack() as stack:
            for p in patchers:
                stack.enter_context(p)
            self.assertEqual(mod.main(), 0)

        self.assertTrue(payloads)
        self.assertEqual(payloads[-1].get("oauth_checked_ts_utc"), "")
        self.assertEqual(payloads[-1].get("data_api_checked_ts_utc"), "")
        self.assertIs(payloads[-1].get("oauth_probe_performed"), False)

    def test_checked_timestamp_not_updated_without_remote_calls(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        oauth = youtube_watchdog_config.OAuthProbeResult(
            enabled=True,
            configured=False,
            probe_ok=True,
            healthy=True,
            reason="oauth not configured",
            mode="shadow",
            video_id="VID123",
            remote_checked=False,
        )
        api_result = youtube_watchdog_config.DataApiCheckResult(
            checked=False,
            api_ok=True,
            live_state="skipped",
            reason="data api check skipped",
        )

        payloads: list[dict] = []

        def capture(payload: dict) -> None:
            payloads.append(dict(payload))

        patchers = self._common_runtime_mocks(mod)
        patchers.extend(
            [
                mock.patch.object(mod, "load_last_watchdog_stats", return_value={}),
                mock.patch.object(mod, "probe_with_oauth", return_value=oauth),
                mock.patch.object(mod, "check_data_api", return_value=api_result),
                mock.patch.object(mod, "write_stats", side_effect=capture),
                mock.patch.object(mod.time, "time", return_value=1_777_938_300),
            ]
        )
        with ExitStack() as stack:
            for p in patchers:
                stack.enter_context(p)
            self.assertEqual(mod.main(), 0)

        self.assertTrue(payloads)
        self.assertEqual(payloads[-1].get("oauth_checked_ts_utc"), "")
        self.assertEqual(payloads[-1].get("data_api_checked_ts_utc"), "")

    def test_remote_attempt_is_distinct_from_success_and_updates_checked_time(
        self,
    ) -> None:
        mod = importlib.reload(youtube_watchdog)
        checked = "2026-05-05T10:00:00Z"
        for succeeded in (True, False):
            with self.subTest(succeeded=succeeded), ExitStack() as stack:
                for patcher in self._common_runtime_mocks(mod):
                    stack.enter_context(patcher)
                payloads = []
                stack.enter_context(
                    mock.patch.object(mod, "OAUTH_PROBE_MIN_INTERVAL_SEC", 0)
                )
                stack.enter_context(
                    mock.patch.object(mod, "load_last_watchdog_stats", return_value={})
                )
                stack.enter_context(
                    mock.patch.object(mod, "utc_now", return_value=checked)
                )
                stack.enter_context(
                    mock.patch.object(
                        mod.time, "time", return_value=mod.parse_iso_ts(checked)
                    )
                )
                stack.enter_context(
                    mock.patch.object(
                        mod,
                        "probe_with_oauth",
                        return_value=youtube_watchdog_config.OAuthProbeResult(
                            enabled=True,
                            configured=True,
                            probe_ok=succeeded,
                            healthy=succeeded,
                            reason="probe result",
                            mode="shadow",
                            remote_checked=True,
                        ),
                    )
                )
                stack.enter_context(
                    mock.patch.object(
                        mod,
                        "check_data_api",
                        return_value=youtube_watchdog_config.DataApiCheckResult(
                            checked=False,
                            api_ok=False,
                            live_state="skipped",
                            reason="not checked",
                        ),
                    )
                )
                stack.enter_context(
                    mock.patch.object(
                        mod,
                        "write_stats",
                        side_effect=lambda p: payloads.append(dict(p)),
                    )
                )
                self.assertEqual(mod.main(), 0)
                self.assertTrue(payloads[-1]["oauth_probe_performed"])
                self.assertEqual(payloads[-1]["oauth_probe_ok"], succeeded)
                self.assertEqual(payloads[-1]["oauth_cache_min_interval_sec"], 0)
                self.assertEqual(payloads[-1]["oauth_checked_ts_utc"], checked)

    def test_quota_guard_keeps_response_cache_even_with_every_cycle_profile(
        self,
    ) -> None:
        mod = importlib.reload(youtube_watchdog)
        checked = "2026-05-05T10:00:00Z"
        now = mod.parse_iso_ts(checked) + 60
        last_stats = {
            "oauth_checked_ts_utc": checked,
            "oauth_probe_ok": True,
            "oauth_enabled": True,
            "oauth_configured": True,
            "oauth_healthy": True,
            "oauth_video_id": "VID123",
            "oauth_reason": "cached",
            "video_id": "VID123",
            "data_api_checked_ts_utc": checked,
            "api_ok": True,
            "api_live_state": "live",
        }
        payloads = []
        with ExitStack() as stack:
            for p in self._common_runtime_mocks(mod):
                stack.enter_context(p)
            stack.enter_context(
                mock.patch.object(mod, "OAUTH_PROBE_MIN_INTERVAL_SEC", 0)
            )
            stack.enter_context(
                mock.patch.object(
                    mod, "load_last_watchdog_stats", return_value=last_stats
                )
            )
            stack.enter_context(
                mock.patch.object(
                    mod,
                    "load_api_cost_burn_rate_status",
                    return_value=mock.Mock(
                        active=True,
                        reason="budget guard",
                        projected_units_per_day=9100,
                        threshold_units_per_day=9000,
                    ),
                )
            )
            stack.enter_context(mock.patch.object(mod.time, "time", return_value=now))
            probe = stack.enter_context(mock.patch.object(mod, "probe_with_oauth"))
            stack.enter_context(
                mock.patch.object(
                    mod, "write_stats", side_effect=lambda p: payloads.append(dict(p))
                )
            )
            self.assertEqual(mod.main(), 0)
            probe.assert_not_called()
        self.assertFalse(payloads[-1]["oauth_probe_performed"])
        self.assertEqual(payloads[-1]["oauth_cache_min_interval_sec"], 600)
        self.assertEqual(payloads[-1]["oauth_checked_ts_utc"], checked)


if __name__ == "__main__":
    unittest.main()
