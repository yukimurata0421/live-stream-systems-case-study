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
            mock.patch.object(mod, "ffmpeg_has_ingest_connection_any", return_value=(True, "ESTAB ...")),
            mock.patch.object(mod, "parse_ingest_ports", return_value=[1935]),
            mock.patch.object(mod, "load_state", return_value={"fail_count": 0, "degraded_public_count": 0}),
            mock.patch.object(mod, "save_state"),
            mock.patch.object(mod, "load_video_resolver_state", return_value={}),
            mock.patch.object(
                mod,
                "check_public_watch_page_verdict",
                return_value=mock.Mock(ok_for_availability=True, reason="watch ok", verdict="live"),
            ),
            mock.patch.object(mod, "quota_guard_status", return_value=(False, "inactive", {})),
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

    def test_promotes_legacy_ts_to_checked_timestamps(self) -> None:
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
                mock.patch.object(mod, "load_last_watchdog_stats", return_value=last_stats),
                mock.patch.object(mod, "probe_with_oauth"),
                mock.patch.object(mod, "check_data_api"),
                mock.patch.object(mod, "write_stats", side_effect=capture),
                mock.patch.object(mod.time, "time", return_value=1_777_938_300),
            ]
        )
        with ExitStack() as stack:
            for p in patchers:
                stack.enter_context(p)
            self.assertEqual(mod.main(), 0)

        self.assertTrue(payloads)
        self.assertEqual(payloads[-1].get("oauth_checked_ts_utc"), legacy_ts)
        self.assertEqual(payloads[-1].get("data_api_checked_ts_utc"), legacy_ts)

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


if __name__ == "__main__":
    unittest.main()
