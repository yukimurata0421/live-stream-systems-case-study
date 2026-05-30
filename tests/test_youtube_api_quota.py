from __future__ import annotations

import importlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_api  # type: ignore
import youtube_watchdog_config  # type: ignore
import youtube_watchdog_state  # type: ignore


class YouTubeApiQuotaResetTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._env_patcher = mock.patch.dict(
            os.environ,
            {
                "STREAM_RUNTIME_STATE_DIR": self._tmpdir.name,
                "YTW_QUOTA_STATE_FILE": str(Path(self._tmpdir.name) / "youtube_quota_state.json"),
                "YTW_API_CALL_LOG_FILE": str(Path(self._tmpdir.name) / "youtube_api_calls.jsonl"),
            },
            clear=False,
        )
        self._env_patcher.start()
        importlib.reload(youtube_watchdog_config)
        importlib.reload(youtube_watchdog_state)
        importlib.reload(youtube_api)

    def tearDown(self) -> None:
        self._env_patcher.stop()
        self._tmpdir.cleanup()

    def _expected_next_reset(self, now_ts: int) -> int:
        pt = ZoneInfo("America/Los_Angeles")
        now_pt = datetime.fromtimestamp(now_ts, tz=pt)
        next_midnight = now_pt.replace(hour=0, minute=0, second=0, microsecond=0)
        if next_midnight <= now_pt:
            from datetime import timedelta

            next_midnight = next_midnight + timedelta(days=1)
        return int(next_midnight.timestamp())

    def _expected_guard_until(self, now_ts: int, margin_sec: int) -> int:
        return self._expected_next_reset(now_ts) + max(0, int(margin_sec))

    def test_next_quota_reset_ts_pacific(self) -> None:
        now_ts = int(datetime(2026, 5, 5, 15, 30, tzinfo=timezone.utc).timestamp())
        expected = self._expected_next_reset(now_ts)
        actual = youtube_api.next_quota_reset_ts_pacific(now_ts)
        self.assertEqual(actual, expected)

    def test_mark_quota_exhausted_uses_next_pacific_midnight(self) -> None:
        now_ts = int(datetime(2026, 5, 5, 15, 30, tzinfo=timezone.utc).timestamp())
        expected = self._expected_guard_until(now_ts, youtube_watchdog_config.QUOTA_RESET_MARGIN_SEC)
        ok, _msg = youtube_api.mark_quota_exhausted("data_api_search", "quota exceeded", now_ts=now_ts)
        self.assertTrue(ok)

        state = youtube_watchdog_state.load_quota_state()
        self.assertTrue(bool(state.get("quota_exhausted")))
        self.assertEqual(int(state.get("quota_exhausted_until_ts", 0)), expected)

    def test_quota_guard_until_ts_pacific_applies_margin(self) -> None:
        now_ts = int(datetime(2026, 5, 5, 15, 30, tzinfo=timezone.utc).timestamp())
        expected = self._expected_guard_until(now_ts, youtube_watchdog_config.QUOTA_RESET_MARGIN_SEC)
        actual = youtube_api.quota_guard_until_ts_pacific(now_ts)
        self.assertEqual(actual, expected)

    def test_quota_guard_until_uses_dst_aware_pacific_timezone(self) -> None:
        # January (PST, UTC-8)
        jan_ts = int(datetime(2026, 1, 15, 20, 30, tzinfo=timezone.utc).timestamp())
        jan_expected = self._expected_guard_until(jan_ts, youtube_watchdog_config.QUOTA_RESET_MARGIN_SEC)
        self.assertEqual(youtube_api.quota_guard_until_ts_pacific(jan_ts), jan_expected)

        # July (PDT, UTC-7)
        jul_ts = int(datetime(2026, 7, 15, 20, 30, tzinfo=timezone.utc).timestamp())
        jul_expected = self._expected_guard_until(jul_ts, youtube_watchdog_config.QUOTA_RESET_MARGIN_SEC)
        self.assertEqual(youtube_api.quota_guard_until_ts_pacific(jul_ts), jul_expected)

    def test_api_call_log_records_only_actual_search_call(self) -> None:
        payload = {
            "items": [
                {
                    "id": {
                        "videoId": "VID12345678",
                    }
                }
            ]
        }
        with mock.patch.object(youtube_api, "fetch", return_value=json.dumps(payload)):
            vid, reason = youtube_api.resolve_live_video_id("UC123", "APIKEY")
        self.assertEqual(vid, "VID12345678")
        self.assertIn("resolved", reason)

        # skip path must not append a new API call event
        skip_vid, skip_reason = youtube_api.resolve_live_video_id("", "")
        self.assertEqual(skip_vid, "")
        self.assertEqual(skip_reason, "live search skipped")

        log_path = Path(os.environ["YTW_API_CALL_LOG_FILE"])
        lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(len(lines), 1)
        event = json.loads(lines[0])
        self.assertEqual(event.get("method"), "search.list")
        self.assertEqual(event.get("status"), "ok")
        self.assertEqual(int(event.get("cost_units", 0) or 0), 100)

    def test_check_data_api_forbidden_auth_error_does_not_become_quota_guard(self) -> None:
        body = json.dumps(
            {
                "error": {
                    "code": 403,
                    "message": "Request had insufficient authentication scopes.",
                    "errors": [
                        {
                            "message": "Insufficient Permission",
                            "domain": "youtube.parameter",
                            "reason": "insufficientPermissions",
                        }
                    ],
                }
            }
        )
        err = HTTPError(
            url="https://www.googleapis.com/youtube/v3/videos",
            code=403,
            msg="Forbidden",
            hdrs=None,
            fp=io.BytesIO(body.encode("utf-8")),
        )
        self.addCleanup(err.close)
        with mock.patch.object(youtube_api, "fetch", side_effect=err):
            result = youtube_api.check_data_api("VID12345678", "APIKEY")
        self.assertEqual(result.live_state, "error")
        state = youtube_watchdog_state.load_quota_state()
        self.assertFalse(bool(state.get("quota_exhausted", False)))

    def test_check_data_api_quota_exceeded_latches_guard(self) -> None:
        body = json.dumps(
            {
                "error": {
                    "code": 403,
                    "message": "The request cannot be completed because you have exceeded your quota.",
                    "errors": [
                        {
                            "message": "The request cannot be completed because you have exceeded your quota.",
                            "domain": "youtube.quota",
                            "reason": "quotaExceeded",
                        }
                    ],
                }
            }
        )
        err = HTTPError(
            url="https://www.googleapis.com/youtube/v3/videos",
            code=403,
            msg="Forbidden",
            hdrs=None,
            fp=io.BytesIO(body.encode("utf-8")),
        )
        self.addCleanup(err.close)
        with mock.patch.object(youtube_api, "fetch", side_effect=err):
            result = youtube_api.check_data_api("VID12345678", "APIKEY")
        self.assertEqual(result.live_state, "quota_exhausted")
        state = youtube_watchdog_state.load_quota_state()
        self.assertTrue(bool(state.get("quota_exhausted", False)))

    def test_check_data_api_daily_limit_exceeded_latches_guard(self) -> None:
        body = json.dumps(
            {
                "error": {
                    "code": 403,
                    "message": "The request cannot be completed because the daily limit has been exceeded.",
                    "errors": [
                        {
                            "message": "Daily Limit Exceeded",
                            "domain": "usageLimits",
                            "reason": "dailyLimitExceeded",
                        }
                    ],
                }
            }
        )
        err = HTTPError(
            url="https://www.googleapis.com/youtube/v3/videos",
            code=403,
            msg="Forbidden",
            hdrs=None,
            fp=io.BytesIO(body.encode("utf-8")),
        )
        self.addCleanup(err.close)
        with mock.patch.object(youtube_api, "fetch", side_effect=err):
            result = youtube_api.check_data_api("VID12345678", "APIKEY")
        self.assertEqual(result.live_state, "quota_exhausted")
        state = youtube_watchdog_state.load_quota_state()
        self.assertTrue(bool(state.get("quota_exhausted", False)))

    def test_check_data_api_rate_limit_exceeded_is_degraded_not_quota_guard(self) -> None:
        body = json.dumps(
            {
                "error": {
                    "code": 403,
                    "message": "The request cannot be completed because you have exceeded your rate limit.",
                    "errors": [
                        {
                            "message": "Rate Limit Exceeded",
                            "domain": "usageLimits",
                            "reason": "rateLimitExceeded",
                        }
                    ],
                }
            }
        )
        err = HTTPError(
            url="https://www.googleapis.com/youtube/v3/videos",
            code=403,
            msg="Forbidden",
            hdrs=None,
            fp=io.BytesIO(body.encode("utf-8")),
        )
        self.addCleanup(err.close)
        with mock.patch.object(youtube_api, "fetch", side_effect=err):
            result = youtube_api.check_data_api("VID12345678", "APIKEY")
        self.assertEqual(result.live_state, "rate_limited")
        state = youtube_watchdog_state.load_quota_state()
        self.assertFalse(bool(state.get("quota_exhausted", False)))

    def test_mark_quota_exhausted_emits_activation_event_with_reason(self) -> None:
        now_ts = int(datetime(2026, 5, 5, 15, 30, tzinfo=timezone.utc).timestamp())
        with mock.patch.object(youtube_api, "append_event") as append_event_mock:
            ok, _msg = youtube_api.mark_quota_exhausted(
                "data_api_search",
                "data api http 403: ...",
                now_ts=now_ts,
                reason_hint="dailyLimitExceeded",
            )
        self.assertTrue(ok)
        append_event_mock.assert_called_once()
        payload = append_event_mock.call_args.args[0]
        self.assertEqual(payload.get("event"), "youtube_quota_guard_activated")
        self.assertEqual(payload.get("reason"), "dailyLimitExceeded")
        self.assertEqual(payload.get("source"), "google_error_reason")

    def test_mark_quota_exhausted_emits_event_only_on_first_activation(self) -> None:
        now_ts = int(datetime(2026, 5, 5, 15, 30, tzinfo=timezone.utc).timestamp())
        with mock.patch.object(youtube_api, "append_event") as append_event_mock:
            youtube_api.mark_quota_exhausted("data_api_search", "first", now_ts=now_ts, reason_hint="quotaExceeded")
            youtube_api.mark_quota_exhausted("data_api_search", "second", now_ts=now_ts, reason_hint="quotaExceeded")
        self.assertEqual(append_event_mock.call_count, 1)

    def test_youtube_live_api_get_logs_quota_exceeded_on_http_403(self) -> None:
        body = json.dumps(
            {
                "error": {
                    "code": 403,
                    "message": "The request cannot be completed because you have exceeded your quota.",
                    "errors": [
                        {
                            "message": "The request cannot be completed because you have exceeded your quota.",
                            "domain": "youtube.quota",
                            "reason": "quotaExceeded",
                        }
                    ],
                }
            }
        )
        err = HTTPError(
            url="https://www.googleapis.com/youtube/v3/liveBroadcasts",
            code=403,
            msg="Forbidden",
            hdrs=None,
            fp=io.BytesIO(body.encode("utf-8")),
        )
        self.addCleanup(err.close)
        with mock.patch.object(youtube_api, "fetch_oauth_json", side_effect=err):
            with self.assertRaises(HTTPError):
                youtube_api.youtube_live_api_get(
                    "liveBroadcasts",
                    "token",
                    {"part": "id,status", "mine": "true"},
                )
        log_path = Path(os.environ["YTW_API_CALL_LOG_FILE"])
        lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload.get("method"), "liveBroadcasts.list")
        self.assertEqual(payload.get("status"), "http_error")
        self.assertTrue(bool(payload.get("quota_exceeded")))

    def test_http_error_body_is_reusable_after_first_read(self) -> None:
        body = '{"error":{"code":403,"errors":[{"reason":"quotaExceeded"}]}}'
        err = HTTPError(
            url="https://www.googleapis.com/youtube/v3/liveBroadcasts",
            code=403,
            msg="Forbidden",
            hdrs=None,
            fp=io.BytesIO(body.encode("utf-8")),
        )
        self.addCleanup(err.close)
        first = youtube_api._http_error_body(err)
        second = youtube_api._http_error_body(err)
        err.close()
        self.assertEqual(first, body)
        self.assertEqual(second, body)

    def test_probe_with_oauth_latches_quota_after_live_api_get_consumes_body(self) -> None:
        body = json.dumps(
            {
                "error": {
                    "code": 403,
                    "message": "The request cannot be completed because you have exceeded your quota.",
                    "errors": [
                        {
                            "message": "The request cannot be completed because you have exceeded your quota.",
                            "domain": "youtube.quota",
                            "reason": "quotaExceeded",
                        }
                    ],
                }
            }
        )
        err = HTTPError(
            url="https://www.googleapis.com/youtube/v3/liveBroadcasts",
            code=403,
            msg="Forbidden",
            hdrs=None,
            fp=io.BytesIO(body.encode("utf-8")),
        )
        self.addCleanup(err.close)
        with mock.patch.object(youtube_api, "OAUTH_ENABLE", True):
            with mock.patch.object(youtube_api, "oauth_is_configured", return_value=True):
                with mock.patch.object(youtube_api, "get_oauth_access_token", return_value=("token", 2_000_000_000, "ok")):
                    with mock.patch.object(youtube_api, "fetch_oauth_json", side_effect=err):
                        result = youtube_api.probe_with_oauth()
        self.assertFalse(result.probe_ok)
        self.assertIn("quota exhausted guard latched", result.reason)
        active, state = youtube_watchdog_state.quota_exhausted_active()
        self.assertTrue(active)
        self.assertTrue(bool(state.get("quota_exhausted", False)))

    def test_youtube_live_api_post_logs_and_latches_quota_exceeded_on_http_403(self) -> None:
        body = json.dumps(
            {
                "error": {
                    "code": 403,
                    "message": "The request cannot be completed because you have exceeded your quota.",
                    "errors": [
                        {
                            "message": "The request cannot be completed because you have exceeded your quota.",
                            "domain": "youtube.quota",
                            "reason": "quotaExceeded",
                        }
                    ],
                }
            }
        )
        err = HTTPError(
            url="https://www.googleapis.com/youtube/v3/liveBroadcasts/transition",
            code=403,
            msg="Forbidden",
            hdrs=None,
            fp=io.BytesIO(body.encode("utf-8")),
        )
        self.addCleanup(err.close)
        with mock.patch("urllib.request.urlopen", side_effect=err):
            with self.assertRaises(HTTPError):
                youtube_api.youtube_live_api_post(
                    "liveBroadcasts/transition",
                    "token",
                    {"broadcastStatus": "live", "id": "BID", "part": "id,status"},
                )

        log_path = Path(os.environ["YTW_API_CALL_LOG_FILE"])
        lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload.get("method"), "liveBroadcasts.transition")
        self.assertEqual(payload.get("status"), "http_error")
        self.assertTrue(bool(payload.get("quota_exceeded")))

        active, state = youtube_watchdog_state.quota_exhausted_active()
        self.assertTrue(active)
        self.assertTrue(bool(state.get("quota_exhausted", False)))


if __name__ == "__main__":
    unittest.main()
