from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_api  # type: ignore
import youtube_watchdog_config  # type: ignore


class YouTubeApiConfigTests(unittest.TestCase):
    def test_oauth_probe_persists_issue_type_and_severity_only(self) -> None:
        broadcast = {
            "id": "BID",
            "snippet": {"resourceId": {"videoId": "VID"}, "channelId": "UC"},
            "status": {"lifeCycleStatus": "live"},
            "contentDetails": {"boundStreamId": "STREAM"},
        }
        stream_response = {
            "items": [
                {
                    "status": {
                        "streamStatus": "active",
                        "healthStatus": {
                            "status": "ok",
                            "configurationIssues": [
                                {
                                    "type": "bitrateLow",
                                    "severity": "warning",
                                    "reason": "localized reason",
                                    "description": "detailed description",
                                }
                            ],
                        },
                    }
                }
            ]
        }
        with (
            mock.patch.object(youtube_api, "OAUTH_ENABLE", True),
            mock.patch.object(youtube_api, "OAUTH_SHADOW_MODE", True),
            mock.patch.object(youtube_api, "oauth_is_configured", return_value=True),
            mock.patch.object(
                youtube_api,
                "get_oauth_access_token",
                return_value=("token", 1_900_000_000, "cached token"),
            ),
            mock.patch.object(youtube_api, "list_owned_broadcasts", return_value=[broadcast]),
            mock.patch.object(youtube_api, "youtube_live_api_get", return_value=stream_response),
        ):
            result = youtube_api.probe_with_oauth()

        self.assertEqual(result.stream_health_status, "ok")
        self.assertEqual(result.stream_health_issues, 1)
        self.assertEqual(
            result.stream_health_issue_details,
            ({"type": "bitrateLow", "severity": "warning"},),
        )

    def test_parse_ingest_ports_prefers_explicit_multi_port_contract(self) -> None:
        with (
            mock.patch.object(youtube_api, "INGEST_TCP_PORT", 1935),
            mock.patch.object(youtube_api, "INGEST_TCP_PORTS_RAW", "1935,443,1935,bad"),
        ):
            self.assertEqual(youtube_api.parse_ingest_ports(), [1935, 443])

    def test_parse_ingest_ports_defaults_to_legacy_and_rtmps_443(self) -> None:
        with (
            mock.patch.object(youtube_api, "INGEST_TCP_PORT", 1935),
            mock.patch.object(youtube_api, "INGEST_TCP_PORTS_RAW", ""),
        ):
            self.assertEqual(youtube_api.parse_ingest_ports(), [1935, 443])

    def test_watchdog_defaults_match_current_youtube_contract(self) -> None:
        try:
            with mock.patch.dict(os.environ, {}, clear=True):
                cfg = importlib.reload(youtube_watchdog_config)

            self.assertEqual(cfg.RESTART_COOLDOWN_SEC, 180)
            self.assertEqual(cfg.API_COST_BURN_RATE_THRESHOLD_UNITS_PER_DAY, 9000)
            self.assertTrue(cfg.API_COST_BURN_RATE_FAIL_CLOSED)
        finally:
            importlib.reload(youtube_watchdog_config)


if __name__ == "__main__":
    unittest.main()
