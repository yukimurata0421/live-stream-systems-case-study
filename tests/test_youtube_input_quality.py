from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_core.common.youtube_input_quality import (  # type: ignore
    classify_input_quality_sample,
    normalize_health_issue_details,
)


class YouTubeInputQualityTests(unittest.TestCase):
    def test_normalization_keeps_only_type_and_severity(self) -> None:
        details = normalize_health_issue_details(
            [
                {
                    "type": "bitrateLow",
                    "severity": "WARNING",
                    "reason": "do not persist localized text",
                    "description": "do not persist detailed text",
                }
            ]
        )

        self.assertEqual(details, ({"type": "bitrateLow", "severity": "warning"},))

    def test_good_health_with_info_issue_is_good(self) -> None:
        payload = {
            "ts_utc": "2026-08-10T00:00:00Z",
            "oauth_checked_ts_utc": "2026-08-10T00:00:00Z",
            "oauth_probe_ok": True,
            "oauth_stream_status": "active",
            "oauth_stream_health_status": "good",
            "oauth_stream_health_issues": 1,
            "oauth_stream_health_issue_details": [
                {"type": "videoResolutionSuboptimal", "severity": "info"}
            ],
            "ingest_connected": True,
        }

        result = classify_input_quality_sample(payload, now_ts=1_786_320_000)

        self.assertTrue(result["eligible"])
        self.assertTrue(result["good"])
        self.assertEqual(result["classification"], "good")

    def test_warning_health_is_bad_but_local_disconnect_is_ineligible(self) -> None:
        payload = {
            "ts_utc": "2026-08-10T00:00:00Z",
            "oauth_checked_ts_utc": "2026-08-10T00:00:00Z",
            "oauth_probe_ok": True,
            "oauth_stream_status": "active",
            "oauth_stream_health_status": "ok",
            "oauth_stream_health_issues": 1,
            "oauth_stream_health_issue_details": [
                {"type": "bitrateLow", "severity": "warning"}
            ],
            "ingest_connected": True,
        }

        result = classify_input_quality_sample(payload, now_ts=1_786_320_000)
        self.assertTrue(result["eligible"])
        self.assertFalse(result["good"])
        self.assertEqual(result["classification"], "bad_health_warning")

        payload["ingest_connected"] = False
        disconnected = classify_input_quality_sample(payload, now_ts=1_786_320_000)
        self.assertFalse(disconnected["eligible"])
        self.assertFalse(disconnected["good"])
        self.assertEqual(disconnected["classification"], "ineligible_local_ingest_disconnected")

    def test_stale_probe_is_not_eligible(self) -> None:
        payload = {
            "ts_utc": "2026-08-10T00:20:00Z",
            "oauth_checked_ts_utc": "2026-08-10T00:00:00Z",
            "oauth_probe_ok": True,
            "oauth_stream_status": "active",
            "oauth_stream_health_status": "good",
            "ingest_connected": True,
        }

        result = classify_input_quality_sample(payload, now_ts=1_786_321_200, max_age_sec=600)

        self.assertFalse(result["fresh"])
        self.assertFalse(result["eligible"])
        self.assertEqual(result["classification"], "ineligible_oauth_probe_stale_or_missing")

    def test_explicit_missing_checked_timestamp_does_not_fall_back_to_fresh_stats_time(self) -> None:
        payload = {
            "ts_utc": "2026-08-10T00:00:00Z",
            "oauth_checked_ts_utc": "",
            "oauth_probe_ok": True,
            "oauth_stream_status": "active",
            "oauth_stream_health_status": "good",
            "ingest_connected": True,
        }

        result = classify_input_quality_sample(payload, now_ts=1_786_320_000)

        self.assertFalse(result["fresh"])
        self.assertFalse(result["eligible"])


if __name__ == "__main__":
    unittest.main()
