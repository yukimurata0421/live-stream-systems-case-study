from __future__ import annotations

import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_core.notifications import incidents
from stream_core.notifications.renderer import format_discord_message


IMPORT_SPEC = importlib.util.spec_from_file_location(
    "stream_v3_external_blackbox_import",
    ROOT / "ops" / "scripts" / "stream_v3_external_blackbox_import.py",
)
assert IMPORT_SPEC is not None and IMPORT_SPEC.loader is not None
blackbox_import = importlib.util.module_from_spec(IMPORT_SPEC)
IMPORT_SPEC.loader.exec_module(blackbox_import)

EXPORT_SPEC = importlib.util.spec_from_file_location("blackbox_export", ROOT / "ops/scripts/stream_v3_external_blackbox_export.py")
blackbox_export = importlib.util.module_from_spec(EXPORT_SPEC)
with patch.dict(sys.modules, {"stream_v3_external_blackbox_import": blackbox_import}):
    EXPORT_SPEC.loader.exec_module(blackbox_export)


class ExternalBlackboxEvidenceTests(unittest.TestCase):
    def test_history_export_keeps_false_missing_and_failed_queries_distinct(self) -> None:
        def fetch(*, metric_type, **_):
            name = metric_type.split("/")[-1]
            if name == "error_code":
                raise RuntimeError("monitoring_api:http_403")
            if name == "request_latency":
                return {"timeSeries": []}
            point = {"http_status": {"stringValue": "200"}, "check_passed": {"boolValue": False},
                     "content_mismatch": {"boolValue": True}}[name]
            row = self.series("original", "region-a", False, 1000)
            row["points"][0]["value"] = point
            return {"timeSeries": [row]}
        with patch.object(blackbox_export, "fetch_time_series", side_effect=fetch):
            result = blackbox_export.export_window(project="test", host="status.test", check_id=None,
                                                    token="fixture", start_ts=900, end_ts=1100, timeout_sec=2)
        self.assertEqual(len(result["points"]), 3)
        self.assertEqual(result["errors"], {"error_code": "monitoring_api:http_403"})
        self.assertEqual(result["points"][0], {"metric": "check_passed", "check_id": "original", "location": "region-a",
                                               "end_time": blackbox_import.iso_utc(1000), "value": {"boolValue": False}})
        self.assertEqual(blackbox_export.timestamp("2026-11-01T09:00:00+09:00"), blackbox_export.timestamp("2026-11-01T00:00:00Z"))

    @staticmethod
    def series(check_id, region, passed, ts):
        return {"metric": {"labels": {"check_id": check_id, "checker_location": region}},
                "points": [{"interval": {"endTime": blackbox_import.iso_utc(ts)}, "value": {"boolValue": passed}}]}

    def test_unpinned_checks_never_mix_across_pages_and_pinned_filters_the_api(self) -> None:
        now = 1_786_320_600
        rows_a = [self.series("first", r, False, now - 40) for r in ("a", "b", "c")]
        rows_b = [self.series("second", r, True, now - 20) for r in ("a", "b", "c")]
        args = dict(project="test", host="status.test", token="fixture", start_ts=now-600, end_ts=now, timeout_sec=2)
        def page(rows, **kwargs):
            return io.BytesIO(json.dumps({"timeSeries": rows, **kwargs}).encode())
        with patch.object(blackbox_import.urllib.request, "urlopen",
                          side_effect=[page(rows_a, nextPageToken="more"), page(rows_b)]) as get:
            data = blackbox_import.fetch_time_series(**args)
        query = parse_qs(urlsplit(get.call_args_list[1].args[0].full_url).query)
        self.assertEqual(query["pageToken"], ["more"])
        self.assertNotIn("check_id", query["filter"][0])
        assessment = dict(host="status.test", now_ts=now, stale_sec=600, minimum_locations=3, minimum_pass_ratio=2/3)
        unknown = blackbox_import.assess_target(data, **assessment)
        self.assertEqual((unknown["status"], unknown["reason"]), ("unknown", "ambiguous_external_check_id"))
        self.assertEqual(unknown["observed_check_ids"], ["first", "second"])
        self.assertFalse(unknown["locations"])
        with patch.object(blackbox_import.urllib.request, "urlopen", return_value=page(rows_a)) as get:
            blackbox_import.fetch_time_series(**args, check_id="first")
        query = parse_qs(urlsplit(get.call_args.args[0].full_url).query)
        self.assertIn('metric.labels.check_id="first"', query["filter"][0])
        self.assertEqual(blackbox_import.assess_target(data, **assessment, check_id="first")["status"], "failed")
        self.assertEqual(blackbox_import.assess_target(data, **assessment, check_id="absent")["status"], "unknown")
        missing_id = {"timeSeries": [self.series("", r, False, now) for r in ("a", "b", "c")]}
        self.assertEqual(blackbox_import.assess_target(missing_id, **assessment)["reason"], "external_check_id_missing")

    def test_failed_second_page_never_supplies_partial_quorum(self) -> None:
        first = {"timeSeries": [self.series("first", "a", False, 1000)], "nextPageToken": "more"}
        for last in ({"executionErrors": [{"message": "partial"}]}, {"nextPageToken": "more"}):
            with self.subTest(last=last), patch.object(blackbox_import.urllib.request, "urlopen",
                    side_effect=[io.BytesIO(json.dumps(p).encode()) for p in (first, last)]):
                with self.assertRaises(RuntimeError):
                    blackbox_import.fetch_time_series(project="test", host="status.test", token="fixture",
                                                       start_ts=900, end_ts=1100, timeout_sec=2)

    def test_live_entrypoint_preserves_alias_and_scopes_the_rendered_notification(self) -> None:
        now = 1_786_320_600
        checks = {"status.test": "status-id", "video.test": "video-id"}
        def fetch(*, host, check_id, **_):
            self.assertEqual(check_id, checks[host])
            points = [self.series(check_id, region, host == "video.test", now-age)
                      for region, age in [("a", 20), ("b", 30), ("c", 40), ("stale", 700), ("future", -5)]]
            return {"timeSeries": points}
        with tempfile.TemporaryDirectory() as td:
            output = Path(td) / "status.json"
            args = ["import", "--project", "fixture", "--now", str(now), "--output", str(output),
                    "--target", "public_status=status.test", "--target", "youtube_public_live=video.test",
                    "--expected-locations", "public_status=a,b,c,absent,stale"]
            with patch.dict(os.environ, {"STREAM_V3_BLACKBOX_CHECK_IDS": "public_status=status-id youtube_public_live=video-id",
                                         "STREAM_V3_BLACKBOX_EXPECTED_LOCATIONS": ""}), patch.object(sys, "argv", args), \
                    patch.object(blackbox_import, "access_token", return_value="fixture"), \
                    patch.object(blackbox_import, "fetch_time_series", side_effect=fetch), patch("builtins.print"):
                self.assertEqual(blackbox_import.main(), 1)
            status = json.loads(output.read_text())
            self.assertFalse(status["formal_sli"])
            region = status["targets"]["public_status"]
            self.assertEqual(region["missing_fresh_locations"], ["absent", "stale"])
            self.assertEqual((region["failed_locations"], region["nonfresh_locations"]), (3, 2))
            self.assertEqual(region["fresh_from_utc"], blackbox_import.iso_utc(now-40))
            self.assertEqual(region["fresh_through_utc"], blackbox_import.iso_utc(now-20))
            self.assertEqual(region["locations"]["future"]["fresh"], False)
            self.assertEqual(status["targets"]["youtube_public_live"]["status"], "ok")
            found = incidents.external_blackbox_incidents(status_file=output, now_ts=now)
            self.assertEqual(found[0]["id"], "external:blackbox_failed")
            body = format_discord_message(phase="detected", incidents=found, state={}, now_ts=now)
            self.assertIn("Public status endpoint check failed", body.splitlines()[0])
            self.assertIn("pass=0 fail=3 nonfresh=2 missing_fresh=2", body)
            self.assertIn("failed_regions=a,b,c", body)
            self.assertIn("youtube_public_live:ok", body)
            status["targets"]["youtube_public_live"]["status"] = "unknown"
            output.write_text(json.dumps(status))
            self.assertNotIn("blackbox_scope", incidents.external_blackbox_incidents(status_file=output, now_ts=now)[0])

    def test_default_youtube_target_describes_public_video_not_live_state(self) -> None:
        self.assertIn("youtube_public_video=www.youtube.com", blackbox_import.DEFAULT_TARGETS)
        self.assertNotIn("youtube_public_live=www.youtube.com", blackbox_import.DEFAULT_TARGETS)

    def test_import_tracks_contiguous_status_samples(self) -> None:
        current = {"checked_at_utc": "2026-08-10T00:10:00Z", "status": "unknown"}
        previous = {
            "checked_at_utc": "2026-08-10T00:05:00Z",
            "status": "unknown",
            "status_since_utc": "2026-08-10T00:00:00Z",
            "consecutive_status_samples": 2,
        }

        result = blackbox_import.add_status_continuity(
            current,
            previous,
            now_ts=1_786_320_600,
        )

        self.assertEqual(result["status_since_utc"], "2026-08-10T00:00:00Z")
        self.assertEqual(result["status_duration_seconds"], 600)
        self.assertEqual(result["consecutive_status_samples"], 3)

    def test_unknown_requires_two_contiguous_imports_and_uses_slow_reminder(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "external_blackbox_status.json"
            payload = {
                "checked_at_utc": "2026-08-10T00:09:00Z",
                "status_since_utc": "2026-08-10T00:09:00Z",
                "status": "unknown",
                "reason": "external_target_evidence_incomplete",
                "targets": {
                    "youtube_public_video": {
                        "status": "unknown",
                        "reason": "external_checker_source_disagreement",
                    }
                },
            }
            status_file.write_text(
                json.dumps({**payload, "consecutive_status_samples": 1}),
                encoding="utf-8",
            )
            first = incidents.external_blackbox_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )
            status_file.write_text(
                json.dumps({**payload, "consecutive_status_samples": 2}),
                encoding="utf-8",
            )
            second = incidents.external_blackbox_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertEqual(first, [])
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]["repeat_sec"], 900)
        self.assertIn("external_checker_source_disagreement", second[0]["evidence"])

    def test_failed_external_quorum_is_immediate_warning_with_five_minute_reminder(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "external_blackbox_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-10T00:09:00Z",
                        "status": "failed",
                        "reason": "one_or_more_external_targets_failed",
                        "targets": {
                            "public_status": {
                                "status": "failed",
                                "reason": "all_external_checker_locations_failed",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.external_blackbox_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]["id"], "external:blackbox_failed")
        self.assertEqual(found[0]["repeat_sec"], 300)
        self.assertNotIn("restart", found[0]["recovery_type"])


if __name__ == "__main__":
    unittest.main()
