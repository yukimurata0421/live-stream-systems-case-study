from __future__ import annotations

import gzip
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from stream_core.cli_support import sli_report  # type: ignore


class SliReportUnitTests(unittest.TestCase):
    @staticmethod
    def _utc(ts: int) -> str:
        return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def test_parse_windows_accepts_hours_days_and_deduplicates(self) -> None:
        specs = sli_report.parse_windows("24h,7d,28d,30d,7d")
        self.assertEqual([item.label for item in specs], ["24h", "7d", "28d", "30d"])
        self.assertEqual(specs[2].duration_sec, 28 * 86400)

        with self.assertRaises(ValueError):
            sli_report.parse_windows("28")

    def test_fetch_metric_points_chunks_28_days_and_deduplicates_boundaries(self) -> None:
        calls: list[tuple[int, int]] = []

        def query_range(base_url: str, query: str, start: int, end: int, step: int, timeout: float) -> list[dict]:
            del base_url, query, step, timeout
            calls.append((start, end))
            return [{"metric": {"__name__": "example"}, "values": [[start, "1"], [end, "1"]]}]

        end = 28 * 86400
        points = sli_report.fetch_metric_points(
            "http://prometheus",
            "example",
            0,
            end,
            query_range=query_range,
        )

        self.assertEqual(len(calls), 4)
        self.assertEqual(calls[0], (0, 7 * 86400))
        self.assertEqual(calls[-1], (21 * 86400, 28 * 86400))
        self.assertEqual(len(points), 5)

    def test_status_summary_calculates_coverage_sli_runs_and_budget(self) -> None:
        status = {0: True, 60: False, 120: False, 180: True}
        summary = sli_report.summarize_status(status, start_ts=0, end_ts=240)
        assessed = sli_report.with_policy(
            summary,
            sli_report.POLICIES["youtube_input_quality"],
            sli_report.WindowSpec("7d", 7 * 86400),
        )

        self.assertEqual(summary["expected_points"], 5)
        self.assertEqual(summary["observed_points"], 4)
        self.assertEqual(summary["bad_points"], 2)
        self.assertEqual(summary["sli_pct"], 50.0)
        self.assertEqual(summary["conservative_sli_pct"], 40.0)
        self.assertEqual(summary["bad_run_count"], 1)
        self.assertEqual(summary["max_bad_run_minutes"], 2.0)
        self.assertEqual(assessed["assessment_scope"], "formal_assessment")
        self.assertEqual(assessed["compliance_status"], "unknown")
        self.assertIn("minimum_coverage_not_met", assessed["measurement_unknown_reasons"])
        self.assertNotIn("official_compliance", assessed)
        self.assertEqual(assessed["nominal_budget_minutes"], 100.8)

    def test_viewer_probe_summary_keeps_capture_failure_unknown(self) -> None:
        events = [
            (0, {"frame_ok": True, "black_detected": False, "freeze_detected": False}),
            (300, {"frame_ok": False, "reason": "ffmpeg_timeout"}),
            (600, {"frame_ok": True, "black_detected": True, "freeze_detected": False}),
        ]

        summary = sli_report.viewer_probe_measurements(events, start_ts=0, end_ts=600)

        self.assertEqual(summary["total_probe_events"], 3)
        self.assertEqual(summary["observed_points"], 2)
        self.assertEqual(summary["good_points"], 1)
        self.assertEqual(summary["bad_points"], 1)
        self.assertEqual(summary["unknown_points"], 1)
        self.assertEqual(summary["missing_points"], 0)
        self.assertEqual(summary["event_coverage_pct"], 100.0)
        self.assertEqual(summary["coverage_pct"], 66.667)
        self.assertEqual(summary["sli_pct"], 50.0)
        self.assertEqual(summary["unknown_reason_counts"], {"ffmpeg_timeout": 1})
        self.assertTrue(summary["probe_failures_are_unknown"])

    def test_rendering_summary_accepts_message_or_position_movement(self) -> None:
        def row(messages: bool, positions: bool) -> dict:
            return {
                "rendering": {
                    "state": "healthy",
                    "aircraft_json_ok": True,
                    "aircraft_messages_moving": messages,
                    "aircraft_positions_moving": positions,
                    "stream1090_report_ok": True,
                    "upstream_stream1090_report_ok": True,
                    "adsb_freshness_ok": True,
                }
            }

        summary = sli_report.rendering_event_measurements(
            [(0, row(True, False)), (60, row(False, True)), (120, row(False, False))],
            start_ts=0,
            end_ts=120,
        )

        self.assertEqual(summary["observed_points"], 3)
        self.assertEqual(summary["good_points"], 2)
        self.assertEqual(summary["bad_points"], 1)
        self.assertEqual(summary["unknown_points"], 0)
        self.assertEqual(summary["sli_pct"], 66.667)
        self.assertEqual(summary["motion_contract"], "messages_moving_or_positions_moving")

    def test_timestamped_event_loader_projects_large_history_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "viewer.jsonl"
            path.write_text(
                json.dumps(
                    {
                        "checked_at_utc": self._utc(300),
                        "frame_ok": True,
                        "black_detected": False,
                        "large_evidence": "x" * 1000,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            events = sli_report.load_timestamped_events(
                path,
                timestamp_field="checked_at_utc",
                start_ts=0,
                end_ts=600,
                project=sli_report._viewer_event_projection,
            )

            self.assertEqual(len(events), 1)
            self.assertNotIn("large_evidence", events[0][1])
            self.assertEqual(events[0][1]["checked_at_utc"], self._utc(300))

    def test_timestamped_event_loader_keeps_newest_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "viewer.jsonl"
            duplicate_ts = self._utc(300)
            with gzip.open(path.with_name(path.name + ".1.gz"), "wt", encoding="utf-8") as fh:
                fh.write(json.dumps({"checked_at_utc": duplicate_ts, "reason": "older"}) + "\n")
            path.write_text(
                json.dumps({"checked_at_utc": duplicate_ts, "reason": "newer"}) + "\n",
                encoding="utf-8",
            )

            events = sli_report.load_timestamped_events(
                path,
                timestamp_field="checked_at_utc",
                start_ts=0,
                end_ts=600,
            )

            self.assertEqual(events, [(300, {"checked_at_utc": duplicate_ts, "reason": "newer"})])

    def test_rotated_watchdog_evidence_is_deduplicated_and_tracks_actual_url(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            current = root / "youtube_watchdog.jsonl"
            rotated = root / "youtube_watchdog.jsonl.1.gz"
            rows = [
                {
                    "ts_utc": "2026-08-01T00:00:00Z",
                    "event_id": "a",
                    "status": "ok",
                    "live_url": "https://youtube.test/watch?v=stable",
                    "video_id": "stable",
                    "expected_video_id": "stable",
                    "public_ok": True,
                    "ingest_connected": True,
                },
                {
                    "ts_utc": "2026-08-01T00:05:00Z",
                    "event_id": "b",
                    "status": "warn",
                    "live_url": "https://youtube.test/watch?v=stable",
                    "video_id": "stable",
                    "expected_video_id": "stable",
                    "public_ok": True,
                    "ingest_connected": False,
                    "failure_kind": "local_pipeline",
                },
            ]
            current.write_text(json.dumps(rows[1]) + "\n", encoding="utf-8")
            with gzip.open(rotated, "wt", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row) + "\n")

            events = sli_report.load_watchdog_events(current, start_ts=0, end_ts=2_000_000_000)
            evidence = sli_report.watchdog_evidence(events, start_ts=0, end_ts=2_000_000_000)

        self.assertEqual(len(events), 2)
        self.assertEqual(evidence["warning_row_count"], 1)
        self.assertEqual(evidence["warning_ingest_false_count"], 1)
        self.assertEqual(evidence["distinct_live_url_count"], 1)
        self.assertEqual(evidence["live_url_transition_count"], 0)
        self.assertTrue(evidence["actual_url_stable"])
        self.assertTrue(evidence["current_video_id_matches_expected"])

    def test_availability_excludes_uncorroborated_public_only_metric_noise(self) -> None:
        public = {-60: 0.0, 0: 1.0, 60: 0.0, 120: 1.0}
        ingest = {-60: 1.0, 0: 1.0, 60: 1.0, 120: 0.0}
        watchdog = {-60: 1.0, 0: 1.0, 60: 1.0, 120: 0.0}
        events = [
            (
                60,
                {
                    "ts_utc": "1970-01-01T00:01:00Z",
                    "status": "ok",
                    "public_ok": True,
                    "ingest_connected": True,
                },
            )
        ]

        result = sli_report.availability_measurements(
            public,
            ingest,
            watchdog,
            events,
            start_ts=0,
            end_ts=120,
            step_sec=60,
        )

        self.assertEqual(result["raw_composite"]["bad_points"], 2)
        self.assertEqual(result["selected"]["bad_points"], 1)
        self.assertEqual(result["public_only_candidate_points"], 1)
        self.assertEqual(result["public_only_excluded_points"], 1)

    def test_input_quality_uses_oauth_health_and_excludes_local_disconnect(self) -> None:
        start = 100_000
        events = [
            (
                start,
                {
                    "ts_utc": self._utc(start),
                    "oauth_checked_ts_utc": self._utc(start),
                    "oauth_probe_ok": True,
                    "oauth_stream_status": "active",
                    "oauth_stream_health_status": "good",
                    "oauth_stream_health_issues": 0,
                    "ingest_connected": True,
                },
            ),
            (
                start + 300,
                {
                    "ts_utc": self._utc(start + 300),
                    "oauth_checked_ts_utc": self._utc(start + 300),
                    "oauth_probe_ok": True,
                    "oauth_stream_status": "active",
                    "oauth_stream_health_status": "ok",
                    "oauth_stream_health_issues": 1,
                    "oauth_stream_health_issue_details": [
                        {"type": "bitrateLow", "severity": "warning"}
                    ],
                    "ingest_connected": True,
                    "status": "ok",
                },
            ),
            (
                start + 600,
                {
                    "ts_utc": self._utc(start + 600),
                    "oauth_checked_ts_utc": self._utc(start + 600),
                    "oauth_probe_ok": True,
                    "oauth_stream_status": "active",
                    "oauth_stream_health_status": "good",
                    "oauth_stream_health_issues": 0,
                    "ingest_connected": False,
                    "status": "warn",
                    "failure_kind": "local_pipeline",
                },
            ),
            (
                start + 900,
                {
                    "ts_utc": self._utc(start + 900),
                    "oauth_checked_ts_utc": self._utc(start + 900),
                    "oauth_probe_ok": True,
                    "oauth_stream_status": "active",
                    "oauth_stream_health_status": "good",
                    "oauth_stream_health_issues": 0,
                    "ingest_connected": True,
                },
            ),
        ]

        result = sli_report.youtube_input_quality_measurements(
            events,
            start_ts=start,
            end_ts=start + 1_200,
        )

        self.assertEqual(result["eligible_seconds"], 900)
        self.assertEqual(result["good_seconds"], 600)
        self.assertEqual(result["bad_seconds"], 300)
        self.assertEqual(result["excluded_seconds"], 300)
        self.assertEqual(result["sli_pct"], 66.667)
        self.assertEqual(result["bad_minutes"], 5.0)
        self.assertEqual(result["bad_run_count"], 1)
        self.assertEqual(result["issue_type_rows"], {"bitrateLow": 1})
        self.assertTrue(result["local_pipeline_excluded_from_input_quality"])

    def test_input_quality_caps_one_probe_at_freshness_deadline(self) -> None:
        start = 100_000
        events = [
            (
                start,
                {
                    "ts_utc": self._utc(start),
                    "oauth_checked_ts_utc": self._utc(start),
                    "oauth_probe_ok": True,
                    "oauth_stream_status": "active",
                    "oauth_stream_health_status": "good",
                    "oauth_stream_health_issues": 0,
                    "ingest_connected": True,
                },
            )
        ]

        result = sli_report.youtube_input_quality_measurements(
            events,
            start_ts=start,
            end_ts=start + 3_600,
            max_sample_span_sec=600,
        )

        self.assertEqual(result["eligible_seconds"], 600)
        self.assertEqual(result["timeline_missing_seconds"], 3_000)
        self.assertEqual(result["coverage_pct"], 16.667)

    def test_build_report_integrates_six_sli_families_and_marks_visual_supporting_only(self) -> None:
        end = 24 * 3600

        def query_range(base_url: str, query: str, start: int, stop: int, step: int, timeout: float) -> list[dict]:
            del base_url, timeout
            values = []
            for ts in range(start, stop + 1, step):
                value = 0.0 if "warn_count" in query or "black_detected" in query or "freeze_detected" in query else 1.0
                if "upload_p95" in query:
                    value = 4.9
                values.append([ts, str(value)])
            return [{"metric": {"__name__": query}, "values": values}]

        with tempfile.TemporaryDirectory() as td:
            ctx = sli_report.SliReportContext(
                youtube_watchdog_events_file=Path(td) / "youtube_watchdog.jsonl",
                prometheus_url="http://prometheus",
            )
            payload = sli_report.build_sli_report(
                ctx,
                windows="24h",
                end_time=str(end),
                query_range=query_range,
            )

        window = payload["windows"]["24h"]
        self.assertEqual(
            set(window),
            {
                "youtube_availability",
                "same_url_preservation",
                "upload_ceiling",
                "youtube_input_quality",
                "visual_correctness",
                "audio_correctness",
            },
        )
        self.assertEqual(window["youtube_input_quality"]["sli_pct"], 100.0)
        self.assertEqual(
            window["youtube_input_quality"]["selected_measurement_source"],
            "prometheus_oauth_quality_fallback",
        )
        self.assertFalse(window["youtube_input_quality"]["legacy_watchdog_warning_signal"]["formal_sli"])
        self.assertEqual(window["upload_ceiling"]["assessment_scope"], "formal_assessment")
        self.assertEqual(window["upload_ceiling"]["compliance_status"], "met")
        self.assertEqual(window["visual_correctness"]["official_assessment"], "manual_sampled_capture_check_required")
        self.assertTrue(window["visual_correctness"]["do_not_convert_to_official_burn"])
        self.assertEqual(window["visual_correctness"]["evidence_unit"], "viewer_probe_events")
        self.assertTrue(window["visual_correctness"]["evidence_units_separated"])

    def test_build_report_prefers_raw_oauth_quality_over_legacy_warn_gauge(self) -> None:
        end = 24 * 3600

        def query_range(base_url: str, query: str, start: int, stop: int, step: int, timeout: float) -> list[dict]:
            del base_url, timeout
            value = "1" if "youtube_warn_count" in query else "1"
            return [
                {
                    "metric": {"__name__": query},
                    "values": [[ts, value] for ts in range(start, stop + 1, step)],
                }
            ]

        with tempfile.TemporaryDirectory() as td:
            watchdog = Path(td) / "youtube_watchdog.jsonl"
            rows = []
            for offset, health, ingest in (
                (-900, "good", True),
                (-600, "ok", True),
                (-300, "good", False),
            ):
                ts = end + offset
                rows.append(
                    {
                        "ts_utc": self._utc(ts),
                        "oauth_checked_ts_utc": self._utc(ts),
                        "oauth_probe_ok": True,
                        "oauth_stream_status": "active",
                        "oauth_stream_health_status": health,
                        "oauth_stream_health_issues": 1 if health == "ok" else 0,
                        "ingest_connected": ingest,
                        "status": "warn" if not ingest else "ok",
                        "failure_kind": "local_pipeline" if not ingest else "",
                    }
                )
            watchdog.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            payload = sli_report.build_sli_report(
                sli_report.SliReportContext(
                    youtube_watchdog_events_file=watchdog,
                    prometheus_url="http://prometheus",
                ),
                windows="24h",
                end_time=str(end),
                query_range=query_range,
            )

        quality = payload["windows"]["24h"]["youtube_input_quality"]
        self.assertEqual(quality["selected_measurement_source"], "raw_oauth_probe_time_weighted")
        self.assertEqual(quality["sli_pct"], 50.0)
        self.assertEqual(quality["bad_minutes"], 5.0)
        self.assertEqual(quality["legacy_watchdog_warning_signal"]["sli_pct"], 0.0)
        self.assertEqual(quality["raw_oauth_measurement"]["excluded_seconds"], 300)


if __name__ == "__main__":
    unittest.main()
