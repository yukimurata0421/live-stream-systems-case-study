"""ADS-B source coverage counts how long monitor evidence stays current, not events per 60 s minute.

Contract: an event is current until the next event of any kind, or 90 s later, whichever is
first; only time opened by a classified event is coverage; the window is exactly [start, end).

Run locally: python3 -m pytest -q tests/test_adsb_event_coverage.py
"""
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from stream_core.cli_support import sli_report as report

S = 1_800_000_000
HOUR_END = S + 3600  # end_ts of a one-hour window


def event(ts, *, known=True, heartbeat=True, moving=True, report_ok=True):
    if not known:
        return ts, {"rendering": {"state": "unknown"}}
    return ts, {"rendering": {
        "aircraft_json_ok": heartbeat, "adsb_source_heartbeat_ok": heartbeat,
        "aircraft_messages_moving": moving, "aircraft_positions_moving": moving,
        "stream1090_report_ok": report_ok, "upstream_stream1090_report_ok": True,
    }}


def measure(events, *, end_ts=HOUR_END):
    return report.rendering_event_measurements(events, start_ts=S, end_ts=end_ts)


class AdsbEventCoverageTests(unittest.TestCase):
    def test_a_66_second_monitor_cadence_is_full_coverage(self):
        events = [event(S + 66 * i) for i in range(56)]  # 0 .. 3630 s; 3630 lies after the window
        result = measure(events)
        self.assertEqual(result["window_seconds"], 3600)
        self.assertEqual(result["total_monitor_events"], 55)
        # The event count basis shows 61 expected vs 55 recorded; time coverage has no gap.
        self.assertEqual((result["expected_points"], result["missing_points"]), (61, 6))
        self.assertEqual((result["coverage_pct"], result["uncovered_seconds"]), (100.0, 0))
        self.assertEqual(result["coverage_basis"], "event_time_span")
        self.assertEqual(result["sli_pct"], 100.0)

    def test_an_unknown_row_ends_the_previous_classified_span(self):
        # Reviewer reproduction: known at 0 s, unknown at 30 s, 60 s window -> 50%, not 100%.
        result = measure([event(S), event(S + 30, known=False)], end_ts=S + 60)
        self.assertEqual(result["coverage_pct"], 50.0)
        self.assertEqual(result["event_coverage_pct"], 100.0)
        self.assertEqual(result["unclassified_seconds"], 30)

    def test_alternating_known_and_unknown_minutes_is_half_coverage(self):
        result = measure([event(S + 60 * i, known=i % 2 == 0) for i in range(60)])
        self.assertEqual((result["coverage_pct"], result["event_coverage_pct"]), (50.0, 100.0))
        self.assertEqual(result["unknown_points"], 30)

    def test_a_real_gap_is_uncovered_after_the_last_events_span(self):
        events = [event(S + 60 * i) for i in range(20)] + [event(S + 60 * i) for i in range(35, 60)]
        result = measure(events)
        # 19:00 stays current until 20:30; nothing is current until 35:00.
        self.assertEqual(result["uncovered_seconds"], 870)
        self.assertEqual(result["coverage_pct"], round((3600 - 870) / 3600 * 100, 3))

    def test_an_event_just_before_the_window_covers_its_opening_seconds(self):
        inside = [event(S + 60 + 60 * i) for i in range(59)]
        self.assertEqual(measure(inside)["uncovered_seconds"], 60)
        self.assertEqual(measure([event(S - 30)] + inside)["uncovered_seconds"], 0)
        # Older than one span: it expired before the window began.
        self.assertEqual(measure([event(S - 100)] + inside)["uncovered_seconds"], 60)
        # Pre-window events never change the event counts.
        self.assertEqual(measure([event(S - 30)] + inside)["total_monitor_events"], 59)

    def test_coverage_never_extends_past_the_declared_end(self):
        result = measure([event(S + 60 * i) for i in range(61)])  # last event exactly at end_ts
        self.assertEqual((result["window_seconds"], result["uncovered_seconds"], result["coverage_pct"]), (3600, 0, 100.0))
        late = measure([event(S + 3590)])
        self.assertEqual(late["uncovered_seconds"], 3590)

    def test_states_runs_and_report_issues_are_unchanged(self):
        events = [event(S + 66 * i, heartbeat=not 10 <= i < 13) for i in range(55)]
        events.append((S + 3000 + 1, {"rendering": {"aircraft_json_ok": True, "aircraft_messages_moving": False,
                                                    "aircraft_positions_moving": False, "stream1090_report_ok": False}}))
        result = measure(events)
        self.assertEqual((result["stale_points"], result["bad_points"], result["bad_run_count"]), (3, 3, 1))
        self.assertAlmostEqual(result["sli_pct"], 52 / 55 * 100, places=2)
        # A static historical row stays unknown but its report issue is still counted, as before.
        self.assertEqual((result["unknown_points"], result["report_issue_points"]), (1, 1))

    def test_quiet_source_and_report_failure_are_separate(self):
        result = measure([event(S, moving=False, report_ok=False)], end_ts=S + 60)
        self.assertEqual((result["quiet_points"], result["good_points"]), (1, 1))
        self.assertEqual((result["report_issue_points"], result["sli_pct"]), (1, 100.0))
        self.assertEqual((result["coverage_pct"], result["unclassified_seconds"]), (100.0, 0))

    def test_stale_source_is_classified_bad_evidence(self):
        result = measure([event(S, heartbeat=False)], end_ts=S + 60)
        self.assertEqual((result["stale_points"], result["sli_pct"]), (1, 0.0))
        self.assertEqual((result["coverage_pct"], result["unclassified_seconds"]), (100.0, 0))

    def test_report_loader_retains_pre_window_heartbeat_and_unknown_cutoff(self):
        for known, expected_coverage, expected_unclassified in [(True, 100.0, 0), (False, 98.333, 60)]:
            with self.subTest(known=known), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                rows = [event(S - 30, moving=False)]
                if not known:
                    rows.append(event(S - 10, known=False))
                rows += [event(S + 60 * i) for i in range(1, 60)]
                source = root / "subsystems.jsonl"
                source.write_text("".join(json.dumps({
                    "ts_utc": datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    **payload,
                }) + "\n" for ts, payload in rows), encoding="utf-8")
                result = report.build_sli_report(
                    report.SliReportContext(
                        youtube_watchdog_events_file=root / "watchdog.jsonl",
                        subsystems_status_events_file=source,
                    ),
                    windows="1h",
                    end_time=str(HOUR_END),
                    query_range=lambda *args: [],
                )
                visual = result["windows"]["1h"]["visual_correctness"]
                adsb = visual["adsb_source_events"]
                self.assertEqual(adsb, visual["adsb_rendering_events"])
                self.assertEqual(adsb["coverage_pct"], expected_coverage)
                self.assertEqual(adsb["unclassified_seconds"], expected_unclassified)
                self.assertEqual(adsb["uncovered_seconds"], 0)
                self.assertEqual((adsb["total_monitor_events"], adsb["quiet_points"]), (59, 0))


if __name__ == "__main__":
    unittest.main()
