from __future__ import annotations

import importlib.util
import json
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = (
    ROOT
    / "ops"
    / "public-publisher"
    / "site"
    / "scripts"
    / "collect_reliability.py"
)


def load_collector():
    spec = importlib.util.spec_from_file_location("collect_reliability_tested", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def summary(sli: float, target: float | None, *, observed: int = 100, bad: int = 1) -> dict:
    result = {
        "sli_pct": sli,
        "observed_points": observed,
        "bad_points": bad,
        "missing_points": 0,
        "coverage_pct": 100.0,
        "max_value": 4.95,
    }
    if target is not None:
        result["target_pct"] = target
    return result


def raw_report() -> dict:
    availability = summary(99.8, 99.0)
    availability_root = {
        "selected": availability,
        "raw_composite": {"bad_points": 3},
        "public_only_excluded_points": 2,
    }
    same_raw = summary(99.2, 99.9, observed=300, bad=2)
    same = {
        "raw_metric": same_raw,
        "actual_url_evidence": {
            "live_url_transition_count": 0,
            "distinct_live_url_count": 1,
            "row_count": 80,
        },
    }
    input_quality = summary(99.6, 99.0, observed=10_020, bad=34)
    input_quality.update(
        {
            "bad_minutes": 34.017,
            "eligible_seconds": 601_200,
            "excluded_seconds": 3_300,
            "timeline_missing_seconds": 180,
            "selected_measurement_source": "raw_oauth_probe_time_weighted",
        }
    )
    input_quality["raw_warning_evidence"] = {"warning_row_count": 4}
    viewer = summary(99.7, None, observed=40, bad=1)
    viewer.update(
        {
            "expected_points": 50,
            "total_probe_events": 42,
            "unknown_points": 2,
            "missing_points": 10,
            "event_coverage_pct": 84.0,
            "coverage_pct": 80.0,
            "evidence_unit": "viewer_probe_events",
        }
    )
    adsb = summary(100.0, None, observed=90, bad=0)
    adsb.update({"total_monitor_events": 91, "unknown_points": 1})
    visual = {
        **viewer,
        "viewer_probe_events": viewer,
        "adsb_rendering_events": adsb,
        "assessment_scope": "supporting_only",
        "evidence_units_separated": True,
    }
    return {
        "generated_at_utc": "2026-08-10T10:00:00Z",
        "metric_errors": {},
        "windows": {
            "24h": {"upload_ceiling": summary(100.0, 99.0, observed=1441, bad=0)},
            "7d": {
                "youtube_availability": availability_root,
                "youtube_input_quality": input_quality,
                "audio_correctness": summary(100.0, 99.5, bad=0),
                "visual_correctness": visual,
            },
            "30d": {"same_url_preservation": same},
        },
    }


class PublicReliabilityCollectorTests(unittest.TestCase):
    def test_public_payload_has_six_neutral_numeric_indicators(self) -> None:
        collector = load_collector()
        payload = collector.build_public_payload(raw_report())
        self.assertEqual(payload["schema"], "stream-v3-reliability-public.v4")
        self.assertEqual(len(payload["items"]), 6)
        self.assertNotIn("status", payload)
        self.assertTrue(all("status" not in item and "state" not in item for item in payload["items"]))
        self.assertNotIn("prometheus", str(payload).lower())

    def test_input_quality_uses_duration_and_keeps_recovery_warnings_separate(self) -> None:
        collector = load_collector()
        payload = collector.build_public_payload(raw_report())
        item = next(row for row in payload["items"] if row["id"] == "youtube_input_quality")
        facts = {row["label"]: row["value"] for row in item["facts"]}
        self.assertEqual(item["value"], 99.6)
        self.assertEqual(facts["Eligible duration"], 10_020)
        self.assertEqual(facts["Bad quality"], 34.017)
        self.assertEqual(facts["Outside denominator"], 55.0)
        self.assertEqual(facts["Missing evidence"], 3.0)
        self.assertEqual(facts["Recovery warnings (separate)"], 4)
        self.assertIn("outside this denominator", item["note"])

    def test_same_url_publishes_counts_without_actual_url(self) -> None:
        collector = load_collector()
        payload = collector.build_public_payload(raw_report())
        item = next(row for row in payload["items"] if row["id"] == "same_url_observation")
        facts = {row["label"]: row["value"] for row in item["facts"]}
        self.assertEqual(item["value"], 0.0)
        self.assertEqual(facts["Distinct URLs"], 1.0)
        self.assertNotIn("live_url", str(payload))
        self.assertNotIn("video_id", str(payload))

    def test_visual_uses_probe_events_and_keeps_unknown_separate(self) -> None:
        collector = load_collector()
        payload = collector.build_public_payload(raw_report())
        item = next(row for row in payload["items"] if row["id"] == "visual_supporting_samples")
        facts = {row["label"]: row["value"] for row in item["facts"]}
        self.assertEqual(item["label"], "Viewer evidence coverage")
        self.assertEqual(item["window"], "rolling 7d scheduled probes")
        self.assertEqual(item["value"], 80.0)
        self.assertEqual(facts["Eligible-frame result"], 99.7)
        self.assertEqual(facts["Scheduled probes"], 50.0)
        self.assertEqual(facts["Viewer probe events"], 42.0)
        self.assertEqual(facts["Eligible frames"], 40.0)
        self.assertEqual(facts["Confirmed visual bad"], 1.0)
        self.assertEqual(facts["Probe unavailable"], 2.0)
        self.assertEqual(facts["Probe event coverage"], 84.0)
        self.assertEqual(facts["ADS-B monitor events"], 91.0)
        self.assertEqual(facts["ADS-B supporting bad"], 0.0)
        self.assertIn("separate denominator", item["note"])
        self.assertIn("not visual correctness", item["note"])

    def test_fetch_uses_pinned_release_code_with_explicit_persistent_state(self) -> None:
        collector = load_collector()
        completed = mock.Mock(returncode=0, stdout=json.dumps(raw_report()), stderr="")
        with mock.patch.object(collector.subprocess, "run", return_value=completed) as run_mock:
            collector.fetch_raw_report(
                "arena-test",
                "/opt/stream_v3",
                "/var/lib/stream-v3/observability-monitor",
                timeout_sec=10,
            )

        command = run_mock.call_args.args[0][-1]
        self.assertIn("cd /opt/stream_v3", command)
        self.assertIn(
            "STREAM_RUNTIME_STATE_DIR=/var/lib/stream-v3/observability-monitor",
            command,
        )
        self.assertIn(
            "STREAM_RUNTIME_LOG_DIR=/var/lib/stream-v3/observability-monitor/logs",
            command,
        )

    def test_visual_keeps_legacy_card_until_event_schema_is_deployed(self) -> None:
        collector = load_collector()
        item = collector.visual_support_item(summary(98.9, None, observed=50, bad=1))
        facts = {row["label"]: row["value"] for row in item["facts"]}
        self.assertEqual(item["label"], "Visual supporting samples")
        self.assertEqual(facts["Observed"], 50.0)
        self.assertIn("pending the event-based source release", item["note"])


if __name__ == "__main__":
    unittest.main()
