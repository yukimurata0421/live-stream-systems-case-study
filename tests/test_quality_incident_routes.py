from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from stream_core.notifications import incidents


class QualityIncidentRouteTests(unittest.TestCase):
    def find(self, *, current=None, burn=False):
        payload = {
            "checked_at_utc": "2026-10-17T10:50:00Z",
            "formal_gates": {"youtube_input_quality": {
                "compliance_status": "breached", "measurement_status": "valid",
                "sli_pct": 98.472, "coverage_pct": 99.956,
            }},
        }
        if current is not None:
            payload["fast_feedback"] = {"youtube_input_quality": {
                "measurement_status": "valid", "raw_current": current,
            }}
        if burn:
            payload["multi_window_burn_alerts"] = [{
                "family": "youtube_input_quality", "severity": "critical",
                "rule": "fast_15m_1h", "burn_rates": {"15m": 20, "1h": 20},
            }]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "operational_reliability_status.json"
            path.write_text(json.dumps(payload))
            result = incidents.operational_reliability_incidents(
                status_file=path, now_ts=1792234200)
            self.assertEqual(json.loads(path.read_text()), payload)
        return {item["id"]: item for item in result}

    def test_formal_breach_stays_history_when_current_oauth_is_good(self):
        found = self.find(current={"available": True, "eligible": True,
                                   "good": True, "classification": "good"})
        self.assertEqual(found, {})

    def test_historical_gate_alone_is_not_a_current_quality_fault(self):
        self.assertEqual(self.find(), {})

    def test_current_oauth_warning_uses_existing_fast_feedback_route(self):
        found = self.find(current={"available": True, "eligible": True,
                                   "good": False, "classification": "bad_health_warning"})
        self.assertEqual(set(found), {"reliability:youtube_input_quality_fast_feedback"})
        self.assertIn("no_automatic_restart", next(iter(found.values()))["recovery_type"])

    def test_current_burn_remains_an_incident_even_with_current_good(self):
        found = self.find(current={"available": True, "eligible": True,
                                   "good": True, "classification": "good"}, burn=True)
        self.assertEqual(set(found), {"reliability:youtube_input_quality_multi_window_burn"})
        self.assertEqual(next(iter(found.values()))["severity"], "critical")


if __name__ == "__main__":
    unittest.main()
