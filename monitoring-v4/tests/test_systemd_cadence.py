from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UNITS = ROOT / "ops" / "systemd"


class SystemdCadenceTests(unittest.TestCase):
    def test_shadow_cycle_is_wall_clock_anchored_without_relative_drift(self) -> None:
        text = (UNITS / "stream-monitoring-v4-shadow.timer").read_text(encoding="utf-8")
        self.assertIn("OnCalendar=*-*-* *:*:20", text)
        self.assertIn("AccuracySec=1s", text)
        self.assertNotIn("OnUnitActiveSec", text)
        self.assertNotIn("OnUnitInactiveSec", text)

    def test_evidence_report_is_wall_clock_anchored_after_shadow_cycle(self) -> None:
        text = (UNITS / "stream-monitoring-v4-shadow-report.timer").read_text(encoding="utf-8")
        self.assertIn("OnCalendar=*-*-* *:0/5:50", text)
        self.assertIn("AccuracySec=1s", text)
        self.assertNotIn("OnUnitActiveSec", text)
        self.assertNotIn("OnUnitInactiveSec", text)


if __name__ == "__main__":
    unittest.main()
