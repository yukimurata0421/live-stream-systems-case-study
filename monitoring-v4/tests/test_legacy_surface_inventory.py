from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from stream_monitoring_v4.inventory.legacy_surface import (
    CONTRACT_SCHEMA,
    evaluate_legacy_surface,
    load_legacy_surface_contract,
    scan_incident_templates,
    scan_prometheus_alerts,
)


ROOT = Path(__file__).resolve().parents[1]
V3_ROOT = ROOT.parent if (ROOT.parent / "src" / "stream_core").is_dir() else ROOT.parent / "stream_v3"


class LegacySurfaceInventoryTests(unittest.TestCase):
    def test_repository_contract_covers_the_complete_observed_v3_surface(self) -> None:
        report = evaluate_legacy_surface(
            load_legacy_surface_contract(
                ROOT / "config" / "monitoring_v4_legacy_surface_contract.json"
            ),
            observed_alerts=scan_prometheus_alerts(
                (V3_ROOT / "ops" / "monitoring" / "prometheus" / "rules").glob(
                    "*.yml"
                )
            ),
            observed_incident_templates=scan_incident_templates(
                V3_ROOT
                / "src"
                / "stream_core"
                / "notifications"
                / "incidents.py"
            ),
        )

        self.assertTrue(report["static_ok"])
        self.assertFalse(report["cutover_ready"])
        self.assertEqual(report["alert_count"], 37)
        self.assertEqual(report["incident_template_count"], 30)
        self.assertEqual(report["retained_v3_count"], 67)
        self.assertEqual(report["missing_alerts"], [])
        self.assertEqual(report["unexpected_alerts"], [])
        self.assertEqual(report["missing_incident_templates"], [])
        self.assertEqual(report["unexpected_incident_templates"], [])

    def test_scanner_extracts_constants_fstrings_and_report_specs(self) -> None:
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            rules = root / "rules.yml"
            rules.write_text(
                "groups:\n  - rules:\n      - alert: ExampleAlert\n",
                encoding="utf-8",
            )
            incidents = root / "incidents.py"
            incidents.write_text(
                """
def sample(family):
    incident(ident="constant:incident")
    incident(ident=f"family:{family}")
    report_specs = [
        ("report:constant", "unused"),
        (f"report:{family}", "unused"),
    ]
""".lstrip(),
                encoding="utf-8",
            )

            self.assertEqual(scan_prometheus_alerts([rules]), ("ExampleAlert",))
            self.assertEqual(
                scan_incident_templates(incidents),
                (
                    "constant:incident",
                    "family:{family}",
                    "report:constant",
                    "report:{family}",
                ),
            )

    def test_missing_and_unexpected_targets_fail_closed(self) -> None:
        contract = {
            "schema": CONTRACT_SCHEMA,
            "v4_first_class_domains": ["rendering"],
            "alerts": {
                "ported_v4_shadow": [],
                "retained_v3": ["DeclaredAlert"],
                "diagnostic_only": [],
                "retired": [],
            },
            "incident_templates": {
                "ported_v4_shadow": [],
                "retained_v3": ["declared:incident"],
                "diagnostic_only": [],
                "retired": [],
            },
        }

        report = evaluate_legacy_surface(
            contract,
            observed_alerts=("UnexpectedAlert",),
            observed_incident_templates=("unexpected:incident",),
        )

        self.assertFalse(report["static_ok"])
        self.assertFalse(report["cutover_ready"])
        self.assertEqual(report["missing_alerts"], ["DeclaredAlert"])
        self.assertEqual(report["unexpected_alerts"], ["UnexpectedAlert"])
        self.assertEqual(
            report["missing_incident_templates"], ["declared:incident"]
        )
        self.assertEqual(
            report["unexpected_incident_templates"], ["unexpected:incident"]
        )


if __name__ == "__main__":
    unittest.main()
