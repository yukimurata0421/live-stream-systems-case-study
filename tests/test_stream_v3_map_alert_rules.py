from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RULES = ROOT / "ops" / "monitoring" / "prometheus" / "rules" / "stream_v3_map.yml"


class StreamV3MapAlertRuleTests(unittest.TestCase):
    def test_all_map_alert_dependencies_have_explicit_absence_detection(self) -> None:
        source = RULES.read_text(encoding="utf-8")
        block = source.split("- alert: StreamV3MapRequiredMetricMissing", 1)[1].split(
            "- alert: StreamV3MapMonitorSampleMissingOrStale",
            1,
        )[0]
        required = {
            "stream_v3_map_monitor_sample_available",
            "stream_v3_map_monitor_sample_age_seconds",
            "stream_v3_map_monitor_delivery_critical_ok",
            "stream_v3_map_semantic_visual_contract_ok",
            "stream_v3_map_asset_identity_ok",
            "stream_v3_map_precipitation_data_ok",
            "stream_v3_map_precipitation_generation_integrity",
            "stream_v3_map_precipitation_render_applied",
            "stream_v3_viewer_synthetic_sample_available",
            "stream_v3_viewer_synthetic_sample_age_seconds",
        }
        for metric in required:
            with self.subTest(metric=metric):
                self.assertIn(f"absent({metric}", block)
        self.assertIn("must not trigger an automatic stream runtime restart", block)

    def test_precipitation_acquisition_and_render_alerts_are_independent(self) -> None:
        source = RULES.read_text(encoding="utf-8")
        acquisition = source.split("- alert: StreamV3MapPrecipitationUnavailable", 1)[1].split(
            "- alert: StreamV3MapPrecipitationRenderMismatch", 1
        )[0]
        rendering = source.split("- alert: StreamV3MapPrecipitationRenderMismatch", 1)[1].split(
            "- alert: StreamV3MapPrecipitationGenerationIntegrityFailure", 1
        )[0]

        acquisition_expr = acquisition.split("for: 20m", 1)[0]
        self.assertIn("stream_v3_map_precipitation_data_ok", acquisition_expr)
        self.assertNotIn("stream_v3_map_precipitation_render_applied", acquisition_expr)

        rendering_expr = rendering.split("for: 20m", 1)[0]
        self.assertIn("stream_v3_map_monitor_delivery_critical_ok", rendering_expr)
        self.assertIn("stream_v3_map_precipitation_data_ok", rendering_expr)
        self.assertIn("stream_v3_map_precipitation_render_applied", rendering_expr)

        for block in (acquisition, rendering):
            self.assertIn("for: 20m", block)
            self.assertIn("severity: warning", block)
            self.assertIn("not", block.lower())
            self.assertIn("runtime", block.lower())

    def test_precipitation_generation_integrity_has_its_own_weather_only_alert(self) -> None:
        source = RULES.read_text(encoding="utf-8")
        block = source.split(
            "- alert: StreamV3MapPrecipitationGenerationIntegrityFailure",
            1,
        )[1].split("- alert: StreamV3MapRuntimeContainerRestarted", 1)[0]

        self.assertIn("stream_v3_map_precipitation_generation_integrity", block)
        self.assertIn("for: 2m", block)
        self.assertIn("severity: warning", block)
        self.assertIn("must not restart", block)


if __name__ == "__main__":
    unittest.main()
