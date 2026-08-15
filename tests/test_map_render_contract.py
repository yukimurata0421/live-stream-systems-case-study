from __future__ import annotations

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_core.map_asset_manifest import (  # noqa: E402
    ASSET_FILES,
    build_asset_manifest,
    verify_asset_manifest,
)
from stream_core.map_render_contract import (  # noqa: E402
    REQUIRED_LAYERS,
    REQUIRED_SOURCES,
    REQUIRED_UI_ELEMENTS,
    normalize_semantic_render_report,
)


def healthy_report() -> dict[str, object]:
    return {
        "schema": "stream_v3.map_semantic_render.v1",
        "map_style_loaded": True,
        "render_context_healthy": True,
        "required_sources": {name: True for name in REQUIRED_SOURCES},
        "required_layers": {name: True for name in REQUIRED_LAYERS},
        "ui_elements": {name: True for name in REQUIRED_UI_ELEMENTS},
        "aircraft_sample_count": 12,
        "aircraft_source_feature_count": 12,
        "aircraft_rendered_feature_count": 10,
        "coverage_point_count": 90,
        "coverage_source_feature_count": 1,
        "range_ring_feature_count": 3,
        "range_label_feature_count": 3,
        "map_error_count": 2,
    }


class MapRenderContractTests(unittest.TestCase):
    def test_semantic_contract_computes_health_instead_of_trusting_browser_ok(self) -> None:
        report = healthy_report()
        report["ok"] = False

        normalized = normalize_semantic_render_report(report)

        self.assertTrue(normalized["ok"])
        self.assertEqual(normalized["failed_checks"], [])
        self.assertEqual(normalized["aircraft_source_feature_count"], 12)
        self.assertEqual(normalized["map_error_count"], 2)

    def test_semantic_contract_detects_missing_layer_and_count_disagreement(self) -> None:
        report = healthy_report()
        report["required_layers"]["aircraft-icon"] = False
        report["aircraft_source_feature_count"] = 11
        report["range_label_feature_count"] = 2

        normalized = normalize_semantic_render_report(report)

        self.assertFalse(normalized["ok"])
        self.assertEqual(
            normalized["failed_checks"],
            ["layer:aircraft-icon", "aircraft_source_count", "range_label_count"],
        )

    def test_semantic_contract_rejects_unknown_keys_and_unbounded_counts(self) -> None:
        report = healthy_report()
        report["required_sources"]["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "exact required names"):
            normalize_semantic_render_report(report)

        report = healthy_report()
        report["aircraft_sample_count"] = 100_001
        with self.assertRaisesRegex(ValueError, "bounded integer"):
            normalize_semantic_render_report(report)

    def test_source_controlled_asset_manifest_matches_every_allowlisted_file(self) -> None:
        root = ROOT / "ui" / "overlay"

        verified = verify_asset_manifest(root)

        self.assertEqual(len(verified["files"]), len(ASSET_FILES))
        self.assertEqual(verified, build_asset_manifest(root))

    def test_asset_manifest_fails_closed_on_modified_or_linked_asset(self) -> None:
        source = ROOT / "ui" / "overlay"
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "overlay"
            shutil.copytree(source, root)
            (root / "adsb-map" / "map.css").write_text("modified", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "does not match"):
                verify_asset_manifest(root)

            shutil.copy2(
                source / "adsb-map" / "map.css",
                root / "adsb-map" / "map.css",
            )
            (root / "adsb-map" / "map.js").unlink()
            (root / "adsb-map" / "map.js").symlink_to(
                source / "adsb-map" / "map.js"
            )
            with self.assertRaisesRegex(ValueError, "missing or linked"):
                verify_asset_manifest(root)

    def test_manifest_generator_output_is_canonical_and_secret_free(self) -> None:
        root = ROOT / "ui" / "overlay"
        manifest = build_asset_manifest(root)
        rendered = json.dumps(manifest, sort_keys=True, separators=(",", ":"))

        self.assertNotIn("/home/", rendered)
        self.assertNotIn("token", rendered.lower())
        self.assertRegex(str(manifest["revision"]), r"^[0-9a-f]{64}$")


if __name__ == "__main__":
    unittest.main()
