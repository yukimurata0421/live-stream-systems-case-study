from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "stream_v3_planned_rollout",
    ROOT / "ops" / "scripts" / "stream_v3_planned_rollout.py",
)
assert SPEC and SPEC.loader
planned_rollout = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(planned_rollout)


class PlannedRolloutTests(unittest.TestCase):
    def test_rollout_patch_updates_annotation_and_image_in_one_template_change(self) -> None:
        patch = planned_rollout.rollout_patch(
            rollout_id="rollout-test",
            reason="coverage ledger update",
            started_ts=1_800,
            ttl_sec=900,
            images=[{"name": "stream-engine", "image": "stream-v3:test"}],
        )

        template = patch["spec"]["template"]
        annotations = template["metadata"]["annotations"]
        self.assertEqual(annotations["stream-v3.yukimurata.dev/planned-rollout-id"], "rollout-test")
        self.assertEqual(annotations["stream-v3.yukimurata.dev/planned-rollout-at"], "1970-01-01T00:30:00Z")
        self.assertEqual(annotations["stream-v3.yukimurata.dev/planned-rollout-expires-at"], "1970-01-01T00:45:00Z")
        self.assertEqual(template["spec"]["containers"], [{"name": "stream-engine", "image": "stream-v3:test"}])

    def test_parse_images_rejects_duplicate_and_invalid_values(self) -> None:
        with self.assertRaises(ValueError):
            planned_rollout.parse_images(["missing-separator"])
        with self.assertRaises(ValueError):
            planned_rollout.parse_images(["stream-engine=a", "stream-engine=b"])


if __name__ == "__main__":
    unittest.main()
