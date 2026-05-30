from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stream_v2.subsystems.registry import stream_components_by_subsystem


class TestSubsystemStreamAdapters(unittest.TestCase):
    def test_each_subsystem_owns_stream_v3_components(self) -> None:
        by_subsystem = stream_components_by_subsystem()
        self.assertEqual(set(by_subsystem), {"rendering", "music", "local_delivery", "youtube_lifecycle", "monitoring"})
        for subsystem, components in by_subsystem.items():
            self.assertGreaterEqual(len(components), 3, subsystem)
            self.assertTrue(all(component.subsystem == subsystem for component in components))

    def test_youtube_lifecycle_adapter_marks_api_mutations_as_destructive(self) -> None:
        ytl = {component.name: component for component in stream_components_by_subsystem()["youtube_lifecycle"]}
        self.assertTrue(ytl["youtube_watchdog"].destructive)
        self.assertEqual(ytl["youtube_watchdog"].url_risk, "can_change_youtube_lifecycle")
        self.assertTrue(ytl["youtube_api"].destructive)
        self.assertFalse(ytl["youtube_video_id_resolver"].destructive)


if __name__ == "__main__":
    unittest.main()
