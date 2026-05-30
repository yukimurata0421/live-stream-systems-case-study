from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from stream_v2.stream_app import stream_app_root
from stream_v2.subsystems.registry import stream_components_by_subsystem, stream_components_payload


RUN_PRODUCTION_CONTRACT_TESTS = os.environ.get("STREAM_V2_RUN_PRODUCTION_CONTRACT_TESTS") == "1"


@unittest.skipUnless(
    RUN_PRODUCTION_CONTRACT_TESTS,
    "set STREAM_V2_RUN_PRODUCTION_CONTRACT_TESTS=1 to run production-root contract tests",
)
class TestSubsystemStreamContract(unittest.TestCase):
    def test_all_declared_stream_components_exist(self) -> None:
        missing = stream_components_payload()["missing"]
        self.assertEqual(missing, [])

    def test_component_paths_are_owned_by_stream_v2_root_tree(self) -> None:
        root = stream_app_root().resolve()
        source_root = Path("/home/yuki/projects/stream").resolve()
        for components in stream_components_by_subsystem().values():
            for component in components:
                self.assertTrue(str(component.path).startswith(str(stream_app_root())), component)
                resolved = component.path.resolve()
                self.assertTrue(str(resolved).startswith(str(root)), component)
                self.assertFalse(resolved.is_relative_to(source_root), component)

    def test_music_adapter_uses_local_ncs_music_asset(self) -> None:
        music_dir = stream_app_root() / "ncs_music"
        self.assertTrue(music_dir.is_dir())
        self.assertFalse(music_dir.is_symlink())
        self.assertTrue((music_dir / "major").exists())
        self.assertTrue((music_dir / "minor").exists())

    def test_subsystem_paths_cli_prints_component_mapping(self) -> None:
        cp = subprocess.run(
            [sys.executable, "-m", "stream_v2", "subsystem-paths"],
            cwd=ROOT,
            env={"PYTHONPATH": "src"},
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(cp.returncode, 0, cp.stderr)
        payload = json.loads(cp.stdout)
        self.assertEqual(payload["missing"], [])
        self.assertIn("rendering", payload["subsystems"])
        self.assertIn("youtube_lifecycle", payload["subsystems"])


if __name__ == "__main__":
    unittest.main()
