from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


def load_snapshot_module():
    path = Path(__file__).resolve().parents[1] / "ops" / "scripts" / "stream_v3_health_snapshot.py"
    spec = importlib.util.spec_from_file_location("stream_v3_health_snapshot", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class StreamV3HealthSnapshotTests(unittest.TestCase):
    def test_build_and_write_snapshots_are_repo_path_independent(self) -> None:
        module = load_snapshot_module()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "repo"
            state = Path(td) / "state"
            output = Path(td) / "snapshots"
            root.mkdir()
            state.mkdir()

            with mock.patch.object(module, "run_json", side_effect=[{"windows": []}, {"metrics": {}}]):
                snapshots = module.build_snapshots(repo_root=root, state_root=state, windows="1,8", timeout_sec=3)
                written = module.write_snapshots(output, snapshots)

            self.assertEqual({path.name for path in written}, {"health_summary_snapshot.json", "objective_sli_snapshot.json"})
            health = json.loads((output / "health_summary_snapshot.json").read_text(encoding="utf-8"))
            objective = json.loads((output / "objective_sli_snapshot.json").read_text(encoding="utf-8"))

        self.assertEqual(health["windows"], [])
        self.assertEqual(objective["metrics"], {})
        self.assertEqual(health["_snapshot"]["snapshot_source"], "stream_v3_health_snapshot")
        self.assertIn(str(root / "bin" / "stream-new"), health["_snapshot"]["command"][0])


if __name__ == "__main__":
    unittest.main()
