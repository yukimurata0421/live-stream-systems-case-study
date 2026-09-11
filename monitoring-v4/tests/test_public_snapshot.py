from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class PublicSnapshotTests(unittest.TestCase):
    def test_public_snapshot_validator_accepts_the_complete_tree(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(ROOT / "ops/scripts/validate_public_snapshot.py")],
            cwd=ROOT,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        payload = json.loads(completed.stdout)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["issue_count"], 0)
        self.assertGreater(payload["file_count"], 100)


if __name__ == "__main__":
    unittest.main()
