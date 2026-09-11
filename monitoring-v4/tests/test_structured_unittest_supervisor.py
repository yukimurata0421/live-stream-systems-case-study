from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class StructuredUnittestSupervisorTests(unittest.TestCase):
    def test_child_imports_project_tests_package_from_script_context(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "test_imports_project_package.py").write_text(
                "from tests.helpers import BASE_TS\n"
                "import unittest\n"
                "class ImportProjectPackage(unittest.TestCase):\n"
                "    def test_base_timestamp(self):\n"
                "        self.assertGreater(BASE_TS, 0)\n",
                encoding="utf-8",
            )
            events = root / "events.jsonl"
            project_root = Path(__file__).resolve().parents[1]
            environment = {**os.environ, "PYTHONPATH": str(project_root / "src")}
            completed = subprocess.run(
                [
                    sys.executable,
                    str(project_root / "tests" / "structured_unittest_supervisor.py"),
                    "--child",
                    "--events",
                    str(events),
                    "--start-dir",
                    str(root),
                ],
                cwd=root,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            payload = [json.loads(line) for line in events.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([item["kind"] for item in payload], ["passed", "summary"])
