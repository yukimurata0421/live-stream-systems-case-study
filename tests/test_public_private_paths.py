from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
PRIVATE = "/home/" + "yuki"


def load_validator():
    spec = importlib.util.spec_from_file_location("validate_public_port", ROOT / "ops/scripts/validate_public_port.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class PrivatePathScanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.validator = load_validator()
        self.tmp = TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "publication").mkdir()
        self.addCleanup(self.tmp.cleanup)

    def scan(self, files: dict[str, str], allowed: dict[str, str]) -> list[tuple[str, str]]:
        for name, text in files.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        allowlist = self.root / "publication/private-path-allowlist.json"
        allowlist.write_text(json.dumps({
            "schema": "stream.public_private_path_allowlist.v1",
            "entries": [{"path": p, "reason": r} for p, r in allowed.items()],
        }))
        with mock.patch.object(self.validator, "ROOT", self.root), \
             mock.patch.object(self.validator, "ALLOWLIST", allowlist), \
             mock.patch.object(self.validator, "tracked_files", return_value=sorted(files)):
            return [(i["path"], i["reason"]) for i in self.validator.validate_private_paths()]

    def test_unlisted_file_is_reported(self) -> None:
        issues = self.scan({"ops/a.service": f"WorkingDirectory={PRIVATE}/projects/x\n"}, {})
        self.assertEqual(issues, [("ops/a.service", "private_workspace_path")])

    def test_quoted_path_is_reported(self) -> None:
        issues = self.scan({"src/a.py": f'HOME = "{PRIVATE}"\n'}, {})
        self.assertEqual(issues, [("src/a.py", "private_workspace_path")])

    def test_longer_user_name_is_not_reported(self) -> None:
        self.assertEqual(self.scan({"docs/a.md": PRIVATE + "murata/x\n"}, {}), [])

    def test_allowlisted_file_passes(self) -> None:
        self.assertEqual(self.scan({"src/a.py": f"ROOT = '{PRIVATE}/x'\n"}, {"src/a.py": "code default"}), [])

    def test_stale_entry_is_reported(self) -> None:
        issues = self.scan({"src/a.py": "ROOT = '/srv/x'\n"}, {"src/a.py": "code default"})
        self.assertEqual(issues, [("src/a.py", "stale_private_path_allowlist_entry")])

    def test_entry_without_reason_is_rejected(self) -> None:
        issues = self.scan({"src/a.py": f"ROOT = '{PRIVATE}/x'\n"}, {"src/a.py": " "})
        self.assertIn(("src/a.py", "invalid_private_path_allowlist_entry"), issues)

    def test_repository_is_clean(self) -> None:
        self.assertEqual(self.validator.validate_private_paths(), [])


if __name__ == "__main__":
    unittest.main()
