from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from stream_monitoring_v4.runtime.source_revision import (
    UNKNOWN_SOURCE_REVISION,
    normalize_source_revision,
    read_source_revision,
)


class SourceRevisionTests(unittest.TestCase):
    def test_effective_hotfix_and_commit_are_pinned_without_other_env_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "deployed-revision.env"
            path.write_text(
                "\n".join(
                    (
                        "STREAM_V3_DEPLOYED_REVISION=base-v1",
                        "STREAM_V3_DEPLOYED_COMMIT=0123456789abcdef0123456789abcdef01234567",
                        "STREAM_V3_DEPLOYED_HOTFIX=hotfix-v2",
                        "DISCORD_WEBHOOK_URL=must-not-be-read",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            value = read_source_revision(path)
        self.assertEqual(
            value,
            "hotfix-v2@0123456789abcdef0123456789abcdef01234567",
        )
        self.assertNotIn("must-not-be-read", value)

    def test_missing_or_invalid_source_revision_is_explicit_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self.assertEqual(read_source_revision(root / "missing.env"), UNKNOWN_SOURCE_REVISION)
            path = root / "invalid.env"
            path.write_text("STREAM_V3_DEPLOYED_HOTFIX=value with spaces\n", encoding="utf-8")
            self.assertEqual(read_source_revision(path), UNKNOWN_SOURCE_REVISION)
        self.assertEqual(normalize_source_revision("bad/value"), UNKNOWN_SOURCE_REVISION)
        self.assertEqual(normalize_source_revision("safe-v1@0123456"), "safe-v1@0123456")
        self.assertEqual(normalize_source_revision("r" * 240), "r" * 240)
        self.assertEqual(normalize_source_revision("r" * 241), UNKNOWN_SOURCE_REVISION)

    def test_symlink_nonregular_and_oversized_revision_are_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            target = root / "target.env"
            target.write_text("STREAM_V3_DEPLOYED_REVISION=safe-v1\n", encoding="utf-8")
            link = root / "link.env"
            link.symlink_to(target)
            self.assertEqual(read_source_revision(link), UNKNOWN_SOURCE_REVISION)
            self.assertEqual(read_source_revision(root), UNKNOWN_SOURCE_REVISION)
            target.write_text(
                "STREAM_V3_DEPLOYED_REVISION=" + "x" * 128,
                encoding="utf-8",
            )
            self.assertEqual(
                read_source_revision(target, max_bytes=32),
                UNKNOWN_SOURCE_REVISION,
            )


if __name__ == "__main__":
    unittest.main()
