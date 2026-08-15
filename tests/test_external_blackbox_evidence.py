from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from stream_core.notifications import incidents


IMPORT_SPEC = importlib.util.spec_from_file_location(
    "stream_v3_external_blackbox_import",
    ROOT / "ops" / "scripts" / "stream_v3_external_blackbox_import.py",
)
assert IMPORT_SPEC is not None and IMPORT_SPEC.loader is not None
blackbox_import = importlib.util.module_from_spec(IMPORT_SPEC)
IMPORT_SPEC.loader.exec_module(blackbox_import)


class ExternalBlackboxEvidenceTests(unittest.TestCase):
    def test_default_youtube_target_describes_public_video_not_live_state(self) -> None:
        self.assertIn("youtube_public_video=www.youtube.com", blackbox_import.DEFAULT_TARGETS)
        self.assertNotIn("youtube_public_live=www.youtube.com", blackbox_import.DEFAULT_TARGETS)

    def test_import_tracks_contiguous_status_samples(self) -> None:
        current = {"checked_at_utc": "2026-08-10T00:10:00Z", "status": "unknown"}
        previous = {
            "checked_at_utc": "2026-08-10T00:05:00Z",
            "status": "unknown",
            "status_since_utc": "2026-08-10T00:00:00Z",
            "consecutive_status_samples": 2,
        }

        result = blackbox_import.add_status_continuity(
            current,
            previous,
            now_ts=1_786_320_600,
        )

        self.assertEqual(result["status_since_utc"], "2026-08-10T00:00:00Z")
        self.assertEqual(result["status_duration_seconds"], 600)
        self.assertEqual(result["consecutive_status_samples"], 3)

    def test_unknown_requires_two_contiguous_imports_and_uses_slow_reminder(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "external_blackbox_status.json"
            payload = {
                "checked_at_utc": "2026-08-10T00:09:00Z",
                "status_since_utc": "2026-08-10T00:09:00Z",
                "status": "unknown",
                "reason": "external_target_evidence_incomplete",
                "targets": {
                    "youtube_public_video": {
                        "status": "unknown",
                        "reason": "external_checker_source_disagreement",
                    }
                },
            }
            status_file.write_text(
                json.dumps({**payload, "consecutive_status_samples": 1}),
                encoding="utf-8",
            )
            first = incidents.external_blackbox_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )
            status_file.write_text(
                json.dumps({**payload, "consecutive_status_samples": 2}),
                encoding="utf-8",
            )
            second = incidents.external_blackbox_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertEqual(first, [])
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]["repeat_sec"], 900)
        self.assertIn("external_checker_source_disagreement", second[0]["evidence"])

    def test_failed_external_quorum_is_immediate_warning_with_five_minute_reminder(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            status_file = Path(td) / "external_blackbox_status.json"
            status_file.write_text(
                json.dumps(
                    {
                        "checked_at_utc": "2026-08-10T00:09:00Z",
                        "status": "failed",
                        "reason": "one_or_more_external_targets_failed",
                        "targets": {
                            "public_status": {
                                "status": "failed",
                                "reason": "all_external_checker_locations_failed",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            found = incidents.external_blackbox_incidents(
                status_file=status_file,
                now_ts=1_786_320_600,
            )

        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]["id"], "external:blackbox_failed")
        self.assertEqual(found[0]["repeat_sec"], 300)
        self.assertNotIn("restart", found[0]["recovery_type"])


if __name__ == "__main__":
    unittest.main()
