from __future__ import annotations

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_api  # type: ignore


class YouTubePublicLiveProbeTests(unittest.TestCase):
    def test_parse_yt_dlp_was_live_as_not_live(self) -> None:
        result = youtube_api.parse_public_live_probe_output(
            "ML0BR5YkyY0\twas_live\tFalse\tTrue\tpublic\n"
        )
        self.assertTrue(result.checked)
        self.assertEqual(result.verdict, "not_live")
        self.assertEqual(result.video_id, "ML0BR5YkyY0")
        self.assertFalse(result.is_live)
        self.assertTrue(result.was_live)

    def test_parse_yt_dlp_live_as_live(self) -> None:
        result = youtube_api.parse_public_live_probe_output(
            "ML0BR5YkyY0\tis_live\tTrue\tFalse\tpublic\n"
        )
        self.assertTrue(result.checked)
        self.assertEqual(result.verdict, "live")
        self.assertTrue(result.is_live)

    def test_parse_unknown_probe_output_as_unknown(self) -> None:
        result = youtube_api.parse_public_live_probe_output(
            "ML0BR5YkyY0\tNA\tNA\tNA\tpublic\n"
        )
        self.assertTrue(result.checked)
        self.assertEqual(result.verdict, "unknown")


if __name__ == "__main__":
    unittest.main()
