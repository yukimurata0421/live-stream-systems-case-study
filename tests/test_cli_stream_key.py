from __future__ import annotations

import sys
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "stream_core"))

import cli  # type: ignore


class ParseStreamKeyTests(unittest.TestCase):
    def test_accepts_rtmp_and_rtmps(self) -> None:
        self.assertEqual(
            cli.parse_stream_key_from_rtmp_url("rtmp://a.rtmp.youtube.com/live2/KEY123"),
            "KEY123",
        )
        self.assertEqual(
            cli.parse_stream_key_from_rtmp_url("rtmps://a.rtmps.youtube.com/live2/KEY456"),
            "KEY456",
        )
        self.assertEqual(
            cli.parse_stream_key_from_rtmp_url("rtmps://a.rtmps.youtube.com:443/live2/KEY443"),
            "KEY443",
        )

    def test_accepts_query_suffix(self) -> None:
        self.assertEqual(
            cli.parse_stream_key_from_rtmp_url("rtmps://a.rtmp.youtube.com/live2/KEY789?foo=1"),
            "KEY789",
        )

    def test_rejects_non_youtube_hosts(self) -> None:
        self.assertEqual(cli.parse_stream_key_from_rtmp_url("rtmp://example.com/live2/KEY"), "")

    def test_rejects_invalid_scheme(self) -> None:
        self.assertEqual(cli.parse_stream_key_from_rtmp_url("https://youtube.com/live/abc"), "")

    def test_rejects_invalid_path(self) -> None:
        self.assertEqual(cli.parse_stream_key_from_rtmp_url("rtmp://a.rtmp.youtube.com/notlive2/KEY"), "")


if __name__ == "__main__":
    unittest.main()
