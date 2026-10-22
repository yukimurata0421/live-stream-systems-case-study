"""Video maxrate and bufsize derive from VIDEO_BITRATE unless set explicitly (2026-10-22)."""
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from stream_core.engine import config as engine_config  # noqa: E402

RATE_KEYS = ("VIDEO_BITRATE", "VIDEO_MAXRATE", "VIDEO_BUFSIZE")


def load(**env):
    clean = {k: v for k, v in os.environ.items() if k not in RATE_KEYS}
    with mock.patch.dict(os.environ, {**clean, **env}, clear=True):
        return engine_config.load_config()


class EncoderRateDerivationTests(unittest.TestCase):
    def test_double_rate_units(self):
        self.assertEqual(engine_config.double_rate("4000k"), "8000k")
        self.assertEqual(engine_config.double_rate("4.5M"), "9000k")
        self.assertEqual(engine_config.double_rate("3400000"), "6800k")

    def test_default_contract_is_4000k_with_derived_maxrate_and_bufsize(self):
        c = load()
        self.assertEqual((c.video_bitrate, c.video_maxrate, c.video_bufsize), ("4000k", "4000k", "8000k"))

    def test_maxrate_and_bufsize_follow_bitrate(self):
        c = load(VIDEO_BITRATE="4300k")
        self.assertEqual((c.video_maxrate, c.video_bufsize), ("4300k", "8600k"))

    def test_explicit_values_win(self):
        c = load(VIDEO_BITRATE="4000k", VIDEO_MAXRATE="4200k", VIDEO_BUFSIZE="6000k")
        self.assertEqual((c.video_maxrate, c.video_bufsize), ("4200k", "6000k"))


if __name__ == "__main__":
    unittest.main()
