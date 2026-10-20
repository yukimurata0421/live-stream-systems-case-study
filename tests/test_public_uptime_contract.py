"""The public page must keep the text that the external uptime check matches.

On 2026-10-20 a redesign removed "Operational reliability" from the page and every checker region failed.
"""
import unittest
from pathlib import Path

INDEX = Path(__file__).resolve().parents[1] / "ops/public-publisher/site/public/index.html"
UPTIME_CONTENT_MATCH = "Operational reliability"


class PublicUptimeContractTests(unittest.TestCase):
    def test_index_contains_uptime_check_content_match(self):
        self.assertIn(UPTIME_CONTENT_MATCH, INDEX.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
