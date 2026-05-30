from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_api  # type: ignore
import youtube_watchdog_config  # type: ignore


class YouTubeApiConfigTests(unittest.TestCase):
    def test_parse_ingest_ports_prefers_explicit_multi_port_contract(self) -> None:
        with (
            mock.patch.object(youtube_api, "INGEST_TCP_PORT", 1935),
            mock.patch.object(youtube_api, "INGEST_TCP_PORTS_RAW", "1935,443,1935,bad"),
        ):
            self.assertEqual(youtube_api.parse_ingest_ports(), [1935, 443])

    def test_parse_ingest_ports_defaults_to_legacy_and_rtmps_443(self) -> None:
        with (
            mock.patch.object(youtube_api, "INGEST_TCP_PORT", 1935),
            mock.patch.object(youtube_api, "INGEST_TCP_PORTS_RAW", ""),
        ):
            self.assertEqual(youtube_api.parse_ingest_ports(), [1935, 443])

    def test_watchdog_defaults_match_current_youtube_contract(self) -> None:
        try:
            with mock.patch.dict(os.environ, {}, clear=True):
                cfg = importlib.reload(youtube_watchdog_config)

            self.assertEqual(cfg.RESTART_COOLDOWN_SEC, 180)
            self.assertEqual(cfg.API_COST_BURN_RATE_THRESHOLD_UNITS_PER_DAY, 9000)
            self.assertTrue(cfg.API_COST_BURN_RATE_FAIL_CLOSED)
        finally:
            importlib.reload(youtube_watchdog_config)


if __name__ == "__main__":
    unittest.main()
