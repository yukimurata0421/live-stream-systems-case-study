from __future__ import annotations

import importlib
import sys
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import youtube_watchdog  # type: ignore


class YouTubeWatchdogRestartBackoffTests(unittest.TestCase):
    def test_restart_stream_returns_failure_detail_without_exit(self) -> None:
        mod = importlib.reload(youtube_watchdog)
        failed = mock.Mock(returncode=1, stdout="", stderr="permission denied")
        with mock.patch.object(mod, "run_systemctl", return_value=failed):
            ok, detail = mod.restart_stream("test reason")
        self.assertFalse(ok)
        self.assertIn("permission denied", detail)


if __name__ == "__main__":
    unittest.main()
