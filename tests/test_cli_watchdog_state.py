from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "stream_core"))

import cli  # type: ignore


class CliWatchdogStateTests(unittest.TestCase):
    def test_state_path_prefers_explicit_ytw_state_file(self) -> None:
        with mock.patch("cli.read_env_file", return_value={"YTW_STATE_FILE": "/tmp/ytw_state.json"}):
            self.assertEqual(cli.youtube_watchdog_state_path(), Path("/tmp/ytw_state.json"))

    def test_state_path_uses_runtime_root_fallback(self) -> None:
        with mock.patch("cli.read_env_file", return_value={"STREAM_RUNTIME_STATE_DIR": "/tmp/runtime-root"}):
            self.assertEqual(
                cli.youtube_watchdog_state_path(),
                Path("/tmp/runtime-root/youtube_watchdog_state.json"),
            )

    def test_unhealthy_true_on_threshold_and_reason_marker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "youtube_watchdog_state.json"
            p.write_text(
                json.dumps(
                    {
                        "fail_count": 3,
                        "last_reason": "liveBroadcastContent=none observed",
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch("cli.youtube_watchdog_state_path", return_value=p):
                with mock.patch("cli.youtube_monitor_max_fails", return_value=3):
                    self.assertTrue(cli.youtube_watchdog_unhealthy())


if __name__ == "__main__":
    unittest.main()
