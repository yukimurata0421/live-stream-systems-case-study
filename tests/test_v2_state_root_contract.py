from __future__ import annotations

import importlib
import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
WATCHERS = SRC / "watchers"
EXPECTED_STATE_ROOT = ROOT / ".state" / "adsb-streamnew-v2"
LEGACY_V2_HOME_ROOT = "/home/testuser/.local/state/adsb-streamnew-v2"

for path in (SRC, WATCHERS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def load_script_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class V2StateRootContractTests(unittest.TestCase):
    def test_runtime_defaults_use_repo_local_v2_state_root(self) -> None:
        with mock.patch.dict(os.environ, {"HOME": "/home/testuser"}, clear=True):
            stream_watchdog = importlib.reload(importlib.import_module("stream_watchdog"))
            youtube_watchdog_config = importlib.reload(importlib.import_module("youtube_watchdog_config"))
            fast_recovery = importlib.reload(importlib.import_module("fast_recovery"))
            observe_stream_health = load_script_module(
                "observe_stream_health_state_contract",
                ROOT / "ops" / "scripts" / "observe_stream_health.py",
            )
            report_youtube_api_cost = load_script_module(
                "report_youtube_api_cost_state_contract",
                ROOT / "ops" / "scripts" / "report_youtube_api_cost.py",
            )

        paths = [
            stream_watchdog.STATE_ROOT,
            stream_watchdog.EVENT_LOG_FILE,
            youtube_watchdog_config.STATE_BASE_DIR,
            youtube_watchdog_config.OAUTH_TOKEN_STATE_FILE,
            fast_recovery.EVENT_LOG_FILE,
            fast_recovery.YTW_STATS_FILE,
            observe_stream_health.STATE_BASE_DIR,
            observe_stream_health.API_COST_OPEN_DAY_LATEST_FILE,
            report_youtube_api_cost.STATE_BASE_DIR,
            report_youtube_api_cost.DEFAULT_LOG_FILE,
        ]

        for path in paths:
            with self.subTest(path=path):
                text = str(path)
                self.assertIn(str(EXPECTED_STATE_ROOT), text)
                self.assertNotIn(LEGACY_V2_HOME_ROOT, text)


if __name__ == "__main__":
    unittest.main()
