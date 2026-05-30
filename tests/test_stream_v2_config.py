from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import stream_v2.config as stream_v2_config


class StreamV2ConfigTests(unittest.TestCase):
    def test_default_v2_state_root_matches_runtime_env_contract(self) -> None:
        try:
            with mock.patch.dict(os.environ, {}, clear=True):
                cfg = importlib.reload(stream_v2_config)
            self.assertEqual(cfg.DEFAULT_V2_STATE_ROOT, ROOT / ".state" / "adsb-streamnew-v2")
        finally:
            importlib.reload(stream_v2_config)

    def test_stream_runtime_state_dir_overrides_default_v2_state_root(self) -> None:
        try:
            with mock.patch.dict(os.environ, {"STREAM_RUNTIME_STATE_DIR": "/tmp/stream-v2-runtime"}, clear=True):
                cfg = importlib.reload(stream_v2_config)
            self.assertEqual(cfg.DEFAULT_V2_STATE_ROOT, Path("/tmp/stream-v2-runtime"))
        finally:
            importlib.reload(stream_v2_config)

    def test_stream_v2_state_root_takes_precedence_for_shadow_tools(self) -> None:
        try:
            with mock.patch.dict(
                os.environ,
                {
                    "STREAM_RUNTIME_STATE_DIR": "/tmp/stream-v2-runtime",
                    "STREAM_V2_STATE_ROOT": "/tmp/stream-v2-shadow",
                },
                clear=True,
            ):
                cfg = importlib.reload(stream_v2_config)
            self.assertEqual(cfg.DEFAULT_V2_STATE_ROOT, Path("/tmp/stream-v2-shadow"))
        finally:
            importlib.reload(stream_v2_config)


if __name__ == "__main__":
    unittest.main()
