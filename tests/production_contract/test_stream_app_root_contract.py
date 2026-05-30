from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from stream_v2.stream_app import STREAM_APP_BIN, stream_app_root


RUN_PRODUCTION_CONTRACT_TESTS = os.environ.get("STREAM_V2_RUN_PRODUCTION_CONTRACT_TESTS") == "1"


@unittest.skipUnless(
    RUN_PRODUCTION_CONTRACT_TESTS,
    "set STREAM_V2_RUN_PRODUCTION_CONTRACT_TESTS=1 to run production-root contract tests",
)
class TestStreamAppRoot(unittest.TestCase):
    def test_stream_v2_root_tree_is_present(self) -> None:
        root = stream_app_root()
        self.assertTrue((root / "src" / "stream_core" / "stream_engine.py").exists())
        self.assertTrue((root / "src" / "watchers" / "youtube_watchdog.py").exists())
        self.assertTrue((root / "src" / "dj" / "auto_dj.py").exists())
        self.assertTrue((root / "ops" / "systemd" / "adsb-streamnew-youtube-stream.service").exists())

    def test_stream_v2_ncs_music_is_local_asset(self) -> None:
        music = stream_app_root() / "ncs_music"
        self.assertTrue(music.is_dir())
        self.assertFalse(music.is_symlink())
        self.assertTrue((music / "major").exists())
        self.assertTrue((music / "minor").exists())

    def test_stream_cli_uses_v2_root_not_original_root(self) -> None:
        cli = stream_app_root() / "src" / "stream_core" / "cli.py"
        text = cli.read_text(encoding="utf-8")
        self.assertIn("STREAM_BASE_DIR", text)
        self.assertNotIn('BASE_DIR = Path("/home/yuki/projects/stream_v2")', text)

    def test_stream_cli_help_runs_from_v2_root(self) -> None:
        cp = subprocess.run([str(STREAM_APP_BIN), "--help"], text=True, capture_output=True, check=False)
        self.assertEqual(cp.returncode, 0, cp.stderr)
        self.assertIn("usage:", cp.stdout.lower())


if __name__ == "__main__":
    unittest.main()
