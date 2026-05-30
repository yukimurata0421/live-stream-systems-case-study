from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from stream_v2.config import RuntimeConfig
from stream_v2.health_summary import build_health_summary, render_text_health_summary
from stream_v2.pipeline import ShadowPipeline
from test_subsystems import NOW, healthy_source, write_json


class TestHealthSummary(unittest.TestCase):
    def test_native_health_summary_joins_source_and_v2_without_legacy_script(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            ShadowPipeline(config).run_once(now=NOW)
            summary = build_health_summary(source_state_root=root, state_root=config.state_root, now=NOW)

        self.assertEqual(summary["answer"], "healthy: source current health OK and v2 same-url shadow selected no action")
        self.assertFalse(summary["source_current"]["current_fail"])
        self.assertEqual(summary["source_current"]["youtube"]["status"], "ok")
        self.assertEqual(summary["stream_v2"]["observed_state"]["overall"], "healthy")
        self.assertEqual(summary["stream_v2"]["selected_action"]["action"], "none")
        self.assertNotIn("legacy", json.dumps(summary["checks"], ensure_ascii=False).lower())

    def test_v2_unknown_does_not_override_source_current_ok(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2", max_consistency_window_sec=1)
            ShadowPipeline(config).run_once(now=NOW)
            summary = build_health_summary(source_state_root=root, state_root=config.state_root, now=NOW)

        self.assertFalse(summary["source_current"]["current_fail"])
        self.assertEqual(summary["stream_v2"]["observed_state"]["overall"], "unknown")
        self.assertIn("v2_unknown", summary["answer"])
        self.assertEqual(summary["stream_v2"]["selected_action"]["action"], "none")

    def test_source_current_failure_is_visible_in_native_summary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            stats_path = root / "youtube_watchdog_stats.json"
            stats = json.loads(stats_path.read_text(encoding="utf-8"))
            stats["status"] = "warn"
            stats["judgment"] = "ng"
            write_json(stats_path, stats)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            ShadowPipeline(config).run_once(now=NOW)
            summary = build_health_summary(source_state_root=root, state_root=config.state_root, now=NOW)

        self.assertTrue(summary["source_current"]["current_fail"])
        self.assertEqual(summary["checks"]["source_current_fail"], True)
        self.assertIn("source_current_fail", summary["answer"])

    def test_health_summary_cli_outputs_json_and_text(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            ShadowPipeline(config).run_once(now=NOW)
            repo = Path(__file__).resolve().parents[1]
            json_cp = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "stream_v2",
                    "health-summary",
                    "--source-state-root",
                    str(root),
                    "--state-root",
                    str(config.state_root),
                    "--max-youtube-stats-stale-sec",
                    "999999999",
                    "--max-v2-status-stale-sec",
                    "999999999",
                ],
                cwd=repo,
                env={"PYTHONPATH": "src"},
                text=True,
                capture_output=True,
                check=False,
            )
            text_cp = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "stream_v2",
                    "health-summary",
                    "--source-state-root",
                    str(root),
                    "--state-root",
                    str(config.state_root),
                    "--max-youtube-stats-stale-sec",
                    "999999999",
                    "--max-v2-status-stale-sec",
                    "999999999",
                    "--text",
                ],
                cwd=repo,
                env={"PYTHONPATH": "src"},
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(json_cp.returncode, 0, json_cp.stderr)
        self.assertEqual(json.loads(json_cp.stdout)["stream_v2"]["selected_action"]["action"], "none")
        self.assertEqual(text_cp.returncode, 0, text_cp.stderr)
        self.assertIn("source: current_fail=False", text_cp.stdout)
        self.assertIn("v2: available=True", text_cp.stdout)

    def test_render_text_health_summary_reports_replacement_policy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            ShadowPipeline(config).run_once(now=NOW)
            text = render_text_health_summary(build_health_summary(source_state_root=root, state_root=config.state_root, now=NOW))

        self.assertIn("replacement: allowed=False", text)
        self.assertIn("selected_action=none", text)


if __name__ == "__main__":
    unittest.main()
