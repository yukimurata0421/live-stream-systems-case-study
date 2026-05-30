from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stream_v2.config import RuntimeConfig
from stream_v2.jsonio import append_jsonl, atomic_write_json
from stream_v2.pipeline import ShadowPipeline
from stream_v2.status_summary import build_status_summary, render_text_summary
from stream_v2.timeutil import isoformat_utc
from test_subsystems import NOW, healthy_source


class TestStatusSummary(unittest.TestCase):
    def test_ops_summary_answers_who_what_when_decision_and_action(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            ShadowPipeline(config).run_once(now=NOW)
            summary = build_status_summary(config.state_root)
        self.assertEqual(summary["answer"], "healthy: same URL live, no recovery action selected")
        self.assertEqual(summary["actor"]["name"], "recovery_orchestrator")
        self.assertEqual(summary["actor"]["mode"], "shadow")
        self.assertEqual(summary["target"]["expected_video_id"], "vid-current")
        self.assertEqual(summary["observed_state"]["stream_public_state"], "same_url_live")
        self.assertEqual(summary["decision"]["state"], "all_subsystems_healthy")
        self.assertEqual(summary["selected_action"]["action"], "none")
        self.assertEqual(summary["selected_action"]["result_reason"], "shadow_mode")
        self.assertEqual(summary["execution_plan"]["action"], "none")
        self.assertFalse(summary["execution_plan"]["execute"])

    def test_ops_summary_keeps_replacement_block_reason_visible(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            ShadowPipeline(config).run_once(now=NOW)
            summary = build_status_summary(config.state_root)
        self.assertFalse(summary["replacement_policy"]["allowed"])
        self.assertEqual(summary["replacement_policy"]["reason"], "expected_url_live_or_recoverable")
        blocked_actions = {entry["action"]: entry for entry in summary["blocked_actions"]}
        self.assertIn("create_replacement_broadcast", blocked_actions)
        self.assertIn("url_preservation", blocked_actions["create_replacement_broadcast"]["blocked_by"])

    def test_ops_summary_warns_when_overall_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            status = {
                "ts_utc": isoformat_utc(NOW),
                "overall": {
                    "state": "unknown",
                    "stream_public_state": "unknown",
                    "expected_video_id": "vid-current",
                    "expected_url_state": "unknown",
                    "degraded_subsystems": ["monitoring"],
                    "consistency_window_sec": 999,
                    "max_consistency_window_sec": 120,
                    "action_reason": "consistency window exceeded",
                },
                "monitoring": {"state": "unknown", "recommended_action": "none", "blocked_by": ["stale"]},
            }
            atomic_write_json(root / "subsystems_status.json", status)
            append_jsonl(root / "logs" / "recovery_orchestrator.jsonl", {
                "ts_utc": isoformat_utc(NOW),
                "event_id": "evt-orch-test",
                "actor": {"name": "recovery_orchestrator", "mode": "shadow", "trigger": "test"},
                "target": {"stream_id": "adsb-streamnew", "expected_video_id": "vid-current"},
                "decision": {"state": "subsystem_unknown", "reason": "consistency window exceeded"},
                "selected_action": {"action": "none", "scope": "none", "execute": False, "reason": "blocked"},
                "execution_plan": {"action": "none", "scope": "none", "execute": False, "executable": True, "blocked_by": ["shadow_mode"], "steps": []},
                "result": {"status": "not_executed", "reason": "shadow_mode"},
            })
            summary = build_status_summary(root)
        self.assertIn("destructive action must remain blocked", summary["answer"])
        self.assertIn("overall_unknown_destructive_action_blocked", summary["warnings"])
        self.assertEqual(summary["subsystems"]["monitoring"]["state"], "unknown")

    def test_render_text_summary_contains_actionable_lines(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            ShadowPipeline(config).run_once(now=NOW)
            text = render_text_summary(build_status_summary(config.state_root))
        self.assertIn("answer: healthy: same URL live", text)
        self.assertIn("selected_action: action=none", text)
        self.assertIn("execution_plan: action=none", text)
        self.assertIn("youtube_lifecycle", text)
        self.assertIn("replacement: allowed=False", text)

    def test_ops_summary_cli_outputs_json_and_text(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            ShadowPipeline(config).run_once(now=NOW)
            repo = Path(__file__).resolve().parents[1]
            json_cp = subprocess.run(
                [sys.executable, "-m", "stream_v2", "ops-summary", "--state-root", str(config.state_root)],
                cwd=repo,
                env={"PYTHONPATH": "src"},
                text=True,
                capture_output=True,
                check=False,
            )
            text_cp = subprocess.run(
                [sys.executable, "-m", "stream_v2", "ops-summary", "--state-root", str(config.state_root), "--text"],
                cwd=repo,
                env={"PYTHONPATH": "src"},
                text=True,
                capture_output=True,
                check=False,
            )
        self.assertEqual(json_cp.returncode, 0, json_cp.stderr)
        self.assertEqual(json.loads(json_cp.stdout)["selected_action"]["action"], "none")
        self.assertEqual(text_cp.returncode, 0, text_cp.stderr)
        self.assertIn("overall: state=healthy", text_cp.stdout)


if __name__ == "__main__":
    unittest.main()
