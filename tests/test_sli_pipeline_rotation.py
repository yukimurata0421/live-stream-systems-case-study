from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stream_v2.config import RuntimeConfig
from stream_v2.jsonio import append_jsonl, read_json
from stream_v2.pipeline import ShadowPipeline
from stream_v2.rotation import manifest
from stream_v2.sli import ObjectiveSliCalculator
from stream_v2.timeutil import isoformat_utc
from test_subsystems import NOW, healthy_source


class TestSliPipelineRotation(unittest.TestCase):
    def test_objective_sli_incomplete_window_uses_null_ratio_and_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = RuntimeConfig(state_root=root / "v2")
            append_jsonl(config.subsystems_status_log_path, {
                "ts_utc": isoformat_utc(NOW - timedelta(minutes=5)),
                "overall": {"state": "healthy", "stream_public_state": "same_url_live"},
            })
            sli = ObjectiveSliCalculator(config).calculate(now=NOW, expected_video_id="vid-current")
        last_24h = sli["windows"]["last_24h"]
        self.assertFalse(last_24h["window_complete"])
        self.assertIsNone(last_24h["same_url_live_ratio"])
        self.assertGreater(last_24h["data_coverage_sec"], 0)

    def test_objective_sli_counts_same_url_live_unknown_replacement_and_budget_override(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = RuntimeConfig(state_root=root / "v2")
            start = NOW - timedelta(hours=23)
            append_jsonl(config.subsystems_status_log_path, {
                "ts_utc": isoformat_utc(start),
                "overall": {"state": "healthy", "stream_public_state": "same_url_live"},
            })
            append_jsonl(config.subsystems_status_log_path, {
                "ts_utc": isoformat_utc(start + timedelta(hours=12)),
                "overall": {"state": "unknown", "stream_public_state": "unknown"},
            })
            append_jsonl(config.orchestrator_log_path, {
                "ts_utc": isoformat_utc(start + timedelta(hours=1)),
                "selected_action": {"action": "create_replacement_broadcast"},
                "gates": {"budget": {"reason": "budget_override"}},
            })
            sli = ObjectiveSliCalculator(config).calculate(now=NOW, expected_video_id="vid-current")
        last_24h = sli["windows"]["last_24h"]
        self.assertTrue(last_24h["window_complete"])
        self.assertGreater(last_24h["same_url_live_ratio"], 0)
        self.assertGreater(last_24h["unknown_ratio"], 0)
        self.assertEqual(last_24h["replacement_count"], 1)
        self.assertEqual(last_24h["budget_override_count"], 1)
        self.assertEqual(sli["shadow_timer_policy"]["expected_interval_sec"], 60)
        self.assertEqual(last_24h["window_complete_threshold_sec"], 77760)
        self.assertIn("subsystems_shadow", last_24h)

    def test_shadow_sli_counts_actions_and_production_diff(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source_root = root / "source"
            config = RuntimeConfig(source_state_root=source_root, state_root=root / "v2")
            start = NOW - timedelta(hours=23)
            append_jsonl(config.subsystems_status_log_path, {
                "ts_utc": isoformat_utc(start),
                "overall": {"state": "healthy", "stream_public_state": "same_url_live"},
            })
            append_jsonl(config.orchestrator_log_path, {
                "ts_utc": isoformat_utc(start + timedelta(hours=1)),
                "selected_action": {"action": "restart_ffmpeg"},
            })
            append_jsonl(config.orchestrator_log_path, {
                "ts_utc": isoformat_utc(start + timedelta(hours=2)),
                "selected_action": {"action": "restart_browser"},
            })
            append_jsonl(config.orchestrator_log_path, {
                "ts_utc": isoformat_utc(start + timedelta(hours=3)),
                "selected_action": {"action": "none"},
            })
            append_jsonl(source_root / "logs" / "fast_recovery_events.jsonl", {
                "ts_utc": isoformat_utc(start + timedelta(hours=1, seconds=30)),
                "kind": "restart",
                "trigger": "tcp_stall",
            })
            append_jsonl(source_root / "logs" / "stream_engine_events.jsonl", {
                "ts_utc": isoformat_utc(start + timedelta(hours=3, seconds=30)),
                "event_type": "ffmpeg_restart_scheduled",
            })

            sli = ObjectiveSliCalculator(config).calculate(now=NOW, expected_video_id="vid-current")

        shadow = sli["windows"]["last_24h"]["subsystems_shadow"]
        self.assertEqual(shadow["selected_action_counts"]["restart_ffmpeg"], 1)
        self.assertEqual(shadow["selected_action_counts"]["restart_browser"], 1)
        self.assertEqual(shadow["production_action_counts"]["restart_stream"], 1)
        self.assertEqual(shadow["production_action_counts"]["restart_ffmpeg"], 1)
        self.assertEqual(shadow["shadow_vs_production_exact_agreement_count"], 0)
        self.assertEqual(shadow["shadow_vs_production_scope_compatible_agreement_count"], 1)
        self.assertEqual(shadow["shadow_vs_production_disagreement_by_reason"]["action_mismatch"], 1)
        self.assertEqual(shadow["shadow_vs_production_disagreement_by_reason"]["production_without_shadow"], 1)
        self.assertEqual(shadow["shadow_vs_production_disagreement_by_reason"]["false_positive_shadow"], 1)

    def test_objective_sli_does_not_count_passed_lock_gate_as_block(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = RuntimeConfig(state_root=root / "v2")
            append_jsonl(config.subsystems_status_log_path, {
                "ts_utc": isoformat_utc(NOW - timedelta(minutes=5)),
                "overall": {"state": "healthy", "stream_public_state": "same_url_live"},
            })
            append_jsonl(config.orchestrator_log_path, {
                "ts_utc": isoformat_utc(NOW - timedelta(minutes=4)),
                "selected_action": {"action": "none"},
                "gates": {"global_action_lock": {"passed": True, "reason": "no_destructive_action_in_progress"}},
                "all_candidate_gates": {
                    "create_replacement_broadcast": {
                        "gates": {"global_action_lock": {"passed": True, "reason": "no_destructive_action_in_progress"}}
                    }
                },
            })
            sli = ObjectiveSliCalculator(config).calculate(now=NOW, expected_video_id="vid-current")
        self.assertEqual(sli["windows"]["last_24h"]["global_action_lock_block_count"], 0)

    def test_new_jsonl_logs_are_in_rotation_manifest(self) -> None:
        paths = {entry["path"] for entry in manifest()["logs"]}
        self.assertIn("logs/subsystems_status.jsonl", paths)
        self.assertIn("logs/recovery_orchestrator.jsonl", paths)
        self.assertIn("logs/recovery_action_plan.jsonl", paths)
        self.assertIn("logs/objective_sli.jsonl", paths)
        self.assertIn("logs/memory_status.jsonl", paths)
        self.assertIn("logs/resource_memory.jsonl", paths)
        self.assertIn("logs/stream_components.jsonl", paths)

    def test_shadow_pipeline_writes_v2_outputs_without_touching_source_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            source_stat_before = (root / "youtube_watchdog_stats.json").read_text(encoding="utf-8")
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            result = ShadowPipeline(config).run_once(now=NOW)
            source_stat_after = (root / "youtube_watchdog_stats.json").read_text(encoding="utf-8")
            self.assertEqual(source_stat_before, source_stat_after)
            self.assertEqual(result.snapshot["overall"]["stream_public_state"], "same_url_live")
            self.assertEqual(result.orchestrator_event["result"]["reason"], "shadow_mode")
            self.assertTrue((root / "v2" / "subsystems_status.json").exists())
            self.assertTrue((root / "v2" / "logs" / "recovery_orchestrator.jsonl").exists())
            self.assertTrue((root / "v2" / "recovery_action_plan.json").exists())
            self.assertTrue((root / "v2" / "logs" / "recovery_action_plan.jsonl").exists())
            self.assertTrue((root / "v2" / "objective_sli.json").exists())
            self.assertTrue((root / "v2" / "stream_components.json").exists())
            self.assertIsNotNone(read_json(root / "v2" / "subsystems_status.json"))
            self.assertEqual(result.recovery_action_plan["action"], "none")
            stream_components = read_json(root / "v2" / "stream_components.json")
            self.assertIsNotNone(stream_components)
            self.assertEqual(stream_components["missing"], [])


if __name__ == "__main__":
    unittest.main()
