from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

from stream_v2.config import RuntimeConfig
from stream_v2.jsonio import iter_jsonl
from stream_v2.pipeline import ShadowPipeline

from test_subsystems import NOW, healthy_source


class SubsystemsShadowCompatTests(unittest.TestCase):
    def test_shadow_pipeline_writes_program_map_status_and_orchestrator_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source_root = root / "source"
            state_root = root / "state"
            healthy_source(source_root)

            result = ShadowPipeline(RuntimeConfig(source_state_root=source_root, state_root=state_root)).run_once(now=NOW)

            self.assertTrue((state_root / "subsystems_status.json").exists())
            self.assertTrue((state_root / "logs" / "subsystems_status.jsonl").exists())
            self.assertTrue((state_root / "logs" / "recovery_orchestrator.jsonl").exists())
            self.assertTrue((state_root / "recovery_action_plan.json").exists())

        self.assertEqual(result.snapshot["overall"]["state"], "healthy")
        self.assertEqual(result.snapshot["overall"]["stream_public_state"], "same_url_live")
        for name in ("rendering", "music", "local_delivery", "youtube_lifecycle", "monitoring"):
            self.assertEqual(result.snapshot[name]["state"], "healthy", name)
        self.assertEqual(result.orchestrator_event["selected_action"]["action"], "none")
        self.assertFalse(result.orchestrator_event["selected_action"]["execute"])

    def test_timer_entrypoints_do_not_duplicate_shadow_writes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source_root = root / "source"
            state_root = root / "state"
            healthy_source(source_root)
            config = RuntimeConfig(source_state_root=source_root, state_root=state_root)
            pipeline = ShadowPipeline(config)

            status_result = pipeline.run_subsystems_status_once(now=NOW)

            self.assertEqual(status_result.snapshot["overall"]["state"], "healthy")
            self.assertTrue((state_root / "subsystems_status.json").exists())
            self.assertFalse((state_root / "logs" / "recovery_orchestrator.jsonl").exists())
            self.assertFalse((state_root / "recovery_action_plan.json").exists())

            orchestrator_result = pipeline.run_recovery_orchestrator_once(now=NOW)

            self.assertEqual(orchestrator_result.orchestrator_event["selected_action"]["action"], "none")
            self.assertEqual(len(list(iter_jsonl(state_root / "logs" / "subsystems_status.jsonl"))), 1)
            self.assertEqual(len(list(iter_jsonl(state_root / "logs" / "recovery_orchestrator.jsonl"))), 1)
            self.assertTrue((state_root / "recovery_action_plan.json").exists())


if __name__ == "__main__":
    unittest.main()
