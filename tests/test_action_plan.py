from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stream_v2.config import RuntimeConfig
from stream_v2.model import ActionCandidate
from stream_v2.orchestrator import ActionPlanBuilder
from stream_v2.recovery_orchestrator.executor import DJ_SERVICE, STREAM_SERVICE
from stream_v2.recovery_orchestrator.types import GateResult
from test_subsystems import NOW, healthy_source
from stream_v2.aggregator import SubsystemAggregator
from stream_v2.source_reader import SourceReader


class TestActionPlan(unittest.TestCase):
    def snapshot(self, root: Path):
        config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
        return SubsystemAggregator(config).aggregate(SourceReader(root).read(), now=NOW)

    def test_restart_stream_plan_maps_to_stream_systemd_unit_but_does_not_execute_in_shadow(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            plan = ActionPlanBuilder().build(
                self.snapshot(root),
                ActionCandidate("restart_stream", "stream_all", 40, "high", True, True),
                GateResult(gates={}, passed=True, blocked_by=[]),
                mode="shadow",
            ).to_dict()
        self.assertTrue(plan["executable"])
        self.assertFalse(plan["execute"])
        self.assertIn("shadow_mode", plan["blocked_by"])
        self.assertEqual(plan["steps"][0]["service_unit"], STREAM_SERVICE)
        self.assertEqual(plan["steps"][0]["command"], ["systemctl", "restart", STREAM_SERVICE])

    def test_restart_dj_plan_stays_in_music_scope(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            plan = ActionPlanBuilder().build(
                self.snapshot(root),
                ActionCandidate("restart_dj", "music", 20, "low", True, True),
                GateResult(gates={}, passed=True, blocked_by=[]),
                mode="shadow",
            ).to_dict()
        self.assertEqual(plan["steps"][0]["service_unit"], DJ_SERVICE)
        self.assertEqual(plan["steps"][0]["url_risk"], "none")
        self.assertEqual(plan["steps"][0]["command"], ["systemctl", "restart", DJ_SERVICE])

    def test_restart_dj_plan_uses_scoped_k8s_container_restart(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            plan = ActionPlanBuilder().build(
                self.snapshot(root),
                ActionCandidate("restart_dj", "music", 20, "low", True, True),
                GateResult(gates={}, passed=True, blocked_by=[]),
                mode="shadow",
                supervisor_mode="k8s",
            ).to_dict()

        self.assertTrue(plan["executable"])
        self.assertEqual(plan["steps"][0]["kind"], "k8s_scoped_container_restart")
        self.assertEqual(plan["steps"][0]["command"], ["python3", "ops/scripts/stream_v3_scoped_recovery.py", "restart-dj", "--reason", "recovery_orchestrator:restart_dj"])
        self.assertEqual(plan["steps"][0]["writes"], ["k8s:container:auto-dj"])

    def test_restart_stream_plan_can_render_k8s_workload_target(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            plan = ActionPlanBuilder().build(
                self.snapshot(root),
                ActionCandidate("restart_stream", "stream_all", 40, "high", True, True),
                GateResult(gates={}, passed=True, blocked_by=[]),
                mode="shadow",
                supervisor_mode="k8s",
            ).to_dict()

        self.assertTrue(plan["executable"])
        self.assertFalse(plan["execute"])
        self.assertEqual(plan["steps"][0]["kind"], "k8s_rollout_restart")
        self.assertEqual(plan["steps"][0]["command"], ["kubectl", "-n", "stream-v3", "rollout", "restart", "deployment/stream-v3-runtime"])
        self.assertIn("k8s:deployment/stream-v3-runtime", plan["steps"][0]["writes"])

    def test_restart_ffmpeg_plan_does_not_alias_to_full_stream_restart(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            plan = ActionPlanBuilder().build(
                self.snapshot(root),
                ActionCandidate("restart_ffmpeg", "local_delivery", 30, "medium", True, True),
                GateResult(gates={}, passed=True, blocked_by=[]),
                mode="shadow",
            ).to_dict()
        self.assertFalse(plan["executable"])
        self.assertIn("native_ffmpeg_control_api_not_yet_available", plan["blocked_by"])
        self.assertEqual(plan["steps"][0]["kind"], "engine_control")
        self.assertEqual(plan["steps"][0]["command"], [])

    def test_restart_ffmpeg_plan_uses_scoped_k8s_process_restart(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            plan = ActionPlanBuilder().build(
                self.snapshot(root),
                ActionCandidate("restart_ffmpeg", "local_delivery", 30, "medium", True, True),
                GateResult(gates={}, passed=True, blocked_by=[]),
                mode="shadow",
                supervisor_mode="k8s",
            ).to_dict()

        self.assertTrue(plan["executable"])
        self.assertEqual(plan["steps"][0]["kind"], "k8s_scoped_process_restart")
        self.assertEqual(plan["steps"][0]["command"], ["python3", "ops/scripts/stream_v3_scoped_recovery.py", "restart-ffmpeg", "--reason", "recovery_orchestrator:restart_ffmpeg"])
        self.assertEqual(plan["steps"][0]["url_risk"], "same_url_preserving")

    def test_youtube_mutation_plan_is_classified_and_blocked_in_v2(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            plan = ActionPlanBuilder().build(
                self.snapshot(root),
                ActionCandidate("force_current_broadcast_live", "youtube_lifecycle", 50, "high", True, True),
                GateResult(gates={}, passed=True, blocked_by=[]),
                mode="shadow",
            ).to_dict()
        self.assertFalse(plan["executable"])
        self.assertFalse(plan["execute"])
        self.assertEqual(plan["steps"][0]["kind"], "youtube_api_mutation")
        self.assertEqual(plan["steps"][0]["url_risk"], "can_change_youtube_lifecycle")
        self.assertIn("youtube_mutation_not_enabled_in_stream_v2", plan["blocked_by"])

    def test_replacement_broadcast_is_hard_blocked_by_same_url_contract(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            plan = ActionPlanBuilder().build(
                self.snapshot(root),
                ActionCandidate("create_replacement_broadcast", "youtube_lifecycle", 90, "very_high", False, True),
                GateResult(gates={}, passed=True, blocked_by=[]),
                mode="shadow",
            ).to_dict()

        self.assertFalse(plan["executable"])
        self.assertIn("same_url_required_absolute", plan["blocked_by"])
        self.assertIn("replacement_broadcast_disabled", plan["blocked_by"])
        self.assertEqual(plan["steps"][0]["url_risk"], "can_change_youtube_url")


if __name__ == "__main__":
    unittest.main()
