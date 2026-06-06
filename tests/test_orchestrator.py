from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stream_v2.action_lock import FileActionLock, LockState
from stream_v2.aggregator import SubsystemAggregator
from stream_v2.config import RuntimeConfig
from stream_v2.model import ActionCandidate
from stream_v2.orchestrator import ActionGate, RecoveryOrchestrator
from stream_v2.source_reader import SourceReader
from stream_v2.timeutil import isoformat_utc
from test_subsystems import NOW, append_jsonl, healthy_source, write_json


class TestOrchestrator(unittest.TestCase):
    def snapshot(self, root: Path, config: RuntimeConfig | None = None):
        config = config or RuntimeConfig(source_state_root=root, state_root=root / "v2")
        return SubsystemAggregator(config).aggregate(SourceReader(root).read(), now=NOW)

    def test_orchestrator_prefers_scoped_action_over_stream_all_restart(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            stats = json.loads((root / "youtube_watchdog_stats.json").read_text())
            stats["ffmpeg_pid"] = 0
            stats["ingest_connected"] = False
            stats["stream_active"] = False
            write_json(root / "youtube_watchdog_stats.json", stats)
            runtime = json.loads((root / "stream_runtime_state_abc.json").read_text())
            runtime["ffmpeg_pid"] = ""
            write_json(root / "stream_runtime_state_abc.json", runtime)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            snap = self.snapshot(root, config)
            decision = RecoveryOrchestrator(config).evaluate(snap, now=NOW, lock_state=LockState(False, False, "no_lock"))
        self.assertEqual(decision.event["selected_action"]["action"], "restart_ffmpeg")
        self.assertEqual(decision.event["execution_plan"]["action"], "restart_ffmpeg")
        self.assertFalse(decision.event["execution_plan"]["executable"])
        self.assertIn("native_ffmpeg_control_api_not_yet_available", decision.event["execution_plan"]["blocked_by"])
        actions = [item["action"] for item in decision.event["action_candidates"]]
        self.assertIn("restart_stream", actions)
        self.assertIn("create_replacement_broadcast", actions)

    def test_orchestrator_does_not_restart_dj_when_local_delivery_failed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            append_jsonl(root / "logs" / "watchdog_state_timeline.jsonl", {
                "ts_utc": "2026-05-06T11:56:30Z",
                "event_id": "evt-stale-now-playing",
                "stream_service_substate": "running",
                "dj_service_substate": "running",
                "ffmpeg_count": 1,
                "runtime_snapshot": {
                    "age_sec": 210,
                    "run_id": "run-1",
                    "status": "running",
                    "ffmpeg_pid": "123",
                    "updated_at_utc": "2026-05-06T11:56:30Z",
                },
                "now_playing_state": {
                    "updated_at_utc": "2026-05-06T11:56:30Z",
                    "status": "playing",
                    "title": "NCS Track",
                },
            })
            runtime = json.loads((root / "stream_runtime_state_abc.json").read_text())
            runtime["updated_at_utc"] = "2026-05-06T11:56:30Z"
            write_json(root / "stream_runtime_state_abc.json", runtime)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            snap = self.snapshot(root, config)
            decision = RecoveryOrchestrator(config).evaluate(snap, now=NOW, lock_state=LockState(False, False, "no_lock"))
        self.assertEqual(snap.local_delivery.state, "failed")
        self.assertEqual(snap.music.state, "degraded")
        self.assertEqual(decision.event["selected_action"]["action"], "restart_ffmpeg")
        actions = [item["action"] for item in decision.event["action_candidates"]]
        self.assertNotIn("restart_dj", actions)

    def test_orchestrator_selects_restart_stream_for_fast_recovery_stream_restart(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            append_jsonl(root / "logs" / "fast_recovery_events.jsonl", {
                "ts_utc": "2026-05-06T11:59:45Z",
                "kind": "restart",
                "trigger": "network_down",
                "message": "network down: gw_ok=True public_ok_count=0 dns_ok=False tcp_probe_ok=False",
                "restart_context": {
                    "component": "stream",
                    "target_unit": "adsb-streamnew-youtube-stream.service",
                    "trigger": "network_down",
                },
            })
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2", supervisor_mode="k8s")
            snap = self.snapshot(root, config)
            decision = RecoveryOrchestrator(config).evaluate(snap, now=NOW, lock_state=LockState(False, False, "no_lock"))

        self.assertEqual(snap.local_delivery.recommended_action, "restart_stream")
        self.assertEqual(decision.event["selected_action"]["action"], "restart_stream")
        self.assertEqual(decision.event["execution_plan"]["action"], "restart_stream")
        self.assertTrue(decision.event["execution_plan"]["executable"])
        self.assertFalse(decision.event["execution_plan"]["execute"])
        self.assertIn("shadow_mode", decision.event["execution_plan"]["blocked_by"])
        self.assertEqual(
            decision.event["execution_plan"]["steps"][0]["command"],
            ["kubectl", "-n", "stream-v3", "rollout", "restart", "deployment/stream-v3-runtime"],
        )

    def test_youtube_lifecycle_expected_url_live_blocks_replacement_broadcast_in_gate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            snap = self.snapshot(root)
            candidate = ActionCandidate("create_replacement_broadcast", "youtube_lifecycle", 90, "very_high", False, False, ["expected_url_live_or_recoverable"])
            gate = ActionGate().evaluate(snap, candidate, lock_state=LockState(False, False, "no_lock"))
        self.assertFalse(gate.passed)
        self.assertIn("url_preservation", gate.blocked_by)
        self.assertIn("expected_url_live_or_recoverable", gate.blocked_by)

    def test_monitoring_quota_guard_degraded_blocks_api_destructive_action_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            stats = json.loads((root / "youtube_watchdog_stats.json").read_text())
            stats["public_ok"] = False
            stats["availability_ok"] = False
            stats["api_live_state"] = "ended"
            stats["failure_kind"] = "remote_ended_confirmed"
            stats["api_cost_burn_rate_active"] = True
            write_json(root / "youtube_watchdog_stats.json", stats)
            snap = self.snapshot(root)
            youtube_candidate = ActionCandidate("force_current_broadcast_live", "youtube_lifecycle", 50, "high", True, True)
            local_candidate = ActionCandidate("restart_ffmpeg", "local_delivery", 30, "medium", True, True)
            gate = ActionGate()
            y_gate = gate.evaluate(snap, youtube_candidate, lock_state=LockState(False, False, "no_lock"))
            local_gate = gate.evaluate(snap, local_candidate, lock_state=LockState(False, False, "no_lock"))
        self.assertFalse(y_gate.passed)
        self.assertIn("quota_guard", y_gate.blocked_by)
        self.assertTrue(local_gate.passed)

    def test_global_action_lock_blocks_second_destructive_action(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            snap = self.snapshot(root)
            candidate = ActionCandidate("restart_stream", "stream_all", 40, "high", True, True)
            gate = ActionGate().evaluate(snap, candidate, lock_state=LockState(True, False, "destructive_action_in_progress", "evt-lock", "restart_stream", "stream_all"))
        self.assertFalse(gate.passed)
        self.assertIn("global_action_lock", gate.blocked_by)
        self.assertEqual(gate.gates["global_action_lock"]["lock_owner_event_id"], "evt-lock")

    def test_file_based_global_action_lock_expires_stale_owner(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            lock_path = Path(td) / "recovery_action.lock.json"
            write_json(lock_path, {
                "lock_owner_event_id": "evt-old",
                "action": "restart_stream",
                "scope": "stream_all",
                "acquired_at_utc": isoformat_utc(NOW - timedelta(seconds=600)),
                "ttl_sec": 30,
                "owner_pid": 1,
            })
            state = FileActionLock(lock_path).check()
        self.assertFalse(state.active)
        self.assertTrue(state.stale)
        self.assertEqual(state.reason, "stale_lock_expired")

    def test_recovery_orchestrator_audit_log_has_accountability_order(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            healthy_source(root)
            config = RuntimeConfig(source_state_root=root, state_root=root / "v2")
            event = RecoveryOrchestrator(config).evaluate(self.snapshot(root, config), now=NOW, lock_state=LockState(False, False, "no_lock")).event
        self.assertEqual(list(event.keys())[:10], [
            "ts_utc",
            "event_id",
            "schema_version",
            "actor",
            "target",
            "observed_state",
            "evidence",
            "decision",
            "action_candidates",
            "gates",
        ])
        self.assertIn("selected_action", event)
        self.assertIn("execution_plan", event)
        self.assertEqual(event["result"]["reason"], "shadow_mode")


if __name__ == "__main__":
    unittest.main()
