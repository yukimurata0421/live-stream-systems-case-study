from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.stateful_runtime import (
    ExplorationConfig,
    StatefulActionRuntime,
    StatefulAttributionGap,
    StatefulHarnessFailure,
    WriteAheadActionLog,
    bounded_explore,
    identity_attribution,
    verified_cleanup,
)
from tests.stateful_delivery_authority_harness import DeliveryStateMachine


def state(*, snapshot: str = "snap-1", transitions: tuple[str, ...] = (), intents: tuple[str, ...] = (), episodes: tuple[str, ...] = ()) -> dict:
    return {
        "canonical": {"state": "good"},
        "current_snapshot_id": snapshot,
        "domain_current_snapshot_id": snapshot,
        "transition_ids": list(transitions),
        "intent_ids": list(intents),
        "episode_ids": list(episodes),
        "transition_records": {},
        "intent_records": {},
        "episode_state": [],
        "last_bad_at": None,
        "transition_count": len(transitions),
        "intent_count": len(intents),
        "repository_generation": 0,
    }


class IdentityAttributionTests(unittest.TestCase):
    def test_created_transition_and_intent_use_set_difference_and_references(self) -> None:
        pre = state()
        post = state(snapshot="snap-2", transitions=("trn-b",), intents=("ntf-b",), episodes=("inc-b",))
        post["transition_records"] = {
            "trn-b": {
                "transition_id": "trn-b",
                "episode_id": "inc-b",
                "phase": "detected",
                "current_snapshot_id": "snap-2",
            }
        }
        post["intent_records"] = {
            "ntf-b": {
                "intent_id": "ntf-b",
                "transition_id": "trn-b",
                "episode_id": "inc-b",
                "phase": "detected",
            }
        }
        result = identity_attribution(pre, post)
        self.assertTrue(result["attribution_complete"])
        self.assertEqual(result["created_transition_ids"], ["trn-b"])
        self.assertEqual(result["created_intent_ids"], ["ntf-b"])
        self.assertEqual(result["created_episode_ids"], ["inc-b"])

    def test_transition_snapshot_mismatch_is_attribution_gap_data(self) -> None:
        pre = state()
        post = state(snapshot="snap-2", transitions=("trn-b",), episodes=("inc-b",))
        post["transition_records"] = {
            "trn-b": {
                "transition_id": "trn-b",
                "episode_id": "inc-b",
                "phase": "detected",
                "current_snapshot_id": "snap-other",
            }
        }
        result = identity_attribution(pre, post)
        self.assertFalse(result["attribution_complete"])
        self.assertEqual(result["attribution_errors"], ["transition_snapshot_not_post_current:trn-b"])


class ActionRuntimeTests(unittest.TestCase):
    def test_failed_action_is_fsynced_with_best_effort_post_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            action_log_path = Path(directory) / "actions.jsonl"
            current = state()
            runtime = StatefulActionRuntime(
                run_id="unit-1",
                seed=1,
                example_index=0,
                capture_state=lambda: current,
                action_log=WriteAheadActionLog(action_log_path),
            )
            with self.assertRaisesRegex(StatefulHarnessFailure, "injected"):
                runtime.run(
                    action_type="inject",
                    logical_now="2026-08-21T00:00:00Z",
                    action_input={"source": "unit"},
                    mutation=lambda: (_ for _ in ()).throw(StatefulHarnessFailure("injected")),
                    trace_provider=lambda: None,
                )
            events = [json.loads(line) for line in action_log_path.read_text().splitlines()]
            self.assertEqual([item["status"] for item in events], ["ACTION_STARTED", "ACTION_FAILED"])
            self.assertEqual(events[-1]["created_transition_ids"], [])
            self.assertEqual(events[-1]["post_state_collection_errors"], [])
            self.assertEqual(events[-1]["failure_stage"], "mutation")
            self.assertTrue(events[-1]["mutation_started"])
            self.assertFalse(events[-1]["mutation_completed"])
            self.assertTrue(events[-1]["post_state_available"])
            self.assertTrue(events[-1]["attribution_available"])

    def test_post_state_capture_failure_has_failed_terminal_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            action_log_path = Path(directory) / "actions.jsonl"
            capture_calls = 0

            def capture() -> dict:
                nonlocal capture_calls
                capture_calls += 1
                if capture_calls == 2:
                    raise StatefulHarnessFailure("injected post-state capture failure")
                return state()

            runtime = StatefulActionRuntime(
                run_id="unit-post-state",
                seed=3,
                example_index=0,
                capture_state=capture,
                action_log=WriteAheadActionLog(action_log_path),
            )
            with self.assertRaisesRegex(StatefulHarnessFailure, "post-state capture"):
                runtime.run(
                    action_type="inject",
                    logical_now="2026-08-21T00:00:00Z",
                    action_input={},
                    mutation=lambda: True,
                    trace_provider=lambda: None,
                )
            events = [json.loads(line) for line in action_log_path.read_text().splitlines()]
            self.assertEqual([item["status"] for item in events], ["ACTION_STARTED", "ACTION_FAILED"])
            self.assertEqual(events[-1]["failure_stage"], "post_state_capture")

    def test_attribution_failure_has_failed_terminal_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            action_log_path = Path(directory) / "actions.jsonl"
            runtime = StatefulActionRuntime(
                run_id="unit-attribution",
                seed=4,
                example_index=0,
                capture_state=state,
                action_log=WriteAheadActionLog(action_log_path),
            )

            def fail_attribution(_post: dict) -> dict:
                raise StatefulAttributionGap("injected attribution failure")

            runtime.attribution = fail_attribution  # type: ignore[method-assign]
            with self.assertRaisesRegex(StatefulAttributionGap, "injected attribution"):
                runtime.run(
                    action_type="inject",
                    logical_now="2026-08-21T00:00:00Z",
                    action_input={},
                    mutation=lambda: True,
                    trace_provider=lambda: None,
                )
            events = [json.loads(line) for line in action_log_path.read_text().splitlines()]
            self.assertEqual([item["status"] for item in events], ["ACTION_STARTED", "ACTION_FAILED"])
            self.assertEqual(events[-1]["failure_stage"], "attribution")

    def test_completion_build_failure_has_failed_terminal_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            action_log_path = Path(directory) / "actions.jsonl"
            runtime = StatefulActionRuntime(
                run_id="unit-completion-build",
                seed=5,
                example_index=0,
                capture_state=state,
                action_log=WriteAheadActionLog(action_log_path),
            )

            def fail_trace() -> None:
                raise StatefulHarnessFailure("injected completion build failure")

            with self.assertRaisesRegex(StatefulHarnessFailure, "completion build"):
                runtime.run(
                    action_type="inject",
                    logical_now="2026-08-21T00:00:00Z",
                    action_input={},
                    mutation=lambda: True,
                    trace_provider=fail_trace,
                )
            events = [json.loads(line) for line in action_log_path.read_text().splitlines()]
            self.assertEqual([item["status"] for item in events], ["ACTION_STARTED", "ACTION_FAILED"])
            self.assertEqual(events[-1]["failure_stage"], "completion_build")

    def test_completion_append_failure_falls_back_to_failed_terminal_only(self) -> None:
        class PrePersistAppendFailure(StatefulHarnessFailure):
            event_persisted = False

        class CompletionFailingLog:
            def __init__(self) -> None:
                self.events: list[dict] = []

            def append(self, event: dict) -> None:
                if event["status"] == "ACTION_COMPLETED":
                    raise PrePersistAppendFailure("injected completion append failure")
                self.events.append(event)

        action_log = CompletionFailingLog()
        runtime = StatefulActionRuntime(
            run_id="unit-completion-persist",
            seed=6,
            example_index=0,
            capture_state=state,
            action_log=action_log,  # type: ignore[arg-type]
        )
        with self.assertRaisesRegex(StatefulHarnessFailure, "completion append"):
            runtime.run(
                action_type="inject",
                logical_now="2026-08-21T00:00:00Z",
                action_input={},
                mutation=lambda: True,
                trace_provider=lambda: None,
            )
        self.assertEqual([item["status"] for item in action_log.events], ["ACTION_STARTED", "ACTION_FAILED"])
        self.assertEqual(action_log.events[-1]["failure_stage"], "completion_persist")
        self.assertEqual(runtime.summary()["double_terminal_actions"], 0)

    def test_failed_terminal_persistence_failure_fails_closed_at_run_level(self) -> None:
        class PrePersistAppendFailure(StatefulHarnessFailure):
            event_persisted = False

        class TerminalFailingLog:
            def __init__(self) -> None:
                self.events: list[dict] = []

            def append(self, event: dict) -> None:
                if event["status"] in {"ACTION_COMPLETED", "ACTION_FAILED"}:
                    raise PrePersistAppendFailure(f"injected {event['status']} append failure")
                self.events.append(event)

        action_log = TerminalFailingLog()
        runtime = StatefulActionRuntime(
            run_id="unit-terminal-persist",
            seed=7,
            example_index=0,
            capture_state=state,
            action_log=action_log,  # type: ignore[arg-type]
        )
        with self.assertRaisesRegex(StatefulHarnessFailure, "terminal evidence persistence failure"):
            runtime.run(
                action_type="inject",
                logical_now="2026-08-21T00:00:00Z",
                action_input={},
                mutation=lambda: True,
                trace_provider=lambda: None,
            )
        summary = runtime.summary()
        self.assertEqual(summary["orphan_started_actions"], 1)
        self.assertEqual(summary["terminal_evidence_persistence_failures"], 1)

    def test_double_terminal_is_refused_without_persisting_second_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            action_log_path = Path(directory) / "actions.jsonl"
            runtime = StatefulActionRuntime(
                run_id="unit-double-terminal",
                seed=9,
                example_index=0,
                capture_state=state,
                action_log=WriteAheadActionLog(action_log_path),
            )
            runtime.run(
                action_type="complete",
                logical_now="2026-08-21T00:00:00Z",
                action_input={},
                mutation=lambda: True,
                trace_provider=lambda: None,
            )
            with self.assertRaisesRegex(StatefulHarnessFailure, "double terminal"):
                runtime._persist_terminal(  # noqa: SLF001 - fault-injection contract test
                    {
                        "status": "ACTION_FAILED",
                        "run_id": "unit-double-terminal",
                        "seed": 9,
                        "example_index": 0,
                        "action_index": 0,
                        "action_type": "complete",
                    }
                )
            events = [json.loads(line) for line in action_log_path.read_text().splitlines()]
            self.assertEqual([item["status"] for item in events], ["ACTION_STARTED", "ACTION_COMPLETED"])
            self.assertEqual(runtime.summary()["double_terminal_actions"], 0)

    def test_success_summary_has_exact_terminal_integrity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runtime = StatefulActionRuntime(
                run_id="unit-summary",
                seed=10,
                example_index=0,
                capture_state=state,
                action_log=WriteAheadActionLog(Path(directory) / "actions.jsonl"),
            )
            runtime.run(
                action_type="complete",
                logical_now="2026-08-21T00:00:00Z",
                action_input={},
                mutation=lambda: True,
                trace_provider=lambda: None,
            )
            summary = runtime.summary()
            self.assertEqual(summary["started_actions"], 1)
            self.assertEqual(summary["completed_actions"], 1)
            self.assertEqual(summary["failed_actions"], 0)
            self.assertEqual(summary["terminal_actions"], 1)
            self.assertEqual(summary["orphan_started_actions"], 0)
            self.assertEqual(summary["double_terminal_actions"], 0)
            self.assertTrue(summary["terminal_integrity_verified"])

    def test_attribution_without_active_action_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runtime = StatefulActionRuntime(
                run_id="unit-2",
                seed=2,
                example_index=0,
                capture_state=state,
                action_log=WriteAheadActionLog(Path(directory) / "actions.jsonl"),
            )
            with self.assertRaises(StatefulAttributionGap):
                runtime.attribution(state())


class CleanupContractTests(unittest.TestCase):
    def test_delivery_cleanup_success_is_verified_by_final_absence(self) -> None:
        machine = DeliveryStateMachine(seed=11, max_steps=1)
        root = machine.root
        evidence = machine.close()
        self.assertTrue(evidence["existed_before"])
        self.assertTrue(evidence["delete_attempted"])
        self.assertIsNone(evidence["delete_exception"])
        self.assertFalse(evidence["exists_after"])
        self.assertTrue(evidence["cleanup_verified"])
        self.assertFalse(root.exists())

    def test_delivery_cleanup_failure_is_not_silently_reported_as_success(self) -> None:
        machine = DeliveryStateMachine(seed=8, max_steps=1)
        root = machine.root

        def injected_rmtree(path: Path, *args: object, **kwargs: object) -> None:
            if kwargs.get("ignore_errors"):
                return
            raise OSError("injected rmtree failure")

        try:
            with patch(
                "tests.stateful_delivery_authority_harness.shutil.rmtree",
                side_effect=injected_rmtree,
            ):
                evidence = machine.close()
            self.assertTrue(root.exists())
            self.assertFalse(evidence["cleanup_verified"])
            self.assertTrue(evidence["exists_after"])
            self.assertEqual(evidence["delete_exception"]["type"], "OSError")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_bounded_run_classifies_injected_cleanup_failure_as_harness_failure(self) -> None:
        leaked_roots: list[Path] = []

        class CleanupFailureAdapter:
            def __init__(self) -> None:
                self.root = Path(tempfile.mkdtemp(prefix="stream-v4-cleanup-unit-"))
                leaked_roots.append(self.root)
                self.trace: list[dict] = []
                self.expected_effects = 0

            def choose_and_apply(self, _rng: object) -> None:
                return None

            def action_trace_summary(self) -> dict:
                return {
                    "started_actions": 0,
                    "completed_actions": 0,
                    "failed_actions": 0,
                    "terminal_actions": 0,
                    "orphan_started_actions": 0,
                    "double_terminal_actions": 0,
                    "terminal_evidence_persistence_failures": 0,
                }

            def close(self) -> dict:
                def fail_delete(_path: Path) -> None:
                    raise OSError("injected cleanup failure")

                return verified_cleanup(self.root, delete=fail_delete)

        try:
            artifact = bounded_explore(
                config=ExplorationConfig(seed=1, examples=1, max_steps=1),
                machine_factory=lambda _seed, _example: CleanupFailureAdapter(),
                artifact_builder=lambda _started, totals, runs, result, _path: {
                    "totals": totals,
                    "runs": runs,
                    "result": result,
                },
                action_log_path=None,
            )
            self.assertEqual(artifact["result"]["classification"], "stateful_harness_failure")
            self.assertEqual(artifact["result"]["failure_type"], "cleanup_verification_failure")
            self.assertEqual(artifact["totals"]["cleanup_failures"], 1)
            self.assertGreater(artifact["totals"]["harness_failures"], 0)
            self.assertEqual(artifact["totals"]["terminal_integrity_failures"], 0)
            self.assertTrue(artifact["runs"][0]["terminal_integrity"]["verified"])
            cleanup = artifact["runs"][0]["cleanup_evidence"]
            self.assertFalse(cleanup["cleanup_verified"])
            self.assertTrue(cleanup["exists_after"])
            self.assertEqual(cleanup["delete_exception"]["type"], "OSError")
        finally:
            for root in leaked_roots:
                shutil.rmtree(root, ignore_errors=True)

    def test_bounded_run_fails_closed_on_inconsistent_terminal_summary(self) -> None:
        class InconsistentAdapter:
            trace: list[dict] = []
            expected_effects = 0

            def choose_and_apply(self, _rng: object) -> None:
                return None

            def action_trace_summary(self) -> dict:
                return {
                    "started_actions": 1,
                    "completed_actions": 0,
                    "failed_actions": 0,
                    "terminal_actions": 0,
                    "orphan_started_actions": 1,
                    "double_terminal_actions": 0,
                    "terminal_evidence_persistence_failures": 0,
                }

            def close(self) -> dict:
                return {
                    "cleanup_target": None,
                    "existed_before": False,
                    "delete_attempted": False,
                    "delete_exception": None,
                    "exists_after": False,
                    "cleanup_verified": True,
                }

        artifact = bounded_explore(
            config=ExplorationConfig(seed=1, examples=1, max_steps=1),
            machine_factory=lambda _seed, _example: InconsistentAdapter(),
            artifact_builder=lambda _started, totals, runs, result, _path: {
                "totals": totals,
                "runs": runs,
                "result": result,
            },
            action_log_path=None,
        )
        self.assertEqual(artifact["result"]["classification"], "stateful_harness_failure")
        self.assertEqual(artifact["result"]["failure_type"], "terminal_integrity_failure")
        self.assertEqual(artifact["totals"]["terminal_integrity_failures"], 1)
        self.assertEqual(artifact["totals"]["orphan_started_actions"], 1)
        self.assertFalse(artifact["runs"][0]["terminal_integrity"]["verified"])


if __name__ == "__main__":
    unittest.main()
