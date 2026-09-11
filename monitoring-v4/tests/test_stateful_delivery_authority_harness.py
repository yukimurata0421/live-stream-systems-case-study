from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.time import unix_ts

from tests.stateful_delivery_authority_harness import (
    DeliveryAuthorityOracle,
    DeliveryStateMachine,
    Evidence,
    SOURCES,
    StatefulHarnessFailure,
    reproduce_equal_timestamp_conflict,
    validate_oracle_policy_alignment,
)


BASE = unix_ts("2026-08-12T16:48:32Z")


def evidence(source: str, state: str, observed: int, received: int | None = None, identity: str = "event") -> Evidence:
    return Evidence(source, state, observed, observed if received is None else received, identity, "synthetic", SOURCES[source].ttl_sec)


class DeliveryAuthorityOracleTests(unittest.TestCase):
    def setUp(self) -> None:
        validate_oracle_policy_alignment()
        self.oracle = DeliveryAuthorityOracle()

    def test_fresh_good_beats_stale_bad(self) -> None:
        current = self.oracle.canonical([evidence("a", "good", BASE), evidence("b", "bad", BASE - 61)], now_ts=BASE)
        self.assertEqual((current.state, current.reason), ("good", "current_good_evidence"))

    def test_stale_good_yields_to_fresh_bad(self) -> None:
        current = self.oracle.canonical([evidence("a", "good", BASE - 181), evidence("b", "bad", BASE)], now_ts=BASE)
        self.assertEqual((current.state, current.reason), ("bad", "current_bad_evidence"))

    def test_equal_priority_disagreement_is_unknown(self) -> None:
        current = self.oracle.canonical([evidence("a", "good", BASE), evidence("b", "bad", BASE)], now_ts=BASE)
        self.assertEqual((current.state, current.reason), ("unknown", "source_disagreement"))

    def test_equal_priority_agreement_is_good(self) -> None:
        current = self.oracle.canonical([evidence("a", "good", BASE), evidence("b", "good", BASE)], now_ts=BASE)
        self.assertEqual((current.state, current.reason), ("good", "current_good_evidence"))

    def test_same_source_old_bad_received_late_does_not_replace_new_good(self) -> None:
        current = self.oracle.canonical([evidence("a", "bad", BASE, BASE + 50, "old"), evidence("a", "good", BASE + 1, BASE + 1, "new")], now_ts=BASE + 50)
        self.assertEqual((current.state, current.observed_ts), ("good", BASE + 1))

    def test_ttl_boundary_is_fresh(self) -> None:
        current = self.oracle.canonical([evidence("b", "good", BASE)], now_ts=BASE + 60)
        self.assertEqual(current.state, "good")

    def test_ttl_plus_one_is_stale(self) -> None:
        current = self.oracle.canonical([evidence("b", "good", BASE)], now_ts=BASE + 61)
        self.assertEqual((current.state, current.reason), ("unknown", "missing_current_evidence"))

    def test_restart_reopen_do_not_change_semantics(self) -> None:
        machine = DeliveryStateMachine(seed=1, max_steps=4)
        try:
            before = machine.trace[-1]["actual"]
            machine.restart_component()
            machine.reopen_repository()
            self.assertEqual(machine.trace[-1]["actual"], before)
        finally:
            machine.close()


class DeliveryAuthorityFixedRegressionTests(unittest.TestCase):
    def test_two_source_conflict_and_recovery_are_deterministic(self) -> None:
        machine = DeliveryStateMachine(seed=2, max_steps=5)
        try:
            machine.ingest("b", "bad")
            self.assertEqual(machine.trace[-1]["actual"]["state"], "unknown")
            machine.advance_clock(61)
            self.assertEqual(machine.trace[-1]["actual"]["state"], "good")
        finally:
            machine.close()

    def test_equal_timestamp_conflict_persists_source_disagreement(self) -> None:
        machine = DeliveryStateMachine(seed=909_100_004, max_steps=1)
        try:
            machine.ingest("b", "bad")
            self.assertEqual(
                (machine.trace[-1]["actual"]["state"], machine.trace[-1]["actual"]["reason"]),
                ("unknown", "source_disagreement"),
            )
        finally:
            machine.close()

    def test_expired_newer_source_yields_to_fresh_older_source(self) -> None:
        machine = DeliveryStateMachine(seed=100_010, max_steps=4)
        try:
            machine.ingest("b", "bad")
            machine.ingest("b", "good")
            machine.advance_clock(60)
            self.assertEqual(
                (
                    machine.trace[-1]["actual"]["state"],
                    machine.trace[-1]["actual"]["selected_sources"],
                    machine.trace[-1]["actual"]["observed_ts"],
                ),
                ("good", ("runtime_delivery_watchdog",), BASE),
            )
        finally:
            machine.close()

    def test_same_timestamp_historical_recovery_is_not_attributed_to_later_bad(self) -> None:
        """Regression for seed 3: attribution is pre/post identity diff only."""
        machine = DeliveryStateMachine(seed=300_027, max_steps=4)
        try:
            machine.ingest("a", "bad")
            machine.replay_old("a")
            machine.replay_old("b")
            machine.ingest("a", "bad")
            final = machine.trace[-1]
            created = final["attribution"]["created_transitions"]
            self.assertTrue(final["attribution"]["attribution_complete"])
            self.assertEqual(final["actual"]["state"], "bad")
            self.assertEqual([item["phase"] for item in created], ["detected"])
            self.assertTrue(
                all(
                    item["current_snapshot_id"] == final["attribution"]["created_transitions"][0]["current_snapshot_id"]
                    for item in created
                )
            )
        finally:
            machine.close()


class DeliveryActionMeasurementTests(unittest.TestCase):
    def test_action_exception_keeps_fsynced_started_and_failed_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            action_log = Path(directory) / "actions.jsonl"
            machine = DeliveryStateMachine(seed=77, max_steps=1, action_log_path=action_log)
            try:
                with self.assertRaisesRegex(StatefulHarnessFailure, "test-only injected"):
                    machine.inject_action_exception_for_test()
                events = [json.loads(line) for line in action_log.read_text(encoding="utf-8").splitlines()]
                started = [event for event in events if event["status"] == "ACTION_STARTED"][-1]
                completed = [event for event in events if event["status"] == "ACTION_COMPLETED"]
                failed = [event for event in events if event["status"] == "ACTION_FAILED"]
                self.assertEqual(started["action_type"], "test_injected_exception")
                self.assertEqual(started["evidence_classification"], "synthetic")
                self.assertIsNotNone(started["observation_identity"])
                self.assertIsNotNone(started["pre_canonical"])
                self.assertEqual(started["pre_transition_ids"], sorted(started["pre_transition_ids"]))
                self.assertEqual(started["pre_intent_ids"], sorted(started["pre_intent_ids"]))
                self.assertEqual(started["pre_episode_ids"], sorted(started["pre_episode_ids"]))
                self.assertEqual(started["pre_transition_count"], 0)
                self.assertEqual(started["pre_intent_count"], 0)
                self.assertFalse(any(event["action_index"] == started["action_index"] for event in completed))
                self.assertEqual(failed[-1]["classification"], "stateful_harness_failure")
                self.assertEqual(failed[-1]["post_transition_ids"], [])
                self.assertEqual(failed[-1]["post_intent_ids"], [])
                self.assertEqual(failed[-1]["created_transition_ids"], [])
                self.assertEqual(failed[-1]["created_intent_ids"], [])
                self.assertEqual(failed[-1]["post_state_collection_errors"], [])
                summary = machine.action_trace_summary()
                self.assertEqual(summary["incomplete_actions"], 0)
                self.assertEqual(summary["orphan_started_actions"], 0)
                self.assertEqual(summary["double_terminal_actions"], 0)
                self.assertEqual(summary["failed_actions"], 1)
                self.assertEqual(summary["terminal_actions"], summary["started_actions"])
                self.assertEqual(summary["last_started_action"]["action_index"], started["action_index"])
                self.assertEqual(summary["last_completed_action"]["action_type"], "historical_anchor")
                self.assertEqual(summary["last_failed_action"]["action_index"], started["action_index"])
            finally:
                machine.close()


if __name__ == "__main__":
    unittest.main()
