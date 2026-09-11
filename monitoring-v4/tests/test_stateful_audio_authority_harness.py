from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.time import unix_ts
from tests.stateful_audio_authority_harness import (
    AudioAuthorityOracle,
    AudioStateMachine,
    Evidence,
    SOURCES,
    StatefulHarnessFailure,
    validate_oracle_policy_alignment,
)


BASE = unix_ts("2026-08-21T00:00:00Z")


def evidence(source: str, state: str, observed: int, received: int | None = None, identity: str = "event") -> Evidence:
    return Evidence(source, state, observed, observed if received is None else received, identity, "synthetic", SOURCES[source].ttl_sec)


class AudioAuthorityOracleTests(unittest.TestCase):
    def setUp(self) -> None:
        validate_oracle_policy_alignment()
        self.oracle = AudioAuthorityOracle()

    def test_au1_fresh_high_good_beats_fresh_low_bad(self) -> None:
        current = self.oracle.canonical([evidence("high", "good", BASE), evidence("low", "bad", BASE)], now_ts=BASE)
        self.assertEqual((current.state, current.selected_sources), ("good", ("audio_watchdog",)))

    def test_au2_fresh_high_bad_beats_fresh_low_good(self) -> None:
        current = self.oracle.canonical([evidence("high", "bad", BASE), evidence("low", "good", BASE)], now_ts=BASE)
        self.assertEqual((current.state, current.selected_sources), ("bad", ("audio_watchdog",)))

    def test_au3_stale_high_falls_back_to_fresh_low(self) -> None:
        current = self.oracle.canonical([evidence("high", "bad", BASE - 181), evidence("low", "good", BASE)], now_ts=BASE)
        self.assertEqual((current.state, current.selected_sources), ("good", ("legacy_subsystem_audio",)))

    def test_au4_high_return_reclaims_authority(self) -> None:
        current = self.oracle.canonical([evidence("low", "bad", BASE), evidence("high", "good", BASE + 1)], now_ts=BASE + 1)
        self.assertEqual((current.state, current.selected_sources), ("good", ("audio_watchdog",)))

    def test_au5_old_high_late_replay_cannot_replace_new_high(self) -> None:
        current = self.oracle.canonical([evidence("high", "bad", BASE, BASE + 50, "old"), evidence("high", "good", BASE + 1, BASE + 1, "new")], now_ts=BASE + 50)
        self.assertEqual((current.state, current.observed_ts), ("good", BASE + 1))

    def test_au6_lower_newer_timestamp_cannot_beat_fresh_high(self) -> None:
        current = self.oracle.canonical([evidence("high", "good", BASE), evidence("low", "bad", BASE + 100)], now_ts=BASE + 100)
        self.assertEqual((current.state, current.selected_sources), ("good", ("audio_watchdog",)))

    def test_au7_ttl_equality_is_fresh(self) -> None:
        self.assertEqual(self.oracle.canonical([evidence("high", "good", BASE)], now_ts=BASE + 180).state, "good")

    def test_au8_ttl_plus_one_is_stale(self) -> None:
        current = self.oracle.canonical([evidence("high", "good", BASE)], now_ts=BASE + 181)
        self.assertEqual((current.state, current.reason), ("unknown", "missing_current_evidence"))


class AudioAuthorityStateMachineTests(unittest.TestCase):
    def test_restart_and_reopen_are_semantically_neutral(self) -> None:
        machine = AudioStateMachine(seed=1, max_steps=4)
        try:
            before = machine.trace[-1]["actual"]
            machine.restart_component()
            machine.reopen_repository()
            self.assertEqual(machine.trace[-1]["actual"], before)
        finally:
            machine.close()

    def test_high_stale_low_fallback_then_high_return(self) -> None:
        machine = AudioStateMachine(seed=2, max_steps=5)
        try:
            machine.advance_clock(181)
            machine.ingest("low", "bad")
            self.assertEqual(machine.trace[-1]["actual"]["selected_sources"], ("legacy_subsystem_audio",))
            machine.ingest("high", "good")
            self.assertEqual(machine.trace[-1]["actual"]["selected_sources"], ("audio_watchdog",))
        finally:
            machine.close()

    def test_action_failure_has_identity_post_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            action_log = Path(directory) / "actions.jsonl"
            machine = AudioStateMachine(seed=3, max_steps=1, action_log_path=action_log)
            try:
                with self.assertRaisesRegex(StatefulHarnessFailure, "test-only injected"):
                    machine.inject_action_exception_for_test()
                events = [json.loads(line) for line in action_log.read_text(encoding="utf-8").splitlines()]
                failed = [event for event in events if event["status"] == "ACTION_FAILED"][-1]
                self.assertEqual(failed["classification"], "stateful_harness_failure")
                self.assertEqual(failed["created_transition_ids"], [])
                self.assertEqual(failed["created_intent_ids"], [])
                self.assertEqual(failed["post_state_collection_errors"], [])
            finally:
                machine.close()


if __name__ == "__main__":
    unittest.main()
