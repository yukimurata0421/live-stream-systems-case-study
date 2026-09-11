from __future__ import annotations

import unittest

from tests.stateful_viewer_external_authority_harness import (
    SYNTHETIC_ANCHOR_TS,
    Evidence,
    ViewerExternalOracle,
    ViewerExternalStateMachine,
    validate_oracle_policy_alignment,
)


def evidence(
    source: str,
    status: str,
    *,
    observed: int = SYNTHETIC_ANCHOR_TS,
    received: int | None = None,
    identity: str | None = None,
) -> Evidence:
    return Evidence(
        source,
        status,
        observed,
        observed if received is None else received,
        identity or f"{source}-{status}-{observed}-{received}",
        "unit",
    )


class ViewerExternalPolicyTests(unittest.TestCase):
    def test_current_policy_is_supporting_current_domain_exception(self) -> None:
        identity = validate_oracle_policy_alignment()
        self.assertEqual(identity["allowed_roles"], ["supporting"])
        self.assertEqual(identity["sources"], ["viewer_synthetic", "external_blackbox"])


class ViewerExternalOracleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.oracle = ViewerExternalOracle()

    def test_fresh_viewer_good_beats_fresh_external_bad(self) -> None:
        result = self.oracle.canonical(
            [evidence("viewer", "good"), evidence("external", "bad")],
            now_ts=SYNTHETIC_ANCHOR_TS,
        )
        self.assertEqual((result.state, result.selected_sources), ("good", ("viewer_synthetic",)))

    def test_fresh_viewer_bad_beats_fresh_external_good(self) -> None:
        result = self.oracle.canonical(
            [evidence("viewer", "bad"), evidence("external", "good")],
            now_ts=SYNTHETIC_ANCHOR_TS,
        )
        self.assertEqual((result.state, result.selected_sources), ("bad", ("viewer_synthetic",)))

    def test_stale_viewer_falls_back_to_fresh_external(self) -> None:
        result = self.oracle.canonical(
            [
                evidence("viewer", "good", observed=SYNTHETIC_ANCHOR_TS),
                evidence("external", "bad", observed=SYNTHETIC_ANCHOR_TS + 901),
            ],
            now_ts=SYNTHETIC_ANCHOR_TS + 901,
        )
        self.assertEqual((result.state, result.selected_sources), ("bad", ("external_blackbox",)))

    def test_lower_priority_newer_timestamp_does_not_beat_fresh_viewer(self) -> None:
        result = self.oracle.canonical(
            [
                evidence("viewer", "good", observed=SYNTHETIC_ANCHOR_TS),
                evidence("external", "bad", observed=SYNTHETIC_ANCHOR_TS + 899),
            ],
            now_ts=SYNTHETIC_ANCHOR_TS + 899,
        )
        self.assertEqual((result.state, result.selected_sources), ("good", ("viewer_synthetic",)))

    def test_ttl_equality_is_fresh_and_plus_one_is_stale(self) -> None:
        item = evidence("viewer", "good")
        fresh = self.oracle.canonical([item], now_ts=SYNTHETIC_ANCHOR_TS + 900)
        stale = self.oracle.canonical([item], now_ts=SYNTHETIC_ANCHOR_TS + 901)
        self.assertEqual(fresh.state, "good")
        self.assertEqual((stale.state, stale.reason), ("unknown", "missing_current_evidence"))

    def test_same_source_old_replay_does_not_replace_newer_recovery(self) -> None:
        result = self.oracle.canonical(
            [
                evidence("external", "good", observed=SYNTHETIC_ANCHOR_TS + 10, identity="new-good"),
                evidence("external", "bad", observed=SYNTHETIC_ANCHOR_TS, received=SYNTHETIC_ANCHOR_TS + 20, identity="old-bad"),
            ],
            now_ts=SYNTHETIC_ANCHOR_TS + 20,
        )
        self.assertEqual((result.state, result.selected_sources), ("good", ("external_blackbox",)))


class ViewerExternalStateMachineTests(unittest.TestCase):
    def test_bad_viewer_detects_and_good_viewer_recovers(self) -> None:
        machine = ViewerExternalStateMachine(seed=1, max_steps=5)
        try:
            machine.ingest("external", "good")
            machine.advance_clock(1)
            machine.ingest("viewer", "bad")
            self.assertEqual(machine.trace[-1]["actual"]["incident_phase"], "detected")
            machine.advance_clock(1)
            machine.ingest("viewer", "good")
            self.assertEqual(machine.trace[-1]["actual"]["incident_phase"], "recovered")
        finally:
            self.assertTrue(machine.close()["cleanup_verified"])

    def test_stale_viewer_external_bad_fallback_then_viewer_failback(self) -> None:
        machine = ViewerExternalStateMachine(seed=2, max_steps=4)
        try:
            machine.advance_clock(901)
            machine.ingest("external", "bad")
            self.assertEqual(machine.trace[-1]["actual"]["selected_sources"], ("external_blackbox",))
            self.assertEqual(machine.trace[-1]["actual"]["incident_phase"], "detected")
            machine.advance_clock(1)
            machine.ingest("viewer", "good")
            self.assertEqual(machine.trace[-1]["actual"]["selected_sources"], ("viewer_synthetic",))
            self.assertEqual(machine.trace[-1]["actual"]["incident_phase"], "recovered")
        finally:
            self.assertTrue(machine.close()["cleanup_verified"])

    def test_old_replay_restart_and_reopen_are_neutral(self) -> None:
        machine = ViewerExternalStateMachine(seed=3, max_steps=6)
        try:
            machine.ingest("external", "bad")
            machine.advance_clock(1)
            machine.ingest("external", "good")
            before = machine.trace[-1]["actual"]
            machine.replay_old("external")
            self.assertEqual(machine.trace[-1]["actual"], before)
            machine.restart_component()
            self.assertEqual(machine.trace[-1]["actual"], before)
            machine.reopen_repository()
            self.assertEqual(machine.trace[-1]["actual"], before)
            self.assertEqual(machine.action_trace_summary()["orphan_started_actions"], 0)
        finally:
            self.assertTrue(machine.close()["cleanup_verified"])


if __name__ == "__main__":
    unittest.main()
