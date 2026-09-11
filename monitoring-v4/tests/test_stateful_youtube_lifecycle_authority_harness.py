from __future__ import annotations

import unittest

from stream_contracts.monitoring_v4.time import unix_ts
from tests.stateful_youtube_lifecycle_authority_harness import (
    Evidence,
    SOURCES,
    YouTubeLifecycleOracle,
    YouTubeLifecycleStateMachine,
    validate_oracle_policy_alignment,
)


BASE = unix_ts("2026-08-21T00:00:00Z")


def evidence(
    source: str,
    state: str,
    observed: int,
    received: int | None = None,
    identity: str = "event",
) -> Evidence:
    return Evidence(
        source,
        state,
        observed,
        observed if received is None else received,
        identity,
        "synthetic",
        SOURCES[source].ttl_sec,
    )


class YouTubeLifecyclePolicyTests(unittest.TestCase):
    def test_current_policy_role_identity(self) -> None:
        identity = validate_oracle_policy_alignment()
        self.assertTrue(identity["watchdog"]["effective_canonical_authority"])
        self.assertTrue(identity["resolver"]["effective_canonical_authority"])
        self.assertFalse(identity["diagnostic"]["effective_canonical_authority"])
        self.assertFalse(identity["supporting"]["effective_canonical_authority"])
        self.assertTrue(identity["supporting"]["configured_current_authority"])
        self.assertFalse(identity["supporting"]["allowed_by_domain_role"])


class YouTubeLifecycleOracleTests(unittest.TestCase):
    def setUp(self) -> None:
        validate_oracle_policy_alignment()
        self.oracle = YouTubeLifecycleOracle()

    def test_yl1_diagnostic_bad_cannot_override_authoritative_good(self) -> None:
        current = self.oracle.canonical(
            [
                evidence("watchdog", "good", BASE),
                evidence("diagnostic", "bad", BASE + 1),
            ],
            now_ts=BASE + 1,
        )
        self.assertEqual((current.state, current.selected_sources), ("good", ("youtube_watchdog",)))

    def test_yl2_diagnostic_good_cannot_recover_authoritative_bad(self) -> None:
        current = self.oracle.canonical(
            [
                evidence("watchdog", "bad", BASE),
                evidence("diagnostic", "good", BASE + 1),
            ],
            now_ts=BASE + 1,
        )
        self.assertEqual((current.state, current.selected_sources), ("bad", ("youtube_watchdog",)))

    def test_yl3_supporting_good_cannot_override_authoritative_bad(self) -> None:
        current = self.oracle.canonical(
            [
                evidence("watchdog", "bad", BASE),
                evidence("supporting", "good", BASE + 1),
            ],
            now_ts=BASE + 1,
        )
        self.assertEqual((current.state, current.selected_sources), ("bad", ("youtube_watchdog",)))

    def test_yl4_supporting_bad_cannot_override_authoritative_good(self) -> None:
        current = self.oracle.canonical(
            [
                evidence("watchdog", "good", BASE),
                evidence("supporting", "bad", BASE + 1),
            ],
            now_ts=BASE + 1,
        )
        self.assertEqual((current.state, current.selected_sources), ("good", ("youtube_watchdog",)))

    def test_yl5_authoritative_priority(self) -> None:
        current = self.oracle.canonical(
            [
                evidence("watchdog", "good", BASE),
                evidence("resolver", "bad", BASE + 1),
            ],
            now_ts=BASE + 1,
        )
        self.assertEqual((current.state, current.selected_sources), ("good", ("youtube_watchdog",)))

    def test_yl6_stale_high_falls_back_to_fresh_resolver(self) -> None:
        current = self.oracle.canonical(
            [
                evidence("watchdog", "good", BASE - 181),
                evidence("resolver", "bad", BASE),
            ],
            now_ts=BASE,
        )
        self.assertEqual((current.state, current.selected_sources), ("bad", ("youtube_video_resolver",)))

    def test_yl7_high_authority_return(self) -> None:
        current = self.oracle.canonical(
            [
                evidence("resolver", "bad", BASE),
                evidence("watchdog", "good", BASE + 1),
            ],
            now_ts=BASE + 1,
        )
        self.assertEqual((current.state, current.selected_sources), ("good", ("youtube_watchdog",)))

    def test_yl8_diagnostic_only_when_authoritative_stale_is_unknown(self) -> None:
        current = self.oracle.canonical(
            [
                evidence("watchdog", "bad", BASE - 181),
                evidence("resolver", "bad", BASE - 91),
                evidence("diagnostic", "good", BASE),
            ],
            now_ts=BASE,
        )
        self.assertEqual((current.state, current.reason), ("unknown", "missing_current_evidence"))

    def test_yl9_supporting_only_when_authoritative_stale_is_unknown(self) -> None:
        current = self.oracle.canonical(
            [
                evidence("watchdog", "bad", BASE - 181),
                evidence("resolver", "bad", BASE - 91),
                evidence("supporting", "good", BASE),
            ],
            now_ts=BASE,
        )
        self.assertEqual((current.state, current.reason), ("unknown", "missing_current_evidence"))

    def test_same_source_old_replay_does_not_regress(self) -> None:
        current = self.oracle.canonical(
            [
                evidence("watchdog", "bad", BASE, BASE + 50, "old"),
                evidence("watchdog", "good", BASE + 1, BASE + 1, "new"),
            ],
            now_ts=BASE + 50,
        )
        self.assertEqual((current.state, current.observed_ts), ("good", BASE + 1))


class YouTubeLifecycleSUTBoundaryTests(unittest.TestCase):
    def test_diagnostic_good_does_not_recover_authoritative_bad(self) -> None:
        machine = YouTubeLifecycleStateMachine(
            slice_name="diagnostic", seed=1, max_steps=4
        )
        try:
            machine.advance_clock(1)
            machine.ingest("watchdog", "bad")
            self.assertEqual(machine.trace[-1]["actual"]["state"], "bad")
            before = machine.capture_state()
            machine.ingest("diagnostic", "good")
            after = machine.capture_state()
            self.assertEqual(machine.trace[-1]["actual"]["state"], "bad")
            self.assertIn(
                "youtube_api_direct_lifecycle",
                machine.trace[-1]["actual"]["diagnostic_sources"],
            )
            self.assertEqual(after["transition_count"], before["transition_count"])
            self.assertEqual(after["intent_count"], before["intent_count"])
        finally:
            machine.close()

    def test_supporting_good_does_not_recover_authoritative_bad(self) -> None:
        machine = YouTubeLifecycleStateMachine(
            slice_name="supporting", seed=2, max_steps=4
        )
        try:
            machine.advance_clock(1)
            machine.ingest("watchdog", "bad")
            self.assertEqual(machine.trace[-1]["actual"]["state"], "bad")
            before = machine.capture_state()
            machine.ingest("supporting", "good")
            after = machine.capture_state()
            self.assertEqual(machine.trace[-1]["actual"]["state"], "bad")
            self.assertNotIn(
                "youtube_public", machine.trace[-1]["actual"]["selected_sources"]
            )
            self.assertIn(
                ("youtube_public", "role_not_current"),
                machine.trace[-1]["actual"]["ignored"],
            )
            self.assertEqual(after["transition_count"], before["transition_count"])
            self.assertEqual(after["intent_count"], before["intent_count"])
        finally:
            machine.close()

    def test_restart_and_reopen_are_neutral(self) -> None:
        machine = YouTubeLifecycleStateMachine(
            slice_name="supporting", seed=3, max_steps=3
        )
        try:
            before = machine.trace[-1]["actual"]
            machine.restart_component()
            machine.reopen_repository()
            self.assertEqual(machine.trace[-1]["actual"], before)
        finally:
            machine.close()


if __name__ == "__main__":
    unittest.main()
