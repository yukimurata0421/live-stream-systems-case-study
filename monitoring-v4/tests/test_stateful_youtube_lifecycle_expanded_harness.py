from __future__ import annotations

import unittest

from stream_contracts.monitoring_v4.time import unix_ts
from tests.stateful_youtube_lifecycle_expanded_harness import (
    AdapterEvidence,
    ExpandedYouTubeLifecycleOracle,
    ExpandedYouTubeLifecycleStateMachine,
    SCENARIOS,
    SOURCES,
    validate_adapter_policy_alignment,
)


BASE = unix_ts("2026-08-21T00:00:00Z")


def evidence(source: str, state: str, observed: int) -> AdapterEvidence:
    return AdapterEvidence(
        source,
        state,
        observed,
        observed,
        f"{source}-{state}-{observed}",
        "synthetic_unit",
        SOURCES[source].ttl_sec,
    )


class ExpandedYouTubeLifecyclePolicyTests(unittest.TestCase):
    def test_adapter_roles_match_current_source_contract(self) -> None:
        identity = validate_adapter_policy_alignment()
        self.assertEqual(identity["watchdog"]["adapter_evidence_role"], "current_authoritative")
        self.assertEqual(identity["resolver"]["adapter_evidence_role"], "current_correlated")
        self.assertEqual(identity["diagnostic"]["adapter_evidence_role"], "current_correlated")
        self.assertTrue(identity["resolver"]["current_authority"])
        self.assertFalse(identity["diagnostic"]["current_authority"])


class ExpandedYouTubeLifecycleOracleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.oracle = ExpandedYouTubeLifecycleOracle()

    def test_authoritative_unknown_uses_current_evidence_unknown_reason(self) -> None:
        current = self.oracle.canonical(
            [evidence("watchdog", "unknown", BASE)], now_ts=BASE
        )
        self.assertEqual(
            (current.state, current.reason, current.selected_sources),
            ("unknown", "current_evidence_unknown", ("youtube_watchdog",)),
        )

    def test_api_disagreement_requires_selected_watchdog_producer(self) -> None:
        watchdog = self.oracle.context(
            [
                evidence("watchdog", "good", BASE),
                evidence("diagnostic", "bad", BASE),
            ],
            now_ts=BASE,
        )
        resolver = self.oracle.context(
            [
                evidence("watchdog", "good", BASE - 181),
                evidence("resolver", "good", BASE),
                evidence("diagnostic", "bad", BASE),
            ],
            now_ts=BASE,
        )
        self.assertTrue(watchdog.same_producer_api_disagreement)
        self.assertFalse(resolver.same_producer_api_disagreement)


class ExpandedYouTubeLifecycleDirectedScenarioTests(unittest.TestCase):
    def test_every_directed_scenario_reaches_its_terminal_invariants(self) -> None:
        for index, name in enumerate(SCENARIOS):
            with self.subTest(scenario=name):
                machine = ExpandedYouTubeLifecycleStateMachine(
                    seed=100 + index,
                    max_steps=len(SCENARIOS[name]),
                    example_index=index,
                    scenario_name=name,
                )
                try:
                    for _ in SCENARIOS[name]:
                        machine.choose_and_apply(None)  # type: ignore[arg-type]
                    self.assertTrue(machine.trace[-1]["scenario_complete"])
                finally:
                    machine.close()


if __name__ == "__main__":
    unittest.main()
