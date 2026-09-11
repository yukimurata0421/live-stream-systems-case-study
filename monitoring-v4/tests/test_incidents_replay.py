from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.incidents.engine import evaluate_current
from stream_monitoring_v4.incidents.policy import DEFAULT_INCIDENT_POLICIES
from stream_monitoring_v4.incidents.replay import ReplayStep, replay
from stream_monitoring_v4.incidents.service import IncidentService
from stream_monitoring_v4.notifications.routing import RoutePolicy
from stream_monitoring_v4.storage.repository import MonitoringRepository

from tests.helpers import BASE_TS, current


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "monitoring_v4"


class IncidentEngineTests(unittest.TestCase):
    def test_new_operational_bad_states_require_sustained_distinct_samples(self) -> None:
        for domain in (
            "network_transport",
            "runtime_resource",
            "adsb_source",
            "api_quota",
            "recovery_policy",
            "notification_delivery",
            "control_loop",
        ):
            policy = DEFAULT_INCIDENT_POLICIES[domain]
            self.assertGreaterEqual(policy.bad_min_samples, 2, domain)
            self.assertGreaterEqual(policy.bad_min_duration_sec, 180, domain)
            self.assertEqual(policy.repeat_sec, 600, domain)

    def test_bad_delivery_activates_immediately_as_critical(self) -> None:
        item = current(state="bad")
        evaluation = evaluate_current(
            item,
            DEFAULT_INCIDENT_POLICIES["delivery"],
            now_at=utc_text(BASE_TS),
            active_episode=None,
            pending_candidate=None,
        )
        self.assertEqual(evaluation.transition.phase, "detected")
        self.assertEqual(evaluation.transition.severity, "critical")
        self.assertEqual(evaluation.episode.next_notification_at, utc_text(BASE_TS + 600))

    def test_unknown_needs_two_distinct_samples_and_five_minutes(self) -> None:
        policy = DEFAULT_INCIDENT_POLICIES["youtube_lifecycle"]
        first = evaluate_current(
            current(domain="youtube_lifecycle", state="unknown", reason="missing_current_evidence"),
            policy,
            now_at=utc_text(BASE_TS),
            active_episode=None,
            pending_candidate=None,
        )
        self.assertIsNone(first.transition)
        duplicate = evaluate_current(
            current(domain="youtube_lifecycle", state="unknown", reason="missing_current_evidence"),
            policy,
            now_at=utc_text(BASE_TS + 300),
            active_episode=None,
            pending_candidate=first.candidate,
        )
        self.assertIsNone(duplicate.transition)
        second = evaluate_current(
            current(
                domain="youtube_lifecycle",
                state="unknown",
                observed_ts=BASE_TS + 300,
                reason="missing_current_evidence",
                marker="second",
            ),
            policy,
            now_at=utc_text(BASE_TS + 300),
            active_episode=None,
            pending_candidate=first.candidate,
        )
        self.assertEqual(second.transition.phase, "detected")

    def test_unknown_reason_family_change_resets_pending_gate(self) -> None:
        policy = DEFAULT_INCIDENT_POLICIES["youtube_lifecycle"]
        first = evaluate_current(
            current(domain="youtube_lifecycle", state="unknown", reason="missing_current_evidence"),
            policy,
            now_at=utc_text(BASE_TS),
            active_episode=None,
            pending_candidate=None,
        )
        changed = evaluate_current(
            current(
                domain="youtube_lifecycle",
                state="unknown",
                observed_ts=BASE_TS + 300,
                reason="source_disagreement",
                marker="changed-family",
            ),
            policy,
            now_at=utc_text(BASE_TS + 300),
            active_episode=None,
            pending_candidate=first.candidate,
        )
        self.assertIsNone(changed.transition)
        self.assertEqual(changed.candidate.samples, 1)
        self.assertEqual(changed.candidate.reason_codes, ("source_disagreement",))

    def test_repeat_cadence_is_anchored_and_does_not_accumulate_scheduler_delay(self) -> None:
        policy = DEFAULT_INCIDENT_POLICIES["delivery"]
        detected = evaluate_current(
            current(state="bad"),
            policy,
            now_at=utc_text(BASE_TS),
            active_episode=None,
            pending_candidate=None,
        )
        before_due = evaluate_current(
            current(state="bad", observed_ts=BASE_TS + 599, marker="before"),
            policy,
            now_at=utc_text(BASE_TS + 599),
            active_episode=detected.episode,
            pending_candidate=None,
        )
        self.assertIsNone(before_due.transition)
        delayed = evaluate_current(
            current(state="bad", observed_ts=BASE_TS + 601, marker="delayed"),
            policy,
            now_at=utc_text(BASE_TS + 601),
            active_episode=before_due.episode,
            pending_candidate=None,
        )
        self.assertEqual(delayed.transition.phase, "repeat")
        self.assertEqual(delayed.episode.next_notification_at, utc_text(BASE_TS + 1200))

    def test_source_disagreement_uses_thirty_minute_repeat(self) -> None:
        policy = DEFAULT_INCIDENT_POLICIES["youtube_lifecycle"]
        first = evaluate_current(
            current(domain="youtube_lifecycle", state="unknown", reason="source_disagreement"),
            policy,
            now_at=utc_text(BASE_TS),
            active_episode=None,
            pending_candidate=None,
        )
        detected = evaluate_current(
            current(
                domain="youtube_lifecycle",
                state="unknown",
                reason="source_disagreement",
                observed_ts=BASE_TS + 300,
                marker="second",
            ),
            policy,
            now_at=utc_text(BASE_TS + 300),
            active_episode=None,
            pending_candidate=first.candidate,
        )
        self.assertEqual(detected.episode.next_notification_at, utc_text(BASE_TS + 2100))

    def test_explicit_bad_clamps_a_slower_unknown_repeat_deadline(self) -> None:
        policy = DEFAULT_INCIDENT_POLICIES["youtube_lifecycle"]
        first = evaluate_current(
            current(
                domain="youtube_lifecycle",
                state="unknown",
                reason="source_disagreement",
            ),
            policy,
            now_at=utc_text(BASE_TS),
            active_episode=None,
            pending_candidate=None,
        )
        detected = evaluate_current(
            current(
                domain="youtube_lifecycle",
                state="unknown",
                reason="source_disagreement",
                observed_ts=BASE_TS + 300,
                marker="unknown-open",
            ),
            policy,
            now_at=utc_text(BASE_TS + 300),
            active_episode=None,
            pending_candidate=first.candidate,
        )
        explicit_bad = evaluate_current(
            current(
                domain="youtube_lifecycle",
                state="bad",
                reason="current_bad_evidence",
                observed_ts=BASE_TS + 360,
                marker="explicit-bad",
            ),
            policy,
            now_at=utc_text(BASE_TS + 360),
            active_episode=detected.episode,
            pending_candidate=None,
        )
        self.assertIsNone(explicit_bad.transition)
        self.assertEqual(
            explicit_bad.episode.next_notification_at,
            utc_text(BASE_TS + 960),
        )

    def test_input_quality_nodata_unknown_and_same_producer_disagreement_are_not_incidents(self) -> None:
        policy = DEFAULT_INCIDENT_POLICIES["youtube_input_quality"]
        for reason in (
            "input_quality_health_nodata",
            "input_quality_health_unknown",
            "missing_current_evidence",
            "source_disagreement",
        ):
            with self.subTest(reason=reason):
                first = evaluate_current(
                    current(domain="youtube_input_quality", state="unknown", reason=reason),
                    policy,
                    now_at=utc_text(BASE_TS),
                    active_episode=None,
                    pending_candidate=None,
                )
                second = evaluate_current(
                    current(
                        domain="youtube_input_quality",
                        state="unknown",
                        observed_ts=BASE_TS + 300,
                        reason=reason,
                        marker="second",
                    ),
                    policy,
                    now_at=utc_text(BASE_TS + 300),
                    active_episode=None,
                    pending_candidate=first.candidate,
                )
                self.assertIsNone(first.candidate)
                self.assertIsNone(first.transition)
                self.assertIsNone(second.candidate)
                self.assertIsNone(second.transition)

    def test_input_quality_coverage_and_freshness_unknown_remain_actionable(self) -> None:
        policy = DEFAULT_INCIDENT_POLICIES["youtube_input_quality"]
        for reason in ("measurement_coverage_missing", "measurement_freshness_stale"):
            with self.subTest(reason=reason):
                first = evaluate_current(
                    current(domain="youtube_input_quality", state="unknown", reason=reason),
                    policy,
                    now_at=utc_text(BASE_TS),
                    active_episode=None,
                    pending_candidate=None,
                )
                detected = evaluate_current(
                    current(
                        domain="youtube_input_quality",
                        state="unknown",
                        observed_ts=BASE_TS + 300,
                        reason=reason,
                        marker="second",
                    ),
                    policy,
                    now_at=utc_text(BASE_TS + 300),
                    active_episode=None,
                    pending_candidate=first.candidate,
                )
                self.assertEqual(detected.transition.phase, "detected")
                self.assertEqual(detected.episode.next_notification_at, utc_text(BASE_TS + 900))

    def test_input_quality_explicit_bad_repeats_at_ten_minutes_and_nodata_closes_once(self) -> None:
        policy = DEFAULT_INCIDENT_POLICIES["youtube_input_quality"]
        detected = evaluate_current(
            current(domain="youtube_input_quality", state="bad", reason="input_quality_health_warning"),
            policy,
            now_at=utc_text(BASE_TS),
            active_episode=None,
            pending_candidate=None,
        )
        repeated = evaluate_current(
            current(
                domain="youtube_input_quality",
                state="bad",
                observed_ts=BASE_TS + 600,
                reason="input_quality_health_warning",
                marker="repeat",
            ),
            policy,
            now_at=utc_text(BASE_TS + 600),
            active_episode=detected.episode,
            pending_candidate=None,
        )
        cleared = evaluate_current(
            current(
                domain="youtube_input_quality",
                state="unknown",
                observed_ts=BASE_TS + 660,
                reason="input_quality_health_nodata",
                marker="nodata",
            ),
            policy,
            now_at=utc_text(BASE_TS + 660),
            active_episode=repeated.episode,
            pending_candidate=None,
        )
        duplicate = evaluate_current(
            current(
                domain="youtube_input_quality",
                state="unknown",
                observed_ts=BASE_TS + 720,
                reason="input_quality_health_nodata",
                marker="still-nodata",
            ),
            policy,
            now_at=utc_text(BASE_TS + 720),
            active_episode=None,
            pending_candidate=None,
        )
        self.assertEqual(detected.transition.phase, "detected")
        self.assertEqual(repeated.transition.phase, "repeat")
        self.assertEqual(cleared.transition.phase, "recovered")
        self.assertIsNone(duplicate.transition)

    def test_maintenance_suppresses_new_episode_without_claiming_good(self) -> None:
        evaluation = evaluate_current(
            current(state="bad", payload={"maintenance": True}),
            DEFAULT_INCIDENT_POLICIES["delivery"],
            now_at=utc_text(BASE_TS),
            active_episode=None,
            pending_candidate=None,
        )
        self.assertIsNone(evaluation.transition)
        self.assertIsNone(evaluation.episode)

    def test_planned_rollout_is_info_and_never_slack(self) -> None:
        evaluation = evaluate_current(
            current(state="bad", payload={"planned_rollout": True}),
            DEFAULT_INCIDENT_POLICIES["delivery"],
            now_at=utc_text(BASE_TS),
            active_episode=None,
            pending_candidate=None,
        )
        routes = RoutePolicy().routes(evaluation.transition, evaluation.episode)
        self.assertEqual(evaluation.transition.severity, "info")
        self.assertEqual(routes, ("discord",))

    def test_bad_state_after_planned_rollout_escalates_immediately(self) -> None:
        policy = DEFAULT_INCIDENT_POLICIES["delivery"]
        planned = evaluate_current(
            current(state="bad", payload={"planned_rollout": True}),
            policy,
            now_at=utc_text(BASE_TS),
            active_episode=None,
            pending_candidate=None,
        )
        unplanned = evaluate_current(
            current(state="bad", observed_ts=BASE_TS + 60, marker="unplanned"),
            policy,
            now_at=utc_text(BASE_TS + 60),
            active_episode=planned.episode,
            pending_candidate=None,
        )
        self.assertEqual(unplanned.transition.phase, "repeat")
        self.assertEqual(unplanned.transition.severity, "critical")


class IncidentServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.repository = MonitoringRepository(Path(self.temp.name) / "db.sqlite3")
        self.repository.initialize(applied_at=utc_text(BASE_TS))
        self.service = IncidentService(self.repository, DEFAULT_INCIDENT_POLICIES, RoutePolicy())

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_duplicate_snapshot_does_not_create_second_transition_or_intent(self) -> None:
        item = current(state="bad")
        first = self.service.process(item, now_at=utc_text(BASE_TS))
        second = self.service.process(item, now_at=utc_text(BASE_TS + 60))
        self.assertFalse(first.duplicate)
        self.assertTrue(second.duplicate)
        self.assertEqual([item.phase for item in self.repository.transitions()], ["detected"])
        self.assertEqual(len(self.repository.intents()), 2)  # critical -> Discord and Slack

    def test_unprocessed_future_current_is_never_backdated_after_clock_rollback(self) -> None:
        item = current(
            state="bad",
            observed_ts=BASE_TS + 600,
            reduced_ts=BASE_TS + 600,
            marker="saved-before-crash",
        )
        self.repository.save_current(item)
        result = self.service.process(item, now_at=utc_text(BASE_TS))
        self.assertIsNotNone(result.transition)
        self.assertEqual(result.transition.occurred_at, utc_text(BASE_TS + 600))
        self.assertEqual(result.episode.last_transition_at, utc_text(BASE_TS + 600))

    def test_recovery_is_emitted_exactly_once_and_old_rolling_value_does_not_reopen(self) -> None:
        self.service.process(current(state="bad"), now_at=utc_text(BASE_TS))
        recovered = current(state="good", observed_ts=BASE_TS + 60, marker="recovered")
        first = self.service.process(recovered, now_at=utc_text(BASE_TS + 60))
        duplicate = self.service.process(recovered, now_at=utc_text(BASE_TS + 61))
        historical_low = current(
            state="good",
            observed_ts=BASE_TS + 120,
            marker="historical-low",
            payload={"historical_sli_pct": 89.387},
        )
        after = self.service.process(historical_low, now_at=utc_text(BASE_TS + 120))
        self.assertEqual(first.transition.phase, "recovered")
        self.assertTrue(duplicate.duplicate)
        self.assertIsNone(after.transition)
        self.assertEqual([item.phase for item in self.repository.transitions()], ["detected", "recovered"])


class ReplayFixtureTests(unittest.TestCase):
    def test_known_2026_08_11_cases(self) -> None:
        paths = sorted(FIXTURES.glob("2026-08-11_*.json"))
        self.assertEqual(len(paths), 6)
        for path in paths:
            with self.subTest(path=path.name):
                fixture = json.loads(path.read_text(encoding="utf-8"))
                self.assertEqual(fixture["schema"], "monitoring_v4.replay_fixture.v1")
                domain = fixture["domain"]
                steps = []
                for raw in fixture["steps"]:
                    offset = int(raw["offset_sec"])
                    steps.append(
                        ReplayStep(
                            current=current(
                                domain=domain,
                                state=raw["state"],
                                observed_ts=BASE_TS + offset,
                                reason=raw["reason"],
                                marker=raw["marker"],
                                payload=raw.get("payload", {}),
                            ),
                            now_at=utc_text(BASE_TS + offset),
                        )
                    )
                transitions = replay(steps, DEFAULT_INCIDENT_POLICIES[domain])
                self.assertEqual([item.phase for item in transitions], fixture["expected_phases"])
                if "expected_severities" in fixture:
                    self.assertEqual(
                        [item.severity for item in transitions],
                        fixture["expected_severities"],
                    )


if __name__ == "__main__":
    unittest.main()
