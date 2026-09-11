from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.incident import IncidentEpisode, IncidentTransition
from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.runtime_evidence import RuntimeLifecycleEvent
from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.incidents.lifecycle import RuntimeLifecycleIncidentService
from stream_monitoring_v4.notifications.routing import RoutePolicy
from stream_monitoring_v4.storage.repository import MonitoringRepository

from tests.helpers import BASE_TS, current


class RuntimeLifecycleIncidentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.repository = MonitoringRepository(Path(self.temp.name) / "monitoring.sqlite3")
        self.repository.initialize(applied_at=utc_text(BASE_TS))
        self.service = RuntimeLifecycleIncidentService(self.repository, RoutePolicy())

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _observation() -> ObservationEnvelope:
        event = RuntimeLifecycleEvent(
            event_id="evt-scheduled-1",
            event_type="ffmpeg_auto_recovered",
            opened_at=utc_text(BASE_TS),
            recovered_at=utc_text(BASE_TS + 5),
            run_id="run-1",
            restart_count=1,
            exit_code=1,
            delay_sec=5,
            scheduled_event_id="evt-scheduled-1",
            recovered_event_id="evt-started-1",
        )
        return ObservationEnvelope.create(
            domain="delivery",
            source="runtime_lifecycle_events",
            source_event_id=event.event_id,
            source_generation="run-1:1",
            evidence_role="historical",
            status="not_applicable",
            reason_code="ffmpeg_child_auto_recovered",
            observed_at=event.recovered_at,
            received_at=utc_text(BASE_TS + 20),
            freshness_limit_sec=86400,
            producer_revision="test-runtime-lifecycle-r1",
            payload=event.to_dict(),
        )

    def test_completed_edge_creates_one_recovery_only_episode_after_current_good(self) -> None:
        observation = self._observation()
        delivery = current(state="good", observed_ts=BASE_TS + 20, reduced_ts=BASE_TS + 20)
        first = self.service.process(
            observation,
            delivery_current=delivery,
            now_at=utc_text(BASE_TS + 20),
        )
        self.assertIsNotNone(first)
        assert first is not None
        self.assertFalse(first.duplicate)
        self.assertEqual(first.transition.phase, "recovered")
        self.assertEqual(first.episode.status, "closed")
        self.assertEqual({item.route for item in first.intents}, {"discord", "slack"})
        self.assertTrue(
            all("FFmpeg自己再起動イベント" in item.subject for item in first.intents)
        )
        self.assertTrue(
            all("FFmpeg自己再起動イベント" in item.content for item in first.intents)
        )
        self.assertTrue(
            all(item.template_revision == "monitoring-v4-ja-r5.2" for item in first.intents)
        )
        self.assertIn("ffmpeg_child_self_recovery", first.intents[0].content)
        self.assertEqual([item.phase for item in self.repository.transitions()], ["recovered"])

        later_delivery = current(
            state="good",
            observed_ts=BASE_TS + 21,
            reduced_ts=BASE_TS + 21,
            marker="later-delivery-snapshot",
        )
        second = self.service.process(
            observation,
            delivery_current=later_delivery,
            now_at=utc_text(BASE_TS + 21),
        )
        self.assertIsNotNone(second)
        assert second is not None
        self.assertTrue(second.duplicate)
        self.assertEqual(len(self.repository.transitions()), 1)
        self.assertEqual(len(self.repository.intents()), 2)

    def test_lifecycle_contract_rejects_numeric_coercion_and_extra_fields(self) -> None:
        payload = RuntimeLifecycleEvent.from_dict(self._observation().payload).to_dict()
        for field, replacement in (("restart_count", True), ("delay_sec", 5.0)):
            malformed = dict(payload)
            malformed[field] = replacement
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    RuntimeLifecycleEvent.from_dict(malformed)
        malformed = dict(payload)
        malformed["unexpected"] = "ignored-before"
        with self.assertRaises(ValueError):
            RuntimeLifecycleEvent.from_dict(malformed)

    def test_preexisting_snapshot_scoped_transition_suppresses_another_recovery(self) -> None:
        observation = self._observation()
        event = RuntimeLifecycleEvent.from_dict(observation.payload)
        first_delivery = current(state="good", observed_ts=BASE_TS + 20)
        episode = IncidentEpisode(
            episode_id=self.service._episode_id(observation),
            domain="delivery",
            status="closed",
            severity="critical",
            opened_at=event.opened_at,
            last_bad_at=event.opened_at,
            closed_at=event.recovered_at,
            bad_samples=1,
            unknown_samples=0,
            last_transition_at=event.recovered_at,
            next_notification_at="",
            policy_revision="monitoring-v4-runtime-lifecycle-r4.1",
            summary="legacy snapshot-scoped transition",
            reason_codes=("ffmpeg_child_auto_recovered",),
        )
        legacy = IncidentTransition.create(
            episode_id=episode.episode_id,
            domain="delivery",
            phase="recovered",
            severity="info",
            occurred_at=event.recovered_at,
            current_snapshot_id=first_delivery.snapshot_id,
            summary=episode.summary,
            reason_codes=episode.reason_codes,
        )
        with self.repository.transaction() as connection:
            self.repository.append_current_snapshot(first_delivery, connection=connection)
            self.repository.save_episode(episode, connection=connection)
            self.repository.append_transition(legacy, connection=connection)

        result = self.service.process(
            observation,
            delivery_current=current(
                state="good",
                observed_ts=BASE_TS + 30,
                marker="different-current-snapshot",
            ),
            now_at=utc_text(BASE_TS + 30),
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertTrue(result.duplicate)
        self.assertIsNone(result.transition)
        self.assertEqual(
            [item.transition_id for item in self.repository.transitions()],
            [legacy.transition_id],
        )
        self.assertEqual(self.repository.intents(), [])

    def test_event_waits_for_post_recovery_good_current_and_expires_boundedly(self) -> None:
        observation = self._observation()
        stale_good = current(state="good", observed_ts=BASE_TS, reduced_ts=BASE_TS + 20)
        bad = current(state="bad", observed_ts=BASE_TS + 20, reduced_ts=BASE_TS + 20)
        self.assertIsNone(
            self.service.process(
                observation,
                delivery_current=stale_good,
                now_at=utc_text(BASE_TS + 20),
            )
        )
        self.assertIsNone(
            self.service.process(
                observation,
                delivery_current=bad,
                now_at=utc_text(BASE_TS + 20),
            )
        )
        self.assertIsNone(
            self.service.process(
                observation,
                delivery_current=current(
                    state="good", observed_ts=BASE_TS + 1900, reduced_ts=BASE_TS + 1900
                ),
                now_at=utc_text(BASE_TS + 1900),
            )
        )
        self.assertEqual(self.repository.transitions(), [])

    def test_sampled_current_recovery_suppresses_duplicate_edge_recovery(self) -> None:
        delivery = current(state="good", observed_ts=BASE_TS + 20, reduced_ts=BASE_TS + 20)
        episode = IncidentEpisode(
            episode_id=stable_id("inc", "ordinary-delivery-episode"),
            domain="delivery",
            status="closed",
            severity="critical",
            opened_at=utc_text(BASE_TS + 1),
            last_bad_at=utc_text(BASE_TS + 3),
            closed_at=utc_text(BASE_TS + 10),
            bad_samples=1,
            unknown_samples=0,
            last_transition_at=utc_text(BASE_TS + 10),
            next_notification_at="",
            policy_revision="ordinary-current-policy-r1",
            summary="sampled delivery recovered",
            reason_codes=("current_bad_evidence",),
        )
        transition = IncidentTransition.create(
            episode_id=episode.episode_id,
            domain="delivery",
            phase="recovered",
            severity="info",
            occurred_at=episode.closed_at,
            current_snapshot_id=delivery.snapshot_id,
            summary=episode.summary,
            reason_codes=episode.reason_codes,
        )
        with self.repository.transaction() as connection:
            self.repository.append_current_snapshot(delivery, connection=connection)
            self.repository.save_episode(episode, connection=connection)
            self.repository.append_transition(transition, connection=connection)

        result = self.service.process(
            self._observation(),
            delivery_current=delivery,
            now_at=utc_text(BASE_TS + 20),
        )

        self.assertIsNone(result)
        self.assertEqual([item.transition_id for item in self.repository.transitions()], [transition.transition_id])
        self.assertEqual(self.repository.intents(), [])


if __name__ == "__main__":
    unittest.main()
