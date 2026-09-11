from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.incidents.policy import DEFAULT_INCIDENT_POLICIES
from stream_monitoring_v4.incidents.service import IncidentService
from stream_monitoring_v4.notifications.converter import convert_v3_rows, reverse_to_v3_rows
from stream_monitoring_v4.notifications.dispatcher import NotificationDispatcher
from stream_monitoring_v4.notifications.providers.base import DeliveryResponse
from stream_monitoring_v4.notifications.providers.recording import RecordingProvider, ScriptedProvider
from stream_monitoring_v4.notifications.routing import RoutePolicy
from stream_monitoring_v4.storage.repository import MonitoringRepository

from tests.helpers import BASE_TS, current


class _InvalidRoutePolicy:
    revision = "invalid-route-policy-test"

    def routes(self, transition, episode):
        return ("INVALID ROUTE",)


class NotificationIntentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.repository = MonitoringRepository(Path(self.temp.name) / "db.sqlite3")
        self.repository.initialize(applied_at=utc_text(BASE_TS))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_transition_episode_and_intents_are_atomic(self) -> None:
        service = IncidentService(
            self.repository,
            DEFAULT_INCIDENT_POLICIES,
            _InvalidRoutePolicy(),
        )
        with self.assertRaisesRegex(ValueError, "route"):
            service.process(current(state="bad"), now_at=utc_text(BASE_TS))
        self.assertEqual(self.repository.transitions(), [])
        self.assertEqual(self.repository.intents(), [])
        self.assertIsNone(self.repository.active_episode("delivery"))

    def test_provider_response_contract_rejects_scalar_coercion(self) -> None:
        malformed = (
            (1, 204, "sent", 0, False),
            (True, "204", "sent", 0, False),
            (True, 204, 7, 0, False),
            (True, 204, "sent", "0", False),
            (True, 204, "sent", 0, 0),
        )
        for values in malformed:
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    DeliveryResponse(*values)

    def test_critical_routes_to_discord_and_slack_immediately(self) -> None:
        service = IncidentService(self.repository, DEFAULT_INCIDENT_POLICIES, RoutePolicy())
        result = service.process(current(state="bad"), now_at=utc_text(BASE_TS))
        self.assertEqual({item.route for item in result.intents}, {"discord", "slack"})

    def test_warning_slack_gate_and_recovery_routes_match_sustained_episode(self) -> None:
        service = IncidentService(self.repository, DEFAULT_INCIDENT_POLICIES, RoutePolicy())
        detected = service.process(
            current(domain="youtube_lifecycle", state="bad"),
            now_at=utc_text(BASE_TS),
        )
        self.assertEqual([item.route for item in detected.intents], ["discord"])
        for offset in (600, 1200):
            repeated = service.process(
                current(
                    domain="youtube_lifecycle",
                    state="bad",
                    observed_ts=BASE_TS + offset,
                    marker=f"bad-{offset}",
                ),
                now_at=utc_text(BASE_TS + offset),
            )
            self.assertEqual([item.route for item in repeated.intents], ["discord"])
        escalated = service.process(
            current(
                domain="youtube_lifecycle",
                state="bad",
                observed_ts=BASE_TS + 1800,
                marker="bad-1800",
            ),
            now_at=utc_text(BASE_TS + 1800),
        )
        self.assertEqual({item.route for item in escalated.intents}, {"discord", "slack"})
        recovered = service.process(
            current(
                domain="youtube_lifecycle",
                state="good",
                observed_ts=BASE_TS + 1860,
                marker="recovered",
            ),
            now_at=utc_text(BASE_TS + 1860),
        )
        self.assertEqual({item.route for item in recovered.intents}, {"discord", "slack"})
        self.assertTrue(all("復旧通知" in item.content for item in recovered.intents))
        self.assertTrue(
            all("FFmpeg自己再起動イベント" not in item.content for item in recovered.intents)
        )

    def test_renderer_uses_jst_and_never_claims_automatic_restart(self) -> None:
        service = IncidentService(self.repository, DEFAULT_INCIDENT_POLICIES, RoutePolicy())
        result = service.process(current(state="bad"), now_at=utc_text(BASE_TS))
        for intent in result.intents:
            self.assertIn("JST", intent.content)
            self.assertIn("human_review_no_automatic_restart", intent.content)

    def test_shadow_intents_remain_quarantined_even_if_provider_is_configured(self) -> None:
        service = IncidentService(
            self.repository,
            DEFAULT_INCIDENT_POLICIES,
            RoutePolicy(),
        )
        result = service.process(current(state="bad"), now_at=utc_text(BASE_TS))
        provider = RecordingProvider()
        summary = NotificationDispatcher(
            self.repository,
            {"discord": provider, "slack": provider},
            owner="accidental-writer",
        ).dispatch_once(now_ts=BASE_TS)

        self.assertEqual(summary.attempted, 0)
        self.assertEqual(provider.intents, [])
        self.assertTrue(
            all(
                self.repository.intent_delivery_metadata(intent.intent_id)["mode"]
                == "shadow"
                for intent in result.intents
            )
        )
        self.assertEqual(
            self.repository.delivery_outbox_counts()["shadow_quarantined"],
            len(result.intents),
        )


class DispatcherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.repository = MonitoringRepository(Path(self.temp.name) / "db.sqlite3")
        self.repository.initialize(applied_at=utc_text(BASE_TS))
        self.delivery_epoch = self.repository.activate_delivery_epoch(
            cutover_at=utc_text(BASE_TS),
            activated_at=utc_text(BASE_TS),
            policy_revision="test-delivery-cutover-r1",
            writer_identity="test-writer",
            note="isolated dispatcher test",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _enqueue_warning(self) -> None:
        service = IncidentService(
            self.repository,
            DEFAULT_INCIDENT_POLICIES,
            RoutePolicy(),
            delivery_epoch_id=self.delivery_epoch,
        )
        result = service.process(
            current(domain="youtube_lifecycle", state="bad"),
            now_at=utc_text(BASE_TS),
        )
        self.assertEqual(len(result.intents), 1)

    def test_success_is_not_sent_twice(self) -> None:
        service = IncidentService(
            self.repository,
            DEFAULT_INCIDENT_POLICIES,
            RoutePolicy(),
            delivery_epoch_id=self.delivery_epoch,
        )
        service.process(current(state="bad"), now_at=utc_text(BASE_TS))
        discord = RecordingProvider()
        slack = RecordingProvider()
        dispatcher = NotificationDispatcher(
            self.repository,
            {"discord": discord, "slack": slack},
            owner="writer-a",
        )
        first = dispatcher.dispatch_once(now_ts=BASE_TS)
        second = dispatcher.dispatch_once(now_ts=BASE_TS + 1)
        self.assertEqual((first.attempted, first.sent, first.failed, first.pending), (2, 2, 0, 0))
        self.assertEqual((second.attempted, second.sent), (0, 0))
        self.assertEqual(len(discord.intents), 1)
        self.assertEqual(len(slack.intents), 1)

    def test_second_writer_cannot_dispatch_while_lease_is_held(self) -> None:
        self._enqueue_warning()
        self.assertTrue(
            self.repository.acquire_lease(
                "notification_dispatcher",
                "writer-a",
                now_ts=BASE_TS,
                ttl_sec=300,
            )
        )
        dispatcher = NotificationDispatcher(
            self.repository,
            {"discord": RecordingProvider()},
            owner="writer-b",
        )
        result = dispatcher.dispatch_once(now_ts=BASE_TS + 1)
        self.assertFalse(result.lease_acquired)
        self.assertEqual(result.attempted, 0)
        self.assertEqual(result.pending, 1)

    def test_429_respects_retry_after(self) -> None:
        self._enqueue_warning()
        provider = ScriptedProvider(
            [
                DeliveryResponse(False, 429, "rate_limited", 10),
                DeliveryResponse(True, 204, "sent"),
            ]
        )
        dispatcher = NotificationDispatcher(
            self.repository,
            {"discord": provider},
            owner="writer-a",
        )
        first = dispatcher.dispatch_once(now_ts=BASE_TS)
        early = dispatcher.dispatch_once(now_ts=BASE_TS + 9)
        retry = dispatcher.dispatch_once(now_ts=BASE_TS + 10)
        self.assertEqual((first.failed, first.pending), (1, 1))
        self.assertEqual(early.attempted, 0)
        self.assertEqual((retry.attempted, retry.sent, retry.pending), (1, 1, 0))

    def test_5xx_uses_exponential_backoff(self) -> None:
        self._enqueue_warning()
        provider = ScriptedProvider(
            [DeliveryResponse(False, 503, "upstream", 0), DeliveryResponse(True, 204, "sent")]
        )
        dispatcher = NotificationDispatcher(
            self.repository,
            {"discord": provider},
            owner="writer-a",
        )
        dispatcher.dispatch_once(now_ts=BASE_TS)
        self.assertEqual(dispatcher.dispatch_once(now_ts=BASE_TS + 1).attempted, 0)
        self.assertEqual(dispatcher.dispatch_once(now_ts=BASE_TS + 2).sent, 1)

    def test_timeout_is_uncertain_and_is_not_automatically_retried(self) -> None:
        self._enqueue_warning()
        provider = ScriptedProvider([TimeoutError("injected"), DeliveryResponse(True, 204, "sent")])
        dispatcher = NotificationDispatcher(
            self.repository,
            {"discord": provider},
            owner="writer-a",
        )
        first = dispatcher.dispatch_once(now_ts=BASE_TS)
        state = self.repository.delivery_state(self.repository.intents()[0].intent_id)
        self.assertEqual(first.failed, 1)
        self.assertEqual(first.uncertain, 1)
        self.assertEqual(state["attempts"][0]["state"], "uncertain")
        self.assertIn("delivery_uncertain", state["results"][0]["detail"])
        self.assertEqual(dispatcher.dispatch_once(now_ts=BASE_TS + 59).attempted, 0)
        self.assertEqual(dispatcher.dispatch_once(now_ts=BASE_TS + 600).sent, 0)

    def test_invalid_provider_response_is_uncertain_and_not_retried(self) -> None:
        class InvalidProvider:
            def send(self, intent):
                del intent
                return {"success": True}

        self._enqueue_warning()
        dispatcher = NotificationDispatcher(
            self.repository,
            {"discord": InvalidProvider()},
            owner="writer-a",
        )
        result = dispatcher.dispatch_once(now_ts=BASE_TS)
        state = self.repository.delivery_state(self.repository.intents()[0].intent_id)
        self.assertEqual((result.failed, result.uncertain, result.pending), (1, 1, 0))
        self.assertEqual(state["attempts"][0]["state"], "uncertain")
        self.assertEqual(
            state["results"][0]["detail"],
            "provider_invalid_response_delivery_uncertain",
        )
        self.assertEqual(dispatcher.dispatch_once(now_ts=BASE_TS + 600).attempted, 0)

    def test_permanent_400_is_not_retried(self) -> None:
        self._enqueue_warning()
        provider = ScriptedProvider([DeliveryResponse(False, 400, "bad_request")])
        dispatcher = NotificationDispatcher(
            self.repository,
            {"discord": provider},
            owner="writer-a",
        )
        result = dispatcher.dispatch_once(now_ts=BASE_TS)
        self.assertEqual((result.failed, result.pending), (1, 0))
        self.assertEqual(dispatcher.dispatch_once(now_ts=BASE_TS + 600).attempted, 0)

    def test_dangling_attempt_becomes_uncertain_without_retry(self) -> None:
        self._enqueue_warning()
        intent = self.repository.intents()[0]
        token = self.repository.acquire_fenced_lease(
            "notification_dispatcher",
            "crashed-writer",
            now_ts=BASE_TS,
            ttl_sec=180,
        )
        self.assertIsNotNone(token)
        self.repository.begin_delivery_attempt(
            intent.intent_id,
            "crashed-writer",
            lease_name="notification_dispatcher",
            fence_token=token,
            now_ts=BASE_TS,
        )
        self.repository.release_lease(
            "notification_dispatcher",
            "crashed-writer",
            token,
        )
        provider = RecordingProvider()
        dispatcher = NotificationDispatcher(
            self.repository,
            {"discord": provider},
            owner="replacement-writer",
            attempt_timeout_sec=120,
            lease_ttl_sec=180,
        )
        self.assertEqual(dispatcher.dispatch_once(now_ts=BASE_TS + 119).attempted, 0)
        self.assertEqual(dispatcher.dispatch_once(now_ts=BASE_TS + 120).sent, 0)
        state = self.repository.delivery_state(intent.intent_id)
        self.assertEqual(len(state["attempts"]), 1)
        self.assertEqual(state["attempts"][0]["state"], "uncertain")

    def test_hard_deadline_and_expired_lease_cannot_duplicate_delivery(self) -> None:
        class SlowProvider:
            def __init__(self) -> None:
                self.calls = 0

            def send(self, intent):
                del intent
                self.calls += 1
                time.sleep(2.5)
                return DeliveryResponse(True, 204, "late_success")

        self._enqueue_warning()
        provider = SlowProvider()
        first_writer = NotificationDispatcher(
            self.repository,
            {"discord": provider},
            owner="writer-a",
            attempt_timeout_sec=1,
            lease_ttl_sec=2,
        )
        second_writer = NotificationDispatcher(
            self.repository,
            {"discord": provider},
            owner="writer-b",
            attempt_timeout_sec=1,
            lease_ttl_sec=2,
        )
        first = first_writer.dispatch_once(now_ts=BASE_TS)
        second = second_writer.dispatch_once(now_ts=BASE_TS + 2)
        self.assertEqual(first.uncertain, 1)
        self.assertEqual(second.attempted, 0)
        self.assertEqual(provider.calls, 1)

    def test_lost_fence_is_committed_as_uncertain_before_error(self) -> None:
        self._enqueue_warning()
        intent = self.repository.intents()[0]
        token = self.repository.acquire_fenced_lease(
            "notification_dispatcher",
            "writer-a/instance-a",
            now_ts=BASE_TS,
            ttl_sec=2,
        )
        self.assertIsNotNone(token)
        attempt = self.repository.begin_delivery_attempt(
            intent.intent_id,
            "writer-a/instance-a",
            lease_name="notification_dispatcher",
            fence_token=token,
            now_ts=BASE_TS,
        )
        replacement_token = self.repository.acquire_fenced_lease(
            "notification_dispatcher",
            "writer-b/instance-b",
            now_ts=BASE_TS + 2,
            ttl_sec=60,
        )
        self.assertIsNotNone(replacement_token)

        with self.assertRaisesRegex(RuntimeError, "fence was lost"):
            self.repository.finish_delivery_attempt(
                attempt,
                success=True,
                status_code=204,
                detail="late-success-after-lease-loss",
                retry_after_sec=0,
                now_ts=BASE_TS + 2,
                outcome_state="succeeded",
            )

        state = self.repository.delivery_state(intent.intent_id)
        self.assertEqual(state["attempts"][0]["state"], "uncertain")
        self.assertEqual(
            state["attempts"][0]["state_detail"],
            "lease_fence_lost_before_result",
        )
        self.assertEqual(state["results"], [])

    def test_lease_must_outlive_attempt_timeout(self) -> None:
        with self.assertRaisesRegex(ValueError, "must exceed"):
            NotificationDispatcher(
                self.repository,
                {},
                owner="invalid",
                attempt_timeout_sec=120,
                lease_ttl_sec=120,
            )

    def test_same_owner_label_gets_distinct_instance_lease_identity(self) -> None:
        first = NotificationDispatcher(self.repository, {}, owner="notifier", instance_id="instance-a")
        second = NotificationDispatcher(self.repository, {}, owner="notifier", instance_id="instance-b")
        self.assertNotEqual(first.lease_owner, second.lease_owner)


class ConverterTests(unittest.TestCase):
    def test_v3_conversion_reports_rejections_and_collisions(self) -> None:
        good = {
            "message_id": "legacy-1",
            "phase": "status",
            "route": "discord",
            "content": "legacy content",
            "created_ts_utc": "2026-08-11T12:00:00Z",
        }
        report = convert_v3_rows([good, dict(good), {"message_id": "broken"}])
        self.assertEqual(len(report.converted), 1)
        self.assertEqual(report.converted[0].phase, "repeat")
        self.assertEqual(report.collisions, ("legacy-1",))
        self.assertEqual(report.rejected, ("row=2:missing_message_id_phase_or_content",))

    def test_reverse_dry_run_preserves_route_phase_and_time(self) -> None:
        report = convert_v3_rows(
            [
                {
                    "message_id": "legacy-recovery",
                    "phase": "recovered",
                    "route": "slack",
                    "content": "recovered once",
                    "created_ts_utc": "2026-08-11T12:00:00Z",
                }
            ]
        )
        rows = reverse_to_v3_rows(report.converted)
        self.assertEqual(rows[0]["phase"], "recovered")
        self.assertEqual(rows[0]["route"], "slack")
        self.assertEqual(rows[0]["status"], "pending")
        self.assertEqual(rows[0]["created_ts_utc"], "2026-08-11T12:00:00Z")


if __name__ == "__main__":
    unittest.main()
