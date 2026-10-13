from __future__ import annotations

import sys
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ops.relay.twitch_session import (
    Action, Binding, BroadcastFacts, BroadcastStatus, Clock, Outcome, Phase,
    RenewalPolicy, RenewalState, RouteContext, Session, WriterFacts, WriterStatus,
    advance, next_daily_renewal_at, parse_get_streams, twitch_forward_update,
)


class TwitchSessionTests(unittest.TestCase):
    origin = datetime(2026, 10, 10, tzinfo=UTC)
    # Origin is 09:00 JST; the next midnight is 15 hours later, at 15:00 UTC.
    due = 15 * 3600
    old = Session("111", origin)

    def clock(self, seconds: float) -> Clock:
        return Clock(self.origin + timedelta(seconds=seconds), seconds, "boot-a")

    def context(self, seconds: float, **changes) -> RouteContext:
        c = RouteContext("123", "PRIMARY", 7, 2, "relay-a", "boot-a", seconds + 15,
                         input_fresh=True, maintenance_authorized=True, effects_enabled=True)
        return replace(c, **changes)

    def broadcast(self, seconds: float, status=BroadcastStatus.LIVE, session=None) -> BroadcastFacts:
        return BroadcastFacts("123", self.clock(seconds).utc, status,
                              (session or self.old) if status == BroadcastStatus.LIVE else None)

    def writer(self, seconds: float, status=WriterStatus.RUNNING, generation=4, **changes) -> WriterFacts:
        w = WriterFacts(self.clock(seconds).utc, "relay-a", generation, status,
                        0.5 if status == WriterStatus.RUNNING else None)
        return replace(w, **changes)

    def tick(self, state: RenewalState, seconds: float, **changes):
        args = dict(now=self.clock(seconds), context=self.context(seconds),
                    broadcast=self.broadcast(seconds), writer=self.writer(seconds))
        args.update(changes)
        return advance(state, **args)

    def stop_request(self):
        return self.tick(RenewalState(), self.due)

    def ack(self, request, seconds, status="STOPPED") -> Outcome:
        c = request.command
        return Outcome(c.action, c.request_id, c.binding, status, self.clock(seconds).utc)

    def stopped(self):
        request = self.stop_request()
        d = self.tick(request.state, self.due + 1, outcome=self.ack(request, self.due + 1),
                      writer=self.writer(self.due + 1, WriterStatus.STOPPED))
        self.assertEqual(d.state.phase, Phase.WAIT_OFFLINE)
        return request, d

    def started_request(self):
        _, stopped = self.stopped()
        seconds = self.due + 121
        d = self.tick(stopped.state, seconds, broadcast=self.broadcast(seconds, BroadcastStatus.OFFLINE),
                      writer=self.writer(seconds, WriterStatus.STOPPED))
        self.assertEqual(d.command.action, Action.START_TWITCH)
        return d

    def verifying(self):
        request = self.started_request()
        seconds = self.due + 122
        d = self.tick(request.state, seconds, outcome=self.ack(request, seconds, "STARTED"),
                      writer=self.writer(seconds, generation=5))
        self.assertEqual(d.state.phase, Phase.VERIFYING)
        return request, d

    def test_jst_midnight_boundary_only_requests_twitch_stop(self):
        below = self.tick(RenewalState(), self.due - 1)
        self.assertIsNone(below.command)
        due = self.tick(below.state, self.due)
        self.assertEqual(due.command.action, Action.STOP_TWITCH)
        self.assertEqual(due.command.provider, "twitch")
        self.assertEqual(due.command.binding.route_generation, 7)
        self.assertEqual(due.reason, "TWITCH_DAILY_JST_RENEWAL_DUE")
        self.assertNotIn("youtube", due.command.__dict__)

    def test_one_time_validation_requires_explicit_boolean_authorization(self):
        for value in (False, None, "true", 1):
            with self.subTest(value=value):
                self.assertIsNone(self.tick(RenewalState(),1,validation_authorized=value).command)
        decision=self.tick(RenewalState(),1,validation_authorized=True)
        self.assertEqual(decision.command.action,Action.STOP_TWITCH)
        self.assertEqual(decision.reason,"TWITCH_AUTHORIZED_VALIDATION_DUE")

    def test_validation_does_not_override_lease_or_maintenance_admission(self):
        for change in ({"maintenance_authorized":False},{"effects_enabled":False},{"lease_expires_elapsed":0}):
            with self.subTest(change=change):
                decision=self.tick(RenewalState(),1,context=self.context(1,**change),validation_authorized=True)
                self.assertIsNone(decision.command)

    def test_duplicate_tick_before_commit_has_same_id_after_commit_has_no_command(self):
        a, b = self.stop_request(), self.stop_request()
        self.assertEqual(a.command, b.command)
        after = self.tick(a.state, self.due + 1)
        self.assertIsNone(after.command)
        self.assertEqual(after.state.phase, Phase.WAIT_STOP)

    def test_equal_provider_started_at_with_different_timezone_has_same_request_id(self):
        utc = self.stop_request()
        offset = Session("111", datetime.fromisoformat("2026-10-10T09:00:00+09:00"))
        d = self.tick(RenewalState(), self.due, broadcast=self.broadcast(self.due, session=offset))
        self.assertEqual(d.command, utc.command)

    def test_accepted_ack_and_stop_ack_with_running_writer_do_not_start(self):
        r = self.stop_request()
        for status in ("ACCEPTED", "STOPPED"):
            with self.subTest(status=status):
                d = self.tick(r.state, self.due + 1, outcome=self.ack(r, self.due + 1, status))
                self.assertEqual(d.state.phase, Phase.WAIT_STOP)
                self.assertIsNone(d.command)

    def test_stopped_writer_snapshot_before_ack_does_not_confirm_stop(self):
        r = self.stop_request()
        d = self.tick(r.state, self.due + 2, outcome=self.ack(r, self.due + 2),
                      writer=self.writer(self.due + 1, WriterStatus.STOPPED))
        self.assertEqual(d.state.phase, Phase.WAIT_STOP)

    def test_stale_wrong_request_or_wrong_generation_ack_is_ignored(self):
        r = self.stop_request()
        ack = self.ack(r, self.due + 1)
        for bad in (replace(ack, request_id="old-request"),
                    replace(ack, observed_at=self.clock(self.due - 1).utc),
                    replace(ack, binding=replace(ack.binding, route_generation=6)),
                    replace(ack, binding=replace(ack.binding, writer_generation=3)),
                    replace(ack, binding=replace(ack.binding, relay_instance_id="relay-old"))):
            with self.subTest(ack=bad):
                d = self.tick(r.state, self.due + 1, outcome=bad,
                              writer=self.writer(self.due + 1, WriterStatus.STOPPED))
                self.assertEqual(d.state.phase, Phase.WAIT_STOP)

    def test_offline_hold_and_provider_offline_are_both_required(self):
        _, stopped = self.stopped()
        early = self.tick(stopped.state, self.due + 120,
                          broadcast=self.broadcast(self.due + 120, BroadcastStatus.OFFLINE),
                          writer=self.writer(self.due + 120, WriterStatus.STOPPED))
        self.assertIsNone(early.command)
        held = self.tick(early.state, self.due + 121,
                         writer=self.writer(self.due + 121, WriterStatus.STOPPED))
        self.assertIsNone(held.command)
        offline = self.tick(held.state, self.due + 122,
                            broadcast=self.broadcast(self.due + 122, BroadcastStatus.OFFLINE),
                            writer=self.writer(self.due + 122, WriterStatus.STOPPED))
        self.assertEqual(offline.command.action, Action.START_TWITCH)

    def test_stale_offline_and_api_unknown_never_authorize_restart(self):
        _, stopped = self.stopped()
        for facts in (self.broadcast(self.due + 1, BroadcastStatus.OFFLINE),
                      self.broadcast(self.due + 121, BroadcastStatus.UNKNOWN)):
            d = self.tick(stopped.state, self.due + 121, broadcast=facts,
                          writer=self.writer(self.due + 121, WriterStatus.STOPPED))
            self.assertIsNone(d.command)
            self.assertEqual(d.state.phase, Phase.WAIT_OFFLINE)

    def test_waiting_provider_offline_preserves_original_broadcast_start(self):
        _, stopped = self.stopped()
        d = self.tick(stopped.state, self.due + 121, writer=self.writer(self.due + 121, WriterStatus.STOPPED))
        self.assertEqual(d.state.session, self.old)

    def test_start_accepted_or_old_writer_generation_is_not_verified(self):
        r = self.started_request()
        for status, generation in (("ACCEPTED", 5), ("STARTED", 4)):
            d = self.tick(r.state, self.due + 122, outcome=self.ack(r, self.due + 122, status),
                          writer=self.writer(self.due + 122, generation=generation))
            self.assertEqual(d.state.phase, Phase.WAIT_START)

    def test_tcp_connected_with_no_byte_progress_never_verifies_restart(self):
        r = self.started_request()
        d = self.tick(r.state, self.due + 122, outcome=self.ack(r, self.due + 122, "STARTED"),
                      writer=self.writer(self.due + 122, generation=5, progress_age_seconds=120))
        self.assertEqual(d.state.phase, Phase.WAIT_START)

    def test_reconnect_to_old_broadcast_never_resets_age(self):
        _, verifying = self.verifying()
        d = self.tick(verifying.state, self.due + 123, writer=self.writer(self.due + 123, generation=5))
        self.assertEqual(d.state.phase, Phase.VERIFYING)
        self.assertEqual(d.state.session, self.old)

    def test_new_stream_id_and_new_started_at_reset_timer_only_after_verification(self):
        r, verifying = self.verifying()
        new = Session("222", self.clock(self.due + 122).utc)
        d = self.tick(verifying.state, self.due + 123, broadcast=self.broadcast(self.due + 123, session=new),
                      writer=self.writer(self.due + 123, generation=5))
        self.assertEqual(d.status, "RENEWED")
        self.assertEqual(d.state.session, new)
        self.assertEqual(d.state.phase, Phase.TRACKING)
        self.assertEqual(d.state.last_completed_operation, r.state.operation_id)
        tomorrow_midnight = self.due + 24 * 3600
        before_due = self.tick(d.state, tomorrow_midnight - 1,
                               broadcast=self.broadcast(tomorrow_midnight - 1, session=new),
                               writer=self.writer(tomorrow_midnight - 1, generation=5))
        self.assertIsNone(before_due.command)
        second = self.tick(before_due.state, tomorrow_midnight,
                           broadcast=self.broadcast(tomorrow_midnight, session=new),
                           writer=self.writer(tomorrow_midnight, generation=5))
        self.assertEqual(second.command.action, Action.STOP_TWITCH)
        self.assertNotEqual(second.command.request_id, r.command.request_id)

    def test_id_change_without_new_start_or_future_start_does_not_complete(self):
        _, verifying = self.verifying()
        invalid = (Session("222", self.origin), Session("111", self.clock(self.due + 122).utc),
                   Session("222", self.clock(self.due - 1).utc),
                   Session("222", self.clock(self.due + 124).utc),
                   Session("", self.clock(self.due + 122).utc))
        for session in invalid:
            d = self.tick(verifying.state, self.due + 123,
                          broadcast=self.broadcast(self.due + 123, session=session),
                          writer=self.writer(self.due + 123, generation=5))
            self.assertEqual(d.state.phase, Phase.VERIFYING)

    def test_lease_expiry_missing_grant_no_action_or_stale_input_blocks_effect(self):
        contexts = [self.context(self.due, lease_expires_elapsed=self.due),
                    self.context(self.due, lease_boot_id="old-boot"),
                    self.context(self.due, lease_expires_elapsed=float("nan")),
                    self.context(self.due, effects_enabled=False),
                    self.context(self.due, maintenance_authorized=False),
                    self.context(self.due, input_fresh=False),
                    self.context(self.due, maintenance_authorized="true")]
        for context in contexts:
            with self.subTest(context=context):
                d = self.tick(RenewalState(), self.due, context=context)
                self.assertIsNone(d.command)
                self.assertEqual(d.status, "BLOCKED")

    def test_fallback_or_route_handoff_never_restarts_twitch(self):
        start = self.started_request()
        for route in ("FALLBACK", "FAILOVER_PENDING", "RECOVERY_PENDING", "UNKNOWN"):
            d = self.tick(start.state, self.due + 122, context=self.context(self.due + 122, route_state=route))
            self.assertEqual(d.state.phase, Phase.CANCELLED)
            self.assertIsNone(d.command)
        d = self.tick(start.state, self.due + 122,
                      context=self.context(self.due + 122, route_change_pending=True))
        self.assertEqual(d.state.phase, Phase.CANCELLED)

    def test_current_route_epoch_or_relay_instance_change_cancels_pending_effect(self):
        r = self.started_request()
        for changes in ({"route_generation": 8}, {"authority_epoch": 3}, {"relay_instance_id": "relay-b"}):
            d = self.tick(r.state, self.due + 122, context=self.context(self.due + 122, **changes))
            self.assertEqual(d.state.phase, Phase.CANCELLED)
            self.assertIsNone(d.command)

    def test_timeout_or_unknown_attempt_does_not_retry_with_new_id(self):
        r = self.stop_request()
        timeout = self.tick(r.state, self.due + 600)
        unknown = self.tick(r.state, self.due + 1, outcome=self.ack(r, self.due + 1, "UNKNOWN"))
        for terminal in (timeout, unknown):
            self.assertEqual(terminal.state.phase, Phase.UNKNOWN)
            repeated = self.tick(terminal.state, self.due + 601)
            self.assertIsNone(repeated.command)
            self.assertEqual(repeated.state.operation_id, r.state.operation_id)

    def test_boot_change_and_wall_clock_jump_require_reconciliation(self):
        r = self.stop_request()
        jumped = replace(self.clock(self.due + 1), utc=self.clock(self.due + 61).utc)
        rebooted = replace(self.clock(self.due + 1), boot_id="boot-b", elapsed_seconds=1)
        for clock in (jumped, rebooted):
            d = self.tick(r.state, self.due + 1, now=clock)
            self.assertEqual(d.status, "UNKNOWN")
            self.assertEqual(d.state.session, self.old)
            self.assertIsNone(d.command)

    def test_process_restart_with_persisted_provider_start_does_not_reset_age(self):
        persisted = RenewalState(session=self.old)
        d = self.tick(persisted, self.due + 1, broadcast=self.broadcast(self.due + 1, BroadcastStatus.UNKNOWN))
        self.assertEqual(d.command.action, Action.STOP_TWITCH)
        self.assertEqual(d.state.session.started_at, self.origin)

    def test_short_transport_reconnect_does_not_change_broadcast_age(self):
        tracked = self.tick(RenewalState(), self.due - 1)
        reconnected = self.tick(tracked.state, self.due, writer=self.writer(self.due, generation=42))
        self.assertEqual(reconnected.command.action, Action.STOP_TWITCH)
        self.assertEqual(reconnected.command.binding.writer_generation, 42)
        self.assertEqual(reconnected.state.session, self.old)

    def test_unconfirmed_start_wrong_broadcaster_or_corrupt_state_never_emits_command(self):
        cases = (
            (RenewalState(), {"broadcast": self.broadcast(self.due, BroadcastStatus.UNKNOWN)}),
            (RenewalState(), {"broadcast": replace(self.broadcast(self.due), broadcaster_id="999")}),
            (RenewalState(phase=Phase.WAIT_START), {}),
            (RenewalState(phase=Phase.WAIT_START, began_at=datetime(2026, 10, 10)), {}),
            (RenewalState(revision=-1), {}),
        )
        for state, changes in cases:
            d = self.tick(state, self.due, **changes)
            self.assertIsNone(d.command)
            self.assertEqual(d.status, "UNKNOWN")

    def test_corrupt_persisted_pending_fields_fail_closed_without_exception(self):
        r = self.stop_request()
        corrupt = (replace(r.state, binding=replace(r.state.binding, writer_generation="4")),
                   replace(r.state, session=Session(None, self.origin)),
                   replace(r.state, stopped_clock="invalid"),
                   replace(r.state, start_requested_at=datetime(2026, 10, 10)))
        for state in corrupt:
            d = self.tick(state, self.due + 1)
            self.assertEqual(d.state.phase, Phase.UNKNOWN)
            self.assertIsNone(d.command)

    def test_future_or_stale_stop_ack_is_ignored(self):
        r = self.stop_request()
        for ack_at in (self.due + 2, self.due - 100):
            d = self.tick(r.state, self.due + 1, outcome=self.ack(r, ack_at),
                          writer=self.writer(self.due + 1, WriterStatus.STOPPED))
            self.assertEqual(d.state.phase, Phase.WAIT_STOP)

    def test_delayed_stop_ack_does_not_undo_a_start_request(self):
        r, _ = self.stopped()
        start = self.started_request()
        d = self.tick(start.state, self.due + 122, outcome=self.ack(r, self.due + 122),
                      writer=self.writer(self.due + 122, generation=5))
        self.assertEqual(d.state.phase, Phase.WAIT_START)
        self.assertIsNone(d.command)

    def test_delayed_tick_after_midnight_still_renews_once(self):
        d = self.tick(RenewalState(session=self.old), self.due + 300)
        self.assertEqual(d.command.action, Action.STOP_TWITCH)
        self.assertEqual(d.reason, "TWITCH_DAILY_JST_RENEWAL_DUE")
        repeated = self.tick(d.state, self.due + 301)
        self.assertIsNone(repeated.command)

    def test_session_started_at_midnight_waits_until_next_day(self):
        new = Session("222", self.clock(self.due).utc)
        d = self.tick(RenewalState(), self.due, broadcast=self.broadcast(self.due, session=new))
        self.assertIsNone(d.command)
        self.assertEqual(next_daily_renewal_at(new.started_at), self.clock(self.due + 24 * 3600).utc)

    def test_session_started_just_before_midnight_is_renewed_at_midnight(self):
        new = Session("222", self.clock(self.due - 1).utc)
        d = self.tick(RenewalState(), self.due, broadcast=self.broadcast(self.due, session=new))
        self.assertEqual(d.command.action, Action.STOP_TWITCH)

    def test_failed_midnight_tick_is_not_forgotten_when_authority_returns(self):
        blocked = self.tick(RenewalState(session=self.old), self.due,
                            context=self.context(self.due, maintenance_authorized=False))
        self.assertIsNone(blocked.command)
        recovered = self.tick(blocked.state, self.due + 300)
        self.assertEqual(recovered.command.action, Action.STOP_TWITCH)

    def test_short_age_guard_can_preempt_daily_schedule(self):
        d = self.tick(RenewalState(), 3600, policy=RenewalPolicy(renew_after_seconds=3600))
        self.assertEqual(d.command.action, Action.STOP_TWITCH)
        self.assertEqual(d.reason, "TWITCH_SESSION_AGE_GUARD_DUE")


class TwitchConfigurationAndFactsTests(unittest.TestCase):
    observed = datetime(2026, 10, 12, tzinfo=UTC)

    def test_offline_requires_a_successful_single_channel_query_not_api_failure(self):
        for status in (401, 429, 503):
            facts = parse_get_streams({"data": []}, broadcaster_id="123", observed_at=self.observed, http_status=status)
            self.assertEqual(facts.status, BroadcastStatus.UNKNOWN)
        self.assertEqual(parse_get_streams({"data": []}, broadcaster_id="123", observed_at=self.observed).status,
                         BroadcastStatus.OFFLINE)

    def test_get_streams_parses_identity_and_rfc3339_start(self):
        body = {"data": [{"user_id": "123", "id": "111", "type": "live", "started_at": "2026-10-10T00:00:00Z"}]}
        facts = parse_get_streams(body, broadcaster_id="123", observed_at=self.observed)
        self.assertEqual(facts.session, Session("111", datetime(2026, 10, 10, tzinfo=UTC)))

    def test_invalid_or_wrong_channel_response_is_unknown_not_offline(self):
        rows = ({}, {"user_id": "999", "id": "111", "type": "live", "started_at": "2026-10-10T00:00:00Z"},
                {"user_id": "123", "id": "111", "type": "live", "started_at": "2026-10-10T00:00:00"},
                {"user_id": "123", "id": "111", "type": "", "started_at": "2026-10-10T00:00:00Z"})
        for row in rows:
            facts = parse_get_streams({"data": [row]}, broadcaster_id="123", observed_at=self.observed)
            self.assertEqual(facts.status, BroadcastStatus.UNKNOWN)
        self.assertEqual(parse_get_streams({}, broadcaster_id="123", observed_at=self.observed).status,
                         BroadcastStatus.UNKNOWN)

    def test_policy_defaults_and_env_loading(self):
        p = RenewalPolicy.from_env({"TWITCH_SESSION_RENEW_TIME_JST": "00:00",
                                   "TWITCH_SESSION_RENEW_AFTER_SEC": "169200", "TWITCH_STREAM_KEY": "do-not-read"})
        self.assertEqual(p, RenewalPolicy())
        self.assertNotIn("do-not-read", repr(p))

    def test_daily_schedule_is_in_jst_independent_of_input_timezone(self):
        utc = datetime.fromisoformat("2026-10-12T14:59:59+00:00")
        jst = datetime.fromisoformat("2026-10-12T23:59:59+09:00")
        expected = datetime.fromisoformat("2026-10-12T15:00:00+00:00")
        self.assertEqual(next_daily_renewal_at(utc), expected)
        self.assertEqual(next_daily_renewal_at(jst), expected)

    def test_after_midnight_and_month_or_year_rollover(self):
        cases = (
            ("2026-10-13T00:02:00+09:00", "2026-10-13T15:00:00+00:00"),
            ("2026-10-31T23:59:59+09:00", "2026-10-31T15:00:00+00:00"),
            ("2026-12-31T23:59:59+09:00", "2026-12-31T15:00:00+00:00"),
        )
        for started, expected in cases:
            self.assertEqual(next_daily_renewal_at(datetime.fromisoformat(started)), datetime.fromisoformat(expected))

    def test_invalid_daily_schedule_is_rejected_without_exposing_values(self):
        for value in ("24:00", "00:60", "0:00", "00:00:00", "$(secret-command)", "", None):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "TWITCH_SESSION_RENEW_TIME_JST_INVALID"):
                RenewalPolicy.from_env({"TWITCH_SESSION_RENEW_TIME_JST": value})

    def test_policy_rejects_48_hours_short_offline_gap_invalid_timeout_and_shell_value(self):
        bad = ({"TWITCH_SESSION_RENEW_AFTER_SEC": "172800"}, {"TWITCH_SESSION_OFFLINE_WAIT_SEC": "90"},
               {"TWITCH_SESSION_OFFLINE_WAIT_SEC": "600"}, {"TWITCH_SESSION_RENEWAL_TIMEOUT_SEC": "3600"},
               {"TWITCH_SESSION_RENEW_AFTER_SEC": "$(secret-command)"}, {"TWITCH_SESSION_RENEW_AFTER_SEC": "NaN"})
        for values in bad:
            with self.assertRaises(ValueError) as error:
                RenewalPolicy.from_env(values)
            self.assertNotIn("secret-command", str(error.exception))

    def test_nonfinite_or_naive_clock_is_rejected(self):
        for utc, elapsed in ((self.observed, float("nan")), (datetime(2026, 10, 12), 0), (self.observed, -1)):
            with self.assertRaisesRegex(ValueError, "TWITCH_SESSION_CLOCK_INVALID"):
                Clock(utc, elapsed, "boot-a")


class TwitchForwardUpdateTests(unittest.TestCase):
    youtube = {"dest": "rtmps://a.rtmps.youtube.com:443/live2#test-youtube-key", "destFingerprint": "unchanged"}
    twitch = {"dest": "rtmps://ingest.global-contribute.live-video.net:443/app#test-twitch-key"}

    def test_stop_removes_only_twitch_and_preserves_youtube_config_and_position(self):
        current = [self.youtube.copy(), self.twitch.copy()]
        update = twitch_forward_update(current, twitch_destination=self.twitch, action=Action.STOP_TWITCH)
        self.assertEqual(update, [self.youtube])
        self.assertEqual(current, [self.youtube, self.twitch])
        self.assertIsNot(update[0], current[0])

    def test_start_appends_twitch_without_changing_or_reordering_youtube(self):
        update = twitch_forward_update([self.youtube], twitch_destination=self.twitch, action=Action.START_TWITCH)
        self.assertEqual(update, [self.youtube, self.twitch])

    def test_identical_payload_is_idempotent_for_duplicate_config_delivery(self):
        off = twitch_forward_update([self.youtube], twitch_destination=self.twitch, action=Action.STOP_TWITCH)
        on = twitch_forward_update([self.youtube, self.twitch], twitch_destination=self.twitch, action=Action.START_TWITCH)
        self.assertEqual(off, [self.youtube])
        self.assertEqual(on, [self.youtube, self.twitch])

    def test_provider_drift_reordering_extra_destinations_or_key_change_is_rejected(self):
        bad = ([self.twitch, self.youtube], [self.youtube, self.youtube], [],
               [self.youtube, self.twitch, self.twitch],
               [self.youtube, {"dest": self.twitch["dest"].replace("test-twitch-key", "old-secret")}])
        for current in bad:
            with self.assertRaises(ValueError) as error:
                twitch_forward_update(current, twitch_destination=self.twitch, action=Action.START_TWITCH)
            self.assertNotIn("secret", str(error.exception))

    def test_unapproved_host_plain_rtmp_and_credential_query_are_rejected(self):
        for url in ("rtmps://attacker.invalid/app#secret", "rtmp://ingest.global-contribute.live-video.net/app#secret",
                    "rtmps://ingest.global-contribute.live-video.net/app?token=secret#key"):
            with self.assertRaises(ValueError) as error:
                twitch_forward_update([self.youtube], twitch_destination={"dest": url}, action=Action.START_TWITCH)
            self.assertNotIn("secret", str(error.exception))


if __name__ == "__main__":
    unittest.main()
