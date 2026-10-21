"""Self-healing fan-out decisions (fallback start/stop, forward healing, daily Twitch renewal) without side effects."""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ops/selfheal"))
from fallback_controller import Controller, START_AFTER, STOP_AFTER, upload_rate, youtube_item  # noqa: E402
from forward_keeper import Keeper, OFFLINE_MAX, OFFLINE_MIN, VERIFY_MAX, AGE_LIMIT  # noqa: E402

YT = {'dest': 'rtmps://a.rtmps.youtube.com:443/live2#k'}
TW = {'dest': 'rtmps://ingest.global-contribute.live-video.net:443/app#k'}


def sample(n, *, ready=True, state='forwarding', error=False):
    return {'ready': ready, 'bytes_received': n, 'youtube': {'state': state, 'error': error, 'outbound': n}}


class FallbackTests(unittest.TestCase):
    def run_samples(self, c, samples, replicas, start=0.0):
        actions = []
        for i, s in enumerate(samples):
            now = start + i * 15
            actions.append((now, c.decide(now, c.healthy(s), replicas)))
        return actions

    def test_progressing_path_is_healthy_and_needs_no_fallback(self):
        c = Controller()
        actions = self.run_samples(c, [sample(i) for i in range(1, 10)], replicas=0)
        self.assertTrue(all(a is None for _, a in actions))

    def test_starts_after_sustained_failure_only(self):
        c = Controller()
        c.healthy(sample(1))
        self.assertIsNone(c.decide(0, c.healthy(None), 0))
        self.assertIsNone(c.decide(START_AFTER - 1, c.healthy(None), 0))
        self.assertEqual(c.decide(START_AFTER, c.healthy(None), 0), 'START')

    def test_stalled_input_or_youtube_error_counts_as_unhealthy(self):
        # input not ready, received bytes not advancing, YouTube forward idle, YouTube forward error
        for bad in (sample(5, ready=False), sample(1), sample(5, state='idle'), sample(5, error=True)):
            with self.subTest(bad=bad):
                c = Controller()
                c.healthy(sample(1))
                self.assertFalse(c.healthy(bad))

    def test_stops_only_after_continuous_healthy_window(self):
        c = Controller()
        c.healthy(sample(0))
        n = 1
        t = 0.0
        while t < STOP_AFTER:
            self.assertIsNone(c.decide(t, c.healthy(sample(n)), 1))
            n += 1
            t += 15
        self.assertEqual(c.decide(t, c.healthy(sample(n)), 1), 'STOP')

    def test_one_bad_sample_restarts_the_healthy_window(self):
        c = Controller()
        c.healthy(sample(0))
        c.decide(0, c.healthy(sample(1)), 1)
        c.decide(290, c.healthy(sample(2)), 1)
        c.decide(295, c.healthy(None), 1)              # missing sample
        c.healthy(sample(3))
        self.assertIsNone(c.decide(310, c.healthy(sample(4)), 1))
        self.assertIsNone(c.decide(310 + STOP_AFTER - 1, c.healthy(sample(5)), 1))

    def test_unknown_replicas_takes_no_action(self):
        c = Controller()
        self.assertIsNone(c.decide(1000, c.healthy(None), None))


class FallbackUploadTests(unittest.TestCase):
    def test_rate_from_two_samples_of_the_same_pod(self):
        a = {'pod': 'p1', 'bytes': 0, 'at': 100.0}
        b = {'pod': 'p1', 'bytes': 7_500_000, 'at': 115.0}
        self.assertEqual(upload_rate(a, b), 4.0)

    def test_no_rate_when_pod_changed_counter_reset_or_missing(self):
        a = {'pod': 'p1', 'bytes': 5_000_000, 'at': 100.0}
        self.assertIsNone(upload_rate(a, {'pod': 'p2', 'bytes': 9_000_000, 'at': 115.0}))
        self.assertIsNone(upload_rate(a, {'pod': 'p1', 'bytes': 1_000, 'at': 115.0}))
        self.assertIsNone(upload_rate(None, a))
        self.assertIsNone(upload_rate(a, None))
        self.assertIsNone(upload_rate(a, {'pod': 'p1', 'bytes': 6_000_000, 'at': 100.0}))


class ProviderIdentityTests(unittest.TestCase):
    def test_youtube_is_found_by_host_wherever_it_sits(self):
        yt, tw = {'pos': 2, 'conf': YT}, {'pos': 1, 'conf': TW}
        self.assertIs(youtube_item([tw, yt]), yt)

    def test_twitch_in_first_position_is_not_taken_for_youtube(self):
        self.assertIsNone(youtube_item([{'pos': 1, 'state': 'forwarding', 'outboundBytes': 9, 'conf': TW}]))

    def test_missing_or_ambiguous_youtube_is_unknown(self):
        self.assertIsNone(youtube_item([{'pos': 1}]))
        self.assertIsNone(youtube_item([{'pos': 1, 'conf': YT}, {'pos': 2, 'conf': YT}]))
        self.assertIsNone(youtube_item([{'pos': 1, 'conf': {'dest': 'rtmps://evil.example/youtube.com#k'}}]))



class HealTests(unittest.TestCase):
    def test_removes_failing_destination_then_restores_it(self):
        k = Keeper([YT, TW])
        bad = [{'pos': 1, 'state': 'forwarding', 'conf': YT}, {'pos': 2, 'state': 'idle', 'lastError': 'x', 'conf': TW}]
        self.assertIsNone(k.heal(0, True, bad))
        self.assertEqual(k.heal(30, True, bad), [YT])
        self.assertEqual(k.heal(35, True, []), [YT, TW])
        # the same destination is not re-created again within 60 seconds
        self.assertIsNone(k.heal(70, True, bad))
        self.assertIsNone(k.heal(85, True, bad))
        self.assertEqual(k.heal(100, True, bad), [YT])

    def test_no_input_is_not_a_forward_failure(self):
        k = Keeper([YT, TW])
        self.assertIsNone(k.heal(0, False, [{'pos': 1, 'state': 'idle', 'conf': YT}]))
        self.assertIsNone(k.heal(100, False, [{'pos': 1, 'state': 'idle', 'conf': YT}]))

    def test_destination_is_matched_by_its_own_dest_not_by_position(self):
        # Right after YouTube is removed, Twitch sits at pos 1; its failure must not count against YouTube.
        k = Keeper([YT, TW])
        shifted = [{'pos': 1, 'state': 'idle', 'lastError': 'x', 'conf': TW}]
        k.heal(0, True, shifted)
        self.assertEqual(set(k.bad_since), {TW['dest']})
        self.assertEqual(k.heal(30, True, shifted), [YT])

    def test_items_without_a_configured_dest_are_ignored(self):
        k = Keeper([YT, TW])
        self.assertIsNone(k.heal(0, True, [{'pos': 1, 'state': 'idle'}, {'pos': 2, 'state': 'idle', 'conf': {'dest': 'rtmps://x/y'}}]))
        self.assertEqual(k.bad_since, {})


class RenewalTests(unittest.TestCase):
    def twitch(self, status='LIVE', sid='1', age=3600, wall=None):
        started = (wall or datetime(2026, 10, 20, 12, tzinfo=timezone.utc)) - timedelta(seconds=age)
        return {'status': status, 'stream_id': sid if status == 'LIVE' else '', 'started_at': started if status == 'LIVE' else None}

    def test_first_start_does_not_renew_the_same_day(self):
        k = Keeper([YT, TW])
        wall = datetime(2026, 10, 20, 1, tzinfo=timezone.utc)
        self.assertIsNone(k.renewal(0, wall, True, self.twitch(wall=wall)))
        self.assertEqual(k.phase, 'IDLE')

    def test_daily_renewal_removes_only_twitch_and_restores_after_offline(self):
        k = Keeper([YT, TW])
        day1 = datetime(2026, 10, 20, 14, 0, tzinfo=timezone.utc)   # 23:00 JST
        k.renewal(0, day1, True, self.twitch(wall=day1))
        day2 = datetime(2026, 10, 20, 15, 0, 5, tzinfo=timezone.utc)  # 00:00:05 JST
        self.assertEqual(k.renewal(10, day2, True, self.twitch(wall=day2)), [YT])
        self.assertIsNone(k.renewal(10 + OFFLINE_MIN - 1, day2, True, self.twitch('OFFLINE')))
        self.assertEqual(k.renewal(10 + OFFLINE_MIN, day2, True, self.twitch('OFFLINE')), [YT, TW])
        self.assertEqual(k.phase, 'VERIFY')
        k.renewal(200, day2, True, self.twitch(sid='2'))
        self.assertEqual(k.phase, 'IDLE')
        # never renew twice on the same JST date
        self.assertIsNone(k.renewal(300, day2 + timedelta(hours=1), True, self.twitch(sid='2', wall=day2)))

    def test_twitch_is_restored_even_if_offline_is_never_observed(self):
        k = Keeper([YT, TW])
        k.last_renewal_date = '2026-10-19'
        wall = datetime(2026, 10, 20, 15, 1, tzinfo=timezone.utc)
        self.assertEqual(k.renewal(0, wall, True, self.twitch(wall=wall)), [YT])
        self.assertIsNone(k.renewal(OFFLINE_MAX - 1, wall, True, self.twitch('UNKNOWN')))
        self.assertEqual(k.renewal(OFFLINE_MAX, wall, True, self.twitch('UNKNOWN')), [YT, TW])
        k.renewal(OFFLINE_MAX + VERIFY_MAX, wall, True, self.twitch('UNKNOWN'))
        self.assertEqual(k.phase, 'IDLE')

    def test_age_limit_triggers_renewal_before_48_hours(self):
        k = Keeper([YT, TW])
        wall = datetime(2026, 10, 20, 5, tzinfo=timezone.utc)
        k.last_renewal_date = '2026-10-20'
        self.assertIsNone(k.renewal(0, wall, True, self.twitch(age=AGE_LIMIT - 1, wall=wall)))
        self.assertEqual(k.renewal(1, wall, True, self.twitch(age=AGE_LIMIT, wall=wall)), [YT])

    def test_no_renewal_without_input_or_unknown_twitch(self):
        k = Keeper([YT, TW])
        k.last_renewal_date = '2026-10-19'
        wall = datetime(2026, 10, 20, 15, 1, tzinfo=timezone.utc)
        self.assertIsNone(k.renewal(0, wall, False, self.twitch(wall=wall)))
        self.assertIsNone(k.renewal(0, wall, True, self.twitch('UNKNOWN')))


if __name__ == '__main__':
    unittest.main()
