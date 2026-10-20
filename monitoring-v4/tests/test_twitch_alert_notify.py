"""Twitch alert delivery: one message on start, repeats while firing, one on resolve; other alerts ignored."""
import unittest

from stream_monitoring_v4.compatibility.twitch_alert_notify import decide

T0 = 1_800_000_000.0


def alert(name, state='firing', active='2027-01-15T08:00:00Z', **labels):
    return {'labels': {'alertname': name, **labels}, 'state': state, 'activeAt': active}


class DecideTests(unittest.TestCase):
    def test_new_firing_alert_sends_once(self):
        msgs, state = decide([alert('TwitchUnexpectedOffline')], {}, T0, repeat_sec=3600)
        self.assertEqual(len(msgs), 1)
        self.assertIn('OFFLINE unexpectedly', msgs[0])
        msgs, state = decide([alert('TwitchUnexpectedOffline')], state, T0 + 60, repeat_sec=3600)
        self.assertEqual(msgs, [])

    def test_repeat_while_firing_and_resolve(self):
        _, state = decide([alert('TwitchRelayNotProgressing')], {}, T0, repeat_sec=3600)
        msgs, state = decide([alert('TwitchRelayNotProgressing')], state, T0 + 3600, repeat_sec=3600)
        self.assertEqual(len(msgs), 1)
        self.assertIn('Still firing', msgs[0])
        msgs, state = decide([], state, T0 + 3700, repeat_sec=3600)
        self.assertEqual(len(msgs), 1)
        self.assertIn('Resolved', msgs[0])
        self.assertEqual(state, {})

    def test_pending_and_unrelated_alerts_are_ignored(self):
        msgs, state = decide([alert('TwitchUnexpectedOffline', state='pending'), alert('StreamV3NotifyActiveIncident')], {}, T0, repeat_sec=3600)
        self.assertEqual((msgs, state), ([], {}))

    def test_slo_alerts_are_tracked_per_sli(self):
        msgs, state = decide([alert('StreamV4TwitchSloBreached', sli='provider_live', window='7d'),
                              alert('StreamV4TwitchSloBreached', sli='daily_renewal', window='7d')], {}, T0, repeat_sec=3600)
        self.assertEqual(len(msgs), 2)
        self.assertEqual(len(state), 2)
        self.assertTrue(any('daily_renewal' in m for m in msgs))


if __name__ == '__main__':
    unittest.main()
