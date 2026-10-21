"""The relay observer reports this renewal's time while renewing, otherwise the next one."""
import unittest
from datetime import datetime, timezone

from stream_monitoring_v4.compatibility.relay_observe import provider, renewal_time


class RenewalTimeTests(unittest.TestCase):
    # The monitor treats Twitch OFFLINE as planned only within 360 seconds of this time during a renewal.
    def test_renewing_reports_this_midnight(self):
        now = datetime(2026, 10, 20, 15, 2, tzinfo=timezone.utc)  # 00:02 JST
        for phase in ('WAIT_OFFLINE', 'VERIFY'):
            with self.subTest(phase=phase):
                self.assertEqual(renewal_time(now, phase), '2026-10-20T15:00:00+00:00')

    def test_idle_reports_next_midnight(self):
        now = datetime(2026, 10, 20, 15, 2, tzinfo=timezone.utc)
        self.assertEqual(renewal_time(now, 'IDLE'), '2026-10-21T15:00:00+00:00')
        self.assertEqual(renewal_time(datetime(2026, 10, 20, 3, tzinfo=timezone.utc), 'IDLE'), '2026-10-20T15:00:00+00:00')



class ProviderTests(unittest.TestCase):
    def test_provider_comes_from_the_destination_host(self):
        self.assertEqual(provider({'pos': 2, 'conf': {'dest': 'rtmps://a.rtmps.youtube.com:443/live2#k'}}), 'youtube')
        self.assertEqual(provider({'pos': 1, 'conf': {'dest': 'rtmps://ingest.global-contribute.live-video.net:443/app#k'}}), 'twitch')
        self.assertIsNone(provider({'pos': 1}))
        self.assertIsNone(provider({'pos': 1, 'conf': {'dest': 'rtmps://evil.example/youtube.com#k'}}))


if __name__ == '__main__':
    unittest.main()
