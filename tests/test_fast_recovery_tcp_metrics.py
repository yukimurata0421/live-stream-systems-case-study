from __future__ import annotations

import unittest

from watchers.fast_recovery_core.tcp_metrics import parse_ss_tcp_metrics


class FastRecoveryTcpMetricsTests(unittest.TestCase):
    def test_parses_delivery_queue_ack_and_rto_counters(self) -> None:
        stdout = "\n".join(
            (
                'ESTAB 0 941214 10.42.0.6:33970 142.250.207.238:443 users:(("ffmpeg",pid=784,fd=8))',
                " cubic wscale:8,7 rto:204 bytes_acked:123456 bytes_sent:223456 "
                "lastsnd:32152 unacked:168 notsent:941214",
            )
        )

        metrics = parse_ss_tcp_metrics(stdout, ffmpeg_pid=784, ports=[443])

        self.assertEqual(metrics["send_q"], 941214)
        self.assertEqual(metrics["bytes_sent"], 223456)
        self.assertEqual(metrics["bytes_acked"], 123456)
        self.assertEqual(metrics["notsent"], 941214)
        self.assertEqual(metrics["unacked"], 168)
        self.assertEqual(metrics["lastsnd_ms"], 32152)
        self.assertEqual(metrics["rto_ms"], 204)


if __name__ == "__main__":
    unittest.main()
