from __future__ import annotations

import socket
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "watchers"))

import network_observer  # type: ignore


def snapshot_template(*, ffmpeg_family: str = "ipv4") -> dict:
    return {
        "route": {
            "ipv4_default": {"ok": True, "dev": "enp2s0", "gateway": "192.0.2.1"},
            "ipv6_default": {"ok": True, "dev": "enp2s0", "gateway": "fe80::1", "expires": 1200},
        },
        "addresses": {
            "interface": "enp2s0",
            "ipv4_global": [{"local": "192.0.2.60", "prefixlen": 24}],
            "ipv6_global": [{"local": "2001:db8:1::10", "prefixlen": 64}],
        },
        "dns": {
            "ok": True,
            "preferred_family": "ipv4",
            "ipv4_count": 2,
            "ipv6_count": 2,
        },
        "tcp_connect_ipv4": {"ok": True, "family": "ipv4"},
        "tcp_connect_ipv6": {"ok": True, "family": "ipv6"},
        "ffmpeg_socket": {
            "connected": True,
            "remote_family": ffmpeg_family,
            "conn": "ESTAB 0 0 192.0.2.60:36682 142.250.23.134:443 users:(('ffmpeg',pid=222,fd=7))",
        },
    }


class NetworkObserverClassificationTests(unittest.TestCase):
    def test_ipv6_prefix_change_is_observe_only_when_ingest_is_ipv4(self) -> None:
        previous = snapshot_template(ffmpeg_family="ipv4")
        current = snapshot_template(ffmpeg_family="ipv4")
        current["addresses"]["ipv6_global"] = [{"local": "2001:db8:2::10", "prefixlen": 64}]

        classification = network_observer.classify_snapshot(current, previous_snapshot=previous)

        self.assertEqual(classification["status"], "route_change_observed")
        self.assertEqual(classification["cause"], "ipv6_prefix_or_default_route_churn")
        self.assertEqual(classification["affected_path"], "non_ingest_ipv6")
        self.assertEqual(classification["impact"], "current_ingest_uses_ipv4_observe_only")

    def test_ipv6_prefix_change_marks_current_ingest_at_risk_when_ffmpeg_is_ipv6(self) -> None:
        previous = snapshot_template(ffmpeg_family="ipv6")
        current = snapshot_template(ffmpeg_family="ipv6")
        current["route"]["ipv6_default"] = {"ok": True, "dev": "enp2s0", "gateway": "fe80::2", "expires": 1200}

        classification = network_observer.classify_snapshot(current, previous_snapshot=previous)

        self.assertEqual(classification["status"], "route_change_observed")
        self.assertEqual(classification["cause"], "ipv6_prefix_or_default_route_churn")
        self.assertEqual(classification["affected_path"], "rtmps_ipv6")
        self.assertEqual(classification["impact"], "current_rtmps_ipv6_tcp_session_may_break")

    def test_current_ipv4_connect_failure_is_incident_candidate_when_ffmpeg_is_ipv4(self) -> None:
        current = snapshot_template(ffmpeg_family="ipv4")
        current["tcp_connect_ipv4"] = {"ok": False, "family": "ipv4", "error": "timeout"}
        current["tcp_connect_ipv6"] = {"ok": True, "family": "ipv6"}

        classification = network_observer.classify_snapshot(current, previous_snapshot=snapshot_template())

        self.assertEqual(classification["status"], "incident_candidate")
        self.assertEqual(classification["cause"], "rtmps_ipv4_connect_failure")
        self.assertEqual(classification["affected_path"], "rtmps_ipv4")

    def test_dns_family_order_change_is_observable_not_restart_trigger(self) -> None:
        previous = snapshot_template()
        current = snapshot_template()
        current["dns"]["preferred_family"] = "ipv6"

        classification = network_observer.classify_snapshot(current, previous_snapshot=previous)

        self.assertEqual(classification["status"], "route_change_observed")
        self.assertEqual(classification["cause"], "rtmps_dns_family_order_changed")
        self.assertEqual(classification["action_hint"], "observe")

    def test_burst_window_uses_jst_minutes(self) -> None:
        # 2026-05-23 23:04:30 UTC == 2026-05-24 08:04:30 JST.
        self.assertTrue(network_observer.in_burst_window(1_779_577_470, "08:03-08:06"))
        self.assertFalse(network_observer.in_burst_window(1_779_577_470, "09:03-09:06"))

    def test_tcp_connect_probe_uses_requested_address_family(self) -> None:
        calls: list[int] = []

        def fake_getaddrinfo(host, port, family, socktype):
            calls.append(family)
            if family == socket.AF_INET:
                return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("192.0.2.10", port))]
            return []

        class FakeSocket:
            def __init__(self, family, socktype):
                self.family = family

            def settimeout(self, timeout):
                self.timeout = timeout

            def connect(self, sockaddr):
                self.sockaddr = sockaddr

            def close(self):
                pass

        with patch.object(network_observer.socket, "getaddrinfo", side_effect=fake_getaddrinfo), patch.object(
            network_observer.socket, "socket", side_effect=lambda family, socktype: FakeSocket(family, socktype)
        ):
            result = network_observer.tcp_connect_probe("a.rtmps.youtube.com", 443, socket.AF_INET, 1.0)

        self.assertTrue(result["ok"])
        self.assertEqual(result["family"], "ipv4")
        self.assertEqual(calls, [socket.AF_INET])


if __name__ == "__main__":
    unittest.main()
