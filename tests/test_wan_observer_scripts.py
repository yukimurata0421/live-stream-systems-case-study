from __future__ import annotations

import argparse
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ops" / "scripts"))

import persistent_tcp_anchor_observer  # type: ignore
import cpe_event_ingest  # type: ignore
import netlink_wan_event_observer  # type: ignore
import rtmps_tcp_burst_observer  # type: ignore
import rtmps_tcpdump_ring  # type: ignore
import wan_address_observer  # type: ignore


def probe(name: str, *, ok: bool, reconnect_after_failure_ok: bool | None = None) -> dict:
    payload = {"name": name, "ok": ok}
    if reconnect_after_failure_ok is not None:
        payload["reconnect_after_failure_ok"] = reconnect_after_failure_ok
    return payload


class WanAddressObserverTests(unittest.TestCase):
    def test_parse_ipv4_and_ipv6_anchors(self) -> None:
        ipv4 = wan_address_observer.parse_anchor("cloudflare_v4=1.1.1.1:443")
        ipv6 = wan_address_observer.parse_anchor("google_v6=[2001:4860:4860::8888]:443")

        self.assertEqual(ipv4["name"], "cloudflare_v4")
        self.assertEqual(ipv4["host"], "1.1.1.1")
        self.assertEqual(ipv4["literal_family"], "ipv4")
        self.assertEqual(ipv6["name"], "google_v6")
        self.assertEqual(ipv6["host"], "2001:4860:4860::8888")
        self.assertEqual(ipv6["literal_family"], "ipv6")

    def test_parse_anchor_rejects_unbracketed_ipv6_target(self) -> None:
        parsed = wan_address_observer.parse_anchor("bad=2001:4860:4860::8888:443")

        self.assertFalse(parsed["ok"])
        self.assertEqual(parsed["error"], "expected_host_colon_port")

    def test_signature_detects_wan_identity_changes(self) -> None:
        previous = {
            "interface": "enp2s0",
            "ipv4_default_gateway": "192.0.2.1",
            "ipv6_networks": ["2001:db8:1::/64"],
            "public_ipv4": "198.51.100.10",
        }
        payload = {
            "interface": "enp2s0",
            "routes": {
                "ipv4_default": {"dev": "enp2s0", "gateway": "192.0.2.1"},
                "ipv6_default": {"dev": "enp2s0", "gateway": "fe80::1"},
            },
            "addresses": {
                "ipv4_global": [{"local": "192.0.2.20"}],
                "ipv6_global": [{"local": "2001:db8:2::10", "network": "2001:db8:2::/64"}],
            },
            "public_ipv4": {"address": "198.51.100.20"},
        }

        current = wan_address_observer.signature(payload)
        changes = wan_address_observer.changed_fields(previous, current)

        self.assertIn("ipv6_networks", changes)
        self.assertIn("public_ipv4", changes)

    def test_loop_mode_honors_cycles(self) -> None:
        args = argparse.Namespace(interval_sec=0.01, duration_sec=0, cycles=3)

        with patch.object(wan_address_observer, "write_sample") as write_sample, patch.object(wan_address_observer.time, "sleep"):
            wan_address_observer.run_observer(args)

        self.assertEqual(write_sample.call_count, 3)


class PersistentTcpAnchorObserverTests(unittest.TestCase):
    def test_parse_anchor_keeps_as_and_family_metadata(self) -> None:
        anchor = persistent_tcp_anchor_observer.parse_anchor(
            "cloudflare_v6|2606:4700:4700::1111|443|cloudflare-dns.com|cloudflare-dns.com|AS13335"
        )

        self.assertEqual(anchor.name, "cloudflare_v6")
        self.assertEqual(anchor.port, 443)
        self.assertEqual(anchor.as_hint, "AS13335")
        self.assertEqual(anchor.literal_family, "ipv6")

    def test_parse_status_and_connection_close_headers(self) -> None:
        headers = b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"

        self.assertEqual(persistent_tcp_anchor_observer.parse_status_code(headers), 204)
        self.assertTrue(persistent_tcp_anchor_observer.connection_close_requested(headers))

    def test_build_payload_summarizes_failed_anchors(self) -> None:
        class FakeFlow:
            def __init__(self, result: dict):
                self.result = result

            def send_probe(self) -> dict:
                return self.result

        args = argparse.Namespace(interval_sec=15.0, timeout_sec=2.0)
        payload = persistent_tcp_anchor_observer.build_payload(
            [
                FakeFlow({"name": "cloudflare_v4", "ok": True}),
                FakeFlow({"name": "google_v4", "ok": False}),
            ],
            args,
        )

        self.assertEqual(payload["schema"], "stream_v3_persistent_tcp_anchor_observer/v1")
        self.assertEqual(payload["ok_count"], 1)
        self.assertEqual(payload["failed"], ["google_v4"])

    def test_all_anchor_failure_triggers_wan_snapshot(self) -> None:
        payload = {
            "probes": [
                probe("cloudflare_v4", ok=False, reconnect_after_failure_ok=False),
                probe("google_v4", ok=False, reconnect_after_failure_ok=False),
            ]
        }

        should_trigger, reason = persistent_tcp_anchor_observer.should_trigger_wan_snapshot(payload)

        self.assertTrue(should_trigger)
        self.assertEqual(reason, "all_anchors_failed")

    def test_partial_reconnect_failure_triggers_wan_snapshot(self) -> None:
        payload = {
            "probes": [
                probe("cloudflare_v4", ok=False, reconnect_after_failure_ok=False),
                probe("google_v4", ok=True),
            ]
        }

        should_trigger, reason = persistent_tcp_anchor_observer.should_trigger_wan_snapshot(payload)

        self.assertTrue(should_trigger)
        self.assertEqual(reason, "reconnect_after_failure_failed:cloudflare_v4")

    def test_reconnect_success_is_baseline_noise(self) -> None:
        payload = {
            "probes": [
                probe("cloudflare_v4", ok=False, reconnect_after_failure_ok=True),
                probe("google_v4", ok=True),
            ]
        }

        should_trigger, reason = persistent_tcp_anchor_observer.should_trigger_wan_snapshot(payload)

        self.assertFalse(should_trigger)
        self.assertEqual(reason, "baseline_or_partial_anchor_failure")

    def test_wan_snapshot_command_collects_short_followup_burst(self) -> None:
        payload = {
            "probes": [
                probe("cloudflare_v4", ok=False, reconnect_after_failure_ok=False),
                probe("google_v4", ok=True),
            ]
        }
        args = argparse.Namespace(
            wan_snapshot_python="/usr/bin/python3",
            wan_snapshot_script=Path("/repo/ops/scripts/wan_address_observer.py"),
            wan_snapshot_interval_sec=5,
            wan_snapshot_cycles=7,
            wan_snapshot_reason_prefix="persistent_anchor_failure",
        )

        command = persistent_tcp_anchor_observer.build_wan_snapshot_command(payload, args)

        self.assertEqual(command[:2], ["/usr/bin/python3", "/repo/ops/scripts/wan_address_observer.py"])
        self.assertIn("--interval-sec", command)
        self.assertIn("5", command)
        self.assertIn("--cycles", command)
        self.assertIn("7", command)
        self.assertIn("persistent_anchor_failure:reconnect_after_failure_failed:cloudflare_v4:cloudflare_v4", command)

    def test_rtmps_burst_command_collects_tcp_socket_followup_burst(self) -> None:
        payload = {
            "probes": [
                probe("cloudflare_v4", ok=False, reconnect_after_failure_ok=False),
                probe("google_v4", ok=True),
            ]
        }
        args = argparse.Namespace(
            rtmps_burst_python="/usr/bin/python3",
            rtmps_burst_script=Path("/repo/ops/scripts/rtmps_tcp_burst_observer.py"),
            rtmps_burst_interval_sec=5,
            rtmps_burst_duration_sec=300,
            rtmps_burst_reason_prefix="persistent_anchor_failure",
        )

        command = persistent_tcp_anchor_observer.build_rtmps_burst_command(payload, args)

        self.assertEqual(command[:2], ["/usr/bin/python3", "/repo/ops/scripts/rtmps_tcp_burst_observer.py"])
        self.assertIn("--interval-sec", command)
        self.assertIn("5", command)
        self.assertIn("--duration-sec", command)
        self.assertIn("300", command)
        self.assertIn("persistent_anchor_failure:reconnect_after_failure_failed:cloudflare_v4:cloudflare_v4", command)


class RtmpsTcpBurstObserverTests(unittest.TestCase):
    def test_parse_ss_output_extracts_ffmpeg_rtmps_metrics(self) -> None:
        output = """State Recv-Q Send-Q Local Address:Port Peer Address:Port Process
ESTAB 0 170 10.42.0.52:37152 172.217.221.134:443 users:(("ffmpeg",pid=1370605,fd=8))
         cubic wscale:7,7 rto:204 rtt:38.25/8.5 mss:1412 cwnd:10 bytes_sent:123456 lastsnd:82 unacked:0 retrans:0/2 notsent:170
ESTAB 0 0 10.42.0.52:55555 1.1.1.1:443 users:(("python3",pid=10,fd=5))
         cubic rto:204 rtt:20/2
"""

        sockets = rtmps_tcp_burst_observer.parse_ss_output(output)

        self.assertEqual(len(sockets), 1)
        self.assertEqual(sockets[0]["pid"], 1370605)
        self.assertEqual(sockets[0]["peer"], "172.217.221.134:443")
        self.assertEqual(sockets[0]["metrics"]["lastsnd"], 82)
        self.assertEqual(sockets[0]["metrics"]["notsent"], 170)
        self.assertEqual(sockets[0]["metrics"]["rtt_ms"], 38.25)
        self.assertEqual(sockets[0]["metrics"]["retrans_total"], 2)


class NetlinkWanEventObserverTests(unittest.TestCase):
    def test_classify_default_route_delete(self) -> None:
        parsed = netlink_wan_event_observer.classify_ip_monitor_line("Deleted default via fe80::1 dev enx0 proto ra metric 1024 expires 1799sec")

        self.assertEqual(parsed["action"], "deleted")
        self.assertEqual(parsed["event_class"], "default_route")
        self.assertEqual(parsed["interface"], "enx0")

    def test_classify_ipv6_prefix(self) -> None:
        parsed = netlink_wan_event_observer.classify_ip_monitor_line("2: enx0    inet6 2001:db8:1::123/64 scope global dynamic")

        self.assertEqual(parsed["event_class"], "ipv6_address_or_prefix")
        self.assertEqual(parsed["interface"], "enx0")
        self.assertEqual(parsed["address"], "2001:db8:1::123/64")


class CpeEventIngestTests(unittest.TestCase):
    def test_classify_cpe_session_and_prefix_event(self) -> None:
        parsed = cpe_event_ingest.classify_cpe_event("PDN disconnect failed; DHCPv6 prefix delegation expired")

        self.assertEqual(parsed["event_class"], "wan_disconnect")
        self.assertIn("pdn disconnect", parsed["matched_keywords"])
        self.assertIn("prefix delegation", parsed["matched_keywords"])
        self.assertEqual(parsed["severity"], "error")

    def test_parse_api_headers(self) -> None:
        parsed = cpe_event_ingest.parse_api_headers(["X-Test: value", "Accept: application/json"])

        self.assertEqual(parsed["X-Test"], "value")
        self.assertEqual(parsed["Accept"], "application/json")

    def test_default_listener_is_localhost_for_public_safety(self) -> None:
        with patch.object(sys, "argv", ["cpe_event_ingest.py"]):
            args = cpe_event_ingest.parse_args()

        self.assertEqual(args.listen_host, "127.0.0.1")


class RtmpsTcpdumpRingTests(unittest.TestCase):
    def test_build_tcpdump_command_uses_small_snaplen_and_output_path(self) -> None:
        args = argparse.Namespace(
            tcpdump_binary="tcpdump",
            interface="any",
            tcpdump_user="root",
            snaplen=128,
            duration_sec=1200,
            capture_filter="tcp port 443",
        )

        command = rtmps_tcpdump_ring.build_tcpdump_command(args, Path("/tmp/capture.pcap"))

        self.assertEqual(command[:2], ["tcpdump", "-i"])
        self.assertIn("-s", command)
        self.assertIn("128", command)
        self.assertIn("-Z", command)
        self.assertIn("root", command)
        self.assertIn("-w", command)
        self.assertIn("/tmp/capture.pcap", command)
        self.assertEqual(command[-1], "tcp port 443")

    def test_public_default_is_dry_run(self) -> None:
        with patch.object(sys, "argv", ["rtmps_tcpdump_ring.py"]):
            args = rtmps_tcpdump_ring.parse_args()

        self.assertTrue(args.dry_run)


if __name__ == "__main__":
    unittest.main()
