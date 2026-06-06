from __future__ import annotations

import argparse
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ops" / "scripts"))

import persistent_tcp_anchor_observer  # type: ignore
import wan_address_observer  # type: ignore


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


if __name__ == "__main__":
    unittest.main()
