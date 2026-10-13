"""継続承認と期限付き送信許可の境界。外部通信・process起動なし。"""
import json
import pathlib
import sys
import threading
import unittest
from unittest.mock import Mock, patch

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "ops/relay"))
from relay_guard import Guard
from relay_lease import Lease, LeaseRejected, sign
from stream_core.engine.relay_lease_client import RelayLeaseClient


class ContinuationTests(unittest.TestCase):
    def client(self, mode):
        values = {"authorization_mode": mode, "hmac_key": "00" * 32, "grant_expires_at": 1050.,
                  "relay_instance": "relay", "target": "target", "grant_id": "consent",
                  "route_generation": 1}
        pipeline = Mock()
        pipeline.snapshot.return_value = {"encoder_running": True, "input_age_seconds": .1, "input_sequence": 100}
        with patch("stream_core.engine.relay_lease_client.time.time", return_value=1000), \
             patch("stream_core.engine.relay_lease_client.time.clock_gettime", return_value=50):
            client = RelayLeaseClient(values, pipeline, Mock())
        client.last_wall, client.last_elapsed = 1998, 1048
        client.process_identity = Mock(return_value="media-instance")
        def request(path, body=None):
            if path == "/status":
                return {"instance": "relay", "sequence": 0, "halted": False}
            self.sent = body
            return {"instance": "relay", "sequence": body["lease"]["sequence"], "halted": False}
        client.request = request
        return client

    def tick(self, client):
        with patch("stream_core.engine.relay_lease_client.time.time", return_value=2000), \
             patch("stream_core.engine.relay_lease_client.time.clock_gettime", return_value=1050):
            client.tick()

    def test_client_continues_past_old_cutoff_with_only_ten_second_lease(self):
        client = self.client("continuous")
        self.tick(client)
        body = self.sent["lease"]
        self.assertEqual((body["issued_at"], body["expires_at"], body["grant_expires_at"]), (2000, 2010, 2010))
        self.assertEqual(self.sent["signature"], sign(body, bytes(32)))
        client.pipeline.snapshot.return_value["input_sequence"] = 101
        client.last_wall = 2001  # backward clock remains blocked
        with self.assertRaisesRegex(ValueError, "CLOCK_CHANGED"):
            self.tick(client)

    def test_default_finite_permission_still_expires(self):
        with self.assertRaisesRegex(ValueError, "GRANT_EXPIRED"):
            self.tick(self.client("finite"))

    def test_stale_and_stationary_media_cannot_renew_continuation(self):
        client = self.client("continuous")
        client.pipeline.snapshot.return_value["input_age_seconds"] = 3
        with self.assertRaisesRegex(ValueError, "SOURCE_STALE"):
            self.tick(client)
        client.pipeline.snapshot.return_value["input_age_seconds"] = .1
        client.last_media_sequence = 100
        with self.assertRaisesRegex(ValueError, "NO_PROGRESS"):
            self.tick(client)

    def guard(self, continuous):
        guard = object.__new__(Guard)
        guard.lock = threading.RLock()
        guard.continuous = continuous
        guard.values = {"operator_grant": {"target": "target", "authority_epoch": "consent", "grant_expires_at": 1050.}}
        guard.key, guard.instance, guard.highwater = bytes(32), "relay", 0
        guard.lease = None
        guard.halted = guard.enabled = False
        guard.proc = Mock(); guard.proc.poll.return_value = None
        guard.last_wall, guard.last_elapsed, guard.grant_elapsed_deadline = 1998, 1048, 100
        guard.last_progress = 1050
        guard.api = Mock(return_value={"items": []})
        guard.stop, guard.db = Mock(), Mock()
        return guard

    def test_guard_continuation_keeps_native_lease_expiry(self):
        guard = self.guard(True)
        with patch("relay_guard.time.time", return_value=2000), patch("relay_guard.elapsed", return_value=1050):
            guard.tick()
            guard.stop.assert_not_called()
            guard.lease = Lease({"issued_at": 1990, "expires_at": 2000, "grant_expires_at": 2000}, 1050)
            guard.tick()
        guard.stop.assert_called_once_with("LEASE_EXPIRED")

    def test_guard_finite_and_identity_boundaries_remain(self):
        guard = self.guard(False)
        with patch("relay_guard.time.time", return_value=2000), patch("relay_guard.elapsed", return_value=1050):
            guard.tick()
        guard.stop.assert_called_once_with("OPERATOR_GRANT_ELAPSED_EXPIRED")
        guard = self.guard(True)
        body = {"schema": "stream.relay_lease.v1", "relay_instance": "relay", "target": "other",
                "encoder_instance": "encoder", "sender_instance": "sender", "route_generation": 1,
                "authority_epoch": "consent", "sequence": 1, "input_sequence": 1,
                "issued_at": 2000, "expires_at": 2010, "grant_expires_at": 2010, "route": "PRIMARY"}
        with self.assertRaisesRegex(LeaseRejected, "GRANT_MISMATCH"):
            guard.accept({"lease": body, "signature": sign(body, guard.key)})

    def test_unknown_mode_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "MODE_INVALID"):
            self.client("continous")


if __name__ == "__main__":
    unittest.main()
