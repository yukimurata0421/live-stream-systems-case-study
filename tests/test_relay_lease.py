from __future__ import annotations

import copy
import importlib.util
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ops/relay"))
from relay_lease import LeaseRejected, admit, sign


class LeaseBoundaryTests(unittest.TestCase):
    key = bytes(range(32))

    def setUp(self):
        self.body = {"schema": "stream.relay_lease.v1", "relay_instance": "vm-new-instance",
                     "target": "stable-youtube-stream", "encoder_instance": "producer-a",
                     "sender_instance": "sender-a", "route_generation": 1, "authority_epoch": "manual-grant-a",
                     "sequence": 2, "input_sequence": 100, "issued_at": 1000., "expires_at": 1010.,
                     "grant_expires_at": 1500., "route": "PRIMARY"}

    def admit(self, **changes):
        b = copy.deepcopy(self.body)
        b.update(changes)
        return admit(b, sign(b, self.key), key=self.key, relay_instance="vm-new-instance",
                     highwater=1, wall=1001, elapsed=50, previous=None)

    def test_absolute_and_suspend_clock_expire_independently(self):
        lease = self.admit()
        self.assertTrue(lease.valid(1009, 58))
        self.assertFalse(lease.valid(1010, 51))
        self.assertFalse(lease.valid(1002, 59))

    def test_expired_replayed_long_future_and_nan_rejected(self):
        for change in ({"expires_at": 1001}, {"sequence": 1}, {"expires_at": 1016},
                       {"issued_at": 1004}, {"grant_expires_at": 1000}, {"issued_at": float("nan")},
                       {"grant_expires_at": 90000}, {"sequence": True}):
            with self.subTest(change=change), self.assertRaises((LeaseRejected, ValueError)):
                self.admit(**change)

    def test_signature_and_exact_schema(self):
        for b, signature in ((self.body, "0" * 64), ({**self.body, "extra": 1}, sign(self.body, self.key))):
            with self.assertRaises(LeaseRejected):
                admit(b, signature, key=self.key, relay_instance="vm-new-instance", highwater=0,
                      wall=1000, elapsed=50, previous=None)

    def test_reboot_and_wrong_route_rejected(self):
        for change in ({"relay_instance": "vm-old-instance"}, {"route": "FALLBACK"}):
            with self.subTest(change=change), self.assertRaises(LeaseRejected):
                self.admit(**change)

    def test_renewal_requires_exact_instance_and_new_media(self):
        old = self.admit()
        valid = {**self.body, "sequence": 3, "input_sequence": 101, "issued_at": 1002, "expires_at": 1012}
        for name, value in (("sender_instance", "sender-b"), ("encoder_instance", "producer-b"),
                            ("route_generation", 2), ("input_sequence", 100), ("grant_expires_at", 2000)):
            bad = {**valid, name: value}
            with self.subTest(name=name), self.assertRaises(LeaseRejected):
                admit(bad, sign(bad, self.key), key=self.key, relay_instance="vm-new-instance",
                      highwater=2, wall=1002, elapsed=51, previous=old)
        new = admit(valid, sign(valid, self.key), key=self.key, relay_instance="vm-new-instance",
                    highwater=2, wall=1002, elapsed=51, previous=old)
        self.assertEqual(new.elapsed_deadline, 61)

    def test_continuation_renews_only_short_media_bound_permission(self):
        first = {**self.body, "grant_expires_at": 1010.}
        old = admit(first, sign(first, self.key), key=self.key, relay_instance="vm-new-instance",
                    highwater=1, wall=1000, elapsed=50, previous=None, continuous=True)
        # 最初の期限を越えても新しいmedia/sequenceで10秒の許可を発行できる。
        new = {**first, "issued_at": 1020., "expires_at": 1030., "grant_expires_at": 1030.,
               "sequence": 3, "input_sequence": 101}
        accepted = admit(new, sign(new, self.key), key=self.key, relay_instance="vm-new-instance",
                         highwater=2, wall=1020, elapsed=70, previous=old, continuous=True)
        self.assertFalse(old.valid(1020, 70))
        self.assertTrue(accepted.valid(1029, 79))
        self.assertFalse(accepted.valid(1030, 80))
        for change in ({"grant_expires_at": 2000}, {"sender_instance": "other"},
                       {"authority_epoch": "other"}, {"input_sequence": 100}, {"sequence": 2}):
            bad = {**new, **change}
            with self.subTest(change=change), self.assertRaises(LeaseRejected):
                admit(bad, sign(bad, self.key), key=self.key, relay_instance="vm-new-instance",
                      highwater=2, wall=1020, elapsed=70, previous=old, continuous=True)
        with self.assertRaises(LeaseRejected):
            admit(new, sign(new, self.key), key=self.key, relay_instance="vm-new-instance",
                  highwater=2, wall=1020, elapsed=70, previous=old)


if __name__ == "__main__":
    unittest.main()
