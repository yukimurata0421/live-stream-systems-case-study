"""Fallback state and upload rate in the public relay artifact (rate sampled from the fallback Pod on the monitoring host)."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from stream_monitoring_v4.compatibility import relay_public as rp

SS = 'ESTAB 0 0 192.0.2.10:58990 198.51.100.7:443\n\t cubic bytes_sent:{n} bytes_acked:{n} segs_out:1\n'


def deploy(desired, ready):
    return json.dumps({'metadata': {}, 'spec': {'replicas': desired}, 'status': {'readyReplicas': ready}})


class FakeKubectl:
    def __init__(self, desired=1, ready=1, pod='fb-1', acked=0, fail=False):
        self.desired, self.ready, self.pod, self.acked, self.fail = desired, ready, pod, acked, fail

    def run(self, cmd, **_):
        args = cmd[3:]
        out, code = '', 0
        if self.fail:
            code = 1
        elif args[:2] == ['get', 'deploy']:
            out = deploy(self.desired, self.ready)
        elif args[:2] == ['get', 'pod']:
            out = self.pod
        elif args[0] == 'exec':
            assert args[1] == self.pod and args[-5:] == ['ss', '-tni', 'state', 'established', '( dport = :443 )']
            out = SS.format(n=self.acked)
        return mock.Mock(returncode=code, stdout=out)


class FallbackTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        p = mock.patch.object(rp, 'STATE', Path(self.tmp.name) / 'fallback-sample.json')
        p.start(); self.addCleanup(p.stop)

    def call(self, kube, at):
        with mock.patch.object(rp.subprocess, 'run', side_effect=kube.run), mock.patch.object(rp.time, 'time', return_value=at):
            return rp.fallback()

    def test_rate_comes_from_two_consecutive_calls_on_the_same_pod(self):
        kube = FakeKubectl(acked=0)
        first = self.call(kube, 1000.0)
        self.assertEqual((first['active'], first['ready'], first['mbps']), (True, True, None))
        kube.acked = 7_500_000
        self.assertEqual(self.call(kube, 1015.0)['mbps'], 4.0)

    def test_no_rate_after_pod_replacement_or_counter_reset(self):
        kube = FakeKubectl(acked=9_000_000)
        self.call(kube, 1000.0)
        kube.pod, kube.acked = 'fb-2', 9_500_000
        self.assertIsNone(self.call(kube, 1060.0)['mbps'])
        kube.acked = 1_000
        self.assertIsNone(self.call(kube, 1120.0)['mbps'])

    def test_scaled_down_reports_inactive_without_rate_and_forgets_the_sample(self):
        kube = FakeKubectl(acked=1)
        self.call(kube, 1000.0)
        kube.desired, kube.ready = 0, 0
        out = self.call(kube, 1060.0)
        self.assertEqual((out['active'], out['ready']), (False, False))
        self.assertNotIn('mbps', out)
        self.assertFalse(rp.STATE.exists())

    def test_kubectl_failure_is_unknown_not_inactive(self):
        out = self.call(FakeKubectl(fail=True), 1000.0)
        self.assertEqual((out['active'], out['ready']), (None, None))


if __name__ == '__main__':
    unittest.main()
