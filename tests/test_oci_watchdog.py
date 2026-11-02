import base64
import gzip
from datetime import datetime, timezone
from email.utils import format_datetime
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('oci_watchdog', Path(__file__).resolve().parents[1] / 'ops/scripts/stream_v3_oci_watchdog.py')
watchdog = importlib.util.module_from_spec(spec)
spec.loader.exec_module(watchdog)

CONFIG = {'schema': 'stream.oci_watchdog.v1', 'tenancy': 'ocid1.tenancy.oc1..test',
          'user': 'ocid1.user.oc1..test', 'compartment': 'ocid1.compartment.oc1..test',
          'region': 'ap-tokyo-1', 'monitor': 'observation-owner', 'fingerprint': ':'.join(['ab'] * 16)}
STAMP = datetime(2026, 11, 2, 3, 0, tzinfo=timezone.utc)


class Raw(io.BytesIO):
    def read(self, size=-1, decode_content=False):
        return super().read(size)


class Response:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status
        self.headers = headers if headers is not None else {'Date': format_datetime(STAMP, usegmt=True)}
        self.raw = Raw(json.dumps(body if body is not None else {'failedMetricsCount': 0, 'failedMetrics': []}).encode())
        self.closed = False

    def close(self):
        self.closed = True


class Session:
    def __init__(self, head=None, post=None):
        self.head_response = head or Response(status=404)
        self.post_response = post or Response()
        self.posts = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def head(self, url, **kwargs):
        self.head_request = (url, kwargs)
        return self.head_response

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return self.post_response


class WatchdogTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.key = Path(self.tmp.name) / 'key.pem'
        self.key.write_text('test key; signer is injected at the network boundary')

    def emit(self, session, check=lambda: True, **kwargs):
        return watchdog.emit(CONFIG, self.key, check, send=True, session=session,
                             signer_factory=lambda *args: object(), **kwargs)

    def test_only_proved_local_health_sends_and_uses_cloud_time(self):
        session = Session()
        result = self.emit(session)
        self.assertEqual(result['status'], 'POINT_ACCEPTED')
        self.assertFalse(result['notification_verified'])
        url, kwargs = session.posts[0]
        data = json.loads(kwargs['data'])['metricData'][0]
        self.assertEqual(data['datapoints'][0]['timestamp'], STAMP.isoformat())
        self.assertEqual(kwargs['headers']['Date'], format_datetime(STAMP, usegmt=True))
        self.assertEqual(url, 'https://telemetry-ingestion.ap-tokyo-1.oraclecloud.com/20180401/metrics')
        self.assertFalse(kwargs['allow_redirects'])
        self.assertFalse(session.trust_env)
        self.assertTrue(session.closed and session.post_response.closed and session.head_response.closed)

    def test_dry_run_does_not_access_cloud_or_key(self):
        self.key.unlink()
        result = watchdog.emit(CONFIG, self.key, lambda: True)
        self.assertEqual(result['status'], 'READY_NOT_SENT')

    def test_unready_or_expired_during_exchange_never_posts(self):
        for readings in ([False], [True, False]):
            with self.subTest(readings=readings):
                session = Session()
                iterator = iter(readings)
                with self.assertRaisesRegex(ValueError, 'local_evidence'):
                    self.emit(session, lambda: next(iterator))
                self.assertEqual(session.posts, [])

    def test_bad_remote_clock_and_redirect_never_post(self):
        responses = [Response(status=302), Response(status=500), Response(headers={}),
                     Response(headers={'Date': 'invalid'}),
                     Response(headers={'Date': format_datetime(STAMP, usegmt=True), 'Age': '1'}),
                     Response(headers={'Date': 'Mon, 02 Nov 2026 12:00:00 +0900'})]
        for response in responses:
            with self.subTest(response=response.headers):
                session = Session(head=response)
                with self.assertRaises((ValueError, KeyError)):
                    self.emit(session)
                self.assertEqual(session.posts, [])
                self.assertTrue(response.closed)

    def test_clock_rtt_is_bounded_and_no_previous_time_is_reused(self):
        for end in (-1, 5.001):
            with self.subTest(end=end):
                ticks = iter([0, end])
                with self.assertRaises(ValueError):
                    watchdog.cloud_time(Session(), 'example.invalid', mono=lambda: next(ticks))
        session = Session()
        for _ in range(2):
            ticks = iter([0, 1])
            watchdog.cloud_time(session, 'example.invalid', mono=lambda: next(ticks))
            if _ == 0:
                first = session.head_request[0]
        self.assertNotEqual(first, session.head_request[0])

    def test_partial_acceptance_invalid_reply_and_http_errors_are_not_success(self):
        cases = [Response(status=401), Response(status=429), Response(status=500),
                 Response(body={}), Response(body=[]), Response(body={'failedMetricsCount': False}),
                 Response(body={'failedMetricsCount': 1}),
                 Response(body={'failedMetricsCount': 0, 'failedMetrics': [{}]})]
        large = Response()
        large.raw = Raw(b' ' * 16385)
        cases.append(large)
        for response in cases:
            with self.subTest(status=response.status_code):
                session = Session(post=response)
                with self.assertRaises(ValueError):
                    self.emit(session)
                self.assertEqual(len(session.posts), 1)
                self.assertTrue(response.closed)

    def test_default_exchange_budget_includes_host_suspend(self):
        with mock.patch.object(watchdog.time, 'clock_gettime', side_effect=[100, 106]) as clock:
            with self.assertRaisesRegex(ValueError, 'cloud_clock_unavailable'):
                watchdog.cloud_time(Session(), 'example.invalid')
            clock.assert_called_with(watchdog.time.CLOCK_BOOTTIME)

    def test_gzip_provider_response_is_decoded_before_acceptance(self):
        from urllib3.response import HTTPResponse
        response = Response()
        response.raw = HTTPResponse(body=io.BytesIO(gzip.compress(b'{"failedMetricsCount":0,"failedMetrics":[]}')),
                                    headers={'Content-Encoding': 'gzip'}, preload_content=False)
        self.assertEqual(self.emit(Session(post=response))['status'], 'POINT_ACCEPTED')

    def test_invalid_config_cannot_redirect_credentials_or_inject_query(self):
        for name, value in [('region', 'example.com/'), ('region', 'ap-tokyo-1\nHost: evil'),
                            ('monitor', 'x"}.absent()'), ('tenancy', ''), ('user', ''),
                            ('compartment', ''), ('fingerprint', '\n'), ('schema', 'other')]:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    watchdog.validate_config({**CONFIG, name: value})

    def test_alarm_is_disabled_until_baseline_and_subscription_are_verified(self):
        topic = 'ocid1.onstopic.oc1.ap-tokyo-1.test'
        live = watchdog.alarm(CONFIG, topic)
        fixture = watchdog.alarm({**CONFIG, 'monitor': 'observation-owner-validation'}, topic, validation=True)
        self.assertFalse(live['is_enabled'])
        self.assertEqual(live['pending_duration'], 'PT5M')
        self.assertIn('.absent(72h)', live['query'])
        self.assertIn('RESET', live['body'])
        self.assertNotEqual(live['query'], fixture['query'])
        self.assertTrue(fixture['display_name'].startswith('[TEST]'))
        with self.assertRaises(ValueError):
            watchdog.alarm(CONFIG, 'https://example.invalid')

    def test_symlink_key_is_rejected(self):
        link = self.key.with_name('link.pem')
        link.symlink_to(self.key)
        with self.assertRaisesRegex(ValueError, 'credential_invalid'):
            watchdog.emit(CONFIG, link, lambda: True, send=True, session=Session(),
                          signer_factory=lambda *args: object())

    def test_malformed_or_future_local_evidence_is_not_healthy(self):
        from stream_v3_external_watchdog_heartbeat import ready
        gateway = {'schema': 'stream.notification_gateway.v1', 'boot_id': 'a', 'mono': 90}
        for notification in ([], None, {}, {'notification_clock': []},
                             {'notification_clock': {'boot_id': 'a', 'mono': 101}},
                             {'notification_clock': {'boot_id': 'previous', 'mono': 90}}):
            with self.subTest(notification=notification):
                self.assertFalse(ready(gateway, notification, boot='a', mono=100, prometheus_ready=True))

    def test_official_sdk_signs_the_server_date_and_exact_payload(self):
        try:
            from oci.signer import Signer
        except ImportError:
            self.skipTest('official OCI SDK required for signature integration')
        import requests
        from cryptography.hazmat.primitives.asymmetric import padding, rsa
        from cryptography.hazmat.primitives import hashes, serialization
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.key.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                             serialization.NoEncryption()))
        session = Session()
        watchdog.emit(CONFIG, self.key, lambda: True, send=True, session=session)
        url, kwargs = session.posts[0]
        request = requests.Request('POST', url, data=kwargs['data'], headers=kwargs['headers'], auth=kwargs['auth']).prepare()
        self.assertEqual(request.headers['date'], format_datetime(STAMP, usegmt=True))
        auth = dict(part.strip().split('=', 1) for part in request.headers['authorization'].removeprefix('Signature ').split(','))
        headers = auth['headers'].strip('"').split()
        parts = {'(request-target)': 'post /20180401/metrics', **{k.lower(): v for k, v in request.headers.items()}}
        signed = '\n'.join(h + ': ' + parts[h] for h in headers).encode()
        key.public_key().verify(base64.b64decode(auth['signature'].strip('"')), signed, padding.PKCS1v15(), hashes.SHA256())


if __name__ == '__main__':
    unittest.main()
