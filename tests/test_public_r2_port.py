"""Portable R2 entrypoints, destinations and diagnostics; no provider access."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / 'ops/public-publisher/site'
with patch.object(sys, 'path', [str(SITE / 'scripts'), *sys.path]):
    import publication_receipts as receipts
    spec = importlib.util.spec_from_file_location(
        'portable_r2_entry', SITE / 'scripts/push_to_r2.py'
    )
    publisher = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = publisher
    spec.loader.exec_module(publisher)


class PublicR2PortTests(unittest.TestCase):
    def test_manifest_covers_the_packaged_page_and_all_static_assets(self):
        manifest = json.loads((SITE / 'scripts/r2-public-files.json').read_text())
        names = set(manifest['files'])
        for path in (SITE / 'public').rglob('*'):
            if (path.is_file() and path.suffix in {'.js', '.css', '.html'}
                    and not path.name.startswith('stream-v3-')):
                self.assertIn(path.relative_to(SITE / 'public').as_posix(), names)
        frozen = {
            name: (SITE / 'public' / name).read_bytes()
            for name in names
            if name.endswith(('.js', '.css')) or name == 'index.html'
        }
        release = publisher.prepare_site_release(frozen)
        self.assertEqual(
            json.loads(release['site-release.json'])['schema'], 'public.site_release.v1'
        )
        self.assertEqual(set(manifest['live']), set(receipts.LIVE_FILES))

    def test_missing_health_configuration_makes_no_request(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(
            receipts.urllib.request, 'urlopen'
        ) as opened:
            result = receipts.health_observation(deadline=time.monotonic() + 1)
            self.assertEqual(result['code'], 'HEALTH_NOT_CONFIGURED')
            opened.assert_not_called()

    def test_health_configuration_rejects_credentials_redirect_shapes_and_http(self):
        for url in [
            'http://status.example.invalid/health/publication',
            'https://user:password@status.example.invalid/health/publication',
            'https://status.example.invalid/health/publication?token=value',
            'https://status.example.invalid/other',
            'https://status.example.invalid:invalid/health/publication',
            'https://[invalid/health/publication',
        ]:
            with self.subTest(url=url), patch.object(
                receipts.urllib.request, 'urlopen'
            ) as opened:
                result = receipts.health_observation(deadline=time.monotonic() + 1, url=url)
                self.assertEqual(result['code'], 'HEALTH_URL_INVALID')
                opened.assert_not_called()

    def test_upload_prefix_is_explicitly_nonroot_and_path_safe(self):
        for prefix in ['', '/', '../private/', 'public/../', 'public', 'a//b/']:
            with patch.object(publisher, 'PREFIX', prefix):
                with self.assertRaises(publisher.PublicationError):
                    publisher.validate_prefix()
        for prefix in ['public/', 'status/site/']:
            with patch.object(publisher, 'PREFIX', prefix):
                publisher.validate_prefix()

    def test_systemd_examples_are_portable_and_keep_ipv6_and_private_dns_separate(self):
        directory = SITE / 'systemd/yukimurata-public-push.service.d'
        self.assertIn(
            'AF_UNIX AF_INET AF_INET6', (directory / '96-r2-ipv6-preferred.conf').read_text()
        )
        self.assertIn(
            '/etc/stream-v3-public-publisher/resolv.conf:/etc/resolv.conf',
            (directory / '95-r2-network.conf').read_text(),
        )
        self.assertIn('push_to_r2.py', (directory / 'r2.conf.example').read_text())


if __name__ == '__main__':
    unittest.main()
