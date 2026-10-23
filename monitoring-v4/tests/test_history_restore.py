from __future__ import annotations

import json
from pathlib import Path
import subprocess
import unittest

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]


class HistoryRestoreTests(unittest.TestCase):
    def test_restore_attestation_preserves_v6_and_accepts_exact_v7(self):
        schema = json.loads((ROOT / 'src/stream_contracts/monitoring_v4/schema/postgresql_restore_verification.v1.json').read_text())
        validator = Draft202012Validator(schema['properties']['schema_versions'])
        for versions in (list(range(1, 7)), list(range(1, 8))):
            with self.subTest(versions=versions):
                self.assertEqual(list(validator.iter_errors(versions)), [])
        for versions in ([1, 2, 3, 4, 5, 7], list(range(1, 9)), [1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 6, '7']):
            with self.subTest(versions=versions):
                self.assertTrue(list(validator.iter_errors(versions)))

    def test_restore_script_syntax_and_emitted_versions_follow_observed_database(self):
        text = (ROOT / 'deploy/k3s/restore-smoke-job.yaml').read_text()
        start = text.index('                  umask 077', text.index('name: restore'))
        end = text.index('              resources:', start)
        script = '\n'.join(line[18:] for line in text[start:end].splitlines())
        result = subprocess.run(['sh', '-n'], input=script, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('"schema_versions":[%s]', script)
        self.assertIn('"${schema_versions}" "${shadow_cycles}"', script)
        self.assertIn('SELECT COUNT(*) FROM metric_windows', script)
