from __future__ import annotations

import json
import subprocess
import unittest
from unittest.mock import patch

from ops.scripts import monitoring_v4_bootstrap_db_secrets as bootstrap


def _existing_secret(data: dict[str, str], *, version: str = "1") -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "database-roles",
            "namespace": bootstrap.NAMESPACE,
            "resourceVersion": version,
        },
        "type": "Opaque",
        "data": data,
    }


class BootstrapSecretTests(unittest.TestCase):
    def test_create_race_preserves_winning_secret_value(self) -> None:
        initial = _existing_secret({}, version="1")
        winner = _existing_secret({"maintenance-password": "winner"}, version="2")
        with patch.object(bootstrap, "_replace", return_value=False) as replace, patch.object(
            bootstrap,
            "_existing",
            return_value=winner,
        ):
            changed = bootstrap._extend_existing(
                "k3s",
                "database-roles",
                initial,
                {"maintenance-password": "loser"},
            )
        self.assertEqual(changed, [])
        self.assertEqual(replace.call_count, 1)

    def test_non_notfound_read_error_is_not_misclassified_as_missing(self) -> None:
        denied = subprocess.CompletedProcess(
            args=[],
            returncode=1,
            stdout="",
            stderr="Error from server (Forbidden)",
        )
        with patch.object(bootstrap.subprocess, "run", return_value=denied):
            with self.assertRaisesRegex(RuntimeError, "failed to read"):
                bootstrap._existing("k3s", "database-roles")

    def test_existing_secret_requires_resource_version_and_exact_identity(self) -> None:
        malformed = _existing_secret({}, version="")
        result = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps(malformed),
            stderr="",
        )
        with patch.object(bootstrap.subprocess, "run", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "invalid structure"):
                bootstrap._existing("k3s", "database-roles")


if __name__ == "__main__":
    unittest.main()
