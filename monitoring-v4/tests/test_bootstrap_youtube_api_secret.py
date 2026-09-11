from __future__ import annotations

import base64
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from ops.scripts import monitoring_v4_bootstrap_youtube_api_secret as bootstrap


VALUES = {
    "client-id": "private-client-id",
    "client-secret": "private-client-secret",
    "refresh-token": "private-refresh-token",
}


def _secret(values: dict[str, str] = VALUES) -> dict[str, object]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": bootstrap.SECRET_NAME,
            "namespace": bootstrap.NAMESPACE,
            "resourceVersion": "1",
        },
        "type": "Opaque",
        "immutable": True,
        "data": {
            key: base64.b64encode(value.encode("utf-8")).decode("ascii")
            for key, value in values.items()
        },
    }


class YouTubeApiSecretBootstrapTests(unittest.TestCase):
    def test_private_stable_env_is_parsed_without_sourcing_shell(self) -> None:
        content = "\n".join(
            (
                "IGNORED=$(dangerous-command)",
                "YTW_OAUTH_CLIENT_ID=private-client-id",
                'YTW_OAUTH_CLIENT_SECRET="private-client-secret"',
                "YTW_OAUTH_REFRESH_TOKEN=private-refresh-token",
                "YTW_OAUTH_TOKEN_URL=https://attacker.invalid/token",
            )
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "source.env"
            path.write_text(content, encoding="utf-8")
            path.chmod(0o600)
            decoded = bootstrap._read_stable_private_file(
                path,
                allowed_owner_uid=os.getuid(),
            )
        self.assertEqual(bootstrap._credentials(decoded), VALUES)

    def test_broad_permissions_symlink_and_duplicate_key_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = root / "source.env"
            path.write_text("YTW_OAUTH_CLIENT_ID=value\n", encoding="utf-8")
            path.chmod(0o644)
            with self.assertRaisesRegex(RuntimeError, "group/world"):
                bootstrap._read_stable_private_file(
                    path,
                    allowed_owner_uid=os.getuid(),
                )
            path.chmod(0o600)
            link = root / "source-link.env"
            link.symlink_to(path)
            with self.assertRaisesRegex(RuntimeError, "symlink"):
                bootstrap._read_stable_private_file(
                    link,
                    allowed_owner_uid=os.getuid(),
                )
        duplicate = "\n".join(
            (
                "YTW_OAUTH_CLIENT_ID=a",
                "YTW_OAUTH_CLIENT_ID=b",
                "YTW_OAUTH_CLIENT_SECRET=",
                "YTW_OAUTH_REFRESH_TOKEN=c",
            )
        )
        with self.assertRaisesRegex(RuntimeError, "duplicate"):
            bootstrap._credentials(duplicate)
        with self.assertRaisesRegex(RuntimeError, "whitespace padding"):
            bootstrap._decode_env_value(" private-refresh-token", key="refresh-token")

    def test_existing_exact_secret_is_unchanged_and_mismatch_requires_rotation(self) -> None:
        with patch.object(bootstrap, "_existing", return_value=_secret()), patch.object(
            bootstrap,
            "_create",
        ) as create:
            self.assertEqual(bootstrap.ensure_secret("k3s", VALUES), "unchanged")
        create.assert_not_called()
        mismatched = _secret({**VALUES, "refresh-token": "different"})
        with patch.object(bootstrap, "_existing", return_value=mismatched), patch.object(
            bootstrap,
            "_create",
        ) as create:
            with self.assertRaisesRegex(RuntimeError, "explicit credential rotation"):
                bootstrap.ensure_secret("k3s", VALUES)
        create.assert_not_called()

    def test_create_race_accepts_only_the_same_exact_winner(self) -> None:
        with patch.object(bootstrap, "_existing", side_effect=[None, _secret()]), patch.object(
            bootstrap,
            "_create",
            return_value=False,
        ):
            self.assertEqual(
                bootstrap.ensure_secret("k3s", VALUES),
                "unchanged_after_create_race",
            )
        with patch.object(
            bootstrap,
            "_existing",
            side_effect=[None, _secret({**VALUES, "client-id": "other"})],
        ), patch.object(bootstrap, "_create", return_value=False):
            with self.assertRaisesRegex(RuntimeError, "create race"):
                bootstrap.ensure_secret("k3s", VALUES)

    def test_main_summary_never_prints_values(self) -> None:
        source = "\n".join(
            (
                "YTW_OAUTH_CLIENT_ID=private-client-id",
                "YTW_OAUTH_CLIENT_SECRET=private-client-secret",
                "YTW_OAUTH_REFRESH_TOKEN=private-refresh-token",
            )
        )
        capture = io.StringIO()
        with patch.object(
            bootstrap,
            "_read_stable_private_file",
            return_value=source,
        ), patch.object(bootstrap, "ensure_secret", return_value="created"), redirect_stdout(capture):
            self.assertEqual(bootstrap.main([]), 0)
        payload = json.loads(capture.getvalue())
        self.assertFalse(payload["values_printed"])
        self.assertFalse(payload["source_token_url_copied"])
        for value in VALUES.values():
            self.assertNotIn(value, capture.getvalue())


if __name__ == "__main__":
    unittest.main()
