from __future__ import annotations

import base64
import json
from pathlib import Path

import yaml

from tools.refresh_target_observer_credential import existing_token, replace_token, token_metadata


def jwt(payload: dict[str, object]) -> str:
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    return f"header.{encoded}.signature"


def kubeconfig(path: Path, token: str) -> None:
    path.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "users": [{"name": "observer", "user": {"token": token}}],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    path.chmod(0o640)


def test_token_metadata_never_returns_credential_value() -> None:
    token = jwt({"sub": "subject", "iat": 100, "exp": 200, "aud": ["cluster"]})
    metadata = token_metadata(token)
    assert metadata == {"subject": "subject", "issued_at": 100, "expires_at": 200, "audience": ["cluster"]}
    assert token not in json.dumps(metadata)


def test_atomic_replacement_preserves_mode_and_updates_only_token(tmp_path: Path) -> None:
    path = tmp_path / "observer.kubeconfig"
    old = jwt({"sub": "old", "iat": 100, "exp": 200})
    new = jwt({"sub": "new", "iat": 200, "exp": 300})
    kubeconfig(path, old)

    replace_token(path, new)

    assert existing_token(path) == new
    assert path.stat().st_mode & 0o777 == 0o640
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert payload["apiVersion"] == "v1"
    assert payload["kind"] == "Config"
