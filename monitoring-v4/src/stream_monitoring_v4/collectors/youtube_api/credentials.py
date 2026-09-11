from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


MAX_CREDENTIAL_BYTES = 16 * 1024


@dataclass(frozen=True)
class OAuthCredentials:
    client_id: str
    client_secret: str
    refresh_token: str

    def __post_init__(self) -> None:
        if not self.client_id or len(self.client_id) > 4096:
            raise ValueError("OAuth client ID is missing or too large")
        if len(self.client_secret) > 4096:
            raise ValueError("OAuth client secret is too large")
        if not self.refresh_token or len(self.refresh_token) > 8192:
            raise ValueError("OAuth refresh token is missing or too large")
        for item in (self.client_id, self.client_secret, self.refresh_token):
            if item != item.strip() or any(ord(character) < 32 or ord(character) == 127 for character in item):
                raise ValueError(
                    "OAuth credential contains whitespace padding or a control character"
                )


def _read(path: Path, *, required: bool) -> str:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        if not required:
            return ""
        raise ValueError(f"required OAuth credential file is missing: {path.name}") from None
    except OSError as exc:
        raise ValueError(f"OAuth credential file is unreadable: {path.name}") from exc
    if len(raw) > MAX_CREDENTIAL_BYTES:
        raise ValueError(f"OAuth credential file is too large: {path.name}")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"OAuth credential file is not UTF-8: {path.name}") from exc


def read_credentials(directory: Path) -> OAuthCredentials:
    root = Path(directory)
    return OAuthCredentials(
        client_id=_read(root / "client-id", required=True),
        client_secret=_read(root / "client-secret", required=False),
        refresh_token=_read(root / "refresh-token", required=True),
    )
