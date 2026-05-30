from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OAuthConfig:
    enabled: bool
    client_id: str
    client_secret: str
    refresh_token: str
    token_url: str
    timeout_sec: int
    min_token_ttl_sec: int

    def is_configured(self) -> bool:
        return bool(self.client_id and self.refresh_token)

    def refresh_form(self) -> dict[str, str]:
        form = {
            "client_id": self.client_id,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }
        if self.client_secret:
            form["client_secret"] = self.client_secret
        return form

    def public_status(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "configured": self.is_configured(),
            "token_url_configured": bool(self.token_url),
            "timeout_sec": self.timeout_sec,
            "min_token_ttl_sec": self.min_token_ttl_sec,
        }

