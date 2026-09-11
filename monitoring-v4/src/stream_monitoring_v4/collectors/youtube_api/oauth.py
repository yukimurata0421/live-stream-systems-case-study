from __future__ import annotations

import time
import urllib.parse
import urllib.request
from dataclasses import dataclass

from .credentials import OAuthCredentials
from .errors import RemoteFailure
from .http import OpenUrl, request_json, strict_urlopen


TOKEN_URL = "https://oauth2.googleapis.com/token"
YOUTUBE_READONLY_SCOPE = "https://www.googleapis.com/auth/youtube.readonly"
YOUTUBE_SCOPE_PREFIX = "https://www.googleapis.com/auth/youtube"


@dataclass(frozen=True)
class AccessToken:
    value: str
    expires_at: int
    refreshed: bool
    scope_class: str
    scope_count: int


def _scope_class(value: object) -> tuple[str, int]:
    if value is None or value == "":
        return "unreported", 0
    if not isinstance(value, str) or len(value) > 16 * 1024:
        raise RemoteFailure("response_invalid")
    scopes = value.split()
    if (
        not scopes
        or len(scopes) > 32
        or len(set(scopes)) != len(scopes)
        or any(
            len(scope) > 1024
            or scope != scope.strip()
            or any(ord(character) < 33 or ord(character) == 127 for character in scope)
            for scope in scopes
        )
    ):
        raise RemoteFailure("response_invalid")
    scope_set = set(scopes)
    if scope_set == {YOUTUBE_READONLY_SCOPE}:
        return "youtube_readonly_only", 1
    if any(scope.startswith(YOUTUBE_SCOPE_PREFIX) for scope in scope_set):
        return "youtube_broader", len(scope_set)
    return "mixed_or_other", len(scope_set)


class OAuthTokenProvider:
    def __init__(
        self,
        credentials: OAuthCredentials,
        *,
        timeout_sec: float = 10.0,
        max_response_bytes: int = 256 * 1024,
        minimum_ttl_sec: int = 300,
        opener: OpenUrl = strict_urlopen,
    ) -> None:
        self.credentials = credentials
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.max_response_bytes = max(1024, int(max_response_bytes))
        self.minimum_ttl_sec = max(60, int(minimum_ttl_sec))
        self.opener = opener
        self._value = ""
        self._expires_at = 0
        self._scope_class = "unavailable"
        self._scope_count = 0

    def invalidate(self) -> None:
        self._value = ""
        self._expires_at = 0
        self._scope_class = "unavailable"
        self._scope_count = 0

    def access_token(self, *, now_ts: int | None = None) -> AccessToken:
        now = int(time.time() if now_ts is None else now_ts)
        if self._value and self._expires_at - now >= self.minimum_ttl_sec:
            return AccessToken(
                self._value,
                self._expires_at,
                False,
                self._scope_class,
                self._scope_count,
            )
        form = {
            "client_id": self.credentials.client_id,
            "refresh_token": self.credentials.refresh_token,
            "grant_type": "refresh_token",
        }
        if self.credentials.client_secret:
            form["client_secret"] = self.credentials.client_secret
        body = urllib.parse.urlencode(form).encode("ascii")
        request = urllib.request.Request(
            TOKEN_URL,
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "stream-monitoring-v4-youtube-api/1.0",
            },
            method="POST",
        )
        payload = request_json(
            request,
            timeout_sec=self.timeout_sec,
            max_bytes=self.max_response_bytes,
            opener=self.opener,
        )
        value = payload.get("access_token")
        expires_in = payload.get("expires_in")
        token_type = payload.get("token_type", "Bearer")
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 16 * 1024
            or value != value.strip()
            or any(ord(character) < 32 or ord(character) == 127 for character in value)
            or type(expires_in) is not int
            or not 60 <= expires_in <= 86400
            or not isinstance(token_type, str)
            or token_type.lower() != "bearer"
        ):
            raise RemoteFailure("response_invalid")
        scope_class, scope_count = _scope_class(payload.get("scope"))
        self._value = value
        self._expires_at = now + expires_in
        self._scope_class = scope_class
        self._scope_count = scope_count
        return AccessToken(
            value,
            self._expires_at,
            True,
            scope_class,
            scope_count,
        )
