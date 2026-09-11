from __future__ import annotations

import hashlib
import time

from stream_contracts.monitoring_v4.time import utc_text
from stream_contracts.monitoring_v4.youtube_api import YouTubeApiEvidence

from .client import YouTubeReadClient
from .credentials import OAuthCredentials
from .errors import PaginationLimit, RemoteFailure
from .http import OpenUrl, strict_urlopen
from .oauth import OAuthTokenProvider


COLLECTOR_REVISION = "monitoring-v4-youtube-api-collector-r1"


def _digest(kind: str, value: str) -> str:
    if not value:
        return ""
    return hashlib.sha256(f"{kind}:v1:{value}".encode("utf-8")).hexdigest()


def _result_kind(prefix: str, kind: str) -> str:
    mapped = {
        "auth": "auth",
        "quota": "quota",
        "rate_limited": "rate_limited",
        "http": "http",
        "upstream": "upstream",
        "network": "network",
        "timeout": "timeout",
        "response_invalid": "response_invalid",
        "response_too_large": "response_too_large",
        "redirect_rejected": "redirect_rejected",
    }.get(kind, "http")
    candidate = f"{prefix}_{mapped}"
    if prefix == "oauth" and mapped in {"quota", "rate_limited", "upstream"}:
        return "oauth_http"
    return candidate


class YouTubeApiCollector:
    def __init__(
        self,
        credentials: OAuthCredentials,
        *,
        timeout_sec: float = 10.0,
        max_response_bytes: int = 512 * 1024,
        opener: OpenUrl = strict_urlopen,
    ) -> None:
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.max_response_bytes = max(1024, int(max_response_bytes))
        self.opener = opener
        self.tokens = OAuthTokenProvider(
            credentials,
            timeout_sec=self.timeout_sec,
            max_response_bytes=min(self.max_response_bytes, 256 * 1024),
            opener=opener,
        )

    def _failure(
        self,
        *,
        started_ts: int,
        collected_ts: int,
        prefix: str,
        failure: RemoteFailure,
        request_count: int,
        refreshed: bool,
        scope_class: str = "unavailable",
        scope_count: int = 0,
    ) -> YouTubeApiEvidence:
        if prefix == "api" and failure.kind == "auth" and failure.http_status == 401:
            self.tokens.invalidate()
        return YouTubeApiEvidence(
            collector_revision=COLLECTOR_REVISION,
            request_started_at=utc_text(started_ts),
            collected_at=utc_text(max(started_ts, collected_ts)),
            probe_status="unknown",
            result_kind=_result_kind(prefix, failure.kind),
            http_status=failure.http_status,
            error_reason=failure.error_reason,
            lifecycle_status="",
            stream_status="",
            stream_health_status="",
            configuration_issues=(),
            broadcast_id_sha256="",
            bound_stream_id_sha256="",
            active_broadcast_count=0,
            api_request_count=min(4, max(0, request_count)),
            oauth_refresh_performed=refreshed,
            oauth_scope_class=scope_class,
            oauth_scope_count=scope_count,
        )

    def _collector_error(
        self,
        *,
        started_ts: int,
        deterministic: bool,
    ) -> YouTubeApiEvidence:
        collected_ts = started_ts if deterministic else max(started_ts, int(time.time()))
        return YouTubeApiEvidence(
            collector_revision=COLLECTOR_REVISION,
            request_started_at=utc_text(started_ts),
            collected_at=utc_text(collected_ts),
            probe_status="unknown",
            result_kind="collector_error",
            http_status=0,
            error_reason="",
            lifecycle_status="",
            stream_status="",
            stream_health_status="",
            configuration_issues=(),
            broadcast_id_sha256="",
            bound_stream_id_sha256="",
            active_broadcast_count=0,
            api_request_count=0,
            oauth_refresh_performed=False,
            oauth_scope_class="unavailable",
            oauth_scope_count=0,
        )

    def collect(self, *, now_ts: int | None = None) -> YouTubeApiEvidence:
        started_ts = int(time.time() if now_ts is None else now_ts)
        try:
            return self._collect(started_ts=started_ts, deterministic=now_ts is not None)
        except Exception:
            # Keep an unavailable measurement observable without leaking an
            # exception string that may contain request or credential data.
            self.tokens.invalidate()
            return self._collector_error(
                started_ts=started_ts,
                deterministic=now_ts is not None,
            )

    def _collect(
        self,
        *,
        started_ts: int,
        deterministic: bool,
    ) -> YouTubeApiEvidence:
        try:
            token = self.tokens.access_token(now_ts=started_ts)
        except RemoteFailure as failure:
            return self._failure(
                started_ts=started_ts,
                collected_ts=int(time.time()) if not deterministic else started_ts,
                prefix="oauth",
                failure=failure,
                request_count=0,
                refreshed=False,
            )
        client = YouTubeReadClient(
            token.value,
            timeout_sec=self.timeout_sec,
            max_response_bytes=self.max_response_bytes,
            opener=self.opener,
        )
        try:
            values = client.probe()
        except PaginationLimit:
            collected_ts = int(time.time()) if not deterministic else started_ts
            return YouTubeApiEvidence(
                collector_revision=COLLECTOR_REVISION,
                request_started_at=utc_text(started_ts),
                collected_at=utc_text(max(started_ts, collected_ts)),
                probe_status="unknown",
                result_kind="api_pagination_limit",
                http_status=0,
                error_reason="",
                lifecycle_status="",
                stream_status="",
                stream_health_status="",
                configuration_issues=(),
                broadcast_id_sha256="",
                bound_stream_id_sha256="",
                active_broadcast_count=0,
                api_request_count=min(4, client.request_count),
                oauth_refresh_performed=token.refreshed,
                oauth_scope_class=token.scope_class,
                oauth_scope_count=token.scope_count,
            )
        except RemoteFailure as failure:
            return self._failure(
                started_ts=started_ts,
                collected_ts=int(time.time()) if not deterministic else started_ts,
                prefix="api",
                failure=failure,
                request_count=client.request_count,
                refreshed=token.refreshed,
                scope_class=token.scope_class,
                scope_count=token.scope_count,
            )
        collected_ts = int(time.time()) if not deterministic else started_ts
        return YouTubeApiEvidence(
            collector_revision=COLLECTOR_REVISION,
            request_started_at=utc_text(started_ts),
            collected_at=utc_text(max(started_ts, collected_ts)),
            probe_status="ok",
            result_kind=values.result_kind,
            http_status=0,
            error_reason="",
            lifecycle_status=values.lifecycle_status,
            stream_status=values.stream_status,
            stream_health_status=values.stream_health_status,
            configuration_issues=values.configuration_issues,
            broadcast_id_sha256=_digest("youtube-broadcast-id", values.broadcast_id),
            bound_stream_id_sha256=_digest("youtube-stream-id", values.bound_stream_id),
            active_broadcast_count=values.active_broadcast_count,
            api_request_count=min(4, client.request_count),
            oauth_refresh_performed=token.refreshed,
            oauth_scope_class=token.scope_class,
            oauth_scope_count=token.scope_count,
        )
