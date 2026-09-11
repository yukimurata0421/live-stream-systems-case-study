from __future__ import annotations

import re
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .errors import PaginationLimit, RemoteFailure
from .http import OpenUrl, request_json, strict_urlopen


API_BASE = "https://www.googleapis.com/youtube/v3"
MAX_BROADCAST_PAGES = 3
_LIFECYCLE_PRIORITY = {
    "live": 50,
    "liveStarting": 40,
    "testing": 30,
    "testStarting": 20,
    "ready": 10,
    "created": 5,
}
_ISSUE_TOKEN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,79}$")


@dataclass(frozen=True)
class ProbeValues:
    result_kind: str
    lifecycle_status: str = ""
    stream_status: str = ""
    stream_health_status: str = ""
    configuration_issues: tuple[dict[str, str], ...] = ()
    broadcast_id: str = ""
    bound_stream_id: str = ""
    active_broadcast_count: int = 0


def _status(value: object, allowed: frozenset[str]) -> str:
    raw = value.strip() if isinstance(value, str) else ""
    return raw if raw in allowed else ("unrecognized" if raw else "")


def _identifier(value: object, *, required: bool) -> str:
    if value is None and not required:
        return ""
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 256
        or value != value.strip()
        or any(ord(character) <= 32 or ord(character) == 127 for character in value)
    ):
        raise RemoteFailure("response_invalid")
    return value


def _issue_token(value: object) -> str:
    raw = value.strip() if isinstance(value, str) else ""
    if _ISSUE_TOKEN.fullmatch(raw):
        return raw
    return "unrecognized"


def _issues(value: object) -> tuple[dict[str, str], ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or len(value) > 32:
        raise RemoteFailure("response_invalid")
    output: list[dict[str, str]] = []
    for raw in value:
        if not isinstance(raw, Mapping):
            raise RemoteFailure("response_invalid")
        severity_value = raw.get("severity")
        severity = (
            severity_value.strip().lower()
            if isinstance(severity_value, str)
            else ""
        )
        if severity not in {"info", "warning", "error"}:
            severity = "unrecognized"
        output.append(
            {
                "type": _issue_token(raw.get("type")),
                "severity": severity,
            }
        )
    return tuple(output)


class YouTubeReadClient:
    def __init__(
        self,
        access_token: str,
        *,
        timeout_sec: float = 10.0,
        max_response_bytes: int = 512 * 1024,
        opener: OpenUrl = strict_urlopen,
    ) -> None:
        if not access_token:
            raise ValueError("access token is required")
        self._access_token = access_token
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.max_response_bytes = max(1024, int(max_response_bytes))
        self.opener = opener
        self.request_count = 0

    def _get(self, endpoint: str, params: Mapping[str, str]) -> Mapping[str, Any]:
        if endpoint not in {"liveBroadcasts", "liveStreams"}:
            raise ValueError("unsupported YouTube API endpoint")
        query = urllib.parse.urlencode(dict(params))
        request = urllib.request.Request(
            f"{API_BASE}/{endpoint}?{query}",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._access_token}",
                "User-Agent": "stream-monitoring-v4-youtube-api/1.0",
            },
            method="GET",
        )
        self.request_count += 1
        return request_json(
            request,
            timeout_sec=self.timeout_sec,
            max_bytes=self.max_response_bytes,
            opener=self.opener,
        )

    def _active_broadcasts(self) -> list[Mapping[str, Any]]:
        items: list[Mapping[str, Any]] = []
        page_token = ""
        for page in range(MAX_BROADCAST_PAGES):
            params = {
                "part": "id,contentDetails,status",
                "broadcastStatus": "active",
                "broadcastType": "all",
                "maxResults": "50",
            }
            if page_token:
                params["pageToken"] = page_token
            payload = self._get("liveBroadcasts", params)
            raw_items = payload.get("items")
            if not isinstance(raw_items, list) or not all(
                isinstance(item, Mapping) for item in raw_items
            ) or len(raw_items) > 50:
                raise RemoteFailure("response_invalid")
            items.extend(raw_items)
            raw_token = payload.get("nextPageToken", "")
            if not isinstance(raw_token, str):
                raise RemoteFailure("response_invalid")
            if (
                raw_token != raw_token.strip()
                or len(raw_token) > 4096
                or any(
                    ord(character) <= 32 or ord(character) == 127
                    for character in raw_token
                )
            ):
                raise RemoteFailure("response_invalid")
            page_token = raw_token
            if not page_token:
                return items
            if page == MAX_BROADCAST_PAGES - 1:
                raise PaginationLimit("active broadcast pagination exceeds limit")
        return items

    @staticmethod
    def _select(items: list[Mapping[str, Any]]) -> Mapping[str, Any] | None:
        if not items:
            return None
        ranked: list[tuple[int, Mapping[str, Any]]] = []
        for item in items:
            status = item.get("status")
            lifecycle = (
                str(status.get("lifeCycleStatus", "")).strip()
                if isinstance(status, Mapping)
                else ""
            )
            ranked.append((_LIFECYCLE_PRIORITY.get(lifecycle, 0), item))
        highest = max(rank for rank, _item in ranked)
        selected = [item for rank, item in ranked if rank == highest]
        return selected[0] if len(selected) == 1 else None

    def probe(self) -> ProbeValues:
        broadcasts = self._active_broadcasts()
        if not broadcasts:
            return ProbeValues(
                "active_broadcast_missing",
                active_broadcast_count=0,
            )
        selected = self._select(broadcasts)
        if selected is None:
            return ProbeValues(
                "active_broadcast_ambiguous",
                active_broadcast_count=len(broadcasts),
            )
        status = selected.get("status")
        lifecycle = _status(
            status.get("lifeCycleStatus") if isinstance(status, Mapping) else "",
            frozenset(
                {
                    "complete",
                    "created",
                    "live",
                    "liveStarting",
                    "ready",
                    "revoked",
                    "testStarting",
                    "testing",
                }
            ),
        )
        broadcast_id = _identifier(selected.get("id"), required=True)
        content = selected.get("contentDetails")
        if content is not None and not isinstance(content, Mapping):
            raise RemoteFailure("response_invalid")
        bound_stream_id = _identifier(
            content.get("boundStreamId") if isinstance(content, Mapping) else None,
            required=False,
        )
        if not bound_stream_id:
            return ProbeValues(
                "bound_stream_missing",
                lifecycle_status=lifecycle,
                broadcast_id=broadcast_id,
                active_broadcast_count=len(broadcasts),
            )
        payload = self._get(
            "liveStreams",
            {"part": "status", "id": bound_stream_id},
        )
        raw_streams = payload.get("items")
        if not isinstance(raw_streams, list) or not all(
            isinstance(item, Mapping) for item in raw_streams
        ):
            raise RemoteFailure("response_invalid")
        if not raw_streams:
            return ProbeValues(
                "stream_missing",
                lifecycle_status=lifecycle,
                broadcast_id=broadcast_id,
                bound_stream_id=bound_stream_id,
                active_broadcast_count=len(broadcasts),
            )
        if len(raw_streams) != 1:
            raise RemoteFailure("response_invalid")
        stream = raw_streams[0]
        stream_id = _identifier(stream.get("id"), required=True)
        if stream_id != bound_stream_id:
            raise RemoteFailure("response_invalid")
        stream_status = stream.get("status")
        if not isinstance(stream_status, Mapping):
            raise RemoteFailure("response_invalid")
        health = stream_status.get("healthStatus")
        if health is not None and not isinstance(health, Mapping):
            raise RemoteFailure("response_invalid")
        return ProbeValues(
            "observed",
            lifecycle_status=lifecycle,
            stream_status=_status(
                stream_status.get("streamStatus"),
                frozenset({"active", "created", "error", "inactive", "ready"}),
            ),
            stream_health_status=_status(
                health.get("status") if isinstance(health, Mapping) else "",
                frozenset({"good", "ok", "bad", "noData"}),
            ),
            configuration_issues=_issues(
                health.get("configurationIssues")
                if isinstance(health, Mapping)
                else None
            ),
            broadcast_id=broadcast_id,
            bound_stream_id=bound_stream_id,
            active_broadcast_count=len(broadcasts),
        )
