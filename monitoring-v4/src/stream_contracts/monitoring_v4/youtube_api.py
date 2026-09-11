from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, ClassVar, Mapping

from ._decode import boolean, contract_object, integer, object_array, text
from .time import parse_utc, require_not_before


PROBE_STATUSES = frozenset({"ok", "unknown"})
RESULT_KINDS = frozenset(
    {
        "observed",
        "active_broadcast_missing",
        "active_broadcast_ambiguous",
        "bound_stream_missing",
        "stream_missing",
        "oauth_auth",
        "oauth_http",
        "oauth_network",
        "oauth_timeout",
        "oauth_response_invalid",
        "oauth_response_too_large",
        "oauth_redirect_rejected",
        "api_auth",
        "api_quota",
        "api_rate_limited",
        "api_http",
        "api_upstream",
        "api_network",
        "api_timeout",
        "api_response_invalid",
        "api_response_too_large",
        "api_redirect_rejected",
        "api_pagination_limit",
        "collector_error",
    }
)
LIFECYCLE_STATUSES = frozenset(
    {
        "",
        "complete",
        "created",
        "live",
        "liveStarting",
        "ready",
        "revoked",
        "testStarting",
        "testing",
        "unrecognized",
    }
)
STREAM_STATUSES = frozenset(
    {"", "active", "created", "error", "inactive", "ready", "unrecognized"}
)
HEALTH_STATUSES = frozenset({"", "good", "ok", "bad", "noData", "unrecognized"})
ISSUE_SEVERITIES = frozenset({"info", "warning", "error", "unrecognized"})
OAUTH_SCOPE_CLASSES = frozenset(
    {
        "unavailable",
        "unreported",
        "youtube_readonly_only",
        "youtube_broader",
        "mixed_or_other",
    }
)
_TOKEN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,79}$")
_SHA256 = re.compile(r"[0-9a-f]{64}")


def _allowed(value: str, allowed: frozenset[str], field: str) -> str:
    if value not in allowed:
        raise ValueError(f"unsupported {field}: {value!r}")
    return value


def _digest(value: str, field: str) -> str:
    if value and not _SHA256.fullmatch(value):
        raise ValueError(f"{field} must be an empty string or lowercase SHA-256")
    return value


def _error_reason(value: str) -> str:
    if value and not _TOKEN.fullmatch(value):
        raise ValueError("error_reason must be an allowlisted token")
    return value


def _issues(values: object) -> tuple[dict[str, str], ...]:
    if not isinstance(values, (list, tuple)) or len(values) > 32:
        raise ValueError("configuration_issues must contain at most 32 objects")
    result: list[dict[str, str]] = []
    for raw in values:
        if not isinstance(raw, Mapping) or set(raw) != {"type", "severity"}:
            raise ValueError("configuration issue must contain exactly type and severity")
        issue_type = raw.get("type")
        severity = raw.get("severity")
        if not isinstance(issue_type, str) or not _TOKEN.fullmatch(issue_type):
            raise ValueError("configuration issue type must be an allowlisted token")
        if not isinstance(severity, str) or severity not in ISSUE_SEVERITIES:
            raise ValueError("configuration issue severity is unsupported")
        result.append({"type": issue_type, "severity": severity})
    return tuple(result)


@dataclass(frozen=True)
class YouTubeApiEvidence:
    SCHEMA: ClassVar[str] = "monitoring_v4.youtube_api_evidence.v1"

    collector_revision: str
    request_started_at: str
    collected_at: str
    probe_status: str
    result_kind: str
    http_status: int
    error_reason: str
    lifecycle_status: str
    stream_status: str
    stream_health_status: str
    configuration_issues: tuple[dict[str, str], ...]
    broadcast_id_sha256: str
    bound_stream_id_sha256: str
    active_broadcast_count: int
    api_request_count: int
    oauth_refresh_performed: bool
    oauth_scope_class: str
    oauth_scope_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.collector_revision, str) or not _TOKEN.fullmatch(
            self.collector_revision
        ):
            raise ValueError("collector_revision must be an allowlisted token")
        parse_utc(self.request_started_at, field="request_started_at")
        parse_utc(self.collected_at, field="collected_at")
        require_not_before(
            self.collected_at,
            self.request_started_at,
            later_field="collected_at",
            earlier_field="request_started_at",
        )
        _allowed(self.probe_status, PROBE_STATUSES, "probe_status")
        _allowed(self.result_kind, RESULT_KINDS, "result_kind")
        if type(self.http_status) is not int or not 0 <= self.http_status <= 599:
            raise ValueError("http_status must be an integer between 0 and 599")
        _error_reason(self.error_reason)
        _allowed(self.lifecycle_status, LIFECYCLE_STATUSES, "lifecycle_status")
        _allowed(self.stream_status, STREAM_STATUSES, "stream_status")
        _allowed(
            self.stream_health_status,
            HEALTH_STATUSES,
            "stream_health_status",
        )
        object.__setattr__(self, "configuration_issues", _issues(self.configuration_issues))
        _digest(self.broadcast_id_sha256, "broadcast_id_sha256")
        _digest(self.bound_stream_id_sha256, "bound_stream_id_sha256")
        if type(self.active_broadcast_count) is not int or not 0 <= self.active_broadcast_count <= 150:
            raise ValueError("active_broadcast_count must be between 0 and 150")
        if type(self.api_request_count) is not int or not 0 <= self.api_request_count <= 4:
            raise ValueError("api_request_count must be between 0 and 4")
        if type(self.oauth_refresh_performed) is not bool:
            raise ValueError("oauth_refresh_performed must be boolean")
        _allowed(self.oauth_scope_class, OAUTH_SCOPE_CLASSES, "oauth_scope_class")
        if type(self.oauth_scope_count) is not int or not 0 <= self.oauth_scope_count <= 32:
            raise ValueError("oauth_scope_count must be between 0 and 32")
        if self.oauth_scope_class in {"unavailable", "unreported"} and self.oauth_scope_count != 0:
            raise ValueError("unavailable or unreported OAuth scope must have count zero")
        if self.oauth_scope_class not in {"unavailable", "unreported"} and self.oauth_scope_count == 0:
            raise ValueError("reported OAuth scope must have a positive count")
        token_unavailable = self.result_kind.startswith("oauth_") or self.result_kind == "collector_error"
        if token_unavailable and self.oauth_scope_class != "unavailable":
            raise ValueError("OAuth or collector failure cannot claim an access-token scope")
        if not token_unavailable and self.oauth_scope_class == "unavailable":
            raise ValueError("API measurement must report or explicitly mark its token scope")
        success_kinds = {
            "observed",
            "active_broadcast_missing",
            "active_broadcast_ambiguous",
            "bound_stream_missing",
            "stream_missing",
        }
        if self.probe_status == "ok" and self.result_kind not in success_kinds:
            raise ValueError("successful probe has a failure result_kind")
        if self.probe_status == "unknown" and self.result_kind in success_kinds:
            raise ValueError("unknown probe has a successful result_kind")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "collector_revision": self.collector_revision,
            "request_started_at": self.request_started_at,
            "collected_at": self.collected_at,
            "probe_status": self.probe_status,
            "result_kind": self.result_kind,
            "http_status": self.http_status,
            "error_reason": self.error_reason,
            "lifecycle_status": self.lifecycle_status,
            "stream_status": self.stream_status,
            "stream_health_status": self.stream_health_status,
            "configuration_issues": [dict(item) for item in self.configuration_issues],
            "broadcast_id_sha256": self.broadcast_id_sha256,
            "bound_stream_id_sha256": self.bound_stream_id_sha256,
            "active_broadcast_count": self.active_broadcast_count,
            "api_request_count": self.api_request_count,
            "oauth_refresh_performed": self.oauth_refresh_performed,
            "oauth_scope_class": self.oauth_scope_class,
            "oauth_scope_count": self.oauth_scope_count,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "YouTubeApiEvidence":
        fields = frozenset(
            {
                "collector_revision",
                "request_started_at",
                "collected_at",
                "probe_status",
                "result_kind",
                "http_status",
                "error_reason",
                "lifecycle_status",
                "stream_status",
                "stream_health_status",
                "configuration_issues",
                "broadcast_id_sha256",
                "bound_stream_id_sha256",
                "active_broadcast_count",
                "api_request_count",
                "oauth_refresh_performed",
                "oauth_scope_class",
                "oauth_scope_count",
            }
        )
        raw = contract_object(value, schema=cls.SCHEMA, fields=fields)
        return cls(
            collector_revision=text(raw, "collector_revision"),
            request_started_at=text(raw, "request_started_at"),
            collected_at=text(raw, "collected_at"),
            probe_status=text(raw, "probe_status"),
            result_kind=text(raw, "result_kind"),
            http_status=integer(raw, "http_status"),
            error_reason=text(raw, "error_reason"),
            lifecycle_status=text(raw, "lifecycle_status"),
            stream_status=text(raw, "stream_status"),
            stream_health_status=text(raw, "stream_health_status"),
            configuration_issues=_issues(object_array(raw, "configuration_issues")),
            broadcast_id_sha256=text(raw, "broadcast_id_sha256"),
            bound_stream_id_sha256=text(raw, "bound_stream_id_sha256"),
            active_broadcast_count=integer(raw, "active_broadcast_count"),
            api_request_count=integer(raw, "api_request_count"),
            oauth_refresh_performed=boolean(raw, "oauth_refresh_performed"),
            oauth_scope_class=text(raw, "oauth_scope_class"),
            oauth_scope_count=integer(raw, "oauth_scope_count"),
        )
