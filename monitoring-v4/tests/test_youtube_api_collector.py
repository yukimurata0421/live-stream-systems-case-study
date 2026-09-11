from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
import urllib.error
import urllib.request
from contextlib import redirect_stdout
from email.message import Message
from pathlib import Path
from unittest.mock import patch

from stream_contracts.monitoring_v4.time import utc_text
from stream_contracts.monitoring_v4.youtube_api import YouTubeApiEvidence
from stream_monitoring_v4.adapters.youtube_api import YouTubeApiEvidenceAdapter
from stream_monitoring_v4.collectors.youtube_api import http as youtube_http
from stream_monitoring_v4.collectors.youtube_api.credentials import (
    OAuthCredentials,
    read_credentials,
)
from stream_monitoring_v4.collectors.youtube_api.http import RemoteFailure, request_json
from stream_monitoring_v4.collectors.youtube_api.probe import YouTubeApiCollector
from stream_monitoring_v4.commands import youtube_api_collector as command
from stream_monitoring_v4.domains.reducer import reduce_domain
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES

from tests.helpers import BASE_TS, observation


class _Response:
    def __init__(self, payload: object, *, url: str) -> None:
        self.body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.url = url
        self.headers = Message()
        self.headers["Content-Type"] = "application/json"
        self.headers["Content-Length"] = str(len(self.body))

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def geturl(self) -> str:
        return self.url

    def read(self, maximum: int = -1) -> bytes:
        return self.body if maximum < 0 else self.body[:maximum]


class _QueueOpener:
    def __init__(self, responses: list[object]) -> None:
        self.responses = list(responses)
        self.requests: list[urllib.request.Request] = []

    def __call__(self, request: urllib.request.Request, *, timeout: float) -> object:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError("unexpected HTTP request")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return _Response(response, url=request.full_url)


def _evidence(**overrides: object) -> YouTubeApiEvidence:
    values: dict[str, object] = {
        "collector_revision": "monitoring-v4-youtube-api-collector-r1",
        "request_started_at": utc_text(BASE_TS),
        "collected_at": utc_text(BASE_TS),
        "probe_status": "ok",
        "result_kind": "observed",
        "http_status": 0,
        "error_reason": "",
        "lifecycle_status": "live",
        "stream_status": "active",
        "stream_health_status": "good",
        "configuration_issues": (),
        "broadcast_id_sha256": "a" * 64,
        "bound_stream_id_sha256": "b" * 64,
        "active_broadcast_count": 1,
        "api_request_count": 2,
        "oauth_refresh_performed": True,
        "oauth_scope_class": "youtube_readonly_only",
        "oauth_scope_count": 1,
    }
    values.update(overrides)
    return YouTubeApiEvidence(**values)  # type: ignore[arg-type]


def _success_responses(*, health: str = "good") -> list[object]:
    return [
        {
            "access_token": "private-access-token",
            "expires_in": 3600,
            "token_type": "Bearer",
            "scope": "https://www.googleapis.com/auth/youtube",
        },
        {
            "items": [
                {
                    "id": "raw-broadcast-id",
                    "contentDetails": {"boundStreamId": "raw-stream-id"},
                    "status": {"lifeCycleStatus": "live"},
                }
            ]
        },
        {
            "items": [
                {
                    "id": "raw-stream-id",
                    "status": {
                        "streamStatus": "active",
                        "healthStatus": {
                            "status": health,
                            "configurationIssues": [
                                {
                                    "type": "bitrateHigh",
                                    "severity": "info",
                                    "reason": "private provider prose",
                                    "description": "private provider prose",
                                }
                            ],
                        },
                    }
                }
            ]
        },
    ]


class YouTubeApiContractTests(unittest.TestCase):
    def test_contract_round_trip_is_strict_and_cross_field_validated(self) -> None:
        payload = _evidence().to_dict()
        self.assertEqual(YouTubeApiEvidence.from_dict(payload).to_dict(), payload)
        invalid = dict(payload)
        invalid["unexpected"] = True
        with self.assertRaises(ValueError):
            YouTubeApiEvidence.from_dict(invalid)
        with self.assertRaisesRegex(ValueError, "successful probe"):
            _evidence(result_kind="api_timeout")
        with self.assertRaisesRegex(ValueError, "unknown probe"):
            _evidence(probe_status="unknown", result_kind="observed")

    def test_http_rejects_redirects_and_non_allowlisted_destinations(self) -> None:
        request = urllib.request.Request("https://www.googleapis.com/youtube/v3/liveStreams")
        headers = Message()
        redirect = urllib.error.HTTPError(
            request.full_url,
            302,
            "redirect",
            headers,
            io.BytesIO(b""),
        )
        with self.assertRaises(RemoteFailure) as raised:
            request_json(request, timeout_sec=1, max_bytes=1024, opener=_QueueOpener([redirect]))
        self.assertEqual(raised.exception.kind, "redirect_rejected")
        rejected = urllib.request.Request("https://example.invalid/value")
        with self.assertRaises(RemoteFailure) as raised:
            request_json(rejected, timeout_sec=1, max_bytes=1024)
        self.assertEqual(raised.exception.kind, "redirect_rejected")
        wrong_port = urllib.request.Request("https://www.googleapis.com:444/value")
        with self.assertRaises(RemoteFailure) as raised:
            request_json(wrong_port, timeout_sec=1, max_bytes=1024)
        self.assertEqual(raised.exception.kind, "redirect_rejected")

    def test_http_disables_environment_proxies_and_rejects_ambiguous_json(self) -> None:
        with patch.dict(
            os.environ,
            {"HTTPS_PROXY": "http://proxy.invalid:8080"},
            clear=False,
        ):
            opener = youtube_http._build_strict_opener()
        proxy_handlers = [
            handler
            for handler in opener.handlers
            if isinstance(handler, urllib.request.ProxyHandler)
        ]
        self.assertEqual(proxy_handlers, [])
        with self.assertRaisesRegex(ValueError, "duplicate JSON object key"):
            youtube_http._strict_json(b'{"status":"good","status":"bad"}')

    def test_http_rejects_negative_content_length(self) -> None:
        url = "https://www.googleapis.com/youtube/v3/liveStreams"
        response = _Response({}, url=url)
        response.headers.replace_header("Content-Length", "-1")
        request = urllib.request.Request(url)
        with self.assertRaises(RemoteFailure) as raised:
            request_json(
                request,
                timeout_sec=1,
                max_bytes=1024,
                opener=lambda _request, *, timeout: response,
            )
        self.assertEqual(raised.exception.kind, "response_invalid")


class YouTubeApiCollectorTests(unittest.TestCase):
    credentials = OAuthCredentials("client-id", "client-secret", "refresh-token")

    def test_projected_credentials_reject_padding_instead_of_normalizing_it(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "client-id").write_text("client-id\n", encoding="utf-8")
            (root / "client-secret").write_text("client-secret", encoding="utf-8")
            (root / "refresh-token").write_text("refresh-token", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "whitespace padding"):
                read_credentials(root)

    def test_command_rejects_output_or_health_aliasing_credentials(self) -> None:
        arguments = command.parser().parse_args(
            [
                "--credentials-dir",
                "/credentials",
                "--output",
                "/credentials/client-id",
                "--ready-file",
                "/tmp/ready",
                "--heartbeat-file",
                "/tmp/heartbeat",
            ]
        )
        with self.assertRaisesRegex(ValueError, "outside credentials-dir"):
            command._validate(arguments)
        arguments.output = Path("/tmp/ready")
        with self.assertRaisesRegex(ValueError, "distinct"):
            command._validate(arguments)

    def test_success_sanitizes_identifiers_and_reuses_memory_only_token(self) -> None:
        opener = _QueueOpener(_success_responses() + _success_responses()[1:])
        collector = YouTubeApiCollector(self.credentials, opener=opener)
        first = collector.collect(now_ts=BASE_TS)
        second = collector.collect(now_ts=BASE_TS + 120)
        self.assertEqual(first.probe_status, "ok")
        self.assertEqual(first.result_kind, "observed")
        self.assertEqual(first.api_request_count, 2)
        self.assertTrue(first.oauth_refresh_performed)
        self.assertEqual(first.oauth_scope_class, "youtube_broader")
        self.assertEqual(first.oauth_scope_count, 1)
        self.assertFalse(second.oauth_refresh_performed)
        self.assertEqual(second.oauth_scope_class, "youtube_broader")
        serialized = json.dumps(first.to_dict(), sort_keys=True)
        for private in (
            "raw-broadcast-id",
            "raw-stream-id",
            "private-access-token",
            "private provider prose",
            "refresh-token",
            "client-secret",
            "https://www.googleapis.com/auth/youtube",
        ):
            self.assertNotIn(private, serialized)
        self.assertEqual(len(first.broadcast_id_sha256), 64)
        self.assertEqual(len(first.bound_stream_id_sha256), 64)
        self.assertEqual(len(opener.requests), 5)
        self.assertEqual(
            sum(request.full_url.endswith("oauth2.googleapis.com/token") for request in opener.requests),
            1,
        )

    def test_api_quota_is_measurement_unknown_not_stream_bad(self) -> None:
        body = json.dumps(
            {"error": {"errors": [{"reason": "quotaExceeded"}]}}
        ).encode("utf-8")
        error = urllib.error.HTTPError(
            "https://www.googleapis.com/youtube/v3/liveBroadcasts",
            403,
            "forbidden",
            Message(),
            io.BytesIO(body),
        )
        opener = _QueueOpener(
            [
                {
                    "access_token": "private-access-token",
                    "expires_in": 3600,
                    "scope": "https://www.googleapis.com/auth/youtube.readonly",
                },
                error,
            ]
        )
        evidence = YouTubeApiCollector(self.credentials, opener=opener).collect(
            now_ts=BASE_TS
        )
        self.assertEqual(evidence.probe_status, "unknown")
        self.assertEqual(evidence.result_kind, "api_quota")
        self.assertEqual(evidence.http_status, 403)
        self.assertEqual(evidence.lifecycle_status, "")
        self.assertEqual(evidence.stream_health_status, "")

    def test_api_403_rate_limit_is_not_misclassified_as_auth(self) -> None:
        body = json.dumps(
            {"error": {"errors": [{"reason": "rateLimitExceeded"}]}}
        ).encode("utf-8")
        error = urllib.error.HTTPError(
            "https://www.googleapis.com/youtube/v3/liveBroadcasts",
            403,
            "forbidden",
            Message(),
            io.BytesIO(body),
        )
        opener = _QueueOpener(
            [
                {
                    "access_token": "private-access-token",
                    "expires_in": 3600,
                    "scope": "https://www.googleapis.com/auth/youtube.readonly",
                },
                error,
            ]
        )
        evidence = YouTubeApiCollector(self.credentials, opener=opener).collect(
            now_ts=BASE_TS
        )
        self.assertEqual(evidence.result_kind, "api_rate_limited")
        self.assertEqual(evidence.error_reason, "rateLimitExceeded")

    def test_unexpected_exception_becomes_sanitized_unavailable_evidence(self) -> None:
        evidence = YouTubeApiCollector(
            self.credentials,
            opener=_QueueOpener([RuntimeError("secret-bearing failure")]),
        ).collect(now_ts=BASE_TS)
        self.assertEqual(evidence.probe_status, "unknown")
        self.assertEqual(evidence.result_kind, "collector_error")
        self.assertEqual(evidence.collected_at, utc_text(BASE_TS))
        self.assertNotIn("secret-bearing", json.dumps(evidence.to_dict()))

    def test_non_bearer_or_malformed_scope_response_is_rejected(self) -> None:
        cases = (
            {
                "access_token": "private-access-token",
                "expires_in": 3600,
                "token_type": "MAC",
            },
            {
                "access_token": "private-access-token",
                "expires_in": 3600,
                "token_type": "Bearer",
                "scope": ["not-a-string"],
            },
        )
        for response in cases:
            with self.subTest(response_keys=sorted(response)):
                evidence = YouTubeApiCollector(
                    self.credentials,
                    opener=_QueueOpener([response]),
                ).collect(now_ts=BASE_TS)
                self.assertEqual(evidence.probe_status, "unknown")
                self.assertEqual(evidence.result_kind, "oauth_response_invalid")
                self.assertEqual(evidence.oauth_scope_class, "unavailable")

    def test_nodata_remains_unknown_in_adapter(self) -> None:
        opener = _QueueOpener(_success_responses(health="noData"))
        evidence = YouTubeApiCollector(self.credentials, opener=opener).collect(
            now_ts=BASE_TS
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "evidence.json"
            path.write_text(json.dumps(evidence.to_dict()), encoding="utf-8")
            batch = YouTubeApiEvidenceAdapter(state_file=path).collect(
                received_at=utc_text(BASE_TS)
            )
        statuses = {item.source: item.status for item in batch.observations}
        self.assertEqual(statuses["youtube_api_direct_lifecycle"], "good")
        self.assertEqual(statuses["youtube_input_quality_api_direct"], "unknown")
        self.assertEqual(batch.rejections, ())

    def test_unknown_issue_severity_cannot_be_silently_classified_good(self) -> None:
        evidence = _evidence(
            configuration_issues=(
                {"type": "newProviderIssue", "severity": "unrecognized"},
            )
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "evidence.json"
            path.write_text(json.dumps(evidence.to_dict()), encoding="utf-8")
            batch = YouTubeApiEvidenceAdapter(state_file=path).collect(
                received_at=utc_text(BASE_TS)
            )
        statuses = {item.source: item.status for item in batch.observations}
        self.assertEqual(statuses["youtube_input_quality_api_direct"], "unknown")

    def test_probe_failure_is_rejection_and_not_an_observation(self) -> None:
        evidence = _evidence(
            probe_status="unknown",
            result_kind="api_timeout",
            lifecycle_status="",
            stream_status="",
            stream_health_status="",
            broadcast_id_sha256="",
            bound_stream_id_sha256="",
            active_broadcast_count=0,
            api_request_count=1,
        )
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "evidence.json"
            path.write_text(json.dumps(evidence.to_dict()), encoding="utf-8")
            batch = YouTubeApiEvidenceAdapter(state_file=path).collect(
                received_at=utc_text(BASE_TS)
            )
        self.assertEqual(batch.observations, ())
        self.assertEqual(len(batch.rejections), 1)
        self.assertEqual(batch.rejections[0].reason_code, "api_timeout")
        self.assertTrue(batch.read_succeeded)

    def test_direct_api_disagreement_is_diagnostic_only(self) -> None:
        authoritative = observation(
            domain="youtube_input_quality",
            source="youtube_input_quality_oauth",
            status="good",
            observed_ts=BASE_TS,
            event="authority-good",
        )
        direct = observation(
            domain="youtube_input_quality",
            source="youtube_input_quality_api_direct",
            status="bad",
            observed_ts=BASE_TS,
            role="current_correlated",
            event="direct-bad",
        )
        reduced = reduce_domain(
            [direct, authoritative],
            DEFAULT_POLICIES["youtube_input_quality"],
            now_ts=BASE_TS,
        )
        self.assertEqual(reduced.state, "good")
        self.assertEqual(reduced.source_observation_ids, (authoritative.observation_id,))
        self.assertTrue(reduced.payload["same_producer_api_disagreement"])
        self.assertIn(
            {
                "source": "youtube_input_quality_api_direct",
                "reason": "correlated_not_current_authority",
            },
            reduced.payload["ignored"],
        )

    def test_once_command_writes_group_read_only_sanitized_evidence(self) -> None:
        evidence = _evidence()

        class _FakeCollector:
            def collect(self) -> YouTubeApiEvidence:
                return evidence

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            output = root / "evidence.json"
            ready = root / "ready"
            heartbeat = root / "heartbeat"
            capture = io.StringIO()
            with patch.object(command, "read_credentials", return_value=self.credentials), patch.object(
                command,
                "YouTubeApiCollector",
                return_value=_FakeCollector(),
            ), redirect_stdout(capture):
                result = command.main(
                    [
                        "--credentials-dir",
                        str(root / "credentials"),
                        "--output",
                        str(output),
                        "--ready-file",
                        str(ready),
                        "--heartbeat-file",
                        str(heartbeat),
                        "--once",
                    ]
                )
            self.assertEqual(result, 0)
            self.assertEqual(os.stat(output).st_mode & 0o777, 0o640)
            self.assertEqual(os.stat(ready).st_mode & 0o777, 0o600)
            self.assertEqual(os.stat(heartbeat).st_mode & 0o777, 0o600)
            self.assertNotIn("broadcast_id_sha256", capture.getvalue())


if __name__ == "__main__":
    unittest.main()
