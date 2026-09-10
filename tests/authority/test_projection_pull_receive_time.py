from __future__ import annotations

import datetime
import errno
import json
import os
import socket
import ssl
import time
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from cra_authority.projection_pull import ProjectionPullConfig, ProjectionPuller, _deadline_clock


class _Headers:
    def get(self, name: str) -> str | None:
        return None

    def get_content_type(self) -> str:
        return "application/json"


class _Response:
    headers = _Headers()

    def __init__(self, events: list[str]) -> None:
        self.events = events

    def __enter__(self) -> _Response:
        self.events.append("response_received")
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, maximum_bytes: int) -> bytes:
        assert maximum_bytes > 2
        return b"{}"


class _Contract:
    def __init__(self, events: list[str], receive_time: datetime.datetime) -> None:
        self.events = events
        self.receive_time = receive_time

    def decode(self, value: dict[str, Any], *, now: datetime.datetime) -> SimpleNamespace:
        assert value == {}
        assert self.events == ["response_received", "clock_read"]
        assert now == self.receive_time
        return SimpleNamespace(sequence=1, value={"payload_sha256": "a", "observation_sequence": 1})


def test_projection_is_validated_with_receive_time(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []
    receive_time = datetime.datetime(2026, 8, 30, 19, 20, tzinfo=datetime.UTC)

    def clock() -> datetime.datetime:
        events.append("clock_read")
        return receive_time

    monkeypatch.setattr(
        "cra_authority.projection_pull._open_projection_url",
        lambda *args, **kwargs: _Response(events),
    )
    puller = ProjectionPuller(
        endpoint_url="https://127.0.0.1/v1/monitoring-evidence/latest",
        ssl_context=ssl.create_default_context(),
        contract=_Contract(events, receive_time),  # type: ignore[arg-type]
        projection_file=tmp_path / "projection.json",
        clock=clock,
    )

    result = puller.pull()

    assert result["disposition"] == "UPDATED"
    assert json.loads((tmp_path / "projection.json").read_text(encoding="utf-8"))["observation_sequence"] == 1


def test_pull_config_requires_explicit_bounded_check_age(tmp_path: Path) -> None:
    value = {
        "schema": "cra.monitoring_projection_pull.v2",
        "puller_release_id": "test-release-a",
        "endpoint_url": "https://127.0.0.1:9444/v1/monitoring-evidence/latest",
        "tls_certificate": "/tls/client.pem",
        "tls_private_key": "/tls/client-key.pem",
        "server_ca_certificate": "/tls/server-ca.pem",
        "projection_schema_file": "/contracts/projection.json",
        "monitoring_key_id": "monitoring-key-a",
        "monitoring_public_key_file": "/keys/monitoring-public.pem",
        "source_instance_id": "arena-monitoring-v4",
        "source_release_id": "monitoring-release-a",
        "maximum_ttl_seconds": 60,
        "maximum_check_age_seconds": 180,
        "timeout_seconds": 0.5,
        "transient_retry_delays_seconds": [0.1, 0.2, 0.4, 0.8],
        "projection_file": "/state/latest.json",
        "status_file": "/state/status.json",
    }
    path = tmp_path / "pull.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    os.chmod(path, 0o600)
    assert ProjectionPullConfig.load(path).value["maximum_check_age_seconds"] == 180

    value["maximum_check_age_seconds"] = 301
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="PROJECTION_PULL_CHECK_AGE_OUT_OF_RANGE"):
        ProjectionPullConfig.load(path)


@pytest.mark.parametrize(
    "failure",
    [
        urllib.error.URLError(ConnectionRefusedError(errno.ECONNREFUSED, "refused")),
        urllib.error.URLError(ConnectionResetError(errno.ECONNRESET, "reset")),
        urllib.error.URLError(TimeoutError(errno.ETIMEDOUT, "timeout")),
        urllib.error.URLError(socket.gaierror(socket.EAI_AGAIN, "temporary dns failure")),
    ],
)
def test_pull_retries_only_bounded_transient_transport_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: BaseException,
) -> None:
    events: list[str] = []
    receive_time = datetime.datetime(2026, 8, 30, 19, 20, tzinfo=datetime.UTC)
    attempts = 0

    def urlopen(*args: object, **kwargs: object) -> _Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise failure
        return _Response(events)

    monkeypatch.setattr("cra_authority.projection_pull._open_projection_url", urlopen)
    waits: list[float] = []

    def clock() -> datetime.datetime:
        events.append("clock_read")
        return receive_time

    puller = ProjectionPuller(
        endpoint_url="https://127.0.0.1/v1/monitoring-evidence/latest",
        ssl_context=ssl.create_default_context(),
        contract=_Contract(events, receive_time),  # type: ignore[arg-type]
        projection_file=tmp_path / "projection.json",
        transient_retry_delays_seconds=(0.1, 0.2, 0.4),
        clock=clock,
        wait=waits.append,
        jitter=lambda cap: cap,
    )

    result = puller.pull()

    assert attempts == 3
    assert waits == [0.1, 0.2]
    assert result["transport_attempt_count"] == 3
    assert result["transport_retry_count"] == 2


@pytest.mark.parametrize(
    "failure",
    [
        urllib.error.URLError(ssl.SSLCertVerificationError("bad certificate")),
        ValueError("invalid response"),
    ],
)
def test_pull_never_retries_protocol_security_or_semantic_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: BaseException,
) -> None:
    attempts = 0

    def urlopen(*args: object, **kwargs: object) -> _Response:
        nonlocal attempts
        attempts += 1
        raise failure

    monkeypatch.setattr("cra_authority.projection_pull._open_projection_url", urlopen)
    waits: list[float] = []
    puller = ProjectionPuller(
        endpoint_url="https://127.0.0.1/v1/monitoring-evidence/latest",
        ssl_context=ssl.create_default_context(),
        contract=object(),  # type: ignore[arg-type]
        projection_file=tmp_path / "projection.json",
        transient_retry_delays_seconds=(0.1, 0.2),
        wait=waits.append,
        jitter=lambda cap: cap,
    )

    with pytest.raises(type(failure)):
        puller.pull()
    assert attempts == 1
    assert waits == []


def test_pull_exhausts_retry_budget_without_unbounded_loop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    attempts = 0

    def urlopen(*args: object, **kwargs: object) -> _Response:
        nonlocal attempts
        attempts += 1
        raise urllib.error.URLError(ConnectionRefusedError(errno.ECONNREFUSED, "refused"))

    monkeypatch.setattr("cra_authority.projection_pull._open_projection_url", urlopen)
    waits: list[float] = []
    monotonic_time = 100.0

    def monotonic() -> float:
        return monotonic_time

    def wait(delay: float) -> None:
        nonlocal monotonic_time
        waits.append(delay)
        monotonic_time += delay

    puller = ProjectionPuller(
        endpoint_url="https://127.0.0.1/v1/monitoring-evidence/latest",
        ssl_context=ssl.create_default_context(),
        contract=object(),  # type: ignore[arg-type]
        projection_file=tmp_path / "projection.json",
        transient_retry_delays_seconds=(0.1, 0.2),
        maximum_retry_elapsed_seconds=0.49,
        monotonic=monotonic,
        wait=wait,
        jitter=lambda cap: cap,
    )

    with pytest.raises(urllib.error.URLError):
        puller.pull()
    assert attempts == 3
    assert waits == [0.1, 0.2]


def test_deadline_clock_counts_host_suspend_when_boottime_is_available(monkeypatch: pytest.MonkeyPatch) -> None:
    clock_id = getattr(time, "CLOCK_BOOTTIME", None)
    if clock_id is None:
        pytest.skip("CLOCK_BOOTTIME unavailable")
    observed: list[int] = []

    def clock_gettime(requested: int) -> float:
        observed.append(requested)
        return 123.5

    monkeypatch.setattr("cra_authority.projection_pull.time.clock_gettime", clock_gettime)

    assert _deadline_clock() == 123.5
    assert observed == [clock_id]


def test_tls_unexpected_eof_is_transient_but_certificate_rejection_is_not() -> None:
    assert ProjectionPuller._transient_transport_error(urllib.error.URLError(ssl.SSLEOFError(8, "unexpected eof"))) is True
    assert ProjectionPuller._transient_transport_error(urllib.error.URLError(ssl.SSLCertVerificationError("expired"))) is False


def test_retryable_http_overload_is_bounded(tmp_path: Path) -> None:
    attempts = 0
    waits: list[float] = []

    def open_url(*args: object, **kwargs: object) -> _Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise urllib.error.HTTPError("https://example", 503, "unavailable", {}, None)
        return _Response([])

    puller = ProjectionPuller(
        endpoint_url="https://127.0.0.1/v1/monitoring-evidence/latest",
        ssl_context=ssl.create_default_context(),
        contract=object(),  # type: ignore[arg-type]
        projection_file=tmp_path / "projection.json",
        transient_retry_delays_seconds=(0.1, 0.2),
        open_url=open_url,
        wait=waits.append,
        jitter=lambda cap: cap,
    )
    assert puller._fetch_body(urllib.request.Request(puller.endpoint_url)) == (b"{}", 3)
    assert waits == [0.1, 0.2]
