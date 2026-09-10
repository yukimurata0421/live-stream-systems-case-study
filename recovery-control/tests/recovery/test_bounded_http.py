from __future__ import annotations

import errno
import http.client
import ssl
import urllib.error
import urllib.request
from email.message import Message
from pathlib import Path
from typing import Any

import pytest

from cra_authority.projection_pull import ProjectionPuller
from cra_dell_recovery.bounded_http import MAXIMUM_TRANSPORT_ATTEMPTS
from cra_dell_recovery.transport_resilience import classify_failure


class _Clock:
    def __init__(self) -> None:
        self.value = 100.0

    def now(self) -> float:
        return self.value

    def wait(self, delay: float) -> None:
        self.value += delay


class _Headers:
    def get(self, _name: str) -> None:
        return None

    def get_content_type(self) -> str:
        return "application/json"


class _Response:
    headers = _Headers()

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, _maximum: int) -> bytes:
        return b"{}"


def _puller(tmp_path: Path, **updates: Any) -> ProjectionPuller:
    values: dict[str, Any] = {
        "endpoint_url": "https://facts.invalid/v1/monitoring-evidence/latest",
        "ssl_context": ssl.create_default_context(),
        "contract": object(),
        "projection_file": tmp_path / "projection.json",
        "timeout_seconds": 2.0,
        "transient_retry_delays_seconds": (0.2, 0.2),
        "maximum_retry_elapsed_seconds": 1.0,
        "jitter": lambda cap: cap,
    }
    values.update(updates)
    return ProjectionPuller(**values)


def test_each_socket_attempt_is_capped_by_remaining_total_budget(tmp_path: Path) -> None:
    clock = _Clock()
    timeouts: list[float] = []
    observed: list[tuple[int, bool]] = []

    def open_url(*_args: object, timeout: float, **_kwargs: object) -> _Response:
        timeouts.append(timeout)
        raise urllib.error.URLError(TimeoutError("injected"))

    puller = _puller(
        tmp_path,
        open_url=open_url,
        monotonic=clock.now,
        wait=clock.wait,
        maximum_retry_elapsed_seconds=0.25,
        failure_observer=lambda _classification, attempt, exhausted: observed.append((attempt, exhausted)),
    )
    with pytest.raises(urllib.error.URLError):
        puller._fetch_body(urllib.request.Request(puller.endpoint_url))

    assert timeouts == pytest.approx([0.25, 0.05])
    assert clock.value == pytest.approx(100.2)
    assert observed == [(1, False), (2, True)]


def test_retry_after_cannot_extend_local_deadline(tmp_path: Path) -> None:
    waits: list[float] = []
    attempts = 0
    headers = Message()
    headers["Retry-After"] = "120"

    def open_url(*_args: object, **_kwargs: object) -> _Response:
        nonlocal attempts
        attempts += 1
        raise urllib.error.HTTPError("https://facts.invalid", 503, "injected", headers, None)

    puller = _puller(tmp_path, open_url=open_url, wait=waits.append, maximum_retry_elapsed_seconds=1.0)
    with pytest.raises(urllib.error.HTTPError):
        puller._fetch_body(urllib.request.Request(puller.endpoint_url))
    assert attempts == 1
    assert waits == []


def test_retry_after_is_respected_inside_local_deadline(tmp_path: Path) -> None:
    clock = _Clock()
    attempts = 0
    headers = Message()
    headers["Retry-After"] = "1"

    def open_url(*_args: object, **_kwargs: object) -> _Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise urllib.error.HTTPError("https://facts.invalid", 429, "injected", headers, None)
        return _Response()

    puller = _puller(
        tmp_path,
        open_url=open_url,
        monotonic=clock.now,
        wait=clock.wait,
        maximum_retry_elapsed_seconds=2.0,
    )
    assert puller._fetch_body(urllib.request.Request(puller.endpoint_url)) == (b"{}", 2)
    assert clock.value == pytest.approx(101.0)


def test_last_retry_delay_is_reused_until_total_deadline(tmp_path: Path) -> None:
    clock = _Clock()
    attempts = 0

    def open_url(*_args: object, **_kwargs: object) -> _Response:
        nonlocal attempts
        attempts += 1
        if attempts < 5:
            raise urllib.error.URLError(TimeoutError("injected"))
        return _Response()

    puller = _puller(
        tmp_path,
        open_url=open_url,
        monotonic=clock.now,
        wait=clock.wait,
        transient_retry_delays_seconds=(0.2,),
        maximum_retry_elapsed_seconds=1.0,
    )

    assert puller._fetch_body(urllib.request.Request(puller.endpoint_url)) == (b"{}", 5)
    assert clock.value == pytest.approx(100.8)


def test_attempt_guard_stops_a_nonadvancing_injected_clock(tmp_path: Path) -> None:
    attempts = 0

    def open_url(*_args: object, **_kwargs: object) -> _Response:
        nonlocal attempts
        attempts += 1
        raise urllib.error.URLError(TimeoutError("injected"))

    puller = _puller(
        tmp_path,
        open_url=open_url,
        monotonic=lambda: 100.0,
        wait=lambda _delay: None,
        jitter=lambda _cap: 0.0,
    )

    with pytest.raises(urllib.error.URLError):
        puller._fetch_body(urllib.request.Request(puller.endpoint_url))
    assert attempts == MAXIMUM_TRANSPORT_ATTEMPTS


@pytest.mark.parametrize(
    ("error", "category", "retryable"),
    [
        (http.client.IncompleteRead(b"{"), "TRANSIENT_TRANSPORT", True),
        (ssl.SSLZeroReturnError(6, "closed"), "TRANSIENT_TRANSPORT", True),
        (OSError(errno.EMFILE, "fd exhausted"), "RESOURCE", False),
        (OSError(errno.ENOBUFS, "socket buffers exhausted"), "RESOURCE", False),
        (OSError(errno.ENOSPC, "disk full"), "STORAGE", False),
    ],
)
def test_failure_taxonomy_separates_transport_resource_and_storage(
    error: BaseException,
    category: str,
    retryable: bool,
) -> None:
    classification = classify_failure(error)
    assert classification.category == category
    assert classification.retryable is retryable


def test_invalid_jitter_fails_closed_without_second_attempt(tmp_path: Path) -> None:
    attempts = 0

    def open_url(*_args: object, **_kwargs: object) -> _Response:
        nonlocal attempts
        attempts += 1
        raise urllib.error.URLError(TimeoutError("injected"))

    puller = _puller(tmp_path, open_url=open_url, jitter=lambda _cap: float("nan"))
    with pytest.raises(ValueError, match="RETRY_JITTER_INVALID"):
        puller._fetch_body(urllib.request.Request(puller.endpoint_url))
    assert attempts == 1
