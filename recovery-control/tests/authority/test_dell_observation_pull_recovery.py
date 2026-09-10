from __future__ import annotations

import ssl
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from monitoring_projection.dell_observation_pull import DellObservationPuller


class _Headers:
    def get(self, name: str) -> str | None:
        return None

    def get_content_type(self) -> str:
        return "application/json"


class _Response:
    headers = _Headers()

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, maximum: int) -> bytes:
        assert maximum > 2
        return b"{}"


def _puller(tmp_path: Path, open_url: Any, waits: list[float]) -> DellObservationPuller:
    return DellObservationPuller(
        endpoint_url="https://dell.invalid/v1/dell-observations/latest",
        ssl_context=ssl.create_default_context(),
        contract=object(),  # type: ignore[arg-type]
        observation_file=tmp_path / "latest.json",
        transient_retry_delays_seconds=(0.1, 0.2),
        open_url=open_url,
        wait=waits.append,
        jitter=lambda cap: cap,
    )


def test_dell_get_recovers_from_bounded_http_overload(tmp_path: Path) -> None:
    attempts = 0
    waits: list[float] = []

    def open_url(*args: object, **kwargs: object) -> _Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise urllib.error.HTTPError("https://dell.invalid", 503, "injected", {}, None)
        return _Response()

    puller = _puller(tmp_path, open_url, waits)
    assert puller._fetch_body(urllib.request.Request(puller.endpoint_url)) == (b"{}", 3)
    assert attempts == 3
    assert waits == [0.1, 0.2]


def test_dell_get_never_retries_certificate_failure(tmp_path: Path) -> None:
    attempts = 0
    waits: list[float] = []

    def open_url(*args: object, **kwargs: object) -> _Response:
        nonlocal attempts
        attempts += 1
        raise urllib.error.URLError(ssl.SSLCertVerificationError("injected"))

    puller = _puller(tmp_path, open_url, waits)
    with pytest.raises(urllib.error.URLError):
        puller._fetch_body(urllib.request.Request(puller.endpoint_url))
    assert attempts == 1
    assert waits == []
