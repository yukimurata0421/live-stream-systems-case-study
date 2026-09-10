from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
import random
import socket
import ssl
import urllib.error
import urllib.request
from email.message import Message
from pathlib import Path
from typing import Any

from cra_authority.projection_pull import ProjectionPuller
from cra_dell_recovery.bounded_http import fetch_bounded_json

SCENARIOS = (
    "TRANSIENT_RECOVERED",
    "TRANSIENT_EXHAUSTED",
    "READ_RESET_RECOVERED",
    "TLS_EOF_RECOVERED",
    "TLS_TIMEOUT_RECOVERED",
    "TEMPORARY_DNS_RECOVERED",
    "HTTP_OVERLOAD_RECOVERED",
    "CERTIFICATE_NOT_RETRIED",
    "PERMANENT_DNS_NOT_RETRIED",
    "CONTENT_TYPE_NOT_RETRIED",
    "OVERSIZE_NOT_RETRIED",
)


class _Headers:
    def __init__(self, *, content_type: str = "application/json", content_length: str | None = None) -> None:
        self.content_type = content_type
        self.content_length = content_length

    def get(self, name: str) -> str | None:
        return self.content_length if name == "Content-Length" else None

    def get_content_type(self) -> str:
        return self.content_type


class _Response:
    def __init__(
        self,
        *,
        body: bytes = b"{}",
        content_type: str = "application/json",
        content_length: str | None = None,
        read_error: BaseException | None = None,
    ) -> None:
        self.headers = _Headers(content_type=content_type, content_length=content_length)
        self.body = body
        self.read_error = read_error

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, _maximum_bytes: int) -> bytes:
        if self.read_error is not None:
            raise self.read_error
        return self.body


class _MonotonicClock:
    def __init__(self, waits: list[float]) -> None:
        self.value = 0.0
        self.waits = waits

    def monotonic(self) -> float:
        return self.value

    def wait(self, delay: float) -> None:
        self.waits.append(delay)
        self.value += delay


def run_projection_transport_chaos(*, case_count: int = 1_000, seed: int = 20260901) -> dict[str, Any]:
    if isinstance(case_count, bool) or not isinstance(case_count, int) or not len(SCENARIOS) <= case_count <= 100_000:
        raise ValueError("PROJECTION_TRANSPORT_CHAOS_CASE_COUNT_INVALID")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("PROJECTION_TRANSPORT_CHAOS_SEED_INVALID")
    rng = random.Random(seed)
    scheduled = list(SCENARIOS)
    scheduled.extend(rng.choice(SCENARIOS) for _ in range(case_count - len(scheduled)))
    rng.shuffle(scheduled)
    failures: list[dict[str, str | int]] = []
    detected = {name: 0 for name in SCENARIOS}
    security_or_semantic_retry_count = 0
    delays = (0.01, 0.02, 0.04, 0.08)
    request = urllib.request.Request("https://127.0.0.1/v1/monitoring-evidence/latest")
    # The injected transport never performs TLS.  Reusing one context keeps
    # a long in-process campaign representative of puller logic instead of
    # retaining OpenSSL trust-store allocations for every synthetic case.
    ssl_context = ssl.create_default_context()
    for index, scenario in enumerate(scheduled):
        attempts = 0
        waits: list[float] = []
        monotonic_clock = _MonotonicClock(waits)
        transient_count = rng.randint(1, len(delays))

        def open_url(
            *_args: object,
            _scenario: str = scenario,
            _transient_count: int = transient_count,
            **_kwargs: object,
        ) -> _Response:
            nonlocal attempts
            attempts += 1
            if _scenario == "TRANSIENT_RECOVERED" and attempts <= _transient_count:
                raise urllib.error.URLError(ConnectionRefusedError(errno.ECONNREFUSED, "refused"))
            if _scenario == "TRANSIENT_EXHAUSTED":
                raise urllib.error.URLError(TimeoutError(errno.ETIMEDOUT, "timeout"))
            if _scenario == "READ_RESET_RECOVERED" and attempts == 1:
                return _Response(read_error=ConnectionResetError(errno.ECONNRESET, "reset"))
            if _scenario == "TLS_EOF_RECOVERED" and attempts == 1:
                raise urllib.error.URLError(ssl.SSLEOFError(8, "unexpected eof"))
            if _scenario == "TLS_TIMEOUT_RECOVERED" and attempts == 1:
                raise urllib.error.URLError(TimeoutError(errno.ETIMEDOUT, "tls handshake timeout"))
            if _scenario == "TEMPORARY_DNS_RECOVERED" and attempts == 1:
                raise urllib.error.URLError(socket.gaierror(socket.EAI_AGAIN, "temporary dns failure"))
            if _scenario == "HTTP_OVERLOAD_RECOVERED" and attempts == 1:
                raise urllib.error.HTTPError("https://127.0.0.1", 503, "unavailable", Message(), None)
            if _scenario == "CERTIFICATE_NOT_RETRIED":
                raise urllib.error.URLError(ssl.SSLCertVerificationError("certificate rejected"))
            if _scenario == "PERMANENT_DNS_NOT_RETRIED":
                raise urllib.error.URLError(socket.gaierror(socket.EAI_NONAME, "not found"))
            if _scenario == "CONTENT_TYPE_NOT_RETRIED":
                return _Response(content_type="text/plain")
            if _scenario == "OVERSIZE_NOT_RETRIED":
                return _Response(content_length=str(2 * 1024 * 1024 + 1))
            return _Response()

        puller = ProjectionPuller(
            endpoint_url=request.full_url,
            ssl_context=ssl_context,
            contract=object(),  # type: ignore[arg-type]
            projection_file=Path("/not-written-by-fetch-body"),
            transient_retry_delays_seconds=delays,
            maximum_retry_elapsed_seconds=0.229,
            monotonic=monotonic_clock.monotonic,
            wait=monotonic_clock.wait,
            open_url=open_url,
            jitter=lambda cap: cap,
        )
        error: BaseException | None = None
        result: tuple[bytes, int] | None = None
        try:
            result = puller._fetch_body(request)
        except BaseException as caught:
            error = caught
        if scenario == "TRANSIENT_RECOVERED":
            passed = result == (b"{}", transient_count + 1) and attempts == transient_count + 1 and waits == list(delays[:transient_count])
        elif scenario == "TRANSIENT_EXHAUSTED":
            passed = isinstance(error, urllib.error.URLError) and attempts == len(delays) + 1 and waits == list(delays)
        elif scenario in {
            "READ_RESET_RECOVERED",
            "TLS_EOF_RECOVERED",
            "TLS_TIMEOUT_RECOVERED",
            "TEMPORARY_DNS_RECOVERED",
            "HTTP_OVERLOAD_RECOVERED",
        }:
            passed = result == (b"{}", 2) and attempts == 2 and waits == [delays[0]]
        else:
            security_or_semantic_retry_count += max(0, attempts - 1)
            passed = error is not None and attempts == 1 and waits == []
        detected[scenario] += int(passed)
        if not passed:
            failures.append(
                {
                    "index": index,
                    "scenario": scenario,
                    "attempts": attempts,
                    "error": "" if error is None else type(error).__name__,
                }
            )
    return {
        "schema": "cra.projection_transport_chaos_report.v1",
        "seed": seed,
        "case_count": case_count,
        "scenario_detection_count": detected,
        "failure_count": len(failures),
        "failures": failures[:20],
        "security_or_semantic_retry_count": security_or_semantic_retry_count,
        "production_target_touched": False,
        "pass": not failures and all(count > 0 for count in detected.values()),
    }


def _source_hashes() -> dict[str, str]:
    project_root = Path(__file__).resolve().parents[2]
    paths = {
        Path(__file__).resolve(),
        Path(ProjectionPuller._transient_transport_error.__code__.co_filename).resolve(),
        Path(fetch_bounded_json.__code__.co_filename).resolve(),
    }
    return {str(path.relative_to(project_root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(paths)}


def _atomic_report(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run bounded projection transport chaos")
    parser.add_argument("--cases", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=20260901)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    source_hashes = _source_hashes()
    report = run_projection_transport_chaos(case_count=args.cases, seed=args.seed)
    if _source_hashes() != source_hashes:
        raise RuntimeError("PROJECTION_TRANSPORT_CHAOS_SOURCE_DRIFT")
    report["source_hashes"] = source_hashes
    if args.output is not None:
        _atomic_report(args.output, report)
    print(json.dumps(report, separators=(",", ":"), sort_keys=True))
    if not report["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
