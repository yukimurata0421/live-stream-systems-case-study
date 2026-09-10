from __future__ import annotations

import math
import ssl
import urllib.error
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

from cra_dell_recovery.transport_resilience import FailureClassification, classify_failure

MAXIMUM_TRANSPORT_ATTEMPTS = 256


def _retry_after_seconds(error: BaseException, *, now: datetime) -> float | None:
    """Return a server-requested retry delay for retryable HTTP failures.

    Invalid or duplicated Retry-After fields are ignored.  The caller still
    applies its local bounded full-jitter policy and never extends the total
    invocation deadline to honor a remote value.
    """

    if not isinstance(error, urllib.error.HTTPError) or error.code not in {429, 503}:
        return None
    headers = error.headers
    if headers is None:
        return None
    if hasattr(headers, "get_all"):
        values = headers.get_all("Retry-After", [])
        if len(values) > 1:
            return None
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    value = str(raw).strip()
    if value.isascii() and value.isdecimal():
        return float(value)
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return max(0.0, (parsed.astimezone(UTC) - now.astimezone(UTC)).total_seconds())


def _read_json_response(
    response: Any,
    *,
    maximum_bytes: int,
    deadline: float,
    monotonic: Callable[[], float],
    error_prefix: str,
) -> bytes:
    headers = response.headers
    if hasattr(headers, "get_all"):
        for name in ("Content-Length", "Content-Type", "Transfer-Encoding", "Content-Encoding"):
            if len(headers.get_all(name, [])) > 1:
                raise ValueError(f"{error_prefix}_DUPLICATE_HEADER")
    content_length = headers.get("Content-Length")
    encoding = headers.get("Content-Encoding")
    transfer = headers.get("Transfer-Encoding")
    if encoding is not None and encoding.lower() != "identity":
        raise ValueError(f"{error_prefix}_CONTENT_ENCODING_INVALID")
    if transfer is not None and (transfer.lower() != "chunked" or content_length is not None):
        raise ValueError(f"{error_prefix}_TRANSFER_ENCODING_INVALID")
    if headers.get_content_type() != "application/json":
        raise ValueError(f"{error_prefix}_CONTENT_TYPE_INVALID")
    if content_length is not None and (not content_length.isascii() or not content_length.isdecimal()):
        raise ValueError(f"{error_prefix}_CONTENT_LENGTH_INVALID")
    if content_length is not None and int(content_length) > maximum_bytes:
        raise ValueError(f"{error_prefix}_RESPONSE_TOO_LARGE")

    if callable(getattr(response, "read1", None)):
        chunks = bytearray()
        while True:
            if monotonic() >= deadline:
                raise ValueError(f"{error_prefix}_RESPONSE_DEADLINE_EXCEEDED")
            remaining = maximum_bytes + 1 - len(chunks)
            chunk = response.read1(min(65536, remaining))
            if monotonic() >= deadline:
                raise ValueError(f"{error_prefix}_RESPONSE_DEADLINE_EXCEEDED")
            chunks.extend(chunk)
            if not chunk or len(chunks) > maximum_bytes:
                break
        body = bytes(chunks)
    else:
        body = response.read(maximum_bytes + 1)
        if monotonic() >= deadline:
            raise ValueError(f"{error_prefix}_RESPONSE_DEADLINE_EXCEEDED")
    if len(body) > maximum_bytes:
        raise ValueError(f"{error_prefix}_RESPONSE_TOO_LARGE")
    if content_length is not None and len(body) != int(content_length):
        raise ValueError(f"{error_prefix}_CONTENT_LENGTH_MISMATCH")
    return body


def fetch_bounded_json(
    request: urllib.request.Request,
    *,
    context: ssl.SSLContext,
    open_url: Callable[..., Any],
    timeout_seconds: float,
    transient_retry_delays_seconds: tuple[float, ...],
    maximum_retry_elapsed_seconds: float,
    maximum_bytes: int,
    monotonic: Callable[[], float],
    wait: Callable[[float], None],
    jitter: Callable[[float], float],
    error_prefix: str,
    retryable: Callable[[BaseException], bool],
    failure_observer: Callable[[FailureClassification, int, bool], None] | None,
    wall_clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> tuple[bytes, int]:
    """Fetch one JSON response inside one suspend-aware total deadline.

    This is deliberately limited to an idempotent caller-supplied request.
    Retry-After can lengthen a local jitter delay but can never lengthen the
    invocation budget.  Every socket attempt receives only the budget that
    remains, preventing a final attempt from overrunning the total deadline.
    """

    invocation_deadline = monotonic() + maximum_retry_elapsed_seconds
    prior_error: BaseException | None = None
    prior_classification: FailureClassification | None = None
    attempt = 0
    while True:
        attempt += 1
        attempt_started = monotonic()
        remaining_before_attempt = invocation_deadline - attempt_started
        if remaining_before_attempt <= 0:
            if prior_error is None or prior_classification is None:
                raise ValueError(f"{error_prefix}_RETRY_BUDGET_EXHAUSTED")
            if failure_observer is not None:
                failure_observer(
                    FailureClassification("TRANSIENT_TRANSPORT", "RETRY_BUDGET_EXHAUSTED", True),
                    attempt,
                    True,
                )
            raise prior_error
        attempt_timeout = min(timeout_seconds, remaining_before_attempt)
        attempt_deadline = attempt_started + attempt_timeout
        try:
            with open_url(request, context=context, timeout=attempt_timeout) as response:
                body = _read_json_response(
                    response,
                    maximum_bytes=maximum_bytes,
                    deadline=attempt_deadline,
                    monotonic=monotonic,
                    error_prefix=error_prefix,
                )
            return body, attempt
        except Exception as error:
            # urlopen raises before entering the response context manager for
            # HTTP errors. Release that socket before retry/callback/exit; its
            # headers and status remain available for bounded Retry-After.
            if isinstance(error, urllib.error.HTTPError):
                error.close()
            classification = classify_failure(error)
            remaining = invocation_deadline - monotonic()
            delay = 0.0
            retry_cap = (
                transient_retry_delays_seconds[min(attempt - 1, len(transient_retry_delays_seconds) - 1)]
                if transient_retry_delays_seconds
                else None
            )
            # The elapsed deadline is authoritative in production.  The
            # independent attempt guard also fails closed if an injected or
            # defective clock never advances while retry jitter is zero.
            can_retry = retry_cap is not None and retryable(error) and remaining > 0 and attempt < MAXIMUM_TRANSPORT_ATTEMPTS
            if can_retry and retry_cap is not None:
                local_delay = float(jitter(retry_cap))
                if not math.isfinite(local_delay) or not 0.0 <= local_delay <= retry_cap:
                    raise ValueError(f"{error_prefix}_RETRY_JITTER_INVALID") from error
                server_delay = _retry_after_seconds(error, now=wall_clock())
                delay = max(local_delay, 0.0 if server_delay is None else server_delay)
                # Keep a positive socket budget for the next attempt.  A
                # remote Retry-After never extends the local deadline.
                can_retry = delay < remaining
            exhausted = not can_retry
            if failure_observer is not None:
                failure_observer(classification, attempt, exhausted)
            if exhausted:
                raise
            prior_error = error
            prior_classification = classification
            if delay > 0:
                wait(delay)
