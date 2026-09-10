from __future__ import annotations

import errno
import http.client
import socket
import ssl
import urllib.error
from dataclasses import dataclass

RETRYABLE_HTTP_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
RETRYABLE_ERRNOS = frozenset(
    {
        errno.EAGAIN,
        errno.ECONNABORTED,
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.EHOSTDOWN,
        errno.EHOSTUNREACH,
        errno.ENETDOWN,
        errno.ENETRESET,
        errno.ENETUNREACH,
        errno.EPIPE,
        errno.ETIMEDOUT,
    }
)
RESOURCE_ERRNOS = frozenset(
    {
        errno.EADDRINUSE,
        errno.EADDRNOTAVAIL,
        errno.EMFILE,
        errno.ENFILE,
        errno.ENOBUFS,
        errno.ENOMEM,
    }
)
STORAGE_ERRNOS = frozenset({errno.EDQUOT, errno.EIO, errno.ENOSPC, errno.EROFS})


@dataclass(frozen=True)
class FailureClassification:
    category: str
    reason_code: str
    retryable: bool


def _unwrap(error: BaseException) -> BaseException:
    if isinstance(error, urllib.error.URLError) and isinstance(error.reason, BaseException):
        return error.reason
    return error


def classify_failure(error: BaseException) -> FailureClassification:
    """Classify an observation GET failure without exposing exception text.

    Only failures that can plausibly heal without changing trust or data are
    retryable.  Authentication, identity, framing, signature, sequence and
    schema failures stay fail-closed even when a later request might succeed.
    """

    if isinstance(error, urllib.error.HTTPError):
        status = int(error.code)
        if status in RETRYABLE_HTTP_STATUS:
            return FailureClassification("TRANSIENT_TRANSPORT", f"HTTP_{status}", True)
        return FailureClassification("PROTOCOL", f"HTTP_{status}", False)

    reason = _unwrap(error)
    if isinstance(reason, ssl.SSLCertVerificationError):
        return FailureClassification("SECURITY", "TLS_CERTIFICATE_REJECTED", False)
    if isinstance(reason, ssl.SSLEOFError):
        return FailureClassification("TRANSIENT_TRANSPORT", "TLS_UNEXPECTED_EOF", True)
    if isinstance(reason, ssl.SSLZeroReturnError):
        return FailureClassification("TRANSIENT_TRANSPORT", "TLS_CLEAN_EOF", True)
    if isinstance(reason, socket.gaierror):
        if reason.errno == socket.EAI_AGAIN:
            return FailureClassification("TRANSIENT_TRANSPORT", "DNS_TEMPORARY_FAILURE", True)
        return FailureClassification("PROTOCOL", "DNS_PERMANENT_FAILURE", False)
    if isinstance(reason, TimeoutError):
        return FailureClassification("TRANSIENT_TRANSPORT", "TRANSPORT_TIMEOUT", True)
    if isinstance(reason, ConnectionResetError):
        return FailureClassification("TRANSIENT_TRANSPORT", "CONNECTION_RESET", True)
    if isinstance(reason, ConnectionRefusedError):
        return FailureClassification("TRANSIENT_TRANSPORT", "CONNECTION_REFUSED", True)
    if isinstance(reason, ConnectionAbortedError):
        return FailureClassification("TRANSIENT_TRANSPORT", "CONNECTION_ABORTED", True)
    if isinstance(reason, BrokenPipeError):
        return FailureClassification("TRANSIENT_TRANSPORT", "BROKEN_PIPE", True)
    if isinstance(reason, http.client.IncompleteRead):
        return FailureClassification("TRANSIENT_TRANSPORT", "HTTP_INCOMPLETE_READ", True)
    if isinstance(reason, EOFError):
        return FailureClassification("TRANSIENT_TRANSPORT", "TRANSPORT_EOF", True)
    if isinstance(reason, OSError) and reason.errno in RETRYABLE_ERRNOS:
        return FailureClassification("TRANSIENT_TRANSPORT", f"OSERROR_{int(reason.errno)}", True)
    if isinstance(reason, OSError) and reason.errno in RESOURCE_ERRNOS:
        return FailureClassification("RESOURCE", f"OSERROR_{int(reason.errno)}", False)
    if isinstance(reason, OSError) and reason.errno in STORAGE_ERRNOS:
        return FailureClassification("STORAGE", f"OSERROR_{int(reason.errno)}", False)

    if isinstance(reason, json_error_types()):
        return FailureClassification("CONTRACT", "JSON_INVALID", False)
    if isinstance(reason, ValueError):
        code = str(reason)
        if "RESPONSE_DEADLINE_EXCEEDED" in code:
            return FailureClassification("TRANSIENT_TRANSPORT", "RESPONSE_DEADLINE_EXCEEDED", True)
        if any(token in code for token in ("SIGNATURE", "CERTIFICATE", "KEY_", "INTEGRITY", "SEQUENCE_")):
            return FailureClassification("SECURITY", "TRUST_OR_SEQUENCE_REJECTED", False)
        if "REDIRECT" in code:
            return FailureClassification("PROTOCOL", "REDIRECT_REJECTED", False)
        return FailureClassification("CONTRACT", "CONTRACT_REJECTED", False)
    if isinstance(reason, ssl.SSLError):
        return FailureClassification("SECURITY", "TLS_PROTOCOL_REJECTED", False)
    if isinstance(reason, http.client.HTTPException):
        return FailureClassification("PROTOCOL", "HTTP_PROTOCOL_REJECTED", False)
    return FailureClassification("INTERNAL", type(reason).__name__.upper(), False)


def json_error_types() -> tuple[type[BaseException], ...]:
    # Kept behind a function to avoid exposing json internals in type aliases.
    import json

    return (json.JSONDecodeError, UnicodeDecodeError)


def retryable_transport_error(error: BaseException) -> bool:
    return classify_failure(error).retryable
