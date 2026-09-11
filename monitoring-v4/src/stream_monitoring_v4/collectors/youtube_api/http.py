from __future__ import annotations

import json
import math
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any

from .errors import RemoteFailure


OpenUrl = Callable[..., Any]
ALLOWED_HOSTS = frozenset({"oauth2.googleapis.com", "www.googleapis.com"})
_SAFE_REASON = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,79}$")


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    """Turn every redirect into an HTTPError before credentials can be forwarded."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Mapping[str, str],
        newurl: str,
    ) -> None:
        return None


def _build_strict_opener() -> urllib.request.OpenerDirector:
    # Do not inherit HTTP(S)_PROXY from the host or a future Pod environment.
    # OAuth and bearer credentials may only travel directly to the allowlisted
    # TLS endpoints below.
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _RejectRedirects(),
    )


_STRICT_OPENER = _build_strict_opener()


def strict_urlopen(request: urllib.request.Request, *, timeout: float) -> Any:
    return _STRICT_OPENER.open(request, timeout=timeout)


def _allowed_url(value: str) -> bool:
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except (TypeError, ValueError):
        return False
    return (
        parsed.scheme == "https"
        and parsed.hostname in ALLOWED_HOSTS
        and port in {None, 443}
        and parsed.username is None
        and parsed.password is None
        and not parsed.fragment
    )


def _strict_json(raw: bytes) -> Mapping[str, Any]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON value: {value}")

    def bounded_int(value: str) -> int:
        parsed = int(value)
        if not -(2**63) <= parsed <= 2**63 - 1:
            raise ValueError("JSON integer is outside signed 64-bit range")
        return parsed

    def finite_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("JSON number is not finite")
        return parsed

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON object key: {key}")
            result[key] = item
        return result

    value = json.loads(
        raw,
        parse_constant=reject_constant,
        parse_int=bounded_int,
        parse_float=finite_float,
        object_pairs_hook=unique_object,
    )
    if not isinstance(value, Mapping):
        raise ValueError("response root is not an object")
    return value


def _safe_reason(raw: bytes) -> str:
    try:
        value = _strict_json(raw)
        error = value.get("error")
        if isinstance(error, Mapping):
            errors = error.get("errors")
            if isinstance(errors, list) and errors and isinstance(errors[0], Mapping):
                candidate = errors[0].get("reason")
                if isinstance(candidate, str):
                    token = candidate.strip()
                    if _SAFE_REASON.fullmatch(token):
                        return token
            candidate = error.get("status")
            if isinstance(candidate, str):
                token = candidate.strip()
                if _SAFE_REASON.fullmatch(token):
                    return token
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        pass
    return "unclassified"


def _kind(status: int, reason: str) -> str:
    lower = reason.lower()
    if 300 <= status <= 399:
        return "redirect_rejected"
    if status == 401:
        return "auth"
    if status == 403 and lower in {
        "quotaexceeded",
        "dailylimitexceeded",
        "dailylimitexceededunreg",
    }:
        return "quota"
    if status in {403, 429} and lower in {
        "ratelimitexceeded",
        "userratelimitexceeded",
        "resource_exhausted",
    }:
        return "rate_limited"
    if status == 429:
        return "rate_limited"
    if status in {500, 502, 503, 504}:
        return "upstream"
    if status == 403 and lower in {
        "accountdelegationforbidden",
        "authenticateduseraccountclosed",
        "authenticateduseraccountsuspended",
        "authenticatedusernotchannel",
        "forbidden",
        "insufficientcapabilities",
        "insufficientlivepermissions",
        "insufficientpermissions",
        "livestreamingnotenabled",
        "youtubesignuprequired",
    }:
        return "auth"
    return "http"


def request_json(
    request: urllib.request.Request,
    *,
    timeout_sec: float,
    max_bytes: int,
    opener: OpenUrl = strict_urlopen,
) -> Mapping[str, Any]:
    if not _allowed_url(request.full_url):
        raise RemoteFailure("redirect_rejected")
    try:
        response = opener(request, timeout=max(0.1, float(timeout_sec)))
        with response:
            if not _allowed_url(response.geturl()):
                raise RemoteFailure("redirect_rejected")
            declared = response.headers.get("Content-Length")
            if declared:
                try:
                    declared_size = int(declared)
                    if declared_size < 0:
                        raise RemoteFailure("response_invalid")
                    if declared_size > max_bytes:
                        raise RemoteFailure("response_too_large")
                except ValueError:
                    raise RemoteFailure("response_invalid") from None
            raw = response.read(max(1, int(max_bytes)) + 1)
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read(max(1, int(max_bytes)) + 1)
        except OSError:
            body = b""
        finally:
            exc.close()
        reason = _safe_reason(body[:max_bytes])
        raise RemoteFailure(_kind(int(exc.code), reason), int(exc.code), reason) from None
    except (TimeoutError, socket.timeout):
        raise RemoteFailure("timeout") from None
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, (TimeoutError, socket.timeout)):
            raise RemoteFailure("timeout") from None
        raise RemoteFailure("network") from None
    except RemoteFailure:
        raise
    except OSError:
        raise RemoteFailure("network") from None
    if len(raw) > max_bytes:
        raise RemoteFailure("response_too_large")
    try:
        return _strict_json(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        raise RemoteFailure("response_invalid") from None
