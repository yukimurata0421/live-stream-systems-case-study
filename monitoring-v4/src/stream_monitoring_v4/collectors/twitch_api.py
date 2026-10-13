"""Arena-owned read-only Helix observation. No delivery or recovery actions.

Standalone stdlib entrypoint permits a source-bound deployment without replacing
the running Monitoring v4 image. Credentials/tokens never enter evidence/logs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import signal
import socket
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

UTC = timezone.utc
SCHEMA = "monitoring_v4.twitch_api_evidence.v1"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"
VALIDATE_URL = "https://id.twitch.tv/oauth2/validate"
USERS_URL = "https://api.twitch.tv/helix/users"
STREAMS_URL = "https://api.twitch.tv/helix/streams"


class ProbeError(Exception):
    def __init__(self, reason: str, status: int = 0, retry_after: float = 0):
        super().__init__(reason)
        self.reason, self.status, self.retry_after = reason, status, retry_after


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        fp.close()
        raise ProbeError("redirect_rejected", code)


def utc(value: float) -> str:
    return datetime.fromtimestamp(value, UTC).isoformat(timespec="seconds")


def retry_delay(headers, now: float) -> float:
    values = [0.0]
    for name, offset in (("Retry-After", 0), ("Ratelimit-Reset", now)):
        try:
            value = float(headers.get(name, "0")) - offset
            if math.isfinite(value):
                values.append(max(0, value))
        except (TypeError, ValueError):
            pass
    return min(86400, max(values))


def request_json(url: str, headers: dict, body: bytes | None, timeout: float):
    # No environment proxy and no redirects carrying Authorization/client secret.
    opener = build_opener(ProxyHandler({}), NoRedirect())
    request = Request(url, data=body, headers=headers)
    deadline = time.monotonic() + timeout
    try:
        with opener.open(request, timeout=timeout) as response:
            chunks, size = [], 0
            while True:
                if time.monotonic() > deadline:
                    raise ProbeError("timeout")
                chunk = response.read1(min(4096, 65537 - size))
                if not chunk:
                    break
                chunks.append(chunk)
                size += len(chunk)
                if size > 65536:
                    raise ProbeError("response_too_large")
            value = json.loads(b"".join(chunks))
            if not isinstance(value, dict):
                raise ProbeError("response_invalid")
            return value, dict(response.headers)
    except HTTPError as exc:
        status, delay = exc.code, retry_delay(exc.headers, time.time())
        exc.close()
        reason = ("auth" if status in (401, 403) else "rate_limited" if status == 429
                  else "upstream" if status >= 500 else "http")
        raise ProbeError(reason, status, delay) from None
    except (TimeoutError, socket.timeout):
        raise ProbeError("timeout") from None
    except (URLError, OSError):
        raise ProbeError("network") from None
    except (ValueError, UnicodeError):
        raise ProbeError("response_invalid") from None


def _positive_int(value) -> bool:
    return type(value) is int and value > 0


def stream_facts(payload: dict, user_id: str, login: str, now: float) -> dict:
    rows = payload.get("data")
    pagination = payload.get("pagination", {})
    # A page with first=1 may include a cursor even for this exact user filter.
    # Do not follow it; the bounded row count and exact identity bind the result.
    cursor = pagination.get("cursor", "") if isinstance(pagination, dict) else None
    if (not isinstance(rows, list) or len(rows) > 1 or not isinstance(pagination, dict)
            or not isinstance(cursor, str) or len(cursor) > 4096):
        raise ProbeError("response_invalid")
    if not rows:
        return {"provider_state": "OFFLINE", "stream_id": None, "started_at": None, "viewer_count": None}
    row = rows[0]
    if not isinstance(row, dict) or row.get("user_id") != user_id or row.get("user_login") != login:
        raise ProbeError("target_mismatch")
    if row.get("type") != "live" or not isinstance(row.get("id"), str) or not re.fullmatch(r"[0-9]{1,64}", row["id"]):
        raise ProbeError("response_invalid")
    try:
        started = datetime.fromisoformat(row["started_at"].replace("Z", "+00:00"))
        if started.tzinfo is None or not 0 <= started.timestamp() <= now + 30:
            raise ValueError()
    except (KeyError, TypeError, AttributeError, ValueError, OverflowError):
        raise ProbeError("response_invalid") from None
    viewers = row.get("viewer_count")
    if type(viewers) is not int or not 0 <= viewers <= 1_000_000_000:
        raise ProbeError("response_invalid")
    return {"provider_state": "LIVE", "stream_id": row["id"],
            "started_at": started.astimezone(UTC).isoformat(timespec="seconds"), "viewer_count": viewers}


class TwitchCollector:
    def __init__(self, credentials_dir: Path, user_id: str, login: str, *, timeout: float = 5,
                 transport=request_json, wall=time.time, monotonic=time.monotonic, ttl: int = 180):
        self.credentials_dir, self.user_id, self.login = credentials_dir, user_id, login
        self.timeout, self.transport, self.wall, self.monotonic, self.ttl = timeout, transport, wall, monotonic, ttl
        self.token, self.client_id = "", ""
        self.expires, self.validated, self.identity_checked = 0.0, float("-inf"), float("-inf")
        self.instance, self.sequence, self.last_success = uuid.uuid4().hex, 0, None
        self.revision = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()

    def _request(self, url, headers, body=None):
        self.request_count += 1
        value, headers = self.transport(url, headers, body, self.timeout)
        try:
            self.remaining = max(0, int(headers.get("Ratelimit-Remaining", "")))
        except (TypeError, ValueError):
            pass
        return value

    def _authenticate(self):
        mono = self.monotonic()
        if not self.token or self.expires - mono <= 120:
            try:
                if any((self.credentials_dir / name).stat().st_size > 1024 for name in ("client-id", "client-secret")):
                    raise ProbeError("credentials_invalid")
                client = (self.credentials_dir / "client-id").read_text().strip()
                secret = (self.credentials_dir / "client-secret").read_text().strip()
            except (OSError, UnicodeError):
                raise ProbeError("credentials_missing") from None
            if not client or not secret:
                raise ProbeError("credentials_missing")
            if not re.fullmatch(r"[A-Za-z0-9]{3,128}", client) or not re.fullmatch(r"[A-Za-z0-9_-]{3,256}", secret):
                raise ProbeError("credentials_invalid")
            self.stage = "oauth"
            result = self._request(TOKEN_URL, {"Content-Type": "application/x-www-form-urlencoded"},
                                   urlencode({"client_id": client, "client_secret": secret,
                                              "grant_type": "client_credentials"}).encode())
            token, expiry = result.get("access_token"), result.get("expires_in")
            if not isinstance(token, str) or not re.fullmatch(r"[A-Za-z0-9_-]{3,512}", token) or not _positive_int(expiry) or result.get("token_type", "").lower() != "bearer" or result.get("scope", []) != []:
                raise ProbeError("response_invalid")
            self.token, self.client_id, self.expires = token, client, mono + expiry
            self.validated, self.identity_checked = float("-inf"), float("-inf")
        if mono - self.validated >= 3600:
            self.stage = "validate"
            result = self._request(VALIDATE_URL, {"Authorization": "OAuth " + self.token})
            # Scope-free app tokens currently validate with scopes=null as well
            # as []. Keep rejecting missing/malformed scopes and user tokens.
            if (result.get("client_id") != self.client_id or "scopes" not in result
                    or result["scopes"] not in (None, []) or result.get("user_id") or result.get("login")):
                self.token = ""
                raise ProbeError("app_token_required")
            expiry = result.get("expires_in")
            if not _positive_int(expiry) or expiry <= 120:
                self.token = ""
                raise ProbeError("token_expiring")
            self.expires, self.validated = self.monotonic() + expiry, self.monotonic()
        return {"Authorization": "Bearer " + self.token, "Client-Id": self.client_id}

    def collect(self) -> dict:
        started, mono = self.wall(), self.monotonic()
        self.sequence += 1
        self.request_count, self.remaining, self.stage = 0, None, "credentials"
        payload = {"schema": SCHEMA, "source_host_id": "arena-monitoring-facts", "source_boot_id": self.boot,
                   "collector_revision": self.revision, "collector_instance": self.instance, "sequence": self.sequence,
                   "user_id": self.user_id, "user_login": self.login, "request_started_at": utc(started),
                   "probe_status": "unknown", "provider_state": "UNKNOWN", "stream_id": None,
                   "started_at": None, "viewer_count": None, "error_reason": "", "http_status": 0,
                   "retry_after_sec": 0.0}
        try:
            headers = self._authenticate()
            if self.monotonic() - self.identity_checked >= 3600:
                self.stage = "users"
                users = self._request(USERS_URL + "?" + urlencode({"id": self.user_id}), headers).get("data")
                if not isinstance(users, list) or len(users) != 1 or not isinstance(users[0], dict) or users[0].get("id") != self.user_id or users[0].get("login") != self.login:
                    raise ProbeError("target_mismatch")
                self.identity_checked = self.monotonic()
            self.stage = "streams"
            streams = self._request(STREAMS_URL + "?" + urlencode({"user_id": self.user_id, "first": 1}), headers)
            facts = stream_facts(streams, self.user_id, self.login, self.wall())
            finished = self.wall()
            if abs((finished - started) - (self.monotonic() - mono)) > 5:
                raise ProbeError("clock_jump")
            payload.update(facts, probe_status="ok", http_status=200)
            self.last_success = utc(finished)
        except ProbeError as exc:
            reason = exc.reason if exc.reason.startswith("credentials_") else self.stage + "_" + exc.reason
            payload.update(error_reason=reason, http_status=exc.status,
                           retry_after_sec=exc.retry_after)
            if exc.status == 401:
                self.token = ""
        except Exception:
            # Only a fixed code is published; provider bodies and traceback may contain secrets.
            payload["error_reason"] = "collector_error"
        finished = self.wall()
        payload.update(collected_at=utc(finished), valid_until=utc(finished + self.ttl),
                       freshness_ttl_sec=self.ttl, last_success_at=self.last_success,
                       api_request_count=self.request_count, rate_limit_remaining=self.remaining)
        return payload


def atomic_write(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            os.fchmod(stream.fileno(), 0o640)
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def metrics(payload: dict) -> str:
    lines = ["# Twitch provider observation only; API LIVE does not prove audiovisual reception."]
    values = {"stream_v4_twitch_api_collected_unixtime": datetime.fromisoformat(payload["collected_at"]).timestamp(),
              "stream_v4_twitch_api_freshness_ttl_seconds": payload["freshness_ttl_sec"],
              "stream_v4_twitch_api_probe_ok": int(payload["probe_status"] == "ok"),
              "stream_v4_twitch_api_http_status": payload["http_status"],
              "stream_v4_twitch_api_requests": payload["api_request_count"]}
    for name, value in values.items():
        lines.extend(["# TYPE " + name + " gauge", name + " " + str(value)])
    for state in ("LIVE", "OFFLINE", "UNKNOWN"):
        lines.append('stream_v4_twitch_provider_state{state="' + state + '"} ' + str(int(payload["provider_state"] == state)))
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--user-login", required=True)
    parser.add_argument("--interval-sec", type=float, default=60)
    parser.add_argument("--timeout-sec", type=float, default=5)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[0-9]{1,64}", args.user_id) or not re.fullmatch(r"[a-z0-9_]{1,25}", args.user_login):
        parser.error("invalid target identity")
    if not math.isfinite(args.interval_sec) or not 30 <= args.interval_sec <= 300 or not math.isfinite(args.timeout_sec) or not 1 <= args.timeout_sec <= 10:
        parser.error("interval must be 30..300 seconds; timeout 1..10 seconds")
    credentials, output = args.credentials_dir.resolve(), args.output_dir.resolve()
    if output == credentials or output.is_relative_to(credentials) or credentials.is_relative_to(output):
        parser.error("credential/output directories must be separate")
    collector = TwitchCollector(credentials, args.user_id, args.user_login, timeout=args.timeout_sec,
                                ttl=max(180, int(args.interval_sec * 3)))
    stopping = False

    def stop(_signal, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    failures = 0
    while not stopping:
        payload = collector.collect()
        atomic_write(output / "twitch_api_evidence.json", json.dumps(payload, sort_keys=True, allow_nan=False) + "\n")
        atomic_write(output / "twitch_api.prom", metrics(payload))
        print(json.dumps({k: payload[k] for k in ("collected_at", "sequence", "probe_status", "provider_state", "error_reason", "api_request_count")}), flush=True)
        if args.once:
            return 0 if payload["probe_status"] == "ok" else 1
        failures = 0 if payload["probe_status"] == "ok" else failures + 1
        delay = max(min(900, args.interval_sec * 2 ** min(max(0, failures - 1), 4)), payload["retry_after_sec"])
        deadline = time.monotonic() + delay
        while not stopping and time.monotonic() < deadline:
            time.sleep(min(0.5, deadline - time.monotonic()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
