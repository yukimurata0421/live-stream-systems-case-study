"""Arena-owned read-only Helix observation. No delivery or recovery actions.

Standalone stdlib entrypoint permits a source-bound deployment without replacing
the running Monitoring v4 image. Credentials/tokens never enter evidence/logs.
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import io
import json
import math
import os
import re
import signal
import socket
import selectors
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler

UTC = timezone.utc
SCHEMA = "monitoring_v4.twitch_api_evidence.v1"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"
VALIDATE_URL = "https://id.twitch.tv/oauth2/validate"
USERS_URL = "https://api.twitch.tv/helix/users"
STREAMS_URL = "https://api.twitch.tv/helix/streams"
STAGES = ("credentials", "oauth", "validate", "users", "streams")
RESULTS = ("ok", "timeout", "network", "rate_limited", "auth", "upstream",
           "credentials", "response", "target", "clock", "collector")
TRANSPORT_PHASES = ("dns", "connect", "tls", "response_headers", "response_body", "deadline", "http", "unknown")
TIMED_PHASES = TRANSPORT_PHASES[:5]
CURL_PATH = "/usr/bin/curl"


class ProbeError(Exception):
    def __init__(self, reason: str, status: int = 0, retry_after: float = 0, *, rate_limit=None,
                 transport_phase="unknown", transport_seconds=None):
        super().__init__(reason)
        self.reason, self.status, self.retry_after = reason, status, retry_after
        self.rate_limit = rate_limit or {}
        self.transport_phase = transport_phase if transport_phase in TRANSPORT_PHASES else "unknown"
        self.transport_seconds = transport_seconds or {}


def rate_limit_fields(headers):
    """Retain only numeric quota metadata, never arbitrary response headers."""
    fields = {}
    for key, value in headers.items():
        name = {"ratelimit-remaining": "remaining", "ratelimit-limit": "limit",
                "ratelimit-reset": "reset"}.get(key.lower())
        if name:
            try:
                number = int(value)
                if 0 <= number <= 10**12:
                    fields[name] = number
            except (TypeError, ValueError):
                pass
    return fields


def error_category(reason, status=0):
    if status == 429: return "rate_limited"
    if status in (401, 403) or reason.endswith(("app_token_required", "token_expiring")): return "auth"
    if status >= 500: return "upstream"
    if reason.startswith("credentials_"): return "credentials"
    for suffix, category in (("timeout", "timeout"), ("network", "network"),
                             ("target_mismatch", "target"), ("clock_jump", "clock")):
        if reason.endswith(suffix): return category
    if "response_" in reason or "redirect_" in reason: return "response"
    return "collector"


def next_poll_deadline(anchor, now, interval, failures=0, retry_after=0):
    """Stay on the original monotonic grid; skip missed slots without catch-up.

    An isolated transient failure keeps the next grid slot. Consecutive failures
    back off; provider Retry-After is always a lower bound.
    """
    backoff = min(900, interval * 2 ** min(failures - 2, 4)) if failures >= 2 else 0
    earliest = now + max(backoff, retry_after)
    slots = max(0, math.ceil((earliest - anchor) / interval))
    due = anchor + slots * interval
    return due if due > now else due + interval


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        fp.close()
        raise ProbeError("redirect_rejected", code)


def utc(value: float) -> str:
    return datetime.fromtimestamp(value, UTC).isoformat(timespec="seconds")


def retry_delay(headers, now: float) -> float:
    headers = {name.lower(): value for name, value in headers.items()}
    values = [0.0]
    for name, offset in (("Retry-After", 0), ("Ratelimit-Reset", now)):
        try:
            value = float(headers.get(name.lower(), "0")) - offset
            if math.isfinite(value):
                values.append(max(0, value))
        except (TypeError, ValueError):
            pass
    return min(86400, max(values))


class TransportHeaders(dict):
    def __init__(self, values, seconds):
        super().__init__(values)
        self.transport_seconds = seconds


def _curl_config(url, headers, body):
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
        raise ProbeError("response_invalid")
    def quote(value):
        if not isinstance(value, str) or any(c in value for c in "\r\n\0"):
            raise ProbeError("response_invalid")
        return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'
    lines = ["url = " + quote(url)]
    for name, value in headers.items():
        if not re.fullmatch(r"[A-Za-z0-9-]+", name): raise ProbeError("response_invalid")
        lines.append("header = " + quote(name + ": " + value))
    if body is not None:
        try: lines.append("data-raw = " + quote(body.decode("ascii")))
        except UnicodeError: raise ProbeError("response_invalid") from None
    config = ("\n".join(lines) + "\n").encode()
    if len(config) > 4096: raise ProbeError("response_too_large")
    return config


def _curl_transfer(config, timeout, marker):
    """Secrets stay in stdin/memory; bound stdout, lifetime, and child cleanup."""
    footer = "\n" + marker + " %{http_code} %{time_namelookup} %{time_connect} %{time_appconnect} %{time_starttransfer} %{time_total}"
    command = [CURL_PATH, "-q", "--config", "-", "--silent", "--verbose", "--include", "--http1.1",
               "--proxy", "", "--noproxy", "*", "--proto", "=https", "--proto-redir", "=https",
               "--max-redirs", "0", "--retry", "0", "--max-time", str(timeout),
               "--max-filesize", "65536", "--write-out", footer]
    deadline = time.monotonic() + timeout + 0.5
    process = None
    output = bytearray()
    guard = None
    observed_phase, debug_status = None, 0
    pending, debug_size, discard_line = bytearray(), 0, False
    try:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
                                   start_new_session=True)
        try: process.stdin.write(config); process.stdin.close()
        except BrokenPipeError: pass
        os.set_blocking(process.stdout.fileno(), False)
        os.set_blocking(process.stderr.fileno(), False)
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, "output")
            selector.register(process.stderr, selectors.EVENT_READ, "debug")
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0: guard = "timeout"; break
                events = selector.select(remaining)
                if not events: guard = "timeout"; break
                for key, _ in events:
                    chunk = os.read(key.fd, 4096)
                    if not chunk: selector.unregister(key.fileobj); continue
                    if key.data == "output":
                        output.extend(chunk)
                        if len(output) > 196608: guard = "response_too_large"; break
                    else:
                        # Verbose headers may contain credentials. Consume only
                        # in RAM, retain fixed phase enums, and discard all text.
                        debug_size += len(chunk)
                        if debug_size > 65536: guard = "response_too_large"; break
                        pending.extend(chunk)
                        while b"\n" in pending:
                            line, _, rest = pending.partition(b"\n"); pending = bytearray(rest)
                            if discard_line: discard_line = False; continue
                            phase = None
                            if line.startswith(b"*   Trying "): phase = "connect"
                            elif line.startswith((b"* TLSv", b"* SSL connection")): phase = "tls"
                            elif line.startswith((b"> GET ", b"> POST ", b"> HEAD ")): phase = "response_headers"
                            elif line.startswith(b"< HTTP/"):
                                match = re.match(rb"< HTTP/[0-9.]+ ([0-9]{3})", line)
                                debug_status = int(match[1]) if match else 0
                            elif line.rstrip(b"\r") == b"< " and debug_status >= 200: phase = "response_body"
                            if phase and (observed_phase is None or TIMED_PHASES.index(phase) > TIMED_PHASES.index(observed_phase)):
                                observed_phase = phase
                        if len(pending) > 8192: pending.clear(); discard_line = True
                if guard: break
        if process.poll() is None:
            if guard: process.kill()
            else:
                try: process.wait(timeout=max(0.001, deadline - time.monotonic()))
                except subprocess.TimeoutExpired: guard = "timeout"; process.kill()
        process.wait(timeout=0.5)
        return bytes(output), process.returncode, guard, observed_phase
    except OSError:
        raise ProbeError("transport_unavailable") from None
    finally:
        if process is not None:
            if process.poll() is None: process.kill()
            process.wait(timeout=0.5)
            try: process.stdin.close()
            except OSError: pass
            process.stdout.close(); process.stderr.close()


def _transport_timings(metadata, failed):
    try:
        values = [float(value) for value in metadata.split()]
        if len(values) != 6 or any(not math.isfinite(v) or not 0 <= v <= 86400 for v in values):
            return "unknown", {}
        _, dns, connected, tls, first_byte, total = values
        ends = [dns, connected, tls, first_byte, total]
        durations, previous = {}, 0.0
        for phase, end in zip(TIMED_PHASES, ends):
            if failed and end == 0:
                durations[phase] = max(0.0, total - previous)
                return phase, durations
            durations[phase] = max(0.0, end - previous)
            previous = max(previous, end)
        return "response_body", durations
    except (TypeError, ValueError):
        return "unknown", {}


def request_json(url: str, headers: dict, body: bytes | None, timeout: float):
    if not math.isfinite(timeout) or timeout <= 0: raise ProbeError("timeout")
    marker = "__TWITCH_TRANSFER_" + uuid.uuid4().hex
    raw, code, guard, observed_phase = _curl_transfer(_curl_config(url, headers, body), timeout, marker)
    prefix, separator, metadata = raw.rpartition(("\n" + marker + " ").encode())
    phase, seconds = _transport_timings(metadata if separator else b"", code != 0 or guard is not None)
    content = prefix if separator else raw
    status, response_headers, response_body = 0, {}, b""
    try:
        stream = io.BytesIO(content)
        while True:
            line = stream.readline(4097)
            match = re.fullmatch(rb"HTTP/1\.[01] ([0-9]{3})[^\r\n]*\r?\n", line)
            if not match: break
            status = int(match[1]); response_headers = dict(http.client.parse_headers(stream).items())
            if not 100 <= status < 200: response_body = stream.read(); break
    except (ValueError, http.client.HTTPException):
        raise ProbeError("response_invalid", transport_phase=phase, transport_seconds=seconds) from None
    if status >= 300:
        reason = ("redirect_rejected" if status < 400 else "auth" if status in (401, 403)
                  else "rate_limited" if status == 429 else "upstream" if status >= 500 else "http")
        raise ProbeError(reason, status, retry_delay(response_headers, time.time()),
                         rate_limit=rate_limit_fields(response_headers), transport_phase="http", transport_seconds=seconds)
    if guard or code:
        reason = guard or ("timeout" if code == 28 else "response_too_large" if code == 63 else "network")
        if observed_phase and observed_phase != phase:
            # Missing end timestamps are not zero-duration completed phases.
            seconds = ({key: value for key, value in seconds.items()
                        if TIMED_PHASES.index(key) < TIMED_PHASES.index(phase)}
                       if phase in TIMED_PHASES else {})
            phase = observed_phase
        elif guard == "timeout" and observed_phase is None: phase = "deadline"
        raise ProbeError(reason, transport_phase=phase, transport_seconds=seconds)
    if status != 200 or len(response_body) > 65536:
        raise ProbeError("response_invalid", transport_phase=phase, transport_seconds=seconds)
    try:
        value = json.loads(response_body)
        if not isinstance(value, dict): raise ValueError()
    except (ValueError, UnicodeError):
        raise ProbeError("response_invalid", transport_phase="response_body", transport_seconds=seconds) from None
    return value, TransportHeaders(response_headers, seconds)


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
        self.probe_counts = {result: 0 for result in RESULTS}
        self.request_counts = {stage: 0 for stage in STAGES[1:]}
        self.request_seconds = {stage: 0.0 for stage in STAGES[1:]}
        self.transport_failures = {stage: {phase: 0 for phase in TRANSPORT_PHASES} for stage in STAGES[1:]}

    def _request(self, url, headers, body=None):
        self.request_count += 1
        started = self.monotonic()
        status, result = 0, "collector"
        transport_phase, transport_seconds = None, {}
        try:
            value, headers = self.transport(url, headers, body, self.timeout)
            transport_seconds = getattr(headers, "transport_seconds", {})
            status, result = 200, "ok"
            self.quota.update(rate_limit_fields(headers))
            return value
        except ProbeError as exc:
            status, result = exc.status, error_category(exc.reason, exc.status)
            transport_phase, transport_seconds = exc.transport_phase, exc.transport_seconds
            self.quota.update(exc.rate_limit)
            raise
        finally:
            elapsed = max(0.0, self.monotonic() - started)
            self.request_counts[self.stage] += 1
            self.request_seconds[self.stage] += elapsed
            record = {"stage": self.stage, "http_status": status,
                      "result": result, "duration_seconds": elapsed}
            if transport_seconds: record["transport_seconds"] = dict(transport_seconds)
            if transport_phase is not None:
                record["transport_phase"] = transport_phase
                self.transport_failures[self.stage][transport_phase] += 1
            self.request_stats.append(record)

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

    def collect(self, *, scheduled_at=None) -> dict:
        started, mono = self.wall(), self.monotonic()
        self.sequence += 1
        self.request_count, self.quota, self.stage = 0, {}, "credentials"
        self.request_stats = []
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
        category = "ok" if payload["probe_status"] == "ok" else error_category(payload["error_reason"], payload["http_status"])
        self.probe_counts[category] += 1
        payload.update(collected_at=utc(finished), valid_until=utc(finished + self.ttl),
                       freshness_ttl_sec=self.ttl, last_success_at=self.last_success,
                       api_request_count=self.request_count, rate_limit_remaining=self.quota.get("remaining"),
                       rate_limit_limit=self.quota.get("limit"), rate_limit_reset=self.quota.get("reset"),
                       error_category="" if category == "ok" else category, request_stage=self.stage,
                       probe_duration_seconds=max(0.0, self.monotonic() - mono),
                       schedule_lag_seconds=max(0.0, mono - scheduled_at) if scheduled_at is not None else 0.0,
                       request_stats=self.request_stats, probe_counts=dict(self.probe_counts),
                       request_counts=dict(self.request_counts), request_seconds=dict(self.request_seconds),
                       transport_failures={stage: dict(values) for stage, values in self.transport_failures.items()})
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
              "stream_v4_twitch_api_requests": payload["api_request_count"],
              "stream_v4_twitch_api_probe_duration_seconds": payload["probe_duration_seconds"],
              "stream_v4_twitch_api_schedule_lag_seconds": payload["schedule_lag_seconds"],
              "stream_v4_twitch_api_retry_after_seconds": payload["retry_after_sec"]}
    for field, metric in (("remaining", "remaining"), ("limit", "limit"), ("reset", "reset_unixtime")):
        value = payload.get("rate_limit_" + field)
        if value is not None: values["stream_v4_twitch_api_rate_limit_" + metric] = value
    for name, value in values.items():
        lines.extend(["# TYPE " + name + " gauge", name + " " + str(value)])
    for state in ("LIVE", "OFFLINE", "UNKNOWN"):
        lines.append('stream_v4_twitch_provider_state{state="' + state + '"} ' + str(int(payload["provider_state"] == state)))
    lines.append("# TYPE stream_v4_twitch_api_probes_total counter")
    for result in RESULTS:
        lines.append(f'stream_v4_twitch_api_probes_total{{result="{result}"}} {payload["probe_counts"][result]}')
    lines.append("# TYPE stream_v4_twitch_api_requests_total counter")
    lines.append("# TYPE stream_v4_twitch_api_request_duration_seconds summary")
    for stage in STAGES[1:]:
        labels = '{stage="' + stage + '"}'
        count = payload["request_counts"][stage]
        lines.extend([f'stream_v4_twitch_api_requests_total{labels} {count}',
                      f'stream_v4_twitch_api_request_duration_seconds_count{labels} {count}',
                      f'stream_v4_twitch_api_request_duration_seconds_sum{labels} {payload["request_seconds"][stage]}'])
    for stage, phases in payload.get("transport_failures", {}).items():
        for phase, count in phases.items():
            lines.append(f'stream_v4_twitch_api_transport_failures_total{{stage="{stage}",phase="{phase}"}} {count}')
    for request in payload.get("request_stats", []):
        for phase, seconds in request.get("transport_seconds", {}).items():
            lines.append(f'stream_v4_twitch_api_transport_phase_seconds{{stage="{request["stage"]}",phase="{phase}"}} {seconds}')
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
    scheduled = time.monotonic()
    # Mid-slot sampling leaves room for normal latency before the next UTC minute.
    anchor = scheduled + (args.interval_sec / 2 - time.time() % args.interval_sec) % args.interval_sec
    while not stopping:
        payload = collector.collect(scheduled_at=scheduled)
        atomic_write(output / "twitch_api_evidence.json", json.dumps(payload, sort_keys=True, allow_nan=False) + "\n")
        atomic_write(output / "twitch_api.prom", metrics(payload))
        print(json.dumps({k: payload[k] for k in ("request_started_at", "collected_at", "sequence", "probe_status", "provider_state", "error_reason", "error_category", "request_stage", "http_status", "api_request_count", "probe_duration_seconds", "schedule_lag_seconds", "rate_limit_remaining", "rate_limit_limit", "retry_after_sec", "request_stats")}), flush=True)
        if args.once:
            return 0 if payload["probe_status"] == "ok" else 1
        failures = 0 if payload["probe_status"] == "ok" else failures + 1
        deadline = next_poll_deadline(anchor, time.monotonic(), args.interval_sec, failures, payload["retry_after_sec"])
        scheduled = deadline
        while not stopping and time.monotonic() < deadline:
            time.sleep(min(0.5, deadline - time.monotonic()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
