"""Private, bounded publication receipts. Contains no provider credentials."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import ssl
import stat
import threading
import time
import urllib.error
import urllib.request
import urllib.parse

LIVE_FILES = ("stream-v3-prometheus.json", "relay-observation.json", "twitch-observation.json")
OBSERVATION_FILES = (*LIVE_FILES, "stream-v3-loki.json", "reliability-indicators.json",
                     "clock-observation.json", "oci-egress.json", "twitch-sli.json")
HEX = re.compile(r"[a-f0-9]{64}\Z")
MAX_EVENTS_BYTES = 512 * 1024


def failure(exc, *, stage, name=None):
    """Only fixed codes and exception types; never stringify transport errors."""
    code = "PUBLICATION_ERROR"
    status = getattr(exc, "status", None)
    reason = getattr(exc, "reason", exc)
    if type(status) is int and 100 <= status <= 599:
        code = "R2_HTTP_ERROR"
    elif isinstance(reason, socket.gaierror): code = "DNS_ERROR"
    elif isinstance(reason, (TimeoutError, socket.timeout)): code = "TIMEOUT"
    elif isinstance(reason, ssl.SSLError): code = "TLS_ERROR"
    elif isinstance(exc, urllib.error.URLError): code = "TRANSPORT_ERROR"
    elif isinstance(exc, json.JSONDecodeError): code = "INVALID_JSON"
    elif type(exc).__name__ == "PublicationError" and exc.args:
        # These strings originate in our code, never in a provider response.
        candidate = exc.args[0]
        allowed = {
            "PUBLIC_SOURCE_STALE_OR_FUTURE", "PUBLIC_SOURCE_TIMESTAMP_INVALID", "SOURCE_CHANGED_DURING_READ", "SOURCE_SYMLINK",
            "SOURCE_NOT_BOUNDED_REGULAR_FILE", "PUBLIC_TREE_TOO_LARGE", "PUBLIC_BUILD_FAILED",
            "PUBLICATION_DEADLINE_EXCEEDED", "R2_UPLOAD_OUTCOME_UNCONFIRMED",
            "R2_UPLOAD_VERIFICATION_FAILED", "R2_UPLOAD_NOT_ACKNOWLEDGED", "R2_UPLOAD_KEY_MISMATCH",
            "R2_RESPONSE_TOO_LARGE", "R2_REDIRECT_REFUSED", "R2_REQUEST_NOT_SENT", "DUPLICATE_JSON_KEY",
            "NONFINITE_JSON_VALUE", "JSON_OBJECT_REQUIRED", "STATIC_RELEASE_INCOMPLETE",
        }
        if candidate in allowed: code = candidate
    out = {"stage": stage, "code": code, "exception_type": type(exc).__name__}
    if isinstance(reason, OSError) and getattr(reason, 'http_request_not_sent', False) is True:
        out['request_not_sent'] = True
    if name is not None: out["object"] = name
    if type(status) is int and 100 <= status <= 599: out["http_status"] = status
    if isinstance(reason, socket.gaierror):
        out['gai_errno'] = reason.errno if type(reason.errno) is int else None
        out['gai_name'] = next((name for name in dir(socket) if name.startswith('EAI_')
                                and getattr(socket, name) == reason.errno), 'OTHER')
        for key in ('dns_elapsed_seconds',):
            v = getattr(reason, key, None)
            if type(v) in (int, float) and math.isfinite(v): out[key] = round(v, 6)
        fingerprint = getattr(reason, 'resolver_config_sha256', '')
        if isinstance(fingerprint, str) and HEX.fullmatch(fingerprint):
            out['resolver_config_sha256'] = fingerprint
        probes = getattr(reason, 'dns_probes', [])
        if isinstance(probes, list):
            out['dns_probes'] = []
            for probe in probes[:3]:
                if not isinstance(probe, dict) or probe.get('code') not in {'DNS_REPLY', 'DNS_TIMEOUT', 'DNS_PROBE_FAILED'}: continue
                row = {'code': probe['code']}
                for key in ('resolver', 'rcode', 'answers', 'errno'):
                    if type(probe.get(key)) is int: row[key] = probe[key]
                v = probe.get('elapsed_seconds')
                if type(v) in (int, float) and math.isfinite(v): row['elapsed_seconds'] = round(v, 6)
                out['dns_probes'].append(row)
    elapsed = getattr(exc, 'elapsed_seconds', None)
    if type(elapsed) in (int,float) and math.isfinite(elapsed): out['elapsed_seconds'] = round(elapsed, 3)
    return out


class Receipts:
    def __init__(self, path, events, *, binding, allowed, write, seed=None):
        self.path, self.events = Path(path), Path(events)
        self.binding, self.allowed, self.write = binding, set(allowed), write
        self.lock = threading.RLock()
        self.objects = {}
        if self.path.is_symlink():
            raise ValueError("invalid_receipts")
        if self.path.exists():
            if self.path.is_symlink() or self.path.stat().st_size > 65536:
                raise ValueError("invalid_receipts")
            value = json.loads(self.path.read_bytes())
            if value.get("schema") != "public.r2_receipts.v1" or value.get("destination") != binding:
                raise ValueError("receipt_binding_mismatch")
            self.objects = value["objects"]
            if not isinstance(self.objects, dict) or set(self.objects) - self.allowed:
                raise ValueError("invalid_receipt_objects")
            for row in self.objects.values():
                if (not isinstance(row, dict) or set(row) != {"state", "sha256"} or row["state"] not in {"VERIFIED", "OUTCOME_UNKNOWN", "NOT_SENT"}
                        or not isinstance(row["sha256"], str) or not HEX.fullmatch(row["sha256"])):
                    raise ValueError("invalid_receipt")
        else:
            self.objects = {name: {"state": "VERIFIED", "sha256": digest}
                            for name, digest in (seed or {}).items()
                            if name in self.allowed and isinstance(digest, str) and HEX.fullmatch(digest)}
            self._save()

    def _save(self):
        self.write(self.path, {"schema": "public.r2_receipts.v1", "destination": self.binding,
                               "objects": self.objects})

    def get(self, name):
        with self.lock: return dict(self.objects.get(name, {}))

    def set(self, name, state, digest):
        if name not in self.allowed or state not in {"VERIFIED", "OUTCOME_UNKNOWN", "NOT_SENT"} or not HEX.fullmatch(digest):
            raise ValueError("invalid_receipt_update")
        with self.lock:
            self.objects[name] = {"state": state, "sha256": digest}
            self._save()  # durable before PUT; uncertainty survives cancellation/crash.

    def event(self, value):
        raw = (json.dumps({"at_epoch": time.time(), **value}, sort_keys=True) + "\n").encode()
        if len(raw) > 8192: raise ValueError("diagnostic_too_large")
        with self.lock:
            if self.events.exists():
                st = self.events.lstat()
                if not stat.S_ISREG(st.st_mode): raise ValueError("invalid_diagnostic_file")
                if st.st_size + len(raw) > MAX_EVENTS_BYTES:
                    previous = self.events.with_suffix(self.events.suffix + ".1")
                    if (previous.exists() or previous.is_symlink()) and not stat.S_ISREG(previous.lstat().st_mode):
                        raise ValueError("invalid_diagnostic_file")
                    os.replace(self.events, previous)
            fd = os.open(self.events, os.O_CREAT | os.O_APPEND | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "ab") as handle:
                handle.write(raw); handle.flush(); os.fsync(handle.fileno())


def health_observation(*, deadline, opener=None, url=None):
    """Optional configured HTTPS GET; retain only allowlisted diagnostic fields."""
    url = os.environ.get("YUKIMURATA_PUBLICATION_HEALTH_URL", "") if url is None else url
    if not url:
        return {"code": "HEALTH_NOT_CONFIGURED"}
    try:
        parsed = urllib.parse.urlsplit(url)
        valid = (parsed.scheme == 'https' and bool(parsed.hostname) and parsed.username is None
                 and parsed.password is None and parsed.port in (None, 443)
                 and parsed.path == '/health/publication' and not parsed.query and not parsed.fragment)
    except (TypeError, ValueError):
        valid = False
    if not valid:
        return {"code": "HEALTH_URL_INVALID"}
    remaining = deadline - time.monotonic()
    if remaining <= 0: return {"code": "HEALTH_BUDGET_EXHAUSTED"}
    request = urllib.request.Request(url,
        headers={"User-Agent": "stream-v3-publication-health/1.0", "Cache-Control": "no-cache"})
    result = {}
    try:
        try:
            response = (opener or urllib.request.urlopen)(request, timeout=min(4, remaining))
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            raw = response.read(32769); status = response.status
        result = {"http_status": status}
        if len(raw) > 32768: return {**result, "code": "HEALTH_BODY_TOO_LARGE"}
        value = json.loads(raw)
        if not isinstance(value, dict) or value.get("schema") != "public.publication_health.v1":
            return {**result, "code": "HEALTH_SCHEMA_INVALID"}
        result["status"] = value.get("status") if value.get("status") in {"ok", "unhealthy"} else "unknown"
        issues = value.get("issues", [])
        result["issues"] = [x for x in issues if isinstance(x, str) and x in {"prom", "relay", "twitch", "loki", "rel", "clock", "tsli", "egress"}] if isinstance(issues, list) else []
        result["page"] = value.get("page") if value.get("page") in {"available", "unavailable"} else "unknown"
        states = {"fresh", "stale", "missing", "future", "invalid_schema", "invalid_timestamp", "read_or_parse_failed", "oversized"}
        result["sources"] = []
        sources = value.get("sources", [])
        for row in (sources if isinstance(sources, list) else [])[:8]:
            if not isinstance(row, dict) or row.get("state") not in states: continue
            if row.get("source") not in {"prom", "relay", "twitch", "loki", "rel", "clock", "tsli", "egress"}: continue
            entry = {"source": row["source"], "state": row["state"]}
            for key in ("generatedAt", "ageSeconds"):
                v = row.get(key)
                if type(v) in (int, float) and math.isfinite(v): entry[key] = v
            result["sources"].append(entry)
        return result
    except (OSError, ValueError, TypeError, KeyError) as exc:
        return {**result, **failure(exc, stage="health_get")}
