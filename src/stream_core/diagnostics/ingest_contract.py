from __future__ import annotations

from pathlib import Path
from typing import Callable
from urllib.parse import urlparse

from .model import CheckResult


YOUTUBE_RTMP_HOSTS = {"a.rtmp.youtube.com", "a.rtmps.youtube.com"}
PREFERRED_YOUTUBE_RTMPS_URL = "rtmps://a.rtmps.youtube.com:443/live2"
PLACEHOLDER_STREAM_KEYS = {
    "",
    "YOUR_STREAM_KEY",
    "YOUR_REAL_STREAM_KEY",
    "REPLACE_WITH_YOUR_YOUTUBE_KEY",
    "REPLACE_ME",
}


def parse_stream_key_from_rtmp_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"rtmp", "rtmps"}:
        return ""
    host = (parsed.hostname or "").strip().lower()
    if host not in YOUTUBE_RTMP_HOSTS:
        return ""
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2 or parts[0] != "live2":
        return ""
    return parts[1].strip()


def stream_ingest_endpoint_status(
    read_env_file: Callable[[Path], dict[str, str]],
    env_path: Path = Path("/etc/default/adsb-streamnew"),
) -> dict:
    cfg = read_env_file(env_path)
    raw_url = cfg.get("RTMP_URL", "").strip()
    stream_key = cfg.get("STREAM_KEY", "").strip()
    placeholder_key = stream_key in PLACEHOLDER_STREAM_KEYS
    effective_url = raw_url or PREFERRED_YOUTUBE_RTMPS_URL
    parsed = urlparse(effective_url)
    scheme = (parsed.scheme or "").lower()
    host = (parsed.hostname or "").lower()
    port = parsed.port
    parts = [part for part in parsed.path.split("/") if part]
    live2_path = bool(parts and parts[0] == "live2")
    youtube_host = host in YOUTUBE_RTMP_HOSTS
    rtmps_preferred = scheme == "rtmps" and host == "a.rtmps.youtube.com" and port in {443, None} and live2_path
    rtmp_legacy = scheme == "rtmp" and host == "a.rtmp.youtube.com" and live2_path
    ok = bool((rtmps_preferred or rtmp_legacy) and not placeholder_key)
    if placeholder_key:
        judgment = "placeholder_stream_key"
        reason = "STREAM_KEY is not configured"
    elif rtmps_preferred and port == 443:
        judgment = "rtmps_preferred"
        reason = "RTMPS ingest endpoint uses explicit port 443"
    elif rtmps_preferred:
        judgment = "rtmps_preferred_implicit_443"
        reason = "RTMPS ingest endpoint is configured without explicit port 443"
    elif rtmp_legacy:
        judgment = "rtmp_legacy"
        reason = "legacy RTMP ingest is still configured"
    elif not youtube_host:
        judgment = "unknown_host"
        reason = "RTMP_URL host is not a known YouTube ingest host"
    else:
        judgment = "invalid"
        reason = "RTMP_URL is not a supported YouTube RTMP/RTMPS live2 endpoint"
    return {
        "ok": ok,
        "path": str(env_path),
        "scheme": scheme,
        "host": host,
        "port": port,
        "live2_path": live2_path,
        "stream_key_configured": not placeholder_key,
        "judgment": judgment,
        "reason": reason,
        "preferred_url": PREFERRED_YOUTUBE_RTMPS_URL,
    }


def ingest_result(status: dict) -> CheckResult:
    judgment = str(status.get("judgment", ""))
    if judgment == "rtmps_preferred":
        severity = "ok"
        ok = True
        fatal = False
    elif judgment in {"rtmps_preferred_implicit_443", "rtmp_legacy"}:
        severity = "warn"
        ok = True
        fatal = False
    else:
        severity = "fail"
        ok = False
        fatal = True
    endpoint = f"{status.get('scheme', '')}://{status.get('host', '')}"
    if status.get("port"):
        endpoint += f":{status['port']}"
    if status.get("live2_path"):
        endpoint += "/live2"
    return CheckResult(
        name="ingest:youtube_endpoint",
        category="ingest_contract",
        severity=severity,
        ok=ok,
        fatal=fatal,
        summary=f"ingest endpoint: {status.get('reason', '')}",
        detail=endpoint,
        path=str(status.get("path", "")),
        data=status,
    )
