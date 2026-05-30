from __future__ import annotations

import re
import subprocess
import urllib.request
from dataclasses import dataclass
from typing import Callable


def extract_video_id(url: str) -> str:
    if not url:
        return ""
    match = re.search(r"(?:youtube\.com/live/|youtu\.be/|v=)([A-Za-z0-9_-]{8,})", url)
    return match.group(1) if match else ""


def resolve_video_id_from_live_page(
    live_page_url: str,
    timeout_sec: int | None = None,
    *,
    default_timeout_sec: int,
    urlopen: Callable = urllib.request.urlopen,
) -> tuple[str, str]:
    if not live_page_url:
        return "", "channel live page skipped"
    try:
        req = urllib.request.Request(
            live_page_url,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome Safari",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        timeout = default_timeout_sec if timeout_sec is None else max(1, int(timeout_sec))
        with urlopen(req, timeout=timeout) as r:
            final_url = r.geturl()
            html = r.read().decode("utf-8", errors="ignore")
        vid = extract_video_id(final_url)
        if vid:
            return vid, "resolved from channel live redirect"
        match = re.search(r'"videoId":"([A-Za-z0-9_-]{8,})"', html)
        if match:
            return match.group(1), "resolved from channel live html"
        return "", "channel live page had no video id"
    except Exception as exc:
        return "", f"channel live page failed: {exc}"


@dataclass(frozen=True)
class WatchPageProbeResult:
    checked: bool
    verdict: str
    fatal: bool
    reason: str

    @property
    def ok_for_availability(self) -> bool:
        return not self.fatal and self.verdict != "not_live"


def check_public_watch_page_verdict(url: str, *, fetch_text: Callable[[str], str]) -> WatchPageProbeResult:
    if not url:
        return WatchPageProbeResult(False, "unknown", False, "watch page check skipped (no YTW_LIVE_URL)")
    try:
        html = fetch_text(url)
    except Exception as exc:
        return WatchPageProbeResult(True, "unknown", True, f"watch page fetch failed: {exc}")
    live_markers = [
        '"isLiveNow":true',
        '"isLive":true',
        '"badgeStyleType":"BADGE_STYLE_TYPE_LIVE_NOW"',
    ]
    for marker in live_markers:
        if marker in html:
            return WatchPageProbeResult(True, "live", False, f"watch page live marker detected ({marker})")

    if '"playabilityStatus":{"status":"ERROR"' in html:
        return WatchPageProbeResult(True, "not_live", True, "watch page playability error")
    if '"playabilityStatus":{"status":"UNPLAYABLE"' in html:
        return WatchPageProbeResult(True, "not_live", True, "watch page unplayable")
    if '"playabilityStatus":{"status":"LOGIN_REQUIRED"' in html:
        return WatchPageProbeResult(True, "not_live", True, "watch page login required")
    if len(html) < 2048:
        return WatchPageProbeResult(True, "unknown", True, "watch page response too short")
    return WatchPageProbeResult(True, "unknown", False, "watch page live marker inconclusive (treated as unknown)")


def check_public_watch_page_nonfatal(url: str, *, fetch_text: Callable[[str], str]) -> tuple[bool, str]:
    result = check_public_watch_page_verdict(url, fetch_text=fetch_text)
    return result.ok_for_availability, result.reason


def check_public_watch_page(url: str, *, fetch_text: Callable[[str], str]) -> tuple[bool, str]:
    return check_public_watch_page_nonfatal(url, fetch_text=fetch_text)


@dataclass(frozen=True)
class PublicLiveProbeResult:
    checked: bool
    verdict: str
    reason: str
    video_id: str = ""
    live_status: str = ""
    is_live: bool | None = None
    was_live: bool | None = None
    availability: str = ""


def _parse_probe_bool(raw: str) -> bool | None:
    value = (raw or "").strip().lower()
    if value in {"true", "1", "yes"}:
        return True
    if value in {"false", "0", "no"}:
        return False
    return None


def parse_public_live_probe_output(output: str) -> PublicLiveProbeResult:
    lines = [line.strip() for line in (output or "").splitlines() if line.strip()]
    if not lines:
        return PublicLiveProbeResult(False, "unknown", "public live probe produced no output")
    parts = lines[-1].split("\t")
    while len(parts) < 5:
        parts.append("")
    video_id, live_status, is_live_raw, was_live_raw, availability = parts[:5]
    live_status_norm = live_status.strip().lower()
    is_live = _parse_probe_bool(is_live_raw)
    was_live = _parse_probe_bool(was_live_raw)
    if is_live is True or live_status_norm in {"is_live", "live"}:
        verdict = "live"
    elif live_status_norm in {"was_live", "not_live", "post_live"} or was_live is True:
        verdict = "not_live"
    else:
        verdict = "unknown"
    reason = (
        f"public live probe verdict={verdict} video_id={video_id or '-'} "
        f"live_status={live_status or '-'} is_live={is_live_raw or '-'} "
        f"was_live={was_live_raw or '-'} availability={availability or '-'}"
    )
    return PublicLiveProbeResult(
        True,
        verdict,
        reason,
        video_id=video_id,
        live_status=live_status,
        is_live=is_live,
        was_live=was_live,
        availability=availability,
    )


def probe_public_live_status(
    url: str,
    timeout_sec: int | None = None,
    *,
    public_live_probe_timeout_sec: int,
    run_cmd: Callable = subprocess.run,
) -> PublicLiveProbeResult:
    if not url:
        return PublicLiveProbeResult(False, "unknown", "public live probe skipped (no URL)")
    timeout = public_live_probe_timeout_sec if timeout_sec is None else max(1, int(timeout_sec))
    cmd = [
        "yt-dlp",
        "--skip-download",
        "--no-warnings",
        "--print",
        "%(id)s\t%(live_status)s\t%(is_live)s\t%(was_live)s\t%(availability)s",
        url,
    ]
    try:
        cp = run_cmd(cmd, text=True, capture_output=True, check=False, timeout=timeout)
    except FileNotFoundError:
        return PublicLiveProbeResult(False, "unknown", "public live probe skipped (yt-dlp not installed)")
    except subprocess.TimeoutExpired:
        return PublicLiveProbeResult(False, "unknown", "public live probe timed out")
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or "").strip()
        detail_short = detail.replace("\n", " ")[:240]
        if "not currently live" in detail.lower():
            return PublicLiveProbeResult(True, "not_live", f"public live probe verdict=not_live: {detail_short}")
        return PublicLiveProbeResult(False, "unknown", f"public live probe failed: {detail_short}")
    return parse_public_live_probe_output(cp.stdout)
