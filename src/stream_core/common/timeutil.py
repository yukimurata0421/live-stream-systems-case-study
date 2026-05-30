from __future__ import annotations

import calendar
import time
from datetime import datetime
from zoneinfo import ZoneInfo


def parse_utc_ts(ts: str) -> int:
    try:
        return int(calendar.timegm(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")))
    except Exception:
        return 0


def utc_now_text(now_ts: int | None = None) -> str:
    ts = int(time.time() if now_ts is None else now_ts)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def utc_text_from_ts(ts: int | float | None) -> str:
    try:
        value = int(ts or 0)
    except Exception:
        value = 0
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(value)) if value > 0 else ""


def jst_text(now_ts: int | None = None) -> str:
    ts = int(time.time() if now_ts is None else now_ts) + 9 * 3600
    return time.strftime("%Y-%m-%d %H:%M:%S JST", time.gmtime(ts))


def jst_text_or_unknown(ts: int | None) -> str:
    try:
        value = int(ts or 0)
    except Exception:
        value = 0
    return jst_text(value) if value > 0 else "unknown"


def jst_day(ts: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(int(ts) + 9 * 3600))


def pt_day(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")

