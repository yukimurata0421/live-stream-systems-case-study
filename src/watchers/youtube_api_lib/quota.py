from __future__ import annotations

import json
import time
import urllib.error
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    import fcntl
except Exception:  # pragma: no cover
    fcntl = None

try:
    from ..youtube_watchdog_config import (
        API_CALL_LOG_FILE,
        QUOTA_EXHAUSTED_COOLDOWN_SEC,
        QUOTA_RESET_MARGIN_SEC,
    )
    from ..youtube_watchdog_state import (
        append_event,
        quota_exhausted_active,
        update_quota_state,
        utc_now,
    )
except ImportError:
    from youtube_watchdog_config import (
        API_CALL_LOG_FILE,
        QUOTA_EXHAUSTED_COOLDOWN_SEC,
        QUOTA_RESET_MARGIN_SEC,
    )
    from youtube_watchdog_state import (
        append_event,
        quota_exhausted_active,
        update_quota_state,
        utc_now,
    )


API_COST_UNITS = {
    "search.list": 100,
    "videos.list": 1,
    "videos.update": 50,
    "liveBroadcasts.list": 1,
    "liveStreams.list": 1,
    "liveBroadcasts.insert": 50,
    "liveBroadcasts.bind": 50,
    "liveBroadcasts.delete": 50,
    "liveBroadcasts.transition": 50,
}


def _http_error_body(err: urllib.error.HTTPError) -> str:
    cached = getattr(err, "_ytw_body_cache", None)
    if isinstance(cached, str):
        return cached
    try:
        body = err.read().decode("utf-8", errors="ignore")
        setattr(err, "_ytw_body_cache", body)
        return body
    except Exception:
        return ""


@contextmanager
def _file_lock(path: Path):
    lock_path = path.with_name(f"{path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_fh:
        if fcntl is not None:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)


def _append_api_call_event(
    *,
    method: str,
    status: str,
    detail: str = "",
    http_code: int = 0,
    quota_exceeded: bool = False,
    source: str = "",
) -> None:
    payload = {
        "ts_utc": utc_now(),
        "source": source or "youtube_api",
        "method": method,
        "cost_units": int(API_COST_UNITS.get(method, 0) or 0),
        "status": status,
        "http_code": http_code or None,
        "quota_exceeded": bool(quota_exceeded),
        "detail": (detail or "")[:320],
    }
    try:
        path = Path(API_CALL_LOG_FILE).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
        with _file_lock(path):
            with path.open("a", encoding="utf-8") as fh:
                fh.write(line)
    except Exception:
        pass


def _is_quota_exceeded_error(code: int, detail: str) -> bool:
    if code != 403:
        return False
    text = (detail or "").lower()
    if "exceeded your quota" in text:
        return True
    try:
        payload = json.loads(detail or "{}")
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    err = payload.get("error")
    if not isinstance(err, dict):
        return False
    errors = err.get("errors")
    if not isinstance(errors, list):
        return False
    for item in errors:
        if not isinstance(item, dict):
            continue
        reason = str(item.get("reason", "")).strip()
        if reason in {"quotaExceeded", "dailyLimitExceeded"}:
            return True
    return False


def _extract_google_error_reason(detail: str) -> str:
    try:
        payload = json.loads(detail or "{}")
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    err = payload.get("error")
    if not isinstance(err, dict):
        return ""
    errors = err.get("errors")
    if isinstance(errors, list):
        for item in errors:
            if not isinstance(item, dict):
                continue
            reason = str(item.get("reason", "")).strip()
            if reason:
                return reason
    return ""


def _has_google_error_reason(detail: str, reason: str) -> bool:
    expected = reason.strip()
    if not expected:
        return False
    if _extract_google_error_reason(detail) == expected:
        return True
    return f'"reason": "{expected}"' in detail or f'"reason":"{expected}"' in detail


def _is_rate_limited_error(code: int, detail: str) -> bool:
    if code not in {403, 429}:
        return False
    text = (detail or "").lower()
    if "rate limit" in text:
        return True
    try:
        payload = json.loads(detail or "{}")
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    err = payload.get("error")
    if not isinstance(err, dict):
        return False
    errors = err.get("errors")
    if not isinstance(errors, list):
        return False
    for item in errors:
        if not isinstance(item, dict):
            continue
        reason = str(item.get("reason", "")).strip()
        if reason in {"rateLimitExceeded", "userRateLimitExceeded"}:
            return True
    return False


def _iso_utc_from_unix(unix_ts: int) -> str:
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def next_quota_reset_ts_pacific(now_ts: int) -> int:
    pt = ZoneInfo("America/Los_Angeles")
    now_pt = datetime.fromtimestamp(now_ts, tz=pt)
    next_midnight_pt = now_pt.replace(hour=0, minute=0, second=0, microsecond=0)
    if next_midnight_pt <= now_pt:
        next_midnight_pt = next_midnight_pt + timedelta(days=1)
    return int(next_midnight_pt.timestamp())


def quota_guard_until_ts_pacific(now_ts: int) -> int:
    base = next_quota_reset_ts_pacific(now_ts)
    if QUOTA_RESET_MARGIN_SEC <= 0:
        return base
    return base + QUOTA_RESET_MARGIN_SEC


def quota_guard_status(now_ts: int | None = None) -> tuple[bool, str, dict]:
    current = int(time.time()) if now_ts is None else int(now_ts)
    active, state = quota_exhausted_active(current)
    if not active:
        return False, "quota guard inactive", state
    until_ts = int(state.get("quota_exhausted_until_ts", 0) or 0)
    left = max(0, until_ts - current) if until_ts > 0 else 0
    until_iso = _iso_utc_from_unix(until_ts) if until_ts > 0 else "-"
    source = str(state.get("quota_exhausted_source", "")).strip() or "unknown"
    return True, f"quota exhausted guard active until {until_iso} ({left}s left, source={source})", state


def mark_quota_exhausted(
    source: str,
    detail: str,
    now_ts: int | None = None,
    reason_hint: str = "",
) -> tuple[bool, str]:
    current = int(time.time()) if now_ts is None else int(now_ts)
    try:
        until_ts = quota_guard_until_ts_pacific(current)
    except Exception:
        if QUOTA_EXHAUSTED_COOLDOWN_SEC <= 0:
            return False, "quota guard disabled (no PT timezone and cooldown disabled)"
        until_ts = current + QUOTA_EXHAUSTED_COOLDOWN_SEC

    normalized_reason = reason_hint.strip()
    reason_source = "google_error_reason"
    if not normalized_reason:
        text = (detail or "").lower()
        if "daily limit" in text:
            normalized_reason = "dailyLimitExceeded"
            reason_source = "google_error_reason_fallback"
        elif "exceeded your quota" in text:
            normalized_reason = "quotaExceeded"
            reason_source = "message_fallback"
        else:
            normalized_reason = "unknown"
            reason_source = "message_fallback"

    def _updater(state: dict) -> tuple[dict, dict]:
        since_ts = int(state.get("quota_exhausted_since_ts", 0) or 0)
        if since_ts <= 0:
            since_ts = current
        was_active = bool(state.get("quota_exhausted", False))
        state.update(
            {
                "quota_exhausted": True,
                "quota_exhausted_since_ts": since_ts,
                "quota_exhausted_until_ts": until_ts,
                "quota_exhausted_source": source,
                "quota_exhausted_reason_code": normalized_reason,
                "quota_exhausted_reason": detail[:600],
                "quota_exhausted_updated_at_utc": utc_now(),
            }
        )
        return state, {"was_active": was_active}

    update_result = update_quota_state(_updater)
    if not bool(update_result.get("was_active")):
        append_event(
            {
                "event": "youtube_quota_guard_activated",
                "reason": normalized_reason,
                "source": reason_source,
                "quota_exhausted_source": source,
                "quota_exhausted_until_ts": until_ts,
            }
        )
    left = max(0, until_ts - current)
    return True, f"quota exhausted guard latched for {left}s (source={source})"
