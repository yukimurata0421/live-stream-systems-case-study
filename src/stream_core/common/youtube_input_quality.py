from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable


DEFAULT_INPUT_QUALITY_MAX_AGE_SEC = 600
INPUT_QUALITY_BAD_SEVERITIES = frozenset({"warning", "error"})


def parse_utc_timestamp(value: object) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def normalize_health_issue_details(raw: object) -> tuple[dict[str, str], ...]:
    """Keep only stable, non-secret fields needed to classify YouTube input quality."""
    if not isinstance(raw, Iterable) or isinstance(raw, (str, bytes, dict)):
        return ()
    normalized: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        issue_type = str(item.get("type", "")).strip()[:80]
        severity = str(item.get("severity", "")).strip().lower()[:16]
        if not issue_type and not severity:
            continue
        normalized.append({"type": issue_type, "severity": severity})
        if len(normalized) >= 32:
            break
    return tuple(normalized)


def _nonnegative_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def classify_input_quality_sample(
    payload: dict[str, Any],
    *,
    now_ts: int,
    max_age_sec: int = DEFAULT_INPUT_QUALITY_MAX_AGE_SEC,
) -> dict[str, Any]:
    """Classify one watchdog snapshot without merging local outages into input quality.

    Eligibility is intentionally narrower than availability: a fresh successful OAuth
    probe must describe an active YouTube stream while the local ingest is connected.
    An eligible sample is good only when YouTube reports ``healthStatus=good`` and
    no persisted configuration issue has warning/error severity.
    """
    if "oauth_checked_ts_utc" in payload:
        checked_ts = parse_utc_timestamp(payload.get("oauth_checked_ts_utc"))
    else:
        checked_ts = parse_utc_timestamp(payload.get("ts_utc"))
    age_sec = now_ts - checked_ts if checked_ts > 0 else None
    fresh = bool(
        age_sec is not None
        and age_sec >= -60
        and age_sec <= max(1, int(max_age_sec))
    )
    probe_ok = payload.get("oauth_probe_ok") is True
    stream_status = str(payload.get("oauth_stream_status", "")).strip().lower()
    health_status = str(payload.get("oauth_stream_health_status", "")).strip().lower()
    ingest_connected = payload.get("ingest_connected") is True
    details = normalize_health_issue_details(payload.get("oauth_stream_health_issue_details"))
    issue_count = max(
        _nonnegative_int(payload.get("oauth_stream_health_issues")),
        len(details),
    )
    warning_or_worse_count = sum(
        item.get("severity") in INPUT_QUALITY_BAD_SEVERITIES for item in details
    )

    eligible = fresh and probe_ok and stream_status == "active" and ingest_connected
    good = eligible and health_status == "good" and warning_or_worse_count == 0

    if not fresh:
        classification = "ineligible_oauth_probe_stale_or_missing"
    elif not probe_ok:
        classification = "ineligible_oauth_probe_failed"
    elif stream_status != "active":
        classification = "ineligible_stream_not_active"
    elif not ingest_connected:
        classification = "ineligible_local_ingest_disconnected"
    elif good:
        classification = "good"
    elif health_status == "nodata":
        classification = "bad_health_nodata"
    elif health_status == "ok":
        classification = "bad_health_warning"
    elif health_status == "bad":
        classification = "bad_health_error"
    elif warning_or_worse_count:
        classification = "bad_configuration_issue"
    else:
        classification = "bad_health_unknown"

    return {
        "eligible": eligible,
        "good": good,
        "fresh": fresh,
        "probe_ok": probe_ok,
        "checked_ts": checked_ts,
        "age_sec": age_sec,
        "stream_status": stream_status,
        "health_status": health_status,
        "ingest_connected": ingest_connected,
        "issue_count": issue_count,
        "warning_or_worse_count": warning_or_worse_count,
        "issue_details_available": bool(details) or issue_count == 0,
        "classification": classification,
    }
