from __future__ import annotations

import json
import urllib.error
import urllib.parse
from typing import Callable

try:
    from ..youtube_watchdog_config import DataApiCheckResult
except ImportError:
    from youtube_watchdog_config import DataApiCheckResult

from .quota import _extract_google_error_reason, _http_error_body, _is_quota_exceeded_error, _is_rate_limited_error


def check_data_api(
    video_id: str,
    api_key: str,
    timeout_sec: int | None = None,
    *,
    fetch_text: Callable[..., str],
    append_api_call_event: Callable[..., None],
    mark_quota_exhausted: Callable[..., tuple[bool, str]],
) -> DataApiCheckResult:
    if not video_id or not api_key:
        return DataApiCheckResult(False, False, "skipped", "data api check skipped")
    query = urllib.parse.urlencode(
        {
            "part": "snippet,liveStreamingDetails,status",
            "id": video_id,
            "key": api_key,
        }
    )
    url = f"https://www.googleapis.com/youtube/v3/videos?{query}"
    try:
        payload = json.loads(fetch_text(url, timeout_sec=timeout_sec))
        append_api_call_event(method="videos.list", status="ok", source="check_data_api")
    except urllib.error.HTTPError as exc:
        body = _http_error_body(exc)
        detail = f"data api http {exc.code}: {body[:240]}"
        quota_exceeded = _is_quota_exceeded_error(exc.code, body)
        rate_limited = _is_rate_limited_error(exc.code, body)
        append_api_call_event(
            method="videos.list",
            status="http_error",
            detail=detail,
            http_code=int(getattr(exc, "code", 0) or 0),
            quota_exceeded=quota_exceeded,
            source="check_data_api",
        )
        if quota_exceeded:
            _latched, quota_note = mark_quota_exhausted(
                "data_api_videos",
                detail,
                reason_hint=_extract_google_error_reason(body),
            )
            detail = f"{detail}; {quota_note}"
            return DataApiCheckResult(True, False, "quota_exhausted", detail)
        if rate_limited:
            return DataApiCheckResult(True, False, "rate_limited", detail)
        return DataApiCheckResult(True, False, "error", detail)
    except Exception as exc:
        append_api_call_event(method="videos.list", status="error", detail=str(exc), source="check_data_api")
        return DataApiCheckResult(True, False, "error", f"data api fetch failed: {exc}")
    items = payload.get("items", [])
    if not items:
        return DataApiCheckResult(True, False, "none", "data api no items for video id")
    item = items[0] if isinstance(items[0], dict) else {}
    snippet = item.get("snippet", {}) or {}
    details = item.get("liveStreamingDetails", {}) or {}
    lbc = str(snippet.get("liveBroadcastContent", "")).strip()
    if lbc == "live":
        return DataApiCheckResult(True, True, "live", "data api says live")

    actual_start = str(details.get("actualStartTime", "")).strip()
    actual_end = str(details.get("actualEndTime", "")).strip()
    if actual_end:
        return DataApiCheckResult(
            True,
            False,
            "ended",
            f"data api stream ended (lbc={lbc or 'empty'}, ended={actual_end})",
        )
    if lbc == "upcoming":
        return DataApiCheckResult(
            True,
            False,
            "upcoming",
            f"data api liveBroadcastContent=upcoming (actualStart={actual_start or '-'})",
        )
    if actual_start and not actual_end:
        return DataApiCheckResult(
            True,
            False,
            "inconsistent_live_details",
            f"data api liveBroadcastContent={lbc or 'empty'} but actualStart exists ({actual_start})",
        )
    return DataApiCheckResult(
        True,
        False,
        lbc or "none",
        f"data api liveBroadcastContent={lbc or 'empty'} (no live start)",
    )


def resolve_live_video_id(
    channel_id: str,
    api_key: str,
    timeout_sec: int | None = None,
    *,
    fetch_text: Callable[..., str],
    append_api_call_event: Callable[..., None],
    mark_quota_exhausted: Callable[..., tuple[bool, str]],
) -> tuple[str, str]:
    if not channel_id or not api_key:
        return "", "live search skipped"
    query = urllib.parse.urlencode(
        {
            "part": "id,snippet",
            "channelId": channel_id,
            "eventType": "live",
            "type": "video",
            "maxResults": 5,
            "key": api_key,
            "order": "date",
        }
    )
    url = f"https://www.googleapis.com/youtube/v3/search?{query}"
    try:
        payload = json.loads(fetch_text(url, timeout_sec=timeout_sec))
        append_api_call_event(method="search.list", status="ok", source="resolve_live_video_id")
    except urllib.error.HTTPError as exc:
        body = _http_error_body(exc)
        detail = f"live search http {exc.code}: {body[:240]}"
        quota_exceeded = _is_quota_exceeded_error(exc.code, body)
        append_api_call_event(
            method="search.list",
            status="http_error",
            detail=detail,
            http_code=int(getattr(exc, "code", 0) or 0),
            quota_exceeded=quota_exceeded,
            source="resolve_live_video_id",
        )
        if quota_exceeded:
            _latched, quota_note = mark_quota_exhausted(
                "data_api_search",
                detail,
                reason_hint=_extract_google_error_reason(body),
            )
            detail = f"{detail}; {quota_note}"
        return "", detail
    except Exception as exc:
        append_api_call_event(method="search.list", status="error", detail=str(exc), source="resolve_live_video_id")
        return "", f"live search failed: {exc}"
    items = payload.get("items", [])
    if not items:
        return "", "live search found no live items"
    for item in items:
        vid = ((item.get("id") or {}).get("videoId") or "").strip()
        if vid:
            return vid, "live search resolved video id"
    return "", "live search had items but no videoId"
