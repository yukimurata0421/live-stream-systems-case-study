from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable


def _url(endpoint: str, params: dict[str, str]) -> str:
    query = urllib.parse.urlencode(params)
    return f"https://www.googleapis.com/youtube/v3/{endpoint}?{query}"


def _method_name(endpoint: str, suffix: str, method_map: dict[str, str]) -> str:
    return method_map.get(endpoint, f"{endpoint}.{suffix}")


def _quota_detail(
    *,
    source: str,
    detail: str,
    quota_exceeded: bool,
    body: str,
    mark_quota_exhausted: Callable[..., tuple[bool, str]],
    extract_google_error_reason: Callable[[str], str],
) -> str:
    if not quota_exceeded:
        return detail
    _latched, quota_note = mark_quota_exhausted(
        source,
        detail,
        reason_hint=extract_google_error_reason(body),
    )
    return f"{detail}; {quota_note}"


def _record_http_error(
    *,
    e: urllib.error.HTTPError,
    method: str,
    detail_prefix: str,
    source: str,
    quota_source: str,
    append_api_call_event: Callable[..., None],
    mark_quota_exhausted: Callable[..., tuple[bool, str]],
    http_error_body: Callable[[urllib.error.HTTPError], str],
    is_quota_exceeded_error: Callable[[int, str], bool],
    extract_google_error_reason: Callable[[str], str],
) -> None:
    body = http_error_body(e)
    quota_exceeded = is_quota_exceeded_error(e.code, body)
    detail = _quota_detail(
        source=quota_source,
        detail=f"{detail_prefix} http {e.code}: {body[:240]}",
        quota_exceeded=quota_exceeded,
        body=body,
        mark_quota_exhausted=mark_quota_exhausted,
        extract_google_error_reason=extract_google_error_reason,
    )
    append_api_call_event(
        method=method,
        status="http_error",
        detail=detail,
        http_code=int(getattr(e, "code", 0) or 0),
        quota_exceeded=quota_exceeded,
        source=source,
    )


def api_get(
    endpoint: str,
    access_token: str,
    params: dict[str, str],
    *,
    oauth_timeout_sec: int,
    fetch_oauth_json: Callable[..., dict],
    append_api_call_event: Callable[..., None],
    mark_quota_exhausted: Callable[..., tuple[bool, str]],
    http_error_body: Callable[[urllib.error.HTTPError], str],
    is_quota_exceeded_error: Callable[[int, str], bool],
    extract_google_error_reason: Callable[[str], str],
) -> dict:
    method = _method_name(
        endpoint,
        "list",
        {
            "liveBroadcasts": "liveBroadcasts.list",
            "liveStreams": "liveStreams.list",
        },
    )
    try:
        payload = fetch_oauth_json(
            _url(endpoint, params),
            headers={"Authorization": f"Bearer {access_token}"},
            timeout_sec=oauth_timeout_sec,
        )
        append_api_call_event(method=method, status="ok", source="youtube_live_api_get")
        return payload
    except urllib.error.HTTPError as e:
        _record_http_error(
            e=e,
            method=method,
            detail_prefix=f"oauth {method}",
            source="youtube_live_api_get",
            quota_source=f"oauth_{method}",
            append_api_call_event=append_api_call_event,
            mark_quota_exhausted=mark_quota_exhausted,
            http_error_body=http_error_body,
            is_quota_exceeded_error=is_quota_exceeded_error,
            extract_google_error_reason=extract_google_error_reason,
        )
        raise
    except Exception as e:
        append_api_call_event(method=method, status="error", detail=str(e), source="youtube_live_api_get")
        raise


def api_post(
    endpoint: str,
    access_token: str,
    params: dict[str, str],
    *,
    oauth_timeout_sec: int,
    append_api_call_event: Callable[..., None],
    mark_quota_exhausted: Callable[..., tuple[bool, str]],
    http_error_body: Callable[[urllib.error.HTTPError], str],
    is_quota_exceeded_error: Callable[[int, str], bool],
    extract_google_error_reason: Callable[[str], str],
) -> dict:
    method = _method_name(
        endpoint,
        "post",
        {
            "liveBroadcasts/transition": "liveBroadcasts.transition",
            "liveBroadcasts/bind": "liveBroadcasts.bind",
        },
    )
    req = urllib.request.Request(
        _url(endpoint, params),
        data=b"",
        headers={
            "User-Agent": "stream-youtube-watchdog/1.0",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=oauth_timeout_sec) as r:
            raw = r.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        _record_http_error(
            e=e,
            method=method,
            detail_prefix=f"oauth {method}",
            source="youtube_live_api_post",
            quota_source=f"oauth_{method}",
            append_api_call_event=append_api_call_event,
            mark_quota_exhausted=mark_quota_exhausted,
            http_error_body=http_error_body,
            is_quota_exceeded_error=is_quota_exceeded_error,
            extract_google_error_reason=extract_google_error_reason,
        )
        raise
    except Exception as e:
        append_api_call_event(method=method, status="error", detail=str(e), source="youtube_live_api_post")
        raise
    append_api_call_event(method=method, status="ok", source="youtube_live_api_post")
    return json.loads(raw) if raw else {}


def api_post_json(
    endpoint: str,
    access_token: str,
    params: dict[str, str],
    body: dict,
    *,
    oauth_timeout_sec: int,
    append_api_call_event: Callable[..., None],
    mark_quota_exhausted: Callable[..., tuple[bool, str]],
    http_error_body: Callable[[urllib.error.HTTPError], str],
    is_quota_exceeded_error: Callable[[int, str], bool],
    extract_google_error_reason: Callable[[str], str],
) -> dict:
    method = _method_name(endpoint, "post_json", {"liveBroadcasts": "liveBroadcasts.insert"})
    req = urllib.request.Request(
        _url(endpoint, params),
        data=json.dumps(body).encode("utf-8"),
        headers={
            "User-Agent": "stream-youtube-watchdog/1.0",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=oauth_timeout_sec) as r:
            raw = r.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        _record_http_error(
            e=e,
            method=method,
            detail_prefix=f"oauth {method}",
            source="youtube_live_api_post_json",
            quota_source=f"oauth_{method}",
            append_api_call_event=append_api_call_event,
            mark_quota_exhausted=mark_quota_exhausted,
            http_error_body=http_error_body,
            is_quota_exceeded_error=is_quota_exceeded_error,
            extract_google_error_reason=extract_google_error_reason,
        )
        raise
    except Exception as e:
        append_api_call_event(method=method, status="error", detail=str(e), source="youtube_live_api_post_json")
        raise
    append_api_call_event(method=method, status="ok", source="youtube_live_api_post_json")
    return json.loads(raw) if raw else {}


def videos_api_update(
    access_token: str,
    params: dict[str, str],
    body: dict,
    *,
    oauth_timeout_sec: int,
    append_api_call_event: Callable[..., None],
    mark_quota_exhausted: Callable[..., tuple[bool, str]],
    http_error_body: Callable[[urllib.error.HTTPError], str],
    is_quota_exceeded_error: Callable[[int, str], bool],
    extract_google_error_reason: Callable[[str], str],
) -> dict:
    req = urllib.request.Request(
        _url("videos", params),
        data=json.dumps(body).encode("utf-8"),
        headers={
            "User-Agent": "stream-youtube-watchdog/1.0",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        },
        method="PUT",
    )
    try:
        with urllib.request.urlopen(req, timeout=oauth_timeout_sec) as r:
            raw = r.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        _record_http_error(
            e=e,
            method="videos.update",
            detail_prefix="oauth videos.update",
            source="youtube_videos_api_update",
            quota_source="oauth_videos.update",
            append_api_call_event=append_api_call_event,
            mark_quota_exhausted=mark_quota_exhausted,
            http_error_body=http_error_body,
            is_quota_exceeded_error=is_quota_exceeded_error,
            extract_google_error_reason=extract_google_error_reason,
        )
        raise
    except Exception as e:
        append_api_call_event(method="videos.update", status="error", detail=str(e), source="youtube_videos_api_update")
        raise
    append_api_call_event(method="videos.update", status="ok", source="youtube_videos_api_update")
    return json.loads(raw) if raw else {}


def api_delete(
    endpoint: str,
    access_token: str,
    params: dict[str, str],
    *,
    oauth_timeout_sec: int,
    append_api_call_event: Callable[..., None],
    mark_quota_exhausted: Callable[..., tuple[bool, str]],
    http_error_body: Callable[[urllib.error.HTTPError], str],
    is_quota_exceeded_error: Callable[[int, str], bool],
    extract_google_error_reason: Callable[[str], str],
) -> None:
    method = _method_name(endpoint, "delete", {"liveBroadcasts": "liveBroadcasts.delete"})
    req = urllib.request.Request(
        _url(endpoint, params),
        headers={
            "User-Agent": "stream-youtube-watchdog/1.0",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        },
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(req, timeout=oauth_timeout_sec) as r:
            r.read()
    except urllib.error.HTTPError as e:
        _record_http_error(
            e=e,
            method=method,
            detail_prefix=f"oauth {method}",
            source="youtube_live_api_delete",
            quota_source=f"oauth_{method}",
            append_api_call_event=append_api_call_event,
            mark_quota_exhausted=mark_quota_exhausted,
            http_error_body=http_error_body,
            is_quota_exceeded_error=is_quota_exceeded_error,
            extract_google_error_reason=extract_google_error_reason,
        )
        raise
    except Exception as e:
        append_api_call_event(method=method, status="error", detail=str(e), source="youtube_live_api_delete")
        raise
    append_api_call_event(method=method, status="ok", source="youtube_live_api_delete")
