from __future__ import annotations

import time
import urllib.error
from pathlib import Path

try:
    from .youtube_watchdog_config import (
        API_KEY,
        API_CALL_LOG_FILE,
        CHANNEL_ID,
        CHANNEL_LIVE_URL,
        FORCE_LIVE_BACKOFF_BASE_SEC,
        FORCE_LIVE_BACKOFF_MAX_EXP,
        FORCE_LIVE_BACKOFF_MAX_SEC,
        FORCE_LIVE_ALLOW_REPLACEMENT_BROADCAST,
        FORCE_LIVE_AUTO_RECOVERY,
        FORCE_LIVE_BROADCAST_ID,
        FORCE_LIVE_CATEGORY_ID,
        FORCE_LIVE_MAX_ATTEMPTS_PER_DAY,
        FORCE_LIVE_MIN_FAILS,
        FORCE_LIVE_MIN_STREAM_UPTIME_SEC,
        FORCE_LIVE_ON_UPCOMING_ONCE,
        FORCE_LIVE_REQUIRE_INGEST,
        FORCE_LIVE_REQUIRE_OAUTH_STREAM_ACTIVE,
        FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_START,
        FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_STOP,
        FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC,
        FORCE_LIVE_SUCCESS_COOLDOWN_SEC,
        FORCE_LIVE_TARGET_STATUS,
        INGEST_TCP_PORT,
        INGEST_TCP_PORTS_RAW,
        LIVE_URL,
        MAX_FAILS,
        OAUTH_CLIENT_ID,
        OAUTH_CLIENT_SECRET,
        OAUTH_ENABLE,
        OAUTH_MIN_TOKEN_TTL_SEC,
        OAUTH_REFRESH_TOKEN,
        OAUTH_REQUIRE_CHANNEL_MATCH,
        OAUTH_SHADOW_MODE,
        OAUTH_STREAM_STATUS_REQUIRED,
        OAUTH_TIMEOUT_SEC,
        OAUTH_TOKEN_URL,
        PUBLIC_LIVE_PROBE_TIMEOUT_SEC,
        QUOTA_EXHAUSTED_COOLDOWN_SEC,
        QUOTA_RESET_MARGIN_SEC,
        TIMEOUT_SEC,
        VIDEO_ID,
        OAuthProbeResult,
        DataApiCheckResult,
    )
    from .youtube_watchdog_state import (
        append_event,
        load_force_live_state,
        load_oauth_token_state,
        quota_exhausted_active,
        save_force_live_state,
        save_oauth_token_state,
        update_quota_state,
        utc_now,
    )
    from .youtube_oauth.config import OAuthConfig
    from .youtube_oauth.token import get_oauth_access_token as refresh_oauth_access_token
    from .youtube_oauth.token import oauth_is_configured as oauth_configured
    from .youtube_api_lib import broadcasts as broadcast_payloads
    from .youtube_api_lib import data_api as data_api_runtime
    from .youtube_api_lib import http as http_runtime
    from .youtube_api_lib import live_api as live_api_runtime
    from .youtube_api_lib import public_probe as public_probe_runtime
    from .youtube_api_lib import quota as quota_runtime
    from .youtube_api_lib.quota import (
        _extract_google_error_reason,
        _has_google_error_reason,
        _http_error_body,
        _is_quota_exceeded_error,
        _is_rate_limited_error,
        next_quota_reset_ts_pacific,
        quota_guard_status,
        quota_guard_until_ts_pacific,
    )
except ImportError:
    from youtube_watchdog_config import (
        API_KEY,
        API_CALL_LOG_FILE,
        CHANNEL_ID,
        CHANNEL_LIVE_URL,
        FORCE_LIVE_BACKOFF_BASE_SEC,
        FORCE_LIVE_BACKOFF_MAX_EXP,
        FORCE_LIVE_BACKOFF_MAX_SEC,
        FORCE_LIVE_ALLOW_REPLACEMENT_BROADCAST,
        FORCE_LIVE_AUTO_RECOVERY,
        FORCE_LIVE_BROADCAST_ID,
        FORCE_LIVE_CATEGORY_ID,
        FORCE_LIVE_MAX_ATTEMPTS_PER_DAY,
        FORCE_LIVE_MIN_FAILS,
        FORCE_LIVE_MIN_STREAM_UPTIME_SEC,
        FORCE_LIVE_ON_UPCOMING_ONCE,
        FORCE_LIVE_REQUIRE_INGEST,
        FORCE_LIVE_REQUIRE_OAUTH_STREAM_ACTIVE,
        FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_START,
        FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_STOP,
        FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC,
        FORCE_LIVE_SUCCESS_COOLDOWN_SEC,
        FORCE_LIVE_TARGET_STATUS,
        INGEST_TCP_PORT,
        INGEST_TCP_PORTS_RAW,
        LIVE_URL,
        MAX_FAILS,
        OAUTH_CLIENT_ID,
        OAUTH_CLIENT_SECRET,
        OAUTH_ENABLE,
        OAUTH_MIN_TOKEN_TTL_SEC,
        OAUTH_REFRESH_TOKEN,
        OAUTH_REQUIRE_CHANNEL_MATCH,
        OAUTH_SHADOW_MODE,
        OAUTH_STREAM_STATUS_REQUIRED,
        OAUTH_TIMEOUT_SEC,
        OAUTH_TOKEN_URL,
        PUBLIC_LIVE_PROBE_TIMEOUT_SEC,
        QUOTA_EXHAUSTED_COOLDOWN_SEC,
        QUOTA_RESET_MARGIN_SEC,
        TIMEOUT_SEC,
        VIDEO_ID,
        OAuthProbeResult,
        DataApiCheckResult,
    )
    from youtube_watchdog_state import (
        append_event,
        load_force_live_state,
        load_oauth_token_state,
        quota_exhausted_active,
        save_force_live_state,
        save_oauth_token_state,
        update_quota_state,
        utc_now,
    )
    from youtube_oauth.config import OAuthConfig
    from youtube_oauth.token import get_oauth_access_token as refresh_oauth_access_token
    from youtube_oauth.token import oauth_is_configured as oauth_configured
    from youtube_api_lib import broadcasts as broadcast_payloads
    from youtube_api_lib import data_api as data_api_runtime
    from youtube_api_lib import http as http_runtime
    from youtube_api_lib import live_api as live_api_runtime
    from youtube_api_lib import public_probe as public_probe_runtime
    from youtube_api_lib import quota as quota_runtime
    from youtube_api_lib.quota import (
        _extract_google_error_reason,
        _has_google_error_reason,
        _http_error_body,
        _is_quota_exceeded_error,
        _is_rate_limited_error,
        next_quota_reset_ts_pacific,
        quota_guard_status,
        quota_guard_until_ts_pacific,
    )


_iso_utc_from_unix = quota_runtime._iso_utc_from_unix
PublicLiveProbeResult = public_probe_runtime.PublicLiveProbeResult
WatchPageProbeResult = public_probe_runtime.WatchPageProbeResult
_parse_probe_bool = public_probe_runtime._parse_probe_bool
parse_public_live_probe_output = public_probe_runtime.parse_public_live_probe_output


def _with_facade_quota_bindings(fn, *args, **kwargs):
    bindings = {
        "API_CALL_LOG_FILE": API_CALL_LOG_FILE,
        "QUOTA_EXHAUSTED_COOLDOWN_SEC": QUOTA_EXHAUSTED_COOLDOWN_SEC,
        "QUOTA_RESET_MARGIN_SEC": QUOTA_RESET_MARGIN_SEC,
        "append_event": append_event,
        "update_quota_state": update_quota_state,
        "utc_now": utc_now,
    }
    previous = {name: getattr(quota_runtime, name) for name in bindings}
    try:
        for name, value in bindings.items():
            setattr(quota_runtime, name, value)
        return fn(*args, **kwargs)
    finally:
        for name, value in previous.items():
            setattr(quota_runtime, name, value)


def _append_api_call_event(
    *,
    method: str,
    status: str,
    detail: str = "",
    http_code: int = 0,
    quota_exceeded: bool = False,
    source: str = "",
) -> None:
    return _with_facade_quota_bindings(
        quota_runtime._append_api_call_event,
        method=method,
        status=status,
        detail=detail,
        http_code=http_code,
        quota_exceeded=quota_exceeded,
        source=source,
    )


def mark_quota_exhausted(
    source: str,
    detail: str,
    now_ts: int | None = None,
    reason_hint: str = "",
) -> tuple[bool, str]:
    return _with_facade_quota_bindings(
        quota_runtime.mark_quota_exhausted,
        source,
        detail,
        now_ts=now_ts,
        reason_hint=reason_hint,
    )


def extract_video_id(url: str) -> str:
    return public_probe_runtime.extract_video_id(url)


def fetch(url: str, timeout_sec: int | None = None) -> str:
    return http_runtime.fetch(url, timeout_sec=timeout_sec, default_timeout_sec=TIMEOUT_SEC)


def fetch_oauth_json(url: str, headers: dict[str, str] | None = None, timeout_sec: int | None = None) -> dict:
    return http_runtime.fetch_oauth_json(
        url,
        headers=headers,
        timeout_sec=timeout_sec,
        default_timeout_sec=OAUTH_TIMEOUT_SEC,
    )


def post_form_json(url: str, form: dict[str, str], timeout_sec: int | None = None) -> dict:
    return http_runtime.post_form_json(
        url,
        form,
        timeout_sec=timeout_sec,
        default_timeout_sec=OAUTH_TIMEOUT_SEC,
    )


def oauth_config() -> OAuthConfig:
    return OAuthConfig(
        enabled=bool(OAUTH_ENABLE),
        client_id=OAUTH_CLIENT_ID,
        client_secret=OAUTH_CLIENT_SECRET,
        refresh_token=OAUTH_REFRESH_TOKEN,
        token_url=OAUTH_TOKEN_URL,
        timeout_sec=OAUTH_TIMEOUT_SEC,
        min_token_ttl_sec=OAUTH_MIN_TOKEN_TTL_SEC,
    )


def oauth_is_configured() -> bool:
    return oauth_configured(oauth_config())


def get_oauth_access_token() -> tuple[str, int, str]:
    return refresh_oauth_access_token(
        config=oauth_config(),
        now_ts=int(time.time()),
        load_state=load_oauth_token_state,
        save_state=save_oauth_token_state,
        post_form_json=post_form_json,
        utc_now=utc_now,
    )


def youtube_live_api_get(endpoint: str, access_token: str, params: dict[str, str]) -> dict:
    return live_api_runtime.api_get(
        endpoint,
        access_token,
        params,
        oauth_timeout_sec=OAUTH_TIMEOUT_SEC,
        fetch_oauth_json=fetch_oauth_json,
        append_api_call_event=_append_api_call_event,
        mark_quota_exhausted=mark_quota_exhausted,
        http_error_body=_http_error_body,
        is_quota_exceeded_error=_is_quota_exceeded_error,
        extract_google_error_reason=_extract_google_error_reason,
    )


def youtube_live_api_post(endpoint: str, access_token: str, params: dict[str, str]) -> dict:
    return live_api_runtime.api_post(
        endpoint,
        access_token,
        params,
        oauth_timeout_sec=OAUTH_TIMEOUT_SEC,
        append_api_call_event=_append_api_call_event,
        mark_quota_exhausted=mark_quota_exhausted,
        http_error_body=_http_error_body,
        is_quota_exceeded_error=_is_quota_exceeded_error,
        extract_google_error_reason=_extract_google_error_reason,
    )


def youtube_live_api_post_json(
    endpoint: str,
    access_token: str,
    params: dict[str, str],
    body: dict,
) -> dict:
    return live_api_runtime.api_post_json(
        endpoint,
        access_token,
        params,
        body,
        oauth_timeout_sec=OAUTH_TIMEOUT_SEC,
        append_api_call_event=_append_api_call_event,
        mark_quota_exhausted=mark_quota_exhausted,
        http_error_body=_http_error_body,
        is_quota_exceeded_error=_is_quota_exceeded_error,
        extract_google_error_reason=_extract_google_error_reason,
    )


def youtube_videos_api_update(access_token: str, params: dict[str, str], body: dict) -> dict:
    return live_api_runtime.videos_api_update(
        access_token,
        params,
        body,
        oauth_timeout_sec=OAUTH_TIMEOUT_SEC,
        append_api_call_event=_append_api_call_event,
        mark_quota_exhausted=mark_quota_exhausted,
        http_error_body=_http_error_body,
        is_quota_exceeded_error=_is_quota_exceeded_error,
        extract_google_error_reason=_extract_google_error_reason,
    )


def youtube_live_api_delete(endpoint: str, access_token: str, params: dict[str, str]) -> None:
    return live_api_runtime.api_delete(
        endpoint,
        access_token,
        params,
        oauth_timeout_sec=OAUTH_TIMEOUT_SEC,
        append_api_call_event=_append_api_call_event,
        mark_quota_exhausted=mark_quota_exhausted,
        http_error_body=_http_error_body,
        is_quota_exceeded_error=_is_quota_exceeded_error,
        extract_google_error_reason=_extract_google_error_reason,
    )


def list_owned_broadcasts(access_token: str, status: str = "all", max_results: int = 20) -> list[dict]:
    payload = youtube_live_api_get(
        "liveBroadcasts",
        access_token,
        {
            "part": "id,snippet,contentDetails,status",
            "mine": "true",
            "broadcastType": "all",
            "maxResults": str(max(1, min(max_results, 50))),
        },
    )
    items = payload.get("items", [])
    if not isinstance(items, list):
        return []
    return broadcast_payloads.filter_broadcasts_by_status(items, status)


def parse_yt_ts(value: str) -> int:
    return broadcast_payloads.parse_yt_ts(value)


def lifecycle_priority(lifecycle: str) -> int:
    return broadcast_payloads.lifecycle_priority(lifecycle)


def select_primary_broadcast(
    broadcasts: list[dict],
    preferred_video_id: str = "",
    preferred_broadcast_id: str = "",
) -> dict | None:
    return broadcast_payloads.select_primary_broadcast(
        broadcasts,
        preferred_video_id=preferred_video_id,
        preferred_broadcast_id=preferred_broadcast_id,
    )


def choose_transition_target_broadcast(
    broadcasts: list[dict],
    preferred_video_id: str,
    preferred_broadcast_id: str,
) -> tuple[str, str, str, str]:
    return broadcast_payloads.choose_transition_target_broadcast(
        broadcasts,
        preferred_video_id=preferred_video_id,
        preferred_broadcast_id=preferred_broadcast_id,
    )


def in_upcoming_like_state(api_reason: str, oauth: OAuthProbeResult) -> bool:
    if "liveBroadcastContent=upcoming" in api_reason:
        return True
    return oauth.life_cycle_status in {"ready", "created", "testStarting", "testing"}


def compute_force_live_backoff_sec(consecutive_failures: int) -> int:
    exp = min(max(0, consecutive_failures - 1), FORCE_LIVE_BACKOFF_MAX_EXP)
    base = FORCE_LIVE_BACKOFF_BASE_SEC * (2**exp)
    return min(base, FORCE_LIVE_BACKOFF_MAX_SEC)


def force_live_transition_statuses(current_lifecycle: str, target_status: str) -> list[str]:
    lifecycle = (current_lifecycle or "").strip()
    target = (target_status or "live").strip().lower()
    if target == "live" and lifecycle in {"ready", "created"}:
        return ["testing", "live"]
    return [target]


def find_broadcast_lifecycle(access_token: str, broadcast_id: str) -> str:
    item = find_owned_broadcast(access_token, broadcast_id)
    return str(((item or {}).get("status") or {}).get("lifeCycleStatus") or "").strip()


def find_owned_broadcast(access_token: str, broadcast_id: str) -> dict | None:
    if not broadcast_id:
        return None
    try:
        broadcasts = list_owned_broadcasts(access_token, status="all", max_results=20)
    except Exception:
        return None
    for item in broadcasts:
        if str(item.get("id", "")).strip() == broadcast_id:
            return item
    return None


def broadcast_channel_id(item: dict | None) -> str:
    return str((((item or {}).get("snippet") or {}).get("channelId") or "")).strip()


def validate_oauth_channel_match(oauth: OAuthProbeResult, target_item: dict | None = None) -> tuple[bool, str]:
    if not OAUTH_REQUIRE_CHANNEL_MATCH:
        return True, "oauth channel match disabled"
    expected = CHANNEL_ID.strip()
    if not expected:
        return True, "oauth channel match skipped: YTW_CHANNEL_ID not configured"
    observed = (oauth.channel_id or broadcast_channel_id(target_item)).strip()
    if not observed:
        return False, f"oauth channel validation failed: expected {expected}, observed missing"
    if observed != expected:
        return False, f"oauth channel validation failed: expected {expected}, observed {observed}"
    return True, f"oauth channel validated: {observed}"


def build_safe_video_snippet_for_category(existing_snippet: dict, category_id: str) -> dict:
    return broadcast_payloads.build_safe_video_snippet_for_category(existing_snippet, category_id)


def update_video_category_from_existing_snippet(access_token: str, video_id: str, category_id: str) -> str:
    vid = (video_id or "").strip()
    cat = (category_id or "").strip()
    if not vid:
        return "category update skipped: missing video id"
    if not cat:
        return "category update skipped: category id not configured"

    payload = youtube_live_api_get(
        "videos",
        access_token,
        {"part": "snippet", "id": vid},
    )
    items = payload.get("items", [])
    if not isinstance(items, list) or not items:
        return f"category update skipped: videos.list returned no item for {vid}"
    item = items[0] if isinstance(items[0], dict) else {}
    snippet = item.get("snippet", {}) or {}
    current = str(snippet.get("categoryId", "")).strip()
    if current == cat:
        return f"category already {cat}"

    update_body = {
        "id": vid,
        "snippet": build_safe_video_snippet_for_category(snippet, cat),
    }
    youtube_videos_api_update(access_token, {"part": "snippet"}, update_body)
    return f"category updated {current or '-'}->{cat}"


def _recovery_broadcast_body(source_broadcast: dict, *, enable_auto_start: bool) -> dict:
    return broadcast_payloads.recovery_broadcast_body(
        source_broadcast,
        enable_auto_start=enable_auto_start,
        enable_auto_stop=FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_STOP,
    )


def create_recovery_broadcast(access_token: str, source_broadcast: dict, stream_id: str) -> tuple[str, str]:
    requested_auto_start = bool(FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_START)
    body = _recovery_broadcast_body(source_broadcast, enable_auto_start=requested_auto_start)
    auto_start_fallback_reason = ""
    try:
        created = youtube_live_api_post_json(
            "liveBroadcasts",
            access_token,
            {"part": "id,snippet,contentDetails,status"},
            body,
        )
        used_auto_start = requested_auto_start
    except urllib.error.HTTPError as e:
        body_text = _http_error_body(e)
        if not (requested_auto_start and _has_google_error_reason(body_text, "invalidAutoStart")):
            raise
        auto_start_fallback_reason = "enableAutoStart=true rejected with invalidAutoStart; retried enableAutoStart=false"
        created = youtube_live_api_post_json(
            "liveBroadcasts",
            access_token,
            {"part": "id,snippet,contentDetails,status"},
            _recovery_broadcast_body(source_broadcast, enable_auto_start=False),
        )
        used_auto_start = False
    broadcast_id = str(created.get("id", "")).strip()
    if not broadcast_id:
        return "", "created broadcast response had no id"
    youtube_live_api_post(
        "liveBroadcasts/bind",
        access_token,
        {
            "id": broadcast_id,
            "part": "id,snippet,contentDetails,status",
            "streamId": stream_id,
        },
    )
    category_reason = ""
    if FORCE_LIVE_CATEGORY_ID:
        try:
            category_reason = update_video_category_from_existing_snippet(
                access_token,
                broadcast_id,
                FORCE_LIVE_CATEGORY_ID,
            )
        except urllib.error.HTTPError as e:
            body_text = _http_error_body(e)
            category_reason = f"category update failed http {e.code}: {body_text[:160]}"
        except Exception as e:
            category_reason = f"category update failed: {e}"
    reason = (
        "created replacement broadcast "
        f"with enableAutoStart={'true' if used_auto_start else 'false'} "
        f"enableAutoStop={'true' if FORCE_LIVE_REPLACEMENT_ENABLE_AUTO_STOP else 'false'} "
        "and bound active stream"
    )
    if auto_start_fallback_reason:
        reason = f"{reason}; {auto_start_fallback_reason}"
    if category_reason:
        reason = f"{reason}; {category_reason}"
    return broadcast_id, reason


def cleanup_replaced_broadcast(access_token: str, source_broadcast: dict, replacement_broadcast_id: str) -> str:
    source_id = str(source_broadcast.get("id", "") or "").strip()
    if not source_id or source_id == replacement_broadcast_id:
        return ""
    lifecycle = str(((source_broadcast.get("status") or {}).get("lifeCycleStatus") or "")).strip()
    if lifecycle not in {"created", "ready", "testStarting", "testing"}:
        return f"cleanup skipped source={source_id} lifecycle={lifecycle or '-'}"
    youtube_live_api_delete("liveBroadcasts", access_token, {"id": source_id})
    return f"deleted stale source broadcast {source_id}"


def wait_for_broadcast_lifecycle(
    access_token: str,
    broadcast_id: str,
    allowed_lifecycles: set[str],
    *,
    attempts: int = 6,
    sleep_sec: float = 5.0,
) -> str:
    last_lifecycle = ""
    for attempt in range(max(1, attempts)):
        lifecycle = find_broadcast_lifecycle(access_token, broadcast_id)
        if lifecycle:
            last_lifecycle = lifecycle
        if lifecycle in allowed_lifecycles:
            return lifecycle
        if attempt < attempts - 1:
            time.sleep(max(0.0, sleep_sec))
    return last_lifecycle


def sanitize_force_live_state(state: dict, now_ts: int) -> dict:
    if not isinstance(state, dict):
        state = {}
    window_start = int(state.get("attempt_window_start_ts", 0) or 0)
    if window_start <= 0 or now_ts - window_start >= 86400:
        state["attempt_window_start_ts"] = now_ts
        state["attempt_count_window"] = 0
    state.setdefault("attempted", False)
    state.setdefault("consecutive_failures", 0)
    state.setdefault("next_allowed_ts", 0)
    return state


def save_force_live_failure(
    once_state: dict,
    *,
    now_ts: int,
    detail: str,
    target_broadcast_id: str,
    target_video_id: str,
    target_lifecycle: str,
    target_reason: str,
    token_reason: str,
) -> None:
    failures = int(once_state.get("consecutive_failures", 0) or 0) + 1
    backoff_sec = compute_force_live_backoff_sec(failures)
    once_state["attempted"] = True
    once_state["attempted_at_utc"] = utc_now()
    once_state["attempt_count_window"] = int(once_state.get("attempt_count_window", 0) or 0) + 1
    once_state["consecutive_failures"] = failures
    once_state["next_allowed_ts"] = now_ts + backoff_sec
    once_state["target_status"] = FORCE_LIVE_TARGET_STATUS
    once_state["target_broadcast_id"] = target_broadcast_id
    once_state["target_video_id"] = target_video_id
    once_state["target_lifecycle_before"] = target_lifecycle
    once_state["target_reason"] = target_reason
    once_state["token_reason"] = token_reason
    once_state["ok"] = False
    once_state["error"] = detail
    save_force_live_state(once_state)


def force_transition_live_once(
    feature_enabled: bool,
    fail_count: int,
    video_id: str,
    api_reason: str,
    stream_active: bool,
    ingest_connected: bool,
    oauth: OAuthProbeResult,
    ffmpeg_uptime_sec: int,
    force_live_once_cli: bool = False,
    url_recovery_elapsed_sec: int = 0,
    replacement_min_elapsed_sec: int | None = None,
) -> tuple[bool, str]:
    now_ts = int(time.time())
    if not feature_enabled:
        return False, "feature disabled"
    if fail_count < FORCE_LIVE_MIN_FAILS:
        return False, f"below min fails ({fail_count}<{FORCE_LIVE_MIN_FAILS})"
    if not in_upcoming_like_state(api_reason, oauth):
        return False, f"not upcoming-like state ({api_reason}; oauth_lifecycle={oauth.life_cycle_status or '-'})"
    if not stream_active:
        return False, "stream service inactive"
    if FORCE_LIVE_REQUIRE_INGEST and not ingest_connected:
        return False, "ingest tcp not connected"
    if FORCE_LIVE_MIN_STREAM_UPTIME_SEC > 0 and ffmpeg_uptime_sec < FORCE_LIVE_MIN_STREAM_UPTIME_SEC:
        return False, (
            f"ffmpeg uptime too short ({ffmpeg_uptime_sec}s<{FORCE_LIVE_MIN_STREAM_UPTIME_SEC}s); "
            "wait for ingest stabilization"
        )
    if FORCE_LIVE_REQUIRE_OAUTH_STREAM_ACTIVE:
        if not oauth.probe_ok:
            return False, "oauth probe unavailable while streamStatus=active is required"
        if oauth.stream_status != "active":
            return False, f"oauth streamStatus is not active ({oauth.stream_status or '-'})"
    if not OAUTH_ENABLE:
        return False, "oauth disabled"
    if not oauth_is_configured():
        return False, "oauth not configured"

    once_state = sanitize_force_live_state(load_force_live_state(), now_ts)
    if (
        once_state.get("attempted")
        and FORCE_LIVE_ON_UPCOMING_ONCE
        and not FORCE_LIVE_AUTO_RECOVERY
        and not force_live_once_cli
    ):
        return False, "already attempted once"
    next_allowed_ts = int(once_state.get("next_allowed_ts", 0) or 0)
    if next_allowed_ts > now_ts and not force_live_once_cli:
        return False, f"backoff active ({next_allowed_ts - now_ts}s remaining)"
    attempts_in_window = int(once_state.get("attempt_count_window", 0) or 0)
    if attempts_in_window >= FORCE_LIVE_MAX_ATTEMPTS_PER_DAY:
        window_end = int(once_state.get("attempt_window_start_ts", now_ts) or now_ts) + 86400
        return False, (
            f"daily attempt cap reached ({attempts_in_window}/{FORCE_LIVE_MAX_ATTEMPTS_PER_DAY}; "
            f"{max(0, window_end - now_ts)}s remaining)"
        )

    access_token, _expires_at, token_reason = get_oauth_access_token()
    if not access_token:
        return False, token_reason

    target_broadcast_id = ""
    target_video_id = ""
    target_lifecycle = ""
    target_reason = ""
    target_stream_id = oauth.bound_stream_id
    replacement_source_item: dict | None = None
    replacement_operation = "force_current_broadcast_live"
    if oauth.probe_ok and oauth.broadcast_id and oauth.life_cycle_status in {"ready", "testStarting", "testing", "liveStarting"}:
        target_broadcast_id = oauth.broadcast_id
        target_video_id = oauth.video_id
        target_lifecycle = oauth.life_cycle_status
        target_reason = "selected from oauth probe"
    else:
        try:
            broadcasts = list_owned_broadcasts(access_token, status="all", max_results=20)
        except urllib.error.HTTPError as e:
            body = _http_error_body(e)
            return False, f"oauth liveBroadcasts list http {e.code}: {body[:240]}"
        except Exception as e:
            return False, f"oauth liveBroadcasts list failed: {e}"

        target_broadcast_id, target_video_id, target_lifecycle, target_reason = choose_transition_target_broadcast(
            broadcasts,
            preferred_video_id=video_id,
            preferred_broadcast_id=FORCE_LIVE_BROADCAST_ID,
        )
        if not target_broadcast_id:
            return False, target_reason
    target_item = find_owned_broadcast(access_token, target_broadcast_id)
    if target_item:
        target_stream_id = str(((target_item.get("contentDetails") or {}).get("boundStreamId") or target_stream_id)).strip()
    channel_ok, channel_reason = validate_oauth_channel_match(oauth, target_item)
    if not channel_ok:
        save_force_live_failure(
            once_state,
            now_ts=now_ts,
            detail=channel_reason,
            target_broadcast_id=target_broadcast_id,
            target_video_id=target_video_id,
            target_lifecycle=target_lifecycle,
            target_reason=target_reason,
            token_reason=token_reason,
        )
        return False, channel_reason

    target_content = (target_item or {}).get("contentDetails") or {}
    replacement_min = (
        FORCE_LIVE_REPLACEMENT_MIN_ELAPSED_SEC
        if replacement_min_elapsed_sec is None
        else max(0, int(replacement_min_elapsed_sec))
    )
    recovery_elapsed = max(0, int(url_recovery_elapsed_sec or 0))
    if (
        FORCE_LIVE_TARGET_STATUS == "live"
        and target_lifecycle in {"ready", "created"}
        and bool(target_content.get("enableAutoStart"))
        and target_stream_id
    ):
        if target_content.get("enableAutoStop") is False:
            target_reason = (
                f"{target_reason}; persistent scheduled broadcast enableAutoStop=false; "
                "using manual transition fallback instead of replacement"
            )
        else:
            if not FORCE_LIVE_ALLOW_REPLACEMENT_BROADCAST:
                detail = (
                    "replacement broadcast disabled; refusing to create scheduled recovery broadcast "
                    f"for autoStart source={target_broadcast_id}"
                )
                save_force_live_failure(
                    once_state,
                    now_ts=now_ts,
                    detail=detail,
                    target_broadcast_id=target_broadcast_id,
                    target_video_id=target_video_id,
                    target_lifecycle=target_lifecycle,
                    target_reason=target_reason,
                    token_reason=token_reason,
                )
                return False, detail
            if (not force_live_once_cli) and recovery_elapsed < replacement_min:
                detail = (
                    "replacement broadcast deferred: url preservation window active "
                    f"({recovery_elapsed}s<{replacement_min}s); keep trying current URL"
                )
                once_state["attempted"] = False
                once_state["attempted_at_utc"] = utc_now()
                once_state["target_status"] = FORCE_LIVE_TARGET_STATUS
                once_state["target_broadcast_id"] = target_broadcast_id
                once_state["target_video_id"] = target_video_id
                once_state["target_lifecycle_before"] = target_lifecycle
                once_state["target_reason"] = target_reason
                once_state["token_reason"] = token_reason
                once_state["ok"] = False
                once_state["error"] = detail
                once_state["operation"] = "create_replacement_broadcast"
                once_state["replacement_allowed"] = False
                once_state["url_recovery_elapsed_sec"] = recovery_elapsed
                once_state["replacement_min_elapsed_sec"] = replacement_min
                save_force_live_state(once_state)
                return False, detail
            try:
                replacement_id, replacement_reason = create_recovery_broadcast(
                    access_token,
                    target_item or {},
                    target_stream_id,
                )
            except urllib.error.HTTPError as e:
                body = _http_error_body(e)
                detail = f"oauth recovery broadcast create/bind http {e.code}: {body[:240]}"
                save_force_live_failure(
                    once_state,
                    now_ts=now_ts,
                    detail=detail,
                    target_broadcast_id=target_broadcast_id,
                    target_video_id=target_video_id,
                    target_lifecycle=target_lifecycle,
                    target_reason=target_reason,
                    token_reason=token_reason,
                )
                return False, detail
            except Exception as e:
                detail = f"oauth recovery broadcast create/bind failed: {e}"
                save_force_live_failure(
                    once_state,
                    now_ts=now_ts,
                    detail=detail,
                    target_broadcast_id=target_broadcast_id,
                    target_video_id=target_video_id,
                    target_lifecycle=target_lifecycle,
                    target_reason=target_reason,
                    token_reason=token_reason,
                )
                return False, detail
            if replacement_id:
                replacement_operation = "create_replacement_broadcast"
                replacement_source_item = target_item or {}
                target_broadcast_id = replacement_id
                target_video_id = replacement_id
                target_lifecycle = find_broadcast_lifecycle(access_token, replacement_id) or "ready"
                target_reason = f"{target_reason}; {replacement_reason}"

    transition_steps = force_live_transition_statuses(target_lifecycle, FORCE_LIVE_TARGET_STATUS)
    try:
        transitioned_lifecycle = target_lifecycle
        transition_log: list[str] = []
        for step in transition_steps:
            result = youtube_live_api_post(
                "liveBroadcasts/transition",
                access_token,
                {
                    "broadcastStatus": step,
                    "id": target_broadcast_id,
                    "part": "id,status,snippet",
                },
            )
            transitioned_lifecycle = str(((result.get("status") or {}).get("lifeCycleStatus") or "")).strip()
            transition_log.append(f"{step}->{transitioned_lifecycle or '-'}")
            if step == "testing":
                waited_lifecycle = wait_for_broadcast_lifecycle(
                    access_token,
                    target_broadcast_id,
                    {"testing", "liveStarting", "live"},
                )
                if waited_lifecycle:
                    transitioned_lifecycle = waited_lifecycle
                    transition_log[-1] = f"{step}->{waited_lifecycle}"
        once_state["attempted"] = True
        once_state["attempted_at_utc"] = utc_now()
        once_state["attempt_count_window"] = int(once_state.get("attempt_count_window", 0) or 0) + 1
        once_state["consecutive_failures"] = 0
        once_state["next_allowed_ts"] = now_ts + FORCE_LIVE_SUCCESS_COOLDOWN_SEC
        once_state["target_status"] = FORCE_LIVE_TARGET_STATUS
        once_state["transition_steps"] = transition_steps
        once_state["transition_log"] = transition_log
        once_state["target_broadcast_id"] = target_broadcast_id
        once_state["target_video_id"] = target_video_id
        once_state["target_lifecycle_before"] = target_lifecycle
        once_state["target_reason"] = target_reason
        once_state["token_reason"] = token_reason
        once_state["oauth_channel_validation"] = channel_reason
        once_state["result_life_cycle_status"] = transitioned_lifecycle
        once_state["operation"] = replacement_operation
        once_state["replacement_allowed"] = replacement_operation == "create_replacement_broadcast"
        once_state["url_recovery_elapsed_sec"] = recovery_elapsed
        once_state["replacement_min_elapsed_sec"] = replacement_min
        if replacement_source_item:
            try:
                cleanup_reason = cleanup_replaced_broadcast(
                    access_token,
                    replacement_source_item,
                    target_broadcast_id,
                )
                if cleanup_reason:
                    once_state["cleanup_reason"] = cleanup_reason
            except Exception as e:
                once_state["cleanup_error"] = f"cleanup stale source broadcast failed: {e}"
        once_state["ok"] = True
        once_state["error"] = ""
        save_force_live_state(once_state)
        return True, (
            f"transition requested: broadcast={target_broadcast_id} video_id={target_video_id or '-'} "
            f"before={target_lifecycle or '-'} after={transitioned_lifecycle or '-'} "
            f"operation={replacement_operation} elapsed={recovery_elapsed}s "
            f"replacement_min={replacement_min}s steps={','.join(transition_log) or '-'} reason={target_reason}"
        )
    except urllib.error.HTTPError as e:
        body = _http_error_body(e)
        detail = f"oauth transition http {e.code}: {body[:240]}"
        if _is_quota_exceeded_error(e.code, body):
            _latched, quota_note = mark_quota_exhausted(
                "oauth_liveBroadcasts.transition",
                detail,
                reason_hint=_extract_google_error_reason(body),
            )
            detail = f"{detail}; {quota_note}"
    except Exception as e:
        detail = f"oauth transition failed: {e}"

    save_force_live_failure(
        once_state,
        now_ts=now_ts,
        detail=detail,
        target_broadcast_id=target_broadcast_id,
        target_video_id=target_video_id,
        target_lifecycle=target_lifecycle,
        target_reason=target_reason,
        token_reason=token_reason,
    )
    return False, detail


def probe_with_oauth() -> OAuthProbeResult:
    mode = "shadow" if OAUTH_SHADOW_MODE else "enforced"
    if not OAUTH_ENABLE:
        return OAuthProbeResult(False, oauth_is_configured(), False, False, "oauth disabled", mode, remote_checked=False)
    if not oauth_is_configured():
        return OAuthProbeResult(True, False, False, False, "oauth not configured", mode, remote_checked=False)

    access_token, _expires_at, token_reason = get_oauth_access_token()
    if not access_token:
        return OAuthProbeResult(True, True, False, False, token_reason, mode, remote_checked=False)

    try:
        items = list_owned_broadcasts(access_token, status="all", max_results=20)
    except urllib.error.HTTPError as e:
        body = _http_error_body(e)
        detail = f"oauth liveBroadcasts http {e.code}: {body[:240]}"
        if _is_quota_exceeded_error(e.code, body):
            _latched, quota_note = mark_quota_exhausted(
                "oauth_liveBroadcasts",
                detail,
                reason_hint=_extract_google_error_reason(body),
            )
            detail = f"{detail}; {quota_note}"
        return OAuthProbeResult(True, True, False, False, detail, mode, remote_checked=True)
    except Exception as e:
        return OAuthProbeResult(True, True, False, False, f"oauth liveBroadcasts failed: {e}", mode, remote_checked=True)

    if not items:
        return OAuthProbeResult(
            True,
            True,
            True,
            False,
            f"{token_reason}; oauth no owned broadcasts",
            mode,
            remote_checked=True,
        )

    live_like = {"live", "liveStarting", "testing", "testStarting"}
    b = select_primary_broadcast(items) or items[0]
    broadcast_id = str(b.get("id", "")).strip()
    snippet = b.get("snippet", {}) or {}
    details = b.get("contentDetails", {}) or {}
    monitor_stream = details.get("monitorStream") or {}
    status = b.get("status", {}) or {}
    life_cycle_status = str(status.get("lifeCycleStatus", "")).strip()
    video_id = str((snippet.get("resourceId") or {}).get("videoId") or "").strip()
    channel_id = str(snippet.get("channelId", "")).strip()
    bound_stream_id = str(details.get("boundStreamId", "")).strip()
    enable_auto_start = bool(details["enableAutoStart"]) if "enableAutoStart" in details else None
    enable_auto_stop = bool(details["enableAutoStop"]) if "enableAutoStop" in details else None
    monitor_stream_enabled = (
        bool(monitor_stream["enableMonitorStream"])
        if "enableMonitorStream" in monitor_stream
        else None
    )

    stream_status = ""
    stream_health_status = ""
    stream_health_issues = 0
    if bound_stream_id:
        try:
            streams = youtube_live_api_get(
                "liveStreams",
                access_token,
                {"part": "id,status", "id": bound_stream_id},
            )
            s_items = streams.get("items", [])
            if s_items:
                s_status = (s_items[0].get("status") or {})
                stream_status = str(s_status.get("streamStatus", "")).strip()
                h = s_status.get("healthStatus") or {}
                stream_health_status = str(h.get("status", "")).strip()
                issues = h.get("configurationIssues") or []
                if isinstance(issues, list):
                    stream_health_issues = len(issues)
        except urllib.error.HTTPError as e:
            body = _http_error_body(e)
            detail = f"oauth liveStreams http {e.code}: {body[:240]}"
            if _is_quota_exceeded_error(e.code, body):
                _latched, quota_note = mark_quota_exhausted(
                    "oauth_liveStreams",
                    detail,
                    reason_hint=_extract_google_error_reason(body),
                )
                detail = f"{detail}; {quota_note}"
            return OAuthProbeResult(
                True,
                True,
                False,
                False,
                detail,
                mode,
                life_cycle_status=life_cycle_status,
                broadcast_id=broadcast_id,
                video_id=video_id,
                bound_stream_id=bound_stream_id,
                stream_status_required=OAUTH_STREAM_STATUS_REQUIRED,
                remote_checked=True,
                enable_auto_start=enable_auto_start,
                enable_auto_stop=enable_auto_stop,
                monitor_stream_enabled=monitor_stream_enabled,
            )
        except Exception as e:
            return OAuthProbeResult(
                True,
                True,
                False,
                False,
                f"oauth liveStreams failed: {e}",
                mode,
                life_cycle_status=life_cycle_status,
                broadcast_id=broadcast_id,
                video_id=video_id,
                bound_stream_id=bound_stream_id,
                stream_status_required=OAUTH_STREAM_STATUS_REQUIRED,
                remote_checked=True,
                enable_auto_start=enable_auto_start,
                enable_auto_stop=enable_auto_stop,
                monitor_stream_enabled=monitor_stream_enabled,
            )

    life_ok = life_cycle_status in live_like
    stream_ok = (stream_status == "active") if OAUTH_STREAM_STATUS_REQUIRED else (stream_status in {"active", "ready", ""})
    if bound_stream_id and OAUTH_STREAM_STATUS_REQUIRED:
        oauth_healthy = life_ok and stream_ok
    else:
        oauth_healthy = life_ok
    reason = (
        f"{token_reason}; oauth broadcast={broadcast_id or '-'} lifecycle={life_cycle_status or '-'} "
        f"stream={bound_stream_id or '-'} streamStatus={stream_status or '-'} health={stream_health_status or '-'}"
        f" autoStart={enable_auto_start if enable_auto_start is not None else '-'}"
        f" autoStop={enable_auto_stop if enable_auto_stop is not None else '-'}"
        f" monitorStream={monitor_stream_enabled if monitor_stream_enabled is not None else '-'}"
    )
    return OAuthProbeResult(
        True,
        True,
        True,
        oauth_healthy,
        reason,
        mode,
        life_cycle_status=life_cycle_status,
        broadcast_id=broadcast_id,
        video_id=video_id,
        channel_id=channel_id,
        bound_stream_id=bound_stream_id,
        stream_status=stream_status,
        stream_health_status=stream_health_status,
        stream_health_issues=stream_health_issues,
        stream_status_required=OAUTH_STREAM_STATUS_REQUIRED,
        remote_checked=True,
        enable_auto_start=enable_auto_start,
        enable_auto_stop=enable_auto_stop,
        monitor_stream_enabled=monitor_stream_enabled,
    )


def resolve_video_id_from_live_page(live_page_url: str, timeout_sec: int | None = None) -> tuple[str, str]:
    return public_probe_runtime.resolve_video_id_from_live_page(
        live_page_url,
        timeout_sec=timeout_sec,
        default_timeout_sec=TIMEOUT_SEC,
    )


def check_public_watch_page(url: str) -> tuple[bool, str]:
    return public_probe_runtime.check_public_watch_page(url, fetch_text=fetch)


def check_public_watch_page_verdict(url: str) -> WatchPageProbeResult:
    return public_probe_runtime.check_public_watch_page_verdict(url, fetch_text=fetch)


def check_public_watch_page_nonfatal(url: str) -> tuple[bool, str]:
    return public_probe_runtime.check_public_watch_page_nonfatal(url, fetch_text=fetch)


def probe_public_live_status(url: str, timeout_sec: int | None = None) -> PublicLiveProbeResult:
    return public_probe_runtime.probe_public_live_status(
        url,
        timeout_sec=timeout_sec,
        public_live_probe_timeout_sec=PUBLIC_LIVE_PROBE_TIMEOUT_SEC,
    )


def check_data_api(video_id: str, api_key: str, timeout_sec: int | None = None) -> DataApiCheckResult:
    return data_api_runtime.check_data_api(
        video_id,
        api_key,
        timeout_sec=timeout_sec,
        fetch_text=fetch,
        append_api_call_event=_append_api_call_event,
        mark_quota_exhausted=mark_quota_exhausted,
    )


def resolve_live_video_id(channel_id: str, api_key: str, timeout_sec: int | None = None) -> tuple[str, str]:
    return data_api_runtime.resolve_live_video_id(
        channel_id,
        api_key,
        timeout_sec=timeout_sec,
        fetch_text=fetch,
        append_api_call_event=_append_api_call_event,
        mark_quota_exhausted=mark_quota_exhausted,
    )


def parse_ingest_ports() -> list[int]:
    ports: list[int] = []
    raw = INGEST_TCP_PORTS_RAW.strip()
    if raw:
        for part in raw.split(","):
            s = part.strip()
            if not s:
                continue
            try:
                p = int(s)
            except ValueError:
                continue
            if p > 0:
                ports.append(p)
    if not ports:
        ports = [INGEST_TCP_PORT, 443]
    dedup: list[int] = []
    for p in ports:
        if p not in dedup:
            dedup.append(p)
    return dedup


def initial_video_context() -> tuple[str, str]:
    return VIDEO_ID or extract_video_id(LIVE_URL), LIVE_URL


def initial_channel_id() -> str:
    return CHANNEL_ID


def initial_api_key() -> str:
    return API_KEY


def initial_channel_live_url() -> str:
    return CHANNEL_LIVE_URL
