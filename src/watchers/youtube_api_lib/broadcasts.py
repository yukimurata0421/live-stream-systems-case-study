from __future__ import annotations

from datetime import datetime, timedelta, timezone


def filter_broadcasts_by_status(items: list[dict], status: str) -> list[dict]:
    if status == "all":
        return items

    lifecycle_by_status = {
        "active": {"live", "liveStarting"},
        "upcoming": {"created", "ready", "testStarting", "testing"},
        "completed": {"complete", "revoked"},
    }
    allowed = lifecycle_by_status.get(status, set())
    if not allowed:
        return items
    return [
        item
        for item in items
        if str(((item.get("status") or {}).get("lifeCycleStatus") or "")).strip() in allowed
    ]


def parse_yt_ts(value: str) -> int:
    s = (value or "").strip()
    if not s:
        return 0
    try:
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return 0


def lifecycle_priority(lifecycle: str) -> int:
    table = {
        "live": 70,
        "liveStarting": 60,
        "testing": 50,
        "testStarting": 45,
        "ready": 40,
        "created": 35,
        "complete": 10,
        "revoked": 0,
    }
    return table.get(lifecycle.strip(), -1)


def select_primary_broadcast(
    broadcasts: list[dict],
    preferred_video_id: str = "",
    preferred_broadcast_id: str = "",
) -> dict | None:
    if not broadcasts:
        return None
    if preferred_broadcast_id:
        for item in broadcasts:
            if str(item.get("id", "")).strip() == preferred_broadcast_id:
                return item
    if preferred_video_id:
        for item in broadcasts:
            vid = str((((item.get("snippet") or {}).get("resourceId") or {}).get("videoId") or "")).strip()
            if vid and vid == preferred_video_id:
                return item

    ranked: list[tuple[int, int, dict]] = []
    for item in broadcasts:
        snippet = item.get("snippet", {}) or {}
        status = item.get("status", {}) or {}
        lifecycle = str(status.get("lifeCycleStatus", "")).strip()
        rank = lifecycle_priority(lifecycle)
        ts = max(
            parse_yt_ts(str(snippet.get("actualStartTime", "")).strip()),
            parse_yt_ts(str(snippet.get("scheduledStartTime", "")).strip()),
            parse_yt_ts(str(snippet.get("publishedAt", "")).strip()),
        )
        ranked.append((rank, ts, item))
    ranked.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return ranked[0][2] if ranked else broadcasts[0]


def choose_transition_target_broadcast(
    broadcasts: list[dict],
    preferred_video_id: str,
    preferred_broadcast_id: str,
) -> tuple[str, str, str, str]:
    if preferred_broadcast_id:
        return preferred_broadcast_id, "", "", "selected by YTW_FORCE_LIVE_BROADCAST_ID"

    allowed_lifecycle = {"ready", "testStarting", "testing", "liveStarting"}
    candidates: list[tuple[str, str, str]] = []
    for b in broadcasts:
        bid = str(b.get("id", "")).strip()
        snippet = b.get("snippet", {}) or {}
        status = b.get("status", {}) or {}
        rid = snippet.get("resourceId", {}) or {}
        vid = str(rid.get("videoId", "")).strip()
        lifecycle = str(status.get("lifeCycleStatus", "")).strip()
        if not bid:
            continue
        if lifecycle not in allowed_lifecycle:
            continue
        candidates.append((bid, vid, lifecycle))

    if not candidates:
        return "", "", "", "no transitionable broadcasts in lifecycle ready/testing/liveStarting"

    if preferred_video_id:
        for bid, vid, lifecycle in candidates:
            if vid and vid == preferred_video_id:
                return bid, vid, lifecycle, "matched preferred video_id"

    if len(candidates) == 1:
        bid, vid, lifecycle = candidates[0]
        return bid, vid, lifecycle, "selected only transitionable broadcast"

    candidate_items: list[dict] = []
    for bid, _vid, _life in candidates:
        for src in broadcasts:
            if str(src.get("id", "")).strip() == bid:
                candidate_items.append(src)
                break
    selected = select_primary_broadcast(candidate_items, preferred_video_id=preferred_video_id)
    if selected:
        sel_bid = str(selected.get("id", "")).strip()
        sel_snippet = selected.get("snippet", {}) or {}
        sel_status = selected.get("status", {}) or {}
        sel_vid = str((sel_snippet.get("resourceId") or {}).get("videoId") or "").strip()
        sel_lifecycle = str(sel_status.get("lifeCycleStatus", "")).strip()
        if sel_bid:
            return sel_bid, sel_vid, sel_lifecycle, "selected highest-priority transitionable broadcast"

    bid, vid, lifecycle = candidates[0]
    return bid, vid, lifecycle, "selected first transitionable broadcast"


def build_safe_video_snippet_for_category(existing_snippet: dict, category_id: str) -> dict:
    snippet: dict = {}
    for key in ("title", "description", "tags", "defaultLanguage", "defaultAudioLanguage"):
        if key in existing_snippet and existing_snippet.get(key) is not None:
            snippet[key] = existing_snippet.get(key)
    snippet["title"] = str(snippet.get("title") or existing_snippet.get("title") or "Recovered live stream")
    snippet["description"] = str(snippet.get("description") or existing_snippet.get("description") or "")
    snippet["categoryId"] = str(category_id).strip()
    return snippet


def recovery_broadcast_body(
    source_broadcast: dict,
    *,
    enable_auto_start: bool,
    enable_auto_stop: bool,
    now_utc: datetime | None = None,
) -> dict:
    snippet = dict(source_broadcast.get("snippet", {}) or {})
    status = dict(source_broadcast.get("status", {}) or {})
    title = str(snippet.get("title", "") or "Recovered live stream")
    description = str(snippet.get("description", "") or "")
    current = now_utc or datetime.now(timezone.utc)
    scheduled = current.astimezone(timezone.utc) + timedelta(seconds=60)
    return {
        "snippet": {
            "title": title,
            "description": description,
            "scheduledStartTime": scheduled.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "status": {
            "privacyStatus": str(status.get("privacyStatus", "") or "public"),
            "selfDeclaredMadeForKids": bool(status.get("selfDeclaredMadeForKids", False)),
        },
        "contentDetails": {
            "monitorStream": {
                "enableMonitorStream": True,
                "broadcastStreamDelayMs": 0,
            },
            "enableAutoStart": bool(enable_auto_start),
            "enableAutoStop": bool(enable_auto_stop),
            "enableDvr": True,
            "recordFromStart": True,
            "enableEmbed": bool((source_broadcast.get("contentDetails") or {}).get("enableEmbed", True)),
            "latencyPreference": str((source_broadcast.get("contentDetails") or {}).get("latencyPreference", "normal")),
        },
    }
