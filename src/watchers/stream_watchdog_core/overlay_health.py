from __future__ import annotations


def check_overlay_outline_json(payload: object) -> tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "overlay actual range outline json missing object"
    actual_range = payload.get("actualRange")
    if not isinstance(actual_range, dict):
        return False, "overlay actual range outline json missing actualRange"
    last24h = actual_range.get("last24h")
    if not isinstance(last24h, dict):
        return False, "overlay actual range outline json missing last24h"
    points = last24h.get("points")
    if not isinstance(points, list):
        return False, "overlay actual range outline json missing points"
    if not points:
        return False, "overlay actual range outline json empty points"
    sample_count = min(len(points), 12)
    for point in points[:sample_count]:
        if not isinstance(point, list) or len(point) < 2:
            return False, "overlay actual range outline json invalid point"
        if not isinstance(point[0], (int, float)) or not isinstance(point[1], (int, float)):
            return False, "overlay actual range outline json invalid point coordinates"
    return True, f"overlay actual range outline json ok ({len(points)} points)"


def adsb_freshness_judgment(
    payload: dict,
    *,
    now_ts: int,
    state: dict,
    max_age_sec: int,
    message_stall_sec: int,
) -> tuple[bool, str, dict | None, dict | None]:
    adsb_now_raw = payload.get("now")
    try:
        adsb_now = float(adsb_now_raw)
    except (TypeError, ValueError):
        return False, "overlay adsb aircraft json missing now timestamp", None, None
    age = now_ts - int(adsb_now)
    if max_age_sec > 0 and age > max_age_sec:
        return False, f"overlay adsb aircraft json stale ({age}s>{max_age_sec}s)", None, None

    messages_raw = payload.get("messages")
    try:
        messages = int(messages_raw)
    except (TypeError, ValueError):
        messages = -1

    last_messages = int(state.get("last_messages", -1) or -1)
    last_change_ts = int(state.get("last_change_ts", 0) or 0)
    if messages >= 0:
        if last_messages < 0 or messages > last_messages:
            return True, "overlay adsb aircraft json fresh", {"last_messages": messages, "last_change_ts": now_ts}, None
        if messages < last_messages:
            next_state = {"last_messages": messages, "last_change_ts": now_ts, "counter_reset": True}
            event = {"previous_messages": last_messages, "current_messages": messages}
            return True, "overlay adsb aircraft json fresh (messages counter reset)", next_state, event
        if message_stall_sec > 0 and last_change_ts > 0 and now_ts - last_change_ts > message_stall_sec:
            return False, f"overlay adsb messages stalled ({now_ts - last_change_ts}s>{message_stall_sec}s)", None, None

    return True, "overlay adsb aircraft json fresh", None, None
