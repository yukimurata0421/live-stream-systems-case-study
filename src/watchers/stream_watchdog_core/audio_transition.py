from __future__ import annotations

from datetime import datetime, timedelta, timezone


JST = timezone(timedelta(hours=9), name="JST")
AUDIO_BUCKET_BOUNDARIES = (
    ("morning", 5),
    ("day", 10),
    ("evening", 16),
    ("night", 21),
)


def parse_utc(ts: str) -> int:
    dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def audio_bucket_boundary_detail(*, current_ts: int, boundary_grace_sec: int) -> dict[str, object]:
    now_jst = datetime.fromtimestamp(current_ts, tz=JST)
    nearest: tuple[int, str, datetime] | None = None
    for day_offset in (-1, 0, 1):
        base = now_jst.date() + timedelta(days=day_offset)
        for bucket, hour in AUDIO_BUCKET_BOUNDARIES:
            boundary = datetime.combine(base, datetime.min.time(), tzinfo=JST).replace(hour=hour)
            delta_sec = int(now_jst.timestamp() - boundary.timestamp())
            abs_delta = abs(delta_sec)
            if nearest is None or abs_delta < nearest[0]:
                nearest = (abs_delta, bucket, boundary)
    if nearest is None:
        return {
            "bucket_boundary_nearest": "",
            "bucket_boundary_delta_sec": None,
            "bucket_boundary_abs_delta_sec": None,
            "bucket_boundary_within_grace": False,
            "bucket_boundary_grace_sec": boundary_grace_sec,
        }
    abs_delta, bucket, boundary = nearest
    signed_delta = int(now_jst.timestamp() - boundary.timestamp())
    return {
        "bucket_boundary_nearest": bucket,
        "bucket_boundary_jst": boundary.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "bucket_boundary_delta_sec": signed_delta,
        "bucket_boundary_abs_delta_sec": abs_delta,
        "bucket_boundary_within_grace": boundary_grace_sec > 0 and abs_delta <= boundary_grace_sec,
        "bucket_boundary_grace_sec": boundary_grace_sec,
    }


def now_playing_transition_detail(
    data: dict,
    *,
    current_ts: int,
    transition_grace_sec: int,
    boundary_grace_sec: int,
) -> dict[str, object]:
    detail: dict[str, object] = {
        "track_transition_age_sec": None,
        "track_transition_within_grace": False,
        "track_transition_grace_sec": transition_grace_sec,
        "now_playing_updated_at_utc": "",
        "now_playing_status": "",
        "now_playing_title": "",
        "now_playing_bucket": "",
        "now_playing_prefix": "",
        "now_playing_note": "",
        "now_playing_heartbeat": False,
    }
    detail.update(audio_bucket_boundary_detail(current_ts=current_ts, boundary_grace_sec=boundary_grace_sec))

    if not isinstance(data, dict):
        return detail
    note = str(data.get("note", "") or "").strip()
    note_lower = note.lower()
    now_playing = data.get("now_playing") if isinstance(data.get("now_playing"), dict) else {}
    detail.update(
        {
            "now_playing_updated_at_utc": str(data.get("updated_at_utc", "") or "").strip(),
            "now_playing_status": str(data.get("status", "") or "").strip(),
            "now_playing_title": str(now_playing.get("title", "") or "").strip(),
            "now_playing_bucket": str(now_playing.get("bucket", "") or "").strip(),
            "now_playing_prefix": str(now_playing.get("prefix", "") or "").strip(),
            "now_playing_note": note,
            "now_playing_heartbeat": "heartbeat update" in note_lower,
        }
    )
    if "heartbeat update" in note_lower:
        return detail
    updated = str(data.get("updated_at_utc", "") or "").strip()
    if not updated:
        return detail
    try:
        ts = parse_utc(updated)
    except Exception:
        return detail
    transition_age = max(0, current_ts - ts)
    detail["track_transition_age_sec"] = transition_age
    detail["track_transition_within_grace"] = transition_grace_sec > 0 and transition_age <= transition_grace_sec
    return detail
