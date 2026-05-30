from __future__ import annotations

from datetime import datetime, timedelta, timezone

TIME_BANDS = (
    ("morning", 5, 10),
    ("day", 10, 16),
    ("evening", 16, 21),
)
PATTERN = ("minor", "minor", "minor", "major")
JST = timezone(timedelta(hours=9), name="JST")


def current_bucket() -> str:
    hour = datetime.now(JST).hour
    for bucket, start, end in TIME_BANDS:
        if start <= hour < end:
            return bucket
    return "night"
