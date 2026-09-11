from __future__ import annotations

from typing import Any


def expected_cycles(window_start_ts: int, window_end_ts: int, cadence_sec: int) -> int:
    return max(0, (window_end_ts - window_start_ts + cadence_sec - 1) // cadence_sec)


def safe_int(value: Any, default: int = 0) -> int:
    return value if type(value) is int else int(default)


def bucket(timestamp: int, start: int, cadence: int) -> int:
    return (timestamp - start) // cadence


def nearest_rank(values: list[int], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, int((len(ordered) * percentile + 0.999999999)))
    return ordered[min(len(ordered), rank) - 1]
