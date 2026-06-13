from __future__ import annotations

import glob
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


ReadJson = Callable[[Path, dict], dict]
NowEpoch = Callable[[], int]


def pick_runtime_state_path(runtime_state_glob: str) -> Path | None:
    patterns = split_runtime_state_patterns(runtime_state_glob)
    if not patterns:
        patterns = [runtime_state_glob]
    candidates: list[str] = []
    for pattern in patterns:
        candidates.extend(glob.glob(pattern))
    candidates = sorted(set(candidates))
    if not candidates:
        return None
    return Path(max(candidates, key=lambda p: os.path.getmtime(p)))


def split_runtime_state_patterns(runtime_state_glob: str) -> list[str]:
    patterns = [p.strip() for p in runtime_state_glob.split(os.pathsep) if p.strip()]
    if len(patterns) > 1 or os.pathsep == ":":
        return patterns
    raw = runtime_state_glob.strip()
    if not raw:
        return []
    return [p.strip() for p in re.split(r":(?=[A-Za-z]:[\\/])", raw) if p.strip()]


def parse_utc_epoch(ts: str) -> int:
    dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def runtime_snapshot_age_sec(
    runtime_state_glob: str,
    *,
    read_json: ReadJson,
    now_epoch: NowEpoch,
) -> int:
    latest_path = pick_runtime_state_path(runtime_state_glob)
    if latest_path is None:
        return 10**9
    data = read_json(latest_path, {})
    updated = str(data.get("updated_at_utc", "")).strip()
    if not updated:
        return 10**9
    try:
        ts = parse_utc_epoch(updated)
    except Exception:
        return 10**9
    return max(0, now_epoch() - ts)
