from __future__ import annotations

import json
from collections import deque
from pathlib import Path
from typing import Any


def trim_samples(raw: Any, *, maxlen: int) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    out: deque[dict[str, Any]] = deque(maxlen=maxlen)
    for item in raw:
        if isinstance(item, dict):
            out.append(item)
    return list(out)


def trim_restart_events(
    raw: Any,
    *,
    now_ts: int,
    restart_downtime_cost_sec: int,
) -> list[dict[str, int | str]]:
    if not isinstance(raw, list):
        raw = []
    out: list[dict[str, int | str]] = []
    for item in raw:
        if isinstance(item, int):
            ts = item
            downtime_sec = restart_downtime_cost_sec
            reason = "legacy"
        elif isinstance(item, dict):
            try:
                ts = int(item.get("ts", 0) or 0)
            except (TypeError, ValueError):
                continue
            try:
                downtime_sec = int(item.get("downtime_sec", restart_downtime_cost_sec) or restart_downtime_cost_sec)
            except (TypeError, ValueError):
                downtime_sec = restart_downtime_cost_sec
            reason = str(item.get("reason", ""))
        else:
            continue

        if ts <= 0 or now_ts - ts > 86400:
            continue
        out.append({"ts": ts, "downtime_sec": max(1, downtime_sec), "reason": reason})
    out.sort(key=lambda x: int(x["ts"]))
    return out


def load_state_file(
    path: Path,
    *,
    now_ts: int,
    default: dict[str, Any],
    samples_max: int,
    restart_downtime_cost_sec: int,
) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return default.copy()
    except Exception:
        return default.copy()

    state = dict(default)
    state.update(payload)
    state["restart_events"] = trim_restart_events(
        state.get("restart_events", []),
        now_ts=now_ts,
        restart_downtime_cost_sec=restart_downtime_cost_sec,
    )
    state["samples"] = trim_samples(state.get("samples", []), maxlen=samples_max)
    return state


def save_state_file(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)
