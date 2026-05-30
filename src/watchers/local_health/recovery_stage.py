from __future__ import annotations

from pathlib import Path
from typing import Callable


ReadJson = Callable[[Path, dict], dict]
WriteJson = Callable[[Path, dict], None]
NowEpoch = Callable[[], int]


DEFAULT_STAGE_STATE = {
    "pulse_stage": 0,
    "pulse_last_ts": 0,
    "audio_stage": 0,
    "audio_last_ts": 0,
}


def load(path: Path, *, read_json: ReadJson) -> dict[str, int]:
    data = read_json(path, DEFAULT_STAGE_STATE)
    for key in DEFAULT_STAGE_STATE:
        try:
            data[key] = int(data.get(key, 0))
        except Exception:
            data[key] = 0
    return data


def save(path: Path, state: dict[str, int], *, write_json: WriteJson) -> None:
    write_json(path, state)


def bump(
    state: dict[str, int],
    *,
    stage_key: str,
    ts_key: str,
    window_sec: int,
    max_stage: int,
    now_epoch: NowEpoch,
) -> int:
    now = now_epoch()
    prev_stage = int(state.get(stage_key, 0))
    prev_ts = int(state.get(ts_key, 0))
    if prev_ts <= 0 or (now - prev_ts) > window_sec:
        stage = 1
    else:
        stage = min(prev_stage + 1, max_stage)
    state[stage_key] = stage
    state[ts_key] = now
    return stage


def reset(state: dict[str, int], *, stage_key: str, ts_key: str) -> None:
    state[stage_key] = 0
    state[ts_key] = 0
