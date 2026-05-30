from __future__ import annotations

from pathlib import Path

try:
    from stream_core.common.json_io import atomic_write_json_file, read_json_file
except ModuleNotFoundError:
    from common.json_io import atomic_write_json_file, read_json_file


def load_notify_state(path: Path) -> dict:
    payload = read_json_file(path)
    return payload if isinstance(payload.get("active"), dict) else {"active": {}, "last_status_sent_ts": 0}


def save_notify_state(state: dict, path: Path) -> None:
    atomic_write_json_file(path, state, indent=2, sort_keys=True)

