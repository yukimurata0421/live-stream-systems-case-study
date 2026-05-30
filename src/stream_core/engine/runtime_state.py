from __future__ import annotations

import json
import os
from pathlib import Path

from .events import utc_now


def write_runtime_snapshot(
    path: Path,
    *,
    run_id: str,
    stream_key_hash: str,
    rtmp_url_masked: str,
    restart_count: int,
    last_health_ok: bool,
    last_event_id: str,
    status: str,
    ffmpeg_pid: str = "",
    note: str = "",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "stream_pid": os.getpid(),
        "stream_key_hash": stream_key_hash,
        "updated_at_utc": utc_now(),
        "status": status,
        "rtmp_url_masked": rtmp_url_masked,
        "ffmpeg_pid": str(ffmpeg_pid),
        "restart_count": restart_count,
        "last_health_ok": str(last_health_ok).lower(),
        "note": note,
        "last_event_id": last_event_id,
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)


def hashed_runtime_state_file(path: Path, stream_key_hash: str) -> Path:
    if path.name != "stream_runtime_state.json":
        return path
    return path.with_name(f"stream_runtime_state_{stream_key_hash}.json")
