from __future__ import annotations

import uuid
from pathlib import Path

from .io import jst_now_iso, utc_now_iso
from .rotation import track_prefix
from .text import NOW_PLAYING_PREFIX

SNAPSHOT_SCHEMA = "now_playing_snapshot/v1"


def now_playing_line(title: str) -> str:
    return f"{NOW_PLAYING_PREFIX}{title}\n"


def history_event(
    *,
    run_id: str,
    sequence: int,
    track: Path,
    title: str,
    bucket: str,
    player: str,
    retry_count: int,
) -> dict:
    return {
        "event_id": f"evt-dj-{run_id}-{sequence:06d}-{uuid.uuid4().hex[:8]}",
        "run_id": run_id,
        "ts_jst": jst_now_iso(),
        "event": "track_selected",
        "sequence": sequence,
        "bucket": bucket,
        "prefix": track_prefix(track),
        "title": title,
        "source_filename": track.name,
        "source_path": str(track),
        "player": player,
        "retry_count": retry_count,
    }


def snapshot_payload(
    *,
    run_id: str,
    sequence: int,
    status: str,
    track: Path | None,
    title: str,
    bucket: str,
    player: str,
    force_pulse_ao: bool,
    pulse_sink: str | None,
    retry_count: int,
    player_exit_code: int | None,
    player_fail_sleep_sec: int,
    note: str | None = None,
) -> dict:
    prefix = track_prefix(track)
    return {
        "event_id": f"evt-dj-snapshot-{run_id}-{sequence:06d}-{uuid.uuid4().hex[:8]}",
        "run_id": run_id,
        "schema": SNAPSHOT_SCHEMA,
        "sequence": sequence,
        "updated_at_utc": utc_now_iso(),
        "status": status,
        "note": note or "",
        "now_playing": {
            "title": title,
            "title_line": f"{NOW_PLAYING_PREFIX}{title}",
            "bucket": bucket,
            "prefix": prefix,
            "source_filename": track.name if track else "",
            "source_path": str(track) if track else "",
        },
        "player": {
            "name": player,
            "force_pulse_ao": force_pulse_ao,
            "pulse_sink": pulse_sink or "",
            "last_exit_code": player_exit_code,
        },
        "retry": {
            "attempt": retry_count,
            "max_attempts": 3,
            "sleep_after_failure_sec": player_fail_sleep_sec,
        },
    }
