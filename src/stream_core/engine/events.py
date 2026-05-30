from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass
class StreamEventWriter:
    event_log_file: Path
    run_id: str
    stream_key_hash: str = ""
    rtmp_url_masked: str = "***"
    restart_count: int = 0
    event_seq: int = 0
    last_event_id: str = ""

    def next_event_id(self) -> str:
        self.event_seq += 1
        return f"evt-{self.run_id}-{self.event_seq:06d}-{uuid.uuid4().hex[:8]}"

    def append(self, event_type: str, **fields: object) -> str:
        event_id = self.next_event_id()
        payload = {
            "ts_utc": utc_now(),
            "event_id": event_id,
            "event_type": event_type,
            "run_id": self.run_id,
            "stream_pid": os.getpid(),
            "stream_key_hash": self.stream_key_hash,
            "restart_count": self.restart_count,
            "rtmp_url_masked": self.rtmp_url_masked,
            **fields,
        }
        self.event_log_file.parent.mkdir(parents=True, exist_ok=True)
        with self.event_log_file.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
            fh.write("\n")
        self.last_event_id = event_id
        return event_id
