from __future__ import annotations

from pathlib import Path
import time
from typing import Any, Mapping, Sequence

from stream_contracts.monitoring_v4.observation import ObservationRejection
from stream_contracts.monitoring_v4.time import parse_utc, unix_ts, utc_text

from .json_file import JsonSnapshot, SnapshotReadError, read_json_snapshot


def read_source(
    path: Path,
    *,
    source: str,
    received_at: str | None,
    rejections: list[ObservationRejection],
) -> JsonSnapshot | None:
    try:
        return read_json_snapshot(Path(path), received_at=received_at)
    except SnapshotReadError as exc:
        rejected_at = received_at or utc_text(int(time.time()))
        rejections.append(
            ObservationRejection.create(
                source=source,
                reason_code=exc.reason_code,
                detail=exc.detail,
                received_at=rejected_at,
                payload_sha256=exc.payload_sha256,
            )
        )
        return None


def source_timestamp(
    snapshot: JsonSnapshot,
    keys: Sequence[str],
    *,
    source: str,
    received_at: str | None,
    rejections: list[ObservationRejection],
    max_future_sec: int = 0,
) -> str | None:
    # A source may be replaced after the observer was scheduled but before this
    # read.  The stable snapshot receipt is therefore the authoritative upper
    # bound; the outer cycle clock is never used for future classification.
    effective_received_at = snapshot.received_at
    raw = next(
        (
            str(snapshot.payload.get(key, "")).strip()
            for key in keys
            if str(snapshot.payload.get(key, "")).strip()
        ),
        "",
    )
    try:
        parse_utc(raw, field="source timestamp")
    except ValueError as exc:
        rejections.append(
            ObservationRejection.create(
                source=source,
                reason_code="source_timestamp_invalid",
                detail=str(exc)[:500],
                received_at=effective_received_at,
                payload_sha256=snapshot.sha256,
            )
        )
        return None
    if unix_ts(raw) > unix_ts(effective_received_at) + max(0, int(max_future_sec)):
        rejections.append(
            ObservationRejection.create(
                source=source,
                reason_code="source_timestamp_future",
                detail=f"source timestamp exceeds received_at by more than {max(0, int(max_future_sec))}s",
                received_at=effective_received_at,
                payload_sha256=snapshot.sha256,
            )
        )
        return None
    return raw


def primitive_fields(payload: Mapping[str, Any], keys: Sequence[str]) -> dict[str, Any]:
    return {
        key: payload[key]
        for key in keys
        if key in payload and isinstance(payload[key], (str, int, float, bool, type(None)))
    }


def bounded_strings(value: Any, *, limit: int = 20, width: int = 96) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item)[:width] for item in value[:limit] if isinstance(item, str)]


def status_word(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"healthy", "ok", "good", "pass", "passed", "ready"}:
        return "good"
    if raw in {"failed", "failure", "bad", "error", "critical", "unhealthy", "degraded"}:
        return "bad"
    return "unknown"
