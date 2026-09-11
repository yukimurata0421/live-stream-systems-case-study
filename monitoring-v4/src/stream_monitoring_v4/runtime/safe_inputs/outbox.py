from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from stream_contracts.monitoring_v4.time import utc_text
from stream_monitoring_v4.adapters.json_file import SnapshotReadError, strict_json_loads

from .stable_io import stable_text_lines


ALLOWED_ROUTES = frozenset({"discord", "slack"})
MAX_OUTBOX_BYTES = 4 * 1024 * 1024
MAX_OUTBOX_LINE_BYTES = 128 * 1024
MAX_OUTBOX_LINES = 10_000


def notification_outbox_projection(path: Path, *, projected_at_ts: int) -> dict[str, Any]:
    pending = 0
    invalid = 0
    max_attempts = 0
    oldest_created_ts: int | None = None
    newest_updated_ts: int | None = None
    route_counts: Counter[str] = Counter()
    present = True
    try:
        for line in stable_text_lines(
            Path(path),
            required=True,
            max_tail_bytes=MAX_OUTBOX_BYTES,
            max_line_bytes=MAX_OUTBOX_LINE_BYTES,
            max_lines=MAX_OUTBOX_LINES,
        ):
            if not line.strip():
                continue
            try:
                item = strict_json_loads(line)
            except (TypeError, ValueError):
                invalid += 1
                continue
            if not isinstance(item, dict):
                invalid += 1
                continue
            if str(item.get("status", "pending")) != "pending":
                invalid += 1
                continue
            pending += 1
            row_invalid = not isinstance(item.get("message_id"), str) or not str(
                item.get("message_id", "")
            ).strip()
            row_invalid = row_invalid or not isinstance(item.get("content"), str)
            route = str(item.get("route", "discord") or "discord").lower()
            route_counts[route if route in ALLOWED_ROUTES else "unrecognized"] += 1
            row_invalid = row_invalid or route not in ALLOWED_ROUTES
            attempts = item.get("attempts")
            if isinstance(attempts, int) and not isinstance(attempts, bool) and attempts >= 0:
                max_attempts = max(max_attempts, attempts)
            else:
                row_invalid = True
            created = item.get("created_ts")
            if isinstance(created, int) and not isinstance(created, bool) and created > 0:
                oldest_created_ts = (
                    created if oldest_created_ts is None else min(oldest_created_ts, created)
                )
            else:
                row_invalid = True
            updated = item.get("updated_ts")
            if isinstance(updated, int) and not isinstance(updated, bool) and updated > 0:
                newest_updated_ts = (
                    updated if newest_updated_ts is None else max(newest_updated_ts, updated)
                )
            elif updated not in (None, ""):
                row_invalid = True
            if row_invalid:
                invalid += 1
    except SnapshotReadError as exc:
        if exc.reason_code != "source_missing":
            raise
        present = False
    return {
        "schema": "monitoring_v4.notification_outbox_projection.v1",
        "projected_at_utc": utc_text(projected_at_ts),
        "source_present": present,
        "pending_count": pending,
        "invalid_row_count": invalid,
        "max_attempts": max_attempts,
        "oldest_created_at_utc": utc_text(oldest_created_ts) if oldest_created_ts else "",
        "newest_updated_at_utc": utc_text(newest_updated_ts) if newest_updated_ts else "",
        "route_counts": dict(sorted(route_counts.items())),
        "content_projected": False,
        "credential_fields_projected": False,
    }
