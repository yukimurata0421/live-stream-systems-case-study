from __future__ import annotations

import os
from pathlib import Path


def validate_distinct_health_files(
    ready_file: Path | None,
    heartbeat_file: Path | None,
) -> None:
    """Reject configurations where liveness can impersonate readiness."""

    if ready_file is None or heartbeat_file is None:
        return
    ready = Path(ready_file)
    heartbeat = Path(heartbeat_file)
    try:
        same_resolved_path = ready.resolve(strict=False) == heartbeat.resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise ValueError("health file paths cannot be resolved safely") from exc
    if same_resolved_path:
        raise ValueError("ready-file and heartbeat-file must be distinct")
    try:
        same_file = os.path.samefile(ready, heartbeat)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise ValueError("health file identity cannot be verified safely") from exc
    if same_file:
        raise ValueError("ready-file and heartbeat-file must not alias the same file")
