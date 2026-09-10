from __future__ import annotations

import os
import signal
import time
from typing import Callable


def restart_stream(
    *,
    stream_service: str,
    reason: str,
    run_systemctl,
    log: Callable[[str], None],
    supervisor=None,
) -> tuple[bool, str]:
    log(f"FAST_RECOVERY restart {stream_service}: {reason}")
    if supervisor is not None:
        result = supervisor.restart(stream_service, reason=reason)
        if result.ok:
            return True, "restart ok"
        detail = (result.stderr or result.stdout or result.detail or "restart failed").strip()
        log(f"FAST_RECOVERY restart failed: {detail}")
        return False, detail
    cp = run_systemctl(["restart", stream_service], require_privilege=True, check=False)
    if cp.returncode == 0:
        return True, ""
    detail = (cp.stderr or cp.stdout or "").strip()
    log(f"FAST_RECOVERY restart failed: {detail}")
    return False, detail


def restart_ffmpeg_child(
    *,
    ffmpeg_pid: int,
    reason: str,
    log: Callable[[str], None],
    send_signal: Callable[[int, int], None] = os.kill,
    process_exists: Callable[[int], bool] | None = None,
    wait_timeout_sec: float = 2.0,
    poll_sec: float = 0.1,
) -> tuple[bool, str]:
    if ffmpeg_pid <= 1:
        return False, "invalid ffmpeg pid"
    if process_exists is None:
        process_exists = lambda pid: os.path.exists(f"/proc/{pid}")

    log(f"FAST_RECOVERY ffmpeg child SIGTERM pid={ffmpeg_pid}: {reason}")
    try:
        send_signal(ffmpeg_pid, signal.SIGTERM)
    except ProcessLookupError:
        return True, "ffmpeg child already exited"
    except (OSError, PermissionError) as exc:
        detail = f"ffmpeg child SIGTERM failed: {type(exc).__name__}: {exc}"
        log(detail)
        return False, detail

    deadline = time.monotonic() + max(0.0, wait_timeout_sec)
    while process_exists(ffmpeg_pid) and time.monotonic() < deadline:
        time.sleep(max(0.01, poll_sec))
    if process_exists(ffmpeg_pid):
        # Do not escalate to SIGKILL from the sidecar. The owning stream engine
        # retains shutdown and force-kill authority for its child.
        return True, "SIGTERM sent; stream engine owns final child cleanup"
    return True, "ffmpeg child exited after SIGTERM"
