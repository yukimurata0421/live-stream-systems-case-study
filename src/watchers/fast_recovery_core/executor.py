from __future__ import annotations

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
