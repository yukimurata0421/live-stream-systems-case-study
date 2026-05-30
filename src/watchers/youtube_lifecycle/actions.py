from __future__ import annotations

from typing import Any, Callable

try:
    from watchers.youtube_api import force_transition_live_once as _api_force_transition_live_once
except ModuleNotFoundError:
    from youtube_api import force_transition_live_once as _api_force_transition_live_once

from .guards import force_live_precheck


def restart_stream(
    *,
    reason: str,
    stream_service: str,
    write_restart_reason: Callable[..., Any],
    run_systemctl,
    log: Callable[[str], None],
    supervisor=None,
) -> tuple[bool, str]:
    write_restart_reason(component="stream", reason=reason, unit=stream_service)
    log(f"Restarting {stream_service}: {reason}")
    if supervisor is not None:
        result = supervisor.restart(stream_service, reason=reason)
        if result.ok:
            return True, "restart ok"
        detail = (result.stderr or result.stdout or result.detail or "restart failed").strip()
        log(f"ERROR restart failed: {detail}")
        return False, detail
    cp = run_systemctl(["restart", stream_service], require_privilege=True, check=False)
    if cp.returncode != 0:
        detail = cp.stderr.strip() or cp.stdout.strip() or "systemctl restart failed"
        log(f"ERROR restart failed: {detail}")
        return False, detail
    return True, "restart ok"


def force_transition_live_once(*args: Any, quota_guard_active: bool = False, **kwargs: Any) -> tuple[bool, str]:
    feature_enabled = bool(kwargs.get("feature_enabled", args[0] if args else False))
    guard = force_live_precheck(feature_enabled=feature_enabled, quota_guard_active=quota_guard_active)
    if not guard.allowed:
        return False, guard.reason
    return _api_force_transition_live_once(*args, **kwargs)
