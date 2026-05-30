from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class RestartActionContext:
    unit: str
    component: str
    reason: str


def restart_service(
    context: RestartActionContext,
    *,
    allow_restart: Callable[[str, str], bool],
    append_event: Callable[..., str],
    write_restart_reason: Callable[[str, str, str, str], None],
    run_systemctl,
    log: Callable[[str], None],
    supervisor=None,
) -> bool:
    if not allow_restart(context.component, context.reason):
        append_event("restart_skipped", component=context.component, reason=context.reason, unit=context.unit)
        return False
    event_id = append_event("restart_trigger", component=context.component, reason=context.reason, unit=context.unit)
    write_restart_reason(context.component, context.reason, context.unit, event_id)
    log(f"Restarting {context.unit}: {context.reason}")
    if supervisor is not None:
        result = supervisor.restart(context.unit, reason=context.reason)
        if result.ok:
            return True
        append_event(
            "restart_failed",
            component=context.component,
            reason=context.reason,
            unit=context.unit,
            returncode=result.returncode,
            stderr=result.stderr.strip(),
            stdout=result.stdout.strip(),
            detail=result.detail,
        )
        log(f"ERROR failed to restart {context.unit}: {(result.stderr or result.stdout or result.detail).strip()}")
        return False
    cp = run_systemctl(["restart", context.unit], require_privilege=True, check=False)
    if cp.returncode != 0:
        append_event(
            "restart_failed",
            component=context.component,
            reason=context.reason,
            unit=context.unit,
            returncode=cp.returncode,
            stderr=(cp.stderr or "").strip(),
            stdout=(cp.stdout or "").strip(),
        )
        log(f"ERROR failed to restart {context.unit}: {(cp.stderr or cp.stdout or '').strip()}")
        return False
    return True
