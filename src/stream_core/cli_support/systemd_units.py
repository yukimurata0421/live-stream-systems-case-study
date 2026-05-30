from __future__ import annotations

import subprocess
from typing import Callable

from stream_core.supervisor import SystemdSupervisor
from stream_core.supervisor.model import SupervisorResult


RunSystemctl = Callable[[list[str], bool], subprocess.CompletedProcess[str]]


def _supervisor(run_systemctl: RunSystemctl) -> SystemdSupervisor:
    return SystemdSupervisor(run_systemctl=run_systemctl)


def unit_installed(unit: str, *, run_systemctl: RunSystemctl) -> bool:
    cp = run_systemctl(["status", unit], False)
    text = (cp.stdout or "") + "\n" + (cp.stderr or "")
    return "Loaded: loaded" in text


def is_active(unit: str, *, run_systemctl: RunSystemctl) -> bool:
    return _supervisor(run_systemctl).status(unit).active


def print_systemctl_error(action: str, unit: str, cp: subprocess.CompletedProcess[str]) -> None:
    print(f"[error] failed to {action} {unit} (exit={cp.returncode})")
    detail = (cp.stderr or cp.stdout or "").strip()
    if detail:
        print(detail)


def print_supervisor_error(result: SupervisorResult) -> None:
    print(f"[error] failed to {result.action} {result.target} (exit={result.returncode})")
    detail = (result.stderr or result.stdout or result.detail).strip()
    if detail:
        print(detail)


def start_unit(
    unit: str,
    *,
    run_systemctl: RunSystemctl,
    is_active: Callable[[str], bool],
    print_error: Callable[[str, str, subprocess.CompletedProcess[str]], None] = print_systemctl_error,
) -> bool:
    del is_active, print_error
    result = _supervisor(run_systemctl).start(unit)
    if not result.ok:
        print_supervisor_error(result)
        return False
    print(f"[ok] started {unit}")
    return True


def restart_unit(
    unit: str,
    *,
    reason: str = "",
    run_systemctl: RunSystemctl,
    is_active: Callable[[str], bool],
    print_error: Callable[[str, str, subprocess.CompletedProcess[str]], None] = print_systemctl_error,
) -> bool:
    del is_active, print_error
    result = _supervisor(run_systemctl).restart(unit, reason=reason)
    if not result.ok:
        print_supervisor_error(result)
        return False
    suffix = f" ({reason})" if reason else ""
    print(f"[ok] restarted {unit}{suffix}")
    return True


def trigger_unit(
    unit: str,
    *,
    reason: str = "",
    run_systemctl: RunSystemctl,
    print_error: Callable[[str, str, subprocess.CompletedProcess[str]], None] = print_systemctl_error,
) -> bool:
    del print_error
    result = _supervisor(run_systemctl).start_once(unit, reason=reason)
    if not result.ok:
        print_supervisor_error(result)
        return False
    suffix = f" ({reason})" if reason else ""
    print(f"[ok] triggered {unit}{suffix}")
    return True


def enable_unit(
    unit: str,
    *,
    run_systemctl: RunSystemctl,
    print_error: Callable[[str, str, subprocess.CompletedProcess[str]], None] = print_systemctl_error,
) -> bool:
    cp = run_systemctl(["enable", unit], False)
    if cp.returncode != 0:
        print_error("enable", unit, cp)
        return False
    print(f"[ok] enabled {unit}")
    return True


def stop_unit(
    unit: str,
    *,
    run_systemctl: RunSystemctl,
    print_error: Callable[[str, str, subprocess.CompletedProcess[str]], None] = print_systemctl_error,
) -> bool:
    del print_error
    result = _supervisor(run_systemctl).stop(unit)
    if not result.ok:
        print_supervisor_error(result)
        return False
    print(f"[ok] stopped {unit}")
    return True
