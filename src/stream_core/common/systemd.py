from __future__ import annotations

import os
import subprocess
from typing import Sequence


READ_ONLY_VERBS = {
    "cat",
    "is-active",
    "is-enabled",
    "is-failed",
    "list-timers",
    "show",
    "status",
}


def _env_flag_auto(name: str, default_auto_for_non_root: bool) -> bool:
    raw = os.environ.get(name, "auto").strip().lower()
    if raw in {"1", "true", "yes", "on", "always"}:
        return True
    if raw in {"0", "false", "no", "off", "never"}:
        return False
    return default_auto_for_non_root and os.geteuid() != 0


def systemctl_prefix(*, require_privilege: bool) -> list[str]:
    if not require_privilege:
        return ["systemctl"]
    use_sudo = _env_flag_auto("WATCHDOG_SYSTEMCTL_USE_SUDO", default_auto_for_non_root=True)
    if use_sudo and os.geteuid() != 0:
        return ["sudo", "-n", "systemctl"]
    return ["systemctl"]


def is_read_only_systemctl_args(args: Sequence[str]) -> bool:
    for arg in args:
        if arg.startswith("-"):
            continue
        return arg in READ_ONLY_VERBS
    return False


def run_systemctl(
    args: Sequence[str],
    *,
    require_privilege: bool = True,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    cmd = [*systemctl_prefix(require_privilege=require_privilege), *args]
    return subprocess.run(cmd, text=True, capture_output=True, check=check)


def run_systemctl_readonly(args: Sequence[str], *, check: bool = False) -> subprocess.CompletedProcess[str]:
    return run_systemctl(args, require_privilege=False, check=check)


def run_systemctl_mutating(args: Sequence[str], *, check: bool = False) -> subprocess.CompletedProcess[str]:
    return run_systemctl(args, require_privilege=True, check=check)


def is_active(unit: str) -> bool:
    cp = run_systemctl_readonly(["is-active", unit], check=False)
    return cp.returncode == 0 and (cp.stdout or "").strip() == "active"


def show_value(unit: str, property_name: str) -> str:
    cp = run_systemctl_readonly(["show", unit, f"--property={property_name}", "--value"], check=False)
    if cp.returncode != 0:
        return ""
    return (cp.stdout or "").strip()


def main_pid(unit: str) -> int:
    raw = show_value(unit, "MainPID")
    try:
        pid = int(raw)
    except ValueError:
        return 0
    return pid if pid > 1 else 0
