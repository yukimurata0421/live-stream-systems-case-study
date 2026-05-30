from __future__ import annotations

import subprocess
from typing import Callable, Sequence

from .model import SupervisorResult, WorkloadStatus


RunSystemctl = Callable[[list[str], bool], subprocess.CompletedProcess[str]]


class SystemdSupervisor:
    """Runtime supervisor adapter for the current v2-style systemd owner."""

    def __init__(self, *, run_systemctl: RunSystemctl) -> None:
        self.run_systemctl = run_systemctl

    def status(self, target: str) -> WorkloadStatus:
        cp = self.run_systemctl(["is-active", target], False)
        text = (cp.stdout or cp.stderr or "").strip()
        return WorkloadStatus(target=target, active=cp.returncode == 0 and text == "active", detail=text, raw=text)

    def start(self, target: str) -> SupervisorResult:
        return self._run("start", target, ["start", target])

    def stop(self, target: str) -> SupervisorResult:
        return self._run("stop", target, ["stop", target])

    def restart(self, target: str, *, reason: str = "") -> SupervisorResult:
        detail = f"reason={reason}" if reason else ""
        return self._run("restart", target, ["restart", target], detail=detail)

    def start_once(self, target: str, *, reason: str = "") -> SupervisorResult:
        detail = f"reason={reason}" if reason else ""
        return self._run("start_once", target, ["start", target], detail=detail)

    def _run(self, action: str, target: str, args: Sequence[str], *, detail: str = "") -> SupervisorResult:
        cp = self.run_systemctl(list(args), False)
        if cp.returncode != 0:
            return SupervisorResult.from_completed(
                action=action,
                target=target,
                command=("systemctl", *args),
                completed=cp,
                ok=False,
                detail=detail,
            )
        if action in {"start", "restart"} and not self.status(target).active:
            return SupervisorResult.from_completed(
                action=action,
                target=target,
                command=("systemctl", *args),
                completed=cp,
                ok=False,
                detail=detail or "target is not active after command",
            )
        return SupervisorResult.from_completed(
            action=action,
            target=target,
            command=("systemctl", *args),
            completed=cp,
            ok=True,
            detail=detail,
        )
