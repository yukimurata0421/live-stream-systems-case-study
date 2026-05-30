from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class WorkloadStatus:
    target: str
    active: bool
    detail: str = ""
    raw: str = ""


@dataclass(frozen=True)
class SupervisorResult:
    action: str
    target: str
    ok: bool
    command: tuple[str, ...] = ()
    returncode: int = 0
    stdout: str = ""
    stderr: str = ""
    detail: str = ""
    dry_run: bool = False

    @classmethod
    def from_completed(
        cls,
        *,
        action: str,
        target: str,
        command: Sequence[str],
        completed: subprocess.CompletedProcess[str],
        ok: bool | None = None,
        detail: str = "",
    ) -> "SupervisorResult":
        return cls(
            action=action,
            target=target,
            ok=(completed.returncode == 0 if ok is None else ok),
            command=tuple(command),
            returncode=int(completed.returncode),
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
            detail=detail,
        )

    @classmethod
    def planned(cls, *, action: str, target: str, command: Sequence[str], detail: str = "") -> "SupervisorResult":
        return cls(
            action=action,
            target=target,
            ok=True,
            command=tuple(command),
            detail=detail,
            dry_run=True,
        )
