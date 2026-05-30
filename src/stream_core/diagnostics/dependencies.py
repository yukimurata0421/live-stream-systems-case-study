from __future__ import annotations

import shutil

from .model import CheckResult


REQUIRED_COMMANDS = ("ffmpeg", "pactl", "xdpyinfo", "python3")


def command_results(required: tuple[str, ...] = REQUIRED_COMMANDS) -> list[CheckResult]:
    results: list[CheckResult] = []
    for name in required:
        path = shutil.which(name)
        results.append(
            CheckResult(
                name=f"command:{name}",
                category="dependency",
                severity="ok" if path else "fail",
                ok=bool(path),
                fatal=not bool(path),
                summary=f"{name}: {path}" if path else f"{name}: not found",
                data={"command": name, "resolved_path": path or ""},
            )
        )
    return results
