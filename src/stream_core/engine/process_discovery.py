from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Callable


RunCommand = Callable[[list[str]], subprocess.CompletedProcess[str]]


def parse_pgrep_output(stdout: str) -> list[tuple[int, str]]:
    rows: list[tuple[int, str]] = []
    for line in (stdout or "").splitlines():
        parts = line.strip().split(maxsplit=1)
        if not parts or not parts[0].isdigit():
            continue
        pid = int(parts[0])
        cmd = parts[1] if len(parts) > 1 else ""
        rows.append((pid, cmd))
    return rows


def pgrep_cmds(pattern: str, *, run_cmd: RunCommand) -> list[tuple[int, str]]:
    cp = run_cmd(["pgrep", "-a", "-f", pattern])
    return parse_pgrep_output(cp.stdout or "")


def _cmd_path_text(path: Path) -> str:
    return path.expanduser().as_posix()


def _normalized_cmd(text: str) -> str:
    return text.replace("\\", "/")


def foreign_rtmp_pids(
    *,
    rtmp_url: str,
    test_mode: bool,
    current_pid: int,
    run_cmd: RunCommand,
) -> list[int]:
    if test_mode:
        return []
    cp = run_cmd(["pgrep", "-a", "ffmpeg"])
    pids: list[int] = []
    for line in (cp.stdout or "").splitlines():
        if rtmp_url not in line:
            continue
        parts = line.strip().split(maxsplit=1)
        if not parts or not parts[0].isdigit():
            continue
        pid = int(parts[0])
        if pid != current_pid:
            pids.append(pid)
    return pids


def stale_capture_helper_pids(
    *,
    base_dir: Path,
    overlay_dir: Path,
    browser_profile_dir: Path,
    display_name: str,
    overlay_port: int,
    current_pid: int,
    run_cmd: RunCommand,
) -> dict[str, list[int]]:
    overlay_script = _cmd_path_text(base_dir / "src" / "stream_core" / "overlay_server.py")
    overlay_dir_str = _cmd_path_text(overlay_dir)
    browser_profile = _cmd_path_text(browser_profile_dir)
    overlay_index_marker = f":{overlay_port}/index.html"

    stale: dict[str, list[int]] = {"xvfb": [], "overlay": [], "browser": []}
    for pid, cmd in pgrep_cmds("Xvfb", run_cmd=run_cmd):
        if pid == current_pid:
            continue
        parts = cmd.split()
        if parts and parts[0].endswith("Xvfb") and display_name in parts[1:]:
            stale["xvfb"].append(pid)

    for pid, cmd in pgrep_cmds("overlay_server.py", run_cmd=run_cmd):
        if pid == current_pid:
            continue
        cmd_text = _normalized_cmd(cmd)
        if overlay_script in cmd_text and f"--port {overlay_port}" in cmd_text and overlay_dir_str in cmd_text:
            stale["overlay"].append(pid)

    for pid, cmd in pgrep_cmds(str(browser_profile_dir), run_cmd=run_cmd):
        if pid == current_pid:
            continue
        cmd_text = _normalized_cmd(cmd)
        if f"--user-data-dir={browser_profile}" in cmd_text and (
            "chromium" in cmd_text
            or "chrome" in cmd_text
            or overlay_index_marker in cmd_text
            or "chrome_crashpad_handler" in cmd_text
        ):
            stale["browser"].append(pid)

    return {label: sorted(set(pids)) for label, pids in stale.items() if pids}


def terminate_stale_pids(
    label: str,
    pids: list[int],
    *,
    current_pid: int,
    pid_alive: Callable[[int], bool],
    append_event: Callable[..., str],
    log: Callable[[str], None],
    kill: Callable[[int, int], None] = os.kill,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    unique_pids = sorted({pid for pid in pids if pid > 1 and pid != current_pid})
    if not unique_pids:
        return
    for pid in unique_pids:
        log(f"Terminating stale {label} helper pid={pid}")
        append_event("stale_capture_helper_kill", helper=label, pid=pid, signal="TERM")
        try:
            kill(pid, signal.SIGTERM)
        except OSError:
            pass
    sleep(0.5)
    for pid in unique_pids:
        if not pid_alive(pid):
            continue
        log(f"Force-killing stale {label} helper pid={pid}")
        append_event("stale_capture_helper_kill", helper=label, pid=pid, signal="KILL")
        try:
            kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
        except OSError:
            pass
