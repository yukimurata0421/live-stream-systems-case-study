from __future__ import annotations

import socket
import subprocess
from typing import Callable


RunCommand = Callable[[list[str]], subprocess.CompletedProcess[str]]


def get_default_gateway(*, run_cmd: RunCommand) -> str:
    cp = run_cmd(["ip", "route", "show", "default"])
    if cp.returncode != 0:
        return ""
    for line in (cp.stdout or "").splitlines():
        parts = line.strip().split()
        if len(parts) < 3 or parts[0] != "default":
            continue
        for i, part in enumerate(parts):
            if part == "via" and i + 1 < len(parts):
                return parts[i + 1]
    return ""


def ping_ok(target: str, *, run_cmd: RunCommand, timeout_sec: int = 1) -> bool:
    cp = run_cmd(["ping", "-c", "1", "-W", str(timeout_sec), target])
    return cp.returncode == 0


def dns_ok(host: str, *, run_cmd: RunCommand) -> bool:
    cp = run_cmd(["getent", "ahosts", host])
    return cp.returncode == 0 and bool((cp.stdout or "").strip())


def tcp_probe_ok(host: str, ports: list[int], timeout_sec: float = 1.0) -> bool:
    for port in ports:
        try:
            with socket.create_connection((host, port), timeout=timeout_sec):
                return True
        except OSError:
            continue
    return False
