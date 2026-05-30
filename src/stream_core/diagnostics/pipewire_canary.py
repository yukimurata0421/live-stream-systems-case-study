from __future__ import annotations

import shutil
from pathlib import Path
from typing import Callable

from .model import CheckResult


def pipewire_canary_status(
    *,
    read_env_file: Callable[[Path], dict[str, str]],
    parse_bool: Callable[[object], bool | None],
    run: Callable[..., object],
    env_path: Path = Path("/etc/default/adsb-streamnew"),
) -> dict:
    cfg = read_env_file(env_path)
    prefer = parse_bool(cfg.get("PREFER_PIPEWIRE_PULSE", ""))
    services: dict[str, str] = {}
    for unit in ("pipewire.service", "pipewire-pulse.service", "pulseaudio.service"):
        cp = run(["systemctl", "--user", "is-active", unit], check=False)
        services[unit] = (cp.stdout or "").strip() if cp.returncode == 0 else "inactive"
    server_name = ""
    if shutil.which("pactl"):
        pactl = run(["pactl", "info"], check=False)
        for line in (pactl.stdout or "").splitlines():
            if line.startswith("Server Name:"):
                server_name = line.split(":", 1)[1].strip()
                break
    pipewire_active = services.get("pipewire.service") == "active" and services.get("pipewire-pulse.service") == "active"
    server_is_pipewire = "pipewire" in server_name.lower()
    if prefer is True and pipewire_active and server_is_pipewire:
        recommendation = "canary_active_observe"
    elif prefer is True:
        recommendation = "canary_configured_but_runtime_not_pipewire"
    elif pipewire_active and server_is_pipewire:
        recommendation = "pipewire_runtime_without_canary_flag"
    else:
        recommendation = "keep_pulse_default_until_audio_regression_recurs"
    return {
        "prefer_pipewire_pulse": prefer,
        "services": services,
        "pactl_server_name": server_name,
        "pipewire_active": pipewire_active,
        "server_is_pipewire": server_is_pipewire,
        "recommendation": recommendation,
    }


def pipewire_result(status: dict) -> CheckResult:
    return CheckResult(
        name="audio:pipewire_canary",
        category="audio_contract",
        severity="info",
        ok=True,
        fatal=False,
        summary=f"pipewire canary: recommendation={status.get('recommendation', '')}",
        data=status,
    )
