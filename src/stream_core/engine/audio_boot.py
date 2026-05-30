from __future__ import annotations

import shutil
import subprocess
import time
from typing import Callable


RunCommand = Callable[[list[str]], subprocess.CompletedProcess[str]]
Logger = Callable[[str], None]


def ensure_pulse_server(*, base_dir, run_cmd: RunCommand) -> None:
    if run_cmd(["pactl", "info"]).returncode == 0:
        return
    ensure_script = base_dir / "ops" / "scripts" / "ensure-pulse.sh"
    if ensure_script.exists():
        run_cmd([str(ensure_script)])
        time.sleep(1.0)
    elif shutil.which("pulseaudio"):
        run_cmd(["pulseaudio", "--start"])
        time.sleep(1.0)
    if run_cmd(["pactl", "info"]).returncode != 0:
        raise RuntimeError("Pulse server is unavailable.")


def ensure_virtual_sink(*, pulse_sink: str, run_cmd: RunCommand) -> None:
    if not pulse_sink:
        return
    cp = run_cmd(["pactl", "list", "short", "sinks"])
    if any(line.split()[1] == pulse_sink for line in (cp.stdout or "").splitlines() if len(line.split()) >= 2):
        return
    run_cmd(
        [
            "pactl",
            "load-module",
            "module-null-sink",
            f"sink_name={pulse_sink}",
            "sink_properties=device.description=StreamSink",
        ]
    )


def detect_pulse_monitor(*, pulse_source: str, pulse_sink: str, run_cmd: RunCommand) -> str:
    if pulse_source:
        return pulse_source
    if pulse_sink:
        return f"{pulse_sink}.monitor"
    cp = run_cmd(["pactl", "info"])
    for line in (cp.stdout or "").splitlines():
        if line.startswith("Default Sink: "):
            return f"{line.split(': ', 1)[1]}.monitor"
    return "default"


def ensure_local_audio_monitor(
    *,
    enabled: bool,
    monitor_sink: str,
    pulse_sink: str,
    latency_msec: int,
    run_cmd: RunCommand,
    log: Logger,
) -> str:
    if not enabled:
        return ""
    target = monitor_sink
    if not target:
        cp = run_cmd(["pactl", "list", "short", "sinks"])
        for line in (cp.stdout or "").splitlines():
            cols = line.split()
            if len(cols) >= 2 and cols[1] != pulse_sink:
                target = cols[1]
                break
    if not target:
        log("Local monitor sink not found. LOCAL_MONITOR_AUDIO skipped.")
        return ""
    cp = run_cmd(
        [
            "pactl",
            "load-module",
            "module-loopback",
            f"source={pulse_sink}.monitor",
            f"sink={target}",
            f"latency_msec={latency_msec}",
        ]
    )
    return (cp.stdout or "").strip()
