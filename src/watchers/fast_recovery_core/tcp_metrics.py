from __future__ import annotations

import re
import subprocess


def extract_int(text: str, key: str) -> int:
    match = re.search(rf"\b{re.escape(key)}:(\d+)", text)
    if not match:
        return 0
    try:
        return int(match.group(1))
    except ValueError:
        return 0


def parse_ss_tcp_metrics(stdout: str, *, ffmpeg_pid: int, ports: list[int]) -> dict[str, int | str]:
    if ffmpeg_pid <= 1:
        return {}
    lines = (stdout or "").splitlines()
    pid_token = f"pid={ffmpeg_pid},"
    for idx, line in enumerate(lines):
        if "ESTAB" not in line or pid_token not in line:
            continue
        parts = line.split()
        peer = parts[4] if len(parts) >= 5 else ""
        if not any(peer.endswith(f":{port}") or peer.endswith(f"]:{port}") for port in ports):
            continue
        details = lines[idx + 1] if idx + 1 < len(lines) else ""
        return {
            "conn": line.strip(),
            "bytes_sent": extract_int(details, "bytes_sent"),
            "notsent": extract_int(details, "notsent"),
            "unacked": extract_int(details, "unacked"),
            "lastsnd_ms": extract_int(details, "lastsnd"),
        }
    return {}


def parse_ffmpeg_tcp_metrics(
    *,
    ffmpeg_pid: int,
    ports: list[int],
    run_cmd,
) -> dict[str, int | str]:
    if ffmpeg_pid <= 1:
        return {}
    cp: subprocess.CompletedProcess[str] = run_cmd(["ss", "-tinp"])
    if cp.returncode != 0:
        return {}
    return parse_ss_tcp_metrics(cp.stdout or "", ffmpeg_pid=ffmpeg_pid, ports=ports)


def send_mbps(*, bytes_delta: int, elapsed_sec: int) -> float | None:
    if elapsed_sec <= 0:
        return None
    return round((max(0, bytes_delta) * 8) / (elapsed_sec * 1_000_000), 3)


def low_upload_pressure_now(
    *,
    enabled: bool,
    metrics: dict[str, int | str],
    bytes_elapsed_sec: int,
    send_mbps_value: float | None,
    max_mbps: float,
    notsent: int,
    unacked: int,
    lastsnd_ms: int,
    notsent_threshold: int,
    unacked_threshold: int,
    lastsnd_ms_threshold: int,
    network_down: bool,
    tcp_probe: bool,
    stall_now: bool,
) -> bool:
    upload_queue_pressure = (
        notsent >= notsent_threshold
        or unacked >= unacked_threshold
        or lastsnd_ms >= lastsnd_ms_threshold
    )
    return (
        enabled
        and bool(metrics)
        and bytes_elapsed_sec > 0
        and send_mbps_value is not None
        and send_mbps_value <= max_mbps
        and upload_queue_pressure
        and not network_down
        and tcp_probe
        and not stall_now
    )
