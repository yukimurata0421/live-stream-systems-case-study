from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


DEFAULT_MARKER_FILE = Path("/state/runtime/stream_boot_established.json")
DEFAULT_BOOT_ID_FILE = Path("/proc/sys/kernel/random/boot_id")
DEFAULT_RTMP_PORTS = (443, 1935)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def int_env(name: str, default: int) -> int:
    try:
        return int(env(name, str(default)))
    except ValueError:
        return default


def read_boot_id(path: Path = DEFAULT_BOOT_ID_FILE) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def parse_ports(raw: str) -> tuple[int, ...]:
    ports = tuple(int(part) for part in raw.split(",") if part.strip().isdigit())
    return ports or DEFAULT_RTMP_PORTS


def process_cmdline(pid: int, *, proc_root: Path = Path("/proc")) -> list[str]:
    if pid <= 1:
        return []
    try:
        raw = (proc_root / str(pid) / "cmdline").read_bytes()
    except OSError:
        return []
    return [part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part]


def process_uptime_sec(pid: int, *, proc_root: Path = Path("/proc")) -> int:
    if pid <= 1:
        return 0
    try:
        fields = (proc_root / str(pid) / "stat").read_text(encoding="utf-8").split()
        start_ticks = int(fields[21])
        system_uptime = float((proc_root / "uptime").read_text(encoding="utf-8").split()[0])
        ticks_per_sec = int(os.sysconf("SC_CLK_TCK"))
    except (OSError, ValueError, IndexError):
        return 0
    return max(0, int(system_uptime - (start_ticks / ticks_per_sec)))


def find_stream_ffmpeg(*, proc_root: Path = Path("/proc")) -> tuple[int, list[str]]:
    try:
        candidates = sorted(
            (entry for entry in proc_root.iterdir() if entry.name.isdigit()),
            key=lambda entry: int(entry.name),
        )
    except OSError:
        return 0, []
    for entry in candidates:
        pid = int(entry.name)
        args = process_cmdline(pid, proc_root=proc_root)
        if not args or Path(args[0]).name != "ffmpeg":
            continue
        joined = "\0".join(args)
        if "rtmp://" not in joined and "rtmps://" not in joined:
            continue
        return pid, args
    return 0, []


def command_uses_nvenc(args: list[str]) -> bool:
    for index, value in enumerate(args[:-1]):
        if value == "-c:v" and args[index + 1] == "h264_nvenc":
            return True
    return False


def established_rtmp_socket(
    pid: int,
    *,
    ports: tuple[int, ...],
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> tuple[bool, str]:
    try:
        cp = runner(
            ["ss", "-Htnp", "state", "established"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=2.0,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"ss unavailable: {type(exc).__name__}"
    if cp.returncode != 0:
        return False, (cp.stderr or f"ss exited {cp.returncode}").strip()
    pid_pattern = re.compile(rf"\bpid={pid},")
    port_pattern = re.compile(rf":(?:{'|'.join(str(port) for port in ports)})\b")
    for line in cp.stdout.splitlines():
        if pid_pattern.search(line) and port_pattern.search(line):
            return True, "ffmpeg RTMP socket established"
    return False, "ffmpeg RTMP socket is not established"


def gpu_driver_version(
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> tuple[bool, str]:
    try:
        cp = runner(
            ["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=3.0,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"nvidia-smi unavailable: {type(exc).__name__}"
    version = (cp.stdout or "").splitlines()[0].strip() if cp.stdout else ""
    if cp.returncode != 0 or not version:
        return False, (cp.stderr or f"nvidia-smi exited {cp.returncode}").strip()
    return True, version


def readiness_status(
    *,
    proc_root: Path = Path("/proc"),
    min_ffmpeg_uptime_sec: int,
    rtmp_ports: tuple[int, ...],
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, Any]:
    boot_id = read_boot_id(proc_root / "sys" / "kernel" / "random" / "boot_id")
    gpu_ok, gpu_detail = gpu_driver_version(runner=runner)
    ffmpeg_pid, ffmpeg_args = find_stream_ffmpeg(proc_root=proc_root)
    ffmpeg_uptime_sec = process_uptime_sec(ffmpeg_pid, proc_root=proc_root)
    nvenc_active = command_uses_nvenc(ffmpeg_args)
    socket_ok, socket_detail = established_rtmp_socket(ffmpeg_pid, ports=rtmp_ports, runner=runner)

    reasons: list[str] = []
    if not boot_id:
        reasons.append("boot id unavailable")
    if not gpu_ok:
        reasons.append(f"GPU unavailable: {gpu_detail}")
    if ffmpeg_pid <= 1:
        reasons.append("stream FFmpeg process not found")
    if ffmpeg_pid > 1 and not nvenc_active:
        reasons.append("stream FFmpeg is not using h264_nvenc")
    if ffmpeg_pid > 1 and ffmpeg_uptime_sec < min_ffmpeg_uptime_sec:
        reasons.append(f"FFmpeg warmup {ffmpeg_uptime_sec}s<{min_ffmpeg_uptime_sec}s")
    if not socket_ok:
        reasons.append(socket_detail)

    return {
        "schema": "stream_v3_runtime_readiness.v1",
        "ready": not reasons,
        "checked_at_utc": utc_now(),
        "boot_id": boot_id,
        "pod_name": env("STREAM_V3_POD_NAME"),
        "pod_uid": env("STREAM_V3_POD_UID"),
        "ffmpeg_pid": ffmpeg_pid,
        "ffmpeg_uptime_sec": ffmpeg_uptime_sec,
        "nvenc_active": nvenc_active,
        "rtmp_socket_established": socket_ok,
        "gpu_ok": gpu_ok,
        "driver_version": gpu_detail if gpu_ok else "",
        "reason": "; ".join(reasons) if reasons else "GPU, NVENC FFmpeg, and RTMP socket are ready",
    }


def write_marker(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def current_pod_establishment(
    marker_file: Path,
    *,
    pod_uid: str,
    pod_name: str = "",
    boot_id_file: Path = DEFAULT_BOOT_ID_FILE,
) -> dict[str, Any]:
    boot_id = read_boot_id(boot_id_file)
    try:
        payload = json.loads(marker_file.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"established": False, "reason": "readiness establishment marker is missing"}
    except (OSError, json.JSONDecodeError) as exc:
        return {"established": False, "reason": f"readiness establishment marker is invalid: {type(exc).__name__}"}
    if not isinstance(payload, dict) or not payload.get("ready"):
        return {"established": False, "reason": "readiness establishment marker is not ready"}
    if not boot_id or str(payload.get("boot_id") or "") != boot_id:
        return {"established": False, "reason": "readiness establishment marker belongs to another host boot"}
    if not pod_uid:
        return {"established": False, "reason": "current Pod UID is unavailable"}
    if str(payload.get("pod_uid") or "") != pod_uid:
        return {"established": False, "reason": "readiness establishment marker belongs to another Pod"}
    if pod_name and str(payload.get("pod_name") or "") != pod_name:
        return {"established": False, "reason": "readiness establishment marker Pod name does not match"}
    return {"established": True, "reason": "current Pod established streaming", "marker": payload}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check the live stream runtime readiness contract.")
    parser.add_argument("--check-only", action="store_true", help="Do not update the establishment marker.")
    parser.add_argument(
        "--marker-file",
        type=Path,
        default=Path(env("STREAM_BOOT_ESTABLISHED_FILE", str(DEFAULT_MARKER_FILE))),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    status = readiness_status(
        min_ffmpeg_uptime_sec=max(1, int_env("STREAM_READINESS_MIN_FFMPEG_UPTIME_SEC", 10)),
        rtmp_ports=parse_ports(env("STREAM_READINESS_RTMP_PORTS", "443,1935")),
    )
    if status["ready"] and not args.check_only:
        write_marker(args.marker_file, status)
    print(json.dumps(status, ensure_ascii=False, sort_keys=True))
    return 0 if status["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
