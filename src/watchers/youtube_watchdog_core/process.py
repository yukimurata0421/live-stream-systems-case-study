from __future__ import annotations

import os
import subprocess

try:
    from ..systemctl_control import run_systemctl
    from ..youtube_lifecycle import actions as lifecycle_actions
    from ..youtube_watchdog_config import STREAM_SERVICE
    from ..youtube_watchdog_state import log, write_restart_reason
except ImportError:
    from systemctl_control import run_systemctl
    from youtube_lifecycle import actions as lifecycle_actions
    from youtube_watchdog_config import STREAM_SERVICE
    from youtube_watchdog_state import log, write_restart_reason


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=False)


K8S_MAIN_PID = 2_147_483_000


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def k8s_mode() -> bool:
    return env("STREAM_RUNTIME_SUPERVISOR", "systemd").lower() in {"k8s", "k3s", "kubernetes"}


def k8s_workload() -> str:
    return env("STREAM_V3_RUNTIME_WORKLOAD", "deployment/stream-v3-runtime")


def k8s_exec(script: str) -> subprocess.CompletedProcess[str]:
    return run(
        [
            env("STREAM_KUBECTL_BIN", "kubectl"),
            "-n",
            env("STREAM_K8S_NAMESPACE", "stream-v3"),
            "exec",
            k8s_workload(),
            "-c",
            env("STREAM_V3_RUNTIME_CONTAINER", "stream-engine"),
            "--",
            "sh",
            "-lc",
            script,
        ]
    )


def is_service_active(unit: str) -> bool:
    if k8s_mode():
        cp = run(
            [
                env("STREAM_KUBECTL_BIN", "kubectl"),
                "-n",
                env("STREAM_K8S_NAMESPACE", "stream-v3"),
                "get",
                k8s_workload(),
                "-o",
                "jsonpath={.status.readyReplicas}/{.spec.replicas}",
            ]
        )
        if cp.returncode != 0:
            return False
        ready, _, desired = (cp.stdout or "").strip().partition("/")
        try:
            return int(ready or "0") >= int(desired or "1")
        except ValueError:
            return False
    cp = run_systemctl(["is-active", unit], require_privilege=False, check=False)
    return cp.returncode == 0 and (cp.stdout or "").strip() == "active"


def get_main_pid(unit: str) -> int:
    if k8s_mode():
        return K8S_MAIN_PID if is_service_active(unit) else 0
    cp = run_systemctl(["show", unit, "--property=MainPID", "--value"], require_privilege=False, check=False)
    if cp.returncode != 0:
        return 0
    raw = (cp.stdout or "").strip()
    if not raw:
        return 0
    try:
        pid = int(raw)
    except ValueError:
        return 0
    return pid if pid > 1 else 0


def get_child_ffmpeg_pid(main_pid: int) -> int:
    if k8s_mode() and main_pid == K8S_MAIN_PID:
        cp = k8s_exec("pgrep -a ffmpeg | awk '/rtmp:\\/\\/|rtmps:\\/\\// {print $1; exit}'")
        if cp.returncode != 0:
            return 0
        raw = (cp.stdout or "").strip().splitlines()
        if not raw:
            return 0
        try:
            pid = int(raw[0].strip())
        except ValueError:
            return 0
        return pid if pid > 1 else 0
    if main_pid <= 1:
        return 0
    cp = run(["pgrep", "-P", str(main_pid), "ffmpeg"])
    if cp.returncode != 0:
        return 0
    for line in (cp.stdout or "").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            pid = int(s)
        except ValueError:
            continue
        if pid > 1:
            return pid
    return 0


def get_process_elapsed_sec(pid: int) -> int:
    if k8s_mode():
        cp = k8s_exec(f"ps -o etimes= -p {int(pid)}")
        if cp.returncode != 0:
            return 0
        raw = (cp.stdout or "").strip()
        try:
            return max(0, int(raw))
        except ValueError:
            return 0
    if pid <= 1:
        return 0
    cp = run(["ps", "-o", "etimes=", "-p", str(pid)])
    if cp.returncode != 0:
        return 0
    raw = (cp.stdout or "").strip()
    if not raw:
        return 0
    try:
        elapsed = int(raw)
    except ValueError:
        return 0
    return max(0, elapsed)


def ffmpeg_has_ingest_connection(ffmpeg_pid: int, tcp_port: int) -> tuple[bool, str]:
    if k8s_mode():
        cp = k8s_exec("ss -tpn")
        if cp.returncode != 0:
            return False, ""
        pid_token = f"pid={ffmpeg_pid},"
        port_token = f":{tcp_port}"
        for line in (cp.stdout or "").splitlines():
            if "ESTAB" not in line:
                continue
            if pid_token not in line:
                continue
            if port_token not in line:
                continue
            return True, line.strip()
        return False, ""
    if ffmpeg_pid <= 1:
        return False, ""
    cp = run(["ss", "-tpn"])
    if cp.returncode != 0:
        return False, ""
    pid_token = f"pid={ffmpeg_pid},"
    port_token = f":{tcp_port}"
    for line in (cp.stdout or "").splitlines():
        if "ESTAB" not in line:
            continue
        if pid_token not in line:
            continue
        if port_token not in line:
            continue
        return True, line.strip()
    return False, ""


def ffmpeg_has_ingest_connection_any(ffmpeg_pid: int, ports: list[int]) -> tuple[bool, str]:
    for p in ports:
        ok, conn = ffmpeg_has_ingest_connection(ffmpeg_pid, p)
        if ok:
            return True, conn
    return False, ""


def trim_restart_history(raw_history: list[int], now_ts: int, retention_sec: int = 86400) -> list[int]:
    out: list[int] = []
    max_age = max(0, int(retention_sec))
    for item in raw_history:
        try:
            ts = int(item)
        except (TypeError, ValueError):
            continue
        if ts <= 0:
            continue
        if now_ts - ts <= max_age:
            out.append(ts)
    return sorted(out)


def restart_stream(reason: str) -> tuple[bool, str]:
    if k8s_mode():
        command = [
            env("STREAM_KUBECTL_BIN", "kubectl"),
            "-n",
            env("STREAM_K8S_NAMESPACE", "stream-v3"),
            "rollout",
            "restart",
            k8s_workload(),
        ]
        cp = run(command)
        detail = (cp.stdout or cp.stderr or "").strip()
        if cp.returncode == 0:
            log(f"Restarted {k8s_workload()} via k8s: {reason}")
            return True, detail
        return False, detail or "kubectl rollout restart failed"
    return lifecycle_actions.restart_stream(
        reason=reason,
        stream_service=STREAM_SERVICE,
        write_restart_reason=write_restart_reason,
        run_systemctl=run_systemctl,
        log=log,
    )
