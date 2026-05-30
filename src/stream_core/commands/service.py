from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Any


K8S_WORKLOADS = (
    "deployment/stream-v3-runtime",
    "deployment/stream-v3-control",
    "deployment/stream-v3-observer",
)


@dataclass(frozen=True)
class ServiceContext:
    systemd_src_dir: Path
    install_targets: dict[str, str]
    system_units: tuple[str, ...]
    all_units: tuple[str, ...]
    notify_env_file: Path
    dj_service: str
    stream_service: str
    watchdog_timer: str
    watchdog_service: str
    youtube_monitor_timer: str
    youtube_monitor_service: str
    youtube_video_resolver_timer: str
    youtube_video_resolver_service: str
    fast_recovery_timer: str
    fast_recovery_service: str
    stream1090_report_timer: str
    stream1090_report_service: str
    upstream_report_timer: str
    upstream_report_service: str
    subsystems_status_timer: str
    subsystems_status_service: str
    recovery_orchestrator_timer: str
    recovery_orchestrator_service: str
    memory_status_timer: str
    memory_status_service: str
    resource_memory_timer: str
    resource_memory_service: str
    notify_timer: str
    notify_service: str
    run: Callable[..., object]
    run_systemctl: Callable[..., object]
    unit_installed: Callable[[str], bool]
    is_active: Callable[[str], bool]
    start_unit: Callable[[str], bool]
    restart_unit: Callable[..., bool]
    trigger_unit: Callable[[str, str], bool]
    enable_unit: Callable[[str], bool]
    guard_start_safety: Callable[[], int]
    expected_stream_key: Callable[[], str]
    running_stream_key: Callable[[], str]
    youtube_watchdog_unhealthy: Callable[[], bool]
    print_systemctl_error: Callable[..., None]
    supervisor_mode: str = "systemd"
    runtime_supervisor: Any = None
    k8s_workloads: tuple[str, ...] = K8S_WORKLOADS


def _is_k8s(ctx: ServiceContext) -> bool:
    return str(getattr(ctx, "supervisor_mode", "systemd")).strip().lower() in {"k8s", "k3s", "kubernetes"}


def _supervisor(ctx: ServiceContext):
    supervisor = getattr(ctx, "runtime_supervisor", None)
    if supervisor is None:
        raise RuntimeError("runtime_supervisor is required for k8s service commands")
    return supervisor


def _print_supervisor_result(result) -> None:
    prefix = "[plan]" if getattr(result, "dry_run", False) else ("[ok]" if result.ok else "[error]")
    command = " ".join(result.command)
    detail = f" {result.detail}" if result.detail else ""
    print(f"{prefix} {result.action} {result.target}{detail}: {command}")


def ensure_installed(ctx: ServiceContext) -> int:
    if _is_k8s(ctx):
        print("[ok] stream-v3 k8s supervisor selected; manifests are managed by kubectl apply -k deploy/k3s/shadow")
        return 0
    missing = [unit for unit in ctx.system_units if not ctx.unit_installed(unit)]
    if not missing:
        print("[ok] stream-new systemd units are already installed")
        return 0
    print("[warn] missing systemd units:")
    for unit in missing:
        print(f"  - {unit}")
    print("Run: stream-new install")
    return 1


def install(ctx: ServiceContext) -> int:
    if _is_k8s(ctx):
        print("[info] k8s install is manifest-driven:")
        print("  python3 ops/scripts/validate_k3s_manifests.py")
        print("  kubectl apply -k deploy/k3s/shadow")
        return 0
    for src_name, dst_name in ctx.install_targets.items():
        src = ctx.systemd_src_dir / src_name
        if not src.exists():
            print(f"[error] missing template: {src}")
            return 1
        ctx.run(["sudo", "-n", "install", "-m", "0644", str(src), f"/etc/systemd/system/{dst_name}"], check=True)
        print(f"[ok] installed /etc/systemd/system/{dst_name}")

    env_src = ctx.systemd_src_dir / "adsb-streamnew.env.example"
    env_dst = Path("/etc/default/adsb-streamnew")
    if env_src.exists() and not env_dst.exists():
        ctx.run(["sudo", "-n", "install", "-m", "0644", str(env_src), str(env_dst)], check=True)
        print(f"[ok] installed {env_dst}")
    elif env_src.exists():
        print(f"[skip] {env_dst} already exists")

    mon_env_src = ctx.systemd_src_dir / "adsb-streamnew-youtube-monitor.env.example"
    mon_env_dst = Path("/etc/default/adsb-streamnew-youtube-monitor")
    if mon_env_src.exists() and not mon_env_dst.exists():
        ctx.run(["sudo", "-n", "install", "-m", "0644", str(mon_env_src), str(mon_env_dst)], check=True)
        print(f"[ok] installed {mon_env_dst}")
    elif mon_env_src.exists():
        print(f"[skip] {mon_env_dst} already exists")

    fr_env_src = ctx.systemd_src_dir / "adsb-streamnew-fast-recovery.env.example"
    fr_env_dst = Path("/etc/default/adsb-streamnew-fast-recovery")
    if fr_env_src.exists() and not fr_env_dst.exists():
        ctx.run(["sudo", "-n", "install", "-m", "0644", str(fr_env_src), str(fr_env_dst)], check=True)
        print(f"[ok] installed {fr_env_dst}")
    elif fr_env_src.exists():
        print(f"[skip] {fr_env_dst} already exists")

    notify_env_src = ctx.systemd_src_dir / "adsb-streamnew-notify.env.example"
    if notify_env_src.exists() and not ctx.notify_env_file.exists():
        ctx.run(["sudo", "-n", "install", "-m", "0600", str(notify_env_src), str(ctx.notify_env_file)], check=True)
        print(f"[ok] installed {ctx.notify_env_file}")
    elif notify_env_src.exists():
        print(f"[skip] {ctx.notify_env_file} already exists")

    ctx.run_systemctl(["daemon-reload"])
    print("[ok] daemon-reload complete")
    return 0


def start(ctx: ServiceContext) -> int:
    if ensure_installed(ctx) != 0:
        return 1
    if _is_k8s(ctx):
        return start_k8s(ctx)
    if ctx.guard_start_safety() != 0:
        return 1
    if ctx.is_active(ctx.dj_service):
        print(f"[skip] {ctx.dj_service} is already active")
    else:
        if not ctx.start_unit(ctx.dj_service):
            return 1

    expected_key = ctx.expected_stream_key()
    if ctx.is_active(ctx.stream_service):
        running_key = ctx.running_stream_key()
        if expected_key and running_key and expected_key != running_key:
            if not ctx.restart_unit(ctx.stream_service, "stream key updated"):
                return 1
        elif ctx.youtube_watchdog_unhealthy():
            if not ctx.restart_unit(ctx.stream_service, "youtube state recovery"):
                return 1
        else:
            print(f"[skip] {ctx.stream_service} is already active")
    else:
        if not ctx.start_unit(ctx.stream_service):
            return 1

    if ctx.is_active(ctx.watchdog_timer):
        print(f"[skip] {ctx.watchdog_timer} is already active")
    else:
        if not ctx.start_unit(ctx.watchdog_timer):
            return 1

    if ctx.is_active(ctx.youtube_monitor_timer):
        if not ctx.restart_unit(ctx.youtube_monitor_timer):
            return 1
    else:
        if not ctx.start_unit(ctx.youtube_monitor_timer):
            return 1
    if ctx.is_active(ctx.youtube_video_resolver_timer):
        if not ctx.restart_unit(ctx.youtube_video_resolver_timer):
            return 1
    else:
        if not ctx.start_unit(ctx.youtube_video_resolver_timer):
            return 1
    if not ctx.trigger_unit(ctx.youtube_video_resolver_service, "oneshot immediate resolve"):
        return 1
    if not ctx.trigger_unit(ctx.youtube_monitor_service, "oneshot immediate check"):
        return 1
    if ctx.is_active(ctx.fast_recovery_timer):
        if not ctx.restart_unit(ctx.fast_recovery_timer):
            return 1
    else:
        if not ctx.start_unit(ctx.fast_recovery_timer):
            return 1
    if not ctx.trigger_unit(ctx.fast_recovery_service, "oneshot immediate check"):
        return 1
    for timer in (
        ctx.stream1090_report_timer,
        ctx.upstream_report_timer,
        ctx.subsystems_status_timer,
        ctx.recovery_orchestrator_timer,
        ctx.memory_status_timer,
        ctx.resource_memory_timer,
    ):
        if ctx.is_active(timer):
            print(f"[skip] {timer} is already active")
        else:
            if not ctx.start_unit(timer):
                return 1
    if ctx.is_active(ctx.notify_timer):
        print(f"[skip] {ctx.notify_timer} is already active")
    else:
        if not ctx.start_unit(ctx.notify_timer):
            return 1
    return 0


def stop(ctx: ServiceContext) -> int:
    if ensure_installed(ctx) != 0:
        return 1
    if _is_k8s(ctx):
        return stop_k8s(ctx)
    for unit in (
        ctx.fast_recovery_timer,
        ctx.fast_recovery_service,
        ctx.stream1090_report_timer,
        ctx.stream1090_report_service,
        ctx.upstream_report_timer,
        ctx.upstream_report_service,
        ctx.subsystems_status_timer,
        ctx.subsystems_status_service,
        ctx.recovery_orchestrator_timer,
        ctx.recovery_orchestrator_service,
        ctx.memory_status_timer,
        ctx.memory_status_service,
        ctx.resource_memory_timer,
        ctx.resource_memory_service,
        ctx.notify_timer,
        ctx.notify_service,
        ctx.youtube_video_resolver_timer,
        ctx.youtube_video_resolver_service,
        ctx.youtube_monitor_timer,
        ctx.youtube_monitor_service,
        ctx.watchdog_timer,
        ctx.watchdog_service,
    ):
        ctx.run_systemctl(["stop", unit], check=False)
    ctx.run_systemctl(["stop", ctx.stream_service], check=False)
    ctx.run_systemctl(["stop", ctx.dj_service], check=False)
    for unit in ctx.all_units:
        ctx.run_systemctl(["disable", unit], check=False)
    still_active: list[str] = [unit for unit in ctx.all_units if ctx.is_active(unit)]
    if still_active:
        print("[error] stop requested, but some units are still active:")
        for unit in still_active:
            print(f"  - {unit}")
        return 1
    print("[ok] stopped stream-new stack and disabled auto-start units")
    return 0


def restart(ctx: ServiceContext) -> int:
    if ensure_installed(ctx) != 0:
        return 1
    if _is_k8s(ctx):
        return restart_k8s(ctx)
    if not ctx.is_active(ctx.dj_service):
        if not ctx.start_unit(ctx.dj_service):
            return 1
    else:
        print(f"[skip] {ctx.dj_service} is already active")
    if not ctx.restart_unit(ctx.stream_service):
        return 1
    if not ctx.restart_unit(ctx.watchdog_timer):
        return 1
    if not ctx.restart_unit(ctx.youtube_monitor_timer):
        return 1
    if not ctx.restart_unit(ctx.youtube_video_resolver_timer):
        return 1
    if not ctx.restart_unit(ctx.fast_recovery_timer):
        return 1
    for timer in (
        ctx.stream1090_report_timer,
        ctx.upstream_report_timer,
        ctx.subsystems_status_timer,
        ctx.recovery_orchestrator_timer,
        ctx.memory_status_timer,
        ctx.resource_memory_timer,
    ):
        if not ctx.restart_unit(timer):
            return 1
    if not ctx.restart_unit(ctx.notify_timer):
        return 1
    print(
        "[ok] restarted stream-new stream/watchdog/youtube-monitor/youtube-video-resolver/fast-recovery/report/shadow/notify timers "
        "(DJ kept running when active)"
    )
    return 0


def enable(ctx: ServiceContext) -> int:
    if ensure_installed(ctx) != 0:
        return 1
    if _is_k8s(ctx):
        print("[ok] k8s workloads are enabled by applied Deployment manifests")
        return 0
    for unit in ctx.all_units:
        if not ctx.enable_unit(unit):
            return 1
    print("[ok] enabled auto-start for stream-new units")
    return 0


def watch(ctx: ServiceContext) -> int:
    if ensure_installed(ctx) != 0:
        return 1
    if _is_k8s(ctx):
        supervisor = _supervisor(ctx)
        result = supervisor.start("deployment/stream-v3-control")
        _print_supervisor_result(result)
        return 0 if result.ok else 1
    cp = ctx.run_systemctl(["enable", "--now", ctx.youtube_monitor_timer], check=False)
    if cp.returncode != 0:
        ctx.print_systemctl_error("enable --now", ctx.youtube_monitor_timer, cp)
        return 1
    if not ctx.is_active(ctx.youtube_monitor_timer):
        print(f"[error] {ctx.youtube_monitor_timer} is not active after enable --now")
        return 1
    if not ctx.trigger_unit(ctx.youtube_monitor_service, "oneshot immediate check"):
        return 1
    print("[ok] enabled and started stream-new youtube monitor")
    return 0


def status(ctx: ServiceContext) -> int:
    if _is_k8s(ctx):
        supervisor = _supervisor(ctx)
        rc = 0
        for target in ctx.k8s_workloads:
            status_item = supervisor.status(target)
            state = "active" if status_item.active else "inactive"
            print(f"{target}: {state} {status_item.detail}".rstrip())
            if not status_item.active:
                rc = 3
        return rc
    cp = ctx.run_systemctl(
        [
            "status",
            ctx.dj_service,
            ctx.stream_service,
            ctx.watchdog_timer,
            ctx.youtube_monitor_timer,
            ctx.youtube_video_resolver_timer,
            ctx.fast_recovery_timer,
            ctx.stream1090_report_timer,
            ctx.upstream_report_timer,
            ctx.subsystems_status_timer,
            ctx.recovery_orchestrator_timer,
            ctx.memory_status_timer,
            ctx.resource_memory_timer,
            ctx.notify_timer,
            "--no-pager",
        ],
        check=False,
    )
    if cp.stdout:
        print(cp.stdout, end="")
    if cp.stderr:
        print(cp.stderr, end="", file=sys.stderr)
    return 0 if cp.returncode in (0, 3) else cp.returncode


def logs(ctx: ServiceContext, lines: int) -> int:
    cp = ctx.run(
        [
            "journalctl",
            "-u",
            ctx.dj_service,
            "-u",
            ctx.stream_service,
            "-u",
            ctx.watchdog_service,
            "-u",
            ctx.youtube_monitor_service,
            "-u",
            ctx.youtube_video_resolver_service,
            "-u",
            ctx.fast_recovery_service,
            "-u",
            ctx.stream1090_report_service,
            "-u",
            ctx.upstream_report_service,
            "-u",
            ctx.subsystems_status_service,
            "-u",
            ctx.recovery_orchestrator_service,
            "-u",
            ctx.memory_status_service,
            "-u",
            ctx.resource_memory_service,
            "-u",
            ctx.notify_service,
            "-n",
            str(lines),
            "--no-pager",
        ],
        check=False,
    )
    if cp.stdout:
        print(cp.stdout, end="")
    if cp.stderr:
        print(cp.stderr, end="", file=sys.stderr)
    return 0


def start_k8s(ctx: ServiceContext) -> int:
    if ctx.guard_start_safety() != 0:
        return 1
    supervisor = _supervisor(ctx)
    for target in ctx.k8s_workloads:
        result = supervisor.start(target)
        _print_supervisor_result(result)
        if not result.ok:
            return 1
    return 0


def stop_k8s(ctx: ServiceContext) -> int:
    supervisor = _supervisor(ctx)
    for target in reversed(ctx.k8s_workloads):
        result = supervisor.stop(target)
        _print_supervisor_result(result)
        if not result.ok:
            return 1
    return 0


def restart_k8s(ctx: ServiceContext) -> int:
    supervisor = _supervisor(ctx)
    for target in ctx.k8s_workloads:
        result = supervisor.restart(target, reason="stream service restart")
        _print_supervisor_result(result)
        if not result.ok:
            return 1
    return 0
