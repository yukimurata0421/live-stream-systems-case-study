from __future__ import annotations

import os
import subprocess
from typing import Callable, Mapping

from .k8s import K8sSupervisor, default_run_command
from .systemd import SystemdSupervisor


RunCommand = Callable[[list[str]], subprocess.CompletedProcess[str]]
RunSystemctl = Callable[[list[str], bool], subprocess.CompletedProcess[str]]


STREAM_V3_K8S_TARGET_MAP = {
    "adsb-streamnew-youtube-stream.service": "deployment/stream-v3-runtime",
    "adsb-streamnew-auto-dj.service": "deployment/stream-v3-runtime",
    "adsb-streamnew-network-observer.service": "deployment/stream-v3-runtime",
    "adsb-streamnew-watchdog.timer": "deployment/stream-v3-control",
    "adsb-streamnew-watchdog.service": "deployment/stream-v3-control",
    "adsb-streamnew-youtube-monitor.timer": "deployment/stream-v3-control",
    "adsb-streamnew-youtube-monitor.service": "deployment/stream-v3-control",
    "adsb-streamnew-youtube-video-resolver.timer": "deployment/stream-v3-control",
    "adsb-streamnew-youtube-video-resolver.service": "deployment/stream-v3-control",
    "adsb-streamnew-fast-recovery.timer": "deployment/stream-v3-control",
    "adsb-streamnew-fast-recovery.service": "deployment/stream-v3-control",
    "adsb-streamnew-notify.timer": "deployment/stream-v3-control",
    "adsb-streamnew-notify.service": "deployment/stream-v3-control",
    "adsb-streamnew-subsystems-status.timer": "deployment/stream-v3-control",
    "adsb-streamnew-subsystems-status.service": "deployment/stream-v3-control",
    "adsb-streamnew-recovery-orchestrator.timer": "deployment/stream-v3-control",
    "adsb-streamnew-recovery-orchestrator.service": "deployment/stream-v3-control",
    "adsb-streamnew-stream1090-report.timer": "deployment/stream-v3-control",
    "adsb-streamnew-stream1090-report.service": "deployment/stream-v3-control",
    "adsb-streamnew-upstream-report.timer": "deployment/stream-v3-control",
    "adsb-streamnew-upstream-report.service": "deployment/stream-v3-control",
    "adsb-streamnew-memory-status.timer": "deployment/stream-v3-control",
    "adsb-streamnew-memory-status.service": "deployment/stream-v3-control",
    "adsb-streamnew-resource-memory.timer": "deployment/stream-v3-control",
    "adsb-streamnew-resource-memory.service": "deployment/stream-v3-control",
    "adsb-streamnew-prometheus-exporter.service": "deployment/stream-v3-observer",
    "adsb-streamnew-youtube-api-cost-open-day-report.timer": "cronjob/stream-v3-youtube-api-cost-open-day",
    "adsb-streamnew-youtube-api-cost-open-day-report.service": "cronjob/stream-v3-youtube-api-cost-open-day",
    "adsb-streamnew-youtube-api-cost-report.timer": "cronjob/stream-v3-youtube-api-cost-closed-day",
    "adsb-streamnew-youtube-api-cost-report.service": "cronjob/stream-v3-youtube-api-cost-closed-day",
}


def build_runtime_supervisor(
    *,
    env: Mapping[str, str] | None = None,
    run_systemctl: RunSystemctl,
    run_command: RunCommand = default_run_command,
):
    source = os.environ if env is None else env
    mode = source.get("STREAM_RUNTIME_SUPERVISOR", "systemd").strip().lower()
    if mode in {"k8s", "k3s", "kubernetes"}:
        dry_run_default = "0" if truthy(source.get("STREAM_V3_CUTOVER_ENABLE", "")) else "1"
        return K8sSupervisor(
            namespace=source.get("STREAM_K8S_NAMESPACE", "stream-v3"),
            kubectl_bin=source.get("STREAM_KUBECTL_BIN", "kubectl"),
            dry_run=truthy(source.get("STREAM_K8S_DRY_RUN", dry_run_default)),
            target_map=STREAM_V3_K8S_TARGET_MAP,
            run_command=run_command,
        )
    if mode not in {"", "systemd"}:
        raise ValueError(f"unsupported STREAM_RUNTIME_SUPERVISOR={mode!r}")
    return SystemdSupervisor(run_systemctl=run_systemctl)


def truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}
