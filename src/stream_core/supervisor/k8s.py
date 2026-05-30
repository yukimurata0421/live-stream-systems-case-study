from __future__ import annotations

import json
import subprocess
import time
from typing import Callable, Sequence

from .model import SupervisorResult, WorkloadStatus


RunCommand = Callable[[list[str]], subprocess.CompletedProcess[str]]


def default_run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, check=False)


class K8sSupervisor:
    """Runtime supervisor adapter for stream_v3 k3s workloads."""

    def __init__(
        self,
        *,
        namespace: str = "stream-v3",
        kubectl_bin: str = "kubectl",
        dry_run: bool = False,
        target_map: dict[str, str] | None = None,
        run_command: RunCommand = default_run_command,
    ) -> None:
        self.namespace = namespace
        self.kubectl_bin = kubectl_bin
        self.dry_run = dry_run
        self.target_map = target_map or {}
        self.run_command = run_command

    def status(self, target: str) -> WorkloadStatus:
        mapped = self.target(target)
        command = self._kubectl("get", mapped, "-o", "json")
        if self.dry_run:
            return WorkloadStatus(target=target, active=False, detail=f"dry-run target={mapped}", raw=" ".join(command))
        cp = self.run_command(command)
        raw = (cp.stdout or cp.stderr or "").strip()
        if cp.returncode != 0:
            return WorkloadStatus(target=target, active=False, detail=raw, raw=raw)
        return self._parse_status(target, raw)

    def start(self, target: str) -> SupervisorResult:
        return self._scale(target, replicas=1, action="start")

    def stop(self, target: str) -> SupervisorResult:
        return self._scale(target, replicas=0, action="stop")

    def restart(self, target: str, *, reason: str = "") -> SupervisorResult:
        mapped = self.target(target)
        command = self._kubectl("rollout", "restart", mapped)
        detail = f"reason={reason}" if reason else ""
        return self._run("restart", target, command, detail=_detail_with_target(detail, mapped, target))

    def start_once(self, target: str, *, reason: str = "") -> SupervisorResult:
        mapped = self.target(target)
        if not mapped.startswith("cronjob/"):
            return SupervisorResult(
                action="start_once",
                target=target,
                ok=False,
                detail=f"k8s start_once requires cronjob/<name>; mapped_target={mapped}",
            )
        name = mapped.split("/", 1)[1]
        suffix = time.strftime("%Y%m%d%H%M%S", time.gmtime())
        command = self._kubectl("create", "job", f"{name}-manual-{suffix}", f"--from={mapped}")
        detail = f"reason={reason}" if reason else ""
        return self._run("start_once", target, command, detail=_detail_with_target(detail, mapped, target))

    def _scale(self, target: str, *, replicas: int, action: str) -> SupervisorResult:
        mapped = self.target(target)
        command = self._kubectl("scale", mapped, f"--replicas={replicas}")
        return self._run(action, target, command, detail=_detail_with_target("", mapped, target))

    def target(self, target: str) -> str:
        return self.target_map.get(target, target)

    def _kubectl(self, *args: str) -> list[str]:
        return [self.kubectl_bin, "-n", self.namespace, *args]

    def _run(self, action: str, target: str, command: Sequence[str], *, detail: str = "") -> SupervisorResult:
        if self.dry_run:
            return SupervisorResult.planned(action=action, target=target, command=command, detail=detail)
        cp = self.run_command(list(command))
        return SupervisorResult.from_completed(
            action=action,
            target=target,
            command=command,
            completed=cp,
            ok=cp.returncode == 0,
            detail=detail,
        )

    def _parse_status(self, target: str, raw: str) -> WorkloadStatus:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return WorkloadStatus(target=target, active=False, detail="invalid json", raw=raw)
        if not isinstance(payload, dict):
            return WorkloadStatus(target=target, active=False, detail="unexpected json", raw=raw)

        kind = str(payload.get("kind") or "").lower()
        status = payload.get("status") if isinstance(payload.get("status"), dict) else {}
        spec = payload.get("spec") if isinstance(payload.get("spec"), dict) else {}

        if kind == "deployment":
            desired = _int(spec.get("replicas"), default=1)
            available = _int(status.get("availableReplicas"), default=0)
            ready = _int(status.get("readyReplicas"), default=0)
            active = desired > 0 and available >= desired and ready >= desired
            return WorkloadStatus(
                target=target,
                active=active,
                detail=f"desired={desired} ready={ready} available={available}",
                raw=raw,
            )
        if kind in {"job", "pod"}:
            phase = str(status.get("phase") or "")
            active = phase in {"Running", "Succeeded"}
            return WorkloadStatus(target=target, active=active, detail=f"phase={phase}", raw=raw)
        conditions = status.get("conditions") if isinstance(status.get("conditions"), list) else []
        active = any(
            isinstance(item, dict)
            and str(item.get("type") or "") in {"Ready", "Available"}
            and str(item.get("status") or "") == "True"
            for item in conditions
        )
        return WorkloadStatus(target=target, active=active, detail=f"kind={kind or 'unknown'}", raw=raw)


def _int(value: object, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _detail_with_target(detail: str, mapped: str, original: str) -> str:
    target_detail = f"mapped_target={mapped}" if mapped != original else ""
    return " ".join(part for part in (detail, target_detail) if part)
