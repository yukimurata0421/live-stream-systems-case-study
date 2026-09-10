from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
import uuid
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.time import isoformat_utc, utc_now

CommandRunner = Callable[[list[str]], str]


def _run(command: list[str]) -> str:
    completed = subprocess.run(command, check=True, capture_output=True, text=True, timeout=5)
    return completed.stdout


def _atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    data = json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n"
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


class AtomicKubernetesTargetSnapshotProducer:
    """Build an 8-field identity using only Kubernetes GET and host /proc reads."""

    def __init__(
        self,
        *,
        host_id: str,
        namespace: str,
        label_selector: str,
        container_name: str = "stream-engine",
        kubectl_bin: str = "kubectl",
        kubeconfig: Path | None = None,
        proc_root: Path = Path("/proc"),
        ttl_seconds: float = 10.0,
        runner: CommandRunner = _run,
    ) -> None:
        self.host_id = host_id
        self.namespace = namespace
        self.label_selector = label_selector
        self.container_name = container_name
        self.kubectl_bin = kubectl_bin
        self.kubeconfig = kubeconfig
        self.proc_root = proc_root
        self.ttl_seconds = ttl_seconds
        self.runner = runner

    def _pods(self) -> dict[str, Any]:
        command = [self.kubectl_bin]
        if self.kubeconfig is not None:
            command.extend(["--kubeconfig", str(self.kubeconfig)])
        command.extend(["-n", self.namespace, "get", "pods", "-l", self.label_selector, "-o", "json"])
        value = json.loads(self.runner(command))
        items = value.get("items", [])
        if not isinstance(items, list) or len(items) != 1:
            raise ValueError("TARGET_POD_CARDINALITY")
        return dict(items[0])

    def _pod_identity(self, pod: dict[str, Any]) -> tuple[str, str, str, bool, str]:
        metadata = dict(pod.get("metadata") or {})
        statuses = list(dict(pod.get("status") or {}).get("containerStatuses") or [])
        matches = [dict(item) for item in statuses if item.get("name") == self.container_name]
        if len(matches) != 1 or not matches[0].get("containerID"):
            raise ValueError("TARGET_CONTAINER_UNAVAILABLE")
        running = dict(dict(matches[0].get("state") or {}).get("running") or {})
        started_at = str(running.get("startedAt") or "").strip()
        if not started_at:
            raise ValueError("RUNTIME_CONTAINER_STARTED_AT_UNAVAILABLE")
        return (
            str(metadata["uid"]),
            str(metadata["resourceVersion"]),
            str(matches[0]["containerID"]),
            matches[0].get("ready") is True,
            started_at,
        )

    @staticmethod
    def _start_ticks(stat_text: str) -> str:
        closing = stat_text.rfind(")")
        if closing < 0:
            raise ValueError("INVALID_PROC_STAT")
        fields_after_comm = stat_text[closing + 2 :].split()
        if len(fields_after_comm) <= 19:
            raise ValueError("INVALID_PROC_STAT")
        return fields_after_comm[19]

    def _ffmpeg(self, container_id: str) -> tuple[int, str]:
        bare = container_id.split("://", 1)[-1]
        matches: list[tuple[int, str]] = []
        for item in self.proc_root.iterdir():
            if not item.name.isdigit():
                continue
            try:
                cgroup = (item / "cgroup").read_text(encoding="utf-8", errors="replace")
                if bare not in cgroup:
                    continue
                command = (item / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", errors="replace")
                if not command.startswith("ffmpeg ") or " rtmps://" not in command or " -f flv " not in command:
                    continue
                ticks = self._start_ticks((item / "stat").read_text(encoding="utf-8"))
                matches.append((int(item.name), ticks))
            except (OSError, ValueError):
                continue
        if len(matches) != 1:
            raise ValueError("FFMPEG_PROCESS_CARDINALITY")
        return matches[0]

    def collect(self) -> dict[str, Any]:
        started = utc_now()
        status = "VALID"
        reason = "SNAPSHOT_CONSISTENT"
        target: dict[str, Any] | None = None
        runtime_status = "INVALID"
        runtime_reason = "RUNTIME_OBSERVATION_UNAVAILABLE"
        runtime_identity: dict[str, str] | None = None
        runtime_container_ready: bool | None = None
        runtime_snapshot_id = f"runtime-snapshot-{uuid.uuid4()}"
        source_values: tuple[str, ...] = ()
        try:
            before = self._pods()
            pod_uid, resource_version, container_id, container_ready, container_started_at = self._pod_identity(before)
            ffmpeg_error: Exception | None = None
            try:
                pid, start_ticks = self._ffmpeg(container_id)
            except (OSError, ValueError) as exc:
                ffmpeg_error = exc
                pid, start_ticks = 0, ""
            after = self._pods()
            after_values = self._pod_identity(after)
            if (pod_uid, resource_version, container_id, container_started_at) != (
                after_values[0],
                after_values[1],
                after_values[2],
                after_values[4],
            ):
                raise RuntimeError("RUNTIME_SNAPSHOT_UNSTABLE")
            generation_input = ":".join((pod_uid, container_id, container_started_at))
            runtime_identity = {
                "host_id": self.host_id,
                "host_boot_id": Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip(),
                "namespace": self.namespace,
                "pod_uid": pod_uid,
                "stream_engine_container_name": self.container_name,
                "stream_engine_container_id": container_id,
                "runtime_generation": f"runtime-{hashlib.sha256(generation_input.encode()).hexdigest()[:32]}",
            }
            runtime_status = "VALID"
            runtime_reason = "RUNTIME_SNAPSHOT_CONSISTENT"
            runtime_container_ready = container_ready
            if ffmpeg_error is not None:
                raise ffmpeg_error
            if not container_ready or not after_values[3]:
                raise ValueError("TARGET_CONTAINER_NOT_READY")
            pid_after, ticks_after = self._ffmpeg(after_values[2])
            source_values = (
                pod_uid,
                resource_version,
                container_id,
                container_started_at,
                str(pid),
                start_ticks,
                after_values[0],
                after_values[1],
                after_values[2],
                str(after_values[3]),
                after_values[4],
                str(pid_after),
                ticks_after,
            )
            if (pod_uid, resource_version, container_id) != after_values[:3] or (pid, start_ticks) != (
                pid_after,
                ticks_after,
            ):
                raise RuntimeError("SNAPSHOT_UNSTABLE")
            ffmpeg_generation_input = ":".join((pod_uid, container_id, str(pid), start_ticks))
            target = TargetIdentity(
                host_id=self.host_id,
                host_boot_id=str(runtime_identity["host_boot_id"]),
                namespace=self.namespace,
                pod_uid=pod_uid,
                container_name=self.container_name,
                container_id=container_id,
                ffmpeg_generation=f"ffmpeg-{hashlib.sha256(ffmpeg_generation_input.encode()).hexdigest()[:32]}",
                ffmpeg_pid=pid,
            ).to_dict()
        except RuntimeError as exc:
            status = "INVALID"
            runtime_reason = str(exc)
            reason = "SNAPSHOT_UNSTABLE" if str(exc) == "RUNTIME_SNAPSHOT_UNSTABLE" else str(exc)
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError, KeyError, ValueError) as exc:
            status = "INVALID"
            reason = str(exc) if str(exc).isupper() else "TARGET_OBSERVATION_UNAVAILABLE"
        finished = utc_now()
        revision_input = "|".join(source_values) if source_values else f"{reason}|{isoformat_utc(started)}"
        return {
            "schema": "cra_dell_recovery.target_snapshot.v1",
            "snapshot_id": f"snapshot-{uuid.uuid4()}",
            "observed_at": isoformat_utc(finished),
            "valid_until": isoformat_utc(finished + timedelta(seconds=self.ttl_seconds)),
            "source_revision": hashlib.sha256(revision_input.encode()).hexdigest(),
            "read_started_at": isoformat_utc(started),
            "read_finished_at": isoformat_utc(finished),
            "status": status,
            "reason_code": reason,
            "target_identity": target,
            "runtime_snapshot_id": runtime_snapshot_id,
            "runtime_status": runtime_status,
            "runtime_reason_code": runtime_reason,
            "runtime_identity": runtime_identity,
            "runtime_container_ready": runtime_container_ready,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="read-only stream-v3 target snapshot producer")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--host-id", required=True)
    parser.add_argument("--namespace", default="stream-v3")
    parser.add_argument("--label-selector", default="app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime")
    parser.add_argument("--container-name", default="stream-engine")
    parser.add_argument("--kubectl-bin", default="kubectl")
    parser.add_argument("--kubeconfig", type=Path)
    parser.add_argument("--ttl-seconds", type=float, default=10.0)
    parser.add_argument("--interval-seconds", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    producer = AtomicKubernetesTargetSnapshotProducer(
        host_id=args.host_id,
        namespace=args.namespace,
        label_selector=args.label_selector,
        container_name=args.container_name,
        kubectl_bin=args.kubectl_bin,
        kubeconfig=args.kubeconfig,
        ttl_seconds=args.ttl_seconds,
    )
    while True:
        snapshot = producer.collect()
        _atomic_write(args.output, snapshot)
        _append_jsonl(args.journal, snapshot)
        if args.once:
            return
        time.sleep(max(0.2, args.interval_seconds))


if __name__ == "__main__":
    main()
