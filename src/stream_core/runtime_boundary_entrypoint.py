from __future__ import annotations

import argparse
import errno
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

STREAM_CORE_DIR = Path(__file__).resolve().parent
if str(STREAM_CORE_DIR) not in sys.path:
    sys.path.insert(0, str(STREAM_CORE_DIR))

from runtime_boundary import (
    EXECUTOR_OWNED_INTENTS,
    RECONCILE_FFMPEG,
    RESTART_FFMPEG,
    EffectExecutorServer,
    EffectLedger,
    EffectRejected,
    OutcomeUnknown,
    RuntimeSnapshotReader,
    TargetSnapshotReader,
)
from runtime_boundary.child_registry import ChildLifecycleRegistry
from runtime_boundary.observation import RuntimeObservationPublisher

from maintenance_audit import audit_maintenance_decision
from stream_core.engine.config import load_config
from stream_core.stream_engine import StreamEngine


class RuntimeBoundaryStreamEngine(StreamEngine):
    """Add the narrow MP-03 effect boundary without replacing MP-10 behavior."""

    def __init__(self, cfg: Any) -> None:
        super().__init__(cfg)
        self.child_registry = ChildLifecycleRegistry()
        self.child_registry.install()
        self.effect_ledger: EffectLedger | None = None
        self.effect_executor: EffectExecutorServer | None = None
        self.runtime_observation_publisher: RuntimeObservationPublisher | None = None
        self.runtime_boundary_instance_id = f"stream-engine-executor-{self.run_id}"
        self._observed_ffmpeg_pid = 0
        self._observed_ffmpeg_started_monotonic = 0.0
        self._reconcile_wakeup = threading.Event()

    def acquire_capture_lock(self) -> None:
        super().acquire_capture_lock()
        self.start_runtime_boundary()

    def runtime_boundary_process_snapshot(self) -> dict[str, object]:
        proc = self.ffmpeg_proc
        running = proc is not None and proc.poll() is None
        pid = int(proc.pid) if running and proc is not None else 0
        if pid != self._observed_ffmpeg_pid:
            self._observed_ffmpeg_pid = pid
            self._observed_ffmpeg_started_monotonic = time.monotonic() if pid > 1 else 0.0
        uptime = (
            max(0, int(time.monotonic() - self._observed_ffmpeg_started_monotonic))
            if pid > 1 and self._observed_ffmpeg_started_monotonic > 0
            else 0
        )
        readiness_min = max(0, int(os.environ.get("STREAM_READINESS_MIN_FFMPEG_UPTIME_SEC", "15") or 15))
        authority = self.effect_ledger.authority() if self.effect_ledger is not None else {}
        child_cardinality, child_pids = self.managed_ffmpeg_child_cardinality()
        return {
            "local_ffmpeg_pid": pid,
            "ffmpeg_generation": self.ffmpeg_generation(pid) if pid > 1 else "",
            "ffmpeg_uptime_sec": uptime,
            "ffmpeg_running": running,
            "pod_uid": os.environ.get("STREAM_V3_POD_UID", ""),
            "pod_name": os.environ.get("STREAM_V3_POD_NAME", ""),
            "stream_established": running and uptime >= readiness_min,
            "active_producer_id": str(authority.get("producer_id") or ""),
            "active_producer_generation": int(authority.get("producer_generation") or 0),
            "authority_version": int(authority.get("authority_version") or 0),
            "in_flight_count": self.effect_ledger.unresolved_count() if self.effect_ledger is not None else 0,
            "runtime_lifecycle_state": self.ffmpeg_lifecycle_state,
            "managed_child_cardinality": child_cardinality,
            "managed_child_pids": child_pids,
        }

    def managed_ffmpeg_child_cardinality(self) -> tuple[str, list[int]]:
        """Observe only direct, delivery-shaped FFmpeg children owned by this engine."""

        try:
            children = Path(f"/proc/{os.getpid()}/task/{os.getpid()}/children").read_text(encoding="utf-8")
        except OSError:
            return "UNKNOWN", []
        managed: set[int] = set()
        for raw_pid in children.split():
            try:
                pid = int(raw_pid)
                command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(
                    "utf-8", errors="replace"
                )
            except (OSError, ValueError):
                continue
            if command.startswith("ffmpeg ") and (" rtmp://" in command or " rtmps://" in command):
                managed.add(pid)
        proc = self.ffmpeg_proc
        if proc is not None and proc.poll() is None:
            managed.add(int(proc.pid))
        if len(managed) >= 2:
            return "2+", sorted(managed)
        return str(len(managed)), sorted(managed)

    def wait_for_ffmpeg_restart_delay(self, delay_sec: float) -> None:
        self._reconcile_wakeup.wait(timeout=max(0.0, delay_sec))
        self._reconcile_wakeup.clear()

    def runtime_boundary_current_target(self) -> dict[str, object]:
        snapshot_path = Path(os.environ["FR_TARGET_SNAPSHOT_FILE"])
        decision = TargetSnapshotReader(snapshot_path).read()
        runtime_decision = RuntimeSnapshotReader(snapshot_path).read()
        process = self.runtime_boundary_process_snapshot()
        if not runtime_decision.available or runtime_decision.runtime_identity is None:
            return {}
        runtime_identity = runtime_decision.runtime_identity
        if str(runtime_identity.get("pod_uid") or "") != str(process["pod_uid"]):
            return {}
        current: dict[str, object] = {
            "runtime_identity": runtime_identity,
            "runtime_snapshot_id": runtime_decision.snapshot_id,
            "executor_instance_id": self.runtime_boundary_instance_id,
            "runtime_lifecycle_state": process["runtime_lifecycle_state"],
            "managed_child_cardinality": process["managed_child_cardinality"],
        }
        if decision.available and decision.target_identity is not None and process["ffmpeg_running"]:
            target = decision.target_identity
            if str(target.get("pod_uid") or "") == str(process["pod_uid"]):
                current.update(
                    {
                        "target_identity": target,
                        "ffmpeg_target_identity": target,
                        "target_snapshot_id": decision.snapshot_id,
                        "ffmpeg_generation": process["ffmpeg_generation"],
                    }
                )
        return current

    def audit_mp03_effect_boundary(self, request: Any) -> None:
        count = self.effect_ledger.unresolved_count() if self.effect_ledger is not None else -1
        audit_maintenance_decision(
            path_id="MP-03",
            phase="EFFECT_BOUNDARY",
            operation=request.operation,
            path_role="NORMAL_MUTATOR",
            process_service="stream-engine-effect-executor",
            resource_identity=(
                f"ffmpeg/host-pid/{int(request.target_identity['ffmpeg_pid'])}"
                if request.intent_type == RESTART_FFMPEG
                else f"runtime/pod/{request.runtime_identity['pod_uid']}"
            ),
            correlation_id=request.correlation_id,
            target_identity=request.target_identity,
            in_flight_evidence={
                "status": "CONFIRMED",
                "count": count,
                "source": "stream-engine durable Effect Executor ledger",
            },
            generation_evidence={
                "status": "CONFIRMED",
                "producer_id": request.producer_id,
                "producer_generation": request.producer_generation,
                "native_ffmpeg_generation": request.expected_ffmpeg_generation,
                "target_snapshot_id": request.target_snapshot_id,
                "runtime_snapshot_id": request.runtime_snapshot_id,
                "intent_type": request.intent_type,
            },
            p2_disabled_evaluation=True,
            native_operation_id=request.request_id,
            native_operation_generation=request.expected_ffmpeg_generation,
            actual_production_decision="RUNTIME_OWNER_EFFECT_CALL_PROCEEDS",
        )

    def perform_fast_recovery_effect(self, request: Any) -> dict[str, object]:
        intent_type = getattr(request, "intent_type", RESTART_FFMPEG)
        if intent_type == RECONCILE_FFMPEG:
            return self.reconcile_ffmpeg(request)
        if intent_type != RESTART_FFMPEG:
            raise EffectRejected("INTENT_NOT_OWNED_BY_EXECUTOR")
        proc = self.ffmpeg_proc
        if proc is None or proc.poll() is not None:
            raise EffectRejected("FFMPEG_NOT_RUNNING_AT_EFFECT_BOUNDARY")
        if self.ffmpeg_generation(int(proc.pid)) != request.expected_ffmpeg_generation:
            raise EffectRejected("FFMPEG_GENERATION_DRIFT_AT_EFFECT_BOUNDARY")
        term_grace_sec = self._bounded_effect_wait("FR_FFMPEG_TERM_GRACE_SEC", default=2.0)
        kill_wait_sec = self._bounded_effect_wait("FR_FFMPEG_KILL_WAIT_SEC", default=1.0)
        force_kill_enabled = os.environ.get("FR_FFMPEG_FORCE_KILL_ENABLED", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        local_pid = int(proc.pid)
        pidfd = self._open_exact_ffmpeg_pidfd(proc)
        self.ffmpeg_stop_context = {
            "termination_initiator": request.producer_id,
            "controller_id": request.producer_id,
            "stop_reason": request.reason,
            "requested_signal": "SIGTERM",
            "recovery_action_id": request.request_id,
            "idempotency_key": request.idempotency_key,
            "recovery_scope": "stream_engine_effect_executor",
            "physical_effect_count": 1,
            "signal_attempt_count": 0,
            "force_kill_enabled": force_kill_enabled,
            "pidfd_used": pidfd is not None,
        }
        result: dict[str, object] = {
            "physical_effect_count": 1,
            "effect": "SIGTERM",
            "local_ffmpeg_pid": local_pid,
            "protocol_host_ffmpeg_pid": int(request.target_identity["ffmpeg_pid"]),
            "expected_ffmpeg_generation": request.expected_ffmpeg_generation,
            "signal_attempt_count": 0,
            "signals_requested": [],
            "exit_observed": False,
            "process_still_running_after_dispatch": None,
            "force_kill_enabled": force_kill_enabled,
            "force_kill_used": False,
            "pidfd_used": pidfd is not None,
            "term_grace_sec": term_grace_sec,
            "kill_wait_sec": kill_wait_sec,
        }
        try:
            self._send_exact_ffmpeg_signal(proc, pidfd, signal.SIGTERM)
            result["signal_attempt_count"] = 1
            result["signals_requested"] = ["SIGTERM"]
            self.ffmpeg_stop_context.update(
                {
                    "signal_attempt_count": 1,
                    "signals_requested": ["SIGTERM"],
                }
            )
            try:
                exit_code = int(proc.wait(timeout=term_grace_sec))
            except subprocess.TimeoutExpired:
                exit_code = None
            if exit_code is not None:
                result.update(
                    {
                        "exit_code": exit_code,
                        "exit_observed": True,
                        "process_still_running_after_dispatch": False,
                    }
                )
                return result
            result["process_still_running_after_dispatch"] = True
            if not force_kill_enabled:
                raise OutcomeUnknown("SIGTERM_SENT_EXIT_NOT_OBSERVED", result=result)
            if pidfd is None:
                raise OutcomeUnknown("SIGKILL_REQUIRES_PIDFD", result=result)
            if self.ffmpeg_proc is not proc or proc.poll() is not None:
                raise OutcomeUnknown("FFMPEG_REFERENCE_DRIFT_BEFORE_SIGKILL", result=result)
            if self.ffmpeg_generation(local_pid) != request.expected_ffmpeg_generation:
                raise OutcomeUnknown("FFMPEG_GENERATION_DRIFT_BEFORE_SIGKILL", result=result)
            result.update(
                {
                    "effect": "SIGTERM_THEN_SIGKILL",
                    "signal_attempt_count": 2,
                    "signals_requested": ["SIGTERM", "SIGKILL"],
                    "force_kill_used": True,
                }
            )
            self.ffmpeg_stop_context.update(
                {
                    "requested_signal": "SIGTERM_THEN_SIGKILL",
                    "signal_attempt_count": 2,
                    "signals_requested": ["SIGTERM", "SIGKILL"],
                    "force_kill_used": True,
                }
            )
            self._send_exact_ffmpeg_signal(proc, pidfd, signal.SIGKILL)
            try:
                exit_code = int(proc.wait(timeout=kill_wait_sec))
            except subprocess.TimeoutExpired:
                exit_code = None
            if exit_code is None:
                raise OutcomeUnknown("SIGKILL_SENT_EXIT_NOT_OBSERVED", result=result)
            result.update(
                {
                    "exit_code": exit_code,
                    "exit_observed": True,
                    "process_still_running_after_dispatch": False,
                }
            )
            return result
        finally:
            if pidfd is not None:
                os.close(pidfd)

    @staticmethod
    def _bounded_effect_wait(name: str, *, default: float) -> float:
        raw = os.environ.get(name, str(default)).strip()
        try:
            value = float(raw)
        except ValueError as exc:
            raise EffectRejected(f"{name}_INVALID") from exc
        if value < 0.001 or value > 10.0:
            raise EffectRejected(f"{name}_OUT_OF_RANGE")
        return value

    @staticmethod
    def _open_exact_ffmpeg_pidfd(proc: subprocess.Popen[Any]) -> int | None:
        open_pidfd = getattr(os, "pidfd_open", None)
        send_pidfd_signal = getattr(signal, "pidfd_send_signal", None)
        if not callable(open_pidfd) or not callable(send_pidfd_signal):
            return None
        try:
            return int(open_pidfd(int(proc.pid)))
        except OSError as exc:
            if exc.errno in {errno.ENOSYS, errno.EINVAL}:
                return None
            result = {
                "physical_effect_count": 0,
                "signal_attempt_count": 0,
                "local_ffmpeg_pid": int(proc.pid),
                "exit_observed": proc.poll() is not None,
                "process_still_running_after_dispatch": proc.poll() is None,
                "pidfd_used": False,
                "pidfd_error_errno": exc.errno,
            }
            raise OutcomeUnknown("PIDFD_OPEN_FAILED_FAIL_CLOSED", result=result) from exc

    @staticmethod
    def _send_exact_ffmpeg_signal(proc: subprocess.Popen[Any], pidfd: int | None, signum: int) -> None:
        if pidfd is not None:
            signal.pidfd_send_signal(pidfd, signum, None, 0)
        elif signum == signal.SIGTERM:
            proc.terminate()
        elif signum == signal.SIGKILL:
            proc.kill()
        else:
            raise ValueError("UNSUPPORTED_FFMPEG_SIGNAL")

    def ffmpeg_exit_evidence(
        self,
        *,
        ffmpeg_pid: int,
        exit_code: int,
        ffmpeg_uptime_sec: int,
        stderr_summary: dict[str, object],
    ) -> dict[str, object]:
        evidence = super().ffmpeg_exit_evidence(
            ffmpeg_pid=ffmpeg_pid,
            exit_code=exit_code,
            ffmpeg_uptime_sec=ffmpeg_uptime_sec,
            stderr_summary=stderr_summary,
        )
        local_context = dict(self.ffmpeg_stop_context)
        for name in ("recovery_action_id", "idempotency_key", "recovery_scope"):
            if not evidence.get(name) and local_context.get(name):
                evidence[name] = str(local_context[name])
        if not evidence.get("recovery_context") and local_context.get("recovery_action_id"):
            evidence["recovery_context"] = local_context
        evidence["signal_attempt_count"] = int(local_context.get("signal_attempt_count", 0) or 0)
        evidence["signals_requested"] = list(local_context.get("signals_requested", []))
        evidence["force_kill_enabled"] = bool(local_context.get("force_kill_enabled", False))
        evidence["force_kill_used"] = bool(local_context.get("force_kill_used", False))
        evidence["pidfd_used"] = bool(local_context.get("pidfd_used", False))
        return evidence

    def reconcile_ffmpeg(self, request: Any) -> dict[str, object]:
        cardinality, child_pids = self.managed_ffmpeg_child_cardinality()
        if cardinality == "1":
            return {
                "physical_effect_count": 0,
                "effect": "ALREADY_RECONCILED",
                "managed_child_pids": child_pids,
            }
        if cardinality == "2+":
            raise EffectRejected("MULTIPLE_MANAGED_FFMPEG_CHILDREN")
        if cardinality == "UNKNOWN":
            raise EffectRejected("MANAGED_CHILD_CARDINALITY_UNKNOWN")
        if self.ffmpeg_lifecycle_state != "RESTART_DELAY":
            raise EffectRejected(f"RUNTIME_NOT_RECONCILABLE_{self.ffmpeg_lifecycle_state}")
        self._reconcile_wakeup.set()
        reconcile_timeout_sec = max(
            1.0,
            self._bounded_effect_wait("FR_RECONCILE_OBSERVE_TIMEOUT_SEC", default=5.0),
        )
        deadline = time.monotonic() + reconcile_timeout_sec
        while time.monotonic() < deadline:
            cardinality, child_pids = self.managed_ffmpeg_child_cardinality()
            if cardinality == "1" and self.ffmpeg_lifecycle_state == "FFMPEG_RUNNING":
                return {
                    "physical_effect_count": 1,
                    "effect": "RESTART_DELAY_RELEASED_AND_CHILD_OBSERVED",
                    "managed_child_pids": child_pids,
                }
            if cardinality in {"2+", "UNKNOWN"}:
                raise OutcomeUnknown(f"RECONCILE_CHILD_CARDINALITY_{cardinality}")
            time.sleep(0.02)
        raise OutcomeUnknown("RECONCILE_WAKEUP_SENT_CHILD_NOT_OBSERVED")

    def start_runtime_boundary(self) -> None:
        if os.environ.get("FR_EFFECT_EXECUTOR_ENABLED", "0").strip().lower() not in {"1", "true", "yes", "on"}:
            raise RuntimeError("EFFECT_EXECUTOR_REQUIRED_BY_ENTRYPOINT")
        if os.environ.get("MAINTENANCE_ENFORCEMENT_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}:
            raise RuntimeError("P2_ENFORCEMENT_NOT_AUTHORIZED")
        ledger_path = Path(os.environ.get("FR_EFFECT_LEDGER_FILE", "/state/runtime/fast_recovery_effects.sqlite3"))
        socket_path = Path(os.environ.get("FR_EFFECT_EXECUTOR_SOCKET", "/run/stream-v3-control/effect.sock"))
        projection_path = Path(os.environ["MAINTENANCE_AUDIT_STATE_FILE"])
        target_snapshot_path = Path(os.environ["FR_TARGET_SNAPSHOT_FILE"])
        controller_uid = max(1, int(os.environ.get("FR_EFFECT_CONTROLLER_UID", "992") or 992))
        socket_gid = max(1, int(os.environ.get("FR_EFFECT_SOCKET_GID", "983") or 983))
        self.effect_ledger = EffectLedger(
            ledger_path,
            initial_producer_id=os.environ.get("FR_EFFECT_INITIAL_PRODUCER_ID", "legacy-in-pod"),
            initial_producer_generation=max(
                1,
                int(os.environ.get("FR_EFFECT_INITIAL_PRODUCER_GENERATION", "1") or 1),
            ),
            allow_initialize=os.environ.get("FR_EFFECT_LEDGER_ALLOW_INITIALIZE", "0").strip().lower()
            in {"1", "true", "yes", "on"},
        )
        recovery_evidence_cycle = None
        recovery_evidence_config = os.environ.get("FR_RECOVERY_EVIDENCE_CONFIG_FILE", "").strip()
        if recovery_evidence_config:
            from runtime_boundary.recovery_publisher import (
                RecoveryEvidencePublisher,
                owned_json,
            )

            recovery_publisher = RecoveryEvidencePublisher(
                ledger=self.effect_ledger,
                config=owned_json(Path(recovery_evidence_config)),
                target_path=target_snapshot_path,
                process_supplier=self.runtime_boundary_process_snapshot,
                child_registry=self.child_registry,
            )
            recovery_evidence_cycle = recovery_publisher.publish_if_due
        self.effect_executor = EffectExecutorServer(
            socket_path=socket_path,
            ledger=self.effect_ledger,
            allowed_peer_uids={0, controller_uid},
            current_target=self.runtime_boundary_current_target,
            perform_effect=self.perform_fast_recovery_effect,
            before_effect=self.audit_mp03_effect_boundary,
            allowed_intents=EXECUTOR_OWNED_INTENTS,
            execute_async=True,
            require_transport_verification=True,
            socket_gid=socket_gid,
        )
        self.effect_executor.start()
        self.runtime_observation_publisher = RuntimeObservationPublisher(
            path=Path(os.environ.get("FR_RUNTIME_OBSERVATION_FILE", "/run/stream-v3-control/runtime-observation.json")),
            projection_path=projection_path,
            target_snapshot_path=target_snapshot_path,
            process_supplier=self.runtime_boundary_process_snapshot,
            producer_instance_id=self.runtime_boundary_instance_id,
            interval_seconds=1.0,
            recovery_evidence_cycle=recovery_evidence_cycle,
            rtmp_ports=tuple(
                int(value)
                for value in os.environ.get("FR_RTMP_PORTS", "443").split(",")
                if value.strip().isdigit()
            ),
        )
        self.runtime_observation_publisher.start()
        self.append_event(
            "fast_recovery_effect_executor_started",
            socket=str(socket_path),
            ledger=str(ledger_path),
            active_authority=self.effect_ledger.authority(),
            p2_enforcement_enabled=False,
        )

    def stop_runtime_boundary(self) -> None:
        if self.runtime_observation_publisher is not None:
            self.runtime_observation_publisher.close()
            self.runtime_observation_publisher = None
        if self.effect_executor is not None:
            self.effect_executor.close()
            self.effect_executor = None
        if self.effect_ledger is not None:
            self.effect_ledger.close()
            self.effect_ledger = None

    def cleanup(self) -> None:
        self.stop_runtime_boundary()
        try:
            super().cleanup()
        finally:
            self.child_registry.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="stream-engine with narrow MP-03 Effect Executor")
    parser.add_argument("--print-config", action="store_true")
    args = parser.parse_args()
    cfg = load_config()
    if args.print_config:
        print(json.dumps(cfg.__dict__, ensure_ascii=False, indent=2, default=str))
        return 0
    engine = RuntimeBoundaryStreamEngine(cfg)
    try:
        return engine.run()
    finally:
        engine.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
