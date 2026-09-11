from __future__ import annotations

import atexit
import json
import os
import queue
import socket
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

EXACT_TARGET_FIELDS = (
    "host_id",
    "host_boot_id",
    "namespace",
    "pod_uid",
    "container_name",
    "container_id",
    "ffmpeg_generation",
    "ffmpeg_pid",
)
ACTIVE_MAINTENANCE_STATES = frozenset(
    {
        "REQUESTED",
        "QUIESCING",
        "ESTABLISHED",
        "MUTATING",
        "VERIFYING_TARGET",
        "RECONCILING",
        "EXIT_PENDING",
        "QUIESCE_FAILED",
        "ABORTING",
        "SAFE_BLOCKED",
    }
)


class AuditPhase(StrEnum):
    OBSERVATION = "OBSERVATION"
    ACTION_PLAN_CREATED = "ACTION_PLAN_CREATED"
    ADMISSION = "ADMISSION"
    EFFECT_BOUNDARY = "EFFECT_BOUNDARY"
    EFFECT_RETURNED = "EFFECT_RETURNED"
    QUIESCE_ACK = "QUIESCE_ACK"
    RELEASE_ACK = "RELEASE_ACK"


class AuditVerdict(StrEnum):
    WOULD_ALLOW = "WOULD_ALLOW"
    WOULD_BLOCK = "WOULD_BLOCK"
    WOULD_ACK = "WOULD_ACK"
    WOULD_REJECT = "WOULD_REJECT"
    UNKNOWN = "UNKNOWN"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class AuditPathRole(StrEnum):
    NORMAL_MUTATOR = "NORMAL_MUTATOR"
    LOCAL_FALLBACK = "LOCAL_FALLBACK"
    PLANNED_EXECUTOR = "PLANNED_EXECUTOR"
    MANUAL_PLANNED_EXECUTOR = "MANUAL_PLANNED_EXECUTOR"
    AUTHORITY_PRODUCER = "AUTHORITY_PRODUCER"
    EXECUTION_SUBSTRATE = "EXECUTION_SUBSTRATE"
    HOST_SAFETY = "HOST_SAFETY"
    BREAK_GLASS = "BREAK_GLASS"
    INACTIVE_LEGACY = "INACTIVE_LEGACY"


@dataclass(frozen=True)
class AuditObservation:
    path_id: str
    phase: AuditPhase
    operation: str
    path_role: AuditPathRole
    process_service: str
    resource_identity: str = ""
    correlation_id: str = ""
    target_identity: Mapping[str, Any] | None = None
    operation_generation: int | None = None
    authorization_id: str = ""
    correlation_owner_path_id: str = ""
    in_flight_evidence: Mapping[str, Any] | None = None
    generation_evidence: Mapping[str, Any] | None = None
    bind_source_target: bool = False
    p2_disabled_evaluation: bool = False
    native_operation_id: str = ""
    native_operation_generation: str = ""
    actual_production_decision: str = ""
    actual_production_result: str = ""


@dataclass(frozen=True)
class AuditDecision:
    verdict: AuditVerdict
    reason_code: str
    production_behavior_modified: bool = False


@dataclass(frozen=True)
class AuditCallResult:
    decision: AuditDecision
    evidence_enqueued: bool
    evidence_status: str


def utc_now_text() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_utc(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def target_identity_complete(value: Mapping[str, Any] | None) -> bool:
    if not isinstance(value, Mapping) or set(value) != set(EXACT_TARGET_FIELDS):
        return False
    for field in EXACT_TARGET_FIELDS:
        if field == "ffmpeg_pid":
            try:
                if int(value[field]) <= 1:
                    return False
            except (TypeError, ValueError):
                return False
        elif not str(value[field] or "").strip():
            return False
    return str(value["container_name"]) == "stream-engine"


def _authorization_for(snapshot: Mapping[str, Any], authorization_id: str) -> Mapping[str, Any] | None:
    authorizations = snapshot.get("authorizations")
    if not isinstance(authorizations, list):
        return None
    for value in authorizations:
        if isinstance(value, Mapping) and str(value.get("authorization_id") or "") == authorization_id:
            return value
    return None


def evaluate_audit_decision(
    observation: AuditObservation,
    snapshot: Mapping[str, Any] | None,
    *,
    now: datetime | None = None,
) -> AuditDecision:
    if observation.path_role in {
        AuditPathRole.HOST_SAFETY,
        AuditPathRole.BREAK_GLASS,
        AuditPathRole.INACTIVE_LEGACY,
    }:
        return AuditDecision(AuditVerdict.NOT_APPLICABLE, "UNSUPPORTED_OR_INACTIVE_PATH")
    if observation.path_role == AuditPathRole.EXECUTION_SUBSTRATE:
        if observation.correlation_owner_path_id:
            return AuditDecision(AuditVerdict.NOT_APPLICABLE, "CONTROLLER_MEDIATED_CONSEQUENCE")
        return AuditDecision(AuditVerdict.UNKNOWN, "EXECUTION_OWNER_CORRELATION_MISSING")
    if not isinstance(snapshot, Mapping) or not bool(snapshot.get("available")):
        return AuditDecision(AuditVerdict.UNKNOWN, "MAINTENANCE_STATE_UNAVAILABLE")
    now_value = now or datetime.now(UTC)
    fresh_until = _parse_utc(snapshot.get("fresh_until"))
    if fresh_until is None or fresh_until <= now_value:
        return AuditDecision(AuditVerdict.UNKNOWN, "MAINTENANCE_STATE_STALE")

    maintenance_state = str(snapshot.get("maintenance_state") or "UNKNOWN")
    active = maintenance_state in ACTIVE_MAINTENANCE_STATES
    current_generation = int(snapshot.get("maintenance_generation") or 0)
    expected_target = snapshot.get("target_identity")
    observed_target = observation.target_identity

    if active and str(snapshot.get("schema_version") or "") == "maintenance.audit_state_snapshot.v2":
        target_status = str(snapshot.get("target_snapshot_status") or "MISSING")
        if target_status != "VALID":
            return AuditDecision(AuditVerdict.UNKNOWN, f"MAINTENANCE_TARGET_{target_status}")

    if observation.phase in {AuditPhase.QUIESCE_ACK, AuditPhase.RELEASE_ACK}:
        if not active:
            return AuditDecision(AuditVerdict.WOULD_REJECT, "MAINTENANCE_NOT_ACTIVE")
        evidence = observation.in_flight_evidence
        if not isinstance(evidence, Mapping) or str(evidence.get("status") or "") != "CONFIRMED":
            return AuditDecision(AuditVerdict.UNKNOWN, "IN_FLIGHT_SOURCE_NOT_CONFIRMED")
        raw_count = evidence.get("count")
        try:
            count = int(raw_count) if raw_count is not None else -1
        except (TypeError, ValueError):
            return AuditDecision(AuditVerdict.UNKNOWN, "IN_FLIGHT_COUNT_UNKNOWN")
        if count < 0:
            return AuditDecision(AuditVerdict.UNKNOWN, "IN_FLIGHT_COUNT_UNKNOWN")
        if count != 0:
            return AuditDecision(AuditVerdict.WOULD_REJECT, "IN_FLIGHT_NOT_ZERO")
        if not target_identity_complete(observed_target) or observed_target != expected_target:
            return AuditDecision(AuditVerdict.WOULD_REJECT, "TARGET_IDENTITY_MISMATCH")
        return AuditDecision(AuditVerdict.WOULD_ACK, "QUIESCE_OR_RELEASE_ACK_ELIGIBLE")

    planned = observation.path_role in {
        AuditPathRole.PLANNED_EXECUTOR,
        AuditPathRole.MANUAL_PLANNED_EXECUTOR,
    }
    if not active:
        if planned:
            return AuditDecision(AuditVerdict.WOULD_REJECT, "MAINTENANCE_NOT_ACTIVE")
        return AuditDecision(AuditVerdict.WOULD_ALLOW, "MAINTENANCE_INACTIVE")

    if observation.path_role == AuditPathRole.LOCAL_FALLBACK:
        return AuditDecision(AuditVerdict.WOULD_BLOCK, "WOULD_BLOCK_LOCAL_FALLBACK")

    if not target_identity_complete(expected_target if isinstance(expected_target, Mapping) else None):
        return AuditDecision(AuditVerdict.UNKNOWN, "MAINTENANCE_TARGET_UNKNOWN")
    if not target_identity_complete(observed_target):
        return AuditDecision(AuditVerdict.UNKNOWN, "OBSERVED_TARGET_UNKNOWN")
    if observed_target != expected_target:
        return AuditDecision(AuditVerdict.WOULD_REJECT, "TARGET_IDENTITY_MISMATCH")
    if observation.operation_generation is not None and observation.operation_generation != current_generation:
        return AuditDecision(AuditVerdict.WOULD_REJECT, "STALE_MAINTENANCE_GENERATION")

    if not planned:
        return AuditDecision(AuditVerdict.WOULD_BLOCK, "MAINTENANCE_FENCE_ACTIVE")

    if not observation.authorization_id:
        return AuditDecision(AuditVerdict.WOULD_REJECT, "MAINTENANCE_AUTHORIZATION_MISSING")
    authorization = _authorization_for(snapshot, observation.authorization_id)
    if authorization is None:
        return AuditDecision(AuditVerdict.WOULD_REJECT, "MAINTENANCE_AUTHORIZATION_UNKNOWN")
    expected_executor = "planned_rollout_executor" if observation.path_role == AuditPathRole.PLANNED_EXECUTOR else "manual_planned_executor"
    checks: tuple[tuple[bool, str], ...] = (
        (str(authorization.get("authorization_kind") or "") == "MAINTENANCE_MUTATION", "AUTHORIZATION_KIND_MISMATCH"),
        (str(authorization.get("state") or "") == "ACCEPTED", "AUTHORIZATION_NOT_ACCEPTED"),
        (bool(authorization.get("single_use")), "AUTHORIZATION_NOT_SINGLE_USE"),
        (int(authorization.get("use_count") or 0) == 0, "AUTHORIZATION_REPLAY"),
        (int(authorization.get("maintenance_generation") or 0) == current_generation, "AUTHORIZATION_GENERATION_MISMATCH"),
        (str(authorization.get("executor_id") or "") == expected_executor, "AUTHORIZATION_EXECUTOR_MISMATCH"),
        (str(authorization.get("operation") or "") == observation.operation, "AUTHORIZATION_OPERATION_MISMATCH"),
        (
            str(authorization.get("resource_identity") or "") == observation.resource_identity,
            "AUTHORIZATION_RESOURCE_MISMATCH",
        ),
        (authorization.get("source_target_identity") == expected_target, "AUTHORIZATION_TARGET_MISMATCH"),
    )
    for passed, reason in checks:
        if not passed:
            return AuditDecision(AuditVerdict.WOULD_REJECT, reason)
    expires_at = _parse_utc(authorization.get("expires_at"))
    if expires_at is None or expires_at <= now_value:
        return AuditDecision(AuditVerdict.WOULD_REJECT, "AUTHORIZATION_EXPIRED")
    return AuditDecision(AuditVerdict.WOULD_ALLOW, "MAINTENANCE_MUTATION_AUTHORIZED")


class AuditEmitter:
    def __init__(
        self,
        *,
        enabled: bool,
        state_supplier: Callable[[], Mapping[str, Any] | None] | None = None,
        event_writer: Callable[[Mapping[str, Any]], None] | None = None,
        queue_capacity: int = 1024,
        state_refresh_seconds: float = 0.25,
        host: str | None = None,
        host_id: str | None = None,
        host_boot_id: str | None = None,
        os_hostname: str | None = None,
    ) -> None:
        self.enabled = enabled
        self._state_supplier = state_supplier or self._read_state_file
        self._event_writer = event_writer or self._append_event_file
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=max(1, queue_capacity))
        self._state_refresh_seconds = max(0.01, state_refresh_seconds)
        self._os_hostname = os_hostname or socket.gethostname()
        configured_host_id = (host_id or host or os.environ.get("MAINTENANCE_AUDIT_HOST_ID", "")).strip()
        self._host_id = configured_host_id or "UNKNOWN_HOST_ID"
        self._host_boot_id = host_boot_id or self._read_host_boot_id()
        self._host = self._host_id
        self._process_instance_id = f"audit-process-{uuid.uuid4()}"
        self._process_id = os.getpid()
        self._started_at = utc_now_text()
        self._state: Mapping[str, Any] | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._enqueue_lock = threading.Lock()
        self._hook_calls = 0
        self._enqueued = 0
        self._written = 0
        self._lost = 0
        self._exceptions = 0
        self._queue_full_losses = 0
        self._evidence_write_failures = 0
        self._state_refresh_failures = 0
        self._hook_exceptions = 0
        self._health_write_failures = 0
        self._queue_high_watermark = 0
        self._hook_latency_max_us = 0.0
        self._p2_disabled_evaluations = 0
        self._p2_disabled_failures = 0
        self._closed = False
        self._last_failure_reason = ""
        self._runtime_release_id = os.environ.get("MAINTENANCE_RUNTIME_RELEASE_ID", "").strip()
        self._p2_enforcement_configured = os.environ.get("MAINTENANCE_ENFORCEMENT_ENABLED", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if enabled:
            self._thread = threading.Thread(target=self._run, name="maintenance-audit-writer", daemon=True)
            self._thread.start()

    def emit(self, observation: AuditObservation) -> AuditCallResult:
        started_ns = time.perf_counter_ns()
        with self._lock:
            self._hook_calls += 1
        if not self.enabled:
            return AuditCallResult(
                AuditDecision(AuditVerdict.NOT_APPLICABLE, "AUDIT_DISABLED"),
                False,
                "DISABLED",
            )
        try:
            snapshot = self._state
            effective_observation = observation
            bind_from_environment = os.environ.get("MAINTENANCE_AUDIT_BIND_SOURCE_TARGET", "0").strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
            if (
                observation.target_identity is None
                and (observation.bind_source_target or bind_from_environment)
                and isinstance(snapshot, Mapping)
                and str(snapshot.get("target_snapshot_status") or "") == "VALID"
                and isinstance(snapshot.get("source_target_identity"), Mapping)
            ):
                effective_observation = replace(observation, target_identity=snapshot["source_target_identity"])
            decision = evaluate_audit_decision(effective_observation, snapshot)
            p2_fields: dict[str, Any] = {
                "p2_disabled_evaluation_requested": observation.p2_disabled_evaluation,
                "p2_disabled_verdict": "NOT_EVALUATED",
                "p2_disabled_reason": "NOT_REQUESTED",
                "p2_enforcement_enabled": False,
                "p2_production_branch_signal": None,
                "p2_production_adapter_connected": False,
                "p2_production_behavior_modified": False,
                "p2_physical_effect_count": 0,
                "p2_effect_boundary_revalidated": False,
            }
            if observation.p2_disabled_evaluation:
                configured_enabled = os.environ.get("MAINTENANCE_ENFORCEMENT_ENABLED", "0").strip().lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                }
                try:
                    # The import is intentionally local: maintenance_enforcement is a pure
                    # evaluator and maintenance_audit remains importable without P2 present.
                    from maintenance_enforcement import evaluate_enforcement_shadow

                    shadow = evaluate_enforcement_shadow(
                        effective_observation,
                        snapshot,
                        enforcement_enabled=configured_enabled,
                    )
                    p2_fields.update(
                        {
                            "p2_disabled_verdict": shadow.disposition.value,
                            "p2_disabled_reason": shadow.reason_code,
                            "p2_enforcement_enabled": shadow.enforcement_enabled,
                            "p2_production_branch_signal": shadow.production_branch_signal,
                            "p2_production_adapter_connected": shadow.production_adapter_connected,
                            "p2_production_behavior_modified": shadow.production_behavior_modified,
                            "p2_physical_effect_count": shadow.physical_effect_count,
                            "p2_effect_boundary_revalidated": shadow.effect_boundary_revalidated,
                        }
                    )
                    with self._lock:
                        self._p2_disabled_evaluations += 1
                except BaseException:  # noqa: BLE001 - P2-disabled observation cannot affect production
                    p2_fields.update(
                        {
                            "p2_disabled_verdict": "UNKNOWN",
                            "p2_disabled_reason": "P2_DISABLED_EVALUATION_ERROR",
                            "p2_enforcement_enabled": configured_enabled,
                        }
                    )
                    with self._lock:
                        self._p2_disabled_failures += 1
            is_v2 = isinstance(snapshot, Mapping) and str(snapshot.get("schema_version") or "") == "maintenance.audit_state_snapshot.v2"
            v2_snapshot: Mapping[str, Any] = snapshot if is_v2 and isinstance(snapshot, Mapping) else {}
            event: dict[str, Any] = {
                "schema_version": "maintenance.audit_event.v2" if is_v2 else "maintenance.audit_event.v1",
                "timestamp": utc_now_text(),
                "event_id": f"audit-{uuid.uuid4()}",
                "host": self._host,
                "process_instance_id": self._process_instance_id,
                "process_id": self._process_id,
                "thread_id": threading.get_ident(),
                "monotonic_ns": time.monotonic_ns(),
                "process_service": observation.process_service,
                "path_id": observation.path_id,
                "phase": observation.phase.value,
                "operation": observation.operation,
                "path_role": observation.path_role.value,
                "resource_identity": observation.resource_identity,
                "correlation_id": observation.correlation_id or f"audit-correlation-{uuid.uuid4()}",
                "correlation_owner_path_id": observation.correlation_owner_path_id,
                "native_operation_id": observation.native_operation_id,
                "native_operation_generation": observation.native_operation_generation,
                "target_identity": (
                    dict(effective_observation.target_identity) if effective_observation.target_identity is not None else None
                ),
                "operation_generation": observation.operation_generation,
                "maintenance_observed_state": (
                    str(snapshot.get("maintenance_state") or "UNKNOWN") if isinstance(snapshot, Mapping) else "UNKNOWN"
                ),
                "maintenance_snapshot_observed_at": (
                    str(snapshot.get("observed_at") or "UNKNOWN") if isinstance(snapshot, Mapping) else "UNKNOWN"
                ),
                "maintenance_snapshot_fresh_until": (
                    str(snapshot.get("fresh_until") or "UNKNOWN") if isinstance(snapshot, Mapping) else "UNKNOWN"
                ),
                "maintenance_id": (str(snapshot.get("maintenance_id") or "UNKNOWN") if isinstance(snapshot, Mapping) else "UNKNOWN"),
                "maintenance_generation": (int(snapshot.get("maintenance_generation") or 0) if isinstance(snapshot, Mapping) else 0),
                "authority_epoch": (int(snapshot.get("authority_epoch") or 0) if isinstance(snapshot, Mapping) else 0),
                "authority_session_id": (
                    str(snapshot.get("authority_session_id") or "UNKNOWN") if isinstance(snapshot, Mapping) else "UNKNOWN"
                ),
                "authorization_id": observation.authorization_id or "",
                "in_flight_evidence": dict(observation.in_flight_evidence or {"status": "MISSING"}),
                "generation_evidence": dict(observation.generation_evidence or {"status": "MISSING"}),
                "audit_verdict": decision.verdict.value,
                "audit_reason": decision.reason_code,
                "production_behavior_modified": False,
                "actual_production_decision": observation.actual_production_decision,
                "actual_production_result": observation.actual_production_result,
                **p2_fields,
            }
            if (
                isinstance(snapshot, Mapping)
                and str(snapshot.get("projection_id") or "")
                and int(snapshot.get("projection_sequence") or 0) > 0
            ):
                event.update(
                    {
                        "maintenance_projection_id": str(snapshot.get("projection_id") or ""),
                        "maintenance_projection_sequence": int(snapshot.get("projection_sequence") or 0),
                        "maintenance_projection_reason": str(snapshot.get("projection_reason") or "PROJECTION_VALID"),
                    }
                )
            if is_v2:
                event.update(
                    {
                        "host_id": self._host_id,
                        "host_boot_id": self._host_boot_id,
                        "os_hostname": self._os_hostname,
                        "maintenance_snapshot_id": str(v2_snapshot.get("snapshot_id") or ""),
                        "maintenance_producer_id": str(v2_snapshot.get("producer_id") or ""),
                        "maintenance_producer_instance_id": str(v2_snapshot.get("producer_instance_id") or ""),
                        "source_target_identity": (
                            dict(v2_snapshot["source_target_identity"])
                            if isinstance(v2_snapshot.get("source_target_identity"), Mapping)
                            else None
                        ),
                        "target_snapshot_id": str(v2_snapshot.get("target_snapshot_id") or ""),
                        "target_snapshot_status": str(v2_snapshot.get("target_snapshot_status") or "MISSING"),
                        "target_snapshot_age_seconds": v2_snapshot.get("target_snapshot_age_seconds"),
                        "target_snapshot_age": v2_snapshot.get("target_snapshot_age_seconds"),
                    }
                )
            with self._enqueue_lock:
                queue_depth_before = self._queue.qsize()
                hook_latency_us = (time.perf_counter_ns() - started_ns) / 1000.0
                event["queue_depth_before"] = queue_depth_before
                event["queue_capacity"] = self._queue.maxsize
                event["audit_hook_latency_us"] = hook_latency_us
                try:
                    self._queue.put_nowait(event)
                except queue.Full:
                    self._record_failure("AUDIT_EVIDENCE_LOST_QUEUE_FULL")
                    return AuditCallResult(decision, False, "AUDIT_EVIDENCE_LOST")
                with self._lock:
                    self._enqueued += 1
                    self._queue_high_watermark = max(self._queue_high_watermark, queue_depth_before + 1)
                    self._hook_latency_max_us = max(
                        self._hook_latency_max_us,
                        hook_latency_us,
                    )
            return AuditCallResult(decision, True, "ENQUEUED")
        except BaseException:  # noqa: BLE001 - audit failure must never escape into production
            self._record_failure("AUDIT_HOOK_EXCEPTION")
            return AuditCallResult(
                AuditDecision(AuditVerdict.UNKNOWN, "AUDIT_HOOK_EXCEPTION"),
                False,
                "AUDIT_EVIDENCE_LOST",
            )

    def health_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "schema_version": "maintenance.audit_health.v1",
                "timestamp": utc_now_text(),
                "host": self._host,
                "host_id": self._host_id,
                "host_boot_id": self._host_boot_id,
                "os_hostname": self._os_hostname,
                "process_instance_id": self._process_instance_id,
                "process_id": self._process_id,
                "started_at": self._started_at,
                "enabled": self.enabled,
                "hook_calls": self._hook_calls,
                "enqueued": self._enqueued,
                "written": self._written,
                "lost": self._lost,
                "exception_count": self._exceptions,
                "queue_full_losses": self._queue_full_losses,
                "evidence_write_failures": self._evidence_write_failures,
                "state_refresh_failures": self._state_refresh_failures,
                "hook_exceptions": self._hook_exceptions,
                "health_write_failures": self._health_write_failures,
                "last_failure_reason": self._last_failure_reason,
                "queue_depth": self._queue.qsize(),
                "queue_capacity": self._queue.maxsize,
                "queue_high_watermark": self._queue_high_watermark,
                "hook_latency_max_us": self._hook_latency_max_us,
                "p2_disabled_evaluations": self._p2_disabled_evaluations,
                "p2_disabled_failures": self._p2_disabled_failures,
                "p2_enforcement_configured": self._p2_enforcement_configured,
                "runtime_release_id": self._runtime_release_id,
                "closed": self._closed,
                "production_behavior_modified": False,
            }

    def wait_until_drained(self, timeout_seconds: float = 2.0) -> bool:
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        while time.monotonic() < deadline:
            if self._queue.unfinished_tasks == 0:
                return True
            time.sleep(0.005)
        return self._queue.unfinished_tasks == 0

    def close(self) -> None:
        if self._closed:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        with self._lock:
            self._closed = True
        self._write_health_file()

    def _record_failure(self, reason: str) -> None:
        with self._lock:
            self._lost += 1
            self._exceptions += 1
            self._last_failure_reason = reason
            if reason == "AUDIT_EVIDENCE_LOST_QUEUE_FULL":
                self._queue_full_losses += 1
            elif reason == "AUDIT_EVIDENCE_LOST_WRITE_FAILED":
                self._evidence_write_failures += 1
            elif reason == "AUDIT_STATE_REFRESH_FAILED":
                self._state_refresh_failures += 1
            elif reason == "AUDIT_HOOK_EXCEPTION":
                self._hook_exceptions += 1

    def _record_health_failure(self) -> None:
        with self._lock:
            self._exceptions += 1
            self._health_write_failures += 1
            self._last_failure_reason = "AUDIT_HEALTH_WRITE_FAILED"

    def _run(self) -> None:
        next_refresh = 0.0
        while not self._stop.is_set() or not self._queue.empty():
            now = time.monotonic()
            if now >= next_refresh:
                try:
                    self._state = self._state_supplier()
                except BaseException:  # noqa: BLE001 - unavailable evidence becomes UNKNOWN
                    self._state = None
                    self._record_failure("AUDIT_STATE_REFRESH_FAILED")
                next_refresh = now + self._state_refresh_seconds
                self._write_health_file()
            try:
                event = self._queue.get(timeout=0.02)
            except queue.Empty:
                continue
            try:
                self._event_writer(event)
            except BaseException:  # noqa: BLE001 - evidence loss cannot affect the caller
                self._record_failure("AUDIT_EVIDENCE_LOST_WRITE_FAILED")
            else:
                with self._lock:
                    self._written += 1
            finally:
                self._queue.task_done()
                self._write_health_file()
        self._write_health_file()

    @staticmethod
    def _read_host_boot_id() -> str:
        try:
            value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
        except OSError:
            value = ""
        return value or "UNKNOWN_HOST_BOOT_ID"

    @staticmethod
    def _read_state_file() -> Mapping[str, Any] | None:
        raw_path = os.environ.get("MAINTENANCE_AUDIT_STATE_FILE", "").strip()
        if not raw_path:
            return None
        projection_enabled = os.environ.get("MAINTENANCE_AUDIT_PROJECTION_ENABLED", "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if projection_enabled:
            # Candidate packaging provides this pure local-file reader from the
            # recovery-control source.  It has no network or mutation adapter.
            from snapshot_projection import ProjectionReader

            reader = ProjectionReader(
                path=Path(raw_path),
                expected_producer_id=os.environ.get(
                    "MAINTENANCE_AUDIT_PROJECTION_PRODUCER_ID",
                    "dell-maintenance-snapshot-projector",
                ).strip(),
                expected_source_producer_id=os.environ.get(
                    "MAINTENANCE_AUDIT_SOURCE_PRODUCER_ID",
                    "arena-maintenance-shadow",
                ).strip(),
                high_water_path=(
                    Path(os.environ["MAINTENANCE_AUDIT_PROJECTION_HIGH_WATER_FILE"])
                    if os.environ.get("MAINTENANCE_AUDIT_PROJECTION_HIGH_WATER_FILE", "").strip()
                    else None
                ),
            )
            decision = reader.read()
            if not decision.available or decision.snapshot is None:
                return {
                    "available": False,
                    "projection_id": decision.projection_id,
                    "projection_sequence": decision.projection_sequence,
                    "projection_reason": decision.reason_code,
                }
            return {
                **dict(decision.snapshot),
                "projection_id": decision.projection_id,
                "projection_sequence": decision.projection_sequence,
                "projection_reason": decision.reason_code,
            }
        with Path(raw_path).open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        return value if isinstance(value, Mapping) else None

    @staticmethod
    def _append_event_file(event: Mapping[str, Any]) -> None:
        raw_path = os.environ.get("MAINTENANCE_AUDIT_EVENT_FILE", "").strip()
        if not raw_path:
            raise FileNotFoundError("MAINTENANCE_AUDIT_EVENT_FILE is not configured")
        path = Path(raw_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def _write_health_file(self) -> None:
        raw_path = os.environ.get("MAINTENANCE_AUDIT_HEALTH_FILE", "").strip()
        if not raw_path:
            return
        path = Path(raw_path)
        temporary = path.with_name(f".{path.name}.{self._process_id}.tmp")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(
                self.health_snapshot(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            temporary.write_text(payload + "\n", encoding="utf-8")
            os.replace(temporary, path)
        except BaseException:  # noqa: BLE001 - health evidence is non-authoritative
            self._record_health_failure()


_GLOBAL_EMITTER: AuditEmitter | None = None
_GLOBAL_LOCK = threading.Lock()


def _enabled_from_environment() -> bool:
    return os.environ.get("MAINTENANCE_AUDIT_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}


def global_emitter() -> AuditEmitter:
    global _GLOBAL_EMITTER
    if _GLOBAL_EMITTER is None:
        with _GLOBAL_LOCK:
            if _GLOBAL_EMITTER is None:
                _GLOBAL_EMITTER = AuditEmitter(enabled=_enabled_from_environment())
                if _GLOBAL_EMITTER.enabled:
                    with suppress(BaseException):  # startup evidence cannot affect production
                        print(
                            "MAINTENANCE_AUDIT_STARTUP "
                            + json.dumps(
                                {
                                    "schema_version": "maintenance.audit_startup.v1",
                                    "timestamp": utc_now_text(),
                                    "runtime_release_id": _GLOBAL_EMITTER._runtime_release_id,
                                    "audit_enabled": True,
                                    "p2_enforcement_enabled": _GLOBAL_EMITTER._p2_enforcement_configured,
                                    "production_behavior_modified": False,
                                },
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                            flush=True,
                        )
    return _GLOBAL_EMITTER


def set_global_emitter_for_tests(emitter: AuditEmitter | None) -> None:
    global _GLOBAL_EMITTER
    with _GLOBAL_LOCK:
        if _GLOBAL_EMITTER is not None and _GLOBAL_EMITTER is not emitter:
            _GLOBAL_EMITTER.close()
        _GLOBAL_EMITTER = emitter


def _close_global_emitter() -> None:
    emitter = _GLOBAL_EMITTER
    if emitter is not None:
        emitter.close()


atexit.register(_close_global_emitter)

# Audit-enabled one-shot processes start the background cache refresh at module
# import, before their decision path reaches the first hook. This remains
# asynchronous: no state-file I/O is performed by emit() or by production code.
if _enabled_from_environment():
    global_emitter()


def audit_maintenance_decision(
    *,
    path_id: str,
    phase: AuditPhase | str,
    operation: str,
    path_role: AuditPathRole | str,
    process_service: str,
    resource_identity: str = "",
    correlation_id: str = "",
    target_identity: Mapping[str, Any] | None = None,
    operation_generation: int | None = None,
    authorization_id: str = "",
    correlation_owner_path_id: str = "",
    in_flight_evidence: Mapping[str, Any] | None = None,
    generation_evidence: Mapping[str, Any] | None = None,
    bind_source_target: bool = False,
    p2_disabled_evaluation: bool = False,
    native_operation_id: str = "",
    native_operation_generation: str = "",
    actual_production_decision: str = "",
    actual_production_result: str = "",
) -> AuditCallResult:
    try:
        observation = AuditObservation(
            path_id=path_id,
            phase=AuditPhase(phase),
            operation=operation,
            path_role=AuditPathRole(path_role),
            process_service=process_service,
            resource_identity=resource_identity,
            correlation_id=correlation_id,
            target_identity=target_identity,
            operation_generation=operation_generation,
            authorization_id=authorization_id,
            correlation_owner_path_id=correlation_owner_path_id,
            in_flight_evidence=in_flight_evidence,
            generation_evidence=generation_evidence,
            bind_source_target=bind_source_target,
            p2_disabled_evaluation=p2_disabled_evaluation,
            native_operation_id=native_operation_id,
            native_operation_generation=native_operation_generation,
            actual_production_decision=actual_production_decision,
            actual_production_result=actual_production_result,
        )
        return global_emitter().emit(observation)
    except BaseException:  # noqa: BLE001 - public audit hook is intentionally fail-isolated
        return AuditCallResult(
            AuditDecision(AuditVerdict.UNKNOWN, "AUDIT_HOOK_EXCEPTION"),
            False,
            "AUDIT_EVIDENCE_LOST",
        )
