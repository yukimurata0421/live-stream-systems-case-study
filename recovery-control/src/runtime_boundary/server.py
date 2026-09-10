from __future__ import annotations

import json
import os
import socket
import stat
import struct
import threading
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from .ledger import EffectLedger
from .model import RESTART_FFMPEG, EffectRequest, ProtocolError, parse_utc, validate_target


class OutcomeUnknown(RuntimeError):
    def __init__(self, reason: str, *, result: Mapping[str, Any] | None = None) -> None:
        super().__init__(reason)
        self.result = dict(result or {})
        self.result["reason"] = reason


class EffectRejected(RuntimeError):
    pass


class EffectExecutorServer:
    def __init__(
        self,
        *,
        socket_path: Path,
        ledger: EffectLedger,
        allowed_peer_uids: set[int],
        current_target: Callable[[], Mapping[str, Any]],
        perform_effect: Callable[[EffectRequest], Mapping[str, Any]],
        before_effect: Callable[[EffectRequest], None] | None = None,
        allowed_intents: set[str] | frozenset[str] | None = None,
        execute_async: bool = False,
        require_transport_verification: bool = False,
        socket_mode: int = 0o660,
        socket_gid: int | None = None,
        worker_shutdown_timeout_seconds: float = 25.0,
    ) -> None:
        if not 0.001 <= worker_shutdown_timeout_seconds <= 60.0:
            raise ValueError("WORKER_SHUTDOWN_TIMEOUT_OUT_OF_RANGE")
        self.socket_path = socket_path
        self.ledger = ledger
        self.allowed_peer_uids = allowed_peer_uids
        self.current_target = current_target
        self.perform_effect = perform_effect
        self.before_effect = before_effect
        self.allowed_intents = frozenset(allowed_intents or {RESTART_FFMPEG})
        self.execute_async = execute_async
        self.require_transport_verification = require_transport_verification
        self.socket_mode = socket_mode
        self.socket_gid = socket_gid
        self.worker_shutdown_timeout_seconds = float(worker_shutdown_timeout_seconds)
        self._stop = threading.Event()
        self._listener: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._workers: set[threading.Thread] = set()
        self._workers_lock = threading.Lock()

    def start(self) -> None:
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            existing_mode = self.socket_path.lstat().st_mode
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISSOCK(existing_mode):
                raise RuntimeError("EFFECT_SOCKET_PATH_NOT_SOCKET")
            self.socket_path.unlink()
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(self.socket_path))
        os.chmod(self.socket_path, self.socket_mode)
        if self.socket_gid is not None:
            os.chown(self.socket_path, -1, self.socket_gid)
        listener.listen(8)
        listener.settimeout(0.2)
        self._listener = listener
        self._thread = threading.Thread(target=self._serve, name="fast-recovery-effect-executor", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._listener is not None:
            self._listener.close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        # The bounded FFmpeg lifecycle may spend up to ten seconds waiting for
        # SIGTERM and another ten waiting for SIGKILL.  Keep the ledger open
        # until such an in-flight worker has had time to persist its result.
        deadline = time.monotonic() + self.worker_shutdown_timeout_seconds
        while time.monotonic() < deadline:
            with self._workers_lock:
                workers = list(self._workers)
            if not workers:
                break
            for worker in workers:
                worker.join(timeout=max(0.0, deadline - time.monotonic()))
        try:
            existing_mode = self.socket_path.lstat().st_mode
        except FileNotFoundError:
            pass
        else:
            if stat.S_ISSOCK(existing_mode):
                self.socket_path.unlink()
        with self._workers_lock:
            workers_remaining = bool(self._workers)
        if workers_remaining:
            raise RuntimeError("EFFECT_EXECUTOR_WORKERS_DID_NOT_STOP")

    def _serve(self) -> None:
        listener = self._listener
        if listener is None:
            return
        while not self._stop.is_set():
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                if self._stop.is_set():
                    return
                continue
            with connection:
                try:
                    response = self._handle_connection(connection)
                except BaseException:
                    # A storage failure must not terminate the Unix-socket
                    # accept loop.  This fallback deliberately makes no
                    # assertion about whether a request was committed.
                    response = {
                        "schema_version": "runtime.effect_response.v1",
                        "ok": False,
                        "reason": "EXECUTOR_INTERNAL_ERROR",
                        "state": "OUTCOME_UNKNOWN",
                        "request_id": "",
                        "correlation_id": "",
                        "intent_type": "",
                        "replay": False,
                        "result": {"reason": "EXECUTOR_RESPONSE_UNAVAILABLE"},
                        "active_authority": {},
                        "in_flight_count": -1,
                        "automatic_retry": False,
                    }
                try:
                    connection.sendall(json.dumps(response, sort_keys=True, separators=(",", ":")).encode() + b"\n")
                except OSError:
                    continue

    def _handle_connection(self, connection: socket.socket) -> dict[str, Any]:
        request: EffectRequest | None = None
        try:
            credentials = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
            _, uid, _ = struct.unpack("3i", credentials)
            if uid not in self.allowed_peer_uids:
                return self._response(False, "PEER_UID_UNAUTHORIZED", "REJECTED")
            raw = bytearray()
            while len(raw) <= 65536:
                chunk = connection.recv(4096)
                if not chunk:
                    break
                raw.extend(chunk)
                if b"\n" in chunk:
                    break
            if len(raw) > 65536:
                return self._response(False, "REQUEST_TOO_LARGE", "REJECTED")
            value = json.loads(bytes(raw).split(b"\n", 1)[0])
            if not isinstance(value, Mapping):
                raise ProtocolError("REQUEST_NOT_OBJECT")
            schema_version = str(value.get("schema_version") or "")
            if schema_version == "runtime.effect_unresolved_query.v1":
                if set(value) != {"schema_version"}:
                    raise ProtocolError("UNRESOLVED_QUERY_FIELDS_NOT_EXACT")
                scopes = self.ledger.unresolved_scopes()
                return {
                    "schema_version": "runtime.effect_unresolved_response.v1",
                    "ok": True,
                    "reason": "UNRESOLVED_SCOPES_READ",
                    "unresolved_count": len(scopes),
                    "unresolved_scopes": scopes,
                    "active_authority": self.ledger.authority(),
                    "automatic_retry": False if scopes else None,
                }
            if schema_version == "runtime.effect_correlation_status_query.v1":
                if set(value) != {"schema_version", "correlation_id"}:
                    raise ProtocolError("CORRELATION_STATUS_QUERY_FIELDS_NOT_EXACT")
                correlation_id = str(value.get("correlation_id") or "").strip()
                if not correlation_id or len(correlation_id) > 256:
                    raise ProtocolError("CORRELATION_STATUS_QUERY_ID_INVALID")
                correlated = self.ledger.request_status_by_correlation(correlation_id)
                matching_count = int(correlated["matching_count"])
                if matching_count != 1:
                    return {
                        "schema_version": "runtime.effect_correlation_status_response.v1",
                        "ok": False,
                        "reason": "CORRELATION_NOT_FOUND" if matching_count == 0 else "CORRELATION_AMBIGUOUS",
                        "correlation_id": correlation_id,
                        "matching_count": matching_count,
                        "request_id": "",
                        "state": "UNKNOWN",
                        "effect_scope_state": "UNKNOWN",
                        "effect_scope_id": "",
                        "effect_boundary_reached": False,
                        "physical_attempt_count": 0,
                        "result": None,
                        "automatic_retry": None,
                    }
                state = str(correlated["state"])
                return {
                    "schema_version": "runtime.effect_correlation_status_response.v1",
                    "ok": True,
                    "reason": "CORRELATION_STATUS_READ",
                    **correlated,
                    "automatic_retry": False
                    if state in {"EXECUTION_STARTED", "OUTCOME_UNKNOWN"}
                    or correlated["effect_scope_state"] == "EFFECT_OBSERVED_AWAITING_VERIFICATION"
                    else None,
                }
            if schema_version == "runtime.effect_request_status_query.v1":
                if set(value) != {"schema_version", "request_id"}:
                    raise ProtocolError("REQUEST_STATUS_QUERY_FIELDS_NOT_EXACT")
                request_id = str(value.get("request_id") or "").strip()
                if not request_id or len(request_id) > 256:
                    raise ProtocolError("REQUEST_STATUS_QUERY_ID_INVALID")
                status = self.ledger.request_status(request_id)
                if status is None:
                    return {
                        "schema_version": "runtime.effect_request_status_response.v1",
                        "ok": False,
                        "reason": "REQUEST_NOT_FOUND",
                        "request_id": request_id,
                        "state": "UNKNOWN",
                        "effect_scope_state": "UNKNOWN",
                        "effect_scope_id": "",
                        "effect_boundary_reached": False,
                        "physical_attempt_count": 0,
                        "result": None,
                        "automatic_retry": None,
                    }
                state = str(status["state"])
                return {
                    "schema_version": "runtime.effect_request_status_response.v1",
                    "ok": True,
                    "reason": "REQUEST_STATUS_READ",
                    **status,
                    "automatic_retry": False
                    if state in {"EXECUTION_STARTED", "OUTCOME_UNKNOWN"}
                    or status["effect_scope_state"] == "EFFECT_OBSERVED_AWAITING_VERIFICATION"
                    else None,
                }
            if schema_version == "runtime.effect_reconciliation_request.v1":
                return self._reconcile(value)
            request = EffectRequest.from_mapping(value)
            if request.intent_type not in self.allowed_intents:
                return self._response(False, "INTENT_NOT_OWNED_BY_EXECUTOR", "REJECTED", request=request)
            if parse_utc(request.issued_at) > datetime.now(UTC):
                return self._response(False, "REQUEST_ISSUED_IN_FUTURE", "REJECTED", request=request)
            if parse_utc(request.expires_at) <= datetime.now(UTC):
                return self._response(False, "REQUEST_EXPIRED", "REJECTED", request=request)
            current = dict(self.current_target())
            mismatch = self._identity_mismatch(request, current)
            if mismatch:
                return self._response(False, mismatch, "REJECTED", request=request)
            accepted = self.ledger.accept(request)
            if not bool(accepted.get("accepted")):
                return self._response(
                    False,
                    str(accepted["reason"]),
                    str(accepted["state"]),
                    request=request,
                    replay=bool(accepted.get("replay")),
                    result=accepted.get("result"),
                )
            if not self.ledger.transition(request.request_id, expected="ACCEPTED", state="EXECUTION_STARTED"):
                return self._response(False, "EXECUTION_START_TRANSITION_FAILED", "OUTCOME_UNKNOWN", request=request)
            revalidated = dict(self.current_target())
            revalidation_mismatch = self._identity_mismatch(request, revalidated)
            if revalidation_mismatch:
                result = {
                    "reason": "EFFECT_BOUNDARY_IDENTITY_DRIFT",
                    "identity_mismatch": revalidation_mismatch,
                    "physical_effect_count": 0,
                }
                self._safe_transition(
                    request.request_id,
                    expected="EXECUTION_STARTED",
                    state="EFFECT_FAILED",
                    result=result,
                )
                return self._response(
                    False,
                    "EFFECT_BOUNDARY_IDENTITY_DRIFT",
                    "EFFECT_FAILED",
                    request=request,
                    replay=bool(accepted.get("replay")),
                    result=result,
                )
            audit_hook_failure = False
            if self.before_effect is not None:
                try:
                    self.before_effect(request)
                except BaseException:  # audit/P2-disabled observation must not change the legacy effect decision
                    audit_hook_failure = True
            if not self.ledger.mark_effect_boundary(request.request_id):
                self._safe_transition(
                    request.request_id,
                    expected="EXECUTION_STARTED",
                    state="OUTCOME_UNKNOWN",
                    result={"reason": "EFFECT_SCOPE_BOUNDARY_RESERVATION_FAILED"},
                )
                return self._response(False, "EFFECT_SCOPE_BOUNDARY_RESERVATION_FAILED", "OUTCOME_UNKNOWN", request=request)
            if self.execute_async:
                self._start_effect_worker(request, audit_hook_failure=audit_hook_failure)
                return self._response(
                    True,
                    "EFFECT_ACCEPTED_EXECUTION_STARTED",
                    "EXECUTION_STARTED",
                    request=request,
                    result={
                        "execution_async": True,
                        "effect_result_pending": True,
                    },
                )
            result = dict(self.perform_effect(request))
            if audit_hook_failure:
                result["audit_hook_failure"] = True
                result["production_behavior_modified_by_audit"] = False
            persisted = self._safe_transition(
                request.request_id,
                expected="EXECUTION_STARTED",
                state="EFFECT_OBSERVED",
                result=result,
                scope_state=(
                    "EFFECT_OBSERVED_AWAITING_VERIFICATION"
                    if self.require_transport_verification and request.intent_type == RESTART_FFMPEG
                    else "EFFECT_OBSERVED"
                ),
            )
            if not persisted:
                return self._response(
                    False,
                    "EFFECT_RESULT_PERSISTENCE_FAILED",
                    "OUTCOME_UNKNOWN",
                    request=request,
                    result={**result, "result_persisted": False},
                )
            return self._response(True, "EFFECT_OBSERVED", "EFFECT_OBSERVED", request=request, result=result)
        except OutcomeUnknown as exc:
            request_id = request.request_id if request is not None else ""
            if request_id:
                self._safe_transition(
                    request_id,
                    expected="EXECUTION_STARTED",
                    state="OUTCOME_UNKNOWN",
                    result=exc.result,
                )
            return self._response(
                False,
                "OUTCOME_UNKNOWN",
                "OUTCOME_UNKNOWN",
                request=request,
                result=exc.result,
            )
        except EffectRejected as exc:
            request_id = request.request_id if request is not None else ""
            result = {"reason": str(exc), "physical_effect_count": 0}
            if request_id:
                self._safe_transition(request_id, expected="EXECUTION_STARTED", state="EFFECT_FAILED", result=result)
            return self._response(False, str(exc), "EFFECT_FAILED", request=request, result=result)
        except (ProtocolError, json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            return self._response(False, str(exc), "REJECTED", request=request)
        except BaseException as exc:
            request_id = request.request_id if request is not None else ""
            if request_id:
                self._safe_transition(
                    request_id, expected="EXECUTION_STARTED", state="OUTCOME_UNKNOWN", result={"error_type": type(exc).__name__}
                )
            return self._response(False, "EXECUTOR_INTERNAL_ERROR", "OUTCOME_UNKNOWN", request=request)

    def _safe_transition(
        self,
        request_id: str,
        *,
        expected: str,
        state: str,
        result: dict[str, Any] | None = None,
        scope_state: str | None = None,
    ) -> bool:
        """Persist a state transition without allowing a storage fault to kill the executor loop."""

        try:
            return self.ledger.transition(
                request_id,
                expected=expected,
                state=state,
                result=result,
                scope_state=scope_state,
            )
        except BaseException:
            return False

    def _start_effect_worker(self, request: EffectRequest, *, audit_hook_failure: bool) -> None:
        worker = threading.Thread(
            target=self._complete_effect,
            args=(request,),
            kwargs={"audit_hook_failure": audit_hook_failure},
            name=f"fast-recovery-effect-{request.request_id[:24]}",
            daemon=True,
        )
        with self._workers_lock:
            self._workers.add(worker)
        try:
            worker.start()
        except BaseException:
            with self._workers_lock:
                self._workers.discard(worker)
            raise

    def _complete_effect(self, request: EffectRequest, *, audit_hook_failure: bool) -> None:
        try:
            result = dict(self.perform_effect(request))
            if audit_hook_failure:
                result["audit_hook_failure"] = True
                result["production_behavior_modified_by_audit"] = False
            self._safe_transition(
                request.request_id,
                expected="EXECUTION_STARTED",
                state="EFFECT_OBSERVED",
                result=result,
                scope_state=(
                    "EFFECT_OBSERVED_AWAITING_VERIFICATION"
                    if self.require_transport_verification and request.intent_type == RESTART_FFMPEG
                    else "EFFECT_OBSERVED"
                ),
            )
        except OutcomeUnknown as exc:
            self._safe_transition(
                request.request_id,
                expected="EXECUTION_STARTED",
                state="OUTCOME_UNKNOWN",
                result=exc.result,
            )
        except EffectRejected as exc:
            self._safe_transition(
                request.request_id,
                expected="EXECUTION_STARTED",
                state="EFFECT_FAILED",
                result={"reason": str(exc), "physical_effect_count": 0},
            )
        except BaseException as exc:
            self._safe_transition(
                request.request_id,
                expected="EXECUTION_STARTED",
                state="OUTCOME_UNKNOWN",
                result={"reason": "EXECUTOR_INTERNAL_ERROR", "error_type": type(exc).__name__},
            )
        finally:
            with self._workers_lock:
                self._workers.discard(threading.current_thread())

    def _reconcile(self, value: Mapping[str, Any]) -> dict[str, Any]:
        required = {
            "schema_version",
            "reconciliation_id",
            "effect_scope_id",
            "owner_request_id",
            "owner_request_digest",
            "resolution",
            "evidence",
        }
        if set(value) != required:
            raise ProtocolError("RECONCILIATION_FIELDS_NOT_EXACT")
        text_fields = {
            name: str(value.get(name) or "").strip()
            for name in ("reconciliation_id", "effect_scope_id", "owner_request_id", "owner_request_digest", "resolution")
        }
        if any(not item for item in text_fields.values()):
            raise ProtocolError("RECONCILIATION_IDENTITY_MISSING")
        resolution = text_fields["resolution"]
        if resolution not in {"EFFECT_OBSERVED", "TARGET_RETIRED"}:
            raise ProtocolError("AUTOMATIC_RECONCILIATION_RESOLUTION_NOT_ALLOWED")
        evidence = value.get("evidence")
        if not isinstance(evidence, Mapping):
            raise ProtocolError("RECONCILIATION_EVIDENCE_NOT_OBJECT")
        common_evidence_fields = {
            "schema_version",
            "oracle",
            "observed_at",
            "automatic_retry_count",
            "before_target",
            "observed_target",
            "runtime_observation_id",
            "transport",
        }
        evidence_fields = common_evidence_fields | (
            {"physical_effect_count"} if resolution == "EFFECT_OBSERVED" else {"physical_attempt_count", "physical_effect_outcome"}
        )
        if set(evidence) != evidence_fields:
            raise ProtocolError("RECONCILIATION_EVIDENCE_FIELDS_NOT_EXACT")
        evidence_schema = str(evidence.get("schema_version") or "")
        if resolution == "EFFECT_OBSERVED":
            expected_oracles = {
                "runtime.delayed_exit_reconciliation_evidence.v1": "DELAYED_FFMPEG_EXIT_AND_HEALTHY_SUCCESSOR",
                "runtime.delayed_exit_reconciliation_evidence.v2": "DELAYED_FFMPEG_EXIT_AND_SUCCESSOR_ACK_PROGRESS",
            }
            if evidence_schema not in expected_oracles:
                raise ProtocolError("RECONCILIATION_EVIDENCE_SCHEMA_INVALID")
            if evidence.get("oracle") != expected_oracles[evidence_schema]:
                raise ProtocolError("RECONCILIATION_ORACLE_INVALID")
            if evidence.get("physical_effect_count") != 1:
                raise ProtocolError("RECONCILIATION_ATTEMPT_COUNT_INVALID")
        else:
            if evidence_schema != "runtime.retired_target_reconciliation_evidence.v1":
                raise ProtocolError("RECONCILIATION_EVIDENCE_SCHEMA_INVALID")
            if evidence.get("oracle") != "EXACT_TARGET_RETIRED_AND_HEALTHY_REPLACEMENT":
                raise ProtocolError("RECONCILIATION_ORACLE_INVALID")
            if evidence.get("physical_attempt_count") != 1 or evidence.get("physical_effect_outcome") != "UNKNOWN":
                raise ProtocolError("RECONCILIATION_ATTEMPT_COUNT_INVALID")
        observed_at = parse_utc(evidence.get("observed_at"))
        age = (datetime.now(UTC) - observed_at).total_seconds()
        if age < -1 or age > 30:
            raise ProtocolError("RECONCILIATION_EVIDENCE_STALE_OR_FUTURE")
        if evidence.get("automatic_retry_count") != 0:
            raise ProtocolError("RECONCILIATION_ATTEMPT_COUNT_INVALID")
        if not str(evidence.get("runtime_observation_id") or "").strip():
            raise ProtocolError("RECONCILIATION_OBSERVATION_ID_MISSING")
        before = validate_target(evidence.get("before_target"))
        observed = validate_target(evidence.get("observed_target"))
        scope = self.ledger.scope(text_fields["effect_scope_id"])
        if scope is None or json.loads(str(scope["identity_json"])) != before:
            raise ProtocolError("RECONCILIATION_BEFORE_TARGET_NOT_BOUND")
        current = dict(self.current_target())
        current_target = current.get("ffmpeg_target_identity", current.get("target_identity"))
        if current_target != observed:
            raise ProtocolError("RECONCILIATION_OBSERVED_TARGET_NOT_CURRENT")
        transport = evidence.get("transport")
        legacy_transport_fields = {"bytes_sent", "network_down", "tcp_probe_ok"}
        ack_transport_fields = legacy_transport_fields | {
            "ffmpeg_generation",
            "ack_observation_count",
            "bytes_acked_start",
            "bytes_acked_end",
            "bytes_acked_delta",
            "ack_samples",
        }
        expected_transport_fields = (
            ack_transport_fields if evidence_schema == "runtime.delayed_exit_reconciliation_evidence.v2" else legacy_transport_fields
        )
        if not isinstance(transport, Mapping) or set(transport) != expected_transport_fields:
            raise ProtocolError("RECONCILIATION_TRANSPORT_FIELDS_INVALID")
        if (
            isinstance(transport.get("bytes_sent"), bool)
            or not isinstance(transport.get("bytes_sent"), int)
            or int(transport["bytes_sent"]) <= 0
            or transport.get("network_down") is not False
            or transport.get("tcp_probe_ok") is not True
        ):
            raise ProtocolError("RECONCILIATION_TRANSPORT_NOT_HEALTHY")
        if evidence_schema == "runtime.delayed_exit_reconciliation_evidence.v2":
            successor_generation = str(observed["ffmpeg_generation"])
            if str(transport.get("ffmpeg_generation") or "") != successor_generation:
                raise ProtocolError("RECONCILIATION_ACK_GENERATION_MISMATCH")
            count = transport.get("ack_observation_count")
            start = transport.get("bytes_acked_start")
            end = transport.get("bytes_acked_end")
            delta = transport.get("bytes_acked_delta")
            samples = transport.get("ack_samples")
            integers = (count, start, end, delta)
            if any(isinstance(item, bool) or not isinstance(item, int) for item in integers):
                raise ProtocolError("RECONCILIATION_ACK_AGGREGATE_INVALID")
            count_value = cast(int, count)
            start_value = cast(int, start)
            end_value = cast(int, end)
            delta_value = cast(int, delta)
            if (
                count_value < 3
                or count_value > 16
                or start_value < 0
                or end_value <= start_value
                or delta_value != end_value - start_value
                or not isinstance(samples, list)
                or len(samples) != count_value
            ):
                raise ProtocolError("RECONCILIATION_ACK_AGGREGATE_INVALID")
            previous_time: datetime | None = None
            previous_acked: int | None = None
            for sample in samples:
                if not isinstance(sample, Mapping) or set(sample) != {
                    "observed_at",
                    "ffmpeg_pid",
                    "ffmpeg_generation",
                    "bytes_acked",
                }:
                    raise ProtocolError("RECONCILIATION_ACK_SAMPLES_INVALID")
                if (
                    isinstance(sample.get("ffmpeg_pid"), bool)
                    or not isinstance(sample.get("ffmpeg_pid"), int)
                    or int(sample["ffmpeg_pid"]) != int(observed["ffmpeg_pid"])
                    or str(sample.get("ffmpeg_generation") or "") != successor_generation
                ):
                    raise ProtocolError("RECONCILIATION_ACK_GENERATION_MISMATCH")
                acked = sample.get("bytes_acked")
                if isinstance(acked, bool) or not isinstance(acked, int) or int(acked) < 0:
                    raise ProtocolError("RECONCILIATION_ACK_SAMPLES_INVALID")
                sample_time = parse_utc(sample.get("observed_at"))
                sample_age = (datetime.now(UTC) - sample_time).total_seconds()
                if sample_age < -1 or sample_age > 30 or sample_time > observed_at + timedelta(seconds=1):
                    raise ProtocolError("RECONCILIATION_ACK_SAMPLE_STALE_OR_FUTURE")
                if previous_time is not None and sample_time <= previous_time:
                    raise ProtocolError("RECONCILIATION_ACK_SAMPLES_INVALID")
                if previous_acked is not None and int(acked) <= previous_acked:
                    raise ProtocolError("RECONCILIATION_ACK_NOT_CONSECUTIVELY_PROGRESSING")
                previous_time = sample_time
                previous_acked = int(acked)
            if int(samples[0]["bytes_acked"]) != start_value or int(samples[-1]["bytes_acked"]) != end_value:
                raise ProtocolError("RECONCILIATION_ACK_AGGREGATE_INVALID")
        if resolution == "EFFECT_OBSERVED":
            stable_fields = ("host_id", "host_boot_id", "namespace", "pod_uid", "container_name", "container_id")
            if any(before[name] != observed[name] for name in stable_fields):
                raise ProtocolError("RECONCILIATION_RUNTIME_IDENTITY_DRIFT")
            if before["ffmpeg_pid"] == observed["ffmpeg_pid"] or before["ffmpeg_generation"] == observed["ffmpeg_generation"]:
                raise ProtocolError("RECONCILIATION_SUCCESSOR_NOT_DISTINCT")
            result = self.ledger.record_reconciliation(
                reconciliation_id=text_fields["reconciliation_id"],
                effect_scope_id=text_fields["effect_scope_id"],
                resolution=resolution,
                evidence=dict(evidence),
                owner_request_id=text_fields["owner_request_id"],
                owner_request_digest=text_fields["owner_request_digest"],
            )
            response_state = "RECONCILED_EFFECT_OBSERVED"
        else:
            if any(before[name] != observed[name] for name in ("host_id", "namespace", "container_name")):
                raise ProtocolError("RECONCILIATION_REPLACEMENT_DOMAIN_MISMATCH")
            if all(before[name] == observed[name] for name in ("host_boot_id", "pod_uid", "container_id")):
                raise ProtocolError("RECONCILIATION_OLD_TARGET_NOT_RETIRED")
            result = self.ledger.record_target_retirement(
                reconciliation_id=text_fields["reconciliation_id"],
                effect_scope_id=text_fields["effect_scope_id"],
                evidence=dict(evidence),
                owner_request_id=text_fields["owner_request_id"],
                owner_request_digest=text_fields["owner_request_digest"],
            )
            response_state = "RETIRED_TARGET_OUTCOME_UNKNOWN"
        return {
            "schema_version": "runtime.effect_reconciliation_response.v1",
            "ok": True,
            "reason": "EFFECT_SCOPE_RECONCILED",
            "state": response_state,
            **result,
            "active_authority": self.ledger.authority(),
            "in_flight_count": self.ledger.unresolved_count(),
            "automatic_retry": False if self.ledger.unresolved_count() else None,
        }

    @staticmethod
    def _identity_mismatch(request: EffectRequest, current: Mapping[str, Any]) -> str:
        if request.intent_type == RESTART_FFMPEG:
            current_target = current.get("ffmpeg_target_identity", current.get("target_identity"))
            if current_target != request.ffmpeg_target_identity:
                return "TARGET_IDENTITY_DRIFT"
            if str(current.get("ffmpeg_generation") or "") != request.expected_ffmpeg_generation:
                return "FFMPEG_GENERATION_DRIFT"
        elif current.get("runtime_identity") != request.runtime_identity:
            return "RUNTIME_IDENTITY_DRIFT"
        if str(current.get("executor_instance_id") or "") != request.expected_executor_instance_id:
            return "EXECUTOR_INSTANCE_DRIFT"
        return ""

    def _response(
        self,
        ok: bool,
        reason: str,
        state: str,
        *,
        request: EffectRequest | None = None,
        replay: bool = False,
        result: object = None,
    ) -> dict[str, Any]:
        try:
            authority = self.ledger.authority()
        except BaseException:
            authority = {}
        try:
            in_flight_count = self.ledger.unresolved_count()
        except BaseException:
            in_flight_count = -1
        return {
            "schema_version": "runtime.effect_response.v1",
            "ok": ok,
            "reason": reason,
            "state": state,
            "request_id": request.request_id if request is not None else "",
            "correlation_id": request.correlation_id if request is not None else "",
            "intent_type": request.intent_type if request is not None else "",
            "replay": replay,
            "result": result,
            "active_authority": authority,
            "in_flight_count": in_flight_count,
            "automatic_retry": (
                False
                if state == "OUTCOME_UNKNOWN" or reason in {"LOGICAL_GENERATION_PID_INVARIANT_BROKEN", "TARGET_EFFECT_UNRESOLVED"}
                else None
            ),
        }
