from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
RECOVERY_SRC = ROOT / "recovery-control" / "src"
sys.path.insert(0, str(RECOVERY_SRC))

from runtime_boundary import EffectRejected

from stream_core.runtime_boundary_entrypoint import RuntimeBoundaryStreamEngine
from watchers import fast_recovery
from watchers.fast_recovery_core import effect_contract
from watchers.runtime_recovery_escalation import RuntimeRecoveryEscalationAdapter


def runtime_observation(*, maintenance_status: str = "UNKNOWN") -> dict[str, object]:
    return {
        "schema_version": "runtime.ffmpeg_observation.v1",
        "observation_id": "observation-1",
        "observed_at": datetime.now(UTC).isoformat(),
        "executor_instance_id": "executor-1",
        "ffmpeg_running": True,
        "ffmpeg_generation": "native-generation-1",
        "target_snapshot_status": "VALID",
        "target_snapshot_id": "target-snapshot-1",
        "target_identity": {
            "host_id": "dell-yuki",
            "host_boot_id": "boot-1",
            "namespace": "stream-v3",
            "pod_uid": "pod-1",
            "container_name": "stream-engine",
            "container_id": "containerd://container-1",
            "ffmpeg_generation": "protocol-generation-1",
            "ffmpeg_pid": 4200,
        },
        "runtime_snapshot_status": "VALID",
        "runtime_snapshot_id": "runtime-snapshot-1",
        "runtime_identity": {
            "host_id": "dell-yuki",
            "host_boot_id": "boot-1",
            "namespace": "stream-v3",
            "pod_uid": "pod-1",
            "stream_engine_container_name": "stream-engine",
            "stream_engine_container_id": "containerd://container-1",
            "runtime_generation": "runtime-generation-1",
        },
        "runtime_lifecycle_state": "FFMPEG_RUNNING",
        "managed_child_cardinality": "1",
        "maintenance_evidence_status": maintenance_status,
        "projection_id": "" if maintenance_status != "AVAILABLE" else "projection-1",
        "projection_sequence": 0 if maintenance_status != "AVAILABLE" else 1,
    }


def test_missing_maintenance_projection_does_not_change_effect_dispatch(tmp_path: Path) -> None:
    observation_path = tmp_path / "observation.json"
    observation_path.write_text(json.dumps(runtime_observation()), encoding="utf-8")
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, _path: Path, *, timeout_seconds: float) -> None:
            assert timeout_seconds == 2.0

        def execute(self, request: dict[str, object]) -> dict[str, object]:
            captured.update(request)
            return {"ok": True, "state": "EFFECT_OBSERVED", "reason": "fixture"}

    with mock.patch.dict(sys.modules, {"runtime_boundary": SimpleNamespace(EffectClient=FakeClient)}):
        result = effect_contract.execute_effect_request(
            socket_path=tmp_path / "effect.sock",
            runtime_observation_path=observation_path,
            producer_id="new-controller",
            producer_generation=2,
            reason="confirmed tcp stall",
            correlation_id="request-1",
        )

    assert result["ok"] is True
    assert captured["maintenance_evidence_status"] == "UNKNOWN"
    assert captured["projection_id"] == ""
    assert captured["projection_sequence"] == 0


def test_invalid_exact_target_is_never_sent_to_executor(tmp_path: Path) -> None:
    observation = runtime_observation()
    observation["target_snapshot_status"] = "UNKNOWN"
    observation_path = tmp_path / "observation.json"
    observation_path.write_text(json.dumps(observation), encoding="utf-8")

    unavailable_client = SimpleNamespace(EffectClient=mock.Mock(side_effect=AssertionError("must not connect")))
    with mock.patch.dict(sys.modules, {"runtime_boundary": unavailable_client}):
        result = effect_contract.execute_effect_request(
            socket_path=tmp_path / "effect.sock",
            runtime_observation_path=observation_path,
            producer_id="new-controller",
            producer_generation=2,
            reason="confirmed tcp stall",
            correlation_id="request-1",
        )

    assert result == {"ok": False, "state": "REJECTED", "reason": "TARGET_IDENTITY_UNAVAILABLE"}


def test_network_down_is_no_action_and_never_routed_to_an_executor() -> None:
    with (
        mock.patch.object(fast_recovery, "EFFECT_EXECUTOR_SOCKET", "/run/stream-v3-control/effect.sock"),
        mock.patch.object(fast_recovery.effect_contract, "read_runtime_observation", return_value=runtime_observation()),
        mock.patch.object(fast_recovery.effect_contract, "execute_typed_effect_request") as execute_typed,
        mock.patch.object(fast_recovery, "restart_stream") as restart_runtime,
    ):
        result = fast_recovery.execute_recovery_action(
            reason_kind="network_down",
            reason="fixture",
            ffmpeg_pid=4200,
            correlation_id="request-1",
        )

    assert result[0] is False
    assert result[2] == "none"
    execute_typed.assert_not_called()
    restart_runtime.assert_not_called()


def test_typed_outcome_unknown_preserves_no_retry_contract() -> None:
    response = {
        "ok": False,
        "state": "OUTCOME_UNKNOWN",
        "reason": "SIGTERM_SENT_EXIT_NOT_OBSERVED",
        "automatic_retry": False,
    }
    with (
        mock.patch.object(fast_recovery, "EFFECT_EXECUTOR_SOCKET", "/run/stream-v3-control/effect.sock"),
        mock.patch.object(fast_recovery.effect_contract, "read_runtime_observation", return_value=runtime_observation()),
        mock.patch.object(
            fast_recovery.effect_contract,
            "configured_paths",
            return_value={
                "socket": Path("/run/stream-v3-control/effect.sock"),
                "runtime_observation": Path("/run/stream-v3-control/runtime-observation.json"),
            },
        ),
        mock.patch.object(fast_recovery.effect_contract, "execute_typed_effect_request", return_value=response),
    ):
        result = fast_recovery.execute_recovery_action_with_policy(
            reason_kind="tcp_stall",
            reason="fixture",
            ffmpeg_pid=4200,
            correlation_id="request-unknown",
        )

    assert result == (
        False,
        "OUTCOME_UNKNOWN: SIGTERM_SENT_EXIT_NOT_OBSERVED",
        "ffmpeg_child",
        False,
    )


def test_unresolved_dispatch_consumes_budget_and_becomes_durable_pending_state() -> None:
    state: dict[str, object] = {}
    action = fast_recovery.new_recovery_action(
        now_ts=2_000,
        reason_kind="tcp_stall",
        reason_first_ts=1_990,
        ffmpeg_pid=4200,
        recovery_scope="ffmpeg_child",
    )
    fast_recovery.record_unresolved_recovery_dispatch(
        state,
        action=action,
        now_ts=2_000,
        reason_kind="tcp_stall",
        reason="fixture",
        ffmpeg_pid=4200,
        detail="OUTCOME_UNKNOWN: SIGTERM_SENT_EXIT_NOT_OBSERVED",
        restart_events=[],
    )

    assert fast_recovery.pending_recovery_count(state) == 1
    assert state["last_restart_ts"] == 2_000
    assert state["restart_failure_count"] == 0
    assert state["restart_events"] == [{"ts": 2_000, "downtime_sec": fast_recovery.RESTART_DOWNTIME_COST_SEC, "reason": "tcp_stall"}]


def test_planning_helper_scope_does_not_override_reachable_network_down_no_action() -> None:
    reasons = ("ffmpeg_missing", "network_down", "tcp_stall", "remote_warning")
    with (
        mock.patch.object(fast_recovery, "EFFECT_EXECUTOR_SOCKET", ""),
        mock.patch.object(fast_recovery, "k8s_supervisor_active", return_value=True),
    ):
        legacy = {reason: fast_recovery.planned_recovery_scope(reason) for reason in reasons}
    missing_observation = runtime_observation()
    missing_observation.update(
        {
            "ffmpeg_running": False,
            "target_snapshot_status": "UNKNOWN",
            "target_identity": None,
            "runtime_lifecycle_state": "RESTART_DELAY",
            "managed_child_cardinality": "0",
        }
    )
    with (
        mock.patch.object(fast_recovery, "EFFECT_EXECUTOR_SOCKET", "/run/stream-v3-control/effect.sock"),
        mock.patch.object(fast_recovery.effect_contract, "read_runtime_observation", return_value=missing_observation),
    ):
        candidate = {reason: fast_recovery.planned_recovery_scope(reason) for reason in reasons}

    assert legacy == {
        "ffmpeg_missing": "runtime",
        "network_down": "runtime",
        "tcp_stall": "ffmpeg_child",
        "remote_warning": "ffmpeg_child",
    }
    assert candidate == {
        "ffmpeg_missing": "ffmpeg_child_convergence",
        "network_down": "none",
        "tcp_stall": "ffmpeg_child",
        "remote_warning": "ffmpeg_child",
    }

    # `planned_recovery_scope()` is a planning helper, not the production
    # admission decision.  The actual network-down branch returns before the
    # restart selector; the test above proves the typed route also stays inert.


def test_ffmpeg_missing_reconcile_uses_runtime_identity_without_positive_pid(tmp_path: Path) -> None:
    observation = runtime_observation()
    observation["ffmpeg_running"] = False
    observation["target_snapshot_status"] = "UNSTABLE"
    observation["target_identity"] = None
    observation["runtime_lifecycle_state"] = "RESTART_DELAY"
    observation["managed_child_cardinality"] = "0"
    observation_path = tmp_path / "observation.json"
    observation_path.write_text(json.dumps(observation), encoding="utf-8")

    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, _path: Path, *, timeout_seconds: float) -> None:
            assert timeout_seconds == 2.0

        def execute(self, request: dict[str, object]) -> dict[str, object]:
            captured.update(request)
            return {"ok": True, "state": "EFFECT_OBSERVED", "reason": "fixture"}

    with mock.patch.dict(sys.modules, {"runtime_boundary": SimpleNamespace(EffectClient=FakeClient)}):
        result = effect_contract.execute_typed_effect_request(
            socket_path=tmp_path / "effect.sock",
            runtime_observation_path=observation_path,
            producer_id="new-controller",
            producer_generation=2,
            intent_type="RECONCILE_FFMPEG",
            failure_domain="FFMPEG_LOCAL",
            reason="ffmpeg missing",
            correlation_id="request-missing",
        )

    assert result["ok"] is True
    assert captured["intent_type"] == "RECONCILE_FFMPEG"
    assert captured["ffmpeg_target_identity"] is None
    assert captured["runtime_identity"] == observation["runtime_identity"]


def test_auto_fenced_authority_uses_durable_runtime_observation(tmp_path: Path) -> None:
    observation_path = tmp_path / "observation.json"
    observation = runtime_observation()
    observation.update({"active_producer_id": "new-controller", "active_producer_generation": 2})
    observation_path.write_text(json.dumps(observation), encoding="utf-8")
    with (
        mock.patch.object(fast_recovery, "EFFECT_EXECUTOR_SOCKET", "/run/stream-v3-control/effect.sock"),
        mock.patch.object(fast_recovery, "EFFECT_AUTHORITY_MODE", "AUTO_FENCED"),
        mock.patch.object(fast_recovery, "EFFECT_PRODUCER_ID", "new-controller"),
        mock.patch.object(fast_recovery, "EFFECT_PRODUCER_GENERATION", 2),
        mock.patch.object(fast_recovery, "RUNTIME_OBSERVATION_FILE", observation_path),
    ):
        assert fast_recovery.effect_authority_active() is True


def test_controller_runtime_contract_rejects_enforcement_and_capability_drift() -> None:
    with (
        mock.patch.dict(fast_recovery.os.environ, {"MAINTENANCE_ENFORCEMENT_ENABLED": "1"}),
        mock.patch.object(fast_recovery, "CONTROLLER_RUNTIME_MODE", "NARROW_EXECUTOR_ONLY"),
    ):
        try:
            fast_recovery.validate_controller_runtime_contract()
        except RuntimeError as exc:
            assert str(exc) == "P2_ENFORCEMENT_NOT_AUTHORIZED"
        else:
            raise AssertionError("enforcement=true must fail startup")

    with (
        mock.patch.dict(fast_recovery.os.environ, {"MAINTENANCE_ENFORCEMENT_ENABLED": "0"}),
        mock.patch.object(fast_recovery, "CONTROLLER_RUNTIME_MODE", "SHADOW_OBSERVER_ONLY"),
        mock.patch.object(fast_recovery, "EFFECT_AUTHORITY_MODE", "AUTO_FENCED"),
        mock.patch.object(fast_recovery, "GPU_PREFLIGHT_ENABLED", False),
    ):
        try:
            fast_recovery.validate_controller_runtime_contract()
        except RuntimeError as exc:
            assert str(exc) == "SHADOW_CONTROLLER_AUTHORITY_MODE_INVALID"
        else:
            raise AssertionError("shadow controller cannot own effect authority")


def test_stream_engine_owned_executor_terminates_only_current_ffmpeg_child() -> None:
    class FakeProcess:
        pid = 4200
        returncode: int | None = None

        def poll(self) -> int | None:
            return self.returncode

        def terminate(self) -> None:
            self.returncode = -15

        def wait(self, timeout: float | None = None) -> int:
            assert timeout is not None
            assert self.returncode is not None
            return self.returncode

    engine = object.__new__(RuntimeBoundaryStreamEngine)
    engine.ffmpeg_proc = FakeProcess()
    engine.run_id = "run-1"
    engine.restart_count = 3
    engine.ffmpeg_stop_context = {}
    request = SimpleNamespace(
        intent_type="RESTART_FFMPEG",
        producer_id="new-controller",
        reason="confirmed tcp stall",
        request_id="request-1",
        idempotency_key="request-1",
        expected_ffmpeg_generation="run-1:3:4200",
        target_identity={"ffmpeg_pid": 437698},
    )

    with (
        mock.patch.dict("os.environ", {"FR_FFMPEG_FORCE_KILL_ENABLED": "0"}, clear=False),
        mock.patch.object(engine, "_open_exact_ffmpeg_pidfd", return_value=None),
    ):
        result = engine.perform_fast_recovery_effect(request)

    assert result["effect"] == "SIGTERM"
    assert result["local_ffmpeg_pid"] == 4200
    assert result["protocol_host_ffmpeg_pid"] == 437698
    assert engine.ffmpeg_stop_context["recovery_action_id"] == "request-1"


def test_ffmpeg_lifecycle_wait_settings_reject_unbounded_values() -> None:
    for name in (
        "FR_FFMPEG_TERM_GRACE_SEC",
        "FR_FFMPEG_KILL_WAIT_SEC",
        "FR_RECONCILE_OBSERVE_TIMEOUT_SEC",
    ):
        with mock.patch.dict("os.environ", {name: "10.001"}, clear=False):
            try:
                RuntimeBoundaryStreamEngine._bounded_effect_wait(name, default=1.0)
            except EffectRejected as exc:
                assert str(exc) == f"{name}_OUT_OF_RANGE"
            else:
                raise AssertionError(f"{name} must remain bounded")


def test_runtime_escalation_adapter_has_one_exact_kubernetes_effect() -> None:
    adapter = object.__new__(RuntimeRecoveryEscalationAdapter)
    adapter.kubectl_bin = "kubectl"
    adapter.namespace = "stream-v3"
    adapter.runtime_target = "deployment/stream-v3-runtime"
    request = SimpleNamespace(intent_type="ESCALATE_RUNTIME_RECOVERY")
    completed = SimpleNamespace(returncode=0, stdout="", stderr="")

    with mock.patch("watchers.runtime_recovery_escalation.subprocess.run", return_value=completed) as run:
        result = adapter.perform_effect(request)

    run.assert_called_once_with(
        ["kubectl", "-n", "stream-v3", "rollout", "restart", "deployment/stream-v3-runtime"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10.0,
    )
    assert result == {
        "physical_effect_count": 1,
        "effect": "KUBERNETES_ROLLOUT_RESTART",
        "target": "deployment/stream-v3-runtime",
    }
