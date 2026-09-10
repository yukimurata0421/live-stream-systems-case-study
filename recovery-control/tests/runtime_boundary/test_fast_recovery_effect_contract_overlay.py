from __future__ import annotations

import importlib.util
import json
import socket
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]
OVERLAY = ROOT / "ops/runtime_overlays/fast_recovery_effect_contract.py"


def _module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("isolated_fast_recovery_effect_contract", OVERLAY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _observation(path: Path, *, pid: int = 100) -> None:
    now = datetime.now(UTC)
    path.write_text(
        json.dumps(
            {
                "schema_version": "runtime.ffmpeg_observation.v1",
                "observed_at": now.isoformat().replace("+00:00", "Z"),
                "target_snapshot_status": "VALID",
                "target_snapshot_id": "snapshot-1",
                "target_identity": {
                    "host_id": "dell-yuki",
                    "host_boot_id": "boot-1",
                    "namespace": "stream-v3",
                    "pod_uid": "pod-1",
                    "container_name": "stream-engine",
                    "container_id": "containerd://one",
                    "ffmpeg_generation": "ffmpeg-one",
                    "ffmpeg_pid": pid,
                },
                "runtime_identity": {},
                "runtime_snapshot_status": "VALID",
                "runtime_snapshot_id": "runtime-1",
                "ffmpeg_generation": "executor:0:100",
                "ffmpeg_running": True,
                "executor_instance_id": "executor-1",
                "observation_id": "observation-1",
                "maintenance_evidence_status": "AVAILABLE",
                "projection_id": "projection-1",
                "projection_sequence": 1,
            }
        ),
        encoding="utf-8",
    )


def test_overlay_reuses_one_request_id_across_independent_correlations(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _module()
    observation = tmp_path / "observation.json"
    _observation(observation)
    payloads: list[dict[str, object]] = []

    def request(_: Path, payload: dict[str, object], *, timeout_seconds: float) -> dict[str, object]:
        assert timeout_seconds == 2.0
        payloads.append(payload)
        return {"ok": True, "state": "EFFECT_OBSERVED"}

    monkeypatch.setattr(module, "_request_effect_executor", request)
    for correlation in ("correlation-a", "correlation-b"):
        module.execute_typed_effect_request(
            socket_path=tmp_path / "effect.sock",
            runtime_observation_path=observation,
            producer_id="independent-controller",
            producer_generation=2,
            intent_type="RESTART_FFMPEG",
            failure_domain="DELIVERY_PATH",
            reason="stall",
            correlation_id=correlation,
        )

    assert payloads[0]["request_id"] == payloads[1]["request_id"]
    assert payloads[0]["idempotency_key"] == payloads[1]["idempotency_key"]
    assert payloads[0]["correlation_id"] != payloads[1]["correlation_id"]


def test_overlay_transport_timeout_replays_exact_same_payload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _module()
    observation = tmp_path / "observation.json"
    _observation(observation)
    payloads: list[dict[str, object]] = []

    def request(_: Path, payload: dict[str, object], *, timeout_seconds: float) -> dict[str, object]:
        payloads.append(payload)
        if len(payloads) == 1:
            assert timeout_seconds == 2.0
            raise TimeoutError
        assert timeout_seconds == 2.0
        return {"ok": False, "state": "OUTCOME_UNKNOWN", "replay": True}

    monkeypatch.setattr(module, "_request_effect_executor", request)
    monkeypatch.setattr(module.time, "sleep", lambda _: None)
    result = module.execute_typed_effect_request(
        socket_path=tmp_path / "effect.sock",
        runtime_observation_path=observation,
        producer_id="independent-controller",
        producer_generation=2,
        intent_type="RESTART_FFMPEG",
        failure_domain="DELIVERY_PATH",
        reason="stall",
        correlation_id="correlation-a",
    )

    assert result["state"] == "OUTCOME_UNKNOWN"
    assert payloads[0] == payloads[1]


def test_controller_overlay_queries_unresolved_without_executor_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    socket_path = tmp_path / "effect.sock"
    ready = threading.Event()

    def serve() -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(socket_path))
            server.listen(1)
            ready.set()
            connection, _ = server.accept()
            with connection:
                request = json.loads(connection.recv(4096).split(b"\n", 1)[0])
                assert request == {"schema_version": "runtime.effect_unresolved_query.v1"}
                response = {
                    "schema_version": "runtime.effect_unresolved_response.v1",
                    "ok": True,
                    "unresolved_count": 0,
                    "unresolved_scopes": [],
                }
                connection.sendall((json.dumps(response) + "\n").encode())

    thread = threading.Thread(target=serve)
    thread.start()
    assert ready.wait(timeout=2)
    monkeypatch.setitem(sys.modules, "runtime_boundary", None)
    overlay = _module()

    response = overlay.unresolved_effect_scopes(socket_path=socket_path)  # type: ignore[attr-defined]

    thread.join(timeout=2)
    assert not thread.is_alive()
    assert response["unresolved_count"] == 0


def test_controller_overlay_reads_terminal_request_without_replaying_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    payloads: list[dict[str, object]] = []

    def request(_: Path, payload: dict[str, object], *, timeout_seconds: float) -> dict[str, object]:
        assert timeout_seconds == 2.0
        payloads.append(payload)
        return {
            "schema_version": "runtime.effect_request_status_response.v1",
            "ok": True,
            "reason": "REQUEST_STATUS_READ",
            "request_id": "request-1",
            "state": "EFFECT_OBSERVED",
            "effect_scope_state": "EFFECT_OBSERVED",
            "effect_scope_id": "scope-1",
            "effect_boundary_reached": True,
            "physical_attempt_count": 1,
            "result": {"exit_observed": True},
            "automatic_retry": None,
        }

    monkeypatch.setattr(module, "_request_effect_executor", request)
    response = module.effect_request_status(socket_path=tmp_path / "effect.sock", request_id="request-1")

    assert payloads == [
        {
            "schema_version": "runtime.effect_request_status_query.v1",
            "request_id": "request-1",
        }
    ]
    assert response["result"]["exit_observed"] is True


def test_controller_overlay_resolves_correlation_without_replaying_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    payloads: list[dict[str, object]] = []

    def request(_: Path, payload: dict[str, object], *, timeout_seconds: float) -> dict[str, object]:
        assert timeout_seconds == 2.0
        payloads.append(payload)
        return {
            "schema_version": "runtime.effect_correlation_status_response.v1",
            "ok": True,
            "reason": "CORRELATION_STATUS_READ",
            "correlation_id": "fra-1",
            "matching_count": 1,
            "request_id": "dell-target-1",
            "state": "OUTCOME_UNKNOWN",
            "effect_scope_state": "RECONCILED_EFFECT_OBSERVED",
            "effect_scope_id": "scope-1",
            "effect_boundary_reached": True,
            "physical_attempt_count": 1,
            "result": {"exit_observed": False},
            "automatic_retry": None,
        }

    monkeypatch.setattr(module, "_request_effect_executor", request)
    response = module.effect_request_status_by_correlation(
        socket_path=tmp_path / "effect.sock",
        correlation_id="fra-1",
    )

    assert payloads == [
        {
            "schema_version": "runtime.effect_correlation_status_query.v1",
            "correlation_id": "fra-1",
        }
    ]
    assert response["request_id"] == "dell-target-1"
    assert response["effect_scope_state"] == "RECONCILED_EFFECT_OBSERVED"


@pytest.mark.parametrize(
    "response",
    [
        {
            "schema_version": "runtime.effect_correlation_status_response.v1",
            "ok": True,
            "correlation_id": "fra-1",
            "matching_count": 0,
            "request_id": "",
            "state": "UNKNOWN",
            "effect_scope_state": "UNKNOWN",
            "physical_attempt_count": 0,
        },
        {
            "schema_version": "runtime.effect_correlation_status_response.v1",
            "ok": False,
            "correlation_id": "fra-1",
            "matching_count": 1,
            "request_id": "dell-target-1",
            "state": "OUTCOME_UNKNOWN",
            "effect_scope_state": "OUTCOME_UNKNOWN",
            "physical_attempt_count": 1,
        },
        {
            "schema_version": "runtime.effect_correlation_status_response.v1",
            "ok": False,
            "correlation_id": "wrong-correlation",
            "matching_count": 0,
            "request_id": "",
            "state": "UNKNOWN",
            "effect_scope_state": "UNKNOWN",
            "physical_attempt_count": 0,
        },
    ],
)
def test_controller_overlay_rejects_invalid_correlation_status_shape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    response: dict[str, object],
) -> None:
    module = _module()
    monkeypatch.setattr(module, "_request_effect_executor", lambda *_args, **_kwargs: response)

    with pytest.raises(ValueError, match="EFFECT_CORRELATION_STATUS_RESPONSE_INVALID"):
        module.effect_request_status_by_correlation(
            socket_path=tmp_path / "effect.sock",
            correlation_id="fra-1",
        )


def _scope() -> dict[str, object]:
    return {
        "effect_scope_id": "scope-1",
        "owner_request_id": "request-1",
        "owner_request_digest": "a" * 64,
        "identity": {
            "host_id": "dell-yuki",
            "host_boot_id": "boot-1",
            "namespace": "stream-v3",
            "pod_uid": "pod-1",
            "container_name": "stream-engine",
            "container_id": "containerd://one",
            "ffmpeg_generation": "ffmpeg-one",
            "ffmpeg_pid": 100,
        },
    }


def _runtime_observation(*, pod_uid: str, container_id: str, generation: str, pid: int) -> dict[str, object]:
    return {
        "target_snapshot_status": "VALID",
        "observation_id": "observation-successor",
        "target_identity": {
            "host_id": "dell-yuki",
            "host_boot_id": "boot-1",
            "namespace": "stream-v3",
            "pod_uid": pod_uid,
            "container_name": "stream-engine",
            "container_id": container_id,
            "ffmpeg_generation": generation,
            "ffmpeg_pid": pid,
        },
    }


def _ack_transport(*, generation: str = "ffmpeg-two", pid: int = 200) -> dict[str, object]:
    now = datetime.now(UTC)
    samples = [
        {
            "observed_at": now.replace(microsecond=index).isoformat(),
            "ffmpeg_pid": pid,
            "ffmpeg_generation": generation,
            "bytes_acked": acked,
        }
        for index, acked in enumerate((100, 200, 300), start=1)
    ]
    return {
        "bytes_sent": 4096,
        "network_down": False,
        "tcp_probe_ok": True,
        "ffmpeg_generation": generation,
        "ack_observation_count": 3,
        "bytes_acked_start": 100,
        "bytes_acked_end": 300,
        "bytes_acked_delta": 200,
        "ack_samples": samples,
    }


def test_overlay_selects_delayed_exit_or_target_retirement_without_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _module()
    payloads: list[dict[str, object]] = []

    def request(_: Path, payload: dict[str, object], *, timeout_seconds: float) -> dict[str, object]:
        assert timeout_seconds == 2.0
        payloads.append(payload)
        return {"ok": True}

    monkeypatch.setattr(module, "_request_effect_executor", request)
    transport = _ack_transport()
    module.reconcile_delayed_effect(
        socket_path=tmp_path / "effect.sock",
        unresolved_scope=_scope(),
        runtime_observation=_runtime_observation(
            pod_uid="pod-1",
            container_id="containerd://one",
            generation="ffmpeg-two",
            pid=200,
        ),
        transport=transport,
    )
    module.reconcile_delayed_effect(
        socket_path=tmp_path / "effect.sock",
        unresolved_scope=_scope(),
        runtime_observation=_runtime_observation(
            pod_uid="pod-2",
            container_id="containerd://two",
            generation="ffmpeg-three",
            pid=300,
        ),
        transport=transport,
    )

    assert payloads[0]["resolution"] == "EFFECT_OBSERVED"
    assert payloads[0]["evidence"]["physical_effect_count"] == 1  # type: ignore[index]
    assert payloads[0]["evidence"]["schema_version"] == "runtime.delayed_exit_reconciliation_evidence.v2"  # type: ignore[index]
    assert payloads[0]["evidence"]["transport"]["bytes_acked_delta"] == 200  # type: ignore[index]
    assert payloads[1]["resolution"] == "TARGET_RETIRED"
    assert payloads[1]["evidence"]["physical_effect_outcome"] == "UNKNOWN"  # type: ignore[index]
    assert payloads[1]["evidence"]["physical_attempt_count"] == 1  # type: ignore[index]


def test_overlay_refuses_to_retire_across_a_different_target_domain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _module()
    observation = _runtime_observation(
        pod_uid="pod-2",
        container_id="containerd://two",
        generation="ffmpeg-three",
        pid=300,
    )
    observation["target_identity"]["namespace"] = "other"  # type: ignore[index]
    called = False

    def request(_: Path, payload: dict[str, object], *, timeout_seconds: float) -> dict[str, object]:
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(module, "_request_effect_executor", request)

    with pytest.raises(ValueError, match="RECONCILIATION_TARGET_RELATION_UNSAFE"):
        module.reconcile_delayed_effect(
            socket_path=tmp_path / "effect.sock",
            unresolved_scope=_scope(),
            runtime_observation=observation,
            transport=_ack_transport(generation="ffmpeg-three", pid=300),
        )
    assert called is False
