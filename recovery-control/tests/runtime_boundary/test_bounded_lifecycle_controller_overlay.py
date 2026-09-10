from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

from tools.build_runtime_fence_release import replace_controller_functions

ROOT = Path(__file__).resolve().parents[2]
BASE_CONTROLLER = ROOT / "artifacts/runtime-fence-476ba5e78f32-v16/controller/src"
CONTROLLER_BASE = BASE_CONTROLLER / "watchers/fast_recovery.py"
CONTROLLER_OVERLAY = ROOT / "ops/runtime_overlays/fast_recovery_bounded_lifecycle.py"
DECISION_BASE = BASE_CONTROLLER / "watchers/fast_recovery_core/decision.py"
DECISION_OVERLAY = ROOT / "ops/runtime_overlays/fast_recovery_youtube_freshness.py"


def load_module(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def controller(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    generated = replace_controller_functions(
        CONTROLLER_BASE.read_text(encoding="utf-8"),
        CONTROLLER_OVERLAY.read_text(encoding="utf-8"),
    )
    path = tmp_path / "candidate_fast_recovery.py"
    path.write_text(generated, encoding="utf-8")
    monkeypatch.syspath_prepend(str(BASE_CONTROLLER / "watchers"))
    monkeypatch.syspath_prepend(str(BASE_CONTROLLER))
    return load_module(path, f"candidate_fast_recovery_{id(path)}")


def target(*, pid: int = 200, generation: str = "successor-generation") -> dict[str, object]:
    return {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-1",
        "namespace": "stream-v3",
        "pod_uid": "pod-1",
        "container_name": "stream-engine",
        "container_id": "containerd://one",
        "ffmpeg_generation": generation,
        "ffmpeg_pid": pid,
    }


def ack_samples(*, pid: int = 200, generation: str = "successor-generation") -> list[dict[str, object]]:
    return [
        {
            "observed_at": f"2026-09-01T22:00:{second:02d}Z",
            "ffmpeg_pid": pid,
            "ffmpeg_generation": generation,
            "bytes_acked": acked,
        }
        for second, acked in ((0, 1000), (10, 2000), (20, 3500))
    ]


def transport(*, acked: int = 3500) -> dict[str, object]:
    return {
        "metrics": {"bytes_sent": 8192, "bytes_acked": acked},
        "network": {"network_down": False, "tcp_probe_ok": True},
    }


def test_dispatch_process_state_is_tri_state_not_false_without_exit_evidence(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[object, ...]] = []
    monkeypatch.setattr(controller, "append_event", lambda *args: events.append(args))

    controller.append_recovery_dispatch_result(
        action={"recovery_scope": "ffmpeg_child"},
        reason_kind="tcp_stall",
        reason="stall",
        ffmpeg_pid=100,
        ok=False,
        detail="OUTCOME_UNKNOWN",
        automatic_retry=False,
    )

    assert events[0][2]["process_still_running_after_dispatch"] is None


def test_ack_samples_are_generation_bound_and_reset_on_gap_or_generation_change(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = {"target_identity": target()}
    monkeypatch.setattr(controller.effect_contract, "read_runtime_observation", lambda _path: current)
    monkeypatch.setattr(controller, "TCP_SEND_SAMPLE_LOG_SEC", 60)
    state: dict[str, object] = {}

    for now_ts, acked in ((100, 1000), (110, 2000), (120, 3500)):
        controller.maybe_append_tcp_send_sample(
            state,
            now_ts=now_ts,
            ffmpeg_pid=200,
            bytes_sent=8192 + now_ts,
            metrics={"bytes_acked": acked},
        )

    samples = state["successor_ack_samples"]
    assert [item["bytes_acked"] for item in samples] == [1000, 2000, 3500]
    assert {item["ffmpeg_generation"] for item in samples} == {"successor-generation"}

    current["target_identity"] = target(generation="newer-generation")
    controller.maybe_append_tcp_send_sample(
        state,
        now_ts=130,
        ffmpeg_pid=200,
        bytes_sent=9000,
        metrics={"bytes_acked": 4000},
    )
    assert len(state["successor_ack_samples"]) == 1
    assert state["successor_ack_samples"][0]["ffmpeg_generation"] == "newer-generation"


def test_terminal_executor_result_waits_for_three_successor_ack_samples(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ts = 1_788_300_030  # 2026-09-01T22:00:30Z
    state = {
        "pending_recovery_actions": [
            {
                "recovery_action_id": "request-1",
                "requested_at_ts": now_ts - 30,
                "requested_ffmpeg_pid": 100,
                "requested_ffmpeg_generation": "old-generation",
                "executor_request_state": "EFFECT_OBSERVED",
                "executor_result": {"exit_observed": True, "signal_attempt_count": 1},
            }
        ],
        "successor_ack_samples": ack_samples(),
    }
    events: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        controller.effect_contract,
        "read_runtime_observation",
        lambda _path: {"target_identity": target()},
    )
    monkeypatch.setattr(controller, "append_event", lambda *args: events.append(args))

    controller.maybe_record_recovery_completed(
        state,
        now_ts=now_ts,
        ffmpeg_pid=200,
        ffmpeg_uptime_sec=30,
        transport_snapshot=transport(),
        youtube_hint={"fresh": False},
    )

    assert state["pending_recovery_actions"] == []
    assert events[0][0] == "recovery_completed"
    assert events[0][2]["transport_verification"]["bytes_acked_delta"] == 2500
    assert events[0][2]["executor_result"]["exit_observed"] is True


def test_frozen_ack_keeps_terminal_result_pending(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ts = 1_788_300_030
    samples = ack_samples()
    samples[-1]["bytes_acked"] = samples[-2]["bytes_acked"]
    state = {
        "pending_recovery_actions": [
            {
                "recovery_action_id": "request-1",
                "requested_ffmpeg_pid": 100,
                "requested_ffmpeg_generation": "old-generation",
                "executor_request_state": "EFFECT_OBSERVED",
            }
        ],
        "successor_ack_samples": samples,
    }
    monkeypatch.setattr(
        controller.effect_contract,
        "read_runtime_observation",
        lambda _path: {"target_identity": target()},
    )

    controller.maybe_record_recovery_completed(
        state,
        now_ts=now_ts,
        ffmpeg_pid=200,
        ffmpeg_uptime_sec=30,
        transport_snapshot=transport(acked=2000),
        youtube_hint={},
    )

    assert len(state["pending_recovery_actions"]) == 1


@pytest.mark.parametrize("oracle_gap", ["missing_observation", "successor_stopped", "stale_samples"])
def test_oracle_gap_never_resolves_successor_transport(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    oracle_gap: str,
) -> None:
    now_ts = 1_788_300_030
    before = target(pid=100, generation="old-generation")
    scope = {
        "effect_scope_id": "scope-1",
        "owner_request_id": "request-1",
        "owner_request_digest": "a" * 64,
        "identity": before,
        "created_at": "2026-09-01T22:00:00Z",
    }
    samples = ack_samples()
    if oracle_gap == "stale_samples":
        for sample in samples:
            sample["observed_at"] = "2026-09-01T21:50:00Z"
    state = {"successor_ack_samples": samples, "pending_recovery_actions": []}
    observation = {} if oracle_gap == "missing_observation" else {"target_identity": target()}
    monkeypatch.setattr(controller.effect_contract, "read_runtime_observation", lambda _path: observation)
    reconciliations: list[dict[str, object]] = []
    monkeypatch.setattr(
        controller.effect_contract,
        "reconcile_delayed_effect",
        lambda **kwargs: reconciliations.append(kwargs),
    )

    resolved = controller.maybe_reconcile_delayed_executor_effects(
        state,
        unresolved_scopes=[scope],
        now_ts=now_ts,
        ffmpeg_pid=0 if oracle_gap == "successor_stopped" else 200,
        ffmpeg_uptime_sec=0 if oracle_gap == "successor_stopped" else 30,
        transport_snapshot=transport(),
        youtube_hint={},
    )

    assert resolved is False
    assert reconciliations == []


def test_unresolved_effect_reconciles_only_with_ack_v2(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ts = 1_788_300_030
    before = target(pid=100, generation="old-generation")
    scope = {
        "effect_scope_id": "scope-1",
        "owner_request_id": "request-1",
        "owner_request_digest": "a" * 64,
        "identity": before,
        "created_at": "2026-09-01T22:00:00Z",
    }
    state = {"successor_ack_samples": ack_samples(), "pending_recovery_actions": []}
    captured: list[dict[str, object]] = []
    events: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        controller.effect_contract,
        "read_runtime_observation",
        lambda _path: {
            "target_snapshot_status": "VALID",
            "observation_id": "observation-1",
            "target_identity": target(),
        },
    )
    monkeypatch.setattr(
        controller.effect_contract,
        "effect_request_status",
        lambda **_kwargs: {"result": {"reason": "SIGTERM_SENT_EXIT_NOT_OBSERVED"}},
        raising=False,
    )

    def reconcile(**kwargs):
        captured.append(kwargs)
        return {
            "ok": True,
            "state": "RECONCILED_EFFECT_OBSERVED",
            "reconciliation_id": "reconciliation-1",
            "evidence_digest": "b" * 64,
        }

    monkeypatch.setattr(controller.effect_contract, "reconcile_delayed_effect", reconcile)
    monkeypatch.setattr(controller, "append_event", lambda *args: events.append(args))

    assert (
        controller.maybe_reconcile_delayed_executor_effects(
            state,
            unresolved_scopes=[scope],
            now_ts=now_ts,
            ffmpeg_pid=200,
            ffmpeg_uptime_sec=30,
            transport_snapshot=transport(),
            youtube_hint={},
        )
        is True
    )

    evidence_transport = captured[0]["transport"]
    assert evidence_transport["ffmpeg_generation"] == "successor-generation"
    assert evidence_transport["ack_observation_count"] == 3
    assert evidence_transport["bytes_acked_delta"] == 2500
    assert events[0][2]["executor_result"]["reason"] == "SIGTERM_SENT_EXIT_NOT_OBSERVED"


def test_sync_keeps_terminal_result_until_scope_reconciled(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statuses = {
        "state": "EFFECT_OBSERVED",
        "effect_scope_state": "EFFECT_OBSERVED_AWAITING_VERIFICATION",
        "result": {"exit_observed": True, "signal_attempt_count": 2},
        "physical_attempt_count": 1,
    }
    monkeypatch.setattr(
        controller.effect_contract,
        "effect_request_status",
        lambda **_kwargs: statuses,
        raising=False,
    )
    state: dict[str, object] = {"pending_recovery_actions": []}
    scope = {
        "owner_request_id": "request-1",
        "owner_request_digest": "a" * 64,
        "effect_scope_id": "scope-1",
        "state": "EFFECT_OBSERVED_AWAITING_VERIFICATION",
        "identity": target(pid=100, generation="old-generation"),
    }

    controller.sync_pending_recovery_from_executor(state, [scope])

    pending = state["pending_recovery_actions"][0]
    assert pending["awaiting_transport_verification"] is True
    assert pending["process_still_running_after_dispatch"] is False
    assert pending["executor_result"]["signal_attempt_count"] == 2

    statuses["effect_scope_state"] = "RECONCILED_EFFECT_OBSERVED"
    controller.sync_pending_recovery_from_executor(state, [])
    assert state["pending_recovery_actions"] == []


def test_v21_migrates_incident_correlation_to_reconciled_owner(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    correlation_id = "fra-1788384540-d5862eb9ffd8"
    owner_request_id = "dell-target-986b413a"
    state: dict[str, object] = {
        "pending_recovery_actions": [
            {
                "recovery_action_id": correlation_id,
                "requested_ffmpeg_pid": 4084557,
                "executor_request_state": "UNKNOWN",
                "executor_scope_state": "UNKNOWN",
            }
        ]
    }
    monkeypatch.setattr(
        controller.effect_contract,
        "effect_request_status_by_correlation",
        lambda **_kwargs: {
            "ok": True,
            "reason": "CORRELATION_STATUS_READ",
            "correlation_id": correlation_id,
            "matching_count": 1,
            "request_id": owner_request_id,
            "state": "OUTCOME_UNKNOWN",
            "effect_scope_state": "RECONCILED_EFFECT_OBSERVED",
            "effect_scope_id": "scope-incident",
            "physical_attempt_count": 1,
            "result": {"effect": "SIGTERM", "exit_observed": False},
        },
        raising=False,
    )
    monkeypatch.setattr(
        controller.effect_contract,
        "effect_request_status",
        lambda **_kwargs: {
            "ok": True,
            "state": "OUTCOME_UNKNOWN",
            "effect_scope_state": "RECONCILED_EFFECT_OBSERVED",
            "effect_scope_id": "scope-incident",
            "physical_attempt_count": 1,
            "result": {"effect": "SIGTERM", "exit_observed": False},
        },
        raising=False,
    )

    controller.sync_pending_recovery_from_executor(state, [])

    assert state["pending_recovery_actions"] == []
    assert controller.pending_recovery_count(state) == 0


@pytest.mark.parametrize(
    ("lookup", "expected_reason"),
    [
        (
            {
                "ok": False,
                "reason": "CORRELATION_NOT_FOUND",
                "matching_count": 0,
                "request_id": "",
            },
            "CORRELATION_NOT_FOUND",
        ),
        (
            {
                "ok": False,
                "reason": "CORRELATION_AMBIGUOUS",
                "matching_count": 2,
                "request_id": "",
            },
            "CORRELATION_AMBIGUOUS",
        ),
    ],
)
def test_v21_unknown_or_ambiguous_correlation_stays_fail_closed(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    lookup: dict[str, object],
    expected_reason: str,
) -> None:
    state: dict[str, object] = {
        "pending_recovery_actions": [
            {
                "recovery_action_id": "fra-provisional",
                "requested_ffmpeg_pid": 100,
            }
        ]
    }
    monkeypatch.setattr(
        controller.effect_contract,
        "effect_request_status_by_correlation",
        lambda **_kwargs: lookup,
        raising=False,
    )

    controller.sync_pending_recovery_from_executor(state, [])

    assert controller.pending_recovery_count(state) == 1
    pending = state["pending_recovery_actions"][0]
    assert pending["executor_request_state"] == "UNKNOWN"
    assert pending["executor_correlation_reason"] == expected_reason


def test_v21_correlation_query_failure_stays_fail_closed(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state: dict[str, object] = {"pending_recovery_actions": [{"recovery_action_id": "fra-provisional"}]}

    def unavailable(**_kwargs):
        raise TimeoutError

    monkeypatch.setattr(
        controller.effect_contract,
        "effect_request_status_by_correlation",
        unavailable,
        raising=False,
    )

    controller.sync_pending_recovery_from_executor(state, [])

    pending = state["pending_recovery_actions"][0]
    assert pending["executor_request_state"] == "UNKNOWN"
    assert pending["executor_correlation_status_error"] == "TimeoutError"


def test_v21_unresolved_scope_merges_with_provisional_action_without_duplicate(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    correlation_id = "fra-provisional"
    owner_request_id = "dell-target-formal"
    state: dict[str, object] = {
        "pending_recovery_actions": [
            {
                "recovery_action_id": correlation_id,
                "requested_at_ts": 100,
                "requested_ffmpeg_pid": 100,
            },
            {
                "recovery_action_id": owner_request_id,
                "owner_request_id": owner_request_id,
                "requested_ffmpeg_pid": 100,
            },
        ]
    }
    status = {
        "ok": True,
        "reason": "CORRELATION_STATUS_READ",
        "correlation_id": correlation_id,
        "matching_count": 1,
        "request_id": owner_request_id,
        "state": "OUTCOME_UNKNOWN",
        "effect_scope_state": "OUTCOME_UNKNOWN",
        "effect_scope_id": "scope-1",
        "physical_attempt_count": 1,
        "result": {"exit_observed": False},
    }
    monkeypatch.setattr(
        controller.effect_contract,
        "effect_request_status_by_correlation",
        lambda **_kwargs: status,
        raising=False,
    )
    monkeypatch.setattr(
        controller.effect_contract,
        "effect_request_status",
        lambda **_kwargs: status,
        raising=False,
    )
    scope = {
        "owner_request_id": owner_request_id,
        "owner_request_digest": "a" * 64,
        "effect_scope_id": "scope-1",
        "state": "OUTCOME_UNKNOWN",
        "identity": target(pid=100, generation="old-generation"),
    }

    controller.sync_pending_recovery_from_executor(state, [scope])

    assert controller.pending_recovery_count(state) == 1
    pending = state["pending_recovery_actions"][0]
    assert pending["recovery_action_id"] == correlation_id
    assert pending["correlation_id"] == correlation_id
    assert pending["owner_request_id"] == owner_request_id
    assert pending["effect_scope_id"] == "scope-1"


def test_v21_remember_pending_persists_formal_owner_identity(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    correlation_id = "fra-new-action"
    owner_request_id = "dell-target-new-action"
    monkeypatch.setattr(
        controller.effect_contract,
        "effect_request_status_by_correlation",
        lambda **_kwargs: {
            "ok": True,
            "reason": "CORRELATION_STATUS_READ",
            "correlation_id": correlation_id,
            "matching_count": 1,
            "request_id": owner_request_id,
            "state": "EXECUTION_STARTED",
            "effect_scope_state": "EXECUTION_STARTED",
            "effect_scope_id": "scope-new-action",
            "physical_attempt_count": 1,
            "result": None,
        },
        raising=False,
    )
    monkeypatch.setattr(controller, "EFFECT_EXECUTOR_SOCKET", "/tmp/isolated-effect.sock")
    state: dict[str, object] = {}

    controller.remember_pending_recovery(
        state,
        action={"recovery_action_id": correlation_id, "recovery_scope": "ffmpeg_child"},
        now_ts=123,
        reason_kind="tcp_stall",
        reason="stalled",
        ffmpeg_pid=100,
    )

    pending = state["pending_recovery_actions"][0]
    assert pending["recovery_action_id"] == correlation_id
    assert pending["correlation_id"] == correlation_id
    assert pending["owner_request_id"] == owner_request_id
    assert pending["executor_request_state"] == "EXECUTION_STARTED"


def test_youtube_hint_suppresses_stale_values_but_preserves_freshness_metadata(tmp_path: Path) -> None:
    generated = replace_controller_functions(
        DECISION_BASE.read_text(encoding="utf-8"),
        DECISION_OVERLAY.read_text(encoding="utf-8"),
    )
    path = tmp_path / "candidate_decision.py"
    path.write_text(generated, encoding="utf-8")
    decision = load_module(path, f"candidate_decision_{id(path)}")
    stale = decision.youtube_hint(
        {
            "ts_utc": "2026-05-31T00:00:00Z",
            "api_live_state": "live",
            "oauth_stream_health_status": "good",
            "_controller_status_age_sec": 8_000_000,
            "_controller_status_max_age_sec": 180,
            "_controller_status_fresh": False,
        }
    )
    fresh = decision.youtube_hint(
        {
            "ts_utc": "2026-09-01T22:00:00Z",
            "api_live_state": "live",
            "oauth_stream_health_status": "good",
            "_controller_status_age_sec": 10,
            "_controller_status_max_age_sec": 180,
            "_controller_status_fresh": True,
        }
    )

    assert stale["api_live_state"] == ""
    assert stale["oauth_stream_health_status"] == ""
    assert stale["fresh"] is False
    assert stale["stale_values_suppressed"] is True
    assert stale["source_observed_at"] == "2026-05-31T00:00:00Z"
    assert fresh["api_live_state"] == "live"
    assert fresh["oauth_stream_health_status"] == "good"
    assert fresh["fresh"] is True


def test_controller_overlay_emits_source_timed_ack_measurement_without_threshold(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current: dict[str, object] = {
        "schema_version": "runtime.ffmpeg_observation.v1",
        "observed_at": "2026-09-03T00:00:00Z",
        "producer_instance_id": "isolated-producer",
        "sequence": 1,
        "protocol_ffmpeg_pid": 200,
        "ffmpeg_generation": "native-generation",
        "target_identity": target(),
        "tcp_metrics": {
            "bytes_sent": 1_000_000,
            "bytes_acked": 999_000,
            "send_q": 0,
            "notsent": 0,
            "unacked": 0,
            "lastsnd_ms": 5,
            "rto_ms": 250,
        },
    }
    events: list[tuple[object, ...]] = []
    monkeypatch.setattr(controller.effect_contract, "read_runtime_observation", lambda _path: current)
    monkeypatch.setattr(controller, "append_event", lambda *args: events.append(args))
    monkeypatch.delenv("FR_ACK_RATE_RESTART_THRESHOLD_MBPS", raising=False)
    monkeypatch.delenv("FR_ACK_RATE_ACTION_ENABLED", raising=False)
    state: dict[str, object] = {}

    controller.maybe_append_tcp_send_sample(
        state,
        now_ts=1_788_393_600,
        ffmpeg_pid=200,
        bytes_sent=1_000_000,
        metrics=dict(current["tcp_metrics"]),
    )
    current["observed_at"] = "2026-09-03T00:01:00Z"
    current["sequence"] = 7
    current_metrics = dict(current["tcp_metrics"])
    current_metrics.update(bytes_sent=37_000_000, bytes_acked=36_999_000)
    current["tcp_metrics"] = current_metrics
    controller.maybe_append_tcp_send_sample(
        state,
        now_ts=1_788_393_660,
        ffmpeg_pid=200,
        bytes_sent=37_000_000,
        metrics=current_metrics,
    )

    measurement = state["ack_delivery_measurement_v1"]
    assert measurement["status"] == "VALID"
    assert measurement["reason_code"] == "ACK_RATE_MEASURE_ONLY_THRESHOLD_UNSET"
    assert measurement["latest"]["ack_mbps"] == 4.8
    assert measurement["action_enabled"] is False
    assert measurement["restart_candidate_confirmed"] is False
    ack_events = [event for event in events if event[0] == "tcp_ack_delivery_measurement"]
    assert len(ack_events) == 1
    assert ack_events[0][2]["physical_effect_count"] == 0
    assert "history" not in ack_events[0][2]["ack_delivery"]


@pytest.mark.parametrize(
    ("action_enabled", "baseline_ready", "network_down", "tcp_probe_ok", "expected_kind"),
    [
        (True, True, False, True, "tcp_stall"),
        (False, True, False, True, ""),
        (True, False, False, True, ""),
        (True, True, True, True, ""),
        (True, True, False, False, ""),
    ],
)
def test_measured_ack_candidate_is_fail_closed_before_exact_child_policy(
    tmp_path: Path,
    controller: ModuleType,
    action_enabled: bool,
    baseline_ready: bool,
    network_down: bool,
    tcp_probe_ok: bool,
    expected_kind: str,
) -> None:
    generated = replace_controller_functions(
        DECISION_BASE.read_text(encoding="utf-8"),
        DECISION_OVERLAY.read_text(encoding="utf-8"),
    )
    path = tmp_path / "candidate_ack_decision.py"
    path.write_text(generated, encoding="utf-8")
    decision = load_module(path, f"candidate_ack_decision_{id(path)}_{action_enabled}_{baseline_ready}")
    tcp = decision.TcpObservation(
        metrics={"bytes_sent": 10_000, "bytes_acked": 5_000},
        bytes_sent=10_000,
        prev_bytes_sent=9_000,
        prev_bytes_ts=100,
        bytes_delta=1_000,
        bytes_elapsed_sec=10,
        send_mbps=0.001,
        notsent=600_000,
        unacked=100,
        lastsnd_ms=2_000,
        stall_now=False,
        low_upload_pressure_now=False,
    )
    network = decision.NetworkObservation(
        gateway="gateway",
        gateway_ok=True,
        public_ok_count=2,
        dns_ok=True,
        tcp_probe_ok=tcp_probe_ok,
        network_down=network_down,
    )
    state = {
        "net_fail_streak": 0,
        "stall_streak": 0,
        "ack_delivery_measurement_v1": {
            "schema_version": "stream_v3.ack_delivery_measurement.v1",
            "status": "VALID",
            "measurement_enabled": True,
            "action_enabled": action_enabled,
            "configured_restart_threshold_mbps": 0.48,
            "below_configured_threshold": True,
            "restart_confirmations": 2,
            "shadow_low_streak": 2,
            "restart_candidate_confirmed": True,
            "statistics": {"baseline_ready": baseline_ready},
            "latest": {
                "ack_mbps": 0.1,
                "queue_pressure": True,
                "pending_effect": False,
            },
        },
    }

    reason_kind, reason = decision.select_restart_reason(
        state,
        url_preservation_mode=True,
        remote_warning_streak=0,
        remote_warning_confirm=1,
        remote_warning_reason="",
        network=network,
        net_fail_confirm=1,
        stall_confirm=2,
        low_upload_confirm=3,
        low_upload_max_mbps=3.2,
        tcp=tcp,
    )

    assert reason_kind == expected_kind
    if expected_kind:
        assert "measured ACK delivery rate low" in reason
        typed = controller.recovery_policy.select_recovery_intent(reason_kind)
        assert typed.intent_type == controller.recovery_policy.RESTART_FFMPEG
        assert typed.effect_scope == "ffmpeg_child"
        assert typed.automatic_retry is False
    else:
        assert reason == ""


def test_source_target_mismatch_invalidates_prior_ack_restart_candidate(
    controller: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observation = {
        "schema_version": "runtime.ffmpeg_observation.v1",
        "observed_at": "2026-09-03T00:00:00Z",
        "producer_instance_id": "isolated-producer",
        "sequence": 1,
        "protocol_ffmpeg_pid": 201,
        "ffmpeg_generation": "native-generation",
        "target_identity": target(pid=201),
        "tcp_metrics": {
            "bytes_sent": 1_000_000,
            "bytes_acked": 999_000,
            "send_q": 600_000,
            "notsent": 600_000,
            "unacked": 100,
            "lastsnd_ms": 2_000,
            "rto_ms": 250,
        },
    }
    monkeypatch.setattr(controller.effect_contract, "read_runtime_observation", lambda _path: observation)
    state: dict[str, object] = {
        "successor_ack_samples": [{"ffmpeg_pid": 200}],
        "ack_delivery_measurement_v1": {
            "schema_version": "stream_v3.ack_delivery_measurement.v1",
            "status": "VALID",
            "restart_candidate_confirmed": True,
        },
    }

    controller.maybe_append_tcp_send_sample(
        state,
        now_ts=1_788_393_600,
        ffmpeg_pid=200,
        bytes_sent=1_000_000,
        metrics=dict(observation["tcp_metrics"]),
    )

    assert state["successor_ack_samples"] == []
    measurement = state["ack_delivery_measurement_v1"]
    assert measurement["status"] == "UNKNOWN"
    assert measurement["restart_candidate_confirmed"] is False
