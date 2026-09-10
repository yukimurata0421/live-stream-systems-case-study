from __future__ import annotations

from typing import Any

import pytest

from cra_dell_recovery.effect_scope import effect_scope_id
from cra_dell_recovery.models import TargetIdentity
from dell_recovery_agent.runtime_effect_adapter import RuntimeEffectAdapter


def _target() -> TargetIdentity:
    return TargetIdentity(
        host_id="dell",
        host_boot_id="boot-a",
        namespace="stream-v3",
        pod_uid="pod-a",
        container_name="stream-engine",
        container_id="containerd://a",
        ffmpeg_generation="generation-a",
        ffmpeg_pid=4100,
    )


class Client:
    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.payloads: list[dict[str, object]] = []

    def execute(self, payload: dict[str, object]) -> dict[str, Any]:
        self.payloads.append(payload)
        return self.response


def test_adapter_uses_stable_scope_id_and_only_delegates_to_executor() -> None:
    target = _target()
    after = TargetIdentity(**{**target.to_dict(), "ffmpeg_generation": "generation-b", "ffmpeg_pid": 4200})
    client = Client({"ok": True, "state": "EFFECT_OBSERVED"})
    adapter = RuntimeEffectAdapter(
        client,
        producer_id="dell-agent",
        producer_generation=4,
        executor_instance_id="executor-a",
        observe_after=lambda: after,
        projection=lambda: ("projection-a", 12, "AVAILABLE"),
    )

    result = adapter.restart_ffmpeg(target)

    expected_scope = effect_scope_id("restart_ffmpeg", target)
    assert result.effect_observed is True
    assert result.after_target == after
    assert client.payloads[0]["request_id"] == f"dell-scope-{expected_scope}"
    assert client.payloads[0]["idempotency_key"] == f"dell-scope-{expected_scope}"
    assert client.payloads[0]["ffmpeg_target_identity"] == target.to_dict()


def test_adapter_propagates_unknown_without_retry() -> None:
    client = Client({"ok": False, "state": "OUTCOME_UNKNOWN"})
    adapter = RuntimeEffectAdapter(
        client,
        producer_id="dell-agent",
        producer_generation=4,
        executor_instance_id="executor-a",
        observe_after=lambda: _target(),
        projection=lambda: ("", 0, "UNKNOWN"),
    )

    with pytest.raises(RuntimeError, match="OPTION_BD_OUTCOME_UNKNOWN"):
        adapter.restart_ffmpeg(_target())
    assert len(client.payloads) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {},
        {"ffmpeg_generation": "generation-b"},
        {"ffmpeg_pid": 4200},
        {"host_id": "other-dell", "ffmpeg_generation": "generation-b", "ffmpeg_pid": 4200},
        {"host_boot_id": "boot-b", "ffmpeg_generation": "generation-b", "ffmpeg_pid": 4200},
        {"namespace": "other", "ffmpeg_generation": "generation-b", "ffmpeg_pid": 4200},
        {"pod_uid": "pod-b", "ffmpeg_generation": "generation-b", "ffmpeg_pid": 4200},
        {"container_id": "containerd://b", "ffmpeg_generation": "generation-b", "ffmpeg_pid": 4200},
    ],
)
def test_adapter_rejects_effect_observed_for_non_successor_target(changes: dict[str, object]) -> None:
    target = _target()
    after = TargetIdentity.from_dict({**target.to_dict(), **changes})
    client = Client({"ok": True, "state": "EFFECT_OBSERVED"})
    adapter = RuntimeEffectAdapter(
        client,
        producer_id="dell-agent",
        producer_generation=4,
        executor_instance_id="executor-a",
        observe_after=lambda: after,
        projection=lambda: ("projection-a", 12, "AVAILABLE"),
    )

    with pytest.raises(RuntimeError, match="POST_EFFECT_TARGET_RELATION_UNSAFE"):
        adapter.restart_ffmpeg(target)
    assert len(client.payloads) == 1


@pytest.mark.parametrize(
    ("generation", "lifetime"),
    [(0, 5.0), (-1, 5.0), (1, 0.0), (1, -1.0), (1, 5.1)],
)
def test_adapter_rejects_invalid_identity_generation_or_lifetime(generation: int, lifetime: float) -> None:
    with pytest.raises(ValueError):
        RuntimeEffectAdapter(
            Client({"ok": True, "state": "EFFECT_OBSERVED"}),
            producer_id="dell-agent",
            producer_generation=generation,
            executor_instance_id="executor-a",
            observe_after=_target,
            projection=lambda: ("projection-a", 12, "AVAILABLE"),
            request_lifetime_seconds=lifetime,
        )


def test_adapter_returns_terminal_rejection_without_observing_after() -> None:
    observed = False

    def observe() -> TargetIdentity:
        nonlocal observed
        observed = True
        return _target()

    adapter = RuntimeEffectAdapter(
        Client({"ok": False, "state": "EFFECT_REJECTED", "reason": "EXECUTOR_REJECTED"}),
        producer_id="dell-agent",
        producer_generation=1,
        executor_instance_id="executor-a",
        observe_after=observe,
        projection=lambda: ("projection-a", 12, "AVAILABLE"),
    )

    result = adapter.restart_ffmpeg(_target())
    assert result.effect_observed is False
    assert result.reason_code == "EXECUTOR_REJECTED"
    assert observed is False


def test_adapter_fails_unknown_when_post_effect_target_is_unavailable() -> None:
    adapter = RuntimeEffectAdapter(
        Client({"ok": True, "state": "EFFECT_OBSERVED"}),
        producer_id="dell-agent",
        producer_generation=1,
        executor_instance_id="executor-a",
        observe_after=lambda: None,
        projection=lambda: ("projection-a", 12, "AVAILABLE"),
    )

    with pytest.raises(RuntimeError, match="POST_EFFECT_TARGET_UNAVAILABLE"):
        adapter.restart_ffmpeg(_target())
