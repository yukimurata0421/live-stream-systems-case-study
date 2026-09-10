from __future__ import annotations

from datetime import timedelta
from typing import Any

from cra_dell_recovery.models import LocalRecoveryCandidate, TargetIdentity
from cra_dell_recovery.time import isoformat_utc, utc_now


def _second_command(environment: object, first: dict[str, Any]) -> dict[str, Any]:
    now = utc_now()
    value = {key: item for key, item in first.items() if key not in {"payload_sha256", "signature"}}
    value.update(
        {
            "message_id": "message-effect-scope-second",
            "command_id": "command-effect-scope-second",
            "authorization_id": "auth-effect-scope-second",
            "idempotency_key": "1:2:auth-effect-scope-second",
            "command_seq": 2,
            "issued_at": isoformat_utc(now),
            "expires_at": isoformat_utc(now + timedelta(seconds=30)),
        }
    )
    return environment.central_codec.encode(value)  # type: ignore[attr-defined,no-any-return]


def _expire_cooldown(environment: object) -> None:
    environment.dell.connection.execute(  # type: ignore[attr-defined]
        "UPDATE execution_attempts SET finished_at='2000-01-01T00:00:00.000Z'"
    )


def _local_candidate(target: TargetIdentity, action_id: str) -> LocalRecoveryCandidate:
    return LocalRecoveryCandidate(
        action_id,
        "stream-target",
        "restart_ffmpeg",
        "confirmed_tcp_stall",
        target,
        {"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
        isoformat_utc(utc_now()),
    )


def test_different_central_command_id_cannot_repeat_same_exact_target(environment: object) -> None:
    first = environment.command("effect-scope-first")  # type: ignore[attr-defined]
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    assert service.handle_command(first)["command_state"] == "EFFECT_OBSERVED"
    _expire_cooldown(environment)

    second = service.handle_command(_second_command(environment, first))

    assert second["reason_code"] == "EFFECT_SCOPE_ALREADY_CLAIMED"
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_central_adapter_false_success_is_kept_outcome_unknown(environment: object) -> None:
    environment.adapter.after_target = environment.target  # type: ignore[attr-defined]
    command = environment.command("unsafe-after-target")  # type: ignore[attr-defined]

    receipt = environment.service(environment.target, environment.target).handle_command(command)  # type: ignore[attr-defined]

    assert receipt["command_state"] == "OUTCOME_UNKNOWN"
    assert receipt["reason_code"] == "PHYSICAL_ADAPTER_OUTCOME_UNKNOWN"
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_central_and_local_candidates_share_the_same_durable_scope(environment: object) -> None:
    command = environment.command("central-winner")  # type: ignore[attr-defined]
    assert environment.service(environment.target, environment.target).handle_command(command)["command_state"] == "EFFECT_OBSERVED"  # type: ignore[attr-defined]
    _expire_cooldown(environment)
    environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "TEST_LAN_PARTITION")  # type: ignore[attr-defined]

    result = environment.service(environment.target, environment.target).handle_local_candidate(  # type: ignore[attr-defined]
        _local_candidate(environment.target, "local-loser")  # type: ignore[attr-defined]
    )

    assert result == "EFFECT_SCOPE_ALREADY_CLAIMED"
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_new_ffmpeg_generation_has_a_new_scope_but_still_uses_same_agent_fence(environment: object) -> None:
    command = environment.command("generation-a")  # type: ignore[attr-defined]
    assert environment.service(environment.target, environment.target).handle_command(command)["command_state"] == "EFFECT_OBSERVED"  # type: ignore[attr-defined]
    _expire_cooldown(environment)
    environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "TEST_LAN_PARTITION")  # type: ignore[attr-defined]
    changed = TargetIdentity(
        **{
            **environment.target.to_dict(),  # type: ignore[attr-defined]
            "ffmpeg_generation": "run-b:0:4200",
            "ffmpeg_pid": 4200,
        }
    )

    result = environment.service(changed, changed).handle_local_candidate(_local_candidate(changed, "local-generation-b"))  # type: ignore[attr-defined]

    assert result == "EFFECT_OBSERVED"
    assert environment.adapter.attempt_count == 2  # type: ignore[attr-defined]


def test_same_generation_with_different_pid_safe_blocks_without_second_effect(environment: object) -> None:
    command = environment.command("generation-pid-a")  # type: ignore[attr-defined]
    assert environment.service(environment.target, environment.target).handle_command(command)["command_state"] == "EFFECT_OBSERVED"  # type: ignore[attr-defined]
    _expire_cooldown(environment)
    environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "TEST_LAN_PARTITION")  # type: ignore[attr-defined]
    changed_pid = TargetIdentity(
        **{
            **environment.target.to_dict(),  # type: ignore[attr-defined]
            "ffmpeg_pid": 4200,
        }
    )

    result = environment.service(changed_pid, changed_pid).handle_local_candidate(  # type: ignore[attr-defined]
        _local_candidate(changed_pid, "local-same-generation-new-pid")
    )

    assert result == "GENERATION_PID_INVARIANT_BROKEN"
    fence = environment.dell.fence("stream-target")  # type: ignore[attr-defined]
    assert fence["authority_state"] == "SAFE_BLOCKED"
    assert fence["state_reason"] == "GENERATION_PID_INVARIANT_BROKEN"
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]
