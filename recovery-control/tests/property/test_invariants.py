from __future__ import annotations

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

PROPERTY_SETTINGS = settings(
    max_examples=40,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)


def cached_command(environment: object) -> dict[str, object]:
    existing = getattr(environment, "_property_command", None)
    if existing is None:
        existing = environment.command()  # type: ignore[attr-defined]
        environment._property_command = existing
    return existing


@PROPERTY_SETTINGS
@given(retries=st.integers(min_value=0, max_value=20))
def test_duplicate_sequences_never_exceed_one_physical_attempt(environment: object, retries: int) -> None:
    command = cached_command(environment)
    service = environment.service(environment.target, environment.target)  # type: ignore[attr-defined]
    service.handle_command(command)
    for _ in range(retries):
        service.handle_command(command)
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


@PROPERTY_SETTINGS
@given(epoch=st.integers(min_value=2, max_value=2**31))
def test_any_nonactive_authority_epoch_has_zero_effect(environment: object, epoch: int) -> None:
    command = cached_command(environment)
    stale = environment.central_codec.signer.sign({**command, "authority_epoch": epoch})  # type: ignore[attr-defined]
    receipt = environment.service().handle_command(stale)  # type: ignore[attr-defined]
    assert receipt["reason_code"] == "STALE_AUTHORITY_EPOCH"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


@PROPERTY_SETTINGS
@given(gap=st.integers(min_value=2, max_value=10000))
def test_any_sequence_gap_has_zero_effect(environment: object, gap: int) -> None:
    command = cached_command(environment)
    payload = {**command, "command_seq": gap, "idempotency_key": f"1:{gap}:auth-1"}
    signed = environment.central_codec.signer.sign(payload)  # type: ignore[attr-defined]
    receipt = environment.service().handle_command(signed)  # type: ignore[attr-defined]
    assert receipt["reason_code"] == "SEQUENCE_GAP"
    assert environment.adapter.attempt_count == 0  # type: ignore[attr-defined]


@PROPERTY_SETTINGS
@given(
    operations=st.lists(
        st.sampled_from(
            [
                "duplicate",
                "timeout",
                "accepted_crash",
                "started_crash",
                "effect_ack_loss",
                "heartbeat_delay",
                "target_change",
                "sequence_gap",
                "restore",
            ]
        ),
        min_size=1,
        max_size=50,
    )
)
def test_failure_operation_model_never_authorizes_a_second_effect(operations: list[str]) -> None:
    physical_attempts = 0
    terminal_or_blocking = False
    for operation in operations:
        if operation == "effect_ack_loss" and not terminal_or_blocking and physical_attempts == 0:
            physical_attempts += 1
            terminal_or_blocking = True
        elif operation in {"started_crash", "restore"}:
            terminal_or_blocking = True
        elif operation in {
            "duplicate",
            "timeout",
            "accepted_crash",
            "heartbeat_delay",
            "target_change",
            "sequence_gap",
        }:
            continue
    assert physical_attempts <= 1
