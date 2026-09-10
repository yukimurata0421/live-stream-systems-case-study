from __future__ import annotations

import pytest

from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.states import (
    AuthorityState,
    CommandState,
    IncidentState,
    TargetHealthState,
    require_transition,
)


@pytest.mark.parametrize(
    ("current", "new"),
    [
        (TargetHealthState.UNKNOWN, TargetHealthState.HEALTHY),
        (TargetHealthState.CONFIRMED_DEGRADED, TargetHealthState.RECOVERING),
        (IncidentState.OPEN, IncidentState.CONFIRMED),
        (IncidentState.AUTHORIZED, IncidentState.RECOVERY_REQUESTED),
        (AuthorityState.CENTRAL_ACTIVE, AuthorityState.CENTRAL_SUSPECT),
        (AuthorityState.LOCAL_FALLBACK, AuthorityState.RECONCILING),
        (CommandState.OUTBOX_PENDING, CommandState.SENT),
        (CommandState.EXECUTION_STARTED, CommandState.OUTCOME_UNKNOWN),
    ],
)
def test_valid_state_transitions(current: object, new: object) -> None:
    require_transition(current, new)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("current", "new"),
    [
        (TargetHealthState.HEALTHY, TargetHealthState.RECOVERING),
        (IncidentState.CLOSED, IncidentState.OPEN),
        (AuthorityState.LOCAL_FALLBACK, AuthorityState.CENTRAL_ACTIVE),
        (CommandState.OUTCOME_UNKNOWN, CommandState.EXECUTION_STARTED),
        (CommandState.VERIFIED, CommandState.SENT),
    ],
)
def test_invalid_state_transitions_are_rejected(current: object, new: object) -> None:
    with pytest.raises(ValueError, match="invalid transition"):
        require_transition(current, new)  # type: ignore[arg-type]


def test_state_machine_mismatch_is_rejected() -> None:
    with pytest.raises(ValueError, match="state machine mismatch"):
        require_transition(TargetHealthState.HEALTHY, IncidentState.OPEN)


def test_target_identity_round_trip() -> None:
    target = TargetIdentity("dell", "boot", "default", "pod", "stream-engine", "cid", "g", 20)
    assert TargetIdentity.from_dict(target.to_dict()) == target


@pytest.mark.parametrize("field", ["host_id", "pod_uid", "container_id", "ffmpeg_generation"])
def test_target_identity_rejects_empty_required_field(field: str) -> None:
    values: dict[str, object] = {
        "host_id": "dell",
        "host_boot_id": "boot",
        "namespace": "default",
        "pod_uid": "pod",
        "container_name": "stream-engine",
        "container_id": "cid",
        "ffmpeg_generation": "gen",
        "ffmpeg_pid": 22,
    }
    values[field] = ""
    with pytest.raises(ValueError):
        TargetIdentity(**values)  # type: ignore[arg-type]


def test_target_identity_rejects_pid_one() -> None:
    with pytest.raises(ValueError, match="greater than 1"):
        TargetIdentity("dell", "boot", "default", "pod", "stream-engine", "cid", "gen", 1)
