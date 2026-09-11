from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Sequence

from .ids import stable_id, validate_id
from .observation import _name
from .time import parse_utc, require_not_before


RUNTIME_ACTIONS = frozenset({"restart_dj", "restart_ffmpeg", "restart_workload"})
ACTION_RESULTS = frozenset({"accepted", "rejected", "started", "completed", "failed"})


def _target(value: str) -> str:
    """Validate a typed target such as ``deployment/stream-v3-runtime``."""

    if not isinstance(value, str) or not value:
        raise ValueError(f"invalid target: {value!r}")
    parts = value.split("/")
    if len(parts) != 2:
        raise ValueError("target must use kind/name syntax")
    _name(parts[0], field="target_kind")
    _name(parts[1], field="target_name")
    return value


@dataclass(frozen=True)
class RecoveryAuthorization:
    SCHEMA: ClassVar[str] = "monitoring_v4.recovery_authorization.v1"

    authorization_id: str
    episode_id: str
    action: str
    target: str
    authorized_at: str
    expires_at: str
    policy_revision: str
    blockers: tuple[str, ...]

    def __post_init__(self) -> None:
        validate_id(self.authorization_id, field="authorization_id")
        validate_id(self.episode_id, field="episode_id")
        _name(self.action, field="runtime_action", allowed=RUNTIME_ACTIONS)
        _target(self.target)
        parse_utc(self.authorized_at, field="authorized_at")
        parse_utc(self.expires_at, field="expires_at")
        require_not_before(
            self.expires_at,
            self.authorized_at,
            later_field="expires_at",
            earlier_field="authorized_at",
        )
        if not self.policy_revision:
            raise ValueError("policy_revision is required")
        for blocker in self.blockers:
            _name(blocker, field="blocker")
        object.__setattr__(self, "blockers", tuple(sorted(set(self.blockers))))

    @classmethod
    def create(
        cls,
        *,
        episode_id: str,
        action: str,
        target: str,
        authorized_at: str,
        expires_at: str,
        policy_revision: str,
        blockers: Sequence[str] = (),
    ) -> "RecoveryAuthorization":
        ident = stable_id("auth", episode_id, action, target, authorized_at, policy_revision)
        return cls(
            ident,
            episode_id,
            action,
            target,
            authorized_at,
            expires_at,
            policy_revision,
            tuple(blockers),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "authorization_id": self.authorization_id,
            "episode_id": self.episode_id,
            "action": self.action,
            "target": self.target,
            "authorized_at": self.authorized_at,
            "expires_at": self.expires_at,
            "policy_revision": self.policy_revision,
            "blockers": list(self.blockers),
        }


@dataclass(frozen=True)
class RuntimeControlCommand:
    SCHEMA: ClassVar[str] = "monitoring_v4.runtime_control_command.v1"

    command_id: str
    authorization_id: str
    action: str
    target: str
    issued_at: str
    expires_at: str
    idempotency_key: str
    target_generation: str

    def __post_init__(self) -> None:
        validate_id(self.command_id, field="command_id")
        validate_id(self.authorization_id, field="authorization_id")
        _name(self.action, field="runtime_action", allowed=RUNTIME_ACTIONS)
        _target(self.target)
        parse_utc(self.issued_at, field="issued_at")
        parse_utc(self.expires_at, field="expires_at")
        require_not_before(
            self.expires_at,
            self.issued_at,
            later_field="expires_at",
            earlier_field="issued_at",
        )
        if not self.idempotency_key or len(self.idempotency_key) > 200:
            raise ValueError("idempotency_key must contain 1-200 characters")
        if not self.target_generation or len(self.target_generation) > 200:
            raise ValueError("target_generation must contain 1-200 characters")

    @classmethod
    def create(
        cls,
        *,
        authorization_id: str,
        action: str,
        target: str,
        issued_at: str,
        expires_at: str,
        idempotency_key: str,
        target_generation: str,
    ) -> "RuntimeControlCommand":
        ident = stable_id("cmd", authorization_id, action, target, idempotency_key, target_generation)
        return cls(
            ident,
            authorization_id,
            action,
            target,
            issued_at,
            expires_at,
            idempotency_key,
            target_generation,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "command_id": self.command_id,
            "authorization_id": self.authorization_id,
            "action": self.action,
            "target": self.target,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "idempotency_key": self.idempotency_key,
            "target_generation": self.target_generation,
        }


@dataclass(frozen=True)
class RuntimeActionResult:
    SCHEMA: ClassVar[str] = "monitoring_v4.runtime_action_result.v1"

    result_id: str
    command_id: str
    status: str
    observed_at: str
    reason_code: str
    target_generation: str

    def __post_init__(self) -> None:
        validate_id(self.result_id, field="result_id")
        validate_id(self.command_id, field="command_id")
        _name(self.status, field="action_result", allowed=ACTION_RESULTS)
        parse_utc(self.observed_at, field="observed_at")
        _name(self.reason_code, field="reason_code")
        if not self.target_generation or len(self.target_generation) > 200:
            raise ValueError("target_generation must contain 1-200 characters")

    @classmethod
    def create(
        cls,
        *,
        command_id: str,
        status: str,
        observed_at: str,
        reason_code: str,
        target_generation: str,
    ) -> "RuntimeActionResult":
        ident = stable_id("act", command_id, status, observed_at, reason_code)
        return cls(ident, command_id, status, observed_at, reason_code, target_generation)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "result_id": self.result_id,
            "command_id": self.command_id,
            "status": self.status,
            "observed_at": self.observed_at,
            "reason_code": self.reason_code,
            "target_generation": self.target_generation,
        }
