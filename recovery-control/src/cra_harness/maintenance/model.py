from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any

from cra_dell_recovery.models import TargetIdentity


class MaintenanceState(StrEnum):
    REQUESTED = "REQUESTED"
    QUIESCING = "QUIESCING"
    ESTABLISHED = "ESTABLISHED"
    MUTATING = "MUTATING"
    VERIFYING_TARGET = "VERIFYING_TARGET"
    RECONCILING = "RECONCILING"
    EXIT_PENDING = "EXIT_PENDING"
    COMPLETED = "COMPLETED"
    QUIESCE_FAILED = "QUIESCE_FAILED"
    ABORTING = "ABORTING"
    ABORTED = "ABORTED"
    SAFE_BLOCKED = "SAFE_BLOCKED"


class MutatorId(StrEnum):
    CRA_COMMAND = "cra_command"
    DELL_RECOVERY_AGENT = "dell_recovery_agent"
    DELL_FAST_RECOVERY = "dell_fast_recovery"
    ARENA_REMOTE_RECOVERY = "arena_remote_recovery"
    ARENA_YOUTUBE_WATCHDOG = "arena_youtube_watchdog"
    ARENA_STREAM_WATCHDOG = "arena_stream_watchdog"
    DELL_LOCAL_FALLBACK = "dell_local_fallback"
    STREAM_ENGINE_SELF_RECOVERY = "stream_engine_self_recovery"
    KUBERNETES_AUTORECONCILE = "kubernetes_autoreconcile"


REQUIRED_MUTATORS: tuple[MutatorId, ...] = tuple(MutatorId)


class MaintenanceExecutorId(StrEnum):
    PLANNED_ROLLOUT = "planned_rollout_executor"
    MANUAL_PLANNED = "manual_planned_executor"


class MaintenanceMutationOperation(StrEnum):
    RESTART_DEPLOYMENT = "restart_deployment"


class ExpectedTargetChange(StrEnum):
    REPLACE_POD_AND_FFMPEG_GENERATION = "replace_pod_and_ffmpeg_generation"


class MutationAuthorizationState(StrEnum):
    ISSUED = "ISSUED"
    ACCEPTED = "ACCEPTED"
    EXECUTION_STARTED = "EXECUTION_STARTED"
    CONSUMED = "CONSUMED"
    EXPIRED = "EXPIRED"
    SUPERSEDED = "SUPERSEDED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    REJECTED = "REJECTED"


class FenceReleaseState(StrEnum):
    HELD = "HELD"
    RELEASE_PREPARED = "RELEASE_PREPARED"
    RELEASED = "RELEASED"


@dataclass(frozen=True)
class MaintenanceIntent:
    maintenance_id: str
    request_id: str
    requested_at: str
    requested_by: str
    target_id: str
    target_identity: TargetIdentity
    authority_epoch: int
    authority_session_id: str
    generation: int
    reason: str

    def __post_init__(self) -> None:
        required = (
            self.maintenance_id,
            self.request_id,
            self.requested_at,
            self.requested_by,
            self.target_id,
            self.authority_session_id,
            self.reason,
        )
        if any(not value.strip() for value in required):
            raise ValueError("maintenance intent string fields are required")
        if self.authority_epoch <= 0 or self.generation <= 0:
            raise ValueError("maintenance authority epoch and generation must be positive")

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["target_identity"] = self.target_identity.to_dict()
        return value

    def to_contract_dict(self) -> dict[str, Any]:
        return {
            "protocol": "maintenance.transaction.intent.v1",
            "maintenance_id": self.maintenance_id,
            "request_id": self.request_id,
            "requested_at": self.requested_at,
            "requested_by": self.requested_by,
            "target_id": self.target_id,
            "target_identity": self.target_identity.to_dict(),
            "authority_epoch": self.authority_epoch,
            "authority_session_id": self.authority_session_id,
            "maintenance_generation": self.generation,
            "reason": self.reason,
            "state": MaintenanceState.REQUESTED.value,
        }


@dataclass(frozen=True)
class QuiesceAck:
    maintenance_id: str
    generation: int
    mutator_id: MutatorId
    quiesced: bool
    persistent_fence_installed: bool
    in_flight_count: int
    last_accepted_generation: int
    last_accepted_sequence: int
    acknowledged_at: str
    target_identity: TargetIdentity

    def __post_init__(self) -> None:
        if self.generation <= 0 or self.in_flight_count < 0:
            raise ValueError("invalid quiesce acknowledgement counters")
        if self.last_accepted_generation < 0 or self.last_accepted_sequence < 0:
            raise ValueError("accepted generation/sequence cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["mutator_id"] = self.mutator_id.value
        value["target_identity"] = self.target_identity.to_dict()
        return value

    def to_contract_dict(self) -> dict[str, Any]:
        return {
            "protocol": "maintenance.transaction.quiesce_ack.v1",
            "maintenance_id": self.maintenance_id,
            "maintenance_generation": self.generation,
            "mutator_id": self.mutator_id.value,
            "quiesced": self.quiesced,
            "persistent_fence_installed": self.persistent_fence_installed,
            "in_flight_count": self.in_flight_count,
            "last_accepted_generation": self.last_accepted_generation,
            "last_accepted_sequence": self.last_accepted_sequence,
            "acknowledged_at": self.acknowledged_at,
            "target_identity": self.target_identity.to_dict(),
        }


def maintenance_operation_digest(
    *,
    maintenance_id: str,
    generation: int,
    executor_id: MaintenanceExecutorId,
    operation: MaintenanceMutationOperation,
    resource_identity: str,
    source_target_identity: TargetIdentity,
    expected_target_change: ExpectedTargetChange,
) -> str:
    payload = {
        "maintenance_id": maintenance_id,
        "maintenance_generation": generation,
        "executor_id": executor_id.value,
        "operation": operation.value,
        "resource_identity": resource_identity,
        "source_target_identity": source_target_identity.to_dict(),
        "expected_target_change": expected_target_change.value,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class MaintenanceMutationAuthorization:
    authorization_id: str
    maintenance_id: str
    generation: int
    executor_id: MaintenanceExecutorId
    operation: MaintenanceMutationOperation
    resource_identity: str
    source_target_identity: TargetIdentity
    expected_target_change: ExpectedTargetChange
    issued_authority_epoch: int
    issued_authority_session_id: str
    issued_at: str
    expires_at: str
    sequence: int
    nonce: str
    operation_digest: str
    single_use: bool = True

    def __post_init__(self) -> None:
        required = (
            self.authorization_id,
            self.maintenance_id,
            self.resource_identity,
            self.issued_authority_session_id,
            self.issued_at,
            self.expires_at,
            self.nonce,
            self.operation_digest,
        )
        if any(not value.strip() for value in required):
            raise ValueError("maintenance authorization string fields are required")
        if self.generation <= 0 or self.issued_authority_epoch <= 0 or self.sequence <= 0:
            raise ValueError("maintenance authorization counters must be positive")
        if not self.single_use:
            raise ValueError("maintenance mutation authorization must be single use")

    def expected_operation_digest(self) -> str:
        return maintenance_operation_digest(
            maintenance_id=self.maintenance_id,
            generation=self.generation,
            executor_id=self.executor_id,
            operation=self.operation,
            resource_identity=self.resource_identity,
            source_target_identity=self.source_target_identity,
            expected_target_change=self.expected_target_change,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "authorization_id": self.authorization_id,
            "maintenance_id": self.maintenance_id,
            "generation": self.generation,
            "executor_id": self.executor_id.value,
            "operation": self.operation.value,
            "resource_identity": self.resource_identity,
            "source_target_identity": self.source_target_identity.to_dict(),
            "expected_target_change": self.expected_target_change.value,
            "issued_authority_epoch": self.issued_authority_epoch,
            "issued_authority_session_id": self.issued_authority_session_id,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "sequence": self.sequence,
            "nonce": self.nonce,
            "operation_digest": self.operation_digest,
            "single_use": self.single_use,
        }

    def to_contract_dict(self) -> dict[str, Any]:
        value = self.to_dict()
        value["protocol"] = "maintenance.mutation_authorization.v1"
        value["maintenance_generation"] = value.pop("generation")
        value["authorization_kind"] = "MAINTENANCE_MUTATION"
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> MaintenanceMutationAuthorization:
        return cls(
            authorization_id=str(value["authorization_id"]),
            maintenance_id=str(value["maintenance_id"]),
            generation=int(value["generation"]),
            executor_id=MaintenanceExecutorId(str(value["executor_id"])),
            operation=MaintenanceMutationOperation(str(value["operation"])),
            resource_identity=str(value["resource_identity"]),
            source_target_identity=TargetIdentity.from_dict(value["source_target_identity"]),
            expected_target_change=ExpectedTargetChange(str(value["expected_target_change"])),
            issued_authority_epoch=int(value["issued_authority_epoch"]),
            issued_authority_session_id=str(value["issued_authority_session_id"]),
            issued_at=str(value["issued_at"]),
            expires_at=str(value["expires_at"]),
            sequence=int(value["sequence"]),
            nonce=str(value["nonce"]),
            operation_digest=str(value["operation_digest"]),
            single_use=bool(value["single_use"]),
        )


@dataclass(frozen=True)
class FenceReleaseAck:
    maintenance_id: str
    generation: int
    mutator_id: MutatorId
    prepared: bool
    fence_still_active: bool
    acknowledged_at: str
    target_identity: TargetIdentity

    def __post_init__(self) -> None:
        if not self.maintenance_id.strip() or not self.acknowledged_at.strip() or self.generation <= 0:
            raise ValueError("invalid fence release acknowledgement")

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["mutator_id"] = self.mutator_id.value
        value["target_identity"] = self.target_identity.to_dict()
        return value

    def to_contract_dict(self) -> dict[str, Any]:
        value = self.to_dict()
        value["protocol"] = "maintenance.fence_release_ready_ack.v1"
        value["maintenance_generation"] = value.pop("generation")
        return value


@dataclass(frozen=True)
class MutationDecision:
    allowed: bool
    reason_code: str
    authorization_state: str
    authorization_use_count: int
    planned_would_mutate_count: int
