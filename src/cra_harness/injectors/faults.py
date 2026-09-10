from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class FaultKind(StrEnum):
    CENTRAL_BEFORE_COMMIT = "CENTRAL_BEFORE_COMMIT"
    CENTRAL_AFTER_COMMIT_BEFORE_SEND = "CENTRAL_AFTER_COMMIT_BEFORE_SEND"
    CENTRAL_AFTER_SEND_BEFORE_RECEIPT = "CENTRAL_AFTER_SEND_BEFORE_RECEIPT"
    DELL_AFTER_ACCEPT_COMMIT = "DELL_AFTER_ACCEPT_COMMIT"
    DELL_AFTER_EXECUTION_STARTED_COMMIT = "DELL_AFTER_EXECUTION_STARTED_COMMIT"
    DELL_AFTER_FAKE_EFFECT_BEFORE_STATUS = "DELL_AFTER_FAKE_EFFECT_BEFORE_STATUS"
    CRA_AFTER_RESTORE = "CRA_AFTER_RESTORE"
    DROP_HEARTBEAT = "DROP_HEARTBEAT"
    DELAY_HEARTBEAT = "DELAY_HEARTBEAT"
    DROP_COMMAND_RESPONSE = "DROP_COMMAND_RESPONSE"
    DROP_STATUS_RESPONSE = "DROP_STATUS_RESPONSE"
    STALE_TARGET_MUTATION = "STALE_TARGET_MUTATION"
    SEQUENCE_GAP = "SEQUENCE_GAP"
    DUPLICATE_DELIVERY = "DUPLICATE_DELIVERY"
    SQLITE_WRITE_FAILURE = "SQLITE_WRITE_FAILURE"
    AGENT_RESTART = "AGENT_RESTART"
    CRA_RESTART = "CRA_RESTART"
    MUTATE_DUPLICATE_EFFECT = "MUTATE_DUPLICATE_EFFECT"
    MUTATE_STALE_TARGET_ACCEPTED = "MUTATE_STALE_TARGET_ACCEPTED"
    MUTATE_EFFECT_BEFORE_DURABLE_ACCEPT = "MUTATE_EFFECT_BEFORE_DURABLE_ACCEPT"
    MUTATE_DUAL_AUTHORITY = "MUTATE_DUAL_AUTHORITY"
    MUTATE_UNKNOWN_RETRY = "MUTATE_UNKNOWN_RETRY"
    MUTATE_RESTORED_PENDING_REPLAY = "MUTATE_RESTORED_PENDING_REPLAY"


class FaultLifecycle(StrEnum):
    REQUESTED = "requested"
    ARMED = "armed"
    TRIGGERED = "triggered"
    COMPLETED = "completed"


@dataclass(frozen=True)
class FaultRecord:
    fault_id: str
    kind: FaultKind
    lifecycle: FaultLifecycle
    parameters: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["kind"] = self.kind.value
        value["lifecycle"] = self.lifecycle.value
        return value


class FaultController:
    """Unified lifecycle ledger; SUT mutation is delegated to an explicit adapter."""

    def __init__(self, definitions: tuple[dict[str, Any], ...]) -> None:
        self._records: dict[str, FaultRecord] = {}
        self._history: list[FaultRecord] = []
        for index, definition in enumerate(definitions, start=1):
            fault_id = str(definition.get("fault_id", f"fault-{index}"))
            if fault_id in self._records:
                raise ValueError(f"duplicate fault_id: {fault_id}")
            record = FaultRecord(
                fault_id,
                FaultKind(str(definition["kind"])),
                FaultLifecycle.REQUESTED,
                dict(definition.get("parameters", {})),
            )
            self._records[fault_id] = record
            self._history.append(record)

    def transition(self, fault_id: str, lifecycle: FaultLifecycle) -> FaultRecord:
        current = self._records[fault_id]
        order = list(FaultLifecycle)
        if order.index(lifecycle) != order.index(current.lifecycle) + 1:
            raise ValueError(f"invalid fault transition: {current.lifecycle}->{lifecycle}")
        updated = FaultRecord(current.fault_id, current.kind, lifecycle, current.parameters)
        self._records[fault_id] = updated
        self._history.append(updated)
        return updated

    def arm_all(self) -> None:
        for fault_id, record in tuple(self._records.items()):
            if record.lifecycle != FaultLifecycle.REQUESTED:
                raise ValueError(f"fault is not requestable: {fault_id}")
            self.transition(fault_id, FaultLifecycle.ARMED)

    def trigger_all(self) -> None:
        for fault_id, record in tuple(self._records.items()):
            if record.lifecycle != FaultLifecycle.ARMED:
                raise ValueError(f"fault is not armed: {fault_id}")
            self.transition(fault_id, FaultLifecycle.TRIGGERED)

    def complete_all(self) -> None:
        for fault_id, record in tuple(self._records.items()):
            if record.lifecycle != FaultLifecycle.TRIGGERED:
                raise ValueError(f"fault was not triggered: {fault_id}")
            self.transition(fault_id, FaultLifecycle.COMPLETED)

    @property
    def history(self) -> tuple[FaultRecord, ...]:
        return tuple(self._history)

    @property
    def untriggered(self) -> tuple[str, ...]:
        return tuple(sorted(key for key, value in self._records.items() if value.lifecycle != FaultLifecycle.COMPLETED))
