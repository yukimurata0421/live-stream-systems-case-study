from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Mapping, Sequence

from ._decode import contract_object, object_value, string_array, text
from .ids import json_copy, stable_id, validate_id
from .observation import DOMAINS, _name
from .time import parse_utc, require_not_before


CURRENT_STATES = frozenset({"good", "bad", "unknown"})


@dataclass(frozen=True)
class DomainCurrent:
    SCHEMA: ClassVar[str] = "monitoring_v4.current.v1"

    snapshot_id: str
    domain: str
    state: str
    reason_codes: tuple[str, ...]
    source_observation_ids: tuple[str, ...]
    observed_at: str
    reduced_at: str
    valid_until: str
    policy_revision: str
    reducer_revision: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        validate_id(self.snapshot_id, field="snapshot_id")
        _name(self.domain, field="domain", allowed=DOMAINS)
        _name(self.state, field="state", allowed=CURRENT_STATES)
        for reason in self.reason_codes:
            _name(reason, field="reason_code")
        for ident in self.source_observation_ids:
            validate_id(ident, field="source_observation_id")
        parse_utc(self.observed_at, field="observed_at")
        parse_utc(self.reduced_at, field="reduced_at")
        parse_utc(self.valid_until, field="valid_until")
        require_not_before(
            self.reduced_at,
            self.observed_at,
            later_field="reduced_at",
            earlier_field="observed_at",
        )
        if not self.policy_revision or len(self.policy_revision) > 200:
            raise ValueError("policy_revision must contain 1-200 characters")
        if not self.reducer_revision or len(self.reducer_revision) > 200:
            raise ValueError("reducer_revision must contain 1-200 characters")
        require_not_before(
            self.valid_until,
            self.reduced_at,
            later_field="valid_until",
            earlier_field="reduced_at",
        )
        if not isinstance(self.payload, Mapping):
            raise ValueError("payload must be an object")
        object.__setattr__(self, "reason_codes", tuple(self.reason_codes))
        object.__setattr__(self, "source_observation_ids", tuple(self.source_observation_ids))
        object.__setattr__(self, "payload", json_copy(self.payload))

    @classmethod
    def create(
        cls,
        *,
        domain: str,
        state: str,
        reason_codes: Sequence[str],
        source_observation_ids: Sequence[str],
        observed_at: str,
        reduced_at: str,
        valid_until: str,
        policy_revision: str,
        reducer_revision: str,
        payload: Mapping[str, Any],
    ) -> "DomainCurrent":
        reasons = tuple(sorted(set(reason_codes)))
        sources = tuple(sorted(set(source_observation_ids)))
        copied = json_copy(payload)
        ident = stable_id(
            "cur",
            cls.SCHEMA,
            domain,
            state,
            reasons,
            sources,
            observed_at,
            policy_revision,
            reducer_revision,
            copied,
        )
        return cls(
            ident,
            domain,
            state,
            reasons,
            sources,
            observed_at,
            reduced_at,
            valid_until,
            policy_revision,
            reducer_revision,
            copied,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "snapshot_id": self.snapshot_id,
            "domain": self.domain,
            "state": self.state,
            "reason_codes": list(self.reason_codes),
            "source_observation_ids": list(self.source_observation_ids),
            "observed_at": self.observed_at,
            "reduced_at": self.reduced_at,
            "valid_until": self.valid_until,
            "policy_revision": self.policy_revision,
            "reducer_revision": self.reducer_revision,
            "payload": json_copy(self.payload),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "DomainCurrent":
        raw = contract_object(
            value,
            schema=cls.SCHEMA,
            fields=frozenset(
                {
                    "snapshot_id",
                    "domain",
                    "state",
                    "reason_codes",
                    "source_observation_ids",
                    "observed_at",
                    "reduced_at",
                    "valid_until",
                    "policy_revision",
                    "reducer_revision",
                    "payload",
                }
            ),
        )
        return cls(
            snapshot_id=text(raw, "snapshot_id"),
            domain=text(raw, "domain"),
            state=text(raw, "state"),
            reason_codes=string_array(raw, "reason_codes"),
            source_observation_ids=string_array(raw, "source_observation_ids"),
            observed_at=text(raw, "observed_at"),
            reduced_at=text(raw, "reduced_at"),
            valid_until=text(raw, "valid_until"),
            policy_revision=text(raw, "policy_revision"),
            reducer_revision=text(raw, "reducer_revision"),
            payload=object_value(raw, "payload"),
        )
