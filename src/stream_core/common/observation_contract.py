"""Observation meaning, independent of transport and recovery policy.

Adapters establish source identity and age using the existing clock contract.
This module neither compares clocks nor authorizes an action.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math
import re


class Validity(str, Enum):
    VALID = "VALID"
    STALE = "STALE"
    UNAVAILABLE = "UNAVAILABLE"


class TargetState(str, Enum):
    OK = "OK"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"


class Activity(str, Enum):
    ACTIVE = "ACTIVE"
    STANDBY = "STANDBY"
    MAINTENANCE = "MAINTENANCE"
    UNSPECIFIED = "UNSPECIFIED"


def identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,96}", value):
        raise ValueError("observation_identifier_invalid")
    return value


def finite(value) -> bool:
    return type(value) in (float, int) and math.isfinite(value)


@dataclass(frozen=True)
class Observation:
    source: str
    scope: str
    validity: Validity
    reason: str
    age_upper_seconds: float | None = None
    max_age_seconds: float = 180.
    target_state: TargetState = TargetState.UNKNOWN
    expected_activity: Activity = Activity.UNSPECIFIED

    def __post_init__(self):
        for value in (self.source, self.scope, self.reason):
            identifier(value)
        if (not isinstance(self.validity, Validity)
                or not isinstance(self.target_state, TargetState)
                or not isinstance(self.expected_activity, Activity)):
            raise ValueError("observation_enum_invalid")
        if not finite(self.max_age_seconds) or self.max_age_seconds <= 0:
            raise ValueError("observation_expiry_invalid")
        age = self.age_upper_seconds
        if age is not None and (not finite(age) or age < 0):
            raise ValueError("observation_age_invalid")
        if self.validity == Validity.VALID and (age is None or age > self.max_age_seconds):
            raise ValueError("observation_validity_inconsistent")
        if self.validity == Validity.STALE and (age is None or age <= self.max_age_seconds):
            raise ValueError("observation_validity_inconsistent")
        if self.validity != Validity.VALID and self.target_state != TargetState.UNKNOWN:
            raise ValueError("unavailable_observation_cannot_assert_target_state")

    @classmethod
    def measured(cls, *, source, scope, age_upper_seconds, max_age_seconds,
                 target_state=TargetState.UNKNOWN, expected_activity=Activity.UNSPECIFIED):
        stale = age_upper_seconds > max_age_seconds
        return cls(source, scope, Validity.STALE if stale else Validity.VALID,
                   "source_expired" if stale else "measurement_available",
                   age_upper_seconds, max_age_seconds,
                   TargetState.UNKNOWN if stale else target_state, expected_activity)

    def payload(self):
        return {"schema": "stream.observation.v1", "source": self.source, "scope": self.scope,
                "validity": self.validity.value, "reason": self.reason,
                "age_upper_seconds": self.age_upper_seconds, "max_age_seconds": self.max_age_seconds,
                "target_state": self.target_state.value, "expected_activity": self.expected_activity.value}
