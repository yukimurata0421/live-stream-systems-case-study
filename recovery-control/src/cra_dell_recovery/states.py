from __future__ import annotations

from enum import StrEnum


class TargetHealthState(StrEnum):
    UNKNOWN = "UNKNOWN"
    HEALTHY = "HEALTHY"
    SUSPECTED = "SUSPECTED"
    CONFIRMED_DEGRADED = "CONFIRMED_DEGRADED"
    RECOVERING = "RECOVERING"
    VERIFYING = "VERIFYING"


class IncidentState(StrEnum):
    OPEN = "OPEN"
    CONFIRMED = "CONFIRMED"
    AUTHORIZED = "AUTHORIZED"
    RECOVERY_REQUESTED = "RECOVERY_REQUESTED"
    VERIFYING = "VERIFYING"
    RECOVERED = "RECOVERED"
    CLOSED = "CLOSED"
    ESCALATED = "ESCALATED"


class AuthorityState(StrEnum):
    AGENT_STARTUP_RECONCILING = "AGENT_STARTUP_RECONCILING"
    CENTRAL_ACTIVE = "CENTRAL_ACTIVE"
    CENTRAL_SUSPECT = "CENTRAL_SUSPECT"
    LOCAL_FALLBACK = "LOCAL_FALLBACK"
    RECONCILING = "RECONCILING"
    RESTORED_NEEDS_RECONCILIATION = "RESTORED_NEEDS_RECONCILIATION"
    SAFE_BLOCKED = "SAFE_BLOCKED"
    MAINTENANCE = "MAINTENANCE"


class CommandState(StrEnum):
    DRAFT = "DRAFT"
    COMMITTED = "COMMITTED"
    OUTBOX_PENDING = "OUTBOX_PENDING"
    SENT = "SENT"
    ACCEPTED = "ACCEPTED"
    EXECUTION_STARTED = "EXECUTION_STARTED"
    EFFECT_OBSERVED = "EFFECT_OBSERVED"
    EFFECT_FAILED = "EFFECT_FAILED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    VERIFYING = "VERIFYING"
    VERIFIED = "VERIFIED"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"
    VERIFICATION_UNKNOWN = "VERIFICATION_UNKNOWN"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    SUPERSEDED = "SUPERSEDED"


TARGET_TRANSITIONS = {
    TargetHealthState.UNKNOWN: {TargetHealthState.HEALTHY, TargetHealthState.SUSPECTED},
    TargetHealthState.HEALTHY: {TargetHealthState.SUSPECTED},
    TargetHealthState.SUSPECTED: {
        TargetHealthState.HEALTHY,
        TargetHealthState.CONFIRMED_DEGRADED,
        TargetHealthState.UNKNOWN,
    },
    TargetHealthState.CONFIRMED_DEGRADED: {
        TargetHealthState.RECOVERING,
        TargetHealthState.HEALTHY,
        TargetHealthState.UNKNOWN,
    },
    TargetHealthState.RECOVERING: {TargetHealthState.VERIFYING, TargetHealthState.UNKNOWN},
    TargetHealthState.VERIFYING: {
        TargetHealthState.HEALTHY,
        TargetHealthState.CONFIRMED_DEGRADED,
        TargetHealthState.UNKNOWN,
    },
}

INCIDENT_TRANSITIONS = {
    IncidentState.OPEN: {IncidentState.CONFIRMED, IncidentState.CLOSED, IncidentState.ESCALATED},
    IncidentState.CONFIRMED: {
        IncidentState.AUTHORIZED,
        IncidentState.CLOSED,
        IncidentState.ESCALATED,
    },
    IncidentState.AUTHORIZED: {
        IncidentState.RECOVERY_REQUESTED,
        IncidentState.CONFIRMED,
        IncidentState.ESCALATED,
    },
    IncidentState.RECOVERY_REQUESTED: {IncidentState.VERIFYING, IncidentState.ESCALATED},
    IncidentState.VERIFYING: {
        IncidentState.RECOVERED,
        IncidentState.CONFIRMED,
        IncidentState.ESCALATED,
    },
    IncidentState.RECOVERED: {IncidentState.CLOSED},
    IncidentState.CLOSED: set(),
    IncidentState.ESCALATED: {IncidentState.CLOSED},
}

AUTHORITY_TRANSITIONS = {
    AuthorityState.AGENT_STARTUP_RECONCILING: {
        AuthorityState.RECONCILING,
        AuthorityState.LOCAL_FALLBACK,
        AuthorityState.SAFE_BLOCKED,
        AuthorityState.MAINTENANCE,
    },
    AuthorityState.CENTRAL_ACTIVE: {
        AuthorityState.CENTRAL_SUSPECT,
        AuthorityState.RECONCILING,
        AuthorityState.SAFE_BLOCKED,
        AuthorityState.MAINTENANCE,
    },
    AuthorityState.CENTRAL_SUSPECT: {
        AuthorityState.CENTRAL_ACTIVE,
        AuthorityState.LOCAL_FALLBACK,
        AuthorityState.RECONCILING,
        AuthorityState.SAFE_BLOCKED,
        AuthorityState.MAINTENANCE,
    },
    AuthorityState.LOCAL_FALLBACK: {
        AuthorityState.RECONCILING,
        AuthorityState.SAFE_BLOCKED,
        AuthorityState.MAINTENANCE,
    },
    AuthorityState.RECONCILING: {
        AuthorityState.CENTRAL_SUSPECT,
        AuthorityState.SAFE_BLOCKED,
        AuthorityState.MAINTENANCE,
    },
    AuthorityState.RESTORED_NEEDS_RECONCILIATION: {
        AuthorityState.RECONCILING,
        AuthorityState.SAFE_BLOCKED,
    },
    AuthorityState.SAFE_BLOCKED: {AuthorityState.RECONCILING, AuthorityState.MAINTENANCE},
    AuthorityState.MAINTENANCE: {AuthorityState.RECONCILING, AuthorityState.SAFE_BLOCKED},
}

COMMAND_TRANSITIONS = {
    CommandState.DRAFT: {CommandState.COMMITTED, CommandState.SUPERSEDED},
    CommandState.COMMITTED: {CommandState.OUTBOX_PENDING, CommandState.SUPERSEDED},
    CommandState.OUTBOX_PENDING: {CommandState.SENT, CommandState.EXPIRED, CommandState.SUPERSEDED},
    CommandState.SENT: {
        CommandState.ACCEPTED,
        CommandState.REJECTED,
        CommandState.EXPIRED,
        CommandState.SUPERSEDED,
    },
    CommandState.ACCEPTED: {
        CommandState.EXECUTION_STARTED,
        CommandState.EFFECT_FAILED,
        CommandState.SUPERSEDED,
    },
    CommandState.EXECUTION_STARTED: {
        CommandState.EFFECT_OBSERVED,
        CommandState.EFFECT_FAILED,
        CommandState.OUTCOME_UNKNOWN,
    },
    CommandState.EFFECT_OBSERVED: {CommandState.VERIFYING},
    CommandState.EFFECT_FAILED: {CommandState.VERIFYING},
    CommandState.OUTCOME_UNKNOWN: set(),
    CommandState.VERIFYING: {
        CommandState.VERIFIED,
        CommandState.VERIFICATION_FAILED,
        CommandState.VERIFICATION_UNKNOWN,
    },
    CommandState.VERIFIED: set(),
    CommandState.VERIFICATION_FAILED: set(),
    CommandState.VERIFICATION_UNKNOWN: set(),
    CommandState.REJECTED: set(),
    CommandState.EXPIRED: set(),
    CommandState.SUPERSEDED: set(),
}


def require_transition(current: StrEnum, new: StrEnum) -> None:
    mapping: dict[StrEnum, set[StrEnum]]
    if isinstance(current, TargetHealthState) and isinstance(new, TargetHealthState):
        mapping = TARGET_TRANSITIONS  # type: ignore[assignment]
    elif isinstance(current, IncidentState) and isinstance(new, IncidentState):
        mapping = INCIDENT_TRANSITIONS  # type: ignore[assignment]
    elif isinstance(current, AuthorityState) and isinstance(new, AuthorityState):
        mapping = AUTHORITY_TRANSITIONS  # type: ignore[assignment]
    elif isinstance(current, CommandState) and isinstance(new, CommandState):
        mapping = COMMAND_TRANSITIONS  # type: ignore[assignment]
    else:
        raise ValueError(f"state machine mismatch: {current} -> {new}")
    if new not in mapping[current]:
        raise ValueError(f"invalid transition: {current} -> {new}")
