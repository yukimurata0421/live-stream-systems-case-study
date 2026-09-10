from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol

from cra_authority.monitoring_evidence import MonitoringEvidenceProjection
from cra_dell_recovery.models import RecoveryAuthorizationInput
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now


@dataclass(frozen=True)
class RecoveryPolicy:
    revision: str
    authorization_lifetime_seconds: int = 30
    minimum_action_interval_seconds: int = 60
    hourly_action_limit: int = 24
    daily_action_limit: int = 96
    materialize_authorization: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.revision, str) or not self.revision.strip():
            raise ValueError("policy revision must be a non-empty string")
        fields = {
            "authorization_lifetime_seconds": (self.authorization_lifetime_seconds, 1, 300),
            "minimum_action_interval_seconds": (self.minimum_action_interval_seconds, 1, 604800),
            "hourly_action_limit": (self.hourly_action_limit, 1, 10000),
            "daily_action_limit": (self.daily_action_limit, 1, 100000),
        }
        for name, (value, minimum, maximum) in fields.items():
            if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
                raise ValueError(f"{name} is outside the safe integer range")
        if self.daily_action_limit < self.hourly_action_limit:
            raise ValueError("daily_action_limit must be greater than or equal to hourly_action_limit")
        if not isinstance(self.materialize_authorization, bool):
            raise ValueError("materialize_authorization must be boolean")


@dataclass(frozen=True)
class AuthorizationDecision:
    decision_id: str
    decision: str
    action: str | None
    candidate_reason_code: str
    decision_reason_code: str
    decision_digest: str
    reason_binding: str
    authorization_id: str | None
    blockers: tuple[str, ...]

    @property
    def reason_code(self) -> str:
        """Compatibility view; new consumers must choose one reason dimension."""

        return self.decision_reason_code


class AuthorizerStore(Protocol):
    def ingest_monitoring_projection(self, projection: dict[str, Any]) -> str: ...

    def central_policy_blockers(
        self,
        target_id: str,
        *,
        now: Any,
        minimum_action_interval_sec: int,
        hourly_action_limit: int,
        daily_action_limit: int,
    ) -> tuple[str, ...]: ...

    def existing_policy_decision(
        self,
        projection_id: str,
        policy_revision: str,
    ) -> tuple[str, str, str | None, str, str, tuple[str, ...], str] | None: ...

    def persist_policy_decision(
        self,
        *,
        projection: dict[str, Any],
        authorization: RecoveryAuthorizationInput,
        decision_id: str,
        decision: str,
        blockers: tuple[str, ...],
        action: str | None = None,
        candidate_reason_code: str | None = None,
        decision_reason_code: str | None = None,
        reason_code: str | None = None,
    ) -> str: ...


class CraAuthorizer:
    """Owns policy and authorization; Monitoring supplies facts only."""

    REQUIRED_CHECKS = {
        "tcp_stall": "CONFIRMED",
        "network_down": "FALSE",
        "ffmpeg_present": "TRUE",
        "target_stable": "TRUE",
        "maintenance": "FALSE",
        "delivery_bad": "TRUE",
    }

    def __init__(self, store: AuthorizerStore, policy: RecoveryPolicy) -> None:
        self.store = store
        self.policy = policy

    def evaluate(self, projection: MonitoringEvidenceProjection) -> AuthorizationDecision:
        self.store.ingest_monitoring_projection(projection.value)
        stable = hashlib.sha256(f"{projection.projection_id}:{self.policy.revision}".encode()).hexdigest()[:32]
        decision_id = f"cra-decision-{stable}"
        authorization_id = f"cra-auth-{stable}"
        candidate_reason_code = (
            "confirmed_tcp_stall"
            if projection.incident["state"] == "CONFIRMED" and projection.checks.get("tcp_stall", {}).get("status") == "CONFIRMED"
            else "NO_CONFIRMED_RECOVERY_CANDIDATE"
        )
        existing = self.store.existing_policy_decision(projection.projection_id, self.policy.revision)
        if existing is not None:
            (
                existing_id,
                existing_decision,
                existing_action,
                existing_candidate_reason_code,
                existing_decision_reason_code,
                existing_blockers,
                existing_digest,
            ) = existing
            return AuthorizationDecision(
                decision_id=existing_id,
                decision=existing_decision,
                action=existing_action,
                candidate_reason_code=existing_candidate_reason_code,
                decision_reason_code=existing_decision_reason_code,
                decision_digest=existing_digest,
                reason_binding=(
                    "LEGACY_UNBOUND" if existing_candidate_reason_code == "LEGACY_UNSEPARATED" else "CANDIDATE_AND_DECISION_BOUND_V1"
                ),
                authorization_id=authorization_id if existing_decision == "AUTHORIZED" else None,
                blockers=existing_blockers,
            )
        now = utc_now()
        blockers: list[str] = []
        if parse_utc(str(projection.value["expires_at"])) <= now:
            blockers.append("MONITORING_EVIDENCE_EXPIRED")
        blockers.extend(f"MONITORING_READINESS_{name.upper()}_FALSE" for name, ready in projection.readiness.items() if not ready)
        incident = projection.incident
        if incident["state"] != "CONFIRMED":
            blockers.append("INCIDENT_NOT_CONFIRMED")
        checks = projection.checks
        for name, required in self.REQUIRED_CHECKS.items():
            if name not in checks:
                blockers.append(f"CHECK_{name.upper()}_MISSING")
            elif checks[name]["status"] != required:
                blockers.append(f"CHECK_{name.upper()}_NOT_{required}")
        blockers.extend(
            self.store.central_policy_blockers(
                projection.target_id,
                now=now,
                minimum_action_interval_sec=self.policy.minimum_action_interval_seconds,
                hourly_action_limit=self.policy.hourly_action_limit,
                daily_action_limit=self.policy.daily_action_limit,
            )
        )
        if not self.policy.materialize_authorization:
            blockers.append("CRA_OPERATING_MODE_NO_ACTION")
        expiry = min(
            parse_utc(str(projection.value["expires_at"])),
            now + timedelta(seconds=self.policy.authorization_lifetime_seconds),
        )
        authorization = RecoveryAuthorizationInput(
            authorization_id=authorization_id,
            incident_id=str(incident["incident_id"]),
            source_episode_id=str(incident["source_episode_id"]),
            target_id=projection.target_id,
            action="restart_ffmpeg",
            reason_code=candidate_reason_code,
            policy_revision=self.policy.revision,
            observation_revision=str(projection.value["observation_revision"]),
            expected_target=projection.observed_target,
            blockers=tuple(sorted(set(blockers))),
            authorized_at=isoformat_utc(now),
            expires_at=isoformat_utc(expiry),
        )
        decision = "AUTHORIZED" if not blockers else ("BLOCKED" if incident["state"] == "CONFIRMED" else "NO_ACTION")
        decision_action = authorization.action if decision == "AUTHORIZED" else None
        if decision == "AUTHORIZED":
            decision_reason_code = authorization.reason_code
        elif decision == "NO_ACTION":
            decision_reason_code = "INCIDENT_NOT_CONFIRMED"
        else:
            decision_reason_code = next(
                (item for item in authorization.blockers if item != "CRA_OPERATING_MODE_NO_ACTION"),
                "CRA_OPERATING_MODE_NO_ACTION",
            )
        persisted = self.store.persist_policy_decision(
            projection=projection.value,
            authorization=authorization,
            decision_id=decision_id,
            decision=decision,
            action=decision_action,
            candidate_reason_code=candidate_reason_code,
            decision_reason_code=decision_reason_code,
            blockers=authorization.blockers,
        )
        saved = self.store.existing_policy_decision(projection.projection_id, self.policy.revision)
        if saved is None:
            raise RuntimeError("CRA_POLICY_DECISION_NOT_DURABLE")
        saved_id, saved_decision, saved_action, saved_candidate, saved_reason, saved_blockers, saved_digest = saved
        if (
            saved_id != decision_id
            or saved_decision != persisted
            or saved_action != decision_action
            or saved_candidate != candidate_reason_code
            or saved_reason != decision_reason_code
            or saved_blockers != authorization.blockers
        ):
            raise RuntimeError("CRA_POLICY_DECISION_DURABLE_BINDING_MISMATCH")
        return AuthorizationDecision(
            decision_id=decision_id,
            decision=persisted,
            action=decision_action,
            candidate_reason_code=candidate_reason_code,
            decision_reason_code=decision_reason_code,
            decision_digest=saved_digest,
            reason_binding="CANDIDATE_AND_DECISION_BOUND_V1",
            authorization_id=authorization_id if persisted == "AUTHORIZED" else None,
            blockers=authorization.blockers,
        )
