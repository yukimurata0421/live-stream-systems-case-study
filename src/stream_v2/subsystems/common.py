from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Optional

from ..model import EvidenceRecord, SubsystemStatus
from ..timeutil import age_seconds, isoformat_utc, parse_utc


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "ok", "running", "live", "healthy"}
    return False


def text(value: Any) -> str:
    return "" if value is None else str(value)


def event_id(prefix: str) -> str:
    return f"evt-{prefix}-{uuid.uuid4().hex[:12]}"


class BaseSubsystemEvaluator:
    name: str

    def evidence(
        self,
        *,
        source: str,
        source_payload: dict[str, Any],
        subsystem: str,
        name: str,
        verdict: str,
        target: dict[str, Any],
        now: datetime,
        ttl_sec: float,
        confidence: str = "high",
        raw_file: str = "",
    ) -> EvidenceRecord:
        observed_at = parse_utc(
            source_payload.get("ts_utc")
            or source_payload.get("ts_jst")
            or source_payload.get("updated_at_utc")
            or source_payload.get("selected_at_utc")
            or source_payload.get("started_at_utc")
            or source_payload.get("oauth_checked_ts_utc")
            or source_payload.get("data_api_checked_ts_utc")
        )
        if observed_at is None:
            observed_at = now
        return EvidenceRecord(
            source=source,
            event_id=text(source_payload.get("event_id") or event_id(source)),
            subsystem=subsystem,
            name=name,
            verdict=verdict,
            observed_at_utc=isoformat_utc(observed_at),
            age_sec=age_seconds(observed_at, now),
            ttl_sec=ttl_sec,
            confidence=confidence,  # type: ignore[arg-type]
            target=target,
            raw_ref={"file": raw_file} if raw_file else {},
        )

    def status(
        self,
        name: str,
        state: str,
        confidence: str,
        names: list[str],
        evidence: list[EvidenceRecord],
        recommended_action: str,
        blocked_by: list[str],
        *,
        caused_by: Optional[list[str]] = None,
        affects: Optional[list[str]] = None,
        extra: Optional[dict[str, Any]] = None,
    ) -> SubsystemStatus:
        ages = [ev.age_sec for ev in evidence if ev.age_sec is not None]
        last_ok = ""
        healthy_ev = [ev for ev in evidence if ev.verdict == "healthy"]
        if healthy_ev:
            last_ok = max(healthy_ev, key=lambda ev: ev.observed_at_utc).observed_at_utc
        return SubsystemStatus(
            name=name,
            state=state,  # type: ignore[arg-type]
            confidence=confidence,  # type: ignore[arg-type]
            evidence=names,
            evidence_records=evidence,
            evidence_age_sec=max(ages) if ages else None,
            last_ok_ts_utc=last_ok,
            caused_by_subsystems=caused_by or [],
            affects_subsystems=affects or [],
            recommended_action=recommended_action,
            blocked_by=blocked_by,
            extra=extra or {},
        )
