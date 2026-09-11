from __future__ import annotations

import json
from pathlib import Path

from stream_contracts.monitoring_v4.current import DomainCurrent
from stream_contracts.monitoring_v4.observation import ObservationEnvelope
from stream_contracts.monitoring_v4.time import utc_text


BASE_TS = 1_786_426_200  # 2026-08-11T05:30:00Z


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def observation(
    *,
    domain: str = "delivery",
    source: str = "runtime_delivery_watchdog",
    status: str = "good",
    observed_ts: int = BASE_TS,
    received_ts: int | None = None,
    role: str = "current_authoritative",
    event: str | None = None,
    reason: str | None = None,
    payload: dict[str, object] | None = None,
) -> ObservationEnvelope:
    received_ts = observed_ts if received_ts is None else received_ts
    return ObservationEnvelope.create(
        domain=domain,
        source=source,
        source_event_id=event or f"event-{observed_ts}-{status}",
        source_generation=f"generation-{source}",
        evidence_role=role,
        status=status,
        reason_code=reason or f"sample_{status}",
        observed_at=utc_text(observed_ts),
        received_at=utc_text(received_ts),
        freshness_limit_sec=600,
        producer_revision="test-fixture-r1",
        payload=payload or {"sample": event or observed_ts},
    )


def current(
    *,
    domain: str = "delivery",
    state: str = "good",
    observed_ts: int = BASE_TS,
    reduced_ts: int | None = None,
    reason: str | None = None,
    marker: str = "a",
    payload: dict[str, object] | None = None,
) -> DomainCurrent:
    reduced_ts = observed_ts if reduced_ts is None else reduced_ts
    reason = reason or f"current_{state}_evidence"
    return DomainCurrent.create(
        domain=domain,
        state=state,
        reason_codes=(reason,),
        source_observation_ids=(),
        observed_at=utc_text(observed_ts),
        reduced_at=utc_text(reduced_ts),
        valid_until=utc_text(max(observed_ts, reduced_ts)),
        policy_revision="test-source-policy-r3",
        reducer_revision="test-reducer-r3",
        payload={"marker": marker, **(payload or {})},
    )
