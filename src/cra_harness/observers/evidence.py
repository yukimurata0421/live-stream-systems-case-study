from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from typing import Any

from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now


@dataclass(frozen=True)
class EvidenceEvent:
    event_id: str
    sequence: int
    run_id: str
    scenario_id: str
    name: str
    producer: str
    observed_at: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EvidenceBundle:
    run_id: str
    scenario_id: str
    started_at: str
    finished_at: str
    events: tuple[EvidenceEvent, ...]

    def named(self, name: str) -> tuple[EvidenceEvent, ...]:
        return tuple(item for item in self.events if item.name == name)

    def latest_data(self, name: str) -> dict[str, Any] | None:
        matches = self.named(name)
        return None if not matches else dict(matches[-1].data)

    def scalar(self, path: str) -> Any:
        event_name, separator, nested = path.partition(".")
        value: Any = self.latest_data(event_name)
        if value is None:
            raise KeyError(path)
        if not separator:
            return value
        for part in nested.split("."):
            if not isinstance(value, dict) or part not in value:
                raise KeyError(path)
            value = value[part]
        return value


class EvidenceCollector:
    def __init__(self, run_id: str, scenario_id: str) -> None:
        self.run_id = run_id
        self.scenario_id = scenario_id
        self.started_at = isoformat_utc(utc_now())
        self._events: list[EvidenceEvent] = []
        self._event_ids: set[str] = set()
        self._frozen = False

    def append(
        self,
        name: str,
        producer: str,
        data: dict[str, Any],
        *,
        event_id: str | None = None,
        observed_at: str | None = None,
    ) -> EvidenceEvent:
        if self._frozen:
            raise RuntimeError("evidence collector is frozen")
        identifier = event_id or f"evidence-{uuid.uuid4()}"
        if identifier in self._event_ids:
            raise ValueError(f"duplicate evidence event_id: {identifier}")
        timestamp = observed_at or isoformat_utc(utc_now())
        parse_utc(timestamp)
        event = EvidenceEvent(
            event_id=identifier,
            sequence=len(self._events) + 1,
            run_id=self.run_id,
            scenario_id=self.scenario_id,
            name=name,
            producer=producer,
            observed_at=timestamp,
            data=dict(data),
        )
        self._event_ids.add(identifier)
        self._events.append(event)
        return event

    def freeze(self, *, finished_at: str | None = None) -> EvidenceBundle:
        if self._frozen:
            raise RuntimeError("evidence collector already frozen")
        self._frozen = True
        return EvidenceBundle(
            run_id=self.run_id,
            scenario_id=self.scenario_id,
            started_at=self.started_at,
            finished_at=finished_at or isoformat_utc(utc_now()),
            events=tuple(self._events),
        )


@dataclass(frozen=True)
class CompletenessResult:
    missing: tuple[str, ...]
    harness_errors: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not self.missing and not self.harness_errors


class EvidenceCompletenessChecker:
    def check(self, bundle: EvidenceBundle, required: tuple[str, ...]) -> CompletenessResult:
        names = {item.name for item in bundle.events}
        missing = tuple(sorted(set(required) - names))
        errors: list[str] = []
        start = parse_utc(bundle.started_at)
        finish = parse_utc(bundle.finished_at)
        if finish < start:
            errors.append("RUN_TIME_REVERSED")
        seen: set[str] = set()
        expected_sequence = 1
        for event in bundle.events:
            if event.event_id in seen:
                errors.append("DUPLICATE_EVIDENCE_EVENT_ID")
            seen.add(event.event_id)
            if event.sequence != expected_sequence:
                errors.append("EVIDENCE_SEQUENCE_GAP")
            expected_sequence += 1
            if event.run_id != bundle.run_id:
                errors.append("RUN_ID_MISMATCH")
            if event.scenario_id != bundle.scenario_id:
                errors.append("SCENARIO_ID_MISMATCH")
            observed = parse_utc(event.observed_at)
            if observed < start or observed > finish:
                errors.append("EVIDENCE_OUTSIDE_RUN_WINDOW")
        return CompletenessResult(missing, tuple(sorted(set(errors))))
