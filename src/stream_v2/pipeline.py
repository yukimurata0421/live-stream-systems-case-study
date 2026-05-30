from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .action_lock import FileActionLock
from .aggregator import SubsystemAggregator
from .config import RuntimeConfig
from .jsonio import append_jsonl, atomic_write_json, read_json
from .model import EvidenceRecord, OverallStatus, SubsystemsSnapshot, SubsystemStatus
from .orchestrator import RecoveryOrchestrator
from .sli import ObjectiveSliCalculator
from .source_reader import SourceReader
from .subsystems.registry import stream_components_payload
from .timeutil import isoformat_utc, now_utc, parse_utc


_SUBSYSTEM_BASE_KEYS = {
    "state",
    "confidence",
    "evidence",
    "evidence_records",
    "evidence_age_sec",
    "last_ok_ts_utc",
    "caused_by_subsystems",
    "affects_subsystems",
    "recommended_action",
    "blocked_by",
}


@dataclass(frozen=True)
class PipelineResult:
    snapshot: dict[str, Any]
    orchestrator_event: dict[str, Any]
    recovery_action_plan: dict[str, Any]
    objective_sli: dict[str, Any]
    stream_components: dict[str, Any]


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any, default: float) -> float:
    parsed = _float_or_none(value)
    return default if parsed is None else parsed


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _evidence_from_payload(payload: dict[str, Any]) -> EvidenceRecord:
    return EvidenceRecord(
        source=_text(payload.get("source")),
        event_id=_text(payload.get("event_id")),
        subsystem=_text(payload.get("subsystem")),
        name=_text(payload.get("name")),
        verdict=_text(payload.get("verdict")),
        observed_at_utc=_text(payload.get("observed_at_utc")),
        age_sec=_float_or_none(payload.get("age_sec")),
        ttl_sec=_float(payload.get("ttl_sec"), 0.0),
        confidence=_text(payload.get("confidence") or "unknown"),  # type: ignore[arg-type]
        target=_dict(payload.get("target")),
        raw_ref=_dict(payload.get("raw_ref")),
    )


def _subsystem_from_payload(name: str, payload: dict[str, Any]) -> SubsystemStatus:
    evidence_records = [
        _evidence_from_payload(item)
        for item in _list(payload.get("evidence_records"))
        if isinstance(item, dict)
    ]
    extra = {key: value for key, value in payload.items() if key not in _SUBSYSTEM_BASE_KEYS}
    return SubsystemStatus(
        name=name,
        state=_text(payload.get("state") or "unknown"),  # type: ignore[arg-type]
        confidence=_text(payload.get("confidence") or "unknown"),  # type: ignore[arg-type]
        evidence=[str(item) for item in _list(payload.get("evidence"))],
        evidence_records=evidence_records,
        evidence_age_sec=_float_or_none(payload.get("evidence_age_sec")),
        last_ok_ts_utc=_text(payload.get("last_ok_ts_utc")),
        caused_by_subsystems=[str(item) for item in _list(payload.get("caused_by_subsystems"))],
        affects_subsystems=[str(item) for item in _list(payload.get("affects_subsystems"))],
        recommended_action=_text(payload.get("recommended_action") or "none"),
        blocked_by=[str(item) for item in _list(payload.get("blocked_by"))],
        extra=extra,
    )


def snapshot_from_payload(payload: dict[str, Any]) -> SubsystemsSnapshot:
    overall = _dict(payload.get("overall"))
    return SubsystemsSnapshot(
        ts_utc=_text(payload.get("ts_utc")),
        schema_version=int(payload.get("schema_version") or 1),
        run_id=_text(payload.get("run_id")),
        overall=OverallStatus(
            state=_text(overall.get("state") or "unknown"),  # type: ignore[arg-type]
            stream_public_state=_text(overall.get("stream_public_state") or "unknown"),
            expected_video_id=_text(overall.get("expected_video_id")),
            expected_url_state=_text(overall.get("expected_url_state") or "unknown"),
            degraded_subsystems=[str(item) for item in _list(overall.get("degraded_subsystems"))],
            oldest_evidence_ts_utc=_text(overall.get("oldest_evidence_ts_utc")),
            consistency_window_sec=_float_or_none(overall.get("consistency_window_sec")),
            max_consistency_window_sec=_float(overall.get("max_consistency_window_sec"), 120.0),
            objective_sli=_dict(overall.get("objective_sli")),
            recommended_action=_text(overall.get("recommended_action") or "none"),
            action_scope=_text(overall.get("action_scope") or "none"),
            action_reason=_text(overall.get("action_reason")),
        ),
        rendering=_subsystem_from_payload("rendering", _dict(payload.get("rendering"))),
        music=_subsystem_from_payload("music", _dict(payload.get("music"))),
        local_delivery=_subsystem_from_payload("local_delivery", _dict(payload.get("local_delivery"))),
        youtube_lifecycle=_subsystem_from_payload("youtube_lifecycle", _dict(payload.get("youtube_lifecycle"))),
        monitoring=_subsystem_from_payload("monitoring", _dict(payload.get("monitoring"))),
    )


class ShadowPipeline:
    """Phase 1/2 pipeline: aggregate, audit, and compute SLI without execution."""

    def __init__(self, config: RuntimeConfig):
        self.config = config
        self.reader = SourceReader(config.source_state_root)
        self.aggregator = SubsystemAggregator(config)
        self.sli = ObjectiveSliCalculator(config)
        self.orchestrator = RecoveryOrchestrator(config)
        self.lock = FileActionLock(config.action_lock_path)

    def run_once(self, *, now: datetime | None = None) -> PipelineResult:
        now = now or now_utc()
        previous_sli = self.sli.calculate(now=now)
        stream_components = self._write_stream_components(now=now)
        snapshot, snapshot_payload = self._write_subsystems_status(
            now=now,
            objective_summary=self._objective_summary(previous_sli),
        )
        decision, action_plan = self._write_recovery_decision(snapshot, now=now)
        objective = self._write_objective_sli(now=now, expected_video_id=snapshot.overall.expected_video_id)
        return PipelineResult(snapshot=snapshot_payload, orchestrator_event=decision.event, recovery_action_plan=action_plan, objective_sli=objective, stream_components=stream_components)

    def run_subsystems_status_once(self, *, now: datetime | None = None) -> PipelineResult:
        now = now or now_utc()
        stream_components = self._write_stream_components(now=now)
        _, snapshot_payload = self._write_subsystems_status(
            now=now,
            objective_summary=self._latest_objective_summary(),
        )
        return PipelineResult(
            snapshot=snapshot_payload,
            orchestrator_event={},
            recovery_action_plan={},
            objective_sli=read_json(self.config.objective_sli_path) or {},
            stream_components=stream_components,
        )

    def run_recovery_orchestrator_once(self, *, now: datetime | None = None) -> PipelineResult:
        now = now or now_utc()
        snapshot_payload = self._read_recent_subsystems_snapshot(now=now)
        stream_components = read_json(self.config.stream_components_path) or {}
        if snapshot_payload is None:
            stream_components = self._write_stream_components(now=now)
            snapshot, snapshot_payload = self._write_subsystems_status(
                now=now,
                objective_summary=self._latest_objective_summary(),
            )
        else:
            snapshot = snapshot_from_payload(snapshot_payload)
        decision, action_plan = self._write_recovery_decision(snapshot, now=now)
        objective = self._write_objective_sli(now=now, expected_video_id=snapshot.overall.expected_video_id)
        return PipelineResult(snapshot=snapshot_payload, orchestrator_event=decision.event, recovery_action_plan=action_plan, objective_sli=objective, stream_components=stream_components)

    def _write_stream_components(self, *, now: datetime) -> dict[str, Any]:
        payload = stream_components_payload()
        payload["ts_utc"] = isoformat_utc(now)
        atomic_write_json(self.config.stream_components_path, payload)
        append_jsonl(self.config.stream_components_log_path, payload)
        return payload

    def _write_subsystems_status(self, *, now: datetime, objective_summary: dict[str, Any]) -> tuple[SubsystemsSnapshot, dict[str, Any]]:
        snapshot = self.aggregator.aggregate(self.reader.read(), now=now, objective_sli=objective_summary)
        payload = snapshot.to_dict()
        atomic_write_json(self.config.subsystems_status_path, payload)
        append_jsonl(self.config.subsystems_status_log_path, payload)
        return snapshot, payload

    def _write_recovery_decision(self, snapshot: SubsystemsSnapshot, *, now: datetime):
        decision = self.orchestrator.evaluate(snapshot, now=now, lock_state=self.lock.check())
        append_jsonl(self.config.orchestrator_log_path, decision.event)
        action_plan = {**decision.execution_plan, "ts_utc": isoformat_utc(now), "event_id": decision.event.get("event_id", "")}
        atomic_write_json(self.config.recovery_action_plan_path, action_plan)
        append_jsonl(self.config.recovery_action_plan_log_path, action_plan)
        return decision, action_plan

    def _write_objective_sli(self, *, now: datetime, expected_video_id: str) -> dict[str, Any]:
        objective = self.sli.calculate(now=now, expected_video_id=expected_video_id)
        atomic_write_json(self.config.objective_sli_path, objective)
        append_jsonl(self.config.objective_sli_log_path, objective)
        return objective

    def _read_recent_subsystems_snapshot(self, *, now: datetime) -> dict[str, Any] | None:
        payload = read_json(self.config.subsystems_status_path)
        if not payload:
            return None
        ts = parse_utc(payload.get("ts_utc"))
        if ts is None:
            return None
        if (now - ts).total_seconds() > self.config.max_consistency_window_sec:
            return None
        return payload

    def _latest_objective_summary(self) -> dict[str, Any]:
        return self._objective_summary(read_json(self.config.objective_sli_path) or {})

    def _objective_summary(self, objective: dict[str, Any]) -> dict[str, Any]:
        last_24h = objective.get("windows", {}).get("last_24h", {}) if isinstance(objective.get("windows"), dict) else {}
        return {
            "last_24h_same_url_live_ratio": last_24h.get("same_url_live_ratio"),
            "last_24h_unknown_ratio": last_24h.get("unknown_ratio"),
            "last_24h_replacement_count": last_24h.get("replacement_count"),
            "last_24h_budget_override_count": last_24h.get("budget_override_count"),
        }
