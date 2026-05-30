from __future__ import annotations

from datetime import datetime
from typing import Any

from ...model import EvidenceRecord, SubsystemStatus
from ..common import BaseSubsystemEvaluator
from .policy import decide
from .signals import collect_rich_signals


class MonitoringEvaluator(BaseSubsystemEvaluator):
    name = "monitoring"

    def evaluate(
        self,
        *,
        ytw: dict[str, Any],
        resolver: dict[str, Any],
        cost: dict[str, Any],
        stream_stats: dict[str, Any],
        timeline: dict[str, Any],
        stream1090_report: dict[str, Any],
        upstream_report: dict[str, Any],
        stream_engine_event: dict[str, Any],
        play_history: dict[str, Any],
        target: dict[str, Any],
        now: datetime,
    ) -> SubsystemStatus:
        signals = collect_rich_signals(
            ytw=ytw,
            resolver=resolver,
            cost=cost,
            stream_stats=stream_stats,
            timeline=timeline,
            stream1090_report=stream1090_report,
            upstream_report=upstream_report,
            stream_engine_event=stream_engine_event,
            play_history=play_history,
            now=now,
        )
        decision = decide(signals)
        evidence: list[EvidenceRecord] = []
        for source in signals.sources:
            if not source.payload:
                continue
            source_payload = dict(source.payload)
            source_payload.setdefault("ts_utc", source.observed_ts_utc)
            verdict = "healthy" if source.fresh else "unknown"
            evidence.append(self.evidence(source=source.source, source_payload=source_payload, subsystem=self.name, name=source.evidence_name, verdict=verdict, target=target, now=now, ttl_sec=source.ttl_sec, raw_file=source.file_name))
        extra = {
            "fresh_sources": signals.fresh_names,
            "stale_sources": signals.stale_names,
            "quota_guard_active": signals.quota_guard_active,
            "cost_report_degraded": signals.cost_report_degraded,
        }
        return self.status(self.name, decision.state, decision.confidence, decision.evidence, evidence, decision.recommended_action, decision.blocked_by, caused_by=decision.caused_by, affects=decision.affects, extra=extra)
