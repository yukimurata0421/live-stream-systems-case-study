from __future__ import annotations

from datetime import datetime
from typing import Any

from ...model import EvidenceRecord, SubsystemStatus
from ..common import BaseSubsystemEvaluator
from .policy import decide
from .signals import collect_signals


class RenderingEvaluator(BaseSubsystemEvaluator):
    name = "rendering"

    def evaluate(
        self,
        *,
        stream_stats: dict[str, Any],
        timeline: dict[str, Any],
        runtime: dict[str, Any],
        stream1090_report: dict[str, Any],
        upstream_report: dict[str, Any],
        adsb_freshness: dict[str, Any],
        target: dict[str, Any],
        now: datetime,
    ) -> SubsystemStatus:
        signals = collect_signals(
            stream_stats=stream_stats,
            timeline=timeline,
            runtime=runtime,
            stream1090_report=stream1090_report,
            upstream_report=upstream_report,
            adsb_freshness=adsb_freshness,
            now=now,
        )
        decision = decide(signals, has_any_input=bool(stream_stats or timeline or runtime or stream1090_report or upstream_report or adsb_freshness))
        evidence: list[EvidenceRecord] = []
        healthy_names = signals.healthy_evidence_names()
        for name in decision.evidence:
            verdict = "healthy" if name in healthy_names else "failed"
            source, payload, raw_file, ttl_sec = self._evidence_source(name, stream1090_report, upstream_report, adsb_freshness, stream_stats, timeline, signals)
            evidence.append(self.evidence(source=source, source_payload=payload, subsystem=self.name, name=name, verdict=verdict, target=target, now=now, ttl_sec=ttl_sec, raw_file=raw_file))
        extra = {
            "runtime_snapshot_age_sec": signals.runtime_snapshot_age_sec,
            "stream1090_report_fresh": signals.stream1090_report_fresh,
            "stream1090_report_ok": signals.stream1090_report_ok,
            "upstream_stream1090_report_fresh": signals.upstream_report_fresh,
            "upstream_stream1090_report_ok": signals.upstream_report_ok,
            "aircraft_json_ok": signals.aircraft_json_ok,
            "aircraft_messages_moving": signals.aircraft_messages_moving,
            "aircraft_positions_moving": signals.aircraft_positions_moving,
            "adsb_freshness_ok": signals.adsb_freshness_ok,
            "adsb_freshness_stale": signals.adsb_freshness_stale,
            "stream1090_target": signals.stream1090_target,
            "reason": signals.reason,
            "recovery_order": ["reload_overlay", "restart_browser"],
        }
        return self.status(self.name, decision.state, decision.confidence, decision.evidence, evidence, decision.recommended_action, decision.blocked_by, caused_by=decision.caused_by, affects=decision.affects, extra=extra)

    def _evidence_source(
        self,
        name: str,
        stream1090_report: dict[str, Any],
        upstream_report: dict[str, Any],
        adsb_freshness: dict[str, Any],
        stream_stats: dict[str, Any],
        timeline: dict[str, Any],
        signals: Any,
    ) -> tuple[str, dict[str, Any], str, float]:
        if name in {"stream1090_report_ok", "overlay_unavailable", "stream1090_unavailable", "adsb_freshness_stall", "video_frame_unhealthy"} and stream1090_report:
            return "stream1090_report", stream1090_report, "logs/stream1090_report.jsonl", 1800.0
        if name in {"upstream_stream1090_report_ok", "upstream_stream1090_unavailable"} and upstream_report:
            return "upstream_stream1090_report", upstream_report, "logs/upstream_stream1090_report.jsonl", 1800.0
        if name == "adsb_freshness_ok" and adsb_freshness:
            return "stream_watchdog", adsb_freshness, "watchdog/adsb_freshness_state.json", 180.0
        payload = {
            "ts_utc": signals.timeline_ts_utc or signals.runtime_snapshot_updated_at_utc or stream_stats.get("ts_utc"),
            "event_id": timeline.get("event_id"),
        }
        return "stream_watchdog", payload, "watchdog_state_timeline.jsonl", 120.0
