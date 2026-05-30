from __future__ import annotations

from datetime import datetime
from typing import Any

from ...model import EvidenceRecord, SubsystemStatus
from ..common import BaseSubsystemEvaluator
from .policy import decide
from .signals import collect_signals


class LocalDeliveryEvaluator(BaseSubsystemEvaluator):
    name = "local_delivery"

    def evaluate(
        self,
        *,
        ytw: dict[str, Any],
        stream_stats: dict[str, Any],
        timeline: dict[str, Any],
        runtime: dict[str, Any],
        fast_recovery: dict[str, Any],
        stream_engine_event: dict[str, Any],
        restart_reason: dict[str, Any],
        recovery_stage: dict[str, Any],
        target: dict[str, Any],
        now: datetime,
    ) -> SubsystemStatus:
        signals = collect_signals(
            ytw=ytw,
            stream_stats=stream_stats,
            runtime=runtime,
            fast_recovery=fast_recovery,
            stream_engine_event=stream_engine_event,
            restart_reason=restart_reason,
            recovery_stage=recovery_stage,
            now=now,
        )
        decision = decide(signals, has_any_input=bool(ytw or stream_stats or timeline or runtime or fast_recovery or stream_engine_event or restart_reason or recovery_stage))
        evidence: list[EvidenceRecord] = []
        healthy_names = signals.healthy_evidence_names()
        for name in decision.evidence:
            source, payload, raw_file = self._evidence_source(name, ytw, stream_stats, runtime, fast_recovery, stream_engine_event, signals)
            verdict = "healthy" if name in healthy_names else "failed"
            evidence.append(self.evidence(source=source, source_payload=payload, subsystem=self.name, name=name, verdict=verdict, target=target, now=now, ttl_sec=90, raw_file=raw_file))
        extra = {
            "ffmpeg_count": signals.ffmpeg_count,
            "runtime_age_sec": signals.runtime_age_sec,
            "ingest_connected": signals.ingest_connected,
            "tcp_send_healthy": signals.tcp_send_healthy,
            "tcp_bytes_sent_delta": signals.tcp_bytes_sent_delta,
            "tcp_mbps": signals.tcp_mbps,
            "tcp_notsent": signals.tcp_notsent,
            "tcp_unacked": signals.tcp_unacked,
            "tcp_lastsnd_ms": signals.tcp_lastsnd_ms,
            "tcp_conn_established": signals.tcp_conn_established,
            "stream_engine_recent": signals.stream_engine_recent,
            "stream_engine_event": signals.stream_engine_event,
            "reason": signals.reason,
            "recovery_order": ["restart_ffmpeg", "restart_stream"],
        }
        return self.status(self.name, decision.state, decision.confidence, decision.evidence, evidence, decision.recommended_action, decision.blocked_by, caused_by=decision.caused_by, affects=decision.affects, extra=extra)

    def _evidence_source(
        self,
        name: str,
        ytw: dict[str, Any],
        stream_stats: dict[str, Any],
        runtime: dict[str, Any],
        fast_recovery: dict[str, Any],
        stream_engine_event: dict[str, Any],
        signals: Any,
    ) -> tuple[str, dict[str, Any], str]:
        if name == "ingest_connected":
            return "youtube_watchdog", ytw, "youtube_watchdog_stats.json"
        if name in {"tcp_send_healthy", "tcp_stall"} and fast_recovery:
            return "fast_recovery", fast_recovery, "logs/fast_recovery_events.jsonl"
        if name == "stream_engine_recent" and stream_engine_event:
            return "stream_engine", stream_engine_event, "logs/stream_engine_events.jsonl"
        if name in {"stream_watchdog_ok", "stream_ffmpeg_duplicate"} and stream_stats:
            return "stream_watchdog", stream_stats, "stream_watchdog_stats.json"
        payload = {
            "ts_utc": signals.observed_ts_utc,
            "event_id": runtime.get("last_event_id") or fast_recovery.get("event_id") or stream_engine_event.get("event_id"),
        }
        return "stream_engine", payload, str(runtime.get("_source_file") or "stream_runtime_state_*.json")
