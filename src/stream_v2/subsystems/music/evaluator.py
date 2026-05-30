from __future__ import annotations

from datetime import datetime
from typing import Any

from ...model import EvidenceRecord, SubsystemStatus
from ..common import BaseSubsystemEvaluator
from .policy import decide
from .signals import collect_signals


class MusicEvaluator(BaseSubsystemEvaluator):
    name = "music"

    def evaluate(
        self,
        *,
        timeline: dict[str, Any],
        restart_reason: dict[str, Any],
        overlay_now_playing: dict[str, Any],
        pulse_health: dict[str, Any],
        play_history: dict[str, Any],
        audio_fail_count: int,
        pulse_source_missing_count: int,
        target: dict[str, Any],
        now: datetime,
    ) -> SubsystemStatus:
        signals = collect_signals(
            timeline=timeline,
            restart_reason=restart_reason,
            overlay_now_playing=overlay_now_playing,
            pulse_health=pulse_health,
            play_history=play_history,
            audio_fail_count=audio_fail_count,
            pulse_source_missing_count=pulse_source_missing_count,
            now=now,
        )
        decision = decide(signals, has_any_input=bool(timeline or restart_reason or overlay_now_playing or pulse_health or play_history or audio_fail_count or pulse_source_missing_count))
        evidence: list[EvidenceRecord] = []
        healthy_names = signals.healthy_evidence_names()
        for name in decision.evidence:
            source, payload, raw_file, ttl_sec = self._evidence_source(name, timeline, restart_reason, overlay_now_playing, pulse_health, play_history, signals)
            verdict = "healthy" if name in healthy_names else "degraded"
            evidence.append(self.evidence(source=source, source_payload=payload, subsystem=self.name, name=name, verdict=verdict, target=target, now=now, ttl_sec=ttl_sec, raw_file=raw_file))
        extra = {
            "now_playing_title": signals.now_playing_title,
            "now_playing_status": signals.now_playing_status,
            "now_playing_age_sec": signals.now_playing_age_sec,
            "pulse_route_ok": signals.pulse_route_ok,
            "play_history_recent": signals.play_history_recent,
            "track_transition_within_grace": signals.track_transition_within_grace,
            "bucket_boundary_within_grace": signals.bucket_boundary_within_grace,
            "audio_fail_count": signals.audio_fail_count,
            "pulse_source_missing_count": signals.pulse_source_missing_count,
            "reason": signals.reason,
            "recovery_order": ["defer", "restart_dj", "repair_pulse"],
        }
        return self.status(self.name, decision.state, decision.confidence, decision.evidence, evidence, decision.recommended_action, decision.blocked_by, caused_by=decision.caused_by, affects=decision.affects, extra=extra)

    def _evidence_source(
        self,
        name: str,
        timeline: dict[str, Any],
        restart_reason: dict[str, Any],
        overlay_now_playing: dict[str, Any],
        pulse_health: dict[str, Any],
        play_history: dict[str, Any],
        signals: Any,
    ) -> tuple[str, dict[str, Any], str, float]:
        if name in {"now_playing_fresh", "now_playing_stale"}:
            if overlay_now_playing:
                return "now_playing_overlay", overlay_now_playing, "ui/overlay/now_playing.json", 120.0
            payload = {"ts_utc": signals.observed_ts_utc, "event_id": timeline.get("event_id")}
            return "stream_watchdog", payload, "watchdog_state_timeline.jsonl", 120.0
        if name in {"pulse_route_ok", "pulse_route_anomaly", "pulse_source_missing"} and pulse_health:
            return "stream_watchdog", pulse_health, "watchdog/pulse_health_state.json", 180.0
        if name == "play_history_recent" and play_history:
            return "auto_dj", play_history, "logs/play_history.jsonl", 900.0
        payload = {
            "ts_utc": signals.observed_ts_utc,
            "event_id": restart_reason.get("event_id") or timeline.get("event_id"),
        }
        return "stream_watchdog", payload, "restart_reason.json", 120.0
