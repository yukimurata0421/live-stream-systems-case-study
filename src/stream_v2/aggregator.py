from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from .config import RuntimeConfig
from .model import OverallStatus, SubsystemsSnapshot, SubsystemStatus
from .source_reader import RuntimeInputs
from .subsystems import LocalDeliveryEvaluator, MonitoringEvaluator, MusicEvaluator, RenderingEvaluator, YouTubeLifecycleEvaluator
from .subsystems.common import text
from .timeutil import isoformat_utc


class SubsystemAggregator:
    """Single writer/evaluator coordinator for subsystem snapshots.

    Subsystem-specific logic lives under ``stream_v2.subsystems``. This class
    only wires inputs, computes cross-subsystem overall state, and returns the
    latest snapshot for the pipeline to write.
    """

    def __init__(self, config: RuntimeConfig):
        self.config = config
        self.rendering = RenderingEvaluator()
        self.music = MusicEvaluator()
        self.local_delivery = LocalDeliveryEvaluator()
        self.youtube_lifecycle = YouTubeLifecycleEvaluator()
        self.monitoring = MonitoringEvaluator()

    def aggregate(self, inputs: RuntimeInputs, *, now: datetime, objective_sli: Optional[dict[str, Any]] = None) -> SubsystemsSnapshot:
        ytw = inputs.youtube_watchdog_stats
        resolver = inputs.youtube_video_id_resolver_state
        stream_stats = inputs.stream_watchdog_stats
        runtime = inputs.latest_runtime_state
        timeline = inputs.latest_watchdog_timeline_event
        cost = inputs.api_cost_latest

        target = self._target(ytw, resolver, runtime)
        rendering = self.rendering.evaluate(
            stream_stats=stream_stats,
            timeline=timeline,
            runtime=runtime,
            stream1090_report=inputs.latest_stream1090_report,
            upstream_report=inputs.latest_upstream_stream1090_report,
            adsb_freshness=inputs.adsb_freshness_state,
            target=target,
            now=now,
        )
        music = self.music.evaluate(
            timeline=timeline,
            restart_reason=inputs.restart_reason,
            overlay_now_playing=inputs.overlay_now_playing,
            pulse_health=inputs.pulse_health_state,
            play_history=inputs.latest_play_history_event,
            audio_fail_count=inputs.audio_fail_count,
            pulse_source_missing_count=inputs.pulse_source_missing_count,
            target=target,
            now=now,
        )
        local_delivery = self.local_delivery.evaluate(
            ytw=ytw,
            stream_stats=stream_stats,
            timeline=timeline,
            runtime=runtime,
            fast_recovery=inputs.latest_fast_recovery_event,
            stream_engine_event=inputs.latest_stream_engine_event,
            restart_reason=inputs.restart_reason,
            recovery_stage=inputs.recovery_stage_state,
            target=target,
            now=now,
        )
        youtube_lifecycle = self.youtube_lifecycle.evaluate(ytw=ytw, resolver=resolver, cost=cost, target=target, now=now)
        monitoring = self.monitoring.evaluate(
            ytw=ytw,
            resolver=resolver,
            cost=cost,
            stream_stats=stream_stats,
            timeline=timeline,
            stream1090_report=inputs.latest_stream1090_report,
            upstream_report=inputs.latest_upstream_stream1090_report,
            stream_engine_event=inputs.latest_stream_engine_event,
            play_history=inputs.latest_play_history_event,
            target=target,
            now=now,
        )

        subsystems = [rendering, music, local_delivery, youtube_lifecycle, monitoring]
        all_evidence = [ev for subsystem in subsystems for ev in subsystem.evidence_records]
        valid_evidence = [ev for ev in all_evidence if ev.age_sec is not None]
        control_evidence = [
            ev
            for ev in valid_evidence
            if ev.ttl_sec <= self.config.max_consistency_window_sec or ev.verdict != "healthy"
        ]
        oldest_ev = max(control_evidence or valid_evidence, key=lambda ev: ev.age_sec, default=None)
        consistency_window_sec = oldest_ev.age_sec if oldest_ev else None
        oldest_evidence_ts_utc = oldest_ev.observed_at_utc if oldest_ev else ""

        degraded_subsystems = [s.name for s in subsystems if s.state in {"degraded", "failed", "recovering", "unknown"}]
        consistency_exceeded = consistency_window_sec is None or consistency_window_sec > self.config.max_consistency_window_sec
        any_failed = any(s.state == "failed" for s in subsystems)
        any_degraded = any(s.state in {"degraded", "recovering"} for s in subsystems)
        any_unknown = any(s.state == "unknown" for s in subsystems)

        if consistency_exceeded:
            overall_state = "unknown"
            action_reason = "consistency window exceeded; destructive action disabled"
        elif any_failed:
            overall_state = "failed"
            action_reason = "one or more subsystems failed"
        elif any_degraded:
            overall_state = "degraded"
            action_reason = "one or more subsystems degraded"
        elif any_unknown:
            overall_state = "unknown"
            action_reason = "one or more subsystems unknown"
        else:
            overall_state = "healthy"
            action_reason = "all subsystems healthy"

        expected_url_state = text(youtube_lifecycle.extra.get("expected_url_state") or "unknown")
        if expected_url_state == "live" and local_delivery.state == "healthy":
            stream_public_state = "same_url_live"
        elif expected_url_state == "live":
            stream_public_state = "same_url_live_local_degraded"
        elif expected_url_state in {"recoverable", "buffering", "not_live"}:
            stream_public_state = f"same_url_{expected_url_state}"
        else:
            stream_public_state = "unknown"

        recommended_action = self._first_recommended_action(subsystems)
        action_scope = "none" if recommended_action == "none" else self._action_scope(recommended_action, subsystems)

        overall = OverallStatus(
            state=overall_state,  # type: ignore[arg-type]
            stream_public_state=stream_public_state,
            expected_video_id=target.get("expected_video_id", ""),
            expected_url_state=expected_url_state,
            degraded_subsystems=degraded_subsystems,
            oldest_evidence_ts_utc=oldest_evidence_ts_utc,
            consistency_window_sec=consistency_window_sec,
            max_consistency_window_sec=self.config.max_consistency_window_sec,
            objective_sli=objective_sli or {},
            recommended_action=recommended_action,
            action_scope=action_scope,
            action_reason=action_reason,
        )
        return SubsystemsSnapshot(
            ts_utc=isoformat_utc(now),
            schema_version=1,
            run_id=text(runtime.get("run_id") or ytw.get("run_id") or ""),
            overall=overall,
            rendering=rendering,
            music=music,
            local_delivery=local_delivery,
            youtube_lifecycle=youtube_lifecycle,
            monitoring=monitoring,
        )

    def _target(self, ytw: dict[str, Any], resolver: dict[str, Any], runtime: dict[str, Any]) -> dict[str, str]:
        expected_video_id = text(ytw.get("expected_video_id") or ytw.get("video_id") or resolver.get("expected_video_id") or resolver.get("video_id"))
        candidate_video_id = text(ytw.get("candidate_new_video_id") or resolver.get("candidate_new_video_id"))
        return {
            "stream_id": self.config.stream_id,
            "expected_video_id": expected_video_id,
            "selected_video_id": text(ytw.get("video_id") or resolver.get("video_id")),
            "candidate_video_id": candidate_video_id,
            "expected_watch_url": f"https://youtube.com/watch?v={expected_video_id}" if expected_video_id else "",
            "broadcast_id": text(ytw.get("oauth_broadcast_id") or expected_video_id),
            "bound_stream_id": text(ytw.get("oauth_bound_stream_id")),
            "stream_key_hash": text(runtime.get("stream_key_hash")),
        }

    def _first_recommended_action(self, subsystems: list[SubsystemStatus]) -> str:
        priorities = [
            "reload_overlay",
            "restart_dj",
            "repair_pulse",
            "restart_ffmpeg",
            "restart_stream",
            "resync_resolver",
            "force_current_broadcast_live",
            "defer",
            "retry_probe",
        ]
        actions = [s.recommended_action for s in subsystems if s.recommended_action and s.recommended_action != "none"]
        for action in priorities:
            if action in actions:
                return action
        return actions[0] if actions else "none"

    def _action_scope(self, action: str, subsystems: list[SubsystemStatus]) -> str:
        for subsystem in subsystems:
            if subsystem.recommended_action == action:
                return subsystem.name
        return "none"
