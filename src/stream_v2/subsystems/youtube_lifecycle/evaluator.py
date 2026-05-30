from __future__ import annotations

from datetime import datetime
from typing import Any

from ...model import EvidenceRecord, SubsystemStatus
from ..common import BaseSubsystemEvaluator, text
from .policy import decide
from .signals import collect_signals


class YouTubeLifecycleEvaluator(BaseSubsystemEvaluator):
    name = "youtube_lifecycle"

    def evaluate(self, *, ytw: dict[str, Any], resolver: dict[str, Any], cost: dict[str, Any], target: dict[str, Any], now: datetime) -> SubsystemStatus:
        signals = collect_signals(ytw=ytw, resolver=resolver, cost=cost, target=target, now=now)
        decision = decide(signals)
        evidence: list[EvidenceRecord] = []
        healthy_names = {"expected_video_id_match", "public_probe_live", "data_api_live", "oauth_broadcast_live", "auto_stop_disabled", "quota_guard_inactive"}
        for name in decision.evidence:
            source, payload, raw_file, ttl_sec = self._evidence_source(name, ytw, resolver, cost)
            if not payload:
                continue
            verdict = "healthy" if name in healthy_names else "degraded"
            if name == "remote_ended_confirmed":
                verdict = "failed"
            evidence.append(self.evidence(source=source, source_payload=payload, subsystem=self.name, name=name, verdict=verdict, target=target, now=now, ttl_sec=ttl_sec, raw_file=raw_file))
        blocked_actions = []
        if not decision.replacement_policy.allowed:
            blocked_actions.append({"action": "create_replacement_broadcast", "blocked_by": decision.replacement_policy.reason})

        extra = {
            "expected_video_id": signals.expected_video_id,
            "selected_video_id": target.get("selected_video_id", ""),
            "candidate_video_id": signals.candidate_video_id,
            "candidate_new_url_found": signals.candidate_found,
            "expected_identity_match": signals.expected_identity_match,
            "expected_url_state": signals.expected_url_state,
            "same_url_preserved": signals.expected_url_state in {"live", "recoverable"},
            "current_url_recoverable": signals.current_url_recoverable,
            "replacement_allowed": decision.replacement_policy.allowed,
            "replacement_policy": decision.replacement_policy.to_dict(),
            "blocked_actions": blocked_actions,
            "broadcast_id": text(ytw.get("oauth_broadcast_id") or signals.expected_video_id),
            "bound_stream_id": text(ytw.get("oauth_bound_stream_id")),
            "oauth_channel_id": text(ytw.get("oauth_channel_id")),
            "expected_channel_id": text(ytw.get("expected_channel_id")),
            "oauth_enable_auto_stop": ytw.get("oauth_enable_auto_stop"),
            "oauth_channel_mismatch": signals.oauth_channel_mismatch,
            "oauth_shadow_invalid_grant": signals.oauth_shadow_invalid_grant,
            "quota_guard_active": signals.quota_guard_active,
            "url_preservation_active": signals.url_preservation_active,
            "public_probe_age_sec": signals.public_probe_age_sec,
            "data_api_age_sec": signals.data_api_age_sec,
            "oauth_age_sec": signals.oauth_age_sec,
            "resolver_age_sec": signals.resolver_age_sec,
            "public_probe_verdict": "live" if signals.public_ok else "not_live" if ytw else "unknown",
            "local_ingest_state": "connected" if ytw.get("ingest_connected") else "unknown",
            "recovery_order": ["retry_probe", "resync_resolver", "force_current_broadcast_live", "create_replacement_broadcast"],
        }
        return self.status(self.name, decision.state, decision.confidence, decision.evidence, evidence, decision.recommended_action, decision.blocked_by, caused_by=decision.caused_by, affects=decision.affects, extra=extra)

    def _evidence_source(self, name: str, ytw: dict[str, Any], resolver: dict[str, Any], cost: dict[str, Any]) -> tuple[str, dict[str, Any], str, float]:
        if name == "expected_video_id_match":
            return "youtube_video_id_resolver", resolver or ytw, "youtube_video_id_resolver_state.json" if resolver else "youtube_watchdog_stats.json", 300.0
        if name == "data_api_live":
            payload = {**ytw, "ts_utc": ytw.get("data_api_checked_ts_utc") or ytw.get("ts_utc")}
            return "youtube_watchdog", payload, "youtube_watchdog_stats.json", 180.0
        if name == "oauth_broadcast_live":
            payload = {**ytw, "ts_utc": ytw.get("oauth_checked_ts_utc") or ytw.get("ts_utc")}
            return "youtube_watchdog", payload, "youtube_watchdog_stats.json", 180.0
        if name == "quota_guard_inactive":
            payload = cost or ytw
            return "youtube_api_cost_report", payload, "reports/youtube_api_cost/open_day_latest.json" if cost else "youtube_watchdog_stats.json", 1800.0
        if name == "candidate_new_url_found_not_promoted":
            return "youtube_video_id_resolver", resolver or ytw, "youtube_video_id_resolver_state.json" if resolver else "youtube_watchdog_stats.json", 300.0
        return "youtube_watchdog", ytw, "youtube_watchdog_stats.json", 180.0
