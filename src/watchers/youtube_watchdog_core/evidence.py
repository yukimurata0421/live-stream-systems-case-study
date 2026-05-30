from __future__ import annotations

try:
    from ..youtube_monitor import classifier as lifecycle_classifier
    from ..youtube_watchdog_config import (
        CHANNEL_ID,
        EVIDENCE_API_COST_TTL_SEC,
        EVIDENCE_DATA_API_TTL_SEC,
        EVIDENCE_INGEST_TTL_SEC,
        EVIDENCE_OAUTH_TTL_SEC,
        EVIDENCE_RESOLVER_TTL_SEC,
        EVIDENCE_WATCH_TTL_SEC,
        RESTART_BUDGET_RELEASE_RECONFIRM_SEC,
        RESTART_COOLDOWN_SEC,
        OAuthProbeResult,
    )
    from ..youtube_watchdog_state import log
    from ..decision.policy import Policy
    from ..evidence.identity import TargetIdentity
    from ..evidence.ledger import EvidenceLedger
    from ..evidence.sources import EvidenceRecord, SourceKind
    from .cache import parse_iso_ts
except ImportError:
    from youtube_monitor import classifier as lifecycle_classifier
    from youtube_watchdog_config import (
        CHANNEL_ID,
        EVIDENCE_API_COST_TTL_SEC,
        EVIDENCE_DATA_API_TTL_SEC,
        EVIDENCE_INGEST_TTL_SEC,
        EVIDENCE_OAUTH_TTL_SEC,
        EVIDENCE_RESOLVER_TTL_SEC,
        EVIDENCE_WATCH_TTL_SEC,
        RESTART_BUDGET_RELEASE_RECONFIRM_SEC,
        RESTART_COOLDOWN_SEC,
        OAuthProbeResult,
    )
    from youtube_watchdog_state import log
    from decision.policy import Policy
    from evidence.identity import TargetIdentity
    from evidence.ledger import EvidenceLedger
    from evidence.sources import EvidenceRecord, SourceKind
    from youtube_watchdog_core.cache import parse_iso_ts


def active_evidence_key(evidence_decision) -> str:
    state = str(getattr(evidence_decision, "state", "") or "")
    if state not in {"remote_ended_confirmed", "local_unhealthy"}:
        return ""
    target = getattr(evidence_decision, "target", None)
    video_id = str(getattr(target, "video_id", "") or "") if target is not None else ""
    broadcast_id = str(getattr(target, "broadcast_id", "") or "") if target is not None else ""
    channel_id = str(getattr(target, "channel_id", "") or "") if target is not None else ""
    sources = ",".join(sorted(str(src.value if hasattr(src, "value") else src) for src in getattr(evidence_decision, "contributing_sources", ()) or ()))
    return f"{state}|channel={channel_id}|video={video_id}|broadcast={broadcast_id}|sources={sources}"


def active_evidence_first_ts(state: dict, evidence_state: str, now_ts: int, evidence_key: str = "") -> int:
    if evidence_state not in {"remote_ended_confirmed", "local_unhealthy"}:
        return 0
    previous_state = str(state.get("active_evidence_state", "")).strip()
    previous_key = str(state.get("active_evidence_key", "")).strip()
    previous_first = int(state.get("active_evidence_first_ts", 0) or 0)
    if previous_state == evidence_state and previous_key == evidence_key and previous_first > 0:
        return previous_first
    return now_ts


def detect_failure_kind(
    *,
    stream_active: bool,
    ingest_connected: bool,
    selected_video_id: str,
    api_live_state: str,
    api_reason: str,
    watch_reason: str,
    oauth_reason: str,
    oauth_life_cycle_status: str,
    oauth_video_id: str,
) -> str:
    return lifecycle_classifier.detect_failure_kind(
        stream_active=stream_active,
        ingest_connected=ingest_connected,
        selected_video_id=selected_video_id,
        api_live_state=api_live_state,
        api_reason=api_reason,
        watch_reason=watch_reason,
        oauth_reason=oauth_reason,
        oauth_life_cycle_status=oauth_life_cycle_status,
        oauth_video_id=oauth_video_id,
    )


def detect_transient_subkind(
    *,
    api_live_state: str,
    api_reason: str,
    watch_reason: str,
    oauth_reason: str,
) -> str:
    return lifecycle_classifier.detect_transient_subkind(
        api_live_state=api_live_state,
        api_reason=api_reason,
        watch_reason=watch_reason,
        oauth_reason=oauth_reason,
    )


def build_evidence_policy() -> Policy:
    return Policy.from_values(
        data_api_ttl_sec=EVIDENCE_DATA_API_TTL_SEC,
        oauth_ttl_sec=EVIDENCE_OAUTH_TTL_SEC,
        watch_ttl_sec=EVIDENCE_WATCH_TTL_SEC,
        resolver_ttl_sec=EVIDENCE_RESOLVER_TTL_SEC,
        ingest_ttl_sec=EVIDENCE_INGEST_TTL_SEC,
        api_cost_ttl_sec=EVIDENCE_API_COST_TTL_SEC,
        budget_release_reconfirm_sec=RESTART_BUDGET_RELEASE_RECONFIRM_SEC,
        min_restart_interval_sec=RESTART_COOLDOWN_SEC,
    )


def evidence_target(
    ledger: EvidenceLedger,
    *,
    video_id: str = "",
    broadcast_id: str = "",
    bound_stream_id: str = "",
    channel_id: str = "",
) -> TargetIdentity:
    canonical = ledger.canonical_target
    return TargetIdentity(
        channel_id=channel_id or CHANNEL_ID or canonical.channel_id,
        video_id=(video_id or "").strip(),
        broadcast_id=(broadcast_id or "").strip(),
        bound_stream_id=(bound_stream_id or "").strip(),
        target_epoch=ledger.current_target_epoch,
        restart_epoch=ledger.current_restart_epoch,
    )


def verdict_from_api_live_state(api_live_state: str) -> str:
    state = (api_live_state or "").strip().lower()
    if state == "live":
        return "live"
    if state == "ended":
        return "ended"
    if state in {"error", "quota_exhausted", "rate_limited", "deferred"}:
        return "degraded"
    return "unknown"


def verdict_from_watch_reason(watch_reason: str) -> str:
    text = (watch_reason or "").strip().lower()
    if "public live probe verdict=live" in text:
        return "live"
    if "public live probe verdict=not_live" in text:
        return "not_live"
    if "live marker detected" in text:
        return "live"
    if "login required" in text:
        return "login_required"
    if "playability error" in text or "unplayable" in text:
        return "unplayable"
    return "unknown"


def verdict_from_oauth(oauth: OAuthProbeResult) -> str:
    if not oauth.probe_ok:
        return "degraded" if oauth.enabled else "unknown"
    lifecycle = (oauth.life_cycle_status or "").strip().lower()
    if lifecycle == "complete":
        return "ended"
    if lifecycle == "live":
        return "live"
    return "unknown"


def record_monitoring_evidence(
    ledger: EvidenceLedger,
    *,
    now_ts: int,
    video_id: str,
    resolver_state: dict,
    stream_active: bool,
    ingest_connected: bool,
    api_live_state: str,
    api_reason: str,
    watch_reason: str,
    watch_page_verdict: str,
    oauth: OAuthProbeResult,
    oauth_checked_ts_utc: str,
    data_api_checked_ts_utc: str,
    api_cost_guard,
) -> None:
    if video_id:
        ledger.ensure_target(evidence_target(ledger, video_id=video_id, channel_id=CHANNEL_ID or oauth.channel_id))
    base_target = evidence_target(ledger, video_id=video_id, channel_id=CHANNEL_ID or oauth.channel_id)
    resolver_video_id = str(resolver_state.get("video_id", "")).strip()
    resolver_observed_at = int(resolver_state.get("resolved_ts", 0) or 0) or now_ts
    data_api_observed_at = parse_iso_ts(data_api_checked_ts_utc) or now_ts
    oauth_observed_at = parse_iso_ts(oauth_checked_ts_utc) or now_ts
    resolver_target = evidence_target(
        ledger,
        video_id=resolver_video_id or video_id,
        channel_id=str(resolver_state.get("channel_id", "")).strip() or CHANNEL_ID or oauth.channel_id,
    )
    records = [
        EvidenceRecord(
            source=SourceKind.RESOLVER,
            verdict="live" if resolver_video_id else "unknown",
            target=resolver_target,
            observed_at=float(resolver_observed_at),
            target_epoch=ledger.current_target_epoch,
            restart_epoch=ledger.current_restart_epoch,
            ttl_sec=float(EVIDENCE_RESOLVER_TTL_SEC),
            raw={
                "source": resolver_state.get("source", ""),
                "reason": resolver_state.get("live_page_reason", ""),
            },
        ),
        EvidenceRecord(
            source=SourceKind.INGEST_LOCAL,
            verdict="live" if stream_active and ingest_connected else "inactive",
            target=base_target,
            observed_at=float(now_ts),
            target_epoch=ledger.current_target_epoch,
            restart_epoch=ledger.current_restart_epoch,
            ttl_sec=float(EVIDENCE_INGEST_TTL_SEC),
            raw={"stream_active": stream_active, "ingest_connected": ingest_connected},
        ),
        EvidenceRecord(
            source=SourceKind.DATA_API,
            verdict=verdict_from_api_live_state(api_live_state),  # type: ignore[arg-type]
            target=base_target,
            observed_at=float(data_api_observed_at),
            target_epoch=ledger.current_target_epoch,
            restart_epoch=ledger.current_restart_epoch,
            ttl_sec=float(EVIDENCE_DATA_API_TTL_SEC),
            raw={"live_state": api_live_state, "reason": api_reason},
        ),
        EvidenceRecord(
            source=SourceKind.WATCH_PAGE,
            verdict=watch_page_verdict or verdict_from_watch_reason(watch_reason),  # type: ignore[arg-type]
            target=base_target,
            observed_at=float(now_ts),
            target_epoch=ledger.current_target_epoch,
            restart_epoch=ledger.current_restart_epoch,
            ttl_sec=float(EVIDENCE_WATCH_TTL_SEC),
            raw={"reason": watch_reason, "watch_page_verdict": watch_page_verdict},
        ),
        EvidenceRecord(
            source=SourceKind.OAUTH,
            verdict=verdict_from_oauth(oauth),  # type: ignore[arg-type]
            target=evidence_target(
                ledger,
                video_id=oauth.video_id,
                broadcast_id=oauth.broadcast_id,
                bound_stream_id=oauth.bound_stream_id,
                channel_id=oauth.channel_id or CHANNEL_ID,
            ),
            observed_at=float(oauth_observed_at),
            target_epoch=ledger.current_target_epoch,
            restart_epoch=ledger.current_restart_epoch,
            ttl_sec=float(EVIDENCE_OAUTH_TTL_SEC),
            raw={
                "probe_ok": oauth.probe_ok,
                "healthy": oauth.healthy,
                "lifecycle": oauth.life_cycle_status,
                "reason": oauth.reason,
            },
        ),
        EvidenceRecord(
            source=SourceKind.API_COST,
            verdict="degraded" if api_cost_guard.active else "live",
            target=base_target,
            observed_at=float(now_ts),
            target_epoch=ledger.current_target_epoch,
            restart_epoch=ledger.current_restart_epoch,
            ttl_sec=float(EVIDENCE_API_COST_TTL_SEC),
            raw={"active": api_cost_guard.active, "reason": api_cost_guard.reason},
        ),
    ]
    for record in records:
        try:
            ledger.record(record)
        except Exception as e:
            log(f"Evidence ledger write failed ({record.source.value}): {e}")
