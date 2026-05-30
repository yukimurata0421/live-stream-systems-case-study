from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

try:
    from evidence.identity import TargetIdentity
    from evidence.ledger import LedgerSnapshot
    from evidence.sources import EvidenceRecord, SourceKind
    from decision.policy import Policy
except ImportError:
    from ..evidence.identity import TargetIdentity  # type: ignore
    from ..evidence.ledger import LedgerSnapshot  # type: ignore
    from ..evidence.sources import EvidenceRecord, SourceKind  # type: ignore
    from .policy import Policy  # type: ignore


DecisionState = Literal[
    "available",
    "remote_unconfirmed",
    "local_unhealthy",
    "inconsistent_remote",
    "remote_ended_suspected",
    "remote_ended_confirmed",
    "public_degraded",
    "telemetry_degraded",
    "local_only",
]


@dataclass(frozen=True)
class Decision:
    state: DecisionState
    reason: str
    contributing_sources: tuple[SourceKind, ...]
    target: TargetIdentity | None


def evaluate(snap: LedgerSnapshot, policy: Policy) -> Decision:
    fresh = _fresh_records(snap)
    canonical = _pick_canonical_target(fresh, snap.canonical_target)

    local = fresh.get(SourceKind.INGEST_LOCAL)
    local_alive = local is not None and local.verdict == "live"
    local_inactive = local is not None and local.verdict == "inactive"

    remote_sources = {SourceKind.DATA_API, SourceKind.OAUTH, SourceKind.WATCH_PAGE}
    remote = {src: ev for src, ev in fresh.items() if src in remote_sources}
    aligned: dict[SourceKind, EvidenceRecord] = {}
    misaligned: dict[SourceKind, EvidenceRecord] = {}
    for src, ev in remote.items():
        if canonical is None or ev.target.matches(canonical):
            aligned[src] = ev
        else:
            misaligned[src] = ev

    if local_inactive:
        return Decision("local_unhealthy", "local ingest inactive", (SourceKind.INGEST_LOCAL,), canonical)

    if not remote:
        if local_alive:
            return Decision("local_only", "no fresh remote evidence; local ingest alive", (SourceKind.INGEST_LOCAL,), canonical)
        return Decision("telemetry_degraded", "no fresh remote evidence", (), canonical)

    if misaligned:
        names = ",".join(sorted(src.value for src in misaligned))
        return Decision("inconsistent_remote", f"identity mismatch from {names}", tuple(misaligned), canonical)

    api = aligned.get(SourceKind.DATA_API)
    oauth = aligned.get(SourceKind.OAUTH)
    watch = aligned.get(SourceKind.WATCH_PAGE)
    api_ended = api is not None and api.verdict == "ended"
    oauth_complete = oauth is not None and oauth.verdict == "ended"
    public_live = watch is not None and watch.verdict == "live"
    authoritative_live = (
        (api is not None and api.verdict == "live")
        or (oauth is not None and oauth.verdict == "live")
    )
    watch_hard_negative = watch is not None and watch.verdict in {"not_live", "unplayable", "login_required"}

    if public_live and (api_ended or oauth_complete):
        sources = tuple(src for src, ev in aligned.items() if ev.verdict in {"live", "ended"})
        return Decision(
            "inconsistent_remote",
            "public live evidence contradicts authoritative ended signal",
            sources,
            canonical,
        )

    if api_ended and oauth_complete and api.target.strong_video_match(oauth.target):
        return Decision(
            "remote_ended_confirmed",
            "data_api=ended AND oauth=complete (fresh, identity matched)",
            (SourceKind.DATA_API, SourceKind.OAUTH),
            canonical,
        )

    if (api_ended or oauth_complete) and watch_hard_negative:
        sources = tuple(
            src
            for src, ev in aligned.items()
            if ev.verdict in {"ended", "not_live", "unplayable", "login_required"}
        )
        return Decision("remote_ended_suspected", "one authoritative ended signal plus watch hard negative", sources, canonical)

    if watch_hard_negative and not (api_ended or oauth_complete):
        return Decision("public_degraded", "watch page hard negative without authoritative ended signal", (SourceKind.WATCH_PAGE,), canonical)

    if authoritative_live or public_live:
        return Decision("available", "fresh authoritative live evidence", tuple(aligned), canonical)

    return Decision("remote_unconfirmed", "no confirmed termination evidence; public live not verified", tuple(aligned), canonical)


def _fresh_records(snap: LedgerSnapshot) -> dict[SourceKind, EvidenceRecord]:
    out: dict[SourceKind, EvidenceRecord] = {}
    for src, ev in snap.latest_by_source.items():
        if ev.is_fresh(snap.taken_at, snap.current_target_epoch, snap.current_restart_epoch):
            out[src] = ev
    return out


def _pick_canonical_target(records: dict[SourceKind, EvidenceRecord], fallback: TargetIdentity) -> TargetIdentity | None:
    for source in (SourceKind.RESOLVER, SourceKind.DATA_API, SourceKind.OAUTH, SourceKind.WATCH_PAGE, SourceKind.INGEST_LOCAL):
        ev = records.get(source)
        if ev is not None and (ev.target.video_id or ev.target.broadcast_id or ev.target.channel_id):
            return ev.target
    if fallback.video_id or fallback.broadcast_id or fallback.channel_id:
        return fallback
    return None
