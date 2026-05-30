from __future__ import annotations

# Read-only YouTube/OAuth probes. This module intentionally re-exports only
# list/probe helpers and does not expose insert/bind/delete/transition calls.
try:
    from watchers.youtube_api import (
        OAuthProbeResult,
        check_data_api,
        check_public_watch_page,
        check_public_watch_page_verdict,
        choose_transition_target_broadcast,
        extract_video_id,
        force_live_transition_statuses,
        parse_ingest_ports,
        probe_public_live_status,
        probe_with_oauth,
        quota_guard_status,
        resolve_live_video_id,
        resolve_video_id_from_live_page,
        select_primary_broadcast,
    )
except ModuleNotFoundError:
    from youtube_api import (
        OAuthProbeResult,
        check_data_api,
        check_public_watch_page,
        check_public_watch_page_verdict,
        choose_transition_target_broadcast,
        extract_video_id,
        force_live_transition_statuses,
        parse_ingest_ports,
        probe_public_live_status,
        probe_with_oauth,
        quota_guard_status,
        resolve_live_video_id,
        resolve_video_id_from_live_page,
        select_primary_broadcast,
    )


__all__ = [
    "OAuthProbeResult",
    "check_data_api",
    "check_public_watch_page",
    "check_public_watch_page_verdict",
    "choose_transition_target_broadcast",
    "extract_video_id",
    "force_live_transition_statuses",
    "parse_ingest_ports",
    "probe_public_live_status",
    "probe_with_oauth",
    "quota_guard_status",
    "resolve_live_video_id",
    "resolve_video_id_from_live_page",
    "select_primary_broadcast",
]
