from __future__ import annotations

SUBSYSTEM = "youtube_lifecycle"

FAILURE_INCONSISTENT_REMOTE = "inconsistent_remote"
FAILURE_REMOTE_ENDED_CONFIRMED = "remote_ended_confirmed"
FAILURE_PUBLIC_NOT_LIVE = "public_not_live"
FAILURE_CANDIDATE_NEW_URL_FOUND = "candidate_new_url_found_not_promoted"
FAILURE_QUOTA_GUARD_ACTIVE = "quota_guard_active"

ACTION_NONE = "none"
ACTION_RETRY_PROBE = "retry_probe"
ACTION_RESYNC_RESOLVER = "resync_resolver"
ACTION_FORCE_CURRENT_BROADCAST_LIVE = "force_current_broadcast_live"
ACTION_CREATE_REPLACEMENT_BROADCAST = "create_replacement_broadcast"

RECOVERY_ORDER = (
    ACTION_RETRY_PROBE,
    ACTION_RESYNC_RESOLVER,
    ACTION_FORCE_CURRENT_BROADCAST_LIVE,
    ACTION_CREATE_REPLACEMENT_BROADCAST,
)
