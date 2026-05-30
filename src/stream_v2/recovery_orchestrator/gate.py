from __future__ import annotations

from ..action_lock import LockState
from ..model import ActionCandidate, DESTRUCTIVE_ACTIONS, SubsystemsSnapshot, YOUTUBE_DESTRUCTIVE_ACTIONS
from .types import GateResult


class ActionGate:
    """Pure gate evaluation for one candidate."""

    def evaluate(self, snapshot: SubsystemsSnapshot, candidate: ActionCandidate, *, lock_state: LockState) -> GateResult:
        is_destructive = candidate.action in DESTRUCTIVE_ACTIONS
        is_youtube_destructive = candidate.action in YOUTUBE_DESTRUCTIVE_ACTIONS
        youtube = snapshot.youtube_lifecycle
        monitoring = snapshot.monitoring
        replacement_policy = youtube.extra.get("replacement_policy") if isinstance(youtube.extra.get("replacement_policy"), dict) else {}
        quota_guard_active = bool(youtube.extra.get("quota_guard_active"))
        oauth_channel_mismatch = bool(youtube.extra.get("oauth_channel_mismatch"))
        consistency = snapshot.overall.consistency_window_sec
        consistency_ok = consistency is not None and consistency <= snapshot.overall.max_consistency_window_sec

        gates = {
            "budget": {"passed": True, "reason": "shadow_budget_not_enforced"},
            "cooldown": {"passed": True, "reason": "shadow_cooldown_not_enforced"},
            "oauth_channel": {
                "passed": not (is_youtube_destructive and oauth_channel_mismatch),
                "reason": "oauth_channel_match" if not oauth_channel_mismatch else "oauth_channel_mismatch_blocks_youtube_destructive_action",
            },
            "quota_guard": {
                "passed": not (is_youtube_destructive and quota_guard_active),
                "reason": "quota_guard_clear" if not quota_guard_active else "quota_guard_active_blocks_youtube_destructive_action",
            },
            "global_action_lock": lock_state.to_gate() if is_destructive else {"passed": True, "reason": "not_lock_scoped_action", "lock_owner_event_id": ""},
            "consistency_window": {
                "passed": (not is_destructive) or consistency_ok,
                "reason": f"{consistency}<={snapshot.overall.max_consistency_window_sec}" if consistency_ok else "stale_or_missing_evidence_blocks_destructive_action",
            },
            "url_preservation": {
                "passed": candidate.action != "create_replacement_broadcast" or bool(replacement_policy.get("allowed")),
                "reason": str(replacement_policy.get("reason") or "not_replacement_action"),
                "blocked_actions": ["create_replacement_broadcast"] if candidate.action == "create_replacement_broadcast" and not replacement_policy.get("allowed") else [],
            },
            "monitoring_safety": {
                "passed": (not is_destructive) or monitoring.state == "healthy",
                "reason": "monitoring_healthy" if monitoring.state == "healthy" else "monitoring_unknown_or_degraded_blocks_destructive_action",
            },
        }
        blocked_by = [name for name, gate in gates.items() if not bool(gate.get("passed"))]
        if not candidate.preconditions_met:
            blocked_by.extend(candidate.blocked_by)
        return GateResult(gates=gates, passed=not blocked_by, blocked_by=blocked_by)
