from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .config import RuntimeConfig
from .jsonio import iter_jsonl
from .subsystems.local_delivery.actions import FAST_RECOVERY_STREAM_RESTART_FAILURE_BY_TRIGGER
from .timeutil import isoformat_utc, parse_utc


WINDOWS = {
    "last_24h": 24 * 3600,
    "last_7d": 7 * 24 * 3600,
    "last_30d": 30 * 24 * 3600,
}
SHADOW_TIMER_INTERVAL_SEC = 60
WINDOW_COMPLETE_COVERAGE_RATIO = 0.90
PRODUCTION_DIFF_MATCH_WINDOW_SEC = 120
LOCAL_RESTART_ACTIONS = {"restart_ffmpeg", "restart_stream"}
EXECUTOR_RECOVERY_ACTIONS = {
    "reload_overlay",
    "restart_browser",
    "restart_dj",
    "restart_ffmpeg",
    "restart_stream",
    "force_current_broadcast_live",
    "create_replacement_broadcast",
}


class ObjectiveSliCalculator:
    """Derive objective SLI from v2 JSONL logs.

    Incomplete windows report null ratios. This prevents confusing "not enough
    data yet" with "0% live".
    """

    def __init__(self, config: RuntimeConfig):
        self.config = config

    def calculate(self, *, now: datetime, expected_video_id: str = "") -> dict[str, Any]:
        status_events = self._load_status_events(self.config.subsystems_status_log_path, now)
        orchestrator_events = self._load_orchestrator_events(self.config.orchestrator_log_path, now)
        production_action_events = self._load_production_action_events(now)
        windows = {}
        for name, seconds in WINDOWS.items():
            windows[name] = self._window(name, seconds, status_events, orchestrator_events, production_action_events, now)
        return {
            "ts_utc": isoformat_utc(now),
            "schema_version": 1,
            "stream_id": self.config.stream_id,
            "expected_video_id": expected_video_id,
            "shadow_timer_policy": {
                "subsystems_status_timer": "adsb-streamnew-subsystems-status.timer",
                "recovery_orchestrator_timer": "adsb-streamnew-recovery-orchestrator.timer",
                "expected_interval_sec": SHADOW_TIMER_INTERVAL_SEC,
                "accuracy_sec": 15,
            },
            "window_policy": {
                "window_complete_coverage_ratio": WINDOW_COMPLETE_COVERAGE_RATIO,
                "production_diff_match_window_sec": PRODUCTION_DIFF_MATCH_WINDOW_SEC,
                "incomplete_ratio_value": None,
            },
            "windows": windows,
            "source_logs": [
                "subsystems_status.jsonl",
                "recovery_orchestrator.jsonl",
                "fast_recovery_events.jsonl",
                "stream_engine_events.jsonl",
                "stream_watchdog_events.jsonl",
            ],
        }

    def _load_status_events(self, path: Path, now: datetime) -> list[dict[str, Any]]:
        out = []
        for event in iter_jsonl(path):
            ts = parse_utc(event.get("ts_utc"))
            if ts is None or ts > now:
                continue
            overall = event.get("overall") if isinstance(event.get("overall"), dict) else {}
            out.append({
                "ts": ts,
                "payload": {
                    "overall": {
                        "state": overall.get("state"),
                        "stream_public_state": overall.get("stream_public_state"),
                    }
                },
            })
        out.sort(key=lambda item: item["ts"])
        return out

    def _load_orchestrator_events(self, path: Path, now: datetime) -> list[dict[str, Any]]:
        out = []
        for event in iter_jsonl(path):
            ts = parse_utc(event.get("ts_utc"))
            if ts is None or ts > now:
                continue
            out.append({"ts": ts, "payload": self._project_orchestrator_event(event)})
        out.sort(key=lambda item: item["ts"])
        return out

    def _project_orchestrator_event(self, event: dict[str, Any]) -> dict[str, Any]:
        selected = event.get("selected_action") if isinstance(event.get("selected_action"), dict) else {}
        execution_plan = event.get("execution_plan") if isinstance(event.get("execution_plan"), dict) else {}
        gates = event.get("gates") if isinstance(event.get("gates"), dict) else {}
        projected_gates: dict[str, Any] = {}
        budget = gates.get("budget") if isinstance(gates.get("budget"), dict) else {}
        if budget:
            projected_gates["budget"] = {"reason": budget.get("reason"), "passed": budget.get("passed")}
        direct_lock = gates.get("global_action_lock") if isinstance(gates.get("global_action_lock"), dict) else {}
        if direct_lock:
            projected_gates["global_action_lock"] = {"passed": direct_lock.get("passed")}

        projected_candidate_gates: dict[str, Any] = {}
        all_candidate_gates = event.get("all_candidate_gates") if isinstance(event.get("all_candidate_gates"), dict) else {}
        for name, candidate_gate in all_candidate_gates.items():
            if not isinstance(candidate_gate, dict):
                continue
            nested_gates = candidate_gate.get("gates") if isinstance(candidate_gate.get("gates"), dict) else {}
            nested_lock = nested_gates.get("global_action_lock") if isinstance(nested_gates.get("global_action_lock"), dict) else {}
            if nested_lock:
                projected_candidate_gates[str(name)] = {"gates": {"global_action_lock": {"passed": nested_lock.get("passed")}}}

        return {
            "selected_action": {"action": selected.get("action")},
            "execution_plan": {
                "action": execution_plan.get("action"),
                "executable": bool(execution_plan.get("executable", False)),
                "execute": bool(execution_plan.get("execute", False)),
                "reason": execution_plan.get("reason"),
                "blocked_by": [str(item) for item in execution_plan.get("blocked_by", []) if item]
                if isinstance(execution_plan.get("blocked_by"), list)
                else [],
            },
            "gates": projected_gates,
            "all_candidate_gates": projected_candidate_gates,
        }

    def _load_production_action_events(self, now: datetime) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for logs_dir in self._production_log_dirs():
            out.extend(self._production_actions_from_fast_recovery(logs_dir / "fast_recovery_events.jsonl", now))
            out.extend(self._production_actions_from_stream_engine(logs_dir / "stream_engine_events.jsonl", now))
            out.extend(self._production_actions_from_stream_watchdog(logs_dir / "stream_watchdog_events.jsonl", now))
        out = self._dedupe_production_actions(out)
        out.sort(key=lambda item: item["ts"])
        return out

    def _production_log_dirs(self) -> list[Path]:
        dirs = []
        for root in (self.config.source_state_root, self.config.state_root):
            logs_dir = root / "logs"
            if logs_dir not in dirs:
                dirs.append(logs_dir)
        return dirs

    def _dedupe_production_actions(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen = set()
        out = []
        for item in events:
            key = (isoformat_utc(item["ts"]), item.get("action"), item.get("source"), item.get("reason"))
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
        return out

    def _production_actions_from_fast_recovery(self, path: Path, now: datetime) -> list[dict[str, Any]]:
        out = []
        for event in iter_jsonl(path):
            ts = parse_utc(event.get("ts_utc"))
            if ts is None or ts > now or event.get("kind") != "restart":
                continue
            restart_context = event.get("restart_context") if isinstance(event.get("restart_context"), dict) else {}
            trigger = str(event.get("trigger") or restart_context.get("trigger") or event.get("message") or "restart")
            out.append({
                "ts": ts,
                "action": "restart_stream",
                "source": "fast_recovery_events",
                "reason": trigger,
                "trigger": trigger,
            })
        return out

    def _production_actions_from_stream_engine(self, path: Path, now: datetime) -> list[dict[str, Any]]:
        out = []
        restart_events = {"ffmpeg_restart_scheduled", "ffmpeg_restarted", "self_recovery"}
        for event in iter_jsonl(path):
            ts = parse_utc(event.get("ts_utc"))
            event_type = str(event.get("event_type") or event.get("kind") or "")
            if ts is None or ts > now or event_type not in restart_events:
                continue
            out.append({
                "ts": ts,
                "action": "restart_ffmpeg",
                "source": "stream_engine_events",
                "reason": event_type,
            })
        return out

    def _production_actions_from_stream_watchdog(self, path: Path, now: datetime) -> list[dict[str, Any]]:
        out = []
        action_by_event = {
            "dj_restart": "restart_dj",
            "restart_dj": "restart_dj",
            "stream_restart": "restart_stream",
            "restart_stream": "restart_stream",
            "youtube_watchdog_restart": "restart_stream",
        }
        for event in iter_jsonl(path):
            ts = parse_utc(event.get("ts_utc"))
            event_type = str(event.get("event_type") or event.get("kind") or "")
            action = action_by_event.get(event_type)
            if ts is None or ts > now or not action:
                continue
            out.append({
                "ts": ts,
                "action": action,
                "source": "stream_watchdog_events",
                "reason": event_type,
            })
        return out

    def _window(
        self,
        name: str,
        seconds: int,
        status_events: list[dict[str, Any]],
        orchestrator_events: list[dict[str, Any]],
        production_action_events: list[dict[str, Any]],
        now: datetime,
    ) -> dict[str, Any]:
        start = now - timedelta(seconds=seconds)
        in_window = [item for item in status_events if item["ts"] >= start]
        orch_in_window = [item for item in orchestrator_events if item["ts"] >= start]
        prod_in_window = [item for item in production_action_events if item["ts"] >= start]
        if not in_window:
            coverage = 0.0
            complete = False
            same_url_live_ratio = None
            unknown_ratio = None
            degraded_ratio = None
        else:
            first_ts = in_window[0]["ts"]
            coverage = max(0.0, (now - first_ts).total_seconds())
            complete = coverage >= seconds * WINDOW_COMPLETE_COVERAGE_RATIO
            live_sec = 0.0
            unknown_sec = 0.0
            degraded_sec = 0.0
            for idx, item in enumerate(in_window):
                this_ts = item["ts"]
                next_ts = in_window[idx + 1]["ts"] if idx + 1 < len(in_window) else now
                dur = max(0.0, (next_ts - this_ts).total_seconds())
                payload = item["payload"]
                overall = payload.get("overall") if isinstance(payload.get("overall"), dict) else {}
                if overall.get("stream_public_state") == "same_url_live":
                    live_sec += dur
                if overall.get("state") == "unknown" or str(overall.get("stream_public_state", "")).startswith("unknown"):
                    unknown_sec += dur
                if overall.get("state") in {"degraded", "failed", "recovering"}:
                    degraded_sec += dur
            if complete and coverage > 0:
                same_url_live_ratio = live_sec / coverage
                unknown_ratio = unknown_sec / coverage
                degraded_ratio = degraded_sec / coverage
            else:
                same_url_live_ratio = None
                unknown_ratio = None
                degraded_ratio = None
        shadow_decisions = self._shadow_decision_sli(orch_in_window, prod_in_window)
        return {
            "data_coverage_sec": round(coverage, 3),
            "window_complete": complete,
            "window_complete_threshold_sec": round(seconds * WINDOW_COMPLETE_COVERAGE_RATIO, 3),
            "expected_sample_interval_sec": SHADOW_TIMER_INTERVAL_SEC,
            "status_sample_count": len(in_window),
            "orchestrator_sample_count": len(orch_in_window),
            "same_url_live_ratio": None if same_url_live_ratio is None else round(same_url_live_ratio, 6),
            "unknown_ratio": None if unknown_ratio is None else round(unknown_ratio, 6),
            "degraded_ratio": None if degraded_ratio is None else round(degraded_ratio, 6),
            "replacement_count": self._count_actions(orch_in_window, "create_replacement_broadcast"),
            "budget_override_count": self._count_gate_reason(orch_in_window, "budget_override"),
            "destructive_action_count": self._count_destructive(orch_in_window),
            "global_action_lock_block_count": self._count_global_action_lock_blocks(orch_in_window),
            "subsystems_shadow": {
                "status_sample_count": len(in_window),
                "orchestrator_sample_count": len(orch_in_window),
                "selected_action_counts": shadow_decisions["selected_action_counts"],
                "selected_action_distribution": shadow_decisions["selected_action_distribution"],
                "execution_plan_counts": shadow_decisions["execution_plan_counts"],
                "executable_plan_counts": shadow_decisions["executable_plan_counts"],
                "recovery_intent_action_counts": shadow_decisions["recovery_intent_action_counts"],
                "production_action_counts": shadow_decisions["production_action_counts"],
                "production_action_sample_count": shadow_decisions["production_action_sample_count"],
                "shadow_non_none_action_count": shadow_decisions["shadow_non_none_action_count"],
                "shadow_executable_plan_action_count": shadow_decisions["shadow_executable_plan_action_count"],
                "shadow_recovery_intent_action_count": shadow_decisions["shadow_recovery_intent_action_count"],
                "shadow_destructive_action_count": shadow_decisions["shadow_destructive_action_count"],
                "shadow_vs_production_exact_agreement_count": shadow_decisions["exact_agreement_count"],
                "shadow_vs_production_scope_compatible_agreement_count": shadow_decisions["scope_compatible_agreement_count"],
                "shadow_vs_production_disagreement_count": shadow_decisions["disagreement_count"],
                "shadow_vs_production_disagreement_ratio": shadow_decisions["disagreement_ratio"],
                "shadow_vs_production_disagreement_by_reason": shadow_decisions["disagreement_by_reason"],
                "current_classifier_replay": shadow_decisions["current_classifier_replay"],
                "match_window_sec": PRODUCTION_DIFF_MATCH_WINDOW_SEC,
                "diff_basis": "execution_plan.executable recovery actions only; report-only alert/probe/resync decisions excluded",
                "interpretation": "shadow decisions are observations only; production actions remain owned by current watchdog/fast-recovery/stream-engine paths",
            },
        }

    def _shadow_decision_sli(self, orchestrator_events: list[dict[str, Any]], production_events: list[dict[str, Any]]) -> dict[str, Any]:
        selected_counts = self._selected_action_counts(orchestrator_events)
        selected_distribution = self._distribution(selected_counts)
        execution_plan_counts = self._execution_plan_counts(orchestrator_events)
        executable_plan_counts = self._executable_plan_counts(orchestrator_events)
        recovery_intent_events = [item for item in orchestrator_events if self._has_recovery_intent(item)]
        recovery_intent_counts = self._execution_plan_counts(recovery_intent_events)
        production_counts = self._production_action_counts(production_events)
        non_none_shadow = [item for item in orchestrator_events if self._selected_action(item) != "none"]
        executable_shadow = [
            item
            for item in orchestrator_events
            if self._execution_plan_action(item) != "none" and self._execution_plan_executable(item)
        ]
        destructive_count = self._count_destructive(orchestrator_events)
        diff = self._diff_shadow_vs_production(orchestrator_events, production_events)
        current_classifier_replay = self._current_classifier_replay(production_events)
        return {
            "selected_action_counts": selected_counts,
            "selected_action_distribution": selected_distribution,
            "execution_plan_counts": execution_plan_counts,
            "executable_plan_counts": executable_plan_counts,
            "recovery_intent_action_counts": recovery_intent_counts,
            "production_action_counts": production_counts,
            "production_action_sample_count": len(production_events),
            "shadow_non_none_action_count": len(non_none_shadow),
            "shadow_executable_plan_action_count": len(executable_shadow),
            "shadow_recovery_intent_action_count": len(recovery_intent_events),
            "shadow_destructive_action_count": destructive_count,
            "current_classifier_replay": current_classifier_replay,
            **diff,
        }

    def _selected_action_counts(self, events: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in events:
            action = self._selected_action(item)
            counts[action] = counts.get(action, 0) + 1
        return dict(sorted(counts.items()))

    def _execution_plan_counts(self, events: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in events:
            action = self._execution_plan_action(item)
            counts[action] = counts.get(action, 0) + 1
        return dict(sorted(counts.items()))

    def _executable_plan_counts(self, events: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in events:
            action = self._execution_plan_action(item)
            if action == "none" or not self._execution_plan_executable(item):
                continue
            counts[action] = counts.get(action, 0) + 1
        return dict(sorted(counts.items()))

    def _production_action_counts(self, events: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in events:
            action = str(item.get("action") or "unknown")
            counts[action] = counts.get(action, 0) + 1
        return dict(sorted(counts.items()))

    def _current_classifier_replay(self, production_events: list[dict[str, Any]]) -> dict[str, Any]:
        eligible = [
            item
            for item in production_events
            if item.get("source") == "fast_recovery_events" and item.get("action") == "restart_stream"
        ]
        covered = [item for item in eligible if self._current_classifier_action_for_production(item) == item.get("action")]
        covered_ids = {id(item) for item in covered}
        uncovered = [item for item in eligible if id(item) not in covered_ids]
        return {
            "classifier": "local_delivery_fast_recovery_stream_restart_v1",
            "target_action": "restart_stream",
            "basis": "current classifier replay over historical fast_recovery restart events; historical orchestrator JSONL is not backfilled",
            "eligible_count": len(eligible),
            "covered_count": len(covered),
            "uncovered_count": len(uncovered),
            "coverage_ratio": None if not eligible else round(len(covered) / len(eligible), 6),
            "covered_by_trigger": self._count_production_triggers(covered),
            "uncovered_by_trigger": self._count_production_triggers(uncovered),
        }

    def _current_classifier_action_for_production(self, event: dict[str, Any]) -> str:
        trigger = str(event.get("trigger") or event.get("reason") or "")
        if trigger in FAST_RECOVERY_STREAM_RESTART_FAILURE_BY_TRIGGER:
            return "restart_stream"
        return "none"

    def _count_production_triggers(self, events: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in events:
            trigger = str(item.get("trigger") or item.get("reason") or "unknown")
            counts[trigger] = counts.get(trigger, 0) + 1
        return dict(sorted(counts.items()))

    def _distribution(self, counts: dict[str, int]) -> dict[str, float]:
        total = sum(counts.values())
        if total <= 0:
            return {}
        return {key: round(value / total, 6) for key, value in counts.items()}

    def _selected_action(self, item: dict[str, Any]) -> str:
        selected = item["payload"].get("selected_action") if isinstance(item.get("payload"), dict) else {}
        selected = selected if isinstance(selected, dict) else {}
        return str(selected.get("action") or "none")

    def _execution_plan(self, item: dict[str, Any]) -> dict[str, Any]:
        payload = item.get("payload") if isinstance(item.get("payload"), dict) else {}
        plan = payload.get("execution_plan") if isinstance(payload.get("execution_plan"), dict) else {}
        return plan

    def _execution_plan_action(self, item: dict[str, Any]) -> str:
        plan = self._execution_plan(item)
        return str(plan.get("action") or "none")

    def _execution_plan_executable(self, item: dict[str, Any]) -> bool:
        return bool(self._execution_plan(item).get("executable", False))

    def _has_recovery_intent(self, item: dict[str, Any]) -> bool:
        action = self._execution_plan_action(item)
        return action in EXECUTOR_RECOVERY_ACTIONS and self._execution_plan_executable(item)

    def _diff_shadow_vs_production(self, orchestrator_events: list[dict[str, Any]], production_events: list[dict[str, Any]]) -> dict[str, Any]:
        exact_agreement = 0
        compatible_agreement = 0
        disagreements = 0
        by_reason: dict[str, int] = {
            "timing": 0,
            "production_without_shadow": 0,
            "false_positive_shadow": 0,
            "action_mismatch": 0,
        }
        recovery_intent_events = [item for item in orchestrator_events if self._has_recovery_intent(item)]
        matched_shadow_ids: set[int] = set()

        for prod in production_events:
            nearest = self._nearest_orchestrator_event(recovery_intent_events, prod["ts"])
            if nearest is None:
                disagreements += 1
                by_reason["production_without_shadow"] += 1
                continue
            matched_shadow_ids.add(id(nearest))
            shadow_action = self._execution_plan_action(nearest)
            prod_action = str(prod.get("action") or "unknown")
            if shadow_action == prod_action:
                exact_agreement += 1
                compatible_agreement += 1
                continue
            if self._actions_scope_compatible(shadow_action, prod_action):
                compatible_agreement += 1
                disagreements += 1
                by_reason["action_mismatch"] += 1
                continue
            disagreements += 1
            if shadow_action == "none":
                by_reason["production_without_shadow"] += 1
            else:
                by_reason["action_mismatch"] += 1

        for item in recovery_intent_events:
            if id(item) in matched_shadow_ids:
                continue
            disagreements += 1
            by_reason["false_positive_shadow"] += 1

        comparable = len(production_events) + sum(
            1 for item in recovery_intent_events if id(item) not in matched_shadow_ids
        )
        return {
            "exact_agreement_count": exact_agreement,
            "scope_compatible_agreement_count": compatible_agreement,
            "disagreement_count": disagreements,
            "disagreement_ratio": None if comparable == 0 else round(disagreements / comparable, 6),
            "disagreement_by_reason": by_reason,
        }

    def _nearest_orchestrator_event(self, events: list[dict[str, Any]], ts: datetime) -> dict[str, Any] | None:
        best: dict[str, Any] | None = None
        best_delta = PRODUCTION_DIFF_MATCH_WINDOW_SEC + 1.0
        for item in events:
            delta = abs((item["ts"] - ts).total_seconds())
            if delta <= PRODUCTION_DIFF_MATCH_WINDOW_SEC and delta < best_delta:
                best = item
                best_delta = delta
        return best

    def _actions_scope_compatible(self, left: str, right: str) -> bool:
        if left == right:
            return True
        return left in LOCAL_RESTART_ACTIONS and right in LOCAL_RESTART_ACTIONS

    def _count_actions(self, events: list[dict[str, Any]], action: str) -> int:
        count = 0
        for item in events:
            selected = item["payload"].get("selected_action") if isinstance(item["payload"].get("selected_action"), dict) else {}
            if selected.get("action") == action:
                count += 1
        return count

    def _count_destructive(self, events: list[dict[str, Any]]) -> int:
        destructive = {
            "restart_stream",
            "force_current_broadcast_live",
            "bind_current_stream",
            "transition_current_broadcast",
            "create_replacement_broadcast",
            "cleanup_stale_broadcast",
        }
        count = 0
        for item in events:
            selected = item["payload"].get("selected_action") if isinstance(item["payload"].get("selected_action"), dict) else {}
            if selected.get("action") in destructive:
                count += 1
        return count

    def _count_gate_reason(self, events: list[dict[str, Any]], needle: str) -> int:
        count = 0
        for item in events:
            gates = item["payload"].get("gates") if isinstance(item["payload"].get("gates"), dict) else {}
            budget = gates.get("budget") if isinstance(gates.get("budget"), dict) else {}
            if needle in str(budget.get("reason", "")):
                count += 1
        return count

    def _count_global_action_lock_blocks(self, events: list[dict[str, Any]]) -> int:
        count = 0
        for item in events:
            payload = item["payload"]
            gates = payload.get("gates") if isinstance(payload.get("gates"), dict) else {}
            direct = gates.get("global_action_lock") if isinstance(gates.get("global_action_lock"), dict) else None
            if direct is not None and direct.get("passed") is False:
                count += 1
                continue
            all_candidate_gates = payload.get("all_candidate_gates") if isinstance(payload.get("all_candidate_gates"), dict) else {}
            for candidate_gate in all_candidate_gates.values():
                if not isinstance(candidate_gate, dict):
                    continue
                nested_gates = candidate_gate.get("gates") if isinstance(candidate_gate.get("gates"), dict) else {}
                lock_gate = nested_gates.get("global_action_lock") if isinstance(nested_gates.get("global_action_lock"), dict) else {}
                if lock_gate.get("passed") is False:
                    count += 1
                    break
        return count
