from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import DEFAULT_V2_STATE_ROOT
from .jsonio import latest_jsonl, read_json

SUBSYSTEM_ORDER = ["rendering", "music", "local_delivery", "youtube_lifecycle", "monitoring"]


def build_status_summary(state_root: Path = DEFAULT_V2_STATE_ROOT) -> dict[str, Any]:
    status = read_json(state_root / "subsystems_status.json") or {}
    orchestrator = latest_jsonl(state_root / "logs" / "recovery_orchestrator.jsonl") or {}
    action_plan = read_json(state_root / "recovery_action_plan.json") or {}
    objective_sli = read_json(state_root / "objective_sli.json") or {}
    stream_components = read_json(state_root / "stream_components.json") or {}

    overall = status.get("overall") if isinstance(status.get("overall"), dict) else {}
    selected_action = orchestrator.get("selected_action") if isinstance(orchestrator.get("selected_action"), dict) else {}
    result = orchestrator.get("result") if isinstance(orchestrator.get("result"), dict) else {}
    actor = orchestrator.get("actor") if isinstance(orchestrator.get("actor"), dict) else {}
    target = orchestrator.get("target") if isinstance(orchestrator.get("target"), dict) else {}
    decision = orchestrator.get("decision") if isinstance(orchestrator.get("decision"), dict) else {}

    subsystems = {name: _subsystem_row(name, status.get(name)) for name in SUBSYSTEM_ORDER}
    replacement_policy = _replacement_policy(status)
    stream_missing = stream_components.get("missing") if isinstance(stream_components.get("missing"), list) else []

    return {
        "schema_version": 1,
        "state_root": str(state_root),
        "answer": _operator_answer(overall, selected_action, replacement_policy, stream_missing),
        "when": {
            "status_ts_utc": status.get("ts_utc", ""),
            "orchestrator_ts_utc": orchestrator.get("ts_utc", ""),
            "objective_sli_ts_utc": objective_sli.get("ts_utc", ""),
        },
        "actor": {
            "name": actor.get("name", ""),
            "mode": actor.get("mode", ""),
            "trigger": actor.get("trigger", ""),
            "event_id": orchestrator.get("event_id", ""),
        },
        "target": {
            "stream_id": target.get("stream_id") or "adsb-streamnew",
            "expected_video_id": overall.get("expected_video_id") or target.get("expected_video_id", ""),
            "expected_watch_url": target.get("expected_watch_url", ""),
            "broadcast_id": target.get("broadcast_id", ""),
            "bound_stream_id": target.get("bound_stream_id", ""),
        },
        "observed_state": {
            "overall": overall.get("state", "unknown"),
            "stream_public_state": overall.get("stream_public_state", "unknown"),
            "expected_url_state": overall.get("expected_url_state", "unknown"),
            "degraded_subsystems": overall.get("degraded_subsystems", []),
            "consistency_window_sec": overall.get("consistency_window_sec"),
            "max_consistency_window_sec": overall.get("max_consistency_window_sec"),
        },
        "subsystems": subsystems,
        "decision": {
            "state": decision.get("state", ""),
            "failure_name": decision.get("failure_name", ""),
            "reason": decision.get("reason") or overall.get("action_reason", ""),
            "confidence": decision.get("confidence", ""),
        },
        "selected_action": {
            "action": selected_action.get("action", "none"),
            "scope": selected_action.get("scope", "none"),
            "execute": bool(selected_action.get("execute")),
            "reason": selected_action.get("reason", ""),
            "result_status": result.get("status", ""),
            "result_reason": result.get("reason", ""),
        },
        "execution_plan": _execution_plan_summary(action_plan or orchestrator.get("execution_plan", {})),
        "blocked_actions": _blocked_actions(orchestrator),
        "replacement_policy": replacement_policy,
        "stream_components": {
            "missing_count": len(stream_missing),
            "missing": stream_missing,
            "subsystems": sorted((stream_components.get("subsystems") or {}).keys()) if isinstance(stream_components.get("subsystems"), dict) else [],
        },
        "objective_sli": _objective_sli_summary(objective_sli),
        "warnings": _warnings(overall, selected_action, replacement_policy, stream_missing),
    }


def render_text_summary(summary: dict[str, Any]) -> str:
    observed = summary.get("observed_state", {})
    selected = summary.get("selected_action", {})
    target = summary.get("target", {})
    lines = [
        f"answer: {summary.get('answer', '')}",
        f"when: status={summary.get('when', {}).get('status_ts_utc', '')} orchestrator={summary.get('when', {}).get('orchestrator_ts_utc', '')}",
        f"target: stream={target.get('stream_id', '')} video={target.get('expected_video_id', '')} url={target.get('expected_watch_url', '')}",
        f"overall: state={observed.get('overall', 'unknown')} public={observed.get('stream_public_state', 'unknown')} expected_url={observed.get('expected_url_state', 'unknown')} consistency={observed.get('consistency_window_sec')}/{observed.get('max_consistency_window_sec')}",
        f"decision: {summary.get('decision', {}).get('state', '')} reason={summary.get('decision', {}).get('reason', '')}",
        f"selected_action: action={selected.get('action', 'none')} scope={selected.get('scope', 'none')} execute={selected.get('execute', False)} result={selected.get('result_reason', '')}",
        f"execution_plan: action={summary.get('execution_plan', {}).get('action', 'none')} executable={summary.get('execution_plan', {}).get('executable', False)} execute={summary.get('execution_plan', {}).get('execute', False)} blocked_by={','.join(summary.get('execution_plan', {}).get('blocked_by', []))}",
        "subsystems:",
    ]
    for name in SUBSYSTEM_ORDER:
        row = summary.get("subsystems", {}).get(name, {})
        lines.append(
            f"  - {name}: state={row.get('state', 'unknown')} action={row.get('recommended_action', 'none')} evidence_age={row.get('evidence_age_sec')} blocked_by={','.join(row.get('blocked_by', []))}"
        )
    replacement = summary.get("replacement_policy", {})
    lines.append(f"replacement: allowed={replacement.get('allowed', False)} reason={replacement.get('reason', '')}")
    warnings = summary.get("warnings", [])
    if warnings:
        lines.append("warnings: " + ", ".join(str(item) for item in warnings))
    return "\n".join(lines)


def dumps_summary(summary: dict[str, Any], *, pretty: bool = False) -> str:
    return json.dumps(summary, ensure_ascii=False, indent=2 if pretty else None)


def _subsystem_row(name: str, payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"state": "unknown", "recommended_action": "none", "evidence": [], "blocked_by": ["missing_subsystem_status"]}
    return {
        "state": payload.get("state", "unknown"),
        "confidence": payload.get("confidence", "unknown"),
        "evidence": payload.get("evidence", []),
        "evidence_age_sec": payload.get("evidence_age_sec"),
        "last_ok_ts_utc": payload.get("last_ok_ts_utc", ""),
        "recommended_action": payload.get("recommended_action", "none"),
        "blocked_by": payload.get("blocked_by", []),
        "caused_by_subsystems": payload.get("caused_by_subsystems", []),
        "affects_subsystems": payload.get("affects_subsystems", []),
    }


def _replacement_policy(status: dict[str, Any]) -> dict[str, Any]:
    youtube = status.get("youtube_lifecycle") if isinstance(status.get("youtube_lifecycle"), dict) else {}
    policy = youtube.get("replacement_policy") if isinstance(youtube.get("replacement_policy"), dict) else {}
    return {
        "allowed": bool(policy.get("allowed", False)),
        "reason": policy.get("reason", "missing_replacement_policy"),
        "required_missing": policy.get("required_missing", []),
        "same_url_preserved": bool(youtube.get("same_url_preserved", False)),
        "current_url_recoverable": bool(youtube.get("current_url_recoverable", False)),
    }


def _blocked_actions(orchestrator: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    candidates = orchestrator.get("all_candidate_gates")
    if not isinstance(candidates, dict):
        return out
    for action, payload in candidates.items():
        if not isinstance(payload, dict):
            continue
        if payload.get("passed"):
            continue
        out.append({"action": action, "blocked_by": payload.get("blocked_by", []), "gates": payload.get("gates", {})})
    return out


def _objective_sli_summary(payload: dict[str, Any]) -> dict[str, Any]:
    windows = payload.get("windows") if isinstance(payload.get("windows"), dict) else {}
    last_24h = windows.get("last_24h") if isinstance(windows.get("last_24h"), dict) else {}
    return {
        "last_24h_same_url_live_ratio": last_24h.get("same_url_live_ratio"),
        "last_24h_unknown_ratio": last_24h.get("unknown_ratio"),
        "last_24h_replacement_count": last_24h.get("replacement_count"),
        "last_24h_budget_override_count": last_24h.get("budget_override_count"),
        "last_24h_window_complete": last_24h.get("window_complete"),
        "last_24h_data_coverage_sec": last_24h.get("data_coverage_sec"),
    }


def _execution_plan_summary(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        payload = {}
    steps = payload.get("steps") if isinstance(payload.get("steps"), list) else []
    return {
        "action": payload.get("action", "none"),
        "scope": payload.get("scope", "none"),
        "mode": payload.get("mode", ""),
        "executable": bool(payload.get("executable", False)),
        "execute": bool(payload.get("execute", False)),
        "reason": payload.get("reason", ""),
        "blocked_by": payload.get("blocked_by", []),
        "step_count": len(steps),
        "steps": steps,
    }


def _operator_answer(overall: dict[str, Any], selected_action: dict[str, Any], replacement_policy: dict[str, Any], stream_missing: list[Any]) -> str:
    state = overall.get("state", "unknown")
    public = overall.get("stream_public_state", "unknown")
    action = selected_action.get("action", "none")
    if stream_missing:
        return "degraded: stream_v2 component mapping has missing files"
    if state == "healthy" and public == "same_url_live" and action == "none":
        return "healthy: same URL live, no recovery action selected"
    if state == "unknown":
        return "unknown: evidence is insufficient or stale; destructive action must remain blocked"
    if action and action != "none":
        return f"{state}: selected shadow action is {action}"
    if not replacement_policy.get("allowed", False):
        return f"{state}: replacement blocked by {replacement_policy.get('reason', '')}"
    return str(state)


def _warnings(overall: dict[str, Any], selected_action: dict[str, Any], replacement_policy: dict[str, Any], stream_missing: list[Any]) -> list[str]:
    warnings: list[str] = []
    if stream_missing:
        warnings.append("stream_components_missing")
    if overall.get("state") == "unknown":
        warnings.append("overall_unknown_destructive_action_blocked")
    action = selected_action.get("action", "none")
    if action not in {"", "none"}:
        warnings.append(f"shadow_selected_action:{action}")
    if replacement_policy.get("allowed"):
        warnings.append("replacement_allowed_requires_operator_attention")
    return warnings
