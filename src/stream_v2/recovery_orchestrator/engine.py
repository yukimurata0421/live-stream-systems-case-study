from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Optional

from ..action_lock import LockState
from ..config import RuntimeConfig
from ..model import ActionCandidate, SubsystemsSnapshot
from ..timeutil import isoformat_utc
from .executor import ActionPlanBuilder
from .gate import ActionGate
from .proposer import ActionProposer
from .types import GateResult, OrchestratorDecision


def _evt(prefix: str) -> str:
    return f"evt-{prefix}-{uuid.uuid4().hex[:12]}"


class RecoveryOrchestrator:
    """Shadow recovery orchestrator engine."""

    def __init__(
        self,
        config: RuntimeConfig,
        proposer: Optional[ActionProposer] = None,
        gate: Optional[ActionGate] = None,
        planner: Optional[ActionPlanBuilder] = None,
    ):
        self.config = config
        self.proposer = proposer or ActionProposer()
        self.gate = gate or ActionGate()
        self.planner = planner or ActionPlanBuilder()

    def evaluate(self, snapshot: SubsystemsSnapshot, *, now: datetime, lock_state: LockState) -> OrchestratorDecision:
        candidates = self.proposer.propose(snapshot)
        selected: Optional[ActionCandidate] = None
        selected_gate: Optional[GateResult] = None
        all_gate_results: dict[str, Any] = {}

        for candidate in candidates:
            gate_result = self.gate.evaluate(snapshot, candidate, lock_state=lock_state)
            all_gate_results[candidate.action] = {"passed": gate_result.passed, "blocked_by": gate_result.blocked_by, "gates": gate_result.gates}
            if selected is None and candidate.preconditions_met and gate_result.passed:
                selected = candidate
                selected_gate = gate_result

        if selected is None:
            selected = ActionCandidate("none", "none", 0, "none", True, True)
            selected_gate = GateResult(gates={}, passed=True, blocked_by=["all_action_candidates_blocked"])

        execution_plan = self.planner.build(
            snapshot,
            selected,
            selected_gate,
            mode=self.config.mode,
            supervisor_mode=self.config.supervisor_mode,
        )
        execute = execution_plan.execute
        event = {
            "ts_utc": isoformat_utc(now),
            "event_id": _evt("orch"),
            "schema_version": 1,
            "actor": {
                "name": "recovery_orchestrator",
                "version": "v1",
                "mode": self.config.mode,
                "trigger": "timer_or_manual_shadow_once",
                "source_event_ids": self._source_event_ids(snapshot),
            },
            "target": {
                "stream_id": self.config.stream_id,
                "expected_video_id": snapshot.overall.expected_video_id,
                "expected_watch_url": f"https://youtube.com/watch?v={snapshot.overall.expected_video_id}" if snapshot.overall.expected_video_id else "",
                "broadcast_id": str(snapshot.youtube_lifecycle.extra.get("broadcast_id", snapshot.overall.expected_video_id)),
                "bound_stream_id": str(snapshot.youtube_lifecycle.extra.get("bound_stream_id", "")),
                "stream_key_hash": "",
            },
            "observed_state": {
                "overall": snapshot.overall.state,
                "stream_public_state": snapshot.overall.stream_public_state,
                "expected_url_state": snapshot.overall.expected_url_state,
                "subsystems": {name: subsystem.state for name, subsystem in snapshot.subsystems.items()},
                "degraded_subsystems": snapshot.overall.degraded_subsystems,
                "consistency_window_sec": snapshot.overall.consistency_window_sec,
                "max_consistency_window_sec": snapshot.overall.max_consistency_window_sec,
            },
            "evidence": self._evidence(snapshot),
            "decision": {
                "state": self._decision_state(snapshot),
                "failure_name": self._failure_name(snapshot),
                "reason": snapshot.overall.action_reason,
                "confidence": self._overall_confidence(snapshot),
                "caused_by_subsystems": self._all_caused_by(snapshot),
                "affects_subsystems": self._all_affects(snapshot),
            },
            "action_candidates": [candidate.to_dict() for candidate in candidates],
            "gates": selected_gate.gates if selected_gate else {},
            "all_candidate_gates": all_gate_results,
            "selected_action": {
                "action": selected.action,
                "scope": selected.scope,
                "mode": self.config.mode,
                "execute": execute,
                "reason": self._selected_reason(selected, selected_gate),
            },
            "execution_plan": execution_plan.to_dict(),
            "result": {"status": "not_executed", "reason": "shadow_mode", "completed_at_utc": ""},
        }
        return OrchestratorDecision(event=event, selected_action=selected.action, execute=execute, execution_plan=execution_plan.to_dict())

    def _selected_reason(self, candidate: ActionCandidate, gate: Optional[GateResult]) -> str:
        if candidate.action == "none":
            return "no executable action in shadow decision"
        if gate and gate.blocked_by:
            return "selected candidate has gate blocks"
        return "lowest destructive action that addresses confirmed subsystem state" if candidate.destructive_level in {"medium", "high", "very_high"} else "lowest scoped non-destructive action"

    def _source_event_ids(self, snapshot: SubsystemsSnapshot) -> list[str]:
        ids: list[str] = []
        for subsystem in snapshot.subsystems.values():
            for ev in subsystem.evidence_records:
                if ev.event_id and ev.event_id not in ids:
                    ids.append(ev.event_id)
        return ids[:50]

    def _evidence(self, snapshot: SubsystemsSnapshot) -> list[dict[str, Any]]:
        records = []
        for subsystem in snapshot.subsystems.values():
            records.extend(ev.to_dict() for ev in subsystem.evidence_records)
        return records

    def _decision_state(self, snapshot: SubsystemsSnapshot) -> str:
        if snapshot.overall.state == "healthy":
            return "all_subsystems_healthy"
        return "subsystem_" + snapshot.overall.state

    def _failure_name(self, snapshot: SubsystemsSnapshot) -> str:
        for subsystem in snapshot.subsystems.values():
            if subsystem.state in {"failed", "degraded"} and subsystem.evidence:
                return subsystem.evidence[0]
        if snapshot.overall.state == "unknown":
            return "unknown_evidence"
        return "none"

    def _overall_confidence(self, snapshot: SubsystemsSnapshot) -> str:
        if snapshot.overall.state == "healthy":
            return "high"
        if snapshot.overall.state == "unknown":
            return "unknown"
        return "medium"

    def _all_caused_by(self, snapshot: SubsystemsSnapshot) -> list[str]:
        return sorted({item for subsystem in snapshot.subsystems.values() for item in subsystem.caused_by_subsystems})

    def _all_affects(self, snapshot: SubsystemsSnapshot) -> list[str]:
        return sorted({item for subsystem in snapshot.subsystems.values() for item in subsystem.affects_subsystems})
