from __future__ import annotations

from typing import Any, Callable


def restart_decision_from_gate(
    *,
    gate_decision: Any,
    restart_decision_cls: Callable[..., Any],
    stream_service: str,
    detail: str = "",
) -> Any:
    if getattr(gate_decision, "action", "") == "restart_stream":
        return restart_decision_cls(True, f"restart {stream_service}", gate_decision.reason)
    if getattr(gate_decision, "action", "") == "none":
        return restart_decision_cls(False, "none", gate_decision.reason)
    if getattr(gate_decision, "action", "") == "restart_suppressed_ingest_connected":
        return restart_decision_cls(False, "restart suppressed: ingest tcp connected", gate_decision.reason)
    if detail:
        return restart_decision_cls(False, f"restart deferred: evidence gate {gate_decision.action}: {detail}", detail)
    return restart_decision_cls(False, f"restart deferred: evidence gate {gate_decision.action}", gate_decision.reason)
