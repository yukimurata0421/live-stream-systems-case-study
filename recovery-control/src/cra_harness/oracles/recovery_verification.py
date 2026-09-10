from __future__ import annotations

from typing import Any

from cra_dell_recovery.time import parse_utc


class IndependentRecoveryVerificationOracle:
    """Compare fixture expectations and contract bindings without importing the fake producer."""

    @staticmethod
    def evaluate(
        value: dict[str, Any],
        *,
        expected_verdict: str,
        active_command_id: str,
        evaluated_at: str,
    ) -> tuple[str, ...]:
        violations: list[str] = []
        if value["command_id"] != active_command_id:
            violations.append("OLD_COMMAND_VERIFICATION")
            return tuple(violations)
        if value["verdict"] != expected_verdict:
            violations.append("VERDICT_MISMATCH")
        if value["verdict"] == "RECOVERED":
            required = (
                "stream_engine_ready",
                "ffmpeg_present",
                "ffmpeg_generation_changed",
                "tcp_flow_healthy",
                "upload_progress_healthy",
                "startup_gate",
            )
            if any(value["checks"][name]["result"] != "PASS" for name in required):
                violations.append("RECOVERED_WITHOUT_CORE_EVIDENCE")
            if parse_utc(evaluated_at) > parse_utc(str(value["evidence_fresh_until"])):
                violations.append("RECOVERED_FROM_STALE_EVIDENCE")
        return tuple(violations)
