from __future__ import annotations

from typing import Any

from cra_dell_recovery.models import TargetIdentity
from cra_dell_recovery.recovery_verification import CORE_RECOVERY_CHECKS, build_recovery_verification
from cra_dell_recovery.time import parse_utc


class FakeMonitoringObserver:
    """Deterministic test producer. It never imports the Harness oracle."""

    def verify(
        self,
        *,
        command_id: str,
        incident_id: str,
        target_id: str,
        monitoring_cycle_id: str,
        command_state: str,
        before_target: TargetIdentity,
        observed_target: TargetIdentity,
        checks: dict[str, dict[str, str]],
        observed_at: str,
        evidence_fresh_until: str,
        evaluated_at: str,
        observation_revision: str,
        policy_revision: str = "monitoring-v4-recovery-verification-r1",
    ) -> dict[str, Any]:
        unexpected_target_change = any(
            getattr(observed_target, field) != getattr(before_target, field)
            for field in ("host_id", "host_boot_id", "namespace", "pod_uid", "container_name", "container_id")
        )
        stale = parse_utc(evaluated_at) > parse_utc(evidence_fresh_until)
        core_results = {name: checks[name]["result"] for name in CORE_RECOVERY_CHECKS}
        reasons: tuple[str, ...]
        if command_state != "EFFECT_OBSERVED":
            verdict, reasons = "UNKNOWN", ("PHYSICAL_EFFECT_NOT_CONFIRMED",)
        elif unexpected_target_change:
            verdict, reasons = "UNKNOWN", ("UNEXPECTED_TARGET_IDENTITY", "RECONCILIATION_REQUIRED")
        elif stale:
            verdict, reasons = "UNKNOWN", ("VERIFICATION_EVIDENCE_STALE",)
        elif any(result == "FAIL" for result in core_results.values()):
            verdict, reasons = (
                "FAILED",
                tuple(sorted(f"{name.upper()}_FAILED" for name, result in core_results.items() if result == "FAIL")),
            )
        elif any(result != "PASS" for result in core_results.values()):
            verdict, reasons = (
                "UNKNOWN",
                tuple(sorted(f"{name.upper()}_NOT_PROVEN" for name, result in core_results.items() if result != "PASS")),
            )
        elif observed_target.ffmpeg_generation == before_target.ffmpeg_generation:
            verdict, reasons = "FAILED", ("FFMPEG_GENERATION_UNCHANGED",)
        else:
            verdict, reasons = "RECOVERED", ("FRESH_CORE_RECOVERY_EVIDENCE_CONFIRMED",)
        return build_recovery_verification(
            command_id=command_id,
            incident_id=incident_id,
            target_id=target_id,
            monitoring_cycle_id=monitoring_cycle_id,
            observed_at=observed_at,
            evidence_fresh_until=evidence_fresh_until,
            expected_effect={
                "action": "restart_ffmpeg",
                "before_ffmpeg_generation": before_target.ffmpeg_generation,
                "require_new_ffmpeg_generation": True,
                "require_same_host_boot_id": True,
                "require_same_pod_uid": True,
            },
            observed_target=observed_target.to_dict(),
            checks=checks,
            verdict=verdict,
            reason_codes=reasons,
            observation_revision=observation_revision,
            policy_revision=policy_revision,
        )
