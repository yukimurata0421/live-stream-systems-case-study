from __future__ import annotations

from datetime import UTC, datetime, timedelta

from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.recovery_verification import (
    CHECK_NAMES,
    SOURCE_CONTRACT,
    RecoveryVerificationProducer,
    VerificationEvidence,
)


def _stamp(offset: int = 0) -> str:
    return (
        (datetime(2026, 8, 23, tzinfo=UTC) + timedelta(seconds=offset))
        .isoformat()
        .replace("+00:00", "Z")
    )


def _checks(
    *, result: str = "PASS", stale: bool = False
) -> dict[str, VerificationEvidence]:
    return {
        name: VerificationEvidence(
            result=result if name != "external_public_signal" else "NOT_APPLICABLE",
            evidence_ref=stable_id("evidence", name),
            observed_at=_stamp(),
            valid_until=_stamp(0 if stale else 60),
            evidence_role="current_authoritative"
            if name not in {"external_public_signal", "youtube_state"}
            else "supporting",
            source_contract=SOURCE_CONTRACT[name],
        )
        for name in CHECK_NAMES
    }


def _produce(
    checks: dict[str, VerificationEvidence],
    *,
    observed_generation: str | None = "generation-new",
):
    return RecoveryVerificationProducer().produce(
        command_id="command-1",
        incident_id="incident-1",
        target_id="stream-target",
        monitoring_cycle_id="cycle-1",
        expected_effect={
            "before_ffmpeg_generation": "generation-old",
            "before_host_boot_id": "boot-1",
            "before_pod_uid": "pod-1",
        },
        observed_target={
            "host_id": "dell",
            "host_boot_id": "boot-1",
            "namespace": "stream-v3",
            "pod_uid": "pod-1",
            "container_name": "stream-engine",
            "container_id": "container-1",
            "ffmpeg_generation": observed_generation,
            "ffmpeg_pid": 10,
        },
        checks=checks,
        policy_revision="recovery-verification-v1",
        now_at=_stamp(1),
    )


def test_fresh_authoritative_core_checks_recover() -> None:
    assert _produce(_checks()).to_dict()["verdict"] == "RECOVERED"


def test_stale_core_evidence_is_unknown() -> None:
    assert _produce(_checks(stale=True)).to_dict()["verdict"] == "UNKNOWN"


def test_public_evidence_cannot_replace_unknown_core() -> None:
    checks = _checks()
    current = checks["tcp_flow_healthy"]
    checks["tcp_flow_healthy"] = VerificationEvidence(
        "UNKNOWN",
        current.evidence_ref,
        current.observed_at,
        current.valid_until,
        current.evidence_role,
        current.source_contract,
    )
    assert _produce(checks).to_dict()["verdict"] == "UNKNOWN"


def test_fresh_generation_unchanged_failure_is_failed_not_unknown() -> None:
    checks = _checks()
    current = checks["ffmpeg_generation_changed"]
    checks["ffmpeg_generation_changed"] = VerificationEvidence(
        "FAIL",
        current.evidence_ref,
        current.observed_at,
        current.valid_until,
        current.evidence_role,
        current.source_contract,
    )

    result = _produce(checks, observed_generation="generation-old").to_dict()
    assert result["verdict"] == "FAILED"
    assert result["reason_codes"] == ["CORE_CHECK_FAILED"]


def test_generation_pass_claim_conflicting_with_unchanged_target_is_unknown() -> None:
    assert (
        _produce(_checks(), observed_generation="generation-old").to_dict()["verdict"]
        == "UNKNOWN"
    )


def test_missing_observed_generation_is_unknown() -> None:
    assert (
        _produce(_checks(), observed_generation=None).to_dict()["verdict"] == "UNKNOWN"
    )
