from __future__ import annotations

import copy
import random
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from cra_no_action_soak.time import isoformat_utc, parse_utc

READINESS = {
    "input_fresh": True,
    "parity_clean": True,
    "projection_clean": True,
    "source_ready": True,
}
PARITY_POLICY_REVISION = "monitoring-v4-parity-convergence-r2"


def _exact_non_negative_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def validate_no_action_cutover_handoff(
    *,
    arena_adapter: Mapping[str, Any],
    arena_producer_status: Mapping[str, Any],
    arena_projection: Mapping[str, Any],
    cra_pull_status: Mapping[str, Any],
    cra_projection: Mapping[str, Any],
    expected_arena_release: str,
    expected_cra_release: str,
    now: datetime,
    minimum_remaining_seconds: float = 10.0,
) -> dict[str, Any]:
    """Validate the exact warning-free handoff used to begin a NO_ACTION soak.

    Capability/effect counters intentionally belong to adapter, producer-status,
    and pull-status schemas. The signed projection schema does not expose them.
    """

    _require(now.tzinfo is not None, "CUTOVER_NOW_NOT_TIMEZONE_AWARE")
    _require(minimum_remaining_seconds > 0, "CUTOVER_MINIMUM_TTL_INVALID")

    _require(arena_adapter.get("schema") == "monitoring_v4.cra_live_adapter_status.v2", "ARENA_ADAPTER_SCHEMA_INVALID")
    _require(arena_adapter.get("status") == "READY", "ARENA_ADAPTER_NOT_READY")
    _require(arena_adapter.get("adapter_release_id") == expected_arena_release, "ARENA_ADAPTER_RELEASE_MISMATCH")
    _require(arena_adapter.get("readiness") == READINESS, "ARENA_ADAPTER_READINESS_INVALID")
    _require(arena_adapter.get("control_capability_count") == 0, "ARENA_ADAPTER_CONTROL_CAPABILITY_NONZERO")
    _require(arena_adapter.get("physical_effect_count") == 0, "ARENA_ADAPTER_PHYSICAL_EFFECT_NONZERO")

    parity = arena_adapter.get("parity")
    if not isinstance(parity, Mapping):
        raise ValueError("ARENA_PARITY_MISSING")
    _require(parity.get("schema") == "monitoring_v4.live_parity.v2", "ARENA_PARITY_SCHEMA_INVALID")
    _require(parity.get("parity_policy_revision") == PARITY_POLICY_REVISION, "ARENA_PARITY_POLICY_MISMATCH")
    _require(parity.get("equivalent") is True, "ARENA_PARITY_NOT_EQUIVALENT")
    _require(parity.get("input_integrity_errors") == [], "ARENA_PARITY_INPUT_INTEGRITY_ERROR")
    _require(parity.get("unclassified_contract_difference_count") == 0, "ARENA_PARITY_UNCLASSIFIED_DIFFERENCE")
    integrity = parity.get("projection_integrity")
    if not isinstance(integrity, Mapping):
        raise ValueError("ARENA_PARITY_PROJECTION_INTEGRITY_MISSING")
    _require(integrity.get("complete") is True, "ARENA_PARITY_PROJECTION_INCOMPLETE")
    _require(integrity.get("missing_keys") == [], "ARENA_PARITY_PROJECTION_KEYS_MISSING")
    _require(integrity.get("unexpected_keys") == [], "ARENA_PARITY_PROJECTION_KEYS_UNEXPECTED")
    _require(integrity.get("rejection_count") == 0, "ARENA_PARITY_PROJECTION_REJECTED")
    _require(
        _exact_non_negative_integer(integrity.get("expected_count"))
        and integrity.get("projection_count") == integrity.get("expected_count"),
        "ARENA_PARITY_PROJECTION_COUNT_CONTRADICTION",
    )

    _require(arena_projection.get("schema") == "monitoring_v4.evidence_projection.v1", "ARENA_PROJECTION_SCHEMA_INVALID")
    _require(arena_projection.get("source_release_id") == expected_arena_release, "ARENA_PROJECTION_RELEASE_MISMATCH")
    _require(arena_projection.get("readiness") == READINESS, "ARENA_PROJECTION_READINESS_INVALID")
    projection_id = arena_projection.get("projection_id")
    projection_sequence = arena_projection.get("observation_sequence")
    _require(isinstance(projection_id, str) and bool(projection_id), "ARENA_PROJECTION_ID_INVALID")
    _require(_exact_non_negative_integer(projection_sequence), "ARENA_PROJECTION_SEQUENCE_INVALID")

    _require(
        arena_producer_status.get("schema") == "monitoring_v4.cra_projection_producer_status.v1",
        "ARENA_PRODUCER_STATUS_SCHEMA_INVALID",
    )
    _require(arena_producer_status.get("status") == "READY", "ARENA_PRODUCER_NOT_READY")
    _require(arena_producer_status.get("producer_release_id") == expected_arena_release, "ARENA_PRODUCER_RELEASE_MISMATCH")
    _require(arena_producer_status.get("projection_id") == projection_id, "ARENA_PRODUCER_PROJECTION_ID_MISMATCH")
    _require(arena_producer_status.get("observation_sequence") == projection_sequence, "ARENA_PRODUCER_SEQUENCE_MISMATCH")
    _require(arena_producer_status.get("control_capability_count") == 0, "ARENA_PRODUCER_CONTROL_CAPABILITY_NONZERO")
    _require(arena_producer_status.get("physical_effect_count") == 0, "ARENA_PRODUCER_PHYSICAL_EFFECT_NONZERO")

    _require(cra_pull_status.get("schema") == "cra.monitoring_projection_pull_status.v1", "CRA_PULL_STATUS_SCHEMA_INVALID")
    _require(cra_pull_status.get("status") == "READY", "CRA_PULL_NOT_READY")
    _require(cra_pull_status.get("puller_release_id") == expected_cra_release, "CRA_PULL_RELEASE_MISMATCH")
    _require(cra_pull_status.get("projection_id") == projection_id, "CRA_PULL_PROJECTION_ID_MISMATCH")
    _require(cra_pull_status.get("observation_sequence") == projection_sequence, "CRA_PULL_SEQUENCE_MISMATCH")
    _require(cra_pull_status.get("transport_attempt_count") == 1, "CRA_PULL_ATTEMPT_COUNT_INVALID")
    _require(cra_pull_status.get("transport_retry_count") == 0, "CRA_PULL_RETRY_COUNT_NONZERO")
    _require(cra_pull_status.get("control_capability_count") == 0, "CRA_PULL_CONTROL_CAPABILITY_NONZERO")
    _require(cra_pull_status.get("physical_effect_count") == 0, "CRA_PULL_PHYSICAL_EFFECT_NONZERO")

    _require(dict(cra_projection) == dict(arena_projection), "CRA_INBOX_PROJECTION_MISMATCH")
    issued_at = parse_utc(str(arena_projection.get("issued_at", "")))
    expires_at = parse_utc(str(arena_projection.get("expires_at", "")))
    _require(issued_at <= now.astimezone(UTC), "ARENA_PROJECTION_ISSUED_IN_FUTURE")
    _require(expires_at > issued_at, "ARENA_PROJECTION_TIME_RANGE_INVALID")
    remaining_seconds = (expires_at - now.astimezone(UTC)).total_seconds()
    _require(remaining_seconds >= minimum_remaining_seconds, "CRA_INBOX_PROJECTION_TTL_INSUFFICIENT")

    return {
        "schema": "cra.no_action_cutover_handoff.v1",
        "status": "PASS",
        "arena_release_id": expected_arena_release,
        "cra_release_id": expected_cra_release,
        "projection_id": projection_id,
        "observation_sequence": projection_sequence,
        "remaining_seconds": round(remaining_seconds, 3),
        "transport_attempt_count": 1,
        "transport_retry_count": 0,
        "control_capability_count": 0,
        "physical_effect_count": 0,
    }


CHAOS_SCENARIOS = (
    "ADAPTER_EFFECT",
    "PARITY_POLICY",
    "PARITY_COUNT",
    "PROJECTION_RELEASE",
    "PROJECTION_READINESS",
    "PRODUCER_ID",
    "PULL_ATTEMPT",
    "PULL_RETRY",
    "CRA_INBOX_MUTATION",
    "TTL_EXHAUSTION",
)


def _valid_fixture(now: datetime, ttl_seconds: float) -> dict[str, dict[str, Any]]:
    arena_release = "arena-cra-projection-test"
    cra_release = "cra-no-action-test"
    projection = {
        "schema": "monitoring_v4.evidence_projection.v1",
        "source_release_id": arena_release,
        "projection_id": "projection-test",
        "observation_sequence": 17,
        "issued_at": isoformat_utc(now - timedelta(seconds=1)),
        "expires_at": isoformat_utc(now + timedelta(seconds=ttl_seconds)),
        "readiness": dict(READINESS),
    }
    return {
        "arena_adapter": {
            "schema": "monitoring_v4.cra_live_adapter_status.v2",
            "status": "READY",
            "adapter_release_id": arena_release,
            "control_capability_count": 0,
            "physical_effect_count": 0,
            "readiness": dict(READINESS),
            "parity": {
                "schema": "monitoring_v4.live_parity.v2",
                "parity_policy_revision": PARITY_POLICY_REVISION,
                "equivalent": True,
                "input_integrity_errors": [],
                "unclassified_contract_difference_count": 0,
                "projection_integrity": {
                    "complete": True,
                    "expected_count": 6,
                    "projection_count": 6,
                    "rejection_count": 0,
                    "missing_keys": [],
                    "unexpected_keys": [],
                },
            },
        },
        "arena_producer_status": {
            "schema": "monitoring_v4.cra_projection_producer_status.v1",
            "status": "READY",
            "producer_release_id": arena_release,
            "projection_id": "projection-test",
            "observation_sequence": 17,
            "control_capability_count": 0,
            "physical_effect_count": 0,
        },
        "arena_projection": projection,
        "cra_pull_status": {
            "schema": "cra.monitoring_projection_pull_status.v1",
            "status": "READY",
            "puller_release_id": cra_release,
            "projection_id": "projection-test",
            "observation_sequence": 17,
            "transport_attempt_count": 1,
            "transport_retry_count": 0,
            "control_capability_count": 0,
            "physical_effect_count": 0,
        },
        "cra_projection": copy.deepcopy(projection),
    }


def run_cutover_handoff_chaos(*, case_count: int, seed: int) -> dict[str, Any]:
    _require(case_count >= len(CHAOS_SCENARIOS), "CUTOVER_CHAOS_CASE_COUNT_TOO_SMALL")
    rng = random.Random(seed)
    now = datetime(2026, 9, 1, 8, 0, tzinfo=UTC)
    detections = dict.fromkeys(CHAOS_SCENARIOS, 0)
    failures: list[dict[str, Any]] = []
    for index in range(case_count):
        scenario = CHAOS_SCENARIOS[index] if index < len(CHAOS_SCENARIOS) else rng.choice(CHAOS_SCENARIOS)
        fixture = _valid_fixture(now, rng.uniform(10.5, 120.0))
        if scenario == "ADAPTER_EFFECT":
            fixture["arena_adapter"]["physical_effect_count"] = 1
        elif scenario == "PARITY_POLICY":
            fixture["arena_adapter"]["parity"]["parity_policy_revision"] = "wrong"
        elif scenario == "PARITY_COUNT":
            fixture["arena_adapter"]["parity"]["projection_integrity"]["projection_count"] = 5
        elif scenario == "PROJECTION_RELEASE":
            fixture["arena_projection"]["source_release_id"] = "old"
        elif scenario == "PROJECTION_READINESS":
            fixture["arena_projection"]["readiness"]["parity_clean"] = False
        elif scenario == "PRODUCER_ID":
            fixture["arena_producer_status"]["projection_id"] = "other"
        elif scenario == "PULL_ATTEMPT":
            fixture["cra_pull_status"]["transport_attempt_count"] = 2
        elif scenario == "PULL_RETRY":
            fixture["cra_pull_status"]["transport_retry_count"] = 1
        elif scenario == "CRA_INBOX_MUTATION":
            fixture["cra_projection"]["projection_id"] = "other"
        elif scenario == "TTL_EXHAUSTION":
            expires = now + timedelta(seconds=rng.uniform(-5.0, 9.999))
            fixture["arena_projection"]["expires_at"] = isoformat_utc(expires)
            fixture["cra_projection"]["expires_at"] = fixture["arena_projection"]["expires_at"]
        try:
            validate_no_action_cutover_handoff(
                arena_adapter=fixture["arena_adapter"],
                arena_producer_status=fixture["arena_producer_status"],
                arena_projection=fixture["arena_projection"],
                cra_pull_status=fixture["cra_pull_status"],
                cra_projection=fixture["cra_projection"],
                expected_arena_release="arena-cra-projection-test",
                expected_cra_release="cra-no-action-test",
                now=now,
            )
        except (TypeError, ValueError):
            detections[scenario] += 1
        else:
            failures.append({"case_index": index, "scenario": scenario})
    return {
        "schema": "cra.no_action_cutover_handoff_chaos.v1",
        "pass": not failures,
        "seed": seed,
        "case_count": case_count,
        "failure_count": len(failures),
        "scenario_detection_count": detections,
        "failures": failures,
        "production_target_touched": False,
    }
