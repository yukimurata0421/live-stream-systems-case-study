"""1080 distinct traces, not 1080 independent fault types or random repetitions.

20 input faults x 3 predecessor states x 3 SQLite lifecycles x 6 recovery traces.
The catalog oracle is independent of production validators. Each trace exercises
the actual signed-file source, CRA NO_ACTION runtime and temporary SQLite DB.
"""

from __future__ import annotations

import json
import sqlite3
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_authority.runtime import CraNoActionRuntime, RuntimeConfig
from cra_dell_recovery.canonical import Signer
from cra_dell_recovery.time import isoformat_utc, parse_utc
from tests.authority.test_cra_no_action_runtime import _write_runtime_fixture

FAULTS = (
    "VALID",
    "PARITY",
    "MAINTENANCE",
    "NETWORK",
    "OPEN",
    "DUPLICATE_ROOT",
    "DUPLICATE_NESTED",
    "NAN",
    "OVERFLOW",
    "DEEP",
    "UTF8",
    "TRUNCATED",
    "BAD_SIGNATURE",
    "WRONG_SOURCE",
    "WRONG_RELEASE",
    "FUTURE",
    "EXPIRED",
    "OLD_OBSERVATION",
    "OLD_CHECK",
    "TIME_ORDER",
)
PREDECESSORS = ("EMPTY", "HEALTHY", "PARITY_BLOCKED")
LIFECYCLES = ("HOT", "REOPEN", "WAL_READER")
RECOVERY_TRACES = ("NEW", "REPLAY", "LATE_OLD", "CONFLICT", "RESTART", "RELAPSE")
POLICY_EXPECTATIONS = {
    "VALID": "WOULD_AUTHORIZE",
    "PARITY": "BLOCKED",
    "MAINTENANCE": "BLOCKED",
    "NETWORK": "BLOCKED",
    "OPEN": "NO_ACTION",
    "HEALTHY": "NO_ACTION",
}


def _body(template: dict[str, Any], sequence: int, fault: str) -> bytes:
    value = deepcopy(template)
    value.update(
        projection_id=f"matrix-projection-{sequence}",
        monitoring_cycle_id=f"matrix-cycle-{sequence}",
        observation_revision=f"matrix-revision-{sequence}",
        observation_sequence=sequence,
    )
    now = parse_utc(value["issued_at"])
    if fault == "PARITY":
        value["readiness"]["parity_clean"] = False
    elif fault == "MAINTENANCE":
        value["checks"]["maintenance"]["status"] = "TRUE"
    elif fault == "NETWORK":
        value["checks"]["network_down"]["status"] = "TRUE"
    elif fault in {"OPEN", "HEALTHY"}:
        value["incident"]["state"] = "OPEN" if fault == "OPEN" else "CLEAR"
        if fault == "HEALTHY":
            value["incident"]["reason_codes"] = []
            value["checks"]["tcp_stall"]["status"] = "FALSE"
            value["checks"]["delivery_bad"]["status"] = "FALSE"
    elif fault == "WRONG_SOURCE":
        value["source_instance_id"] = "untrusted-source"
    elif fault == "WRONG_RELEASE":
        value["source_release_id"] = "other-release"
    elif fault == "FUTURE":
        value["issued_at"] = isoformat_utc(now + timedelta(seconds=10))
        value["expires_at"] = isoformat_utc(now + timedelta(seconds=45))
    elif fault == "EXPIRED":
        value["issued_at"] = isoformat_utc(now - timedelta(seconds=50))
        value["observed_at"] = isoformat_utc(now - timedelta(seconds=51))
        value["expires_at"] = isoformat_utc(now - timedelta(seconds=1))
        for check in value["checks"].values():
            check["observed_at"] = value["observed_at"]
    elif fault == "OLD_OBSERVATION":
        value["observed_at"] = isoformat_utc(now - timedelta(seconds=31))
        for check in value["checks"].values():
            check["observed_at"] = value["observed_at"]
    elif fault == "OLD_CHECK":
        value["checks"]["delivery_bad"]["observed_at"] = isoformat_utc(now - timedelta(seconds=181))
    elif fault == "TIME_ORDER":
        value["expires_at"] = value["issued_at"]
    signer = Signer("monitoring-key-a", Ed25519PrivateKey.from_private_bytes(bytes(range(121, 153))))
    value = signer.sign(value)
    if fault == "BAD_SIGNATURE":
        value["signature"] = "A" * 86 + "=="
    raw = json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    if fault == "DUPLICATE_ROOT":
        raw = b'{"observation_sequence":999999,' + raw[1:]
    elif fault == "DUPLICATE_NESTED":
        raw = raw.replace(b'"input_fresh":true', b'"input_fresh":false,"input_\\u0066resh":true')
    elif fault in {"NAN", "OVERFLOW", "DEEP"}:
        addition = {"NAN": b"NaN", "OVERFLOW": b"1e999", "DEEP": b"[" * 40 + b"0" + b"]" * 40}[fault]
        raw = b'{"unexpected":' + addition + b"," + raw[1:]
    elif fault == "UTF8":
        raw += b"\xff"
    elif fault == "TRUNCATED":
        raw = raw[:-1]
    return raw


def _assert_outcome(status: dict[str, Any], fault: str) -> None:
    assert status["command_delivery_enabled"] is False
    assert status["control_capability_count"] == status["physical_effect_count"] == 0
    if fault in POLICY_EXPECTATIONS:
        assert status["readiness"] == "NO_ACTION_READY"
        assert status["policy_decision"] == POLICY_EXPECTATIONS[fault]
        assert len(status["policy_decision_digest"]) == 64
        assert status["policy_reason_binding"] == "CANDIDATE_AND_DECISION_BOUND_V1"
        if fault == "PARITY":
            assert status["monitoring_readiness"]["parity_clean"] is False
            assert "MONITORING_READINESS_PARITY_CLEAN_FALSE" in status["policy_blockers"]
    else:
        assert status["readiness"] == "SAFE_BLOCKED"
        assert status["policy_decision"] == "NO_ACTION"
        assert status["policy_reason_binding"] == "UNAVAILABLE"
        assert status["policy_blockers"]


@pytest.mark.parametrize("fault", FAULTS)
@pytest.mark.parametrize("predecessor", PREDECESSORS)
@pytest.mark.parametrize("lifecycle", LIFECYCLES)
@pytest.mark.parametrize("recovery_trace", RECOVERY_TRACES)
def test_signed_input_sqlite_and_recovery_trace(
    tmp_path: Path,
    fault: str,
    predecessor: str,
    lifecycle: str,
    recovery_trace: str,
) -> None:
    config = RuntimeConfig.load(_write_runtime_fixture(tmp_path))
    path = Path(config.value["monitoring"]["projection_file"])
    template = json.loads(path.read_bytes())
    runtime = CraNoActionRuntime(config)
    reader: sqlite3.Connection | None = None

    def cycle(sequence: int, kind: str) -> dict[str, Any]:
        path.write_bytes(_body(template, sequence, kind))
        result = runtime.run_once()
        _assert_outcome(result, kind)
        return result

    try:
        if predecessor != "EMPTY":
            cycle(1, "HEALTHY" if predecessor == "HEALTHY" else "PARITY")
        if lifecycle == "REOPEN":
            runtime.close()
            runtime = CraNoActionRuntime(config)
        if lifecycle == "WAL_READER":
            reader = sqlite3.connect(config.value["database"], timeout=0.1)
            reader.execute("BEGIN")
            snapshot_count = reader.execute("SELECT count(*) FROM cra_policy_decisions").fetchone()[0]
        cycle(2, fault)
        recovered = cycle(3, "VALID")
        if recovery_trace == "REPLAY":
            assert cycle(3, "VALID")["policy_decision_digest"] == recovered["policy_decision_digest"]
        elif recovery_trace in {"LATE_OLD", "CONFLICT"}:
            path.write_bytes(
                _body(template, 2 if recovery_trace == "LATE_OLD" else 3, "VALID" if recovery_trace == "LATE_OLD" else "PARITY")
            )
            _assert_outcome(runtime.run_once(), "REJECT")
            cycle(4, "VALID")
        elif recovery_trace == "RESTART":
            runtime.close()
            runtime = CraNoActionRuntime(config)
            assert cycle(3, "VALID")["policy_decision_digest"] == recovered["policy_decision_digest"]
        elif recovery_trace == "RELAPSE":
            cycle(4, fault)
            cycle(5, "VALID")
        if reader is not None:
            assert reader.execute("SELECT count(*) FROM cra_policy_decisions").fetchone()[0] == snapshot_count
            runtime.store.checkpoint("PASSIVE")
            reader.rollback()
        # Independent durable oracle, not just the runtime's zero counters.
        for table in ("recovery_authorizations", "commands", "effect_scope_ledger"):
            row = runtime.store.read_one(f"SELECT count(*) FROM {table}")
            assert row is not None and row[0] == 0
        assert runtime.store.integrity_check() == "ok"
    finally:
        if reader is not None:
            reader.close()
        runtime.close()


def test_matrix_has_distinct_ids_and_cannot_accept_false_green() -> None:
    from itertools import product

    cases = list(product(FAULTS, PREDECESSORS, LIFECYCLES, RECOVERY_TRACES))
    assert len(cases) == len(set(cases)) == 1080
    for observed in (
        {"command_delivery_enabled": True},
        {"command_delivery_enabled": False, "control_capability_count": 1, "physical_effect_count": 0},
        {
            "command_delivery_enabled": False,
            "control_capability_count": 0,
            "physical_effect_count": 0,
            "readiness": "NO_ACTION_READY",
            "policy_decision": "WOULD_AUTHORIZE",
        },
    ):
        with pytest.raises(AssertionError):
            _assert_outcome(observed, "REJECT")
