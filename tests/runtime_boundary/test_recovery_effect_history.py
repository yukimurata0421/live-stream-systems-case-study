from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from cra_authority.recovery_safety import query_only
from cra_dell_recovery.recovery_history import POLICY_SCHEMA, empty_policy, history_hash, validate_effect_history, validate_policy
from runtime_boundary.ledger import EffectLedger
from runtime_boundary.recovery_evidence import effect_safety

NOW = datetime(2026, 9, 4, tzinfo=UTC)
TARGET = {
    "host_id": "dell-yuki",
    "host_boot_id": "boot",
    "namespace": "stream-v3",
    "pod_uid": "current-pod",
    "container_name": "stream-engine",
    "container_id": "containerd://current",
    "ffmpeg_pid": 1000,
    "ffmpeg_generation": "current",
}


@pytest.fixture
def history(tmp_path: Path) -> Iterator[tuple[EffectLedger, dict[str, Any]]]:
    ledger = EffectLedger(tmp_path / "owner.sqlite3", initial_producer_id="owner", initial_producer_generation=1)
    entries = []
    for number in (1, 2):
        identity = {**TARGET, "pod_uid": f"old-pod-{number}", "ffmpeg_pid": 100 + number}
        scope = hashlib.sha256(f"scope-{number}".encode()).hexdigest()
        proof = {
            "schema_version": "runtime.retired_target_reconciliation_evidence.v1",
            "oracle": "EXACT_TARGET_RETIRED_AND_HEALTHY_REPLACEMENT",
            "physical_effect_outcome": "UNKNOWN",
            "automatic_retry_count": 0,
            "physical_attempt_count": 1,
            "before_target": identity,
            "observed_target": TARGET,
        }
        encoded = json.dumps(proof, sort_keys=True, separators=(",", ":"))
        created, retired = "2026-09-01T00:00:00.000Z", "2026-09-01T01:00:00.000Z"
        ledger.connection.execute(
            "INSERT INTO effect_scope_fences(effect_scope_id,action,identity_json,owner_request_id,owner_request_digest,state,"
            "physical_attempt_count,created_at,updated_at,result_json) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                scope,
                "restart_ffmpeg",
                json.dumps(identity),
                f"old-request-{number}",
                "digest",
                "RETIRED_TARGET_OUTCOME_UNKNOWN",
                1,
                created,
                retired,
                encoded,
            ),
        )
        ledger.connection.execute(
            "INSERT INTO effect_scope_retirements VALUES(?,?,?,?,?)",
            (f"retirement-{number}", scope, encoded, hashlib.sha256(encoded.encode()).hexdigest(), retired),
        )
        entries.append(
            {
                "effect_scope_id": scope,
                "target_sha256": history_hash(identity),
                "retirement_sha256": history_hash(proof),
                "created_at": created,
                "retired_at": retired,
                "physical_attempt_count": 1,
                "physical_effect_outcome": "UNKNOWN",
            }
        )
    policy = {
        "schema": POLICY_SCHEMA,
        "frozen_at": "2026-09-03T00:00:00.000Z",
        "retired_unknown": sorted(entries, key=lambda item: item["effect_scope_id"]),
    }
    try:
        yield ledger, policy
    finally:
        ledger.close()


def read(ledger: EffectLedger, policy: dict[str, Any], target: dict[str, Any] = TARGET) -> dict[str, Any]:
    with query_only(ledger.path) as connection:
        return effect_safety(connection, now=NOW, allowed_producers=["owner"], target_identity=target, history_policy=policy)


def test_two_unknowns_remain_visible_without_changing_ledger(history: Any) -> None:
    ledger, policy = history
    before = ledger.connection.iterdump()
    original = list(before)
    result = read(ledger, policy)
    validate_effect_history(result, policy=policy, target=history_hash(TARGET), now=NOW)
    assert result["unresolved_scope_count_total"] == result["physical_attempt_count"] == 2
    assert result["unresolved_scope_count"] == result["current_target_unresolved_scope_count"] == 0
    assert result["historical_retired_unknown"] == policy["retired_unknown"]
    assert all(e["physical_effect_outcome"] == "UNKNOWN" for e in result["historical_retired_unknown"])
    assert list(ledger.connection.iterdump()) == original
    assert ledger.connection.execute("PRAGMA query_only").fetchone()[0] == 0
    assert read(ledger, empty_policy())["unresolved_scope_count"] == 2


@pytest.mark.parametrize("state", ["OUTCOME_UNKNOWN", "RETIRED_TARGET_OUTCOME_UNKNOWN"])
@pytest.mark.parametrize("created", ["2026-09-01T00:00:00.000Z", "2026-09-04T00:00:00.000Z"])
def test_unlisted_unknown_never_becomes_a_historical_exception(history: Any, state: str, created: str) -> None:
    ledger, policy = history
    ledger.connection.execute(
        "INSERT INTO effect_scope_fences(effect_scope_id,action,identity_json,owner_request_id,owner_request_digest,state,"
        "physical_attempt_count,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
        ("new-scope", "restart_ffmpeg", json.dumps(TARGET), "new-request", "digest", state, 1, created, created),
    )
    result = read(ledger, policy)
    assert result["unresolved_scope_count_total"] == result["physical_attempt_count"] == 3
    assert result["unresolved_scope_count"] == result["current_target_unresolved_scope_count"] == 1
    assert len(result["historical_retired_unknown"]) == 2


@pytest.mark.parametrize("change", ["missing", "proof", "digest", "target", "state", "result", "action", "created"])
def test_frozen_record_change_fails_closed(history: Any, change: str) -> None:
    ledger, policy = history
    scope = policy["retired_unknown"][0]["effect_scope_id"]
    if change == "missing":
        ledger.connection.execute("DELETE FROM effect_scope_retirements WHERE effect_scope_id=?", (scope,))
    elif change == "proof":
        ledger.connection.execute("UPDATE effect_scope_retirements SET evidence_json='{}' WHERE effect_scope_id=?", (scope,))
    elif change == "digest":
        ledger.connection.execute("UPDATE effect_scope_retirements SET evidence_digest='wrong' WHERE effect_scope_id=?", (scope,))
    else:
        column, value = {
            "target": ("identity_json", json.dumps(TARGET)),
            "state": ("state", "RECONCILED_EFFECT_OBSERVED"),
            "result": ("result_json", "{}"),
            "action": ("action", "restart_host"),
            "created": ("created_at", NOW.isoformat()),
        }[change]
        ledger.connection.execute(f"UPDATE effect_scope_fences SET {column}=? WHERE effect_scope_id=?", (value, scope))
    with pytest.raises(ValueError, match="EFFECT_HISTORY_FROZEN_RECORD"):
        read(ledger, policy)


def test_bookkeeping_timestamp_is_not_mistaken_for_new_effect(history: Any) -> None:
    ledger, policy = history
    ledger.connection.execute("UPDATE effect_scope_fences SET updated_at=?", (NOW.isoformat(),))
    assert read(ledger, policy)["unresolved_scope_count"] == 0


def test_old_target_cannot_reappear_as_an_excluded_current_target(history: Any) -> None:
    ledger, policy = history
    scope = policy["retired_unknown"][0]["effect_scope_id"]
    target = json.loads(
        ledger.connection.execute("SELECT identity_json FROM effect_scope_fences WHERE effect_scope_id=?", (scope,)).fetchone()[0]
    )
    with pytest.raises(ValueError, match="RETIRED_TARGET_REAPPEARED"):
        read(ledger, policy, target)


@pytest.mark.parametrize("change", ["duplicate", "missing-timezone", "after-freeze", "success", "bool", "hash", "order"])
def test_invalid_policy_cannot_approve_history(history: Any, change: str) -> None:
    _, original = history
    policy = copy.deepcopy(original)
    item = policy["retired_unknown"][0]
    if change == "duplicate":
        policy["retired_unknown"].append(dict(item))
    elif change == "missing-timezone":
        policy["frozen_at"] = "2026-09-03T00:00:00"
    elif change == "after-freeze":
        item["retired_at"] = NOW.isoformat()
    elif change == "success":
        item["physical_effect_outcome"] = "SUCCESS"
    elif change == "bool":
        item["physical_attempt_count"] = True
    elif change == "hash":
        item["retirement_sha256"] = "unbound"
    else:
        policy["retired_unknown"].reverse()
    with pytest.raises(ValueError):
        validate_policy(policy, now=NOW)


@pytest.mark.parametrize("change", ["total", "scoped", "current", "missing", "bool", "hash", "retimed"])
def test_signed_facts_cannot_hide_or_reclassify_history(history: Any, change: str) -> None:
    ledger, policy = history
    value = read(ledger, policy)
    if change == "total":
        value["unresolved_scope_count_total"] = 0
    elif change == "scoped":
        value["unresolved_scope_count"] = 1
    elif change == "current":
        value["current_target_unresolved_scope_count"] = 1
    elif change == "missing":
        value["historical_retired_unknown"].pop()
    elif change == "bool":
        value["historical_retired_unknown"][0]["physical_attempt_count"] = True
    elif change == "hash":
        value["history_policy_sha256"] = "a" * 64
    else:
        value["historical_retired_unknown"][0]["retired_at"] = NOW.isoformat()
    with pytest.raises(ValueError):
        validate_effect_history(value, policy=policy, target=history_hash(TARGET), now=NOW)


def test_freeze_must_precede_the_new_epoch(history: Any) -> None:
    _, policy = history
    with pytest.raises(ValueError, match="FREEZE_AFTER_EPOCH"):
        validate_policy(policy, now=NOW - timedelta(days=2))
