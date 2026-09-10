from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from cra_dell_recovery.effect_scope import effect_scope_id
from runtime_boundary.ledger import EffectLedger
from runtime_boundary.legacy_reconciliation import reconcile_legacy_effect_pairs
from runtime_boundary.model import EffectRequest


def _request(request_id: str, target: dict[str, object], *, offset: int) -> EffectRequest:
    issued = datetime(2026, 8, 25, 8, 22, tzinfo=UTC) + timedelta(seconds=offset)
    return EffectRequest.from_mapping(
        {
            "schema_version": "runtime.typed_effect_request.v2",
            "request_id": request_id,
            "idempotency_key": request_id,
            "producer_id": "independent-controller",
            "producer_generation": 2,
            "intent_type": "RESTART_FFMPEG",
            "reason": "historical timeout",
            "failure_domain": "DELIVERY_PATH",
            "issued_at": issued.isoformat().replace("+00:00", "Z"),
            "expires_at": (issued + timedelta(seconds=3)).isoformat().replace("+00:00", "Z"),
            "ffmpeg_target_identity": target,
            "runtime_identity": None,
            "expected_ffmpeg_generation": "generation-1",
            "correlation_id": request_id,
            "target_snapshot_id": f"snapshot-{request_id}",
            "runtime_snapshot_id": "",
            "runtime_observation_id": f"observation-{request_id}",
            "expected_executor_instance_id": "executor-1",
            "maintenance_evidence_status": "AVAILABLE",
            "projection_id": f"projection-{request_id}",
            "projection_sequence": 10 + offset,
        }
    )


def test_legacy_pair_reconciliation_preserves_raw_unknown_and_is_idempotent(tmp_path: Path) -> None:
    target = {
        "host_id": "dell-yuki",
        "host_boot_id": "boot-1",
        "namespace": "stream-v3",
        "pod_uid": "pod-1",
        "container_name": "stream-engine",
        "container_id": "containerd://one",
        "ffmpeg_generation": "ffmpeg-one",
        "ffmpeg_pid": 100,
    }
    ledger = EffectLedger(
        tmp_path / "ledger.sqlite3",
        initial_producer_id="independent-controller",
        initial_producer_generation=2,
    )
    first = _request("first", target, offset=0)
    assert ledger.accept(first)["accepted"] is True
    assert ledger.transition("first", expected="ACCEPTED", state="EXECUTION_STARTED") is True
    assert ledger.mark_effect_boundary("first") is True
    assert (
        ledger.transition(
            "first",
            expected="EXECUTION_STARTED",
            state="OUTCOME_UNKNOWN",
            result={"reason": "SIGTERM_SENT_EXIT_NOT_OBSERVED"},
        )
        is True
    )
    ledger.connection.execute("DELETE FROM effect_scope_fences")

    second = _request("second", target, offset=32)
    ledger.connection.execute(
        """INSERT INTO typed_effect_requests(
               request_id,idempotency_key,request_digest,producer_id,producer_generation,
               intent_type,operation,failure_domain,correlation_id,target_snapshot_id,
               runtime_snapshot_id,runtime_observation_id,expected_executor_instance_id,
               maintenance_evidence_status,projection_id,projection_sequence,identity_type,
               identity_json,ffmpeg_target_identity_json,runtime_identity_json,
               expected_ffmpeg_generation,request_json,state,result_json,accepted_at,
               execution_started_at,finished_at
           ) SELECT ?,?,? ,producer_id,producer_generation,intent_type,operation,failure_domain,
                    ?,?,runtime_snapshot_id,?,expected_executor_instance_id,
                    maintenance_evidence_status,?,projection_sequence+1,identity_type,
                    identity_json,ffmpeg_target_identity_json,runtime_identity_json,
                    expected_ffmpeg_generation,?,'EFFECT_OBSERVED',?,accepted_at,
                    execution_started_at,finished_at
             FROM typed_effect_requests WHERE request_id='first'""",
        (
            second.request_id,
            second.idempotency_key,
            second.digest(),
            second.correlation_id,
            second.target_snapshot_id,
            second.runtime_observation_id,
            second.projection_id,
            __import__("json").dumps(second.canonical(), separators=(",", ":"), sort_keys=True),
            '{"physical_effect_count":1}',
        ),
    )
    ledger._backfill_effect_scope_fences()

    report = reconcile_legacy_effect_pairs(ledger)
    assert report["candidate_pair_count"] == 1
    assert report["reconciled_count"] == 1
    assert report["unresolved_scope_count"] == 0
    assert ledger.request("first")["state"] == "OUTCOME_UNKNOWN"  # type: ignore[index]
    scope = ledger.scope(effect_scope_id("restart_ffmpeg", target))
    assert scope is not None
    assert scope["state"] == "RECONCILED_EFFECT_OBSERVED"

    repeated = reconcile_legacy_effect_pairs(ledger)
    assert repeated["reconciled_count"] == 0
    assert repeated["already_recorded_count"] == 1
    ledger.close()
