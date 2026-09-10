from __future__ import annotations

import json
from typing import Any

import pytest

from cra_authority.reconciliation import CentralReconciler
from cra_dell_recovery.errors import CommandBlocked
from cra_dell_recovery.models import LocalRecoveryCandidate
from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.execution import FakeTargetObserver
from dell_recovery_agent.reconciliation import DellReconciliationService


def _candidate(environment: object, action_id: str = "local-handback-a") -> LocalRecoveryCandidate:
    return LocalRecoveryCandidate(
        local_action_id=action_id,
        target_id="stream-target",
        action="restart_ffmpeg",
        reason_code="confirmed_tcp_stall",
        target_identity=environment.target,  # type: ignore[attr-defined]
        evidence={"tcp_stall_confirmed": True, "distinct_sample_count": 3, "stall_confirm_threshold": 3},
        observed_at=isoformat_utc(utc_now()),
    )


def _peer(environment: object) -> DellReconciliationService:
    return DellReconciliationService(
        environment.dell,  # type: ignore[attr-defined]
        environment.agent_codec,  # type: ignore[attr-defined]
        FakeTargetObserver(environment.target),  # type: ignore[attr-defined]
    )


def _complete_local_action(environment: object) -> None:
    environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "TEST_LAN_DOWN")  # type: ignore[attr-defined]
    result = environment.service(environment.target, environment.target).handle_local_candidate(_candidate(environment))  # type: ignore[attr-defined]
    assert result == "EFFECT_OBSERVED"


def test_completed_local_action_is_pulled_once_and_never_replayed_as_command(environment: object) -> None:
    _complete_local_action(environment)
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]

    first_epoch = CentralReconciler(environment.central, environment.central_codec, _peer(environment)).reconcile("stream-target")  # type: ignore[attr-defined]
    imported = environment.central.read_one("SELECT count(*) FROM dell_journal_records")  # type: ignore[attr-defined]
    ack = environment.dell.read_one("SELECT ack_sequence FROM local_action_journal_acks WHERE target_id='stream-target'")  # type: ignore[attr-defined]

    assert first_epoch == 2
    assert imported is not None and int(imported[0]) == 3
    assert ack is not None and int(ack[0]) == 3
    assert environment.central.command_count() == 0  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]
    effect = environment.central.read_one(  # type: ignore[attr-defined]
        """SELECT origin_kind,origin_id,state,physical_attempt_count
           FROM effect_scope_ledger"""
    )
    assert effect is not None
    assert tuple(effect) == ("IMPORTED_LOCAL_ACTION", "local-handback-a", "EFFECT_OBSERVED", 1)

    second_epoch = CentralReconciler(environment.central, environment.central_codec, _peer(environment)).reconcile("stream-target")  # type: ignore[attr-defined]
    assert second_epoch == 3
    assert environment.central.read_one("SELECT count(*) FROM dell_journal_records")[0] == 3  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]


def test_verifier_rejects_local_after_target_not_bound_to_imported_journal(environment: object) -> None:
    _complete_local_action(environment)
    CentralReconciler(environment.central, environment.central_codec, _peer(environment)).reconcile("stream-target")  # type: ignore[attr-defined]
    terminal = environment.central.read_one(  # type: ignore[attr-defined]
        """SELECT local_action_id,effect_scope_id,payload_json FROM dell_journal_records
           ORDER BY journal_sequence DESC LIMIT 1"""
    )
    assert terminal is not None
    payload = json.loads(str(terminal["payload_json"]))
    forged_after = {
        **environment.target.to_dict(),  # type: ignore[attr-defined]
        "ffmpeg_generation": "forged-local-generation",
        "ffmpeg_pid": 4300,
    }
    with pytest.raises(CommandBlocked, match="CRA_VERIFIER_LOCAL_AFTER_TARGET_MISMATCH"):
        environment.central.record_verifier_decision(  # type: ignore[attr-defined]
            {
                "verifier_decision_id": "local-forged-verification",
                "command_id": None,
                "local_action_id": str(terminal["local_action_id"]),
                "effect_scope_id": str(terminal["effect_scope_id"]),
                "pre_projection_id": "pre-not-persisted",
                "post_projection_id": "post-not-persisted",
                "verdict": "RECOVERED",
                "reason_codes": [],
                "execution_evidence": {
                    "command_id": None,
                    "local_action_id": str(terminal["local_action_id"]),
                    "effect_scope_id": str(terminal["effect_scope_id"]),
                    "state": str(payload["record_state"]),
                    "physical_attempt_count": int(payload["physical_attempt_count"]),
                    "before_target": payload["before_target"],
                    "after_target": forged_after,
                },
                "decided_at": isoformat_utc(utc_now()),
            }
        )
    assert environment.central.read_one("SELECT count(*) FROM cra_verifier_decisions")[0] == 0  # type: ignore[attr-defined]


def test_unresolved_local_action_blocks_new_epoch(environment: object) -> None:
    environment.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "TEST_LAN_DOWN")  # type: ignore[attr-defined]
    session = environment.dell.begin_local_fallback("stream-target")  # type: ignore[attr-defined]
    stamp = isoformat_utc(utc_now())
    with environment.dell.write() as db:  # type: ignore[attr-defined]
        db.execute(
            "INSERT INTO local_actions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                "local-unresolved",
                session,
                "stream-target",
                1,
                "restart_ffmpeg",
                "confirmed_tcp_stall",
                json.dumps(environment.target.to_dict()),  # type: ignore[attr-defined]
                json.dumps({"tcp_stall_confirmed": True}),
                "OUTCOME_UNKNOWN",
                stamp,
                stamp,
            ),
        )

    with pytest.raises(ValueError, match="unresolved Dell action"):
        CentralReconciler(environment.central, environment.central_codec, _peer(environment)).reconcile("stream-target")  # type: ignore[attr-defined]

    assert environment.central.authority_snapshot("stream-target")["current_authority_epoch"] == 1  # type: ignore[attr-defined]
    assert environment.dell.fence("stream-target")["authority_state"] == "RECONCILING"  # type: ignore[attr-defined]


class AckLossPeer:
    def __init__(self, delegate: DellReconciliationService) -> None:
        self.delegate = delegate
        self.fail_once = True

    def issue_challenge(self, target_id: str) -> dict[str, Any]:
        return self.delegate.issue_challenge(target_id)

    def journal_page(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.delegate.journal_page(payload)

    def commit(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self.delegate.commit(payload)
        if self.fail_once:
            self.fail_once = False
            raise TimeoutError("simulated ACK loss after durable Dell commit")
        return result


class GapPeer:
    def __init__(self, environment: object, delegate: DellReconciliationService) -> None:
        self.environment = environment
        self.delegate = delegate

    def issue_challenge(self, target_id: str) -> dict[str, Any]:
        return self.delegate.issue_challenge(target_id)

    def journal_page(self, payload: dict[str, Any]) -> dict[str, Any]:
        page = self.environment.central_codec.decode(self.delegate.journal_page(payload))  # type: ignore[attr-defined]
        page["records"] = [page["records"][0], page["records"][2]]
        return self.environment.agent_codec.encode(page)  # type: ignore[attr-defined]

    def commit(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.delegate.commit(payload)


def test_signed_but_gapped_journal_page_blocks_handback_without_partial_import(environment: object) -> None:
    _complete_local_action(environment)
    peer = GapPeer(environment, _peer(environment))

    with pytest.raises(CommandBlocked, match="DELL_JOURNAL_SEQUENCE_GAP"):
        CentralReconciler(environment.central, environment.central_codec, peer).reconcile("stream-target")  # type: ignore[attr-defined]

    assert environment.central.read_one("SELECT count(*) FROM dell_journal_records")[0] == 0  # type: ignore[attr-defined]
    assert environment.central.authority_snapshot("stream-target")["current_authority_epoch"] == 1  # type: ignore[attr-defined]


def test_commit_ack_loss_reconciles_with_new_epoch_without_duplicate_import(environment: object) -> None:
    _complete_local_action(environment)
    lossy = AckLossPeer(_peer(environment))
    reconciler = CentralReconciler(environment.central, environment.central_codec, lossy)  # type: ignore[attr-defined]

    with pytest.raises(TimeoutError):
        reconciler.reconcile("stream-target")

    assert environment.central.read_one("SELECT count(*) FROM dell_journal_records")[0] == 3  # type: ignore[attr-defined]
    assert environment.central.authority_snapshot("stream-target")["current_authority_epoch"] == 1  # type: ignore[attr-defined]
    assert environment.dell.fence("stream-target")["highest_authority_epoch_seen"] == 2  # type: ignore[attr-defined]

    epoch = reconciler.reconcile("stream-target")
    assert epoch == 3
    assert environment.central.read_one("SELECT count(*) FROM dell_journal_records")[0] == 3  # type: ignore[attr-defined]
    assert environment.adapter.attempt_count == 1  # type: ignore[attr-defined]
