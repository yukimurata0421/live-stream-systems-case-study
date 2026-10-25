from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from .ledger import EffectLedger
from .model import EffectRequest


@dataclass(frozen=True)
class LegacyEffectPair:
    effect_scope_id: str
    unknown_request_id: str
    observed_request_id: str
    unknown_finished_at: str
    observed_finished_at: str
    unknown_result: dict[str, Any]
    observed_result: dict[str, Any]

    @property
    def reconciliation_id(self) -> str:
        return f"legacy-effect-pair-{self.effect_scope_id}"

    def evidence(self) -> dict[str, Any]:
        return {
            "schema": "runtime.legacy_effect_pair_reconciliation.v1",
            "effect_scope_id": self.effect_scope_id,
            "unknown_request_id": self.unknown_request_id,
            "observed_request_id": self.observed_request_id,
            "unknown_finished_at": self.unknown_finished_at,
            "observed_finished_at": self.observed_finished_at,
            "unknown_result": self.unknown_result,
            "observed_result": self.observed_result,
            "historical_request_count": 2,
            "historical_effect_boundary_count": 2,
            "duplicate_request_count": 1,
            "physical_effect_count": 1,
            "automatic_retry_count": 0,
            "conclusion": "A_LATER_REQUEST_FOR_THE_SAME_EXACT_SCOPE_OBSERVED_AN_EFFECT",
            "limitation": "THIS_DOES_NOT_ATTRIBUTE_THE_EFFECT_TO_EITHER_REQUEST",
        }


def find_legacy_effect_pairs(ledger: EffectLedger) -> tuple[LegacyEffectPair, ...]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    rows = ledger.connection.execute(
        """SELECT request_id,state,request_json,result_json,finished_at
           FROM typed_effect_requests
           WHERE intent_type='RESTART_FFMPEG'
           ORDER BY accepted_at,request_id"""
    ).fetchall()
    for row in rows:
        try:
            request = EffectRequest.from_mapping(dict(json.loads(str(row["request_json"]))))
            result = json.loads(str(row["result_json"] or "{}"))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            continue
        if not isinstance(result, dict):
            continue
        grouped[request.effect_scope_id].append(
            {
                "request_id": str(row["request_id"]),
                "state": str(row["state"]),
                "finished_at": str(row["finished_at"] or ""),
                "result": result,
            }
        )

    pairs: list[LegacyEffectPair] = []
    for scope_id, items in grouped.items():
        if len(items) != 2:
            continue
        first, second = items
        if first["state"] != "OUTCOME_UNKNOWN" or second["state"] != "EFFECT_OBSERVED":
            continue
        if int(second["result"].get("physical_effect_count") or 0) != 1:
            continue
        pairs.append(
            LegacyEffectPair(
                effect_scope_id=scope_id,
                unknown_request_id=first["request_id"],
                observed_request_id=second["request_id"],
                unknown_finished_at=first["finished_at"],
                observed_finished_at=second["finished_at"],
                unknown_result=first["result"],
                observed_result=second["result"],
            )
        )
    return tuple(sorted(pairs, key=lambda item: item.unknown_finished_at))


def reconcile_legacy_effect_pairs(ledger: EffectLedger) -> dict[str, Any]:
    pairs = find_legacy_effect_pairs(ledger)
    reconciled: list[str] = []
    already_recorded: list[str] = []
    for pair in pairs:
        existing = ledger.connection.execute(
            "SELECT resolution,evidence_digest FROM effect_reconciliations WHERE reconciliation_id=?",
            (pair.reconciliation_id,),
        ).fetchone()
        if existing is not None:
            if str(existing["resolution"]) != "EFFECT_OBSERVED":
                raise RuntimeError("LEGACY_RECONCILIATION_CONFLICT")
            already_recorded.append(pair.effect_scope_id)
            continue
        scope = ledger.scope(pair.effect_scope_id)
        if scope is None:
            raise RuntimeError("LEGACY_RECONCILIATION_SCOPE_MISSING")
        if str(scope["owner_request_id"]) != pair.unknown_request_id:
            raise RuntimeError("LEGACY_RECONCILIATION_OWNER_MISMATCH")
        ledger.record_reconciliation(
            reconciliation_id=pair.reconciliation_id,
            effect_scope_id=pair.effect_scope_id,
            resolution="EFFECT_OBSERVED",
            evidence=pair.evidence(),
        )
        reconciled.append(pair.effect_scope_id)
    return {
        "schema": "runtime.legacy_effect_pair_reconciliation_report.v1",
        "candidate_pair_count": len(pairs),
        "reconciled_count": len(reconciled),
        "already_recorded_count": len(already_recorded),
        "reconciled_scope_ids": reconciled,
        "already_recorded_scope_ids": already_recorded,
        "raw_outcome_unknown_count": ledger.raw_unresolved_count(),
        "unresolved_scope_count": ledger.unresolved_count(),
    }
