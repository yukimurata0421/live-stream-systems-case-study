from __future__ import annotations

from collections.abc import Callable
from typing import Any

from cra_harness.injectors.faults import FaultKind


class BrokenMutationAdapter:
    """Test-only violations used to prove that the independent oracle can fail."""

    _OBSERVATIONS: dict[str, dict[str, Any]] = {
        "NC-01": {"physical_count": 2},
        "NC-02": {"stale_target_effect": True},
        "NC-03": {"effect_before_durable_accept": True},
        "NC-04": {"dual_authority_active": True},
        "NC-05": {"outcome_unknown_retry_count": 1, "physical_count": 2},
        "NC-06": {"restored_pending_replay_count": 1},
    }

    _FAULT_KINDS = {
        "NC-01": FaultKind.MUTATE_DUPLICATE_EFFECT,
        "NC-02": FaultKind.MUTATE_STALE_TARGET_ACCEPTED,
        "NC-03": FaultKind.MUTATE_EFFECT_BEFORE_DURABLE_ACCEPT,
        "NC-04": FaultKind.MUTATE_DUAL_AUTHORITY,
        "NC-05": FaultKind.MUTATE_UNKNOWN_RETRY,
        "NC-06": FaultKind.MUTATE_RESTORED_PENDING_REPLAY,
    }

    def inject(self, scenario_id: str, *, receipt: Callable[[FaultKind, str], None] | None = None) -> dict[str, Any]:
        try:
            result = dict(self._OBSERVATIONS[scenario_id])
            kind = self._FAULT_KINDS[scenario_id]
        except KeyError as error:
            raise ValueError(f"unsupported negative control: {scenario_id}") from error
        if receipt is not None:
            receipt(kind, "BrokenMutationAdapter.inject")
        return result
