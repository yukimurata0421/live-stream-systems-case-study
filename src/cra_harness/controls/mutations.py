from __future__ import annotations

from typing import Any


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

    def inject(self, scenario_id: str) -> dict[str, Any]:
        try:
            return dict(self._OBSERVATIONS[scenario_id])
        except KeyError as error:
            raise ValueError(f"unsupported negative control: {scenario_id}") from error
