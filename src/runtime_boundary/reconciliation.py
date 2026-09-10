from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any

from cra_dell_recovery.models import TargetIdentity, is_expected_ffmpeg_successor

from .ledger import EffectLedger


class EffectOutcomeReconciler:
    """Observe a prior uncertain attempt; never invokes or retries an effect."""

    def __init__(
        self,
        ledger: EffectLedger,
        observe_target: Callable[[], Mapping[str, Any] | None],
        *,
        monotonic: Callable[[], float] = time.monotonic,
        wait: Callable[[float], None] = time.sleep,
    ) -> None:
        self.ledger = ledger
        self.observe_target = observe_target
        self.monotonic = monotonic
        self.wait = wait

    def reconcile(
        self,
        *,
        reconciliation_id: str,
        effect_scope_id: str,
        before_target: TargetIdentity,
        deadline_seconds: float,
        poll_interval_seconds: float = 1.0,
    ) -> str:
        if not 0 < deadline_seconds <= 300:
            raise ValueError("deadline_seconds must be in (0, 300]")
        if not 0 < poll_interval_seconds <= deadline_seconds:
            raise ValueError("poll_interval_seconds must be in (0, deadline]")
        scope = self.ledger.scope(effect_scope_id)
        if scope is None or int(scope["physical_attempt_count"]) != 1:
            raise ValueError("reconciliation requires one reserved physical attempt")
        deadline = self.monotonic() + deadline_seconds
        samples = 0
        while self.monotonic() <= deadline:
            samples += 1
            raw = self.observe_target()
            if raw is not None:
                try:
                    observed = TargetIdentity.from_dict(dict(raw))
                except (TypeError, ValueError):
                    observed = None
                if observed is not None and is_expected_ffmpeg_successor(before_target, observed):
                    self.ledger.record_reconciliation(
                        reconciliation_id=reconciliation_id,
                        effect_scope_id=effect_scope_id,
                        resolution="EFFECT_OBSERVED",
                        evidence={
                            "physical_effect_count": 1,
                            "sample_count": samples,
                            "before_target": before_target.to_dict(),
                            "observed_target": observed.to_dict(),
                            "automatic_retry_count": 0,
                        },
                    )
                    return "EFFECT_OBSERVED"
            if self.monotonic() >= deadline:
                break
            self.wait(min(poll_interval_seconds, deadline - self.monotonic()))
        self.ledger.record_reconciliation(
            reconciliation_id=reconciliation_id,
            effect_scope_id=effect_scope_id,
            resolution="REMAINS_UNKNOWN",
            evidence={
                "physical_effect_count": 1,
                "sample_count": samples,
                "before_target": before_target.to_dict(),
                "automatic_retry_count": 0,
            },
        )
        return "OUTCOME_UNKNOWN"
