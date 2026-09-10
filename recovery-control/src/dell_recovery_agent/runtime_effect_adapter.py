from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any, Protocol

from cra_dell_recovery.effect_scope import effect_scope_id
from cra_dell_recovery.models import TargetIdentity, is_expected_ffmpeg_successor
from cra_dell_recovery.time import isoformat_utc, utc_now
from dell_recovery_agent.execution import FakeExecutionResult


class RuntimeEffectClient(Protocol):
    def execute(self, payload: dict[str, object]) -> dict[str, Any]: ...


class RuntimeEffectAdapter:
    """The only Dell Agent bridge to the existing Option B+D executor.

    It never sends a signal itself.  A stable effect-scope-derived request ID is
    used so retries and independently-created candidates converge again at the
    EffectLedger boundary.
    """

    def __init__(
        self,
        client: RuntimeEffectClient,
        *,
        producer_id: str,
        producer_generation: int,
        executor_instance_id: str,
        observe_after: Callable[[], TargetIdentity | None],
        projection: Callable[[], tuple[str, int, str]],
        request_lifetime_seconds: float = 5.0,
    ) -> None:
        if producer_generation <= 0:
            raise ValueError("producer_generation must be positive")
        if not 0 < request_lifetime_seconds <= 5.0:
            raise ValueError("request_lifetime_seconds must be in (0, 5]")
        self.client = client
        self.producer_id = producer_id
        self.producer_generation = producer_generation
        self.executor_instance_id = executor_instance_id
        self.observe_after = observe_after
        self.projection = projection
        self.request_lifetime_seconds = request_lifetime_seconds

    def restart_ffmpeg(self, expected_target: TargetIdentity) -> FakeExecutionResult:
        scope_id = effect_scope_id("restart_ffmpeg", expected_target)
        projection_id, projection_sequence, evidence_status = self.projection()
        now = utc_now()
        request_id = f"dell-scope-{scope_id}"
        payload: dict[str, object] = {
            "schema_version": "runtime.typed_effect_request.v2",
            "request_id": request_id,
            "producer_id": self.producer_id,
            "producer_generation": self.producer_generation,
            "intent_type": "RESTART_FFMPEG",
            "reason": "CRA or Dell local candidate passed durable target-wide admission",
            "failure_domain": "FFMPEG_LOCAL",
            "issued_at": isoformat_utc(now),
            "expires_at": isoformat_utc(now + timedelta(seconds=self.request_lifetime_seconds)),
            "ffmpeg_target_identity": expected_target.to_dict(),
            "runtime_identity": None,
            "expected_ffmpeg_generation": expected_target.ffmpeg_generation,
            "idempotency_key": request_id,
            "correlation_id": request_id,
            "target_snapshot_id": f"dell-target-{scope_id}",
            "runtime_snapshot_id": "",
            "runtime_observation_id": f"dell-observation-{scope_id}",
            "expected_executor_instance_id": self.executor_instance_id,
            "maintenance_evidence_status": evidence_status,
            "projection_id": projection_id,
            "projection_sequence": projection_sequence,
        }
        response = self.client.execute(payload)
        state = str(response.get("state") or "OUTCOME_UNKNOWN")
        if state == "OUTCOME_UNKNOWN":
            raise RuntimeError("OPTION_BD_OUTCOME_UNKNOWN")
        if response.get("ok") is not True or state != "EFFECT_OBSERVED":
            return FakeExecutionResult(False, expected_target, str(response.get("reason") or "OPTION_BD_EFFECT_REJECTED"))
        after = self.observe_after()
        if after is None:
            raise RuntimeError("POST_EFFECT_TARGET_UNAVAILABLE")
        if not is_expected_ffmpeg_successor(expected_target, after):
            raise RuntimeError("POST_EFFECT_TARGET_RELATION_UNSAFE")
        return FakeExecutionResult(True, after, "OPTION_BD_EFFECT_OBSERVED")
