from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stream_contracts.monitoring_v4.ids import stable_id
from stream_contracts.monitoring_v4.time import utc_text

from stream_monitoring_v4.adapters.factory import SHADOW_DOMAINS, state_file_adapters
from stream_monitoring_v4.compatibility.legacy_current import (
    normalized_v3_current,
    runtime_rollout_projection,
)
from stream_monitoring_v4.compatibility.live_parity import live_parity_report
from stream_monitoring_v4.compatibility.public_safe import (
    public_safe_bytes,
    public_safe_projection,
)
from stream_monitoring_v4.compatibility.publication import (
    reconcile_public_safe_publication,
)
from stream_monitoring_v4.current.service import CurrentReducerService
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES
from stream_monitoring_v4.incidents.lifecycle import RuntimeLifecycleIncidentService
from stream_monitoring_v4.incidents.policy import DEFAULT_INCIDENT_POLICIES
from stream_monitoring_v4.incidents.service import IncidentService
from stream_monitoring_v4.notifications.routing import RoutePolicy
from stream_monitoring_v4.reliability.projector import (
    ReliabilityProjector,
    projection_integrity,
)
from stream_monitoring_v4.runtime.isolation import validate_isolated_output_path
from stream_monitoring_v4.runtime.observer import Observer
from stream_monitoring_v4.runtime.pipeline import ShadowPipeline, ShadowPipelineResult
from stream_monitoring_v4.storage.publications import PUBLIC_SAFE_ARTIFACT_KEY


@dataclass(frozen=True)
class ShadowCycleRequest:
    state_root: Path
    source_repository: Path
    database: Path | None
    public_shadow_output: Path | None
    build_revision: str
    source_revision: str
    fixed_now_ts: int | None
    summary_only: bool
    youtube_api_state_file: Path | None = None


def _run_pipeline(repository: Any, request: ShadowCycleRequest) -> ShadowPipelineResult:
    route_policy = RoutePolicy()
    pipeline = ShadowPipeline(
        Observer(repository),
        CurrentReducerService(repository, DEFAULT_POLICIES),
        IncidentService(repository, DEFAULT_INCIDENT_POLICIES, route_policy),
        RuntimeLifecycleIncidentService(repository, route_policy),
    )
    return pipeline.run_once(
        state_file_adapters(
            request.state_root,
            youtube_api_state_file=request.youtube_api_state_file,
        ),
        SHADOW_DOMAINS,
        now_ts=request.fixed_now_ts,
        now_at=(
            utc_text(request.fixed_now_ts)
            if request.fixed_now_ts is not None
            else None
        ),
    )


def _output_path(request: ShadowCycleRequest) -> Path:
    if request.public_shadow_output is None and request.database is None:
        raise ValueError("PostgreSQL backend requires --public-shadow-output")
    selected = request.public_shadow_output
    if selected is None:
        selected = request.database.with_name("public-safe-shadow.json")  # type: ignore[union-attr]
    return validate_isolated_output_path(
        selected,
        source_state_root=request.state_root,
        migration_source_repository=request.source_repository,
    )


def _stage_cycle(
    repository: Any,
    *,
    request: ShadowCycleRequest,
    result: ShadowPipelineResult,
    cycle_id: str,
    completed_at: str,
    parity: dict[str, Any],
    public_payload: dict[str, Any] | None,
) -> str:
    staged_publication_id = ""
    with repository.transaction() as connection:
        repository.append_shadow_cycle(
            cycle_id=cycle_id,
            started_at=result.observer.started_at,
            completed_at=completed_at,
            build_revision=request.build_revision,
            source_revision=request.source_revision,
            observer=result.observer.to_dict(),
            current_states={item.domain: item.state for item in result.currents},
            parity=parity,
            notification_intent_count=sum(
                len(item.intents) for item in result.incidents
            ),
            connection=connection,
        )
        if public_payload is not None:
            staged = repository.stage_artifact_publication(
                artifact_key=PUBLIC_SAFE_ARTIFACT_KEY,
                cycle_id=cycle_id,
                created_at=completed_at,
                payload=public_payload,
                connection=connection,
            )
            staged_publication_id = staged.publication_id
    return staged_publication_id


def _full_payload(
    result: ShadowPipelineResult,
    *,
    projection_result: Any,
    parity: dict[str, Any],
    output_path: Path,
    public_shadow_written: bool,
) -> dict[str, Any]:
    payload = result.to_dict()
    payload["reliability_projection"] = projection_result.to_dict()
    payload["parity"] = parity
    payload["public_shadow_output"] = str(output_path)
    payload["public_shadow_written"] = public_shadow_written
    payload["safety_boundary"] = {
        "real_delivery_enabled": False,
        "runtime_mutation_enabled": False,
        "raspberry_pi_dependency": False,
    }
    return payload


def _summary_payload(
    result: ShadowPipelineResult,
    *,
    request: ShadowCycleRequest,
    cycle_id: str,
    completed_at: str,
    projection_result: Any,
    projection_status: dict[str, Any],
    parity: dict[str, Any],
    public_shadow_written: bool,
    safety_boundary: dict[str, bool],
) -> dict[str, Any]:
    return {
        "schema": "monitoring_v4.shadow_cycle_summary.v2",
        "cycle_id": cycle_id,
        "build_revision": request.build_revision,
        "source_revision": request.source_revision,
        "observer": result.observer.to_dict(),
        "decision_at": result.decision_at,
        "completed_at": completed_at,
        "current_states": {item.domain: item.state for item in result.currents},
        "transition_count": sum(
            item.transition is not None for item in result.incidents
        ),
        "notification_intent_count": sum(
            len(item.intents) for item in result.incidents
        ),
        "projection_count": len(projection_result.projections),
        "projection_rejection_count": len(projection_result.rejections),
        "projection_integrity": projection_status,
        "public_shadow_written": public_shadow_written,
        "parity": {
            "equivalent": parity["equivalent"],
            "accepted_difference_count": parity["accepted_difference_count"],
            "unclassified_contract_difference_count": parity[
                "unclassified_contract_difference_count"
            ],
        },
        "safety_boundary": safety_boundary,
    }


def execute_shadow_cycle(repository: Any, request: ShadowCycleRequest) -> dict[str, Any]:
    # Validate every write destination before collecting or persisting a cycle.
    # This boundary was intentionally first in the original command and must
    # remain first after responsibility-level refactors.
    output_path = _output_path(request)
    reconciliation_ts = (
        request.fixed_now_ts
        if request.fixed_now_ts is not None
        else int(time.time())
    )
    try:
        # Repair a previously committed publication intent before doing new
        # source work.  A filesystem outage remains retryable and must not stop
        # monitoring evidence from being recorded; ledger corruption still
        # fails closed via the non-OSError path.
        reconcile_public_safe_publication(
            repository,
            output_path,
            now_ts=reconciliation_ts,
        )
    except OSError:
        pass
    result = _run_pipeline(repository, request)
    projection_result = ReliabilityProjector(
        repository,
        formal_status_file=request.state_root / "operational_reliability_status.json",
        burn_status_file=(
            request.state_root / "operational_reliability_burn_status.json"
        ),
    ).run_once(
        received_at=(
            utc_text(request.fixed_now_ts)
            if request.fixed_now_ts is not None
            else None
        )
    )
    parity = live_parity_report(
        result.currents,
        normalized_v3_current(request.state_root),
        runtime_rollout_projection(request.state_root),
    )
    completed_ts = (
        request.fixed_now_ts
        if request.fixed_now_ts is not None
        else int(time.time())
    )
    completed_at = utc_text(completed_ts)
    projection_status = projection_integrity(
        projection_result.projections,
        projection_result.rejections,
    )
    parity["projection_integrity"] = projection_status
    public_payload = None
    if projection_status["complete"]:
        public_payload = public_safe_projection(
            projection_result.projections,
            generated_at=completed_at,
        )
        public_safe_bytes(public_payload)
    cycle_id = stable_id(
        "cyc",
        result.observer.started_at,
        request.build_revision,
        request.source_revision,
        [item.snapshot_id for item in result.currents],
    )
    staged_id = _stage_cycle(
        repository,
        request=request,
        result=result,
        cycle_id=cycle_id,
        completed_at=completed_at,
        parity=parity,
        public_payload=public_payload,
    )
    publication = reconcile_public_safe_publication(
        repository,
        output_path,
        now_ts=completed_ts,
    )
    public_shadow_written = bool(
        publication is not None
        and publication.changed
        and publication.publication_id == staged_id
    )
    payload = _full_payload(
        result,
        projection_result=projection_result,
        parity=parity,
        output_path=output_path,
        public_shadow_written=public_shadow_written,
    )
    if request.summary_only:
        return _summary_payload(
            result,
            request=request,
            cycle_id=cycle_id,
            completed_at=completed_at,
            projection_result=projection_result,
            projection_status=projection_status,
            parity=parity,
            public_shadow_written=public_shadow_written,
            safety_boundary=payload["safety_boundary"],
        )
    return payload
