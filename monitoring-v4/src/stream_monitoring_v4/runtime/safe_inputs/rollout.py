from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from stream_contracts.monitoring_v4.runtime_evidence import (
    RuntimeRolloutProjection,
    VerifiedRolloutEvidence,
)

from .constants import PLANNED_ROLLOUT_ANNOTATIONS
from .sanitize import mapping, timestamp, token


def runtime_rollout_projection(
    snapshot: Mapping[str, Any],
) -> RuntimeRolloutProjection:
    observed_at = timestamp(snapshot.get("updated_at_utc"))
    pods = snapshot.get("pods") if isinstance(snapshot.get("pods"), list) else []
    evidence: list[VerifiedRolloutEvidence] = []
    for raw_pod in pods:
        pod = mapping(raw_pod)
        if str(pod.get("phase", "")) != "Running":
            continue
        annotations = mapping(pod.get("planned_rollout_annotations"))
        rollout_id = token(annotations.get(PLANNED_ROLLOUT_ANNOTATIONS["id"]))
        planned_at = timestamp(annotations.get(PLANNED_ROLLOUT_ANNOTATIONS["at"]))
        expires_at = timestamp(
            annotations.get(PLANNED_ROLLOUT_ANNOTATIONS["expires"])
        )
        pod_started_at = timestamp(
            pod.get("started_at_utc") or pod.get("created_at_utc")
        )
        pod_uid = token(pod.get("uid"))
        reason = token(annotations.get(PLANNED_ROLLOUT_ANNOTATIONS["reason"]))
        if not all((rollout_id, pod_uid, reason)):
            continue
        try:
            evidence.append(
                VerifiedRolloutEvidence.create(
                    rollout_id=rollout_id,
                    planned_at=planned_at,
                    expires_at=expires_at,
                    pod_started_at=pod_started_at,
                    pod_uid=pod_uid,
                    reason=reason,
                )
            )
        except ValueError:
            # Invalid, expired, or inherited annotations are deliberately not
            # promoted into evidence. The raw source remains outside the core.
            continue
    evidence.sort(key=lambda item: (item.planned_at, item.evidence_id))
    return RuntimeRolloutProjection(
        observed_at=observed_at,
        evidence=tuple(evidence[-8:]),
    )
