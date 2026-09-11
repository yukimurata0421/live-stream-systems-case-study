from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from stream_contracts.monitoring_v4.sli import SLIProjection
from stream_monitoring_v4.runtime.atomic_file import atomic_write_text


PUBLIC_SCHEMA = "monitoring-v4-public-safe-shadow.v1"
PUBLIC_OBJECTIVES = frozenset(
    {
        "youtube_availability",
        "same_url_preservation",
        "upload_ceiling",
        "youtube_input_quality",
        "audio_correctness",
    }
)


def public_safe_projection(
    projections: Iterable[SLIProjection],
    *,
    generated_at: str,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for item in sorted(
        (value for value in projections if value.objective_id in PUBLIC_OBJECTIVES),
        key=lambda value: (value.objective_id, value.assessment_scope, value.window),
    ):
        sli_pct = item.payload.get("sli_pct")
        target_pct = item.payload.get("target_pct")
        items.append(
            {
                "id": item.objective_id,
                "window": item.window,
                "assessment_scope": item.assessment_scope,
                "value_pct": float(sli_pct) if isinstance(sli_pct, (int, float)) else None,
                "reference_pct": float(target_pct) if isinstance(target_pct, (int, float)) else None,
                "coverage_pct": item.coverage_pct,
                "source_freshness_pct": item.source_freshness_pct,
                "evidence_available": not bool(item.measurement_unknown_reasons),
                "evaluated_at": item.evaluated_at,
                "no_automatic_recovery": True,
            }
        )
    return {
        "schema": PUBLIC_SCHEMA,
        "generated_at": generated_at,
        "scope": "isolated_non_public_compatibility_shadow",
        "interpretation": "Measurements only; this artifact is not an incident or runtime control input.",
        "items": items,
    }


def validate_public_safe_projection(payload: dict[str, Any]) -> None:
    if set(payload) != {"schema", "generated_at", "scope", "interpretation", "items"}:
        raise ValueError("public-safe top-level fields changed")
    if payload.get("schema") != PUBLIC_SCHEMA:
        raise ValueError("unsupported public-safe schema")
    if payload.get("scope") != "isolated_non_public_compatibility_shadow":
        raise ValueError("public-safe shadow scope changed")
    items = payload.get("items")
    if not isinstance(items, list):
        raise ValueError("public-safe items must be a list")
    allowed = {
        "id",
        "window",
        "assessment_scope",
        "value_pct",
        "reference_pct",
        "coverage_pct",
        "source_freshness_pct",
        "evidence_available",
        "evaluated_at",
        "no_automatic_recovery",
    }
    for item in items:
        if not isinstance(item, dict) or set(item) != allowed:
            raise ValueError("public-safe item fields changed")
        if item.get("id") not in PUBLIC_OBJECTIVES:
            raise ValueError("public-safe objective is not allowlisted")
        if item.get("no_automatic_recovery") is not True:
            raise ValueError("public-safe artifact gained recovery authority")


def write_public_safe_atomic(path: Path, payload: dict[str, Any]) -> None:
    content = public_safe_bytes(payload)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(
        path,
        content.decode("utf-8"),
        encoding="utf-8",
        mode=0o600,
    )


def public_safe_bytes(payload: dict[str, Any]) -> bytes:
    validate_public_safe_projection(payload)
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode(
        "utf-8"
    )
