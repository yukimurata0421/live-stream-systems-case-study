from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from stream_contracts.monitoring_v4.time import unix_ts
from stream_contracts.monitoring_v4.youtube_api import YouTubeApiEvidence
from stream_monitoring_v4.adapters.json_file import SnapshotReadError, read_json_snapshot
from stream_monitoring_v4.runtime.backup_integrity import (
    verified_backup_status,
    verified_restore_status,
)

from .contracts import REPORT_SCHEMA, integer, mapping, text


def youtube_api_status(
    path: Path,
    *,
    now_ts: int,
    max_age_sec: int,
) -> dict[str, Any]:
    try:
        snapshot = read_json_snapshot(path, max_bytes=64 * 1024)
        evidence = YouTubeApiEvidence.from_dict(snapshot.payload)
        collected_ts = unix_ts(evidence.collected_at)
        age = now_ts - collected_ts
        mode = snapshot.mode
        return {
            "readable": True,
            "schema": str(snapshot.payload.get("schema", "")),
            "schema_supported": (
                snapshot.payload.get("schema") == YouTubeApiEvidence.SCHEMA
            ),
            "collected_at": evidence.collected_at,
            "age_sec": age,
            "future": age < 0,
            "fresh": 0 <= age <= max_age_sec,
            "permissions_safe": mode == 0o640,
            "mode": f"{mode:04o}",
            "probe_ok": evidence.probe_status == "ok",
            "result_kind": evidence.result_kind,
            "collector_revision": evidence.collector_revision,
            "lifecycle_status": evidence.lifecycle_status,
            "stream_status": evidence.stream_status,
            "stream_health_status": evidence.stream_health_status,
            "configuration_issue_count": len(evidence.configuration_issues),
            "api_request_count": evidence.api_request_count,
            "oauth_refresh_performed": evidence.oauth_refresh_performed,
            "oauth_scope_class": evidence.oauth_scope_class,
            "oauth_scope_count": evidence.oauth_scope_count,
        }
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        OverflowError,
        json.JSONDecodeError,
        SnapshotReadError,
    ) as exc:
        return {
            "readable": False,
            "schema_supported": False,
            "fresh": False,
            "permissions_safe": False,
            "probe_ok": False,
            "error": type(exc).__name__,
        }


def report_status(path: Path, *, now_ts: int, max_age_sec: int) -> dict[str, Any]:
    try:
        payload = read_json_snapshot(path, max_bytes=2 * 1024 * 1024).payload
        generated_at = text(payload, "generated_at")
        generated_ts = unix_ts(generated_at)
        age = now_ts - generated_ts
        parity = mapping(payload.get("parity_14d"))
        projection = mapping(payload.get("projection_integrity_14d"))
        latest = mapping(payload.get("latest_cycle"))
        safety_boundary = mapping(payload.get("safety_boundary"))
        parity_violations = integer(parity, "unclassified_contract_difference_count")
        parity_invalid = integer(parity, "invalid_payload_count")
        raw_rollout_conflicts = parity.get("conflicting_rollout_evidence_count", 0)
        if type(raw_rollout_conflicts) is not int:
            raise ValueError("conflicting_rollout_evidence_count must be an integer")
        parity_rollout_conflicts = raw_rollout_conflicts
        projection_failures = integer(projection, "failed_cycles")
        latest_clean = (
            latest.get("parity_payload_valid") is True
            and integer(latest, "parity_unclassified_contract_difference_count") == 0
            and latest.get("projection_integrity_complete") is True
            and integer(latest, "projection_rejection_count") == 0
            and integer(latest, "projection_count")
            == integer(latest, "projection_expected_count")
        )
        return {
            "readable": True,
            "schema": str(payload.get("schema", "")),
            "schema_supported": payload.get("schema") == REPORT_SCHEMA,
            "generated_at": generated_at,
            "age_sec": age,
            "future": age < 0,
            "fresh": 0 <= age <= max_age_sec,
            "build_revision": text(payload, "build_revision"),
            "build_revision_immutable": payload.get("build_revision_immutable") is True,
            "source_revision_known": payload.get("source_revision_known") is True,
            "unsafe_real_delivery": (
                safety_boundary.get("real_delivery_enabled") is not False
            ),
            "unsafe_runtime_mutation": (
                safety_boundary.get("runtime_mutation_enabled") is not False
            ),
            "unsafe_raspberry_pi_dependency": (
                safety_boundary.get("raspberry_pi_dependency") is not False
            ),
            "parity_violation_count": parity_violations,
            "parity_invalid_payload_count": parity_invalid,
            "parity_conflicting_rollout_evidence_count": parity_rollout_conflicts,
            "parity_clean": (
                parity_violations == 0
                and parity_invalid == 0
                and parity_rollout_conflicts == 0
            ),
            "projection_failed_cycle_count": projection_failures,
            "projection_clean": projection_failures == 0,
            "latest_cycle_clean": latest_clean,
        }
    except (
        OSError,
        KeyError,
        TypeError,
        ValueError,
        OverflowError,
        json.JSONDecodeError,
        SnapshotReadError,
    ) as exc:
        return {"readable": False, "fresh": False, "error": type(exc).__name__}


def backup_status(path: Path, *, now_ts: int, max_age_sec: int) -> dict[str, Any]:
    return verified_backup_status(path, now_ts=now_ts, max_age_sec=max_age_sec)


def restore_verification_status(
    path: Path,
    *,
    backup: dict[str, Any],
    now_ts: int,
    max_age_sec: int,
    pending_grace_sec: int,
) -> dict[str, Any]:
    return verified_restore_status(
        path,
        backup=backup,
        now_ts=now_ts,
        max_age_sec=max_age_sec,
        pending_grace_sec=pending_grace_sec,
    )
