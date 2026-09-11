from __future__ import annotations

from typing import Any

from stream_contracts.monitoring_v4.time import utc_text

from .contracts import POSTGRES_IMAGE, REQUIRED_COMPONENT_COUNTS, SENTINEL_SCHEMA
from .facts import SentinelFacts
from .models import SentinelEvidence


def build_payload(
    evidence: SentinelEvidence,
    facts: SentinelFacts,
    *,
    healthy: bool,
) -> dict[str, Any]:
    """Build the stable public sentinel status payload."""

    return {
        "schema": SENTINEL_SCHEMA,
        "checked_at": utc_text(evidence.now_ts),
        "status": "good" if healthy else "bad",
        "detection_only": True,
        "automatic_k3s_restart_enabled": False,
        "automatic_runtime_mutation_enabled": False,
        "k3s": {
            "active": evidence.k3s_active,
            "detail": evidence.k3s_detail.strip()[:200],
        },
        "pods_readable": evidence.pods_ok,
        "pods_error": evidence.pods_error,
        "release": {
            "readable": evidence.release_ok,
            "revision": facts.release_revision,
            "error": evidence.release_error,
            "expected_app_image": facts.expected_app_image,
            "expected_app_image_id": facts.expected_app_image_id,
            "running_app_images": facts.running_app_images,
            "app_images_match_release": facts.app_images_match_release,
            "running_app_image_ids": facts.running_app_image_ids,
            "app_image_ids_complete": facts.app_image_ids_complete,
            "app_image_ids_match_release": facts.app_image_ids_match_release,
            "expected_database_image": POSTGRES_IMAGE,
            "expected_database_image_id": facts.expected_database_image_id,
            "running_database_images": facts.running_database_images,
            "database_images_match_release": facts.database_images_match_release,
            "running_database_image_ids": facts.running_database_image_ids,
            "database_image_ids_match_release": (
                facts.database_image_ids_match_release
            ),
            "report_build_matches_release": facts.report_build_matches_release,
        },
        "component_ready": facts.component_ready,
        "component_running_count": facts.component_running_count,
        "component_required_count": REQUIRED_COMPONENT_COUNTS,
        "pod_restart_total": facts.pod_restart_total,
        "pod_restart_delta": facts.restart_delta,
        "pod_replacement_count": facts.pod_replacement_count,
        "continuity_baseline_present": facts.continuity_baseline_present,
        "unexpected_active_pods": facts.unexpected_active_pods,
        "active_mutating_auxiliary_pods": facts.active_mutating_auxiliary_pods,
        "auxiliary_image_identity_matches_release": (
            facts.auxiliary_image_identity_matches_release
        ),
        "pod_runtime": facts.pod_runtime,
        "youtube_api_evidence": dict(evidence.youtube_api_evidence),
        "backup": dict(evidence.backup),
        "independent_backup": dict(evidence.independent_backup),
        "backup_copies_match": facts.backup_copies_match,
        "restore_verification": dict(evidence.restore_verification),
        "filesystem": {
            "database": dict(evidence.database_filesystem),
            "backup": dict(evidence.backup_filesystem),
            "independent_backup": dict(evidence.independent_backup_filesystem),
            "backup_filesystems_independent": (
                facts.backup_filesystems_independent
            ),
            "minimum_free_bytes": evidence.minimum_free_bytes,
            "minimum_free_percent": evidence.minimum_free_percent,
            "minimum_free_inode_percent": evidence.minimum_free_inode_percent,
        },
        "report": dict(evidence.report),
    }
