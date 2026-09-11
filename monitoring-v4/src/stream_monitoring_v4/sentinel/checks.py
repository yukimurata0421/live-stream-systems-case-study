from __future__ import annotations

from collections.abc import Mapping

from .facts import SentinelFacts
from .models import SentinelEvidence


def evaluate_checks(
    evidence: SentinelEvidence,
    facts: SentinelFacts,
) -> Mapping[str, bool]:
    """Evaluate the ordered, fail-closed sentinel policy checks."""

    return {
        "k3s_active": evidence.k3s_active,
        "pods_readable": evidence.pods_ok,
        "release_readable": evidence.release_ok,
        "required_components_ready": all(facts.component_ready.values()),
        "no_unexpected_active_pods": not facts.unexpected_active_pods,
        "no_active_mutating_auxiliary_pods": (
            not facts.active_mutating_auxiliary_pods
        ),
        "auxiliary_image_identity_matches_release": (
            facts.auxiliary_image_identity_matches_release
        ),
        "app_images_match_release": facts.app_images_match_release,
        "app_image_ids_match_release": facts.app_image_ids_match_release,
        "database_images_match_release": facts.database_images_match_release,
        "database_image_ids_match_release": (
            facts.database_image_ids_match_release
        ),
        "youtube_api_evidence_readable": (
            evidence.youtube_api_evidence.get("readable") is True
        ),
        "youtube_api_evidence_schema_supported": (
            evidence.youtube_api_evidence.get("schema_supported") is True
        ),
        "youtube_api_evidence_permissions_safe": (
            evidence.youtube_api_evidence.get("permissions_safe") is True
        ),
        "youtube_api_evidence_fresh": (
            evidence.youtube_api_evidence.get("fresh") is True
        ),
        "youtube_api_probe_ok": (
            evidence.youtube_api_evidence.get("probe_ok") is True
        ),
        "continuity_baseline_present": facts.continuity_baseline_present,
        "pod_restart_total_zero": facts.pod_restart_total == 0,
        "pod_restart_delta_zero": facts.restart_delta == 0,
        "pod_replacement_count_zero": facts.pod_replacement_count == 0,
        "backup_fresh": evidence.backup.get("fresh") is True,
        "independent_backup_fresh": (
            evidence.independent_backup.get("fresh") is True
        ),
        "backup_copies_match": facts.backup_copies_match,
        "restore_verification_acceptable": (
            evidence.restore_verification.get("acceptable") is True
        ),
        "database_filesystem_sufficient": (
            evidence.database_filesystem.get("sufficient") is True
        ),
        "backup_filesystem_sufficient": (
            evidence.backup_filesystem.get("sufficient") is True
        ),
        "independent_backup_filesystem_sufficient": (
            evidence.independent_backup_filesystem.get("sufficient") is True
        ),
        "backup_filesystems_independent": facts.backup_filesystems_independent,
        "report_schema_supported": (
            evidence.report.get("schema_supported") is True
        ),
        "report_fresh": evidence.report.get("fresh") is True,
        "report_build_revision_immutable": (
            evidence.report.get("build_revision_immutable") is True
        ),
        "report_source_revision_known": (
            evidence.report.get("source_revision_known") is True
        ),
        "report_build_matches_release": facts.report_build_matches_release,
        "report_parity_clean": evidence.report.get("parity_clean") is True,
        "report_projection_clean": (
            evidence.report.get("projection_clean") is True
        ),
        "report_latest_cycle_clean": (
            evidence.report.get("latest_cycle_clean") is True
        ),
        "real_delivery_disabled": (
            evidence.report.get("unsafe_real_delivery") is False
        ),
        "runtime_mutation_disabled": (
            evidence.report.get("unsafe_runtime_mutation") is False
        ),
        "raspberry_pi_dependency_disabled": (
            evidence.report.get("unsafe_raspberry_pi_dependency") is False
        ),
    }
