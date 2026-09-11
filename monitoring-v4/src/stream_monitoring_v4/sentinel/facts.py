from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from stream_monitoring_v4.runtime.backup_integrity import matching_backup_copies

from .continuity import previous_continuity
from .contracts import (
    ALLOWED_AUXILIARY_COMPONENTS,
    APP_COMPONENTS,
    MUTATING_AUXILIARY_COMPONENTS,
    POSTGRES_IMAGE,
    REQUIRED_COMPONENT_COUNTS,
    SHA256_PATTERN,
)
from .models import SentinelEvidence
from .storage import filesystems_independent


@dataclass(frozen=True)
class SentinelFacts:
    release_revision: str
    expected_app_image_id: str
    component_running_count: dict[str, int]
    component_ready: dict[str, bool]
    expected_app_image: str
    running_app_images: list[str]
    app_images_match_release: bool
    running_app_image_ids: list[str]
    app_image_ids_complete: bool
    app_image_ids_match_release: bool
    expected_database_image_id: str
    running_database_images: list[str]
    database_images_match_release: bool
    running_database_image_ids: list[str]
    database_image_ids_match_release: bool
    unexpected_active_pods: list[str]
    active_mutating_auxiliary_pods: list[str]
    auxiliary_image_identity_matches_release: bool
    pod_runtime: dict[str, dict[str, Any]]
    continuity_baseline_present: bool
    restart_delta: int
    pod_replacement_count: int
    pod_restart_total: int
    report_build_matches_release: bool
    backup_copies_match: bool
    backup_filesystems_independent: bool


@dataclass(frozen=True)
class _ImageFacts:
    expected_image: str
    expected_image_id: str
    running_images: list[str]
    images_match: bool
    running_image_ids: list[str]
    image_ids_complete: bool
    image_ids_match: bool


@dataclass(frozen=True)
class _PodInventory:
    unexpected_active: list[str]
    mutating_auxiliary: list[str]
    runtime: dict[str, dict[str, Any]]


@dataclass(frozen=True)
class _ContinuityFacts:
    baseline_present: bool
    restart_delta: int
    replacement_count: int
    restart_total: int


def _image_ids(values: list[str]) -> tuple[list[str], bool]:
    normalized = sorted(
        match.group(0)
        for value in values
        for match in [SHA256_PATTERN.search(value)]
        if match is not None
    )
    return normalized, bool(values) and len(normalized) == len(values)


def _component_facts(
    pods: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, int], dict[str, bool]]:
    counts = {
        component: sum(
            value["component"] == component
            and value["phase"] == "Running"
            and value["ready"]
            for value in pods.values()
        )
        for component in REQUIRED_COMPONENT_COUNTS
    }
    ready = {
        component: counts[component] == required
        for component, required in REQUIRED_COMPONENT_COUNTS.items()
    }
    return counts, ready


def _application_images(evidence: SentinelEvidence, revision: str) -> _ImageFacts:
    expected_image = f"stream-monitoring-v4:{revision}" if evidence.release_ok else ""
    running_images = sorted(
        image
        for value in evidence.pods.values()
        if value["component"] in APP_COMPONENTS
        and value["phase"] == "Running"
        and value["ready"]
        for image in value["images"]
    )
    raw_ids = [
        image_id
        for value in evidence.pods.values()
        if value["component"] in APP_COMPONENTS
        and value["phase"] == "Running"
        and value["ready"]
        for image_id in value["image_ids"]
    ]
    image_ids, complete = _image_ids(raw_ids)
    expected_id = evidence.release_identity.get("app_image_id", "")
    return _ImageFacts(
        expected_image=expected_image,
        expected_image_id=expected_id,
        running_images=running_images,
        images_match=(
            bool(expected_image)
            and bool(running_images)
            and all(image == expected_image for image in running_images)
        ),
        running_image_ids=image_ids,
        image_ids_complete=complete,
        image_ids_match=(
            complete
            and bool(expected_id)
            and all(image_id == expected_id for image_id in image_ids)
        ),
    )


def _database_images(evidence: SentinelEvidence) -> _ImageFacts:
    expected_digest = SHA256_PATTERN.search(POSTGRES_IMAGE)
    expected_id = expected_digest.group(0) if expected_digest is not None else ""
    running_images = sorted(
        image
        for value in evidence.pods.values()
        if value["component"] == "database"
        and value["phase"] == "Running"
        and value["ready"]
        for image in value["images"]
    )
    raw_ids = [
        image_id
        for value in evidence.pods.values()
        if value["component"] == "database"
        and value["phase"] == "Running"
        and value["ready"]
        for image_id in value["image_ids"]
    ]
    image_ids, complete = _image_ids(raw_ids)
    return _ImageFacts(
        expected_image=POSTGRES_IMAGE,
        expected_image_id=expected_id,
        running_images=running_images,
        images_match=running_images == [POSTGRES_IMAGE],
        running_image_ids=image_ids,
        image_ids_complete=complete,
        image_ids_match=(len(raw_ids) == 1 and image_ids == [expected_id]),
    )


def _pod_inventory(
    pods: Mapping[str, Mapping[str, Any]],
) -> _PodInventory:
    allowed = set(REQUIRED_COMPONENT_COUNTS) | set(ALLOWED_AUXILIARY_COMPONENTS)
    unexpected = sorted(
        name
        for name, value in pods.items()
        if value["phase"] not in {"Succeeded", "Failed"}
        and value["component"] not in allowed
    )
    mutating = sorted(
        name
        for name, value in pods.items()
        if value["phase"] not in {"Succeeded", "Failed"}
        and value["component"] in MUTATING_AUXILIARY_COMPONENTS
    )
    runtime = {
        name: {
            "uid": value["uid"],
            "component": value["component"],
            "restart_count": int(value["restart_count"]),
            "image_ids": value["image_ids"],
        }
        for name, value in pods.items()
        if value["component"] in REQUIRED_COMPONENT_COUNTS
    }
    return _PodInventory(unexpected, mutating, runtime)


def _auxiliary_images_match(
    evidence: SentinelEvidence,
    *,
    revision: str,
) -> bool:
    expected_app_image = f"stream-monitoring-v4:{revision}"
    expected_app_digest = evidence.release_identity.get("app_image_id", "")
    postgres_digest_match = SHA256_PATTERN.search(POSTGRES_IMAGE)
    expected_postgres_digest = (
        postgres_digest_match.group(0) if postgres_digest_match is not None else ""
    )
    for value in evidence.pods.values():
        component = value["component"]
        if (
            value["phase"] in {"Succeeded", "Failed"}
            or component not in ALLOWED_AUXILIARY_COMPONENTS
        ):
            continue
        postgres_workload = component in {"backup", "verification"}
        expected_image = POSTGRES_IMAGE if postgres_workload else expected_app_image
        expected_digest = (
            expected_postgres_digest if postgres_workload else expected_app_digest
        )
        raw_image_ids = list(value["image_ids"])
        image_ids, complete = _image_ids(raw_image_ids)
        if (
            not value["images"]
            or any(image != expected_image for image in value["images"])
            or not expected_digest
        ):
            return False
        # Before a Job reaches its main container Kubernetes may not expose a
        # container imageID yet. The digest-pinned spec must already match; once
        # any ID is available it must be complete and exact.
        if raw_image_ids and (
            not complete or any(image_id != expected_digest for image_id in image_ids)
        ):
            return False
        if value["ready"] and not complete:
            return False
    return True


def _continuity_facts(
    evidence: SentinelEvidence,
    *,
    revision: str,
    runtime: Mapping[str, Mapping[str, Any]],
) -> _ContinuityFacts:
    previous_runtime, previous_revision, previous_complete = previous_continuity(
        evidence.previous
    )
    baseline = (
        previous_complete
        and evidence.release_ok
        and previous_revision == revision
    )
    previous_by_uid = {value["uid"]: value for value in previous_runtime.values()}
    restart_delta = sum(
        max(
            0,
            int(value["restart_count"])
            - previous_by_uid.get(str(value["uid"]), {}).get(
                "restart_count",
                value["restart_count"],
            ),
        )
        for value in runtime.values()
    )
    previous_component_uids: dict[str, set[str]] = {}
    for value in previous_runtime.values():
        previous_component_uids.setdefault(value["component"], set()).add(
            value["uid"]
        )
    replacements = (
        sum(
            str(value["uid"])
            not in previous_component_uids.get(str(value["component"]), set())
            for value in runtime.values()
        )
        if baseline
        else 0
    )
    return _ContinuityFacts(
        baseline_present=baseline,
        restart_delta=restart_delta,
        replacement_count=replacements,
        restart_total=sum(int(item["restart_count"]) for item in runtime.values()),
    )


def derive_facts(evidence: SentinelEvidence) -> SentinelFacts:
    revision = evidence.release_identity.get("revision", "")
    counts, ready = _component_facts(evidence.pods)
    application = _application_images(evidence, revision)
    database = _database_images(evidence)
    inventory = _pod_inventory(evidence.pods)
    continuity = _continuity_facts(
        evidence,
        revision=revision,
        runtime=inventory.runtime,
    )
    return SentinelFacts(
        release_revision=revision,
        expected_app_image_id=application.expected_image_id,
        component_running_count=counts,
        component_ready=ready,
        expected_app_image=application.expected_image,
        running_app_images=application.running_images,
        app_images_match_release=application.images_match,
        running_app_image_ids=application.running_image_ids,
        app_image_ids_complete=application.image_ids_complete,
        app_image_ids_match_release=application.image_ids_match,
        expected_database_image_id=database.expected_image_id,
        running_database_images=database.running_images,
        database_images_match_release=database.images_match,
        running_database_image_ids=database.running_image_ids,
        database_image_ids_match_release=database.image_ids_match,
        unexpected_active_pods=inventory.unexpected_active,
        active_mutating_auxiliary_pods=inventory.mutating_auxiliary,
        auxiliary_image_identity_matches_release=_auxiliary_images_match(
            evidence,
            revision=revision,
        ),
        pod_runtime=inventory.runtime,
        continuity_baseline_present=continuity.baseline_present,
        restart_delta=continuity.restart_delta,
        pod_replacement_count=continuity.replacement_count,
        pod_restart_total=continuity.restart_total,
        report_build_matches_release=(
            evidence.release_ok
            and evidence.report.get("build_revision") == revision
        ),
        backup_copies_match=matching_backup_copies(
            dict(evidence.backup),
            dict(evidence.independent_backup),
        ),
        backup_filesystems_independent=filesystems_independent(
            evidence.backup_filesystem,
            evidence.independent_backup_filesystem,
        ),
    )
