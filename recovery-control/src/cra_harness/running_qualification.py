"""Fail-closed comparison of separately captured running and tested identities.

This is an evidence comparator, not a live collector. Callers must establish
physical host, process/release identity and capture freshness independently.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

_SHA = re.compile(r"^[0-9a-f]{64}$")
_SOURCES = {"arena_oauth": "arena-monitoring-facts", "arena_network": "arena-monitoring-facts",
            "arena_control_path": "arena-monitoring-facts"}
_SQLITE = {"cra_core": "cra-01-central-authority", "dell_agent": "dell-stream-runtime",
           "dell_owner": "dell-stream-runtime"}


def compare_running_qualification(
    running: Mapping[str, Any], qualified: Mapping[str, Any]
) -> dict[str, Any]:
    """Require component-local source and SQLite parity; never compare versions across components."""
    errors: list[str] = []
    for component, host_id in _SOURCES.items():
        observed = running.get("sources", {}).get(component) if isinstance(running.get("sources"), dict) else None
        tested = qualified.get("sources", {}).get(component) if isinstance(qualified.get("sources"), dict) else None
        if not isinstance(observed, dict) or not isinstance(tested, dict):
            errors.append(f"SOURCE_QUALIFICATION_MISSING:{component}")
            continue
        if observed.get("host_id") != host_id or not observed.get("running_process_identity"):
            errors.append(f"RUNNING_SOURCE_IDENTITY_UNBOUND:{component}")
        if component == "arena_oauth" and (
            observed.get("source_resolution") != "process_mount_namespace"
            or observed.get("bind_read_only_paths_checked") is not True
        ):
            errors.append("RUNNING_SOURCE_NAMESPACE_UNVERIFIED:arena_oauth")
        digest = observed.get("source_sha256")
        if not isinstance(digest, str) or not _SHA.fullmatch(digest) or digest != tested.get("source_sha256"):
            errors.append(f"RUNNING_SOURCE_NOT_QUALIFIED:{component}")
        test_digest = tested.get("test_artifact_sha256")
        if tested.get("test_status") != "PASS" or not isinstance(test_digest, str) or not _SHA.fullmatch(test_digest):
            errors.append(f"SOURCE_TEST_NOT_BOUND:{component}")
    for component, host_id in _SQLITE.items():
        observed = running.get("sqlite", {}).get(component) if isinstance(running.get("sqlite"), dict) else None
        tested = qualified.get("sqlite", {}).get(component) if isinstance(qualified.get("sqlite"), dict) else None
        if not isinstance(observed, dict) or not isinstance(tested, dict):
            errors.append(f"SQLITE_QUALIFICATION_MISSING:{component}")
            continue
        if observed.get("host_id") != host_id or not observed.get("running_process_identity"):
            errors.append(f"RUNNING_SQLITE_IDENTITY_UNBOUND:{component}")
        version = observed.get("version")
        digest = observed.get("library_sha256")
        if (
            not isinstance(version, str) or not isinstance(digest, str) or not _SHA.fullmatch(digest)
            or version != tested.get("version") or digest != tested.get("library_sha256")
        ):
            errors.append(f"RUNNING_SQLITE_NOT_QUALIFIED:{component}")
        test_digest = tested.get("test_artifact_sha256")
        if tested.get("test_status") != "PASS" or not isinstance(test_digest, str) or not _SHA.fullmatch(test_digest):
            errors.append(f"SQLITE_TEST_NOT_BOUND:{component}")
    return {
        "schema": "cra.running_qualification_comparison.v1",
        "status": "PASS" if not errors else "HOLD",
        "errors": sorted(set(errors)),
        "source_components": sorted(_SOURCES),
        "sqlite_components": sorted(_SQLITE),
        "claim_boundary": "identity comparison only; direct running capture and test artifact authenticity are external prerequisites",
    }
