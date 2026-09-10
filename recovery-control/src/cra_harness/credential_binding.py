from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from cra_no_action_soak.time import parse_utc

HOSTS = frozenset({"arena", "cra", "dell"})
CREDENTIALS = frozenset({"arena_cra", "arena_dell", "dell_target_observer"})
SHA256_LENGTH = 64


def _canonical(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def credential_rehearsal_digest(value: Mapping[str, Any]) -> str:
    unsigned = {name: item for name, item in value.items() if name != "evidence_digest"}
    return hashlib.sha256(_canonical(unsigned)).hexdigest()


def _string_map(value: object, fields: frozenset[str], *, digest: bool = False) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError("CREDENTIAL_REHEARSAL_MAP_FIELDS_INVALID")
    result = {str(name): str(item) for name, item in value.items()}
    if any(not item for item in result.values()):
        raise ValueError("CREDENTIAL_REHEARSAL_MAP_VALUE_INVALID")
    if digest and any(
        len(item) != SHA256_LENGTH or any(character not in "0123456789abcdef" for character in item) for item in result.values()
    ):
        raise ValueError("CREDENTIAL_REHEARSAL_DIGEST_INVALID")
    return result


def validate_credential_rehearsal(
    value: Mapping[str, Any],
    *,
    expected_releases: Mapping[str, str],
    expected_configuration_sha256: Mapping[str, str],
    now: datetime | None = None,
    maximum_age_seconds: int = 30 * 24 * 3600,
) -> dict[str, Any]:
    required = {
        "schema",
        "runtime_release_ids",
        "configuration_set_sha256",
        "credential_fingerprints",
        "executed_at",
        "tested_endpoint_identities",
        "result",
        "evidence_digest",
    }
    if set(value) != required or value.get("schema") != "cra.credential_rotation_rehearsal.v1":
        raise ValueError("CREDENTIAL_REHEARSAL_FIELDS_INVALID")
    releases = _string_map(value["runtime_release_ids"], HOSTS)
    configurations = _string_map(value["configuration_set_sha256"], HOSTS, digest=True)
    _string_map(value["credential_fingerprints"], CREDENTIALS, digest=True)
    _string_map(value["tested_endpoint_identities"], CREDENTIALS)
    if releases != dict(expected_releases) or configurations != dict(expected_configuration_sha256):
        raise ValueError("CREDENTIAL_REHEARSAL_RELEASE_BINDING_MISMATCH")
    executed = parse_utc(str(value["executed_at"]))
    current = (now or datetime.now(UTC)).astimezone(UTC)
    age = (current - executed).total_seconds()
    if age < 0 or age > maximum_age_seconds:
        raise ValueError("CREDENTIAL_REHEARSAL_TIMESTAMP_INVALID")
    if value["result"] != "PASS":
        raise ValueError("CREDENTIAL_REHEARSAL_RESULT_NOT_PASS")
    digest = value["evidence_digest"]
    if not isinstance(digest, str) or digest != credential_rehearsal_digest(value):
        raise ValueError("CREDENTIAL_REHEARSAL_EVIDENCE_DIGEST_MISMATCH")
    return dict(value)


def run_credential_binding_harness() -> dict[str, Any]:
    now = datetime(2026, 9, 1, tzinfo=UTC)
    releases = {"arena": "arena-release", "cra": "cra-release", "dell": "dell-release"}
    configurations = {name: hashlib.sha256(f"config:{name}".encode()).hexdigest() for name in HOSTS}
    valid: dict[str, Any] = {
        "schema": "cra.credential_rotation_rehearsal.v1",
        "runtime_release_ids": releases,
        "configuration_set_sha256": configurations,
        "credential_fingerprints": {name: hashlib.sha256(f"credential:{name}".encode()).hexdigest() for name in CREDENTIALS},
        "executed_at": now.isoformat(),
        "tested_endpoint_identities": {name: f"endpoint-{name}" for name in CREDENTIALS},
        "result": "PASS",
    }
    valid["evidence_digest"] = credential_rehearsal_digest(valid)
    validate_credential_rehearsal(
        valid,
        expected_releases=releases,
        expected_configuration_sha256=configurations,
        now=now,
    )
    mutations: dict[str, Callable[[dict[str, Any]], object]] = {
        "release_binding_removed": lambda item: item["runtime_release_ids"].update(cra="wrong-release"),
        "configuration_binding_removed": lambda item: item["configuration_set_sha256"].update(cra="0" * 64),
        "credential_fingerprint_removed": lambda item: item["credential_fingerprints"].pop("arena_cra"),
        "execution_timestamp_removed": lambda item: item.update(executed_at=""),
        "endpoint_identity_removed": lambda item: item["tested_endpoint_identities"].update(arena_dell=""),
        "result_changed": lambda item: item.update(result="FAIL"),
        "evidence_digest_changed": lambda item: item.update(evidence_digest="0" * 64),
    }
    results: list[dict[str, Any]] = []
    for identity, mutate in mutations.items():
        candidate = copy.deepcopy(valid)
        mutate(candidate)
        detected = False
        reason = ""
        try:
            validate_credential_rehearsal(
                candidate,
                expected_releases=releases,
                expected_configuration_sha256=configurations,
                now=now,
            )
        except (KeyError, ValueError) as error:
            detected = True
            reason = str(error)
        results.append({"mutation_identity": identity, "detected": detected, "reason": reason})
    passed = sum(bool(item["detected"]) for item in results)
    return {
        "schema": "cra.credential_binding_harness.v1",
        "classification": "PASS" if passed == len(results) else "HARNESS_FAILURE",
        "scenario_family": "credential_rehearsal_release_and_endpoint_binding",
        "scenario_count": 1 + len(results),
        "negative_control_count": len(results),
        "detected_negative_control_count": passed,
        "sut_failure_count": 0,
        "harness_failure_count": len(results) - passed,
        "secret_value_count": 0,
        "mutations": results,
    }
