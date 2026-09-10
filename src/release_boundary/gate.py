from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _values(value: object) -> set[str]:
    if not isinstance(value, list):
        return set()
    return {str(item) for item in value}


def _contract_violations(
    consumer: Mapping[str, Any],
    contract: Mapping[str, Any],
    *,
    prefix: str = "",
) -> list[dict[str, str]]:
    consumer_id = str(consumer.get("consumer_id") or "UNKNOWN")
    installed_options = _values(contract.get("installed_cli_options"))
    accepted_options = _values(contract.get("accepted_cli_options"))
    required_env = _values(contract.get("required_env"))
    provided_env = _values(contract.get("provided_env"))
    required_paths = _values(contract.get("required_paths"))
    available_paths = _values(contract.get("available_paths"))
    violations: list[dict[str, str]] = []

    def add(code: str, detail: str) -> None:
        violations.append({"code": f"{prefix}{code}", "consumer_id": consumer_id, "detail": detail})

    missing_options = sorted(installed_options - accepted_options)
    if missing_options:
        add("INSTALLED_CLI_ARG_REMOVED", ",".join(missing_options))
    missing_env = sorted(required_env - provided_env)
    if missing_env:
        add("REQUIRED_ENV_MISSING", ",".join(missing_env))
    if not bool(contract.get("entrypoint_exists", False)):
        add("ENTRYPOINT_DELETED", str(contract.get("entrypoint") or ""))
    if not bool(contract.get("working_directory_compatible", False)):
        add("WORKING_DIRECTORY_MISMATCH", str(contract.get("working_directory") or ""))
    missing_paths = sorted(required_paths - available_paths)
    if missing_paths:
        add("EXPECTED_FILE_ABSENT", ",".join(missing_paths))
    if not bool(contract.get("installed_argv_checked", False)):
        add("INSTALLED_ARGV_NOT_CHECKED", "candidate parser was not checked against installed argv")
    return violations


def evaluate_release_deployment_compatibility(manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate a normalized release/deployment manifest without executing effects.

    The manifest is deliberately data-only.  It can be built from installed units,
    release manifests, and Kubernetes objects without importing a production
    mutator or invoking its action adapter.
    """

    violations: list[dict[str, str]] = []

    def add(code: str, detail: str, consumer_id: str = "") -> None:
        row = {"code": code, "detail": detail}
        if consumer_id:
            row["consumer_id"] = consumer_id
        violations.append(row)

    candidate = manifest.get("candidate")
    candidate = candidate if isinstance(candidate, Mapping) else {}
    if str(candidate.get("source_hash") or "") != str(candidate.get("actual_source_hash") or ""):
        add("CANDIDATE_SOURCE_HASH_MISMATCH", "declared and actual source hashes differ")
    release_path = str(candidate.get("release_path") or "")
    if not bool(candidate.get("immutable")) or release_path.endswith(("/current", "/latest")):
        add("MUTABLE_RELEASE_IDENTITY", release_path or "missing release path")

    consumers = manifest.get("consumers")
    consumers = consumers if isinstance(consumers, list) else []
    declared_consumers = {str(item.get("consumer_id") or "") for item in consumers if isinstance(item, Mapping)}
    expected_consumers = _values(manifest.get("expected_consumer_ids"))
    for missing in sorted(expected_consumers - declared_consumers):
        add("CONSUMER_OMITTED_FROM_GRAPH", missing, missing)

    requested_scope = _values(manifest.get("requested_change_scope"))
    rollback = manifest.get("rollback")
    rollback = rollback if isinstance(rollback, Mapping) else {}
    rollback_contracts = rollback.get("contracts")
    rollback_contracts = rollback_contracts if isinstance(rollback_contracts, Mapping) else {}
    rollback_changed = _values(rollback.get("executable_identities_changed"))

    for item in consumers:
        if not isinstance(item, Mapping):
            continue
        consumer_id = str(item.get("consumer_id") or "UNKNOWN")
        if not bool(item.get("in_dependency_graph", False)):
            add("CONSUMER_OMITTED_FROM_GRAPH", consumer_id, consumer_id)
        if bool(item.get("hidden_shared_dependency", False)):
            add("HIDDEN_SHARED_CURRENT_DEPENDENCY", str(item.get("release_path") or ""), consumer_id)
        executable_changes = bool(item.get("executable_identity_changes", False))
        classification = str(item.get("sharing_classification") or "UNKNOWN")
        in_scope = consumer_id in requested_scope or bool(item.get("in_requested_scope", False))
        if executable_changes and not in_scope and classification != "COUPLED_BY_DESIGN":
            add("OUT_OF_SCOPE_CONSUMER_CHANGE", "shared release changes an out-of-scope executable", consumer_id)
        contract = item.get("contract")
        if isinstance(contract, Mapping):
            violations.extend(_contract_violations(item, contract))
        if consumer_id in rollback_changed and not in_scope:
            add("ROLLBACK_CHANGES_UNRELATED_CONSUMER", "rollback changes an out-of-scope executable", consumer_id)
        rollback_contract = rollback_contracts.get(consumer_id)
        if isinstance(rollback_contract, Mapping):
            violations.extend(_contract_violations(item, rollback_contract, prefix="ROLLBACK_"))

    actions = manifest.get("deployment_actions")
    actions = actions if isinstance(actions, list) else []
    for raw_action in actions:
        if not isinstance(raw_action, Mapping):
            continue
        action_id = str(raw_action.get("action_id") or "UNKNOWN")
        target = str(raw_action.get("target_component") or "")
        containers = _values(raw_action.get("containers_replaced"))
        affected_effect_owners = _values(raw_action.get("effect_owners_affected"))
        declared_effect_owners = _values(raw_action.get("declared_effect_owners"))
        if target == "MP-03" and "stream-engine" in containers:
            add("MP03_REPLACES_STREAM_ENGINE", action_id)
        if target == "MP-03" and "report" in _values(raw_action.get("executable_identities_changed")):
            add("CONSUMER_SPECIFIC_RELEASE_CHANGES_REPORT", action_id)
        omitted_effect_owners = sorted(affected_effect_owners - declared_effect_owners)
        if omitted_effect_owners:
            add("EFFECT_OWNER_OMITTED", f"{action_id}:{','.join(omitted_effect_owners)}")
        actual_pod_change = bool(raw_action.get("pod_uid_changes"))
        if actual_pod_change and not bool(raw_action.get("pod_blast_radius_declared")):
            add("POD_BLAST_RADIUS_OMITTED", action_id)
        if bool(raw_action.get("independent_deploy_claim")) and (actual_pod_change or len(containers) > 1):
            add("FALSE_INDEPENDENT_DEPLOY_CLAIM", action_id)
        if bool(raw_action.get("projection_requires_write_credential")):
            add("PROJECTION_REQUIRES_WRITE_CREDENTIAL", action_id)
        if bool(raw_action.get("mutable_identity")):
            add("DEPLOYMENT_USES_MUTABLE_IDENTITY", action_id)

    codes = sorted({row["code"] for row in violations})
    return {
        "schema_version": "recovery_control.release_deployment_gate_result.v1",
        "pass": not violations,
        "violation_count": len(violations),
        "violation_codes": codes,
        "violations": violations,
        "production_behavior_modified": False,
        "physical_effect_count": 0,
    }
