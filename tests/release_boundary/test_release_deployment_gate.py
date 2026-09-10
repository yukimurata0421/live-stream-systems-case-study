from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from cra_harness.oracles.release_boundary import expected_violations
from release_boundary import evaluate_release_deployment_compatibility


def good_contract() -> dict[str, Any]:
    return {
        "entrypoint": "bin/stream-prod",
        "entrypoint_exists": True,
        "installed_cli_options": ["--record"],
        "accepted_cli_options": ["--record", "--no-record"],
        "required_env": ["PYTHONPATH"],
        "provided_env": ["PYTHONPATH"],
        "required_paths": ["bin/stream-prod", "src/stream_core/cli.py"],
        "available_paths": ["bin/stream-prod", "src/stream_core/cli.py"],
        "working_directory": "/immutable/report-v2",
        "working_directory_compatible": True,
        "installed_argv_checked": True,
    }


def valid_manifest() -> dict[str, Any]:
    return {
        "schema_version": "recovery_control.release_deployment_manifest.v1",
        "requested_change_scope": ["remote-recovery"],
        "candidate": {
            "release_id": "remote-recovery-v2",
            "release_path": "/releases/remote-recovery-v2",
            "immutable": True,
            "source_hash": "abc",
            "actual_source_hash": "abc",
        },
        "expected_consumer_ids": ["remote-recovery", "report"],
        "consumers": [
            {
                "consumer_id": "remote-recovery",
                "in_dependency_graph": True,
                "in_requested_scope": True,
                "sharing_classification": "SAFE_SHARED",
                "executable_identity_changes": True,
                "hidden_shared_dependency": False,
                "contract": good_contract(),
            },
            {
                "consumer_id": "report",
                "in_dependency_graph": True,
                "in_requested_scope": False,
                "sharing_classification": "SAFE_SHARED",
                "executable_identity_changes": False,
                "hidden_shared_dependency": False,
                "contract": good_contract(),
            },
        ],
        "rollback": {
            "release_id": "remote-recovery-v1",
            "executable_identities_changed": ["remote-recovery"],
            "contracts": {
                "remote-recovery": good_contract(),
                "report": good_contract(),
            },
        },
        "deployment_actions": [
            {
                "action_id": "switch-remote-recovery-unit",
                "target_component": "remote-recovery",
                "executable_identities_changed": ["remote-recovery"],
                "containers_replaced": [],
                "pod_uid_changes": False,
                "pod_blast_radius_declared": True,
                "effect_owners_affected": ["remote-recovery"],
                "declared_effect_owners": ["remote-recovery"],
                "independent_deploy_claim": True,
                "projection_requires_write_credential": False,
                "mutable_identity": False,
            }
        ],
    }


def result_codes(manifest: dict[str, Any]) -> set[str]:
    return set(evaluate_release_deployment_compatibility(manifest)["violation_codes"])


@pytest.mark.parametrize(
    ("control", "injected", "mutate"),
    [
        ("NC-RB-01", {"installed_cli_arg_removed": True}, lambda m: m["consumers"][0]["contract"].update(accepted_cli_options=[])),
        ("NC-RB-02", {"required_env_renamed": True}, lambda m: m["consumers"][0]["contract"].update(provided_env=[])),
        ("NC-RB-03", {"entrypoint_deleted": True}, lambda m: m["consumers"][0]["contract"].update(entrypoint_exists=False)),
        (
            "NC-RB-04",
            {"working_directory_mismatch": True},
            lambda m: m["consumers"][0]["contract"].update(working_directory_compatible=False),
        ),
        ("NC-RB-05", {"expected_file_absent": True}, lambda m: m["consumers"][0]["contract"].update(available_paths=[])),
        (
            "NC-RB-06",
            {"out_of_scope_consumer_upgrade": True},
            lambda m: m["consumers"][1].update(executable_identity_changes=True, sharing_classification="ACCIDENTALLY_SHARED"),
        ),
        (
            "NC-RB-07",
            {"rollback_contract_incompatible": True},
            lambda m: m["rollback"]["contracts"]["remote-recovery"].update(accepted_cli_options=[]),
        ),
        ("NC-RB-08", {"source_hash_mismatch": True}, lambda m: m["candidate"].update(actual_source_hash="different")),
        (
            "NC-RB-09",
            {"mutable_release_identity": True},
            lambda m: m["candidate"].update(immutable=False, release_path="/releases/current"),
        ),
        (
            "NC-RB-10",
            {"consumer_omitted": True},
            lambda m: m.update(consumers=[m["consumers"][0]]),
        ),
        (
            "NC-DB-01",
            {"mp03_replaces_stream_engine": True},
            lambda m: m["deployment_actions"][0].update(
                target_component="MP-03",
                containers_replaced=["fast-recovery-loop", "stream-engine"],
            ),
        ),
        (
            "NC-DB-02",
            {"consumer_release_changes_report": True},
            lambda m: m["deployment_actions"][0].update(target_component="MP-03", executable_identities_changed=["MP-03", "report"]),
        ),
        (
            "NC-DB-03",
            {"rollback_changes_unrelated": True},
            lambda m: m["rollback"].update(executable_identities_changed=["remote-recovery", "report"]),
        ),
        (
            "NC-DB-04",
            {"hidden_shared_dependency": True},
            lambda m: m["consumers"][1].update(hidden_shared_dependency=True, release_path="/releases/current"),
        ),
        (
            "NC-DB-05",
            {"effect_owner_omitted": True},
            lambda m: m["deployment_actions"][0].update(declared_effect_owners=[]),
        ),
        (
            "NC-DB-06",
            {"pod_blast_radius_omitted": True},
            lambda m: m["deployment_actions"][0].update(pod_uid_changes=True, pod_blast_radius_declared=False),
        ),
        (
            "NC-DB-07",
            {"deployment_mutable_identity": True},
            lambda m: m["deployment_actions"][0].update(mutable_identity=True),
        ),
        (
            "NC-DB-08",
            {"projection_write_credential": True},
            lambda m: m["deployment_actions"][0].update(projection_requires_write_credential=True),
        ),
        (
            "NC-DB-09",
            {"false_independent_deploy": True},
            lambda m: m["deployment_actions"][0].update(
                independent_deploy_claim=True,
                pod_uid_changes=True,
                pod_blast_radius_declared=True,
                containers_replaced=["fast-recovery-loop", "stream-engine"],
            ),
        ),
        (
            "NC-DB-10",
            {"installed_argv_not_checked": True},
            lambda m: m["consumers"][0]["contract"].update(installed_argv_checked=False),
        ),
    ],
)
def test_negative_control_is_detected(
    control: str,
    injected: dict[str, bool],
    mutate: Any,
) -> None:
    manifest = deepcopy(valid_manifest())
    mutate(manifest)
    expected = expected_violations(injected)
    assert expected <= result_codes(manifest), control


def test_valid_manifest_passes() -> None:
    result = evaluate_release_deployment_compatibility(valid_manifest())
    assert result["pass"] is True
    assert result["physical_effect_count"] == 0


def test_explicit_shared_release_is_allowed_only_when_all_contracts_pass() -> None:
    manifest = valid_manifest()
    manifest["consumers"][1].update(executable_identity_changes=True, sharing_classification="COUPLED_BY_DESIGN")
    assert evaluate_release_deployment_compatibility(manifest)["pass"] is True
    manifest["consumers"][1]["contract"]["accepted_cli_options"] = []
    assert "INSTALLED_CLI_ARG_REMOVED" in result_codes(manifest)


def test_stage_c_bad_release_and_hotfix_replay() -> None:
    bad = valid_manifest()
    bad["requested_change_scope"] = ["remote-recovery"]
    bad["consumers"][1].update(
        executable_identity_changes=True,
        sharing_classification="ACCIDENTALLY_SHARED",
    )
    bad["consumers"][1]["contract"]["accepted_cli_options"] = ["--no-record"]
    bad_codes = result_codes(bad)
    assert "OUT_OF_SCOPE_CONSUMER_CHANGE" in bad_codes
    assert "INSTALLED_CLI_ARG_REMOVED" in bad_codes

    hotfix = deepcopy(bad)
    hotfix["requested_change_scope"] = ["remote-recovery", "report"]
    hotfix["consumers"][1].update(in_requested_scope=True, sharing_classification="COUPLED_BY_DESIGN")
    hotfix["consumers"][1]["contract"]["accepted_cli_options"] = ["--record", "--no-record"]
    assert evaluate_release_deployment_compatibility(hotfix)["pass"] is True


def test_public_health_failure_chain_fixture_remains_explicit() -> None:
    fixture = {
        "report_timer_exit_code": 2,
        "report_fresh": False,
        "subsystems_degraded": 1,
        "public_contract_ok": 22,
        "public_contract_total": 23,
    }
    assert fixture["report_timer_exit_code"] == 2
    assert fixture["report_fresh"] is False
    assert fixture["subsystems_degraded"] == 1
    assert fixture["public_contract_ok"] < fixture["public_contract_total"]
