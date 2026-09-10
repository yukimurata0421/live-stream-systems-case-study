from __future__ import annotations

from collections.abc import Mapping


def expected_violations(injected: Mapping[str, bool]) -> set[str]:
    """Independent truth table for release/deployment negative controls.

    This module intentionally does not import the gate, graph builder, or blast
    radius evaluator.
    """

    mapping = {
        "installed_cli_arg_removed": "INSTALLED_CLI_ARG_REMOVED",
        "required_env_renamed": "REQUIRED_ENV_MISSING",
        "entrypoint_deleted": "ENTRYPOINT_DELETED",
        "working_directory_mismatch": "WORKING_DIRECTORY_MISMATCH",
        "expected_file_absent": "EXPECTED_FILE_ABSENT",
        "out_of_scope_consumer_upgrade": "OUT_OF_SCOPE_CONSUMER_CHANGE",
        "rollback_contract_incompatible": "ROLLBACK_INSTALLED_CLI_ARG_REMOVED",
        "source_hash_mismatch": "CANDIDATE_SOURCE_HASH_MISMATCH",
        "mutable_release_identity": "MUTABLE_RELEASE_IDENTITY",
        "consumer_omitted": "CONSUMER_OMITTED_FROM_GRAPH",
        "mp03_replaces_stream_engine": "MP03_REPLACES_STREAM_ENGINE",
        "consumer_release_changes_report": "CONSUMER_SPECIFIC_RELEASE_CHANGES_REPORT",
        "rollback_changes_unrelated": "ROLLBACK_CHANGES_UNRELATED_CONSUMER",
        "hidden_shared_dependency": "HIDDEN_SHARED_CURRENT_DEPENDENCY",
        "effect_owner_omitted": "EFFECT_OWNER_OMITTED",
        "pod_blast_radius_omitted": "POD_BLAST_RADIUS_OMITTED",
        "deployment_mutable_identity": "DEPLOYMENT_USES_MUTABLE_IDENTITY",
        "projection_write_credential": "PROJECTION_REQUIRES_WRITE_CREDENTIAL",
        "false_independent_deploy": "FALSE_INDEPENDENT_DEPLOY_CLAIM",
        "installed_argv_not_checked": "INSTALLED_ARGV_NOT_CHECKED",
    }
    return {code for field, code in mapping.items() if injected.get(field) is True}
