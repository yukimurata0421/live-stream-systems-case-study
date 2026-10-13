from __future__ import annotations

from copy import deepcopy

from cra_harness.running_qualification import compare_running_qualification


def _fixture() -> tuple[dict, dict]:
    sources = {
        key: {"host_id": "arena-monitoring-facts", "running_process_identity": "pid/start/boot",
              "source_sha256": char * 64}
        for key, char in (("arena_oauth", "a"), ("arena_network", "b"), ("arena_control_path", "c"))
    }
    sqlite = {
        key: {"host_id": host, "running_process_identity": "pid/start/boot", "version": version,
              "library_sha256": char * 64}
        for key, host, version, char in (
            ("cra_core", "cra-01-central-authority", "3.51.3", "d"),
            ("dell_agent", "dell-stream-runtime", "3.51.3", "e"),
            ("dell_owner", "dell-stream-runtime", "3.40.1", "f"),
        )
    }
    sources["arena_oauth"]["source_resolution"] = "process_mount_namespace"
    sources["arena_oauth"]["bind_read_only_paths_checked"] = True
    running = {"sources": sources, "sqlite": sqlite}
    qualified = {
        "sources": {key: {"source_sha256": row["source_sha256"], "test_status": "PASS",
                          "test_artifact_sha256": "1" * 64} for key, row in sources.items()},
        "sqlite": {key: {"version": row["version"], "library_sha256": row["library_sha256"],
                         "test_status": "PASS", "test_artifact_sha256": "2" * 64}
                   for key, row in sqlite.items()},
    }
    return running, qualified


def test_component_specific_sqlite_versions_can_differ_if_each_exact_runtime_was_tested() -> None:
    running, qualified = _fixture()
    assert compare_running_qualification(running, qualified)["status"] == "PASS"


def test_arena_running_source_drift_and_owner_sqlite_mismatch_hold() -> None:
    running, qualified = _fixture()
    running = deepcopy(running)
    running["sources"]["arena_oauth"]["source_sha256"] = "0" * 64
    running["sqlite"]["dell_owner"]["version"] = "3.51.3"
    result = compare_running_qualification(running, qualified)
    assert result["status"] == "HOLD"
    assert "RUNNING_SOURCE_NOT_QUALIFIED:arena_oauth" in result["errors"]
    assert "RUNNING_SQLITE_NOT_QUALIFIED:dell_owner" in result["errors"]


def test_missing_or_unbound_running_evidence_holds() -> None:
    running, qualified = _fixture()
    del running["sqlite"]["dell_owner"]
    running["sources"]["arena_network"].pop("running_process_identity")
    result = compare_running_qualification(running, qualified)
    assert "SQLITE_QUALIFICATION_MISSING:dell_owner" in result["errors"]
    assert "RUNNING_SOURCE_IDENTITY_UNBOUND:arena_network" in result["errors"]


def test_host_path_hash_cannot_qualify_bind_mounted_arena_process() -> None:
    running, qualified = _fixture()
    running["sources"]["arena_oauth"]["source_resolution"] = "host_filesystem"
    assert "RUNNING_SOURCE_NAMESPACE_UNVERIFIED:arena_oauth" in compare_running_qualification(running, qualified)["errors"]
