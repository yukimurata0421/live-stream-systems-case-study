from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "ops/systemd"


def _text(name: str) -> str:
    return (SYSTEMD / name).read_text(encoding="utf-8")


def _directives(name: str) -> str:
    return "\n".join(line for line in _text(name).splitlines() if line and not line.lstrip().startswith("#"))


def test_dell_observation_service_is_get_only_with_one_scoped_evidence_write_path() -> None:
    unit = _directives("dell-observation-server@.service")
    assert "User=stream-recovery-observer" in unit
    assert "ConditionPathExists=/opt/stream-recovery-observation/releases/%i/release_manifest.json" in unit
    assert "ConditionPathExists=/opt/stream-recovery-observation/runtimes/%i/runtime_manifest.json" in unit
    assert "/opt/stream-recovery-observation/runtimes/%i/.venv/bin/python -m dell_recovery_agent.observation_server" in unit
    assert "PYTHONPATH=/opt/stream-recovery-observation/releases/%i/src" in unit
    assert "Environment=CRA_IMMUTABLE_RELEASE_ID=%i" in unit
    assert "ReadWritePaths=/var/lib/stream-recovery-observation/releases/%i" in unit
    assert "ReadOnlyPaths=-/var/lib/stream-recovery-observation/host-status/releases/%i" in unit
    assert "StateDirectory=stream-recovery-observation/releases/%i" in unit
    assert "StateDirectoryMode=0750" in unit
    assert "-/var/lib/stream-recovery-control/dell/agent.sqlite3" in unit
    assert "-/var/lib/stream-recovery-control/dell/agent.sqlite3-wal" in unit
    assert "-/var/lib/stream-recovery-control/dell/agent.sqlite3-shm" in unit
    assert "-/var/lib/stream-recovery-control/dell/backups" in unit
    assert "-/run/stream-v3-control/effect.sock" in unit
    assert "MemoryDenyWriteExecute=true" in unit
    assert "NoNewPrivileges=true" in unit
    assert "ExecReload=/bin/kill -HUP $MAINPID" in unit


def test_arena_transport_adapter_signer_and_server_have_disjoint_credentials() -> None:
    puller = _directives("monitoring-v4-dell-observation-pull@.service")
    adapter = _directives("monitoring-v4-cra-live-adapter@.service")
    signer = _directives("monitoring-v4-cra-projection@.service")
    server = _directives("monitoring-v4-cra-projection-server@.service")

    assert "cra-projection-postgres.conninfo" in puller
    assert "InaccessiblePaths=/etc/stream-monitoring-v4/cra-projection-postgres.conninfo" in puller
    assert "monitoring-ed25519-private.pem" in puller
    assert "ReadWritePaths=/var/lib/stream-monitoring-v4/cra-dell-observation" in puller

    assert "ReadOnlyPaths=/etc/stream-monitoring-v4/cra-projection-postgres.conninfo" in adapter
    assert "Requires=monitoring-v4-dell-observation-pull@%i.service" in adapter
    assert "After=network-online.target monitoring-v4-dell-observation-pull@%i.service" in adapter
    assert "InaccessiblePaths=/etc/stream-monitoring-cra-projection/credentials/monitoring-ed25519-private.pem" in adapter
    assert "ReadWritePaths=/var/lib/stream-monitoring-v4/cra-facts" in adapter

    assert "RestrictAddressFamilies=AF_UNIX" in signer
    assert "Requires=monitoring-v4-cra-live-adapter@%i.service" in signer
    assert "After=monitoring-v4-cra-live-adapter@%i.service" in signer
    assert "AF_INET" not in signer
    assert "InaccessiblePaths=/etc/stream-monitoring-v4/cra-projection-postgres.conninfo" in signer
    assert "ReadWritePaths=/var/lib/stream-monitoring-v4/cra-projection" in signer

    assert "monitoring-ed25519-private.pem" in server
    assert "InaccessiblePaths=/etc/stream-monitoring-cra-projection/credentials/monitoring-ed25519-private.pem" in server
    assert "ReadWritePaths=/var/lib/stream-monitoring-v4/cra-projection/releases/%i" in server
    assert "--config /etc/stream-monitoring-cra-projection/releases/%i/projection-server.json" in server
    assert "0.0.0.0" not in server
    assert "ExecReload=/bin/kill -HUP $MAINPID" in server
    for unit in (puller, adapter, signer, server):
        assert "NoNewPrivileges=true" in unit
        assert "MemoryDenyWriteExecute=true" in unit
        assert "ConditionPathExists=/opt/stream-monitoring-cra-projection/runtimes/%i/runtime_manifest.json" in unit
        assert "/opt/stream-monitoring-cra-projection/runtimes/%i/.venv/bin/python -m monitoring_projection." in unit
        assert "Environment=CRA_IMMUTABLE_RELEASE_ID=%i" in unit
    for unit in (puller, adapter, signer):
        assert "StartLimitIntervalSec=10s" in unit
        assert "StartLimitBurst=10" in unit


def test_observation_plane_timers_are_bounded_and_instance_pinned() -> None:
    expected = {
        "monitoring-v4-dell-observation-pull@.timer": ("2s", "monitoring-v4-dell-observation-pull@%i.service"),
        "monitoring-v4-cra-live-adapter@.timer": ("5s", "monitoring-v4-cra-live-adapter@%i.service"),
    }
    for name, (interval, service) in expected.items():
        timer = _directives(name)
        assert f"OnUnitActiveSec={interval}" in timer
        assert "AccuracySec=200ms" in timer
        assert "RandomizedDelaySec=0" in timer
        assert f"Unit={service}" in timer
    projection_timer = _directives("monitoring-v4-cra-projection@.timer")
    assert "OnUnitInactiveSec=1s" in projection_timer
    assert "OnUnitActiveSec=" not in projection_timer
    assert "AccuracySec=100ms" in projection_timer
    assert "RandomizedDelaySec=0" in projection_timer
    assert "Unit=monitoring-v4-cra-projection@%i.service" in projection_timer
    assert "WantedBy=timers.target" not in _directives("monitoring-v4-dell-observation-pull@.timer")
    assert "WantedBy=timers.target" not in _directives("monitoring-v4-cra-live-adapter@.timer")
    assert "WantedBy=timers.target" in _directives("monitoring-v4-cra-projection@.timer")


def test_example_configs_bind_new_host_identity_and_separate_release_ids() -> None:
    dell = json.loads(_text("dell-observation-publisher.example.json"))
    pull = json.loads(_text("dell-observation-pull.example.json"))
    adapter = json.loads(_text("monitoring-live-adapter.example.json"))
    projection = json.loads(_text("monitoring-live-projection.example.json"))
    server = json.loads(_text("monitoring-projection-server.example.json"))

    assert dell["expected_target"]["host_id"] == "dell-yuki"
    assert dell["maximum_bundle_ttl_seconds"] == pull["maximum_ttl_seconds"] == 30
    assert dell["maximum_transport_age_seconds"] == dell["maximum_state_age_seconds"] == 30
    assert adapter["fact_ttl_seconds"] == projection["projection_ttl_seconds"] == 20
    assert projection["maximum_fact_age_seconds"] == projection["maximum_target_age_seconds"] == 30
    assert pull["expected_target"] == adapter["expected_target"] == projection["expected_target"]
    assert pull["source_release_id"] == adapter["dell_source_release_id"]
    assert adapter["source_release_id"] == projection["source_release_id"]
    assert projection["source_release_id"] == server["server_release_id"]
    assert server["schema"] == "monitoring_v4.cra_projection_server_config.v3"
    assert server["resilient_host_status_file"].endswith("/resilient-host-status.json")
    assert server["host_status_host_id"] == "arena-monitoring-facts"
    assert adapter["output_file"] == projection["fact_bundle_file"]
    assert adapter["target_snapshot_output_file"] == projection["target_snapshot_file"]
    for state_path in (
        pull["observation_file"],
        pull["status_file"],
        adapter["output_file"],
        adapter["target_snapshot_output_file"],
        adapter["status_file"],
        projection["output_file"],
        projection["status_file"],
        projection["state_database"],
        server["projection_file"],
        server["host_status_file"],
    ):
        assert "/releases/replace-with-immutable-arena-projection-release-id/" in state_path
    assert adapter["postgres_conninfo_file"].endswith("cra-projection-postgres.conninfo")
    assert dell["signing_private_key_file"] != projection["private_key_file"]


def test_signed_host_status_units_are_separate_and_formal_collection_has_no_remote_shell() -> None:
    dell_publisher = _directives("dell-no-action-host-status-publisher@.service")
    arena_pull = _directives("monitoring-v4-dell-host-status-pull@.service")
    arena_publisher = _directives("monitoring-v4-no-action-host-status-publisher@.service")
    arena_publisher_timer = _directives("monitoring-v4-no-action-host-status-publisher@.timer")
    cra_pull = _directives("cra-arena-host-status-pull@.service")
    collector = _directives("cra-no-action-soak-local-collector@.service")

    assert "RestrictAddressFamilies=AF_UNIX" in dell_publisher
    assert "User=root" in dell_publisher
    assert "Group=stream-recovery" in dell_publisher
    assert "CapabilityBoundingSet=" in dell_publisher
    assert (
        "LoadCredential=dell-host-status-signing-key.pem:/etc/stream-recovery-observation/credentials/dell-observation-ed25519-private.pem"
    ) in dell_publisher
    assert "CRA_HOST_STATUS_SIGNING_KEY_FILE=%d/dell-host-status-signing-key.pem" in dell_publisher
    assert "StateDirectory=stream-recovery-observation/host-status/releases/%i" in dell_publisher
    assert "StateDirectoryMode=0750" in dell_publisher
    assert "ReadWritePaths=/var/lib/stream-recovery-observation/host-status/releases/%i" in dell_publisher
    assert "InaccessiblePaths=/etc/stream-recovery-observation/credentials/dell-observation-ed25519-private.pem" in dell_publisher
    assert "effect.sock" in dell_publisher
    assert "dell-observation-server-key.pem" in dell_publisher
    assert "cra_no_action_soak.host_status_pull" in arena_pull
    assert "ReadWritePaths=/var/lib/stream-monitoring-v4/cra-dell-observation" in arena_pull
    assert "cra-projection-postgres.conninfo" in arena_pull
    assert "RestrictAddressFamilies=AF_UNIX" in arena_publisher
    assert "Wants=monitoring-v4-dell-host-status-pull@%i.service" in arena_publisher
    assert "User=stream-monitoring-projection" in arena_publisher
    assert "Group=stream-monitoring-projection" in arena_publisher
    assert "SupplementaryGroups=systemd-journal" in arena_publisher
    assert "CapabilityBoundingSet=" in arena_publisher
    assert "dell-observation-client-key.pem" in arena_publisher
    assert "ReadWritePaths=/var/lib/stream-monitoring-v4/cra-projection/releases/%i" in arena_publisher
    assert "OnUnitInactiveSec=3s" in arena_publisher_timer
    assert "cra_no_action_soak.host_status_pull" in cra_pull
    assert "InaccessiblePaths=/var/lib/stream-recovery-control/cra" in cra_pull
    assert "CRA_SOAK_REMOTE_EXECUTION=forbidden" in collector
    assert "Wants=cra-arena-host-status-pull@%i.service" in collector
    assert "CRA_SOAK_LOCAL_DIRECT=1" in collector
    assert "User=stream-recovery" in collector
    assert "Group=stream-recovery" in collector
    assert "SupplementaryGroups=systemd-journal" in collector
    assert "RestrictAddressFamilies=AF_UNIX" in collector
    assert "InaccessiblePaths=-/usr/bin/ssh -/bin/ssh" in collector
    assert "/etc/stream-recovery-control/cra-monitoring-client-key.pem" in collector
    assert "/etc/stream-recovery-control/cra-archive-signing-private.pem" in collector
    assert "/opt/stream-recovery-control/runtimes/%i/.venv/bin/python" in collector


def test_signed_host_status_example_chain_is_release_and_role_bound() -> None:
    dell_server = json.loads(_text("dell-observation-server.example.json"))
    dell_publisher = json.loads(_text("dell-no-action-host-status-publisher.example.json"))
    arena_pull = json.loads(_text("monitoring-v4-dell-host-status-pull.example.json"))
    arena_publisher = json.loads(_text("monitoring-v4-no-action-host-status-publisher.example.json"))
    cra_pull = json.loads(_text("cra-arena-host-status-pull.example.json"))
    profile = json.loads(_text("cra-no-action-soak-local-profile.example.json"))

    assert dell_server["schema"] == "cra_dell_recovery.observation_server_config.v3"
    assert dell_server["resilient_host_status_file"].endswith("/resilient-host-status.json")
    assert dell_server["published_observation_file"] == dell_publisher["role_inputs"]["observation_file"]
    assert dell_server["host_status_file"] == dell_publisher["output_file"]
    assert "/host-status/releases/replace-with-immutable-dell-observation-release-id/" in dell_server["host_status_file"]
    assert dell_server["server_resource_status_file"] == dell_publisher["role_inputs"]["server_resource_status_file"]
    assert dell_publisher["output_owner"] == "root"
    assert dell_publisher["minimum_remaining_validity_seconds"] == 5
    assert dell_publisher["role"] == arena_pull["source_role"] == "dell"
    assert arena_pull["output_file"] == arena_publisher["role_inputs"]["dell_host_status_file"]
    assert arena_publisher["role"] == cra_pull["source_role"] == "arena"
    assert arena_publisher["minimum_remaining_validity_seconds"] == 10
    assert arena_pull["maximum_retry_elapsed_seconds"] == 15
    assert arena_pull["minimum_remaining_validity_seconds"] == 12
    assert arena_pull["minimum_remaining_validity_seconds"] >= arena_publisher["minimum_remaining_validity_seconds"] + 2
    assert cra_pull["maximum_retry_elapsed_seconds"] == 15
    assert cra_pull["minimum_remaining_validity_seconds"] == 5
    assert arena_publisher["output_file"].endswith("/host-status.json")
    assert arena_publisher["role_inputs"]["server_resource_status_file"].endswith("/server-resource-status.json")
    assert cra_pull["output_file"] == profile["arena_host_status_file"]
    assert profile["collection_mode"] == "local_host_status_v1"
    assert set(profile["host_status_sources"]) == {"arena", "dell"}


def test_soak_collector_is_persistent_and_samples_below_gate_interval() -> None:
    service = _directives("cra-no-action-soak-collector@.service")
    timer = _directives("cra-no-action-soak-collector@.timer")
    assert "/opt/cra-no-action-soak/%i/tools/collect_cra_no_action_soak.py" in service
    assert "/var/lib/cra-no-action-soak/%i/samples.jsonl" in service
    assert "--gate-output /var/lib/cra-no-action-soak/%i/gate-current.json" in service
    assert "--terminal-output /var/lib/cra-no-action-soak/%i/terminal-failure.json" in service
    assert "ConditionPathExists=!/var/lib/cra-no-action-soak/%i/terminal-failure.json" not in service
    assert "OnUnitInactiveSec=10s" in timer
    assert "AccuracySec=1s" in timer
    assert "RandomizedDelaySec=0" in timer
    assert "WantedBy=timers.target" in timer


def test_resilient_status_v3_chain_clamps_report_to_evidence_and_keeps_local_only_soak() -> None:
    dell = json.loads(_text("dell-resilient-host-status-publisher.example.json"))
    arena_pull = json.loads(_text("monitoring-v4-dell-resilient-status-pull.example.json"))
    arena = json.loads(_text("monitoring-v4-resilient-status-publisher.example.json"))
    cra_pull = json.loads(_text("cra-arena-resilient-status-pull.example.json"))
    soak = json.loads(_text("cra-resilient-status-soak.example.json"))

    assert dell["schema"] == "cra.resilient_host_status_publisher.v3"
    assert dell["reporter_lease_seconds"] == arena["reporter_lease_seconds"] == 45
    assert dell["component_validity_seconds"] == arena["component_validity_seconds"] == 20
    assert dell["maximum_clock_tracking_age_seconds"] == 20
    assert arena["maximum_clock_tracking_age_seconds"] == 10
    assert dell["last_good_retention_seconds"] == arena["last_good_retention_seconds"] == 300
    assert set(dell["service_units"]) >= {"k3s_service", "target_snapshot_producer"}
    assert arena_pull["endpoint_url"].endswith("/v3/no-action-soak-status/latest")
    assert arena_pull["source_release_id"] == arena["dell_release_id"]
    assert all(
        arena_pull[name] is None
        for name in (
            "upstream_key_id",
            "upstream_public_key_file",
            "upstream_role",
            "upstream_host_id",
            "upstream_release_id",
        )
    )
    assert arena_pull["output_file"] == arena["dell_status_file"]
    assert arena_pull["status_file"] == arena["dell_pull_status_file"]
    assert arena["schema"] == "cra.resilient_host_status_relay_publisher.v4"
    assert set(arena["service_units"]) == {"arena_projection_server", "arena_resilient_status_publisher"}
    assert set(arena["pipeline_status_files"]) == {"arena_live_adapter", "arena_projection"}
    assert arena["pipeline_status_files"]["arena_live_adapter"].endswith(
        "/cra-facts/releases/replace-with-immutable-arena-projection-release-id/status.json"
    )
    assert arena["pipeline_status_files"]["arena_projection"].endswith(
        "/cra-projection/releases/replace-with-immutable-arena-projection-release-id/status.json"
    )
    assert arena["output_file"].endswith("/resilient-host-status.json")
    assert cra_pull["endpoint_url"].endswith("/v3/no-action-soak-status/latest")
    assert cra_pull["source_release_id"] == soak["arena_release_id"]
    assert cra_pull["upstream_role"] == "dell"
    assert cra_pull["upstream_host_id"] == "dell-stream-runtime"
    assert cra_pull["upstream_release_id"] == soak["dell_release_id"]
    assert cra_pull["upstream_public_key_file"].endswith("/dell-observation-ed25519-public.pem")
    assert arena_pull["minimum_remaining_lease_seconds"] == 10
    assert cra_pull["minimum_remaining_lease_seconds"] == 5
    # The Dell report is clamped to the clock evidence deadline.  Preserve one
    # complete publisher + pull scheduling budget before arena applies its
    # admission floor; equality made every live report impossible to admit.
    assert "OnUnitInactiveSec=5s" in _directives("dell-resilient-host-status-publisher@.timer")
    assert "OnUnitInactiveSec=3s" in _directives("monitoring-v4-dell-resilient-status-pull@.timer")
    assert dell["maximum_clock_tracking_age_seconds"] >= arena_pull["minimum_remaining_lease_seconds"] + 5 + 3
    assert cra_pull["output_file"] == soak["arena_status_inbox_file"]
    assert cra_pull["status_file"] == soak["cra_pull_status_file"]
    assert cra_pull["recovery_state_file"] == soak["cra_pull_recovery_state_file"]
    assert soak["schema"] == "cra.resilient_status_soak_config.v2"
    assert soak["minimum_duration_seconds"] == 604800
    assert soak["maximum_recovery_episode_seconds"] == 60
    assert soak["maximum_sample_gap_seconds"] == 45
    assert soak["maximum_evidence_bytes"] == 2147483648

    dell_unit = _directives("dell-resilient-host-status-publisher@.service")
    clock_unit = _directives("dell-clock-status-probe@.service")
    arena_unit = _directives("monitoring-v4-resilient-status-publisher@.service")
    arena_clock_unit = _directives("monitoring-v4-clock-status-probe@.service")
    cra_unit = _directives("cra-resilient-status-soak@.service")
    cra_timer = _directives("cra-resilient-status-soak@.timer")
    cra_gate_unit = _directives("cra-resilient-status-soak-gate@.service")
    cra_gate_timer = _directives("cra-resilient-status-soak-gate@.timer")
    cra_watchdog_unit = _directives("cra-resilient-status-soak-watchdog@.service")
    cra_watchdog_timer = _directives("cra-resilient-status-soak-watchdog@.timer")
    assert "RestrictAddressFamilies=AF_UNIX" in dell_unit
    assert "effect.sock" in dell_unit
    assert "User=_chrony" in clock_unit
    assert "CapabilityBoundingSet=" in clock_unit
    assert "RestrictAddressFamilies=AF_UNIX" in clock_unit
    assert "RestrictAddressFamilies=AF_UNIX" in arena_unit
    assert "cra-projection-postgres.conninfo" in arena_unit
    assert arena["clock_tracking_file"].endswith("/clock-status/releases/replace-with-immutable-arena-projection-release-id/tracking.json")
    assert "User=_chrony" in arena_clock_unit
    assert "CapabilityBoundingSet=" in arena_clock_unit
    assert "RestrictAddressFamilies=AF_UNIX" in arena_clock_unit
    assert "stream-monitoring-cra-projection" in arena_clock_unit
    assert "cra-projection-postgres.conninfo" not in arena_clock_unit
    assert "RestrictAddressFamilies=AF_UNIX" in cra_unit
    assert "central.sqlite3" in cra_unit
    assert "--evaluate" not in cra_unit
    assert "OnUnitInactiveSec=15s" in cra_timer
    assert "--evaluate" in cra_gate_unit
    assert "RestrictAddressFamilies=AF_UNIX" in cra_gate_unit
    assert "TimeoutStartSec=1800s" in cra_gate_unit
    assert "MemoryMax=256M" in cra_gate_unit
    assert "IOSchedulingClass=idle" in cra_gate_unit
    assert "OnUnitInactiveSec=1h" in cra_gate_timer
    assert "--watchdog" in cra_watchdog_unit
    assert "RestrictAddressFamilies=AF_UNIX" in cra_watchdog_unit
    assert "MemoryMax=128M" in cra_watchdog_unit
    assert "OnUnitInactiveSec=5min" in cra_watchdog_timer
