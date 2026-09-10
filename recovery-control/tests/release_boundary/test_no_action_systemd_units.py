from __future__ import annotations

import json
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "ops/systemd"


def _text(name: str) -> str:
    return (SYSTEMD / name).read_text(encoding="utf-8")


def _directives(name: str) -> str:
    return "\n".join(line for line in _text(name).splitlines() if line and not line.lstrip().startswith("#"))


def test_all_new_unit_entrypoints_are_packaged_and_module_invocations_are_runtime_pinned() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    scripts = set(project["project"]["scripts"])
    expected = {
        "cra-monitoring-project",
        "cra-monitoring-serve",
        "cra-monitoring-pull",
        "cra-runtime",
    }
    assert expected <= scripts
    units = {
        "cra_authority.projection_pull": _text("cra-monitoring-projection-pull@.service"),
        "cra_authority.runtime": _text("cra-runtime-no-action@.service"),
        "monitoring_projection.producer": _text("monitoring-v4-cra-projection@.service"),
        "monitoring_projection.server": _text("monitoring-v4-cra-projection-server@.service"),
    }
    for module, unit in units.items():
        assert f"/.venv/bin/python -m {module}" in unit
        assert "/runtimes/%i/runtime_manifest.json" in unit


def test_no_action_runtime_and_producer_have_no_network_or_effect_credential() -> None:
    runtime = _directives("cra-runtime-no-action@.service")
    producer = _directives("monitoring-v4-cra-projection.service")
    for unit in (runtime, producer):
        assert "RestrictAddressFamilies=AF_UNIX" in unit
        assert "AF_INET" not in unit
        assert "NoNewPrivileges=true" in unit
        assert "MemoryDenyWriteExecute=true" in unit
        assert "dell" not in unit.lower()
        assert "effect" not in unit.lower()


def test_transport_processes_do_not_receive_central_or_effect_write_paths() -> None:
    server = _directives("monitoring-v4-cra-projection-server.service")
    puller = _directives("cra-monitoring-projection-pull@.service")
    assert "ReadWritePaths=" not in server
    assert "ReadWritePaths=/var/lib/stream-recovery-control/monitoring-inbox" in puller
    assert "InaccessiblePaths=/var/lib/stream-recovery-control/cra" in puller
    assert "ReadWritePaths=/var/lib/stream-recovery-control/cra" not in puller
    assert "dell" not in puller.lower()
    assert "effect" not in puller.lower()
    runtime = _directives("cra-runtime-no-action@.service")
    assert "InaccessiblePaths=/etc/stream-recovery-control/cra-monitoring-client-key.pem" in runtime
    assert "InaccessiblePaths=/etc/stream-recovery-control/monitoring-ed25519-private.pem" in server


def test_resilient_soak_cannot_see_the_real_central_database_and_allows_absent_sidecars() -> None:
    soak = _directives("cra-resilient-status-soak@.service")
    gate = _directives("cra-resilient-status-soak-gate@.service")
    watchdog = _directives("cra-resilient-status-soak-watchdog@.service")

    for unit in (soak, gate, watchdog):
        assert "InaccessiblePaths=/var/lib/stream-recovery-control/cra/central.db" in unit
        assert "-/var/lib/stream-recovery-control/cra/central.db-wal" in unit
        assert "-/var/lib/stream-recovery-control/cra/central.db-shm" in unit
        assert "InaccessiblePaths=/var/lib/stream-recovery-control/cra/central.sqlite3" not in unit
        assert "ReadWritePaths=/var/lib/stream-recovery-control/cra/releases/%i" in unit
        assert "RestrictAddressFamilies=AF_UNIX" in unit
    assert "--watchdog" in watchdog
    assert "cra-resilient-status-soak-watchdog@%i.service" in _directives("cra-resilient-status-soak-watchdog@.timer")


def test_monitoring_projection_services_use_a_dedicated_identity() -> None:
    for name in ("monitoring-v4-cra-projection.service", "monitoring-v4-cra-projection-server.service"):
        unit = _directives(name)
        assert "User=stream-monitoring-projection" in unit
        assert "Group=stream-monitoring-projection" in unit
        assert "User=yuki" not in unit


def test_projection_and_pull_timers_are_bounded_and_explicit() -> None:
    projection_timer = _text("monitoring-v4-cra-projection.timer")
    assert "OnUnitActiveSec=2s" in projection_timer
    assert "AccuracySec=200ms" in projection_timer
    assert "RandomizedDelaySec=0" in projection_timer

    pull_timer = _text("cra-monitoring-projection-pull@.timer")
    assert "OnBootSec=1s" in pull_timer
    assert "OnUnitInactiveSec=1s" in pull_timer
    assert "OnUnitActiveSec=" not in pull_timer
    assert "AccuracySec=100ms" in pull_timer
    assert "RandomizedDelaySec=0" in pull_timer

    pull_service = _text("cra-monitoring-projection-pull@.service")
    assert "StartLimitIntervalSec=10s" in pull_service
    assert "StartLimitBurst=60" in pull_service


def test_cra_units_bind_runtime_and_config_to_the_same_immutable_release_instance() -> None:
    runtime = _directives("cra-runtime-no-action@.service")
    puller = _directives("cra-monitoring-projection-pull@.service")
    timer = _directives("cra-monitoring-projection-pull@.timer")
    for unit in (runtime, puller):
        assert "ConditionPathExists=/opt/stream-recovery-control/releases/%i/release_manifest.json" in unit
        assert "ConditionPathExists=/opt/stream-recovery-control/runtimes/%i/runtime_manifest.json" in unit
        assert "/opt/stream-recovery-control/releases/%i/" in unit
        assert "/opt/stream-recovery-control/runtimes/%i/" in unit
        assert "/etc/stream-recovery-control/releases/%i/" in unit
        assert "Environment=CRA_IMMUTABLE_RELEASE_ID=%i" in unit
        assert "/opt/stream-recovery-control/.venv" not in unit
    assert "Unit=cra-monitoring-projection-pull@%i.service" in timer

    runtime_config = json.loads(_text("cra-runtime.example.json"))
    pull_config = json.loads(_text("cra-monitoring-pull.example.json"))
    assert runtime_config["cycle_interval_seconds"] == 1
    assert pull_config["timeout_seconds"] == 0.5
    assert pull_config["schema"] == "cra.monitoring_projection_pull.v3"
    assert pull_config["transient_retry_delays_seconds"] == [0.1, 0.2, 0.4, 0.8]
    assert runtime_config["monitoring"]["maximum_check_age_seconds"] == 180
    assert pull_config["maximum_check_age_seconds"] == 180
    assert runtime_config["runtime_release_id"] == pull_config["puller_release_id"] == "replace-with-immutable-release-id"
    assert runtime_config["database"] == "/var/lib/stream-recovery-control/cra/central.db"
    assert runtime_config["lock_file"] == "/var/lib/stream-recovery-control/cra/cra-runtime.lock"
    for path in (
        runtime_config["status_file"],
        runtime_config["monitoring"]["projection_file"],
        pull_config["projection_file"],
        pull_config["status_file"],
        runtime_config["migration"],
        runtime_config["monitoring"]["schema_file"],
        pull_config["projection_schema_file"],
    ):
        assert "/releases/replace-with-immutable-release-id/" in path


def test_soak_collector_uses_the_operator_ssh_identity_and_terminal_stop_marker() -> None:
    collector = _directives("cra-no-action-soak-collector@.service")

    assert "User=yuki" in collector
    assert "Group=yuki" in collector
    assert "ConditionPathExists=!/var/lib/cra-no-action-soak/%i/terminal-failure.json" not in collector
    assert "--terminal-output /var/lib/cra-no-action-soak/%i/terminal-failure.json" in collector
