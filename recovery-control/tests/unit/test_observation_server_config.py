from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from cra_dell_recovery.release_identity import RELEASE_ID_ENVIRONMENT
from dell_recovery_agent.observation_server import _load_server_config, _safe_reason_code
from monitoring_projection.server import _read_config


def _write(path: Path, value: dict[str, object]) -> Path:
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")
    os.chmod(path, 0o600)
    return path


def test_dell_server_config_requires_an_exact_private_lan_bind(tmp_path: Path) -> None:
    base: dict[str, object] = {
        "schema": "cra_dell_recovery.observation_server_config.v1",
        "publisher_config": "/safe/publisher.json",
        "tls_certificate": "/safe/server.pem",
        "tls_private_key": "/safe/server-key.pem",
        "client_ca_certificate": "/safe/client-ca.pem",
        "listen_host": "192.168.10.20",
        "listen_port": 9445,
        "maximum_response_bytes": 2097152,
    }
    assert _load_server_config(_write(tmp_path / "valid.json", base))["listen_host"] == "192.168.10.20"
    for index, address in enumerate(("0.0.0.0", "127.0.0.1", "8.8.8.8", "not-an-address")):
        with pytest.raises(ValueError, match="DELL_OBSERVATION_SERVER_LISTEN_ADDRESS"):
            _load_server_config(_write(tmp_path / f"invalid-{index}.json", {**base, "listen_host": address}))


def test_dell_server_exposes_only_machine_safe_failure_reasons() -> None:
    assert _safe_reason_code(ValueError("DELL_OBSERVATION_MAINTENANCE_NOT_FRESH")) == "DELL_OBSERVATION_MAINTENANCE_NOT_FRESH"
    assert _safe_reason_code(ValueError("secret path: /private/key")) == "DELL_OBSERVATION_BUILD_FAILED"
    assert _safe_reason_code(OSError("raw operating system detail")) == "DELL_OBSERVATION_BUILD_FAILED"


def test_dell_server_v2_binds_immutable_release_and_status_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id = "dell-observation-release-a"
    monkeypatch.setenv(RELEASE_ID_ENVIRONMENT, release_id)
    value: dict[str, object] = {
        "schema": "cra_dell_recovery.observation_server_config.v2",
        "server_release_id": release_id,
        "publisher_config": "/safe/publisher.json",
        "published_observation_file": "/safe/observation-current.json",
        "host_status_file": "/safe/host-status.json",
        "host_status_host_id": "dell-stream-runtime",
        "server_resource_status_file": "/safe/server-resource-status.json",
        "tls_certificate": "/safe/server.pem",
        "tls_private_key": "/safe/server-key.pem",
        "client_ca_certificate": "/safe/client-ca.pem",
        "listen_host": "192.168.10.20",
        "listen_port": 9445,
        "maximum_response_bytes": 2097152,
    }

    assert _load_server_config(_write(tmp_path / "v2.json", value))["server_release_id"] == release_id
    with pytest.raises(ValueError, match="DELL_OBSERVATION_SERVER_RUNTIME_RELEASE_MISMATCH"):
        _load_server_config(_write(tmp_path / "wrong-v2.json", {**value, "server_release_id": "other-release"}))


def test_projection_server_config_binds_release_and_private_lan_address(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_id = "arena-cra-projection-release-a"
    monkeypatch.setenv(RELEASE_ID_ENVIRONMENT, release_id)
    base: dict[str, object] = {
        "schema": "monitoring_v4.cra_projection_server_config.v1",
        "server_release_id": release_id,
        "projection_file": "/safe/latest.json",
        "tls_certificate": "/safe/server.pem",
        "tls_private_key": "/safe/server-key.pem",
        "client_ca_certificate": "/safe/client-ca.pem",
        "listen_host": "192.168.10.21",
        "listen_port": 9444,
        "maximum_response_bytes": 2097152,
    }
    assert _read_config(_write(tmp_path / "valid.json", base))["server_release_id"] == release_id
    with pytest.raises(ValueError, match="PROJECTION_SERVER_RUNTIME_RELEASE_MISMATCH"):
        _read_config(_write(tmp_path / "wrong-release.json", {**base, "server_release_id": "other-release"}))
    with pytest.raises(ValueError, match="PROJECTION_SERVER_LISTEN_ADDRESS_NOT_PRIVATE_LAN"):
        _read_config(_write(tmp_path / "wildcard.json", {**base, "listen_host": "0.0.0.0"}))

    v2 = {
        **base,
        "schema": "monitoring_v4.cra_projection_server_config.v2",
        "host_status_file": "/safe/host-status.json",
        "host_status_host_id": "arena-monitoring-facts",
        "server_resource_status_file": "/safe/server-resource-status.json",
    }
    assert _read_config(_write(tmp_path / "valid-v2.json", v2))["host_status_host_id"] == "arena-monitoring-facts"
