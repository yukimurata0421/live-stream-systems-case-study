from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from cra_no_action_soak import recovery_transport as module


def _config(host_id: str, mode: str) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema": module.SCHEMA,
        "mode": mode,
        "release_id": "recovery-transport-test-v1",
        "host_id": host_id,
        "host_contract_file": "/etc/test/host-contract.json",
        "tls_certificate": "/etc/test/tls.crt",
        "tls_private_key": "/etc/test/tls.key",
        "peer_ca_certificate": "/etc/test/ca.crt",
    }
    permitted = {
        ("dell-stream-runtime", "serve"): {"dell"},
        ("arena-monitoring-facts", "serve"): {"dell", "arena"},
        ("arena-monitoring-facts", "pull"): {"dell"},
        ("cra-01-central-authority", "pull"): {"dell", "arena"},
    }
    roles = permitted[(host_id, mode)]
    if mode == "serve":
        value.update(
            {
                "listen_host": "192.0.2.3",
                "listen_port": 24443,
                "packets": {role: f"/run/test/{role}.json" for role in roles},
            }
        )
    else:
        value.update(
            {
                "sources": {
                    role: {
                        "url": f"https://192.0.2.3:24443{module.ROUTES[role]}",
                        "output_file": f"/run/test/{role}.json",
                    }
                    for role in roles
                },
                "status_file": "/run/test/status.json",
            }
        )
    return value


def _validate(value: dict[str, Any], monkeypatch: pytest.MonkeyPatch, *, contract_host: str | None = None) -> None:
    observed: list[dict[str, Any]] = []

    def read(path: Path, **kwargs: Any) -> bytes:
        observed.append({"path": path, **kwargs})
        return json.dumps({"host_id": contract_host or value["host_id"]}).encode()

    monkeypatch.setattr(module, "read_regular_bytes", read)
    module.validate_config(value)
    assert observed == [
        {
            "path": Path(value["host_contract_file"]),
            "maximum_bytes": module.MAXIMUM_BYTES,
            "expected_owner_uid": 0,
        }
    ]


@pytest.mark.parametrize(
    ("host_id", "mode"),
    [
        ("dell-stream-runtime", "serve"),
        ("arena-monitoring-facts", "serve"),
        ("arena-monitoring-facts", "pull"),
        ("cra-01-central-authority", "pull"),
    ],
)
def test_exact_host_mode_role_matrix_is_accepted(host_id: str, mode: str, monkeypatch: pytest.MonkeyPatch) -> None:
    _validate(_config(host_id, mode), monkeypatch)


@pytest.mark.parametrize(
    ("host_id", "mode"),
    [
        ("dell-stream-runtime", "pull"),
        ("cra-01-central-authority", "serve"),
        ("unknown", "pull"),
    ],
)
def test_unowned_host_mode_pair_is_rejected(host_id: str, mode: str, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("arena-monitoring-facts" if mode == "pull" else "dell-stream-runtime", mode)
    value["host_id"] = host_id
    with pytest.raises(ValueError, match="HOST_ROLE_BOUNDARY"):
        _validate(value, monkeypatch)


def test_root_owned_contract_host_binding_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("dell-stream-runtime", "serve")
    with pytest.raises(ValueError, match="HOST_MISMATCH"):
        _validate(value, monkeypatch, contract_host="other-host")


def test_systemd_release_identity_mismatch_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("dell-stream-runtime", "serve")
    monkeypatch.setenv("CRA_IMMUTABLE_RELEASE_ID", "different-release")
    with pytest.raises(ValueError, match="RELEASE_MISMATCH"):
        _validate(value, monkeypatch)


@pytest.mark.parametrize("port", [1023, 65536, True, 1.5, "24443"])
def test_listener_port_boundaries_reject_invalid_values(port: object, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("dell-stream-runtime", "serve")
    value["listen_port"] = port
    with pytest.raises(ValueError, match="PORT_INVALID"):
        _validate(value, monkeypatch)


@pytest.mark.parametrize("port", [1024, 65535])
def test_listener_port_boundaries_accept_endpoints(port: int, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("dell-stream-runtime", "serve")
    value["listen_port"] = port
    _validate(value, monkeypatch)


@pytest.mark.parametrize("address", ["0.0.0.0", "224.0.0.1", "8.8.8.8", "fd00::1"])
def test_listener_must_be_a_specific_private_ipv4(address: str, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("dell-stream-runtime", "serve")
    value["listen_host"] = address
    with pytest.raises(ValueError, match="PRIVATE_LISTENER_REQUIRED"):
        _validate(value, monkeypatch)


@pytest.mark.parametrize(
    "url",
    [
        "http://192.0.2.3/v1/recovery-facts/dell",
        "https://192.0.2.3/v1/recovery-facts/arena",
        "https://192.0.2.3/v1/recovery-facts/dell?fresh=1",
        "https://user@192.0.2.3/v1/recovery-facts/dell",
        "https://192.0.2.3/v1/recovery-facts/dell#fragment",
    ],
)
def test_pull_url_is_exact_https_role_route_without_metadata(url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("arena-monitoring-facts", "pull")
    value["sources"]["dell"]["url"] = url
    with pytest.raises(ValueError, match="URL_INVALID"):
        _validate(value, monkeypatch)


@pytest.mark.parametrize("address", ["0.0.0.0", "224.0.0.1", "8.8.8.8"])
def test_pull_peer_must_be_private_and_specific(address: str, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("arena-monitoring-facts", "pull")
    value["sources"]["dell"]["url"] = f"https://{address}{module.ROUTES['dell']}"
    with pytest.raises(ValueError, match="PRIVATE_PEER_REQUIRED"):
        _validate(value, monkeypatch)


def test_output_paths_must_be_distinct(monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("cra-01-central-authority", "pull")
    value["sources"]["arena"]["output_file"] = value["sources"]["dell"]["output_file"]
    with pytest.raises(ValueError, match="OUTPUT_INVALID"):
        _validate(value, monkeypatch)


def test_output_must_not_overlap_contract_or_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("arena-monitoring-facts", "pull")
    value["sources"]["dell"]["output_file"] = value["tls_private_key"]
    with pytest.raises(ValueError, match="PATH_COLLISION"):
        _validate(value, monkeypatch)


@pytest.mark.parametrize("field", ["host_contract_file", "tls_certificate", "tls_private_key", "peer_ca_certificate"])
def test_security_inputs_must_be_absolute(field: str, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("dell-stream-runtime", "serve")
    value[field] = "relative/path"
    with pytest.raises(ValueError, match="PATH_INVALID"):
        _validate(value, monkeypatch)


def test_extra_configuration_field_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    value = copy.deepcopy(_config("dell-stream-runtime", "serve"))
    value["command_endpoint"] = "https://192.0.2.3/v1/commands"
    with pytest.raises(ValueError, match="CONFIG_INVALID"):
        _validate(value, monkeypatch)


def test_history_profile_requires_fixed_same_peer_routes_and_distinct_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    serve = _config("dell-stream-runtime", "serve")
    serve["schema"] = module.HISTORY_SCHEMA
    serve["histories"] = {"dell": "/run/test/dell-history.jsonl"}
    _validate(serve, monkeypatch)
    serve["histories"]["dell"] = serve["packets"]["dell"]
    with pytest.raises(ValueError, match="HISTORY_PATH_COLLISION"):
        _validate(serve, monkeypatch)

    pull = _config("arena-monitoring-facts", "pull")
    pull["schema"] = module.HISTORY_SCHEMA
    pull["sources"]["dell"].update(
        history_url="https://192.0.2.3:24443/v1/recovery-history/dell",
        history_output_file="/run/test/dell-history.jsonl",
    )
    _validate(pull, monkeypatch)
    pull["sources"]["dell"]["history_url"] = "https://192.0.2.4:24443/v1/recovery-history/dell"
    with pytest.raises(ValueError, match="HISTORY_URL_INVALID"):
        _validate(pull, monkeypatch)


def test_history_page_reaches_retained_packets_beyond_old_tail_scan_limit(tmp_path: Path) -> None:
    archive = tmp_path / "history.jsonl"
    with archive.open("w") as stream:
        for sequence in range(1, 1025):
            stream.write(json.dumps({"role": "dell", "sequence": sequence, "padding": "x" * 20_000}) + "\n")
    assert archive.stat().st_size > 16 * 1024 * 1024
    first = module.history_page(archive, role="dell", after=0)
    middle = module.history_page(archive, role="dell", after=512)
    end = module.history_page(archive, role="dell", after=1024)
    assert [packet["sequence"] for packet in first["packets"]] == list(range(1, 17))
    assert [packet["sequence"] for packet in middle["packets"]] == list(range(513, 529))
    assert first["more"] and middle["more"] and not end["more"]
    assert end["packets"] == []


def test_history_page_rejects_missing_sequence_at_requested_position(tmp_path: Path) -> None:
    archive = tmp_path / "history.jsonl"
    archive.write_text('::{bad}\n')
    with pytest.raises(ValueError):
        module.history_page(archive, role="dell", after=0)
    archive.write_text('{"role":"dell","sequence":1}\n{"role":"dell","sequence":3}\n')
    with pytest.raises(ValueError, match="SEQUENCE_GAP"):
        module.history_page(archive, role="dell", after=1)


@pytest.mark.parametrize("packets", [{}, {"unknown": "/run/test/value.json"}, []])
def test_serve_packet_role_mapping_must_match_host_contract(packets: object, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("dell-stream-runtime", "serve")
    value["packets"] = packets
    with pytest.raises(ValueError, match="HOST_ROLE_BOUNDARY"):
        _validate(value, monkeypatch)


def test_serve_packet_path_must_be_absolute(monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("dell-stream-runtime", "serve")
    value["packets"]["dell"] = "relative.json"
    with pytest.raises(ValueError, match="PATH_INVALID"):
        _validate(value, monkeypatch)


@pytest.mark.parametrize("sources", [{}, {"unknown": {}}, []])
def test_pull_source_role_mapping_must_match_host_contract(sources: object, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("arena-monitoring-facts", "pull")
    value["sources"] = sources
    with pytest.raises(ValueError, match="HOST_ROLE_BOUNDARY"):
        _validate(value, monkeypatch)


@pytest.mark.parametrize("source", [None, {}, {"url": "https://192.0.2.3/"}, {"url": "x", "output_file": "x", "extra": 1}])
def test_pull_source_entry_has_an_exact_shape(source: object, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("arena-monitoring-facts", "pull")
    value["sources"]["dell"] = source
    with pytest.raises(ValueError, match="SOURCE_INVALID"):
        _validate(value, monkeypatch)


@pytest.mark.parametrize("mode", ["serve", "pull"])
def test_tls_context_enforces_certificate_roles_and_tls13(mode: str, monkeypatch: pytest.MonkeyPatch) -> None:
    value = _config("dell-stream-runtime" if mode == "serve" else "arena-monitoring-facts", mode)
    reads: list[tuple[Path, dict[str, Any]]] = []
    leaf: list[tuple[Path, str]] = []
    ca: list[Path] = []

    class Context:
        minimum_version: object = None
        verify_mode: object = None

        def load_cert_chain(self, certificate: Path, key: Path) -> None:
            self.chain = (certificate, key)

        def load_verify_locations(self, *, cafile: Path) -> None:
            self.ca = cafile

    context = Context()
    monkeypatch.setattr(module, "read_regular_bytes", lambda path, **kwargs: reads.append((path, kwargs)) or b"test")
    monkeypatch.setattr(module, "validate_leaf_certificate", lambda path, _code, *, usage: leaf.append((path, usage)))
    monkeypatch.setattr(module, "validate_ca_certificate", lambda path, _code: ca.append(path))
    monkeypatch.setattr(module.ssl, "SSLContext", lambda _protocol: context)
    monkeypatch.setattr(module.ssl, "create_default_context", lambda *, cafile: context)

    assert module.tls_context(value) is context
    assert leaf == [(Path(value["tls_certificate"]), "server" if mode == "serve" else "client")]
    assert ca == [Path(value["peer_ca_certificate"])]
    assert reads[0] == (Path(value["tls_private_key"]), {"maximum_bytes": module.MAXIMUM_BYTES, "secret": True})
    assert context.minimum_version == module.ssl.TLSVersion.TLSv1_3
    if mode == "serve":
        assert context.verify_mode == module.ssl.CERT_REQUIRED
        assert context.ca == Path(value["peer_ca_certificate"])
