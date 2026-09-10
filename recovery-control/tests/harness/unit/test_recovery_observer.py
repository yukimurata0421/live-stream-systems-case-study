from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_dell_recovery.canonical import KeyRing
from cra_no_action_soak import recovery_observer as observer
from cra_no_action_soak.host_status import atomic_write_json
from cra_no_action_soak.recovery_facts import HostBinding, admit_facts

NOW = datetime(2026, 9, 4, tzinfo=UTC)


@pytest.fixture
def config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, Any], KeyRing]:
    private = Ed25519PrivateKey.generate()
    key = tmp_path / "key.pem"
    key.write_bytes(private.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    key.chmod(0o600)
    contract = tmp_path / "host-contract.json"
    atomic_write_json(contract, {"host_id": "pi-test-host"})
    original_stat = Path.stat

    def metadata(path: Path, **kwargs: Any) -> Any:
        if path == contract:
            return SimpleNamespace(st_uid=0, st_mode=0o100644)
        return original_stat(path, **kwargs)

    monkeypatch.setattr(Path, "stat", metadata)
    sources = {name: str(tmp_path / (name + ".json")) for name in observer.SOURCE_NAMES["raspi"]}
    raw = {
        "local_artifact": {"generated_at": NOW.timestamp()},
        "remote_artifact": {"generated_at": NOW.timestamp()},
        "publisher": {"completed_at": NOW.isoformat(), "status": "SUCCEEDED"},
        "network": {"state": "UP"},
    }
    for name, value in raw.items():
        atomic_write_json(Path(sources[name]), value)
    value = {
        "schema": observer.CONFIG_SCHEMA,
        "binding": {
            "role": "raspi",
            "host_id": "pi-test-host",
            "release_id": "candidate",
            "source_commit": "a" * 40,
            "stream_id": "public-only",
            "key_id": "pi-test-key",
        },
        "sources": sources,
        "state_file": str(tmp_path / "observer-state.json"),
        "output_file": str(tmp_path / "signed.json"),
        "signing_private_key_file": str(key),
        "host_contract_file": str(contract),
    }
    return value, KeyRing({"pi-test-key": private.public_key()})


def test_real_signing_and_durable_failure_counter(config: tuple[dict[str, Any], KeyRing]) -> None:
    value, keys = config
    first = observer.observe(value, now=NOW)
    assert admit_facts(first, binding=HostBinding(**value["binding"]), keys=keys, now=NOW)["sequence"] == 1
    source = Path(value["sources"]["remote_artifact"])
    original = source.read_bytes()
    source.unlink()
    failed = observer.observe(value, now=NOW + timedelta(seconds=15))
    assert failed["source_failure_count"] == 1
    source.write_bytes(original)
    source.chmod(0o644)
    final = observer.observe(value, now=NOW + timedelta(seconds=30))
    assert final["sequence"] == 3 and final["source_failure_count"] == 1
    assert final["facts"]["publication"]["status"] == "READY"


def test_output_failure_is_reserved_and_not_reused(config: tuple[dict[str, Any], KeyRing], monkeypatch: pytest.MonkeyPatch) -> None:
    value, _ = config
    output = Path(value["output_file"])
    real_write = observer.atomic_write_json

    def fail_output(path: Path, data: dict[str, Any]) -> None:
        if path == output:
            raise OSError("isolated output failure")
        real_write(path, data)

    monkeypatch.setattr(observer, "atomic_write_json", fail_output)
    with pytest.raises(OSError):
        observer.observe(value, now=NOW)
    monkeypatch.setattr(observer, "atomic_write_json", real_write)
    packet = observer.observe(value, now=NOW + timedelta(seconds=15))
    assert packet["sequence"] == 2 and packet["source_failure_count"] == 1


def test_contract_mismatch_stops_before_writes(config: tuple[dict[str, Any], KeyRing]) -> None:
    value, _ = config
    value["binding"]["host_id"] = "different-host"
    with pytest.raises(ValueError, match="PHYSICAL_HOST"):
        observer.observe(value, now=NOW)
    assert not Path(value["state_file"]).exists()


@pytest.mark.parametrize("change", ["missing-source", "relative-path", "extra-field", "collision"])
def test_observer_rejects_ambiguous_config_before_writes(config: tuple[dict[str, Any], KeyRing], change: str) -> None:
    value, _ = config
    if change == "missing-source":
        del value["sources"]["remote_artifact"]
    elif change == "relative-path":
        value["sources"]["network"] = "relative.json"
    elif change == "extra-field":
        value["command_delivery_enabled"] = True
    else:
        value["output_file"] = str(Path(value["state_file"]).with_suffix(".lock"))
    with pytest.raises(ValueError):
        observer.observe(value, now=NOW)
    assert not Path(value["state_file"]).exists()


def test_state_removal_or_boot_rotation_requires_operator_decision(config: tuple[dict[str, Any], KeyRing]) -> None:
    value, _ = config
    observer.observe(value, now=NOW)
    state_path = Path(value["state_file"])
    state = json.loads(state_path.read_text())
    state["host_boot_id"] = "different-boot"
    atomic_write_json(state_path, state)
    with pytest.raises(ValueError, match="BOOT_ROTATION"):
        observer.observe(value, now=NOW + timedelta(seconds=15))
    state_path.unlink()
    with pytest.raises(ValueError, match="STATE_MISSING"):
        observer.observe(value, now=NOW + timedelta(seconds=30))
