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
from cra_dell_recovery.errors import SignatureValidationError
from cra_no_action_soak import recovery_observer as observer
from cra_no_action_soak.append_only_jsonl import _serialized_line
from cra_no_action_soak.host_status import atomic_write_json
from cra_no_action_soak.recovery_facts import HostBinding, admit_facts, source_hash

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


def test_history_profile_appends_signed_packets_before_latest_projection(config: tuple[dict[str, Any], KeyRing]) -> None:
    value, keys = config
    value["schema"] = observer.HISTORY_CONFIG_SCHEMA
    value["history_file"] = str(Path(value["state_file"]).with_name("history.jsonl"))
    first = observer.observe(value, now=NOW)
    second = observer.observe(value, now=NOW + timedelta(seconds=15))
    rows = [json.loads(line) for line in Path(value["history_file"]).read_text().splitlines()]
    assert rows == [first, second]
    assert [
        admit_facts(row, binding=HostBinding(**value["binding"]), keys=keys, now=NOW + timedelta(seconds=15))["sequence"] for row in rows
    ] == [1, 2]


def test_history_profile_finishes_reserved_packet_after_append_failure(
    config: tuple[dict[str, Any], KeyRing], monkeypatch: pytest.MonkeyPatch
) -> None:
    value, _ = config
    value["schema"] = observer.HISTORY_CONFIG_SCHEMA
    value["history_file"] = str(Path(value["state_file"]).with_name("history.jsonl"))
    append = observer._append_line

    def fail_history(*args: Any, **kwargs: Any) -> None:
        raise OSError("isolated history write failure")

    monkeypatch.setattr(observer, "_append_line", fail_history)
    with pytest.raises(OSError, match="history write failure"):
        observer.observe(value, now=NOW)
    assert not Path(value["output_file"]).exists()
    monkeypatch.setattr(observer, "_append_line", append)
    next_packet = observer.observe(value, now=NOW + timedelta(seconds=15))
    rows = [json.loads(line) for line in Path(value["history_file"]).read_text().splitlines()]
    assert [row["sequence"] for row in rows] == [1, 2]
    assert rows[-1] == next_packet
    assert json.loads(Path(value["output_file"]).read_text()) == next_packet
    assert json.loads(Path(value["state_file"]).read_text())["pending_packet"] is None


def test_history_profile_finishes_reserved_packet_after_projection_failure(
    config: tuple[dict[str, Any], KeyRing], monkeypatch: pytest.MonkeyPatch
) -> None:
    value, _ = config
    value["schema"] = observer.HISTORY_CONFIG_SCHEMA
    value["history_file"] = str(Path(value["state_file"]).with_name("history.jsonl"))
    output = Path(value["output_file"])
    original = observer.atomic_write_json

    def fail_output(path: Path, data: dict[str, Any]) -> None:
        if path == output:
            raise OSError("isolated projection failure")
        original(path, data)

    monkeypatch.setattr(observer, "atomic_write_json", fail_output)
    with pytest.raises(OSError, match="projection failure"):
        observer.observe(value, now=NOW)
    assert not output.exists()
    assert [json.loads(line)["sequence"] for line in Path(value["history_file"]).read_text().splitlines()] == [1]
    monkeypatch.setattr(observer, "atomic_write_json", original)
    assert observer.observe(value, now=NOW + timedelta(seconds=15))["sequence"] == 2
    assert [json.loads(line)["sequence"] for line in Path(value["history_file"]).read_text().splitlines()] == [1, 2]


def test_history_profile_rejects_changed_pending_packet(
    config: tuple[dict[str, Any], KeyRing], monkeypatch: pytest.MonkeyPatch
) -> None:
    value, _ = config
    value["schema"] = observer.HISTORY_CONFIG_SCHEMA
    value["history_file"] = str(Path(value["state_file"]).with_name("history.jsonl"))
    original = observer._append_line
    monkeypatch.setattr(observer, "_append_line", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk failure")))
    with pytest.raises(OSError):
        observer.observe(value, now=NOW)
    monkeypatch.setattr(observer, "_append_line", original)
    state_path = Path(value["state_file"])
    state = json.loads(state_path.read_text())
    state["pending_packet"]["facts"]["publication"]["status"] = "UNKNOWN"
    atomic_write_json(state_path, state)
    with pytest.raises(SignatureValidationError, match="digest mismatch"):
        observer.observe(value, now=NOW + timedelta(seconds=15))
    assert not Path(value["history_file"]).exists()


def test_history_profile_completes_exact_partial_pending_packet_without_truncation(
    config: tuple[dict[str, Any], KeyRing], monkeypatch: pytest.MonkeyPatch,
) -> None:
    value, _ = config
    value["schema"] = observer.HISTORY_CONFIG_SCHEMA
    history = Path(value["state_file"]).with_name("history.jsonl")
    value["history_file"] = str(history)
    original = observer._append_line

    def partial_write(path: Path, packet: dict[str, Any], **_kwargs: Any) -> None:
        path.write_bytes(_serialized_line(packet)[:17])
        raise OSError("isolated short write")

    monkeypatch.setattr(observer, "_append_line", partial_write)
    with pytest.raises(OSError, match="short write"):
        observer.observe(value, now=NOW)
    prefix = history.read_bytes()
    pending = json.loads(Path(value["state_file"]).read_text())["pending_packet"]
    assert _serialized_line(pending).startswith(prefix)
    monkeypatch.setattr(observer, "_append_line", original)
    next_packet = observer.observe(value, now=NOW + timedelta(seconds=15))
    lines = history.read_bytes().splitlines()
    assert lines[0] == _serialized_line(pending).rstrip(b"\n")
    assert [json.loads(line)["sequence"] for line in lines] == [1, 2]
    assert next_packet["sequence"] == 2
    assert json.loads(Path(value["state_file"]).read_text())["pending_packet"] is None


def test_history_profile_keeps_conflicting_partial_bytes_unchanged(
    config: tuple[dict[str, Any], KeyRing], monkeypatch: pytest.MonkeyPatch,
) -> None:
    value, _ = config
    value["schema"] = observer.HISTORY_CONFIG_SCHEMA
    history = Path(value["state_file"]).with_name("history.jsonl")
    value["history_file"] = str(history)

    def conflicting_write(path: Path, _packet: dict[str, Any], **_kwargs: Any) -> None:
        path.write_bytes(b'{"different":')
        raise OSError("isolated conflict")

    monkeypatch.setattr(observer, "_append_line", conflicting_write)
    with pytest.raises(OSError):
        observer.observe(value, now=NOW)
    before = history.read_bytes()
    with pytest.raises(ValueError, match="PENDING_PREFIX_CONFLICT"):
        observer.observe(value, now=NOW + timedelta(seconds=15))
    assert history.read_bytes() == before


def test_history_profile_does_not_require_formal_admission_of_prior_facts(
    config: tuple[dict[str, Any], KeyRing], monkeypatch: pytest.MonkeyPatch
) -> None:
    value, keys = config
    value["schema"] = observer.HISTORY_CONFIG_SCHEMA
    value["history_file"] = str(Path(value["state_file"]).with_name("history.jsonl"))
    original = observer.build_facts
    monkeypatch.setattr(
        observer,
        "build_facts",
        lambda *_args, **_kwargs: (
            {"publication": {"status": "UNKNOWN"}},
            {"read_errors": source_hash({"names": []}), "invalid key": "a" * 64},
        ),
    )
    prior = observer.observe(value, now=NOW)
    with pytest.raises(ValueError, match="SOURCE_HASHES_INVALID"):
        admit_facts(prior, binding=HostBinding(**value["binding"]), keys=keys, now=NOW)
    monkeypatch.setattr(observer, "build_facts", original)
    assert observer.observe(value, now=NOW + timedelta(seconds=15))["sequence"] == 2
