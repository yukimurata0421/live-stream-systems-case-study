from __future__ import annotations

import copy
import json
import os
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from cra_authority.recovery_safety import query_only
from cra_dell_recovery.recovery_history import empty_policy, history_hash
from cra_dell_recovery.time import isoformat_utc
from cra_no_action_soak import recovery_observer as observer
from cra_no_action_soak.host_status import read_regular_bytes
from cra_no_action_soak.recovery_facts import HostBinding, runtime_evidence_fact, source_hash
from cra_no_action_soak.recovery_live import observation
from runtime_boundary.recovery_evidence import effect_safety, owner_evidence

NOW = datetime(2026, 9, 4, tzinfo=UTC)
AT = isoformat_utc(NOW)
TARGET = {
    "host_id": "dell-yuki",
    "host_boot_id": "boot",
    "namespace": "stream-v3",
    "pod_uid": "pod",
    "container_name": "stream-engine",
    "container_id": "containerd://container",
    "ffmpeg_pid": 123,
    "ffmpeg_generation": "generation",
}
BINDING = HostBinding("dell", "dell-stream-runtime", "observer-release", "a" * 40, "stream", "key")
RUNTIME = {"release_id": "runtime-release", "source_commit": "b" * 40}


@pytest.fixture
def owner_export(tmp_path: Path) -> dict[str, Any]:
    path = tmp_path / "ledger.sqlite3"
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript("""
        CREATE TABLE effect_scope_fences(
            effect_scope_id text, state text, physical_attempt_count integer, owner_request_id text, identity_json text);
        CREATE TABLE typed_effect_requests(producer_id text, operation text);
        CREATE TABLE effect_requests(producer_id text, operation text);
        CREATE TABLE effect_reconciliations(
            effect_scope_id text, resolution text, evidence_json text, recorded_at text, reconciliation_id text);
        INSERT INTO effect_scope_fences VALUES('scope', 'RETIRED_TARGET_OUTCOME_UNKNOWN', 1, 'request', '{}');
        INSERT INTO effect_requests VALUES('unexpected-legacy-owner', 'restart_host');
        """)
    original = path.read_bytes()
    with query_only(path) as db:
        value = owner_evidence(
            db,
            target_identity=TARGET,
            **RUNTIME,
            host_id=BINDING.host_id,
            stream_id="stream",
            allowed_producers=["independent-controller"],
            now=NOW,
            proc_root=tmp_path / "absent-proc",
        )
    assert path.read_bytes() == original
    return value


def admit(value: dict[str, Any], *, now: datetime = NOW) -> tuple[dict[str, Any], dict[str, Any]]:
    return runtime_evidence_fact(
        value,
        target={
            "status": "VALID",
            "target_identity": TARGET,
            "observed_at": AT,
            "valid_until": isoformat_utc(NOW + timedelta(seconds=120)),
        },
        binding=BINDING,
        runtime_binding={**RUNTIME, "target_host_id": TARGET["host_id"]},
        now=now,
    )


def test_owner_export_keeps_retirement_unknown_and_checks_legacy_requests(owner_export: dict[str, Any]) -> None:
    effects, activation = admit(owner_export)
    assert effects["unresolved_scope_count"] == effects["physical_attempt_count"] == effects["unauthorized_effect_count"] == 1
    assert effects["observed_at"] == AT
    assert activation == {"bounded_termination_enabled": None, "rw_timeout_enabled": None}
    assert "database_file" not in json.dumps(owner_export)
    assert "environ" not in json.dumps(owner_export)


@pytest.mark.parametrize("target_host_id", [None, "another-host", "dell-stream-runtime"])
def test_protocol_host_mapping_must_be_explicit_and_exact(owner_export: dict[str, Any], target_host_id: str | None) -> None:
    runtime = {**RUNTIME, **({"target_host_id": target_host_id} if target_host_id is not None else {})}
    with pytest.raises(ValueError, match="BINDING_INVALID"):
        runtime_evidence_fact(
            owner_export,
            target={"status": "VALID", "target_identity": TARGET, "observed_at": AT, "valid_until": owner_export["valid_until"]},
            binding=BINDING,
            runtime_binding=runtime,
            now=NOW,
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema", "other"),
        ("host_id", "arena"),
        ("host_boot_id", "other"),
        ("release_id", "old"),
        ("source_commit", "c" * 40),
        ("stream_id", "other"),
        ("target_sha256", "c" * 64),
        ("observed_at", isoformat_utc(NOW + timedelta(seconds=1))),
        ("valid_until", AT),
        ("valid_until", isoformat_utc(NOW + timedelta(seconds=46))),
    ],
)
def test_owner_export_binding_and_times_fail_closed(owner_export: dict[str, Any], field: str, value: str) -> None:
    owner_export[field] = value
    with pytest.raises(ValueError):
        admit(owner_export)


def test_cached_export_does_not_gain_a_new_observed_at(owner_export: dict[str, Any]) -> None:
    effects, _ = admit(owner_export, now=NOW + timedelta(seconds=30))
    assert effects["observed_at"] == AT
    with pytest.raises(ValueError):
        admit(owner_export, now=NOW + timedelta(seconds=45))


@pytest.mark.parametrize("change", ["extra", "bool-counter", "missing-counter", "proof-secret", "retimed-effects", "activation-int"])
def test_malformed_or_overshared_owner_facts_are_rejected(owner_export: dict[str, Any], change: str) -> None:
    if change == "extra":
        owner_export["database_file"] = "/not-an-input.sqlite3"
    elif change == "bool-counter":
        owner_export["effects"]["physical_attempt_count"] = True
    elif change == "missing-counter":
        del owner_export["effects"]["unresolved_scope_count"]
    elif change == "proof-secret":
        owner_export["effects"]["last_reconciliation"] = {"raw_process_data": "not-exportable"}
    elif change == "retimed-effects":
        owner_export["effects"]["observed_at"] = isoformat_utc(NOW - timedelta(seconds=15))
    else:
        owner_export["activation"]["rw_timeout_enabled"] = 1
    with pytest.raises(ValueError):
        admit(owner_export)


def config(tmp_path: Path) -> dict[str, Any]:
    return {
        "schema": observer.CONFIG_SCHEMA,
        "effect_history_policy": empty_policy(),
        "binding": vars(BINDING),
        "runtime_binding": {**RUNTIME, "target_host_id": TARGET["host_id"]},
        "host_contract_file": str(tmp_path / "host.json"),
        "signing_private_key_file": str(tmp_path / "key.pem"),
        "state_file": str(tmp_path / "state.json"),
        "output_file": str(tmp_path / "signed.json"),
        "sources": {name: str(tmp_path / f"{name}.json") for name in observer.SOURCE_NAMES["dell"]},
    }


@pytest.mark.parametrize("field", ["database_file", "allowed_producers", "effect_socket", "proc_root"])
def test_dell_config_rejects_privileged_inputs_before_any_write(tmp_path: Path, field: str) -> None:
    value = config(tmp_path)
    value[field] = "/forbidden"
    with pytest.raises(ValueError, match="CONFIG_FIELDS_INVALID"):
        observer.observe(value, now=NOW)
    assert not list(tmp_path.iterdir())


def test_dell_builder_has_no_database_or_process_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    value = config(tmp_path)
    observer.validate_config(value)

    def deny_connect(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Dell observer attempted a database connection")

    monkeypatch.setattr(sqlite3, "connect", deny_connect)
    facts, hashes = observer.build_facts(value, now=NOW)
    assert facts["effects"] == {"observed_at": None, "integrity": "UNKNOWN"}
    assert facts["activation"]["rw_timeout_enabled"] is None
    assert hashes["read_errors"] != source_hash({"names": []})


def test_runtime_owner_export_cannot_be_read_from_an_observer_owned_file(tmp_path: Path) -> None:
    path = tmp_path / "owner.json"
    path.write_text("{}")
    path.chmod(0o644)
    with pytest.raises(ValueError, match="OWNER_MISMATCH"):
        read_regular_bytes(path, expected_owner_uid=path.stat().st_uid + 1)


def test_real_json_path_is_root_bound_and_preserves_source_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owner_export: dict[str, Any],
) -> None:
    value = config(tmp_path)
    raw = {
        "target": {"status": "VALID", "observed_at": AT, "valid_until": owner_export["valid_until"], "target_identity": TARGET},
        "runtime_evidence": owner_export,
        "network": {},
        "transport": {},
    }
    for name, item in raw.items():
        path = Path(value["sources"][name])
        path.write_text(json.dumps(item))
        path.chmod(0o644)
    real_fstat = os.fstat
    owner_inode = Path(value["sources"]["runtime_evidence"]).stat().st_ino

    def metadata(fd: int) -> Any:
        item = real_fstat(fd)
        if item.st_ino == owner_inode:
            return SimpleNamespace(st_uid=0, st_mode=item.st_mode, st_size=item.st_size)
        return item

    monkeypatch.setattr(os, "fstat", metadata)
    facts, _ = observer.build_facts(value, now=NOW + timedelta(seconds=15), host_boot_id="boot")
    assert facts["effects"]["observed_at"] == AT
    assert facts["effects"]["physical_attempt_count"] == 1
    facts, _ = observer.build_facts(value, now=NOW, host_boot_id="different-physical-boot")
    assert facts["effects"]["integrity"] == "UNKNOWN"
    assert facts["effects"]["observed_at"] is None


def test_owner_api_requires_consistent_readonly_transaction(tmp_path: Path) -> None:
    with closing(sqlite3.connect(tmp_path / "test.sqlite3")) as db, pytest.raises(ValueError, match="QUERY_ONLY_SNAPSHOT"):
        effect_safety(db, now=NOW, allowed_producers=["owner"])


def test_stale_unresolved_zero_cannot_complete_live_recovery() -> None:
    facts = {
        "dell": {
            "effects": {
                "observed_at": AT,
                "unresolved_scope_count": 0,
                "unresolved_scope_count_total": 0,
                "current_target_unresolved_scope_count": 0,
                "current_target_sha256": "target",
                "physical_attempt_count": 0,
                "historical_retired_unknown": [],
                "history_policy_sha256": history_hash(empty_policy()),
            }
        }
    }
    assert observation(copy.deepcopy(facts), stream_id="stream", now=NOW).unresolved_effect_count == 0
    assert observation(facts, stream_id="stream", now=NOW + timedelta(seconds=46)).unresolved_effect_count is None
