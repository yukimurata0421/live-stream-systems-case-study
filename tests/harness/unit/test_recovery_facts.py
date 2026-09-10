from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cra_authority.recovery_safety import central_safety, query_only
from cra_dell_recovery.time import isoformat_utc
from cra_no_action_soak.recovery_facts import network_fact, platform_fact, publication_fact, strict_object, transport_fact, wifi_diagnosis
from runtime_boundary.recovery_evidence import effect_safety, runtime_activation

NOW = datetime(2026, 9, 4, tzinfo=UTC)
AT = isoformat_utc(NOW)


def test_reconnect_success_is_not_miscounted_as_wan_down() -> None:
    probes = [
        {"name": name, "ok": False, "reconnect_after_failure_ok": True}
        for name in ("cloudflare_v4", "cloudflare_v6", "google_v4", "google_v6")
    ]
    assert network_fact({"ts_utc": AT, "probes": probes})["state"] == "UP"
    for probe in probes:
        probe["reconnect_after_failure_ok"] = False
    assert network_fact({"ts_utc": AT, "probes": probes})["state"] == "DOWN"
    probes[0]["ok"] = True
    assert network_fact({"ts_utc": AT, "probes": probes})["state"] == "UNKNOWN"


def test_single_anchor_failure_is_not_a_whole_wan_outage() -> None:
    probes = [{"name": name, "ok": name != "cloudflare_v4"} for name in ("cloudflare_v4", "cloudflare_v6", "google_v4", "google_v6")]
    assert network_fact({"ts_utc": AT, "probes": probes})["state"] == "UP"
    probes[-1]["name"] = "cloudflare_v4"
    assert network_fact({"ts_utc": AT, "probes": probes})["state"] == "UNKNOWN"


def test_completed_network_episode_is_preserved_without_addresses() -> None:
    started = "2026-09-03T23:59:50.000Z"
    episode = {
        "schema": "stream_v3_network_episode/v1",
        "episode_id": hashlib.sha256(f"boot:1:{started}".encode()).hexdigest()[:32],
        "sequence": 1,
        "host_boot_id": "boot",
        "state": "RECOVERED",
        "classification": "FULL_WAN",
        "started_at": started,
        "last_down_at": "2026-09-03T23:59:55.000Z",
        "recovered_at": "2026-09-04T00:00:00.000Z",
        "anchor_names": ["cloudflare_v4", "cloudflare_v6", "google_v4", "google_v6"],
        "provider_count": 2,
        "address_family_count": 2,
    }
    probes = [{"name": name, "ok": True} for name in episode["anchor_names"]]
    result = network_fact({"ts_utc": AT, "probes": probes, "network_episode": episode})
    assert result == {"state": "UP", "observed_at": AT, "episode": episode}
    assert "address" not in result["episode"]


def test_tampered_network_episode_fails_closed() -> None:
    episode = {
        "schema": "stream_v3_network_episode/v1",
        "episode_id": "0" * 32,
        "sequence": 1,
        "host_boot_id": "boot",
        "state": "RECOVERED",
        "classification": "FULL_WAN",
        "started_at": "2026-09-03T23:59:50.000Z",
        "last_down_at": "2026-09-03T23:59:55.000Z",
        "recovered_at": "2026-09-04T00:00:00.000Z",
        "anchor_names": ["cloudflare_v4", "cloudflare_v6", "google_v4", "google_v6"],
        "provider_count": 2,
        "address_family_count": 2,
    }
    probes = [{"name": name, "ok": True} for name in episode["anchor_names"]]
    with pytest.raises(ValueError, match="DIGEST_INVALID"):
        network_fact({"ts_utc": AT, "probes": probes, "network_episode": episode})


def test_healthy_availability_cannot_override_inactive_nodata() -> None:
    value = {
        "healthy": True,
        "status": "ok",
        "api_ok": True,
        "ingest_connected": True,
        "oauth_broadcast_id": "expected",
        "oauth_probe_ok": True,
        "oauth_healthy": False,
        "oauth_stream_status": "inactive",
        "oauth_stream_health_status": "noData",
        "oauth_checked_ts_utc": AT,
    }
    result = platform_fact(value, stream_id="stream", expected_video_id="expected")
    assert result == {"state": "DOWN", "observed_at": AT, "stream_id": "stream"}
    assert platform_fact(value, stream_id="stream", expected_video_id="other")["state"] == "UNKNOWN"


def test_missing_platform_source_timestamp_is_not_wrapper_timestamp() -> None:
    value = {
        "video_id": "expected",
        "oauth_probe_ok": True,
        "oauth_healthy": True,
        "oauth_stream_status": "active",
        "oauth_stream_health_status": "good",
        "ts_utc": AT,
    }
    assert platform_fact(value, stream_id="stream", expected_video_id="expected")["observed_at"] is None


def test_transport_requires_fresh_exact_target_and_acked_bytes() -> None:
    identity = {
        "host_id": "dell",
        "host_boot_id": "boot",
        "namespace": "stream",
        "pod_uid": "pod",
        "container_name": "stream-engine",
        "container_id": "container",
        "ffmpeg_pid": 123,
        "ffmpeg_generation": "generation",
    }
    target = {"status": "VALID", "observed_at": AT, "valid_until": isoformat_utc(NOW + timedelta(seconds=15)), "target_identity": identity}
    transport = {"ts_utc": AT, "ffmpeg_pid": 123, "metrics": {"bytes_acked": 100}}
    assert transport_fact(target, transport, now=NOW)["bytes_acked"] == 100
    transport["ffmpeg_pid"] = 124
    assert transport_fact(target, transport, now=NOW)["bytes_acked"] is None
    transport["ffmpeg_pid"] = 123
    assert transport_fact(target, transport, now=NOW + timedelta(seconds=16))["target"] is None


def test_publication_supports_real_numeric_generated_at_and_separates_mirror() -> None:
    result = publication_fact(
        local_generated_at=NOW.timestamp(),
        remote_generated_at=NOW.timestamp() - 1000,
        upload_completed_at=AT,
        upload_succeeded=True,
        network_state="UP",
        now=NOW,
    )
    assert result["reason_codes"] == ["PUBLIC_MIRROR_STALE_OR_UNKNOWN"]
    assert result["control_input_allowed"] is False


@pytest.mark.parametrize("bad", ['{"a":1,"a":2}', '{"a":NaN}', '{"a":Infinity}', "[]"])
def test_strict_json_does_not_accept_ambiguous_evidence(tmp_path: Path, bad: str) -> None:
    path = tmp_path / "input.json"
    path.write_text(bad)
    path.chmod(0o644)
    with pytest.raises(ValueError):
        strict_object(path)


def test_symlink_input_is_not_admitted(tmp_path: Path) -> None:
    source = tmp_path / "actual.json"
    source.write_text("{}")
    link = tmp_path / "link.json"
    link.symlink_to(source)
    with pytest.raises(OSError):
        strict_object(link)


def test_wifi_upstream_failure_does_not_reassociate_local_interface() -> None:
    result = wifi_diagnosis(link_up=True, gateway_up=True, dns_ok=True, independent_https_successes=0, publisher_ok=False)
    assert result["reason_code"] == "UPSTREAM_UNREACHABLE_NOT_PROOF_OF_WIFI_FAILURE"
    assert result["automatic_reassociate"] is False
    result = wifi_diagnosis(link_up=True, gateway_up=True, dns_ok=True, independent_https_successes=2, publisher_ok=False)
    assert result["reason_code"] == "PUBLISHER_OR_CREDENTIAL_FAILURE"


def test_sqlite_observation_is_query_only_and_uses_all_five_action_tables(tmp_path: Path) -> None:
    path = tmp_path / "central.sqlite3"
    tables = ("commands", "recovery_authorizations", "delivery_attempts", "effect_scope_ledger", "effect_reconciliations")
    with closing(sqlite3.connect(path)) as db, db:
        for name in tables:
            db.execute(f"CREATE TABLE {name}(id integer)")
    before = path.read_bytes()
    result = central_safety(path, {"command_delivery_enabled": False, "control_capability_count": 0, "observed_at": AT}, now=NOW)
    assert result["action_table_counts"] == dict.fromkeys(tables, 0)
    with query_only(path) as db, pytest.raises(sqlite3.OperationalError):
        db.execute("INSERT INTO commands VALUES(1)")
    assert path.read_bytes() == before


def test_effect_adapter_counts_duplicates_and_unknown_scopes(tmp_path: Path) -> None:
    path = tmp_path / "effects.sqlite3"
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript("""
        CREATE TABLE effect_scope_fences(
            effect_scope_id text, state text, physical_attempt_count integer, owner_request_id text, identity_json text);
        CREATE TABLE typed_effect_requests(producer_id text, operation text);
        CREATE TABLE effect_requests(producer_id text, operation text);
        CREATE TABLE effect_reconciliations(
            effect_scope_id text, resolution text, evidence_json text, recorded_at text, reconciliation_id text);
        INSERT INTO effect_scope_fences VALUES('scope', 'OUTCOME_UNKNOWN', 2, 'request', '{}');
        INSERT INTO typed_effect_requests VALUES('unexpected-owner', 'restart_host');
        """)
    with query_only(path) as db:
        result = effect_safety(db, now=NOW, allowed_producers=["independent-controller"])
    assert result["physical_attempt_count"] == 2
    assert result["duplicate_attempt_count"] == result["unauthorized_effect_count"] == result["unresolved_scope_count"] == 1


def test_activation_is_not_claimed_from_code_presence(tmp_path: Path) -> None:
    proc = tmp_path / "123"
    proc.mkdir()
    (proc / "environ").write_bytes(b"OTHER=value\0")
    target = {"target_identity": {"ffmpeg_pid": 123}}
    assert runtime_activation(target, proc_root=tmp_path)["bounded_termination_enabled"] is False
    (proc / "environ").write_bytes(b"FR_FFMPEG_FORCE_KILL_ENABLED=1\0FFMPEG_RW_TIMEOUT_ENABLED=1\0")
    (proc / "cmdline").write_bytes(b"ffmpeg\0-rw_timeout\0" + b"15000000\0")
    assert runtime_activation(target, proc_root=tmp_path)["bounded_termination_enabled"] is True
    assert runtime_activation(target, proc_root=tmp_path)["rw_timeout_enabled"] is True
    assert "OTHER" not in json.dumps(runtime_activation(target, proc_root=tmp_path))


def test_activation_requires_bounded_waits_and_actual_ffmpeg_argument(tmp_path: Path) -> None:
    proc = tmp_path / "123"
    proc.mkdir()
    target = {"target_identity": {"ffmpeg_pid": 123}}
    (proc / "environ").write_bytes(b"FR_FFMPEG_FORCE_KILL_ENABLED=1\0FR_FFMPEG_TERM_GRACE_SEC=600\0FFMPEG_RW_TIMEOUT_ENABLED=1\0")
    (proc / "cmdline").write_bytes(b"ffmpeg\0-rw_timeout\0" + b"15000000\0")
    assert runtime_activation(target, proc_root=tmp_path) == {"bounded_termination_enabled": False, "rw_timeout_enabled": True}
    (proc / "environ").write_bytes(b"FR_FFMPEG_FORCE_KILL_ENABLED=1\0FFMPEG_RW_TIMEOUT_ENABLED=1\0")
    (proc / "cmdline").write_bytes(b"ffmpeg\0")
    assert runtime_activation(target, proc_root=tmp_path) == {"bounded_termination_enabled": True, "rw_timeout_enabled": False}
