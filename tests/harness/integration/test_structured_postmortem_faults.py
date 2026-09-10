"""Structured-corpus hypotheses exercised with local files and test signatures.

No corpus text, live credentials, remote hosts, or production state is used.
"""

from __future__ import annotations

import copy
import socket
import threading
import urllib.request
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from cra_dell_recovery.canonical import Signer
from cra_dell_recovery.reloading_tls_server import ReloadingTLSHTTPServer
from cra_no_action_soak import recovery_observer, recovery_soak
from cra_no_action_soak import recovery_transport as transport_module
from cra_no_action_soak.recovery_facts import source_hash
from cra_no_action_soak.recovery_soak import Config
from tests.harness.unit.test_recovery_soak import START, frames, packet, replay_checkpoints
from tests.harness.unit.test_recovery_soak import setup as setup
from tests.integration.test_mtls_transport import mtls_contexts
from tests.unit.test_reloading_tls_server import _wait_until


def test_tls_capacity_recovers_after_stalled_peer_without_losing_packet(tmp_path: Path) -> None:
    server_context, client_context, _ = mtls_contexts(tmp_path)
    path = tmp_path / "packet.json"
    original = b'{"sequence":19,"source_timestamp":"unchanged"}'
    path.write_bytes(original)
    path.chmod(0o644)
    server = ReloadingTLSHTTPServer(
        ("127.0.0.1", 0),
        transport_module.handler_for({"dell": str(path)}),
        server_context,
        connection_timeout_seconds=1.0,
        maximum_concurrent_requests=1,
    )
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    worker.start()
    stalled = None
    rejected = None
    try:
        address = (str(server.server_address[0]), int(server.server_address[1]))
        stalled = socket.create_connection(address, timeout=1)
        _wait_until(lambda: server.active_request_count == 1)
        rejected = socket.create_connection(address, timeout=1)
        _wait_until(lambda: server.rejected_connection_count == 1)
        assert server.active_request_count == 1
        stalled.close()
        _wait_until(lambda: server.active_request_count == 0)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=client_context))
        with opener.open(f"https://127.0.0.1:{address[1]}" + transport_module.ROUTES["dell"], timeout=2) as response:
            assert response.status == 200
            assert response.read(1024) == original
        assert path.read_bytes() == original
    finally:
        for connection in (stalled, rejected):
            if connection is not None:
                connection.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


@pytest.mark.parametrize("role,part", [("dell", "network"), ("arena", "network"), ("arena", "platform"), ("arena", "control_path")])
@pytest.mark.parametrize("invalid", [[], {}, True, 1, None, "HEALTHY"])
def test_signed_invalid_health_does_not_abort_or_pass_collector(
    setup: tuple[Config, dict[str, Signer]], role: str, part: str, invalid: Any
) -> None:
    config, signers = setup
    samples = []
    for seconds in (0, 15, 30, 45, 60):
        for source in config.bindings:
            body = packet(config, signers, source, seconds)
            if seconds == 45 and source == role:
                body["facts"][part]["state"] = invalid
                # Valid test signature: exercise bad producer output, not a
                # transport-tamper rejection before reaching the parser.
                body = signers[source].sign(body)
            recovery_soak.atomic_write_json(Path(config.value["hosts"][source]["inbox_file"]), body)
        sample = recovery_soak.collect(config, now=START + timedelta(seconds=seconds))
        samples.append(sample)
        assert sample["inputs"]["cra"]["facts"]["safety"]["action_table_counts"] == dict.fromkeys(
            ("commands", "recovery_authorizations", "delivery_attempts", "effect_scope_ledger", "effect_reconciliations"), 0
        )
    persisted = Path(config.value["evidence_file"]).read_bytes().splitlines()
    assert len(persisted) == 5
    assert samples[3]["inputs"][role]["facts"][part]["state"] == invalid
    result = recovery_soak.evaluate(samples, config=config, now=START + timedelta(seconds=60))
    assert role.upper() + "_SIGNED_EVIDENCE_INVALID" in result["blockers"], result
    assert not result["eligible"]
    assert result["physical_attempt_delta"] == 0
    assert not result["oracle_errors"]


@pytest.mark.parametrize("invalid_name", [[], {}, None, True])
def test_bad_probe_name_preserves_other_observer_facts(tmp_path: Path, invalid_name: Any) -> None:
    at = START.isoformat()
    probes = [{"name": name, "ok": True} for name in ("cloudflare_v4", "cloudflare_v6", "google_v4", "google_v6")]
    probes[0]["name"] = invalid_name
    raw = {
        "network": {"ts_utc": at, "probes": probes},
        "platform": {},
        "control_path": {"status": "READY", "observed_at": at},
    }
    paths = {name: str(tmp_path / (name + ".json")) for name in raw}
    for name, data in raw.items():
        recovery_soak.atomic_write_json(Path(paths[name]), data)
    config = {"binding": {"role": "arena", "stream_id": "fixture"}, "expected_video_id": "fixture", "sources": paths}
    facts, hashes = recovery_observer.build_facts(config, now=START)
    assert facts["network"] == {"state": "UNKNOWN", "observed_at": at}
    assert facts["control_path"] == {"state": "UP", "observed_at": at}
    assert hashes["network"] == source_hash(raw["network"])
    assert hashes["control_path"] == source_hash(raw["control_path"])


@pytest.mark.parametrize("trigger", ["network", "platform", "control_path"])
def test_explicit_outage_onset_includes_simultaneous_unknown_source(setup: tuple[Config, dict[str, Signer]], trigger: str) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    # Confirm a fault at t=45, while an independent observation becomes
    # unavailable in the same sample. Keep ledger and CRA evidence fresh.
    arena_changes = {"platform": {"state": "UNKNOWN"}}
    dell_changes: dict[str, Any] = {}
    if trigger == "network":
        dell_changes["network"] = {"state": "DOWN"}
    elif trigger == "platform":
        arena_changes = {"platform": {"state": "DOWN"}, "control_path": {"state": "UNKNOWN"}}
    else:
        arena_changes["control_path"] = {"state": "DOWN"}
    rows[3]["inputs"]["dell"] = packet(config, signers, "dell", 45, **dell_changes)
    rows[3]["inputs"]["arena"] = packet(config, signers, "arena", 45, **arena_changes)
    replay_checkpoints(config, rows)
    result = recovery_soak.evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["harness_classification"] == "PASS", result
    assert result["recovery"]["recovered_episode_count"] == 1
    assert result["recovery"]["verified_network_recovered_episode_count"] == int(trigger == "network")
    assert not result["unknown_reasons"] and not result["blockers"] and not result["oracle_errors"]


@pytest.mark.parametrize("fault", ["unknown-only", "unknown-before-down", "ledger-missing-at-onset", "activation-false"])
def test_outage_tolerance_never_erases_unproven_onset_or_safety_failure(setup: tuple[Config, dict[str, Signer]], fault: str) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    if fault in ("unknown-only", "unknown-before-down"):
        rows[3]["inputs"]["arena"] = packet(config, signers, "arena", 45, platform={"state": "UNKNOWN"})
        if fault == "unknown-before-down":
            rows[4]["inputs"]["arena"] = packet(config, signers, "arena", 60, platform={"state": "DOWN"})
    else:
        config.value["require_independent_effect_evidence"] = True
        # Bind all samples to the strict policy, with known exact-target data.
        for row in rows:
            row["config_sha256"] = config.identity
            body = copy.deepcopy(row["inputs"]["dell"])
            body["facts"]["effects"]["target_status"] = "VALID"
            row["inputs"]["dell"] = signers["dell"].sign(body)
        body = copy.deepcopy(rows[3]["inputs"]["dell"])
        body["facts"]["network"]["state"] = "DOWN"
        if fault == "ledger-missing-at-onset":
            body["facts"]["effects"] = {"observed_at": None, "integrity": "UNKNOWN"}
        else:
            body["facts"]["activation"]["bounded_termination_enabled"] = False
        rows[3]["inputs"]["dell"] = signers["dell"].sign(body)
    replay_checkpoints(config, rows)
    result = recovery_soak.evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert not result["eligible"]
    assert result["harness_classification"] != "PASS", result
    assert not result["oracle_errors"]
    if fault.startswith("unknown"):
        assert "SOURCE_EVIDENCE_UNKNOWN_OUTSIDE_KNOWN_OUTAGE" in result["unknown_reasons"]
    elif fault == "ledger-missing-at-onset":
        assert "DELL_EFFECT_EVIDENCE_MISSING_OR_STALE" in result["unknown_reasons"]
    else:
        assert "BOUNDED_TERMINATION_NOT_ACTIVE" in result["blockers"]


@pytest.mark.parametrize("fault", ["arena-absent", "arena-stale", "cra-absent", "cra-stale", "arena-bad-signature", "arena-earlier-gap"])
def test_outage_onset_packet_gap_keeps_authority_and_authenticity_requirements(setup: tuple[Config, dict[str, Signer]], fault: str) -> None:
    config, signers = setup
    rows = frames(config, signers, outage=False)
    rows[3]["inputs"]["dell"] = packet(config, signers, "dell", 45, network={"state": "DOWN"})
    role = fault.split("-")[0]
    if fault.endswith("absent"):
        rows[3]["inputs"][role] = None
    elif fault.endswith("stale"):
        rows[3]["inputs"][role] = packet(config, signers, role, 0)
    elif fault == "arena-earlier-gap":
        rows[2]["inputs"][role] = None
    else:
        rows[3]["inputs"][role]["facts"]["platform"]["state"] = "UNKNOWN"
    replay_checkpoints(config, rows)
    result = recovery_soak.evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert not result["oracle_errors"], result
    if fault in ("arena-absent", "arena-stale"):
        assert result["harness_classification"] == "PASS", result
        assert result["recovery"]["verified_network_recovered_episode_count"] == 1
        assert not result["unknown_reasons"]
    else:
        assert result["harness_classification"] != "PASS", result
        assert not result["eligible"]
    if role == "cra":
        assert "CRA_SAFETY_EVIDENCE_MISSING_OR_STALE" in result["unknown_reasons"]
    elif fault == "arena-bad-signature":
        assert "ARENA_SIGNED_EVIDENCE_INVALID" in result["blockers"]
    elif fault == "arena-earlier-gap":
        assert "ARENA_EVIDENCE_UNAVAILABLE_OUTSIDE_KNOWN_OUTAGE" in result["unknown_reasons"]


def test_unobserved_child_transition_onset_remains_uncertified_after_recovery(setup: tuple[Config, dict[str, Signer]]) -> None:
    config, signers = setup
    config.value["require_independent_effect_evidence"] = True
    rows = frames(config, signers, outage=False)
    # Sanitized mechanism from the live episode: target and platform UNKNOWN
    # precede the confirmed delivery/control interruption. The live archive
    # is replayed separately; no production identifiers are embedded here.
    for index, row in enumerate(rows):
        seconds = index * 15
        body = copy.deepcopy(row["inputs"]["dell"])
        effects = body["facts"]["effects"]
        effects["target_status"] = "VALID"
        if seconds in (45, 60):
            body["facts"]["transport"].update(target=None, bytes_acked=None)
            effects.update(target_status="UNKNOWN", current_target_sha256=None, current_target_unresolved_scope_count=None)
            body["facts"]["activation"] = dict.fromkeys(("bounded_termination_enabled", "rw_timeout_enabled"))
        elif seconds >= 75:
            body["facts"]["transport"]["target"] = "target-2"
            effects["current_target_sha256"] = "target-2"
        row["inputs"]["dell"] = signers["dell"].sign(body)
        if 45 <= seconds <= 105:
            row["inputs"]["arena"] = packet(config, signers, "arena", seconds, platform={"state": "UNKNOWN"})
        if seconds == 60:
            row["inputs"]["cra"] = packet(config, signers, "cra", seconds, safety={"runtime_readiness": "SAFE_BLOCKED"})
    replay_checkpoints(config, rows)
    assert rows[3]["live_recovery"]["window"]["active"] is None
    assert rows[3]["live_recovery"]["window"]["current_health"] == "UNKNOWN"
    result = recovery_soak.evaluate(rows, config=config, now=START + timedelta(seconds=180))
    assert result["harness_classification"] == "MISSING_EVIDENCE", result
    assert result["live_health"] == "READY"
    assert result["recovery"]["recovered_episode_count"] == 1
    assert result["recovery"]["verified_network_recovered_episode_count"] == 0
    assert result["physical_attempt_delta"] == 0
    assert not result["blockers"] and not result["oracle_errors"]
    assert set(result["unknown_reasons"]) == {
        "RECOVERY_SOURCE_GAP_OUTSIDE_KNOWN_OUTAGE",
        "SOURCE_EVIDENCE_UNKNOWN_OUTSIDE_KNOWN_OUTAGE",
    }
