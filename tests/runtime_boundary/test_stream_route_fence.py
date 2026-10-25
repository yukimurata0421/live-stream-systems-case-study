from __future__ import annotations

import hashlib
import json
import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

from runtime_boundary import EffectLedger, EffectRequest, RESTART_FFMPEG, SWITCH_STREAM_ROUTE
from runtime_boundary.server import EffectExecutorServer
from runtime_boundary.operator_route import OperatorRouteGrant


TARGET = {"host_id": "delivery-host", "host_boot_id": "boot-a", "namespace": "stream-v3",
          "pod_uid": "pod-a", "container_name": "stream-engine", "container_id": "containerd://a",
          "ffmpeg_generation": "delivery-generation-a", "ffmpeg_pid": 200}


def request(intent, name):
    now = datetime.now(UTC)
    value = {"schema_version": "runtime.typed_effect_request.v2", "request_id": name,
             "producer_id": "approved-operator", "producer_generation": 1, "intent_type": intent,
             "reason": "explicit operator route change", "failure_domain": "FFMPEG_LOCAL",
             "issued_at": now.isoformat(), "expires_at": (now + timedelta(seconds=5)).isoformat(),
             "ffmpeg_target_identity": dict(TARGET), "runtime_identity": None,
             "expected_ffmpeg_generation": "delivery-generation-a", "idempotency_key": name,
             "correlation_id": name, "target_snapshot_id": "target-a", "runtime_snapshot_id": "",
             "runtime_observation_id": "observation-a", "expected_executor_instance_id": "executor-a",
             "maintenance_evidence_status": "UNKNOWN", "projection_id": "", "projection_sequence": 0}
    if intent == SWITCH_STREAM_ROUTE:
        value.update(schema_version="runtime.typed_effect_request.v3", route_transition={
            "from_route": "FALLBACK", "to_route": "PRIMARY", "expected_route_generation": 1,
            "operator_grant_id": "manual-approval-a"})
    return EffectRequest.from_mapping(value)


def ledger(tmp_path):
    return EffectLedger(tmp_path / "effects.sqlite3", initial_producer_id="approved-operator", initial_producer_generation=1)


def test_route_uses_existing_exact_and_logical_fence_namespace():
    restart, route = request(RESTART_FFMPEG, "restart"), request(SWITCH_STREAM_ROUTE, "route")
    encoded = json.dumps({"action": "restart_ffmpeg", "exact_target_identity": TARGET}, sort_keys=True, separators=(",", ":")).encode()
    assert restart.effect_scope_id == route.effect_scope_id == hashlib.sha256(encoded).hexdigest()
    assert restart.logical_generation_scope_id == route.logical_generation_scope_id
    assert route.operation == "switch_stream_route"
    assert route.digest() != restart.digest()
    assert EffectRequest.from_mapping(route.canonical()) == route


def test_route_schema_matches_wire_and_rejects_wrong_intent():
    from pathlib import Path
    import jsonschema
    schema = json.loads((Path(__file__).resolve().parents[2]/"contracts/runtime_boundary/typed_effect_request.v3.schema.json").read_text())
    wire = request(SWITCH_STREAM_ROUTE, "route").canonical()
    jsonschema.Draft202012Validator(schema).validate(wire)
    wire["intent_type"] = RESTART_FFMPEG
    with pytest.raises(Exception, match="V3_ROUTE_INTENT_REQUIRED"):
        EffectRequest.from_mapping(wire)


@pytest.mark.parametrize("first,second", [(RESTART_FFMPEG, SWITCH_STREAM_ROUTE), (SWITCH_STREAM_ROUTE, RESTART_FFMPEG)])
def test_either_order_consumes_one_shared_scope(tmp_path, first, second):
    owner = ledger(tmp_path)
    try:
        a, b = request(first, "first"), request(second, "second")
        assert owner.accept(a)["accepted"] is True
        assert owner.accept(b)["reason"] == "EFFECT_SCOPE_ALREADY_CLAIMED"
        owner.transition(a.request_id, expected="ACCEPTED", state="EXECUTION_STARTED")
        assert owner.mark_effect_boundary(a.request_id)
        assert owner.scope(a.effect_scope_id)["physical_attempt_count"] == 1
    finally:
        owner.close()


def test_concurrent_route_and_restart_have_one_winner(tmp_path):
    owner = ledger(tmp_path)
    try:
        values = [request(RESTART_FFMPEG, "restart"), request(SWITCH_STREAM_ROUTE, "route")]
        with ThreadPoolExecutor(max_workers=2) as pool:
            result = list(pool.map(owner.accept, values))
        assert sum(r["accepted"] for r in result) == 1
        assert owner.connection.execute("SELECT count(*) FROM effect_scope_fences").fetchone()[0] == 1
    finally:
        owner.close()


def test_restart_preserves_unknown_route_and_rejects_new_id(tmp_path):
    owner = ledger(tmp_path)
    route = request(SWITCH_STREAM_ROUTE, "route")
    assert owner.accept(route)["accepted"]
    owner.transition(route.request_id, expected="ACCEPTED", state="EXECUTION_STARTED")
    owner.mark_effect_boundary(route.request_id)
    owner.close()
    owner = ledger(tmp_path)
    try:
        assert owner.scope(route.effect_scope_id)["state"] == "OUTCOME_UNKNOWN"
        assert owner.accept(request(RESTART_FFMPEG, "another-id"))["accepted"] is False
        assert owner.scope(route.effect_scope_id)["physical_attempt_count"] == 1
    finally:
        owner.close()


def test_current_route_generation_checked_before_effect():
    route = request(SWITCH_STREAM_ROUTE, "route")
    current = {"ffmpeg_target_identity": TARGET, "ffmpeg_generation": "delivery-generation-a",
               "executor_instance_id": "executor-a", "route": "FALLBACK", "route_generation": 1}
    assert EffectExecutorServer._identity_mismatch(route, current) == ""
    for changes in ({"route_generation": 2}, {"route": "PRIMARY"}):
        assert EffectExecutorServer._identity_mismatch(route, {**current, **changes}) == "STREAM_ROUTE_GENERATION_DRIFT"


def grant():
    return OperatorRouteGrant({"grant_id": "manual-approval-a", "producer_id": "approved-operator",
        "producer_generation": 1, "peer_uid": 0, "expires_at": time.time()+60,
        **{k:TARGET[k] for k in ("host_id", "namespace", "pod_uid", "container_name", "container_id")}},
        executor_instance="executor-a")


@pytest.mark.parametrize("fault", ["none", "peer", "producer", "grant", "target", "executor"])
def test_manual_grant_does_not_borrow_existing_producer(fault):
    authority = grant()
    value = request(SWITCH_STREAM_ROUTE, "route").canonical()
    uid = 0
    if fault == "peer": uid = 992
    if fault == "producer": value["producer_id"] = "legacy-in-pod"
    if fault == "grant": value["route_transition"]["operator_grant_id"] = "other"
    if fault == "target": value["ffmpeg_target_identity"]["container_id"] = "containerd://other"
    if fault == "executor": value["expected_executor_instance_id"] = "other"
    assert (authority.validate(EffectRequest.from_mapping(value), uid) == "") is (fault == "none")


def test_grant_checks_elapsed_expiry_and_clock_regression(monkeypatch):
    authority = grant()
    route = request(SWITCH_STREAM_ROUTE, "route")
    monkeypatch.setattr(time, "clock_gettime", lambda _: authority.deadline+1)
    assert authority.validate(route, 0) == "OPERATOR_ROUTE_GRANT_EXPIRED_OR_CLOCK_CHANGED"
    monkeypatch.setattr(time, "clock_gettime", lambda _: authority.elapsed)
    monkeypatch.setattr(time, "time", lambda: authority.wall-1)
    assert authority.validate(route, 0) == "OPERATOR_ROUTE_GRANT_EXPIRED_OR_CLOCK_CHANGED"


def test_server_rechecks_manual_grant_before_shared_fence(tmp_path):
    route = request(SWITCH_STREAM_ROUTE, "route")
    class FakePeer:
        def getsockopt(self, *_): return struct.pack("3i", 100, 0, 0)
        def recv(self, *_): return json.dumps(route.canonical()).encode()+b"\n"
    owner = ledger(tmp_path)
    seen = []
    def authorize(req, uid):
        seen.append((req.request_id, uid))
        return "" if len(seen) == 1 else "OPERATOR_ROUTE_GRANT_REVOKED"
    effects = []
    server = EffectExecutorServer(socket_path=tmp_path/'effect.sock', ledger=owner, allowed_peer_uids={0},
        allowed_intents={SWITCH_STREAM_ROUTE}, route_authorizer=authorize,
        current_target=lambda:{"ffmpeg_target_identity":TARGET,"ffmpeg_generation":"delivery-generation-a",
                               "executor_instance_id":"executor-a","route":"FALLBACK","route_generation":1},
        perform_effect=lambda req:effects.append(req.request_id) or {})
    try:
        response = server._handle_connection(FakePeer())
        assert response["reason"] == "OPERATOR_ROUTE_GRANT_REVOKED"
        assert len(seen) == 2 and effects == []
        assert owner.scope(route.effect_scope_id)["physical_attempt_count"] == 0
    finally:
        owner.close()


def test_legacy_constraint_migration_preserves_history_and_fences(tmp_path):
    owner = ledger(tmp_path)
    first = request(RESTART_FFMPEG, "historical")
    owner.accept(first)
    owner.transition(first.request_id, expected="ACCEPTED", state="EXECUTION_STARTED")
    owner.mark_effect_boundary(first.request_id)
    owner.transition(first.request_id, expected="EXECUTION_STARTED", state="EFFECT_OBSERVED")
    before = [tuple(row) for row in owner.connection.execute("SELECT * FROM typed_effect_requests")]
    sql = owner.connection.execute("SELECT sql FROM sqlite_master WHERE name='typed_effect_requests'").fetchone()[0]
    sql = sql.replace(",'SWITCH_STREAM_ROUTE'", "")
    owner.connection.executescript("ALTER TABLE typed_effect_requests RENAME TO previous;" + sql + ";INSERT INTO typed_effect_requests SELECT * FROM previous;DROP TABLE previous;")
    owner.close()
    owner = ledger(tmp_path)
    try:
        after = [tuple(row) for row in owner.connection.execute("SELECT * FROM typed_effect_requests")]
        assert before == after
        assert owner.scope(first.effect_scope_id)["physical_attempt_count"] == 1
        assert owner.accept(request(SWITCH_STREAM_ROUTE, "route"))["accepted"] is False
        assert owner.connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        owner.close()
