from __future__ import annotations

import copy

import pytest

from cra import audit_ack
from cra.sync import ACKED_KEY
from tests.reflex.support import Harness
from tests.reflex.test_reflex_central import central as central  # noqa: PLC0414 - export the pytest fixture
from tests.reflex.test_reflex_central import connect


@pytest.mark.parametrize("fault", ["unsigned", "signature", "agent", "instance", "nonce", "digest", "range", "outer"])
def test_invalid_ack_never_advances_local_watermark(tmp_path, central, monkeypatch, fault):
    h = Harness(tmp_path / "dell")
    sync = connect(h, central)
    h.tick(stalled=False)
    post = sync._http

    def invalid(method, path, body=None):
        if method != "POST":
            return post(method, path, body)
        request = copy.deepcopy(body["body"])
        if fault in {"agent", "instance", "nonce"}:
            request[{"agent": "agent_id", "instance": "instance_id", "nonce": "nonce"}[fault]] = "other"
        if fault == "digest":
            request["records"][0]["payload"]["tampered"] = True
        response = audit_ack.sign_ack(central.policy_key, request, request["records"][-1]["seq"] + int(fault == "range"))
        if fault == "unsigned":
            response = {"acked_seq": response["acked_seq"]}
        elif fault == "signature":
            response["signature"] = "bad"
        elif fault == "outer":
            response["acked_seq"] += 1
        return response

    monkeypatch.setattr(sync, "_http", invalid)
    with pytest.raises(ValueError, match="ACK_"):
        sync.push_audit()
    assert h.agent.store.get(ACKED_KEY) is None


def test_lost_signed_ack_retries_records_with_fresh_nonce(tmp_path, central, monkeypatch):
    h = Harness(tmp_path / "dell")
    sync = connect(h, central)
    h.tick(stalled=False)
    post = sync._http
    saved = []

    def lose_response(method, path, body=None):
        response = post(method, path, body)
        saved.append(response)
        raise TimeoutError("reply lost after central commit")

    monkeypatch.setattr(sync, "_http", lose_response)
    with pytest.raises(TimeoutError):
        sync.push_audit()
    assert h.agent.store.get(ACKED_KEY) is None
    stored = central.store.audit_records()
    assert len(stored) == h.agent.store.last_seq()
    monkeypatch.setattr(sync, "_http", lambda *_: saved[0])
    with pytest.raises(ValueError, match="ACK_REQUEST_MISMATCH"):
        sync.push_audit()  # Same records, but a new request nonce: replay must fail.
    assert h.agent.store.get(ACKED_KEY) is None
    monkeypatch.setattr(sync, "_http", post)
    assert sync.push_audit() == h.agent.store.last_seq()
    assert central.store.audit_records() == stored


def test_old_unsigned_server_blocks_audit_ack_but_keeps_dry_run(tmp_path, central, monkeypatch):
    h = Harness(tmp_path / "dell")
    sync = connect(h, central)
    h.tick(stalled=False)
    post = sync._http

    def legacy(method, path, body=None):
        response = post(method, path, body)
        return {"acked_seq": response["acked_seq"]} if method == "POST" else response

    monkeypatch.setattr(sync, "_http", legacy)
    assert sync.cycle(h.clock()).startswith("ERROR:")
    assert h.agent.store.get(ACKED_KEY) is None
    assert h.agent.policy().mode == "dry_run"
    assert h.executor.requests == []
