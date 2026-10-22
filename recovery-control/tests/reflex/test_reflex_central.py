from __future__ import annotations

import contextlib
import json
import threading
from datetime import timedelta
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from cra import policy as policy_mod
from cra import signing
from cra.agent import CentralConfig
from cra.central.server import make_handler
from cra.central.store import CentralStore
from cra.central.verify import verify
from cra.decide import RESTART, WOULD_RESTART
from cra.policy import Policy
from cra.store import AgentStore
from cra.sync import CentralSync
from tests.reflex.support import T0, Harness


class Central:
    """A real cra-central HTTP server on an ephemeral port."""

    def __init__(self, tmp: Path) -> None:
        self.tmp = tmp
        self.policy_public = signing.generate_key(tmp / "policy.key")
        self.policy_key = signing.load_private_key(tmp / "policy.key")
        self.agent_public = signing.generate_key(tmp / "agent.key")
        self.store = CentralStore(tmp / "central.db")
        self.envelope_path = tmp / "policy.signed.json"
        self.agents = {"dell-test": self.agent_public}
        self.httpd: ThreadingHTTPServer | None = None
        self.port = 0

    def publish(self, policy: Policy) -> None:
        envelope = policy_mod.sign_envelope(self.policy_key, policy)
        self.store.record_policy(envelope["policy"])
        self.envelope_path.write_text(json.dumps(envelope))

    def start(self) -> None:
        self.httpd = ThreadingHTTPServer(
            ("127.0.0.1", self.port), make_handler(self.store, self.envelope_path, self.agents, self.policy_key)
        )
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def stop(self) -> None:
        assert self.httpd is not None
        self.httpd.shutdown()
        self.httpd.server_close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


@pytest.fixture
def central(tmp_path: Path):
    c = Central(tmp_path)
    c.publish(Policy(stall_grace_sec=0, version=1, mode="dry_run"))
    c.start()
    yield c
    if c.httpd is not None:
        with contextlib.suppress(OSError):
            c.stop()


def connect(h: Harness, c: Central) -> CentralSync:
    import dataclasses

    h.config = dataclasses.replace(
        h.config, central=CentralConfig(url=c.url, policy_public_key=c.policy_public, agent_key_path=c.tmp / "agent.key")
    )
    h.agent = h.new_agent(h.agent.store)
    return CentralSync(
        store=h.agent.store,
        agent_id="dell-test",
        url=c.url,
        policy_public_key=c.policy_public,
        agent_key=signing.load_private_key(c.tmp / "agent.key"),
    )


def test_policy_adoption_and_audit_delivery(tmp_path: Path, central: Central) -> None:
    h = Harness(tmp_path / "dell")
    sync = connect(h, central)
    assert sync.cycle(h.clock()).startswith("ADOPTED")
    assert h.agent.policy().version == 1
    central.publish(Policy(stall_grace_sec=0, version=2, mode="enforce"))
    sync.cycle(h.clock())
    assert h.agent.policy().mode == "enforce"
    for _ in range(6):
        h.tick(stalled=True)
    status = sync.cycle(h.clock())
    assert status == f"UNCHANGED:acked={h.agent.store.last_seq()}"
    kinds = [r["kind"] for r in central.store.audit_records()]
    assert {"policy_adopted", "decision", "action_reserved", "action_result"} <= set(kinds)


def test_older_or_reused_policy_versions_are_rejected(tmp_path: Path, central: Central) -> None:
    h = Harness(tmp_path / "dell")
    sync = connect(h, central)
    central.publish(Policy(stall_grace_sec=0, version=5, mode="enforce"))
    sync.cycle(h.clock())
    central.envelope_path.write_text(
        json.dumps(policy_mod.sign_envelope(central.policy_key, Policy(stall_grace_sec=0, version=4, mode="off")))
    )
    assert sync.cycle(h.clock()).startswith("REJECTED")
    central.envelope_path.write_text(
        json.dumps(policy_mod.sign_envelope(central.policy_key, Policy(stall_grace_sec=0, version=5, mode="off")))
    )
    assert sync.cycle(h.clock()).startswith("REJECTED")
    assert h.agent.policy() == Policy(stall_grace_sec=0, version=5, mode="enforce")


def test_lan_outage_does_not_change_behaviour_and_backlog_drains(tmp_path: Path, central: Central) -> None:
    h = Harness(tmp_path / "dell")
    sync = connect(h, central)
    central.publish(Policy(stall_grace_sec=0, version=2, mode="enforce"))
    sync.cycle(h.clock())
    central.stop()  # LAN down
    assert sync.cycle(h.clock()).startswith("ERROR")
    assert h.agent.policy().mode == "enforce"  # last adopted policy stays in force
    for _ in range(6):
        decision = h.tick(stalled=True)
    assert decision.outcome == RESTART
    assert len(h.executor.requests) == 1
    for _ in range(1200):  # more records than one sync batch
        h.clock.advance(1)
        h.agent.store.audit(h.clock(), "filler", {})
    central.start()  # LAN back (same port)
    sync.cycle(h.clock())
    assert int(h.agent.store.get("acked_seq") or 0) == h.agent.store.last_seq()
    seqs = [r["seq"] for r in central.store.audit_records()]
    assert seqs == sorted(set(seqs)) and seqs[-1] == h.agent.store.last_seq()


def test_forged_audit_is_refused(tmp_path: Path, central: Central) -> None:
    signing.generate_key(tmp_path / "intruder.key")
    h = Harness(tmp_path / "dell")
    connect(h, central)
    h.tick(stalled=False)
    forged = CentralSync(
        store=h.agent.store,
        agent_id="dell-test",
        url=central.url,
        policy_public_key=central.policy_public,
        agent_key=signing.load_private_key(tmp_path / "intruder.key"),
    )
    assert forged.cycle(h.clock()).startswith("ERROR")
    assert central.store.audit_records() == []


def test_replayed_records_are_deduplicated_and_conflicts_kept(tmp_path: Path) -> None:
    store = CentralStore(tmp_path / "c.db")
    records = [{"seq": i, "ts": f"t{i}", "kind": "k", "payload": {"i": i}} for i in (1, 2, 3)]
    assert store.ingest("a", "i1", records) == 3
    assert store.ingest("a", "i1", records) == 3
    assert store.ingest("a", "i1", [{"seq": 2, "ts": "t2", "kind": "k", "payload": {"i": 99}}]) == 3
    assert len(store.audit_records()) == 3
    assert store.conflict_count() == 1
    assert store.ingest("a", "i2", records[:1]) == 1  # re-provisioned agent: new instance, new sequence


# --- verify ----------------------------------------------------------------------------


def _record(seq: int, kind: str, payload: dict, minutes: float = 0) -> dict:
    return {
        "agent_id": "dell",
        "instance_id": "i",
        "seq": seq,
        "ts": (T0 + timedelta(minutes=minutes)).isoformat(),
        "kind": kind,
        "payload": payload,
    }


def _reserved(seq: int, action_id: int, generation: str, minutes: float, *, mode="enforce", network=None) -> dict:
    network = network or {"network_down": False, "ingest_ok": True}
    return _record(
        seq,
        "action_reserved",
        {
            "action_id": action_id,
            "generation_key": generation,
            "mode": mode,
            "policy_version": 2,
            "evidence": {"network": network},
        },
        minutes,
    )


POLICIES = {2: {**Policy(stall_grace_sec=0, version=2, mode="enforce").to_dict()}}


def test_verify_clean_run_from_real_agent(tmp_path: Path, central: Central) -> None:
    h = Harness(tmp_path / "dell")
    sync = connect(h, central)
    central.publish(Policy(stall_grace_sec=0, version=2, mode="enforce", cooldown_sec=10))
    sync.cycle(h.clock())
    for _ in range(6):
        h.tick(stalled=True)
    h.pod.new_generation()
    h.pod.uptime = 0
    h.tick(stalled=False)
    h.tick(stalled=False)
    sync.cycle(h.clock())
    report = verify(central.store.audit_records(), central.store.policies(), now=h.clock())
    assert report["violations"] == []
    summary = next(iter(report["summary"].values()))
    assert summary["actions_by_mode"] == {"enforce": 1}
    assert summary["verification_results"] == {"DELIVERY_RESUMED": 1}


def test_verify_detects_duplicate_generation_action() -> None:
    records = [
        _reserved(1, 1, "pod/g1", 0),
        _record(2, "action_result", {"action_id": 1, "state": "OUTCOME_UNKNOWN"}, 0),
        _reserved(3, 2, "pod/g1", 5),
        _record(4, "action_result", {"action_id": 2, "state": "EFFECT_OBSERVED"}, 5),
    ]
    codes = [v["code"] for v in verify(records, POLICIES, now=T0 + timedelta(minutes=6))["violations"]]
    assert codes == ["I1_DUPLICATE_GENERATION_ACTION"]


def test_verify_detects_policy_network_budget_and_result_gaps() -> None:
    records = [
        _reserved(1, 1, "pod/g1", 0, network={"network_down": True, "ingest_ok": False}),
        _reserved(2, 2, "pod/g2", 0.5),
        _reserved(3, 3, "pod/g3", 2),
        _reserved(4, 4, "pod/g4", 4),
        _reserved(5, 5, "pod/g5", 6),
        _reserved(6, 6, "pod/g6", 8),
    ]
    records[5]["payload"]["policy_version"] = 9
    codes = {v["code"] for v in verify(records, POLICIES, now=T0 + timedelta(minutes=40))["violations"]}
    assert codes == {
        "I4_ACTION_WITHOUT_LOCALIZED_STALL",
        "I6_COOLDOWN",
        "I6_LIMIT",
        "I8_UNKNOWN_POLICY_VERSION",
        "I2_ACTION_WITHOUT_RESULT",
        "AGENT_SILENT",
    }


def test_verify_flags_enforce_under_dry_run_policy() -> None:
    policies = {2: Policy(stall_grace_sec=0, version=2, mode="dry_run").to_dict()}
    records = [_reserved(1, 1, "pod/g1", 0), _record(2, "action_result", {"action_id": 1, "state": "EFFECT_OBSERVED"})]
    codes = [v["code"] for v in verify(records, policies, now=T0)["violations"]]
    assert codes == ["I7_ENFORCE_WITHOUT_ENFORCE_POLICY"]


def test_dry_run_actions_are_not_held_to_enforce_rules(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    for _ in range(8):
        assert h.tick(stalled=True).outcome in ("NONE", WOULD_RESTART)
    records = [{"agent_id": "dell", "instance_id": "i", **r} for r in AgentStore(h.config.db_path).read_audit(0, 1000)]
    assert verify(records, {}, now=h.clock())["violations"] == []
