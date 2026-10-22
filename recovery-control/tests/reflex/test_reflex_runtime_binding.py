from __future__ import annotations

import dataclasses
import os
import subprocess
import sys
from pathlib import Path

import pytest

from cra import runtime_binding as binding
from tests.reflex.test_reflex_agent import stall, use_policy


def test_kernel_socket_sample_is_unique_and_bound_to_owned_fd():
    # The target owns only its sockets; pytest itself accumulates unrelated SQLite FDs.
    script = 'import os, socket\nfrom cra import runtime_binding as binding\nwith socket.socket() as listener:\n    listener.bind(("127.0.0.1", 0))\n    listener.listen(2)\n    with socket.create_connection(listener.getsockname()) as client, listener.accept()[0] as peer:\n        client.sendall(b"test")\n        assert peer.recv(4) == b"test"\n        process = {\n            "local_ffmpeg_pid": os.getpid(),\n            "pod_uid": "test-pod",\n            "ffmpeg_generation": "test-gen",\n            "delivery_route": "FALLBACK",\n            "delivery_route_generation": 1,\n        }\n        sample = binding.collect_transport(process, "test-exec", (listener.getsockname()[1],))\n        assert sample["status"] == "AVAILABLE", sample\n        assert sample["binding"]["peer_port"] == listener.getsockname()[1]\n        assert sample["binding"]["pid"] == os.getpid()\n        assert sample["binding"]["socket_inode"] > 0\n        with socket.create_connection(listener.getsockname()), listener.accept()[0]:\n            ambiguous = binding.collect_transport(process, "test-exec", (listener.getsockname()[1],))\n            assert ambiguous["status"] == "AMBIGUOUS"\n            assert ambiguous["binding"] is None and ambiguous["tcp_metrics"] == {}\n            assert ambiguous["legacy_tcp_metrics"]  # Old controller\'s input remains available.\n\nprint("owned TCP process PASS")\n'  # noqa: E501
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src")}
    result = subprocess.run([sys.executable, "-B", "-c", script], capture_output=True, text=True, timeout=10, check=False, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "owned TCP process PASS" in result.stdout


@pytest.mark.parametrize("fault", ["request", "pid", "exit", "generation"])
def test_owner_link_requires_exact_exit_to_start_chain(fault):
    tracker = binding.RecoveryLinks()
    old = {"ffmpeg_pid": 42, "ffmpeg_generation": "g1", "recovery_action_id": "r1"}
    new = {
        "ffmpeg_pid": 43,
        "ffmpeg_generation": "g2",
        "replaces_ffmpeg_pid": 42,
        "recovery_action_id": "r1",
        "prior_exit_event_id": "exit1",
    }
    tracker.record("ffmpeg_exited", "exit1", old, "e1", "p1")
    broken = dict(new)
    broken[
        {
            "request": "recovery_action_id",
            "pid": "replaces_ffmpeg_pid",
            "exit": "prior_exit_event_id",
            "generation": "ffmpeg_generation",
        }[fault]
    ] = 99 if fault == "pid" else ("g1" if fault == "generation" else "other")
    tracker.record("ffmpeg_started", "start1", broken, "e1", "p1")
    assert tracker.current("g2") is None
    tracker.record("ffmpeg_exited", "exit1", old, "e1", "p1")
    tracker.record("ffmpeg_started", "start1", new, "e1", "p1")
    assert binding.linked_successor(tracker.current("g2"), request_id="r1", executor="e1", pod_uid="p1", before="g1", after="g2")
    tracker.record("ffmpeg_started", "start2", {**new, "ffmpeg_generation": "g3", "recovery_action_id": ""}, "e1", "p1")
    assert tracker.current("g2") is None and tracker.current("g3") is None


def test_route_less_legacy_is_dry_run_only(harness, tmp_path):
    h = harness
    h.config = dataclasses.replace(h.config, ingest_endpoints={})
    h.agent = h.new_agent(h.agent.store)

    def tick():
        h.clock.advance(2)
        h.pod.write(stalled=True)
        import json

        raw = json.loads(h.config.observation_path.read_text())
        for key in ("delivery_transport", "cra_runtime_capabilities", "delivery_route", "delivery_route_generation"):
            raw.pop(key, None)
        h.config.observation_path.write_text(json.dumps(raw))
        return h.agent.step()

    for _ in range(6):
        decision = tick()
    assert decision.outcome == "WOULD_RESTART"
    assert h.agent.status["transport_binding_status"] == "LEGACY_UNBOUND"
    use_policy(h, tmp_path)
    for _ in range(6):
        decision = tick()
    assert "RUNTIME_CONTEXT_UNAVAILABLE" in decision.blockers
    assert h.executor.requests == []


def test_same_generation_new_socket_resets_streak(harness):
    h = harness
    h.pod.bound = True
    stall(h, 5)
    h.pod.write(stalled=True)
    import json

    raw = json.loads(h.config.observation_path.read_text())
    raw["delivery_transport"]["binding"]["socket_inode"] += 1
    h.config.observation_path.write_text(json.dumps(raw))
    assert h.agent.step().outcome == "NONE" and h.agent.tracker.streak == 0


def test_delivery_progress_needs_no_owner_link(harness, tmp_path):
    h = harness
    use_policy(h, tmp_path)
    stall(h)
    h.pod.new_generation()
    h.pod.uptime = 0
    h.tick(stalled=False, recovery_successor=None)
    assert h.agent.store.db.execute("SELECT verify_result FROM actions").fetchone()[0] is None
    h.tick(stalled=False, recovery_successor=None)
    assert h.agent.store.db.execute("SELECT verify_result FROM actions").fetchone()[0] == "DELIVERY_RESUMED"


def test_response_of_different_request_stays_unknown(harness, tmp_path):
    h = harness
    use_policy(h, tmp_path)
    h.executor.response = {"ok": True, "state": "EFFECT_OBSERVED", "request_id": "other"}
    stall(h)
    assert h.agent.store.db.execute("SELECT state FROM actions").fetchone()[0] == "OUTCOME_UNKNOWN"


@pytest.mark.parametrize("fault", ["request_id", "executor_instance_id", "effect_boundary_reached", "physical_attempt_count"])
def test_status_cannot_resolve_unknown_without_bound_terminal_evidence(harness, tmp_path, fault):
    h = harness
    use_policy(h, tmp_path)
    h.executor.response = TimeoutError("reply lost")
    stall(h)
    row = h.agent.store.db.execute("SELECT * FROM actions").fetchone()
    status = {
        "schema_version": "runtime.effect_request_status_response.v1",
        "ok": True,
        "request_id": row["request_id"],
        "executor_instance_id": "exec-1",
        "state": "EFFECT_OBSERVED",
        "effect_boundary_reached": True,
        "physical_attempt_count": 1,
        "effect_scope_state": "EFFECT_OBSERVED",
        "effect_scope_id": "scope-1",
    }
    invalid = {
        **status,
        fault: False if fault == "effect_boundary_reached" else (True if fault == "physical_attempt_count" else "other"),
    }
    h.agent.status_reader = lambda *_: invalid
    h.pod.new_generation()
    h.pod.uptime = 0
    h.tick(stalled=False)
    assert h.agent.store.db.execute("SELECT state FROM actions").fetchone()[0] == "OUTCOME_UNKNOWN"
    h.agent.status_reader = lambda *_: status
    h.tick(stalled=False)
    row = h.agent.store.db.execute("SELECT * FROM actions").fetchone()
    assert row["state"] == "EFFECT_OBSERVED" and row["verify_result"] == "DELIVERY_RESUMED"
    assert h.kinds().count("action_reserved") == 1 and h.kinds().count("action_reconciled") == 1


def test_central_verifies_observed_progress_in_audit(harness, tmp_path):
    import copy

    from cra.central.verify import verify

    h = harness
    use_policy(h, tmp_path)
    stall(h)
    h.pod.new_generation()
    h.pod.uptime = 0
    h.tick(stalled=False)
    h.tick(stalled=False)
    records = [{"agent_id": "dell", "instance_id": "i", **r} for r in h.agent.store.read_audit(0, 1000)]
    policies = {h.agent.policy().version: h.agent.policy().to_dict()}
    assert verify(records, policies, now=h.clock())["violations"] == []
    corrupted = copy.deepcopy(records)
    next(r for r in corrupted if r["kind"] == "action_verified")["payload"]["sample"]["tcp"]["bytes_sent"] = 0
    assert "RECOVERY_EVIDENCE_INVALID" in {v["code"] for v in verify(corrupted, policies, now=h.clock())["violations"]}
