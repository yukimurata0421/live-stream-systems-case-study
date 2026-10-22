"""Minimal v2 behaviour: local observations never substitute for an execution result."""

import copy

import pytest

from cra.central.verify import verify
from tests.reflex.test_reflex_agent import stall, use_policy


def row(h):
    return h.agent.store.db.execute("SELECT * FROM actions").fetchone()


def terminal(h):
    return {
        "schema_version": "runtime.effect_request_status_response.v1",
        "ok": True,
        "request_id": row(h)["request_id"],
        "state": "EFFECT_OBSERVED",
        "effect_boundary_reached": True,
        "physical_attempt_count": 1,
        "effect_scope_state": "EFFECT_OBSERVED_AWAITING_VERIFICATION",
        "effect_scope_id": "scope-1",
    }


def report(h, records=None):
    records = records or [{"agent_id": "dell", "instance_id": "i", **r} for r in h.agent.store.read_audit(0, 1000)]
    return verify(records, {h.agent.policy().version: h.agent.policy().to_dict()}, now=h.clock())


def test_v4_fields_never_select_v4_request(harness, tmp_path):
    h = harness
    h.pod.bound = True
    use_policy(h, tmp_path)
    stall(h)
    request = h.executor.requests[0]
    assert request["schema_version"] == "runtime.typed_effect_request.v2"
    assert "expected_transport" not in request


def test_legacy_v2_requires_runtime_and_route_context(harness, tmp_path):
    h = harness
    use_policy(h, tmp_path)
    for _ in range(6):
        decision = h.tick(stalled=True, executor_instance_id="")
    assert "RUNTIME_CONTEXT_UNAVAILABLE" in decision.blockers
    assert h.executor.requests == []


def test_exit_start_gap_does_not_finalize_generation_verification(harness, tmp_path):
    h = harness
    use_policy(h, tmp_path)
    stall(h)
    h.tick(stalled=False, ffmpeg_running=False, ffmpeg_generation="", target_snapshot_status="UNKNOWN")
    assert row(h)["verify_result"] is None
    h.pod.new_generation()
    h.pod.uptime = 0
    h.tick(stalled=False)
    h.tick(stalled=False)
    assert row(h)["verify_result"] == "DELIVERY_RESUMED"


def test_verification_keeps_original_policy_threshold(harness, tmp_path):
    h = harness
    original = use_policy(h, tmp_path)
    stall(h)
    current = use_policy(h, tmp_path, version=2, mode="off", stall_lastsnd_ms=10000)
    h.pod.new_generation()
    h.pod.uptime = 0
    for n in range(2):
        h.tick(stalled=False, tcp_metrics={"bytes_sent": n + 100, "lastsnd_ms": 5000})
    assert row(h)["verify_result"] is None
    h.tick(stalled=False)
    assert row(h)["verify_result"] == "DELIVERY_RESUMED"
    records = [{"agent_id": "dell", "instance_id": "i", **r} for r in h.agent.store.read_audit(0, 1000)]
    assert verify(records, {1: original.to_dict(), 2: current.to_dict()}, now=h.clock())["violations"] == []


def test_missing_observation_resets_progress_baseline(harness, tmp_path):
    h = harness
    use_policy(h, tmp_path)
    stall(h)
    h.pod.new_generation()
    h.pod.uptime = 0
    h.tick(stalled=False)
    h.config.observation_path.unlink()
    h.agent.step()
    h.tick(stalled=False)
    assert row(h)["verify_result"] is None
    h.tick(stalled=False)
    assert row(h)["verify_result"] == "DELIVERY_RESUMED"


@pytest.mark.parametrize("state", ["EXECUTION_STARTED", "OUTCOME_UNKNOWN"])
def test_legacy_terminal_query_resolves_without_new_fields(harness, tmp_path, state):
    h = harness
    use_policy(h, tmp_path)
    h.executor.response = {"ok": True, "state": state}
    stall(h)
    assert row(h)["state"] == "OUTCOME_UNKNOWN"
    reads = []
    h.agent.status_reader = lambda _, rid: reads.append(rid) or terminal(h)
    h.pod.new_generation()
    h.pod.uptime = 0
    h.tick(stalled=False)
    assert row(h)["state"] == "EFFECT_OBSERVED" and row(h)["verify_result"] is None
    h.tick(stalled=False)
    assert row(h)["verify_result"] == "DELIVERY_RESUMED"
    assert reads == [row(h)["request_id"]] and len(h.executor.requests) == 1
    assert report(h)["violations"] == []


@pytest.mark.parametrize("change", ["epoch", "pod", "stale", "regressed"])
def test_legacy_query_observation_drift_cannot_confirm_execution(harness, tmp_path, change):
    h = harness
    use_policy(h, tmp_path)
    h.executor.response = TimeoutError()
    stall(h)

    def query(*_):
        status = terminal(h)
        overrides = (
            {"executor_instance_id": "other"}
            if change == "epoch"
            else ({"pod_uid": "other"} if change == "pod" else {"sequence": 0} if change == "regressed" else {})
        )
        h.pod.write(stalled=True, **overrides)
        if change == "stale":
            h.clock.advance(4)
        return status

    h.agent.status_reader = query
    h.tick(stalled=True)
    assert row(h)["state"] == "OUTCOME_UNKNOWN" and row(h)["verify_result"] is None
    assert h.kinds().count("action_reconciled") == 0
    assert len(h.executor.requests) == 1


def test_unknown_delivery_progress_does_not_credit_execution_or_resend(harness, tmp_path):
    h = harness
    use_policy(h, tmp_path)
    h.executor.response = TimeoutError()
    stall(h)
    h.pod.new_generation()
    h.pod.uptime = 0
    h.tick(stalled=False)
    h.tick(stalled=False)
    assert row(h)["verify_result"] is None  # allow the async terminal result to arrive
    for _ in range(40):
        h.tick(stalled=False)
    assert row(h)["state"] == "OUTCOME_UNKNOWN"
    assert row(h)["verify_result"] == "DELIVERY_RESUMED_EXECUTION_UNKNOWN"
    assert report(h)["violations"] == []
    h.agent.store.close()
    h.agent = h.new_agent()
    assert row(h)["state"] == "OUTCOME_UNKNOWN" and len(h.executor.requests) == 1
    assert h.agent.store.history(now=h.clock(), generation_key="pod-a/gen-1", mode="enforce").actions_last_hour == 1


@pytest.mark.parametrize("fault", ["constant", "duplicate", "gap", "reset", "route"])
def test_new_generation_alone_or_bad_progress_never_means_resumed(harness, tmp_path, fault):
    h = harness
    use_policy(h, tmp_path)
    stall(h)
    h.pod.new_generation()
    h.pod.uptime = 0
    h.tick(stalled=False)
    if fault == "constant":
        h.pod.bytes_sent -= 500_000
    elif fault == "duplicate":
        h.pod.sequence -= 1
    elif fault == "gap":
        h.clock.advance(6)
    elif fault == "reset":
        h.pod.bytes_sent = 0
    h.tick(stalled=False, **({"delivery_route_generation": 2} if fault == "route" else {}))
    assert row(h)["verify_result"] != "DELIVERY_RESUMED"


@pytest.mark.parametrize("fault", ["bytes", "sequence", "executor", "timestamp", "route", "state", "uptime"])
def test_central_rejects_bad_delivery_facts_without_any_successor_hook(harness, tmp_path, fault):
    h = harness
    use_policy(h, tmp_path)
    stall(h)
    h.pod.new_generation()
    h.pod.uptime = 0
    h.tick(stalled=False)
    h.tick(stalled=False)
    records = [{"agent_id": "dell", "instance_id": "i", **r} for r in h.agent.store.read_audit(0, 1000)]
    assert report(h, records)["violations"] == []
    records = copy.deepcopy(records)
    p = next(r["payload"] for r in records if r["kind"] == "action_verified")
    first, second = p["progress_from"], p["sample"]
    if fault == "bytes":
        second["tcp"]["bytes_sent"] = first["tcp"]["bytes_sent"]
    elif fault == "sequence":
        second["sequence"] = first["sequence"]
    elif fault == "executor":
        first["executor_instance_id"] = second["executor_instance_id"] = "other"
    elif fault == "timestamp":
        first["observed_at"] = second["observed_at"]
    elif fault == "route":
        first["delivery_route"] = second["delivery_route"] = "other"
    elif fault == "state":
        p["execution_state"] = "OUTCOME_UNKNOWN"
    else:
        second["uptime_sec"] = 10000
    assert "RECOVERY_EVIDENCE_INVALID" in {v["code"] for v in report(h, records)["violations"]}
