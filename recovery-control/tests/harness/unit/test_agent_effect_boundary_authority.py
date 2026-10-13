"""Authority must still be valid at the physical-effect reservation boundary."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import pytest

from cra_dell_recovery.crash import CrashPoint, InjectedCrash, crash_at
from cra_dell_recovery.models import LocalRecoveryCandidate
from cra_dell_recovery.time import isoformat_utc, parse_utc, utc_now
from cra_harness.controls.environment import HarnessEnvironment
from dell_recovery_agent import execution

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize("drift", ["deadline", "authority_revoked", "command_expired", "policy_expired", "session_rotated"])
def test_pre_effect_authority_drift_cannot_reach_adapter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, drift: str) -> None:
    env = HarnessEnvironment.create(tmp_path, ROOT)
    try:
        command = env.command("pre-effect-" + drift)
        ticks = [100.0]
        calls = [0]

        class Observer:
            def observe(self):  # type: ignore[no-untyped-def]
                calls[0] += 1
                if calls[0] == 2:
                    if drift == "deadline":
                        ticks[0] += 4.25
                    elif drift == "authority_revoked":
                        env.dell.set_process_lease_valid("stream-target", False)
                        env.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "TEST_REVOKED")
                    elif drift == "command_expired":
                        monkeypatch.setattr(
                            execution, "utc_now", lambda: parse_utc(str(command["expires_at"])) + timedelta(seconds=1)
                        )
                    elif drift == "policy_expired":
                        env.dell.connection.execute(
                            "UPDATE agent_safety_policies SET expires_at=? WHERE target_id='stream-target'",
                            ("2000-01-01T00:00:00.000Z",),
                        )
                    else:
                        env.dell.connection.execute(
                            "UPDATE authority_fences SET active_authority_session_id='other' WHERE target_id='stream-target'"
                        )
                return env.target

        service = execution.AgentService(
            env.dell, env.agent_codec, Observer(), env.adapter,
            critical_db_deadline_seconds=3, monotonic=lambda: ticks[0],
        )
        receipt = service.handle_command(command)
        assert env.adapter.attempt_count == 0
        assert receipt["command_state"] == "REJECTED"
        scope = env.dell.connection.execute(
            "SELECT physical_attempt_count,state FROM effect_scope_fences WHERE owner_id=?", (command["command_id"],)
        ).fetchone()
        assert tuple(scope) == (0, "RELEASED_NO_EFFECT")
    finally:
        env.close()


def test_writer_wait_exhausts_authority_before_effect(tmp_path: Path) -> None:
    env = HarnessEnvironment.create(tmp_path, ROOT)
    try:
        command = env.command("pre-effect-writer-wait")
        ticks = [100.0]
        writes = [0]
        original = env.dell.write

        @contextmanager
        def delayed_write() -> Iterator[object]:
            writes[0] += 1
            if writes[0] == 3:
                ticks[0] += 4
            with original() as db:
                yield db

        env.dell.write = delayed_write  # type: ignore[method-assign]
        receipt = execution.AgentService(
            env.dell, env.agent_codec, execution.FakeTargetObserver(env.target, env.target), env.adapter,
            monotonic=lambda: ticks[0],
        ).handle_command(command)
        assert receipt["command_state"] == "REJECTED"
        assert env.adapter.attempt_count == 0
        assert env.dell.connection.execute(
            "SELECT physical_attempt_count FROM effect_scope_fences WHERE owner_id=?", (command["command_id"],)
        ).fetchone()[0] == 0
    finally:
        env.close()


def test_crash_after_effect_boundary_is_conservative_unknown_without_retry(tmp_path: Path) -> None:
    env = HarnessEnvironment.create(tmp_path, ROOT)
    try:
        command = env.command("effect-boundary-crash")
        service = env.service(env.target, env.target)
        with pytest.raises(InjectedCrash):
            service.handle_command(command, crash=crash_at(CrashPoint.DELL_AFTER_EFFECT_BOUNDARY_COMMIT))
        assert env.adapter.attempt_count == 0
        env.dell.startup_recover("stream-target")
        retry = service.handle_command(command)
        assert env.adapter.attempt_count == 0
        assert retry["disposition"] == "DUPLICATE"
        assert env.dell.connection.execute(
            "SELECT physical_attempt_count FROM effect_scope_fences WHERE owner_id=?", (command["command_id"],)
        ).fetchone()[0] == 1
    finally:
        env.close()


@pytest.mark.parametrize("drift", ["authority", "policy", "hourly_budget", "daily_budget"])
def test_local_fallback_rechecks_authority_at_effect_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, drift: str
) -> None:
    env = HarnessEnvironment.create(tmp_path, ROOT)
    try:
        env.dell.set_authority_state("stream-target", "LOCAL_FALLBACK", "TEST_FALLBACK")
        candidate = LocalRecoveryCandidate(
            "local-boundary-" + drift, "stream-target", "restart_ffmpeg", "confirmed_tcp_stall",
            env.target, {"tcp_stall_confirmed": True, "distinct_sample_count": 3,
                         "stall_confirm_threshold": 3}, isoformat_utc(utc_now()),
        )
        original_audit = execution.audit_maintenance_decision

        def drift_at_boundary(**kwargs):  # type: ignore[no-untyped-def]
            if kwargs.get("path_id") == "MP-09" and kwargs.get("phase") == "EFFECT_BOUNDARY":
                if drift == "authority":
                    env.dell.set_authority_state("stream-target", "CENTRAL_SUSPECT", "TEST_REVOKED")
                elif drift == "policy":
                    env.dell.connection.execute(
                        "UPDATE agent_safety_policies SET expires_at=? WHERE target_id='stream-target'",
                        ("2000-01-01T00:00:00.000Z",),
                    )
                elif drift == "hourly_budget":
                    env.dell.connection.execute(
                        "UPDATE agent_safety_policies SET hourly_action_cost_limit_sec=1 WHERE target_id='stream-target'"
                    )
                else:
                    env.dell.connection.execute(
                        "UPDATE agent_safety_policies SET daily_action_cost_limit_sec=1 WHERE target_id='stream-target'"
                    )
            return original_audit(**kwargs)

        monkeypatch.setattr(execution, "audit_maintenance_decision", drift_at_boundary)
        result = env.service(env.target, env.target).handle_local_candidate(candidate)
        assert result == {
            "authority": "LOCAL_AUTHORITY_CHANGED_BEFORE_EFFECT",
            "policy": "SAFETY_POLICY_UNAVAILABLE",
            "hourly_budget": "HOURLY_ACTION_BUDGET_EXCEEDED",
            "daily_budget": "DAILY_ACTION_BUDGET_EXCEEDED",
        }[drift]
        assert env.adapter.attempt_count == 0
        scope = env.dell.connection.execute(
            "SELECT physical_attempt_count,state FROM effect_scope_fences WHERE owner_id=?", (candidate.local_action_id,)
        ).fetchone()
        assert tuple(scope) == (0, "RELEASED_NO_EFFECT")
    finally:
        env.close()
