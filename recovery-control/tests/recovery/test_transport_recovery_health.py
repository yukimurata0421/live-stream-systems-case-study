from __future__ import annotations

import hashlib
import json
import ssl
import urllib.error
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cra_dell_recovery import recovery_health
from cra_dell_recovery.recovery_health import RecoveryHealthStore
from cra_dell_recovery.transport_resilience import classify_failure


def _store(tmp_path: Path) -> RecoveryHealthStore:
    return RecoveryHealthStore(
        state_file=tmp_path / "state.json",
        event_file=tmp_path / "events.jsonl",
        component="arena_dell_pull",
        release_id="arena-cra-projection-0123456789ab",
    )


def test_healthy_polls_do_not_grow_transition_journal(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    for offset in range(100):
        store.record_success(attempt_count=1, now=now + timedelta(seconds=offset))

    assert not (tmp_path / "events.jsonl").exists()
    assert store.snapshot(now=now + timedelta(seconds=100))["event_count"] == 0


def test_retry_episode_is_append_only_hash_chained_and_recovers(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    transient = classify_failure(urllib.error.URLError(TimeoutError("timeout")))
    store.record_failure(transient, attempt=1, exhausted=False, now=now)
    store.record_failure(transient, attempt=2, exhausted=True, now=now + timedelta(seconds=2))
    store.record_success(attempt_count=1, now=now + timedelta(seconds=5))

    snapshot = store.snapshot(now=now + timedelta(seconds=6))
    assert snapshot["current_state"] == "READY"
    assert snapshot["episode_count"] == 1
    assert snapshot["recovered_episode_count"] == 1
    assert snapshot["exhausted_invocation_count"] == 1
    assert snapshot["maximum_recovery_duration_ms"] == 5000
    events = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
    assert [event["event"] for event in events] == ["FAILURE_OBSERVED", "FAILURE_OBSERVED", "RECOVERED"]
    previous = "0" * 64
    for sequence, event in enumerate(events, start=1):
        event_hash = event.pop("event_hash")
        assert event["sequence"] == sequence
        assert event["previous_event_hash"] == previous
        canonical = json.dumps(event, separators=(",", ":"), sort_keys=True).encode()
        assert event_hash == hashlib.sha256(previous.encode() + b"\0" + canonical).hexdigest()
        previous = event_hash


def test_certificate_failure_never_becomes_recovering(tmp_path: Path) -> None:
    store = _store(tmp_path)
    classification = classify_failure(urllib.error.URLError(ssl.SSLCertVerificationError("expired")))
    state = store.record_failure(classification, attempt=1, exhausted=True)
    assert state["current_state"] == "SAFE_BLOCKED"
    assert state["terminal_episode_count"] == 1
    assert state["current_episode"] is None


def test_crash_after_event_fsync_replays_journal_and_repairs_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    transient = classify_failure(urllib.error.URLError(TimeoutError("timeout")))
    real_atomic_json = recovery_health._atomic_json
    calls = 0

    def crash_once(path: Path, value: dict[str, object]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("injected crash after event fsync")
        real_atomic_json(path, value)

    monkeypatch.setattr(recovery_health, "_atomic_json", crash_once)
    with pytest.raises(OSError, match="injected crash"):
        store.record_failure(transient, attempt=1, exhausted=True, now=now)
    assert not store.state_file.exists()
    assert store.event_file.exists()

    repaired = store.snapshot(now=now + timedelta(seconds=1))
    assert repaired["current_state"] == "RECOVERING"
    assert repaired["event_count"] == 1
    assert repaired["failure_attempt_count"] == 1
    assert store.state_file.exists()

    store.record_success(attempt_count=1, now=now + timedelta(seconds=2))
    final = store.snapshot(now=now + timedelta(seconds=3))
    assert final["current_state"] == "READY"
    assert final["recovered_episode_count"] == 1
    assert final["event_count"] == 2


def test_terminal_recovery_is_journaled_and_replayable(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    terminal = classify_failure(urllib.error.URLError(ssl.SSLCertVerificationError("expired")))
    store.record_failure(terminal, attempt=1, exhausted=True, now=now)
    store.record_success(attempt_count=1, now=now + timedelta(seconds=1))
    store.state_file.unlink()

    replayed = store.snapshot(now=now + timedelta(seconds=2))
    assert replayed["current_state"] == "READY"
    assert replayed["terminal_episode_count"] == 1
    assert replayed["event_count"] == 2
    events = [json.loads(line) for line in store.event_file.read_text().splitlines()]
    assert [event["event"] for event in events] == ["FAILURE_OBSERVED", "READY_RESTORED"]


def test_event_chain_tampering_is_fail_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    transient = classify_failure(urllib.error.URLError(TimeoutError("timeout")))
    store.record_failure(transient, attempt=1, exhausted=True)
    value = json.loads(store.event_file.read_text())
    value["reason_code"] = "TAMPERED"
    store.event_file.write_text(json.dumps(value) + "\n")

    with pytest.raises(ValueError, match="EVENT_HASH_INVALID"):
        store.snapshot()
