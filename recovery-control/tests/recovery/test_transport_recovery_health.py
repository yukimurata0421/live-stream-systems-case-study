from __future__ import annotations

import hashlib
import json
import os
import ssl
import urllib.error
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cra_dell_recovery import recovery_health
from cra_dell_recovery.errors import EvidenceFreshnessError
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


def test_evidence_freshness_exhaustion_remains_recoverable_and_closes_on_refresh(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    freshness = classify_failure(EvidenceFreshnessError("RESILIENT_STATUS_PULL_INSUFFICIENT_EVIDENCE_VALIDITY"))

    recovering = store.record_failure(freshness, attempt=1, exhausted=True, now=now)

    assert recovering["current_state"] == "RECOVERING"
    assert recovering["terminal_episode_count"] == 0
    recovered = store.record_success(attempt_count=1, now=now + timedelta(seconds=5))
    assert recovered["current_state"] == "READY"
    assert recovered["recovered_episode_count"] == 1
    assert recovered["terminal_episode_count"] == 0


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
        if calls == 2:
            raise OSError("injected crash after event fsync")
        real_atomic_json(path, value)

    monkeypatch.setattr(recovery_health, "_atomic_json", crash_once)
    with pytest.raises(OSError, match="injected crash"):
        store.record_failure(transient, attempt=1, exhausted=True, now=now)
    assert store.state_file.exists()
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


def test_matching_legacy_state_migrates_without_full_journal_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    transient = classify_failure(urllib.error.URLError(TimeoutError("timeout")))
    store.record_failure(transient, attempt=1, exhausted=True, now=now)
    state = json.loads(store.state_file.read_text())
    state["schema"] = recovery_health.LEGACY_STATE_SCHEMA
    del state["event_verified_bytes"]
    del state["pending_event"]
    store.state_file.write_text(json.dumps(state) + "\n")
    monkeypatch.setattr(recovery_health, "MAXIMUM_EVENT_REPLAY_BYTES", 1)

    snapshot = store.snapshot(now=now + timedelta(seconds=1))

    migrated = json.loads(store.state_file.read_text())
    assert snapshot["event_count"] == 1
    assert migrated["schema"] == recovery_health.STATE_SCHEMA
    assert migrated["event_verified_bytes"] == store.event_file.stat().st_size
    assert migrated["pending_event"] is None


def test_legacy_state_with_unrecognized_fields_is_not_migrated(tmp_path: Path) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    transient = classify_failure(urllib.error.URLError(TimeoutError("timeout")))
    store.record_failure(transient, attempt=1, exhausted=True, now=now)
    state = json.loads(store.state_file.read_text())
    state["schema"] = recovery_health.LEGACY_STATE_SCHEMA
    del state["event_verified_bytes"]
    del state["pending_event"]
    state["unrecognized_watermark"] = store.event_file.stat().st_size
    store.state_file.write_text(json.dumps(state) + "\n")

    with pytest.raises(ValueError, match="RECOVERY_HEALTH_STATE_INVALID"):
        store.snapshot(now=now + timedelta(seconds=1))


def test_partial_reserved_event_is_completed_exactly_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    transient = classify_failure(urllib.error.URLError(TimeoutError("timeout")))
    real_write_all = recovery_health._write_all
    injected = False

    def partial_once(descriptor: int, raw: bytes) -> None:
        nonlocal injected
        if not injected:
            injected = True
            os.write(descriptor, raw[: len(raw) // 2])
            raise OSError("injected partial event write")
        real_write_all(descriptor, raw)

    monkeypatch.setattr(recovery_health, "_write_all", partial_once)
    with pytest.raises(OSError, match="partial event"):
        store.record_failure(transient, attempt=1, exhausted=True, now=now)
    partial = store.event_file.read_bytes()
    assert partial and not partial.endswith(b"\n")

    repaired = store.snapshot(now=now + timedelta(seconds=1))
    lines = store.event_file.read_text().splitlines()
    assert repaired["event_count"] == 1
    assert len(lines) == 1
    assert json.loads(lines[0])["sequence"] == 1


def test_conflicting_partial_reserved_event_is_preserved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    transient = classify_failure(urllib.error.URLError(TimeoutError("timeout")))

    def conflicting_write(descriptor: int, _raw: bytes) -> None:
        os.write(descriptor, b'{"different":')
        raise OSError("injected conflicting event write")

    monkeypatch.setattr(recovery_health, "_write_all", conflicting_write)
    with pytest.raises(OSError, match="conflicting event"):
        store.record_failure(transient, attempt=1, exhausted=True, now=now)
    before = store.event_file.read_bytes()

    with pytest.raises(ValueError, match="PENDING_EVENT_CONFLICT"):
        store.snapshot(now=now + timedelta(seconds=1))
    assert store.event_file.read_bytes() == before


def test_full_replay_rejects_journal_growth_beyond_opened_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    transient = classify_failure(urllib.error.URLError(TimeoutError("timeout")))
    store.record_failure(transient, attempt=1, exhausted=True, now=now)
    store.state_file.unlink()
    original = store.event_file.read_bytes()
    real_lines = recovery_health._frozen_event_lines
    injected = False

    def grow_after_open(handle: object, *, start: int, end: int):
        nonlocal injected
        for raw in real_lines(handle, start=start, end=end):
            yield raw
            if not injected:
                injected = True
                with store.event_file.open("ab") as stream:
                    stream.write(original)
                    stream.flush()
                    os.fsync(stream.fileno())

    monkeypatch.setattr(recovery_health, "_frozen_event_lines", grow_after_open)

    with pytest.raises(ValueError, match="EVENT_FILE_CHANGED_DURING_READ"):
        store.snapshot(now=now + timedelta(seconds=1))
    assert store.event_file.read_bytes() == original * 2


def test_suffix_replay_rejects_journal_growth_beyond_opened_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    now = datetime(2026, 9, 2, 7, 0, tzinfo=UTC)
    transient = classify_failure(urllib.error.URLError(TimeoutError("timeout")))
    store.record_failure(transient, attempt=1, exhausted=True, now=now)
    committed_state = store.state_file.read_bytes()
    store.record_failure(transient, attempt=2, exhausted=True, now=now + timedelta(seconds=1))
    store.state_file.write_bytes(committed_state)
    original = store.event_file.read_bytes()
    real_lines = recovery_health._frozen_event_lines
    injected = False

    def grow_after_open(handle: object, *, start: int, end: int):
        nonlocal injected
        for raw in real_lines(handle, start=start, end=end):
            yield raw
            if start and not injected:
                injected = True
                with store.event_file.open("ab") as stream:
                    stream.write(b"unverified-suffix")
                    stream.flush()
                    os.fsync(stream.fileno())

    monkeypatch.setattr(recovery_health, "_frozen_event_lines", grow_after_open)

    with pytest.raises(ValueError, match="EVENT_FILE_CHANGED_DURING_READ"):
        store.snapshot(now=now + timedelta(seconds=2))
    assert store.event_file.read_bytes() == original + b"unverified-suffix"
