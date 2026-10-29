"""Verify observation-only recorder boundaries with real SQLite and synthetic sources."""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "ops/scripts/stream_v3_transport_recorder.py"
SPEC = importlib.util.spec_from_file_location("transport_recorder", SCRIPT)
m = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = m
SPEC.loader.exec_module(m)
BASE = datetime(2026, 10, 29, tzinfo=timezone.utc).timestamp()


def sample(
    t, *, ack=None, conn="conn1", generation="gen1", state="ACK_ADVANCING", notsent=0
):
    return {
        "schema": "stream_v3.ffmpeg_transport.v1",
        "observed_at": datetime.fromtimestamp(BASE + t, timezone.utc).isoformat(),
        "ffmpeg_pid": 583,
        "ffmpeg_generation": generation,
        "connection_id": conn,
        "connection_generation": 1 if conn == "conn1" else 2,
        "state": state,
        "fifo_recovering": False,
        "ack_bytes_observed_total": 1000000,
        "connections_observed_total": 1,
        "reconnections_observed_total": 0,
        "fifo_recovery_reports_total": 0,
        "fifo_recovery_failures_total": 0,
        "fifo_queue_overflows_total": 0,
        "tcp_metrics": {
            "bytes_acked": int(t * 500000) if ack is None else ack,
            "bytes_sent": int(t * 500000),
            "notsent": notsent,
            "send_q": notsent,
            "unacked": 0,
            "lastsnd_ms": 40,
            "rto_ms": 250,
        },
    }


@pytest.fixture
def recorder(tmp_path):
    store = m.Store(tmp_path / "state", m.Config())
    r = m.Recorder(store, now=BASE, mono=0)
    yield r
    store.close()


def step(r, t, value=None, **kw):
    return r.step(sample(t) if value is None else value, now=BASE + t, mono=t, **kw)


def kinds(r):
    return [x[0] for x in r.store.db.execute("SELECT kind FROM triggers ORDER BY id")]


def test_normal_traffic_only_writes_minutes_and_exposes_no_fault_label(recorder):
    for t in range(65):
        frame, reasons = step(recorder, t)
        assert reasons == []
    assert frame["ack_mbps"] == 4
    assert recorder.store.db.execute("SELECT count(*) FROM episodes").fetchone()[0] == 0
    payload = json.loads(
        recorder.store.db.execute("SELECT payload FROM minutes").fetchone()[0]
    )
    assert payload["counts"]["FRESH"] == 60
    assert payload["metrics"]["ack_mbps"]["mean"] == 4
    assert payload["fault_label"] == "UNASSESSED"


def test_queue_trigger_uses_elapsed_source_time_not_sample_count(recorder):
    for t in (0, 1.1, 2.2):
        _, reasons = step(recorder, t, sample(t, notsent=300000))
        assert "QUEUE_RECORDING_CANDIDATE" not in reasons
    _, reasons = step(recorder, 3.3, sample(3.3, notsent=300000))
    assert reasons == ["QUEUE_RECORDING_CANDIDATE"]
    step(recorder, 4.4, sample(4.4, notsent=300000))
    assert kinds(recorder).count("QUEUE_RECORDING_CANDIDATE") == 1


def test_event_persists_full_prehistory_and_readonly_timeline(recorder):
    for t in range(121):
        step(recorder, t)
    v = sample(121, state="TCP_ACK_STALLED")
    step(recorder, 121, v)
    rows = list(m.readonly_report(recorder.store.directory, 1))
    assert len(rows) == 121
    assert rows[0]["received_us"] == int((BASE + 1) * 1e6)
    assert rows[-1]["source"]["state"] == "TCP_ACK_STALLED"
    assert (
        recorder.store.db.execute("SELECT prehistory_seconds FROM episodes").fetchone()[
            0
        ]
        == 120
    )
    r = next(m.readonly_report(recorder.store.directory))
    assert r["episodes"][0]["triggers"][0]["kind"] == "STATE_TCP_ACK_STALLED"
    assert r["action_enabled"] is False


def test_duplicate_poll_is_not_zero_rate_or_extra_saved_sample(recorder):
    step(recorder, 0, sample(0, state="TCP_ACK_STALLED"))
    v = sample(1)
    step(recorder, 1, v)
    frame, reasons = step(recorder, 1.5, v)
    assert frame["quality"] == "DUPLICATE" and frame["ack_mbps"] is None
    assert reasons == []
    assert recorder.store.db.execute("SELECT count(*) FROM samples").fetchone()[0] == 2


def test_new_fresh_source_with_zero_delta_is_real_zero_rate(recorder):
    step(recorder, 0, sample(0, ack=100))
    frame, _ = step(recorder, 1, sample(1, ack=100))
    assert frame["quality"] == "FRESH" and frame["ack_mbps"] == 0


def test_connection_change_never_bridges_counters(recorder):
    step(recorder, 0, sample(0, ack=10000000))
    frame, reasons = step(recorder, 1, sample(1, ack=1, conn="conn2"))
    assert frame["ack_mbps"] is None
    assert "CONNECTION_CHANGED" in reasons


def test_process_restart_with_same_connection_label_is_preserved(recorder):
    step(recorder, 0)
    frame, reasons = step(recorder, 1, sample(1, generation="gen2"))
    assert "PROCESS_GENERATION_CHANGED" in reasons and frame["ack_mbps"] is None


@pytest.mark.parametrize(
    "state", ["SOCKET_UNAVAILABLE", "COUNTER_REGRESSION", "TCP_ACK_STALLED"]
)
def test_all_abnormal_states_trigger_once_on_entry(recorder, state):
    step(recorder, 0)
    v = sample(1, state=state)
    if state == "SOCKET_UNAVAILABLE":
        v["tcp_metrics"] = {}
    _, reasons = step(recorder, 1, v)
    assert "STATE_" + state in reasons
    v["observed_at"] = sample(2)["observed_at"]
    _, reasons = step(recorder, 2, v)
    assert "STATE_" + state not in reasons


def test_starting_is_warmup_not_fault(recorder):
    _, reasons = step(recorder, 0, sample(0, state="STARTING"))
    assert not reasons


def test_fifo_counter_catches_error_and_reopen_between_polls(recorder):
    step(recorder, 0)
    v = sample(1)
    v["fifo_recovery_failures_total"] = 1
    v["fifo_recovery_reports_total"] = 1
    _, reasons = step(recorder, 1, v)
    assert reasons.count("FIFO_COUNTER_CHANGED") == 2
    assert v["fifo_recovering"] is False


def test_missing_source_is_unknown_not_stall_and_deadline_advances(recorder):
    step(recorder, 0)
    for t in range(1, 7):
        f, _ = step(recorder, t, read_error="READ_UNAVAILABLE")
    assert f["quality"] == "UNAVAILABLE" and f["ack_mbps"] is None
    assert kinds(recorder) == ["SOURCE_UNAVAILABLE"]
    for t in range(7, 190):
        step(recorder, t, read_error="READ_UNAVAILABLE")
    assert (
        recorder.store.db.execute("SELECT closed_reason FROM episodes").fetchone()[0]
        == "WINDOW_COMPLETE"
    )


def test_stale_retained_snapshot_triggers_after_five_seconds(recorder):
    v = sample(0)
    for t in range(7):
        step(recorder, t, v)
    assert "SOURCE_UNAVAILABLE" in kinds(recorder)
    first = recorder.store.db.execute(
        "SELECT received_us FROM triggers WHERE kind='SOURCE_UNAVAILABLE'"
    ).fetchone()[0]
    assert first <= int((BASE + 6) * 1e6)


def test_read_failure_breaks_derivative_even_after_intervening_duplicate(recorder):
    v = sample(0)
    step(recorder, 0, v)
    step(recorder, 1, read_error="READ_UNAVAILABLE")
    step(recorder, 2, v)
    f, _ = step(recorder, 3)
    assert f["ack_mbps"] is None
    assert step(recorder, 4)[0]["ack_mbps"] == 4


def test_gap_resets_queue_duration_and_is_not_interpolated(recorder):
    step(recorder, 0, sample(0, notsent=300000))
    step(recorder, 1, sample(1, notsent=300000))
    f, reasons = step(recorder, 10, sample(10, notsent=300000))
    assert f["ack_mbps"] is None and "COLLECTOR_GAP" in reasons
    assert "QUEUE_RECORDING_CANDIDATE" not in reasons


def test_counter_and_source_time_regression_never_become_negative_rates(recorder):
    step(recorder, 0, sample(0, ack=1000))
    f, reasons = step(recorder, 1, sample(1, ack=1))
    assert f["ack_mbps"] is None and "COUNTER_REGRESSION" in reasons
    f, reasons = step(recorder, 2, sample(0.5, ack=2))
    assert f["ack_mbps"] is None and "SOURCE_TIME_REGRESSION" in reasons


def test_clock_rollback_preserves_both_minute_segments(recorder):
    for t in range(61):
        step(recorder, t)
    recorder.step(sample(1), now=BASE + 1, mono=61)
    recorder.finish(now=BASE + 2)
    assert "COLLECTOR_CLOCK_JUMP" in kinds(recorder)
    assert (
        recorder.store.db.execute(
            "SELECT count(*) FROM minutes WHERE bucket_us=?", (int(BASE * 1e6),)
        ).fetchone()[0]
        == 2
    )


def test_episode_cap_and_shutdown_are_explicit_truncations(tmp_path):
    config = m.Config(post_seconds=4, max_episode_seconds=6)
    s = m.Store(tmp_path / "s", config)
    try:
        r = m.Recorder(s, now=BASE, mono=0)
        for t in range(9):
            v = sample(t)
            v["fifo_queue_overflows_total"] = t
            step(r, t, v)
        r.finish(now=BASE + 9)
        assert [
            x[0] for x in s.db.execute("SELECT closed_reason FROM episodes ORDER BY id")
        ] == ["MAX_DURATION", "RECORDER_STOPPED"]
    finally:
        s.close()


def test_restart_preserves_history_and_marks_interrupted_episode(tmp_path):
    p = tmp_path / "s"
    s = m.Store(p, m.Config())
    r = m.Recorder(s, now=BASE, mono=0)
    step(r, 0, sample(0, state="TCP_ACK_STALLED"))
    s.close()  # Simulate abrupt recorder exit; no orderly finish call.
    s = m.Store(p, m.Config())
    try:
        m.Recorder(s, now=BASE + 50, mono=0)
        assert (
            s.db.execute("SELECT closed_reason FROM episodes").fetchone()[0]
            == "RECORDER_RESTART_GAP"
        )
        assert s.db.execute("SELECT count(*) FROM samples").fetchone()[0] == 1
        assert s.db.execute("SELECT count(*) FROM sessions").fetchone()[0] == 2
    finally:
        s.close()


def test_single_writer_and_wal_full_settings(recorder):
    s = recorder.store
    with pytest.raises(BlockingIOError):
        m.Store(s.directory, m.Config())
    assert s.db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert s.db.execute("PRAGMA synchronous").fetchone()[0] == 2
    assert next(m.readonly_report(s.directory))["sessions"] == 1


def test_budget_stops_without_deleting_existing_evidence(recorder):
    s = recorder.store
    step(recorder, 0, sample(0, state="TCP_ACK_STALLED"))
    path = s.directory / "budget-fixture"
    with path.open("wb") as f:
        f.truncate(s.config.max_storage_bytes)
    with pytest.raises(m.StorageLimit):
        step(recorder, 1)
    assert s.db.execute("SELECT count(*) FROM samples").fetchone()[0] == 1
    assert path.stat().st_size == s.config.max_storage_bytes


def test_unknown_database_is_rejected_without_altering_it(tmp_path):
    p = tmp_path / "s"
    p.mkdir(mode=0o700)
    dbpath = p / "recorder.sqlite3"
    with sqlite3.connect(dbpath) as db:
        db.execute("CREATE TABLE unrelated(value)")
    original = dbpath.read_bytes()
    with pytest.raises(ValueError, match="NOT_OWNED"):
        m.Store(p, m.Config())
    assert dbpath.read_bytes() == original


def test_input_is_allowlisted_and_never_persists_extra_values(recorder):
    v = sample(0, state="TCP_ACK_STALLED")
    v["unrelated_sensitive_field"] = "fixture-value-that-must-not-be-persisted"
    step(recorder, 0, v)
    assert (
        "fixture-value"
        not in recorder.store.db.execute("SELECT payload FROM samples").fetchone()[0]
    )


def test_unsafe_inputs_and_invalid_numeric_types(tmp_path):
    p = tmp_path / "input"
    p.write_text("x" * 100)
    with pytest.raises(ValueError, match="TOO_LARGE"):
        m.bounded_read(p, 10)
    link = tmp_path / "link"
    link.symlink_to(p)
    with pytest.raises(OSError):
        m.bounded_read(link)
    v = sample(0)
    v["tcp_metrics"]["bytes_acked"] = True
    with pytest.raises(ValueError):
        m.snapshot(v)
    with pytest.raises(ValueError):
        m.Config(max_storage_bytes=0)


def test_journal_bootstrap_rotation_and_deduplication(tmp_path):
    p = tmp_path / "events.jsonl"

    def event(n):
        return {
            "kind": "fifo_recovery_failed",
            "ffmpeg_generation": "gen1",
            "sequence": n,
            "observed_at": sample(n)["observed_at"],
        }

    p.write_text(json.dumps(event(1)) + "\n")
    j = m.JournalReader(p)
    assert j.poll() == []
    p.write_text(json.dumps(event(1)) + "\n" + json.dumps(event(2)) + "\n")
    assert [x["sequence"] for x in j.poll()] == [2]
    p.write_text(json.dumps(event(2)) + "\n")
    assert j.poll() == []
    p.unlink()
    assert j.poll() == [] and j.status == "UNAVAILABLE"


def test_journal_event_retains_source_and_receipt_times(recorder):
    step(recorder, 0)
    e = {
        "kind": "fifo_recovery_failed",
        "generation": "gen1",
        "sequence": 4,
        "source_us": int((BASE + 0.5) * 1e6),
    }
    step(recorder, 2, journal=[e], journal_status="VALID")
    row = recorder.store.db.execute(
        "SELECT received_us,source_us,kind FROM triggers"
    ).fetchone()
    assert row == (int((BASE + 2) * 1e6), e["source_us"], "JOURNAL_EVENT")


def test_corrupt_journal_does_not_advance_cursor_past_unsaved_event(tmp_path):
    p = tmp_path / "events"
    p.write_text("")
    j = m.JournalReader(p)
    j.poll()
    event = {
        "kind": "fifo_output_error_observed",
        "ffmpeg_generation": "gen1",
        "sequence": 1,
        "observed_at": sample(1)["observed_at"],
    }
    p.write_text(json.dumps(event) + "\ninvalid-json\n")
    assert j.poll() == [] and j.status == "UNAVAILABLE"
    p.write_text(json.dumps(event) + "\n")
    assert j.poll()[0]["sequence"] == 1


def test_large_journal_reads_a_bounded_tail_and_reports_limit(tmp_path):
    p = tmp_path / "events"
    p.write_text("")
    j = m.JournalReader(p)
    j.poll()
    event = {
        "kind": "fifo_output_error_observed",
        "ffmpeg_generation": "gen1",
        "sequence": 1,
        "observed_at": sample(1)["observed_at"],
    }
    p.write_text(" " * (600 * 1024) + "\n" + json.dumps(event) + "\n")
    assert j.poll()[0]["sequence"] == 1
    assert j.status == "BOUNDED_TAIL"


def test_clock_context_is_explicit_and_filters_unrelated_fields(recorder):
    clock = {
        "schema": "stream_v3.clock_observation.v1",
        "synchronized": True,
        "observed_at": sample(0)["observed_at"],
        "offset_seconds": 0.01,
        "uncertainty_seconds": 0.1,
        "not_allowed": "fixture-sensitive",
    }
    f, _ = step(recorder, 0, sample(0, state="TCP_ACK_STALLED"), clock=clock)
    assert f["clock"]["offset_seconds"] == 0.01 and f["clock"]["status"] == "VALID"
    assert "fixture-sensitive" not in m.packed(f)
    assert m.clock_facts(None, BASE) == {"status": "UNKNOWN"}
    assert m.clock_facts(clock, BASE + 301) == {"status": "UNKNOWN"}
    clock["synchronized"] = False
    assert m.clock_facts(clock, BASE) == {"status": "UNKNOWN"}


def test_reference_comparison_uses_interval_and_clock_uncertainty():
    r = {
        "last_good_at": sample(10)["observed_at"],
        "first_bad_at": sample(15)["observed_at"],
        "clock_uncertainty_seconds": 1,
    }
    result = m.lead_interval(int((BASE + 12) * 1e6), r)
    assert result["comparison"] == "OVERLAPPING_INTERVALS"
    assert (result["lead_min_seconds"], result["lead_max_seconds"]) == (-3, 4)
    assert (
        m.lead_interval(int((BASE + 5) * 1e6), r)["comparison"]
        == "TRIGGER_PRECEDES_REFERENCE_INTERVAL"
    )
    assert (
        m.lead_interval(int((BASE + 20) * 1e6), r)["comparison"]
        == "REFERENCE_INTERVAL_PRECEDES_TRIGGER"
    )
    r["clock_uncertainty_seconds"] = None
    assert (
        m.lead_interval(int(BASE * 1e6), r)["comparison"]
        == "UNKNOWN_CLOCK_OR_SOURCE_TIME"
    )
    r["clock_uncertainty_seconds"] = -1
    with pytest.raises(ValueError):
        m.lead_interval(int(BASE * 1e6), r)


def test_report_can_compare_queue_trigger_without_claiming_prediction(recorder):
    for t in range(5):
        step(recorder, t, sample(t, notsent=300000))
    ref = {
        "episode_id": 1,
        "last_good_at": sample(2)["observed_at"],
        "first_bad_at": sample(6)["observed_at"],
        "clock_uncertainty_seconds": 0.2,
    }
    result = next(m.readonly_report(recorder.store.directory, references=[ref]))
    assert (
        result["episodes"][0]["reference_comparisons"][0]["comparison"]
        == "OVERLAPPING_INTERVALS"
    )
    assert (
        result["predictive_performance"] == "NOT_EVALUATED_WITHOUT_INDEPENDENT_LABELS"
    )


def test_sqlite_full_keeps_committed_rows_and_integrity(tmp_path):
    s = m.Store(tmp_path / "s", m.Config(max_storage_bytes=1024**2))
    r = m.Recorder(s, now=BASE, mono=0)
    hit_full = False
    try:
        for t in range(1500):
            v = sample(t)
            v["fifo_queue_overflows_total"] = t + 1
            try:
                step(r, t, v)
            except sqlite3.OperationalError as e:
                assert e.sqlite_errorcode == sqlite3.SQLITE_FULL
                hit_full = True
                break
        assert hit_full
        assert s.db.execute("SELECT count(*) FROM samples").fetchone()[0] > 0
        assert s.db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert s.bytes_used() < 1024**2
    finally:
        s.close()


def test_sigkill_and_restart_through_real_cli_preserve_interrupted_record(tmp_path):
    source = tmp_path / "source.json"
    v = sample(0, state="TCP_ACK_STALLED")
    v["observed_at"] = datetime.now(timezone.utc).isoformat()
    source.write_text(json.dumps(v))
    state = tmp_path / "state"
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    cmd = [
        sys.executable,
        "-B",
        str(SCRIPT),
        "collect",
        "--source",
        str(source),
        "--state",
        str(state),
    ]
    child = subprocess.Popen(
        cmd, env=env, cwd=tmp_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        deadline = time.monotonic() + 5
        while not (state / "status.json").exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert (state / "status.json").exists()
        child.kill()  # Only the test-owned recorder process; no stream or systemd operations.
        child.communicate(timeout=3)
        resumed = subprocess.run(
            cmd + ["--steps", "1"],
            env=env,
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        assert resumed.returncode == 0, resumed.stderr
        result = next(m.readonly_report(state))
        assert result["sessions"] == 2
        assert result["episodes"][-1]["closed_reason"] == "RECORDER_RESTART_GAP"
        assert result["episodes"][-1]["sample_count"] == 1
        with sqlite3.connect(state / "recorder.sqlite3") as db:
            assert db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=3)


def test_real_cli_collect_and_readonly_report(tmp_path):
    source = tmp_path / "source.json"
    # Freeze the test source at current wall time; only the owned subprocess reads it.
    value = sample(0, state="TCP_ACK_STALLED")
    value["observed_at"] = datetime.now(timezone.utc).isoformat()
    source.write_text(json.dumps(value))
    state = tmp_path / "state"
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            str(SCRIPT),
            "collect",
            "--source",
            str(source),
            "--state",
            str(state),
            "--steps",
            "2",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
        cwd=tmp_path,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    before = (state / "status.json").read_bytes()
    result = subprocess.run(
        [sys.executable, "-B", str(SCRIPT), "report", "--state", str(state)],
        capture_output=True,
        text=True,
        timeout=5,
        env=env,
        cwd=tmp_path,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["episodes"][0]["closed_reason"] == "RECORDER_STOPPED"
    assert report["episodes"][0]["sample_count"] == 1
    assert (state / "status.json").read_bytes() == before


def test_reader_separates_missing_invalid_and_atomic_replacement(tmp_path):
    p = tmp_path / "source"
    assert m.read_source(p) == (None, "READ_MISSING")
    p.write_text("not-json")
    assert m.read_source(p) == (None, "READ_JSON_INVALID")
    p.write_text(json.dumps(sample(0)))
    assert m.read_source(p)[0]["tcp_metrics"]["bytes_acked"] == 0
    replacement = tmp_path / "replacement"
    replacement.write_text(json.dumps(sample(1)))
    replacement.replace(p)
    assert m.read_source(p)[0]["tcp_metrics"]["bytes_acked"] == 500000


def test_unit_candidate_has_no_control_or_network_access():
    unit = (ROOT / "ops/systemd/stream-v3-transport-recorder.service").read_text()
    assert "SystemCallFilter=~@network-io" in unit
    assert "Restart=no" in unit
    assert "ReadOnlyPaths=-/run/stream-v3-control" in unit
    assert "InaccessiblePaths=-/run/stream-v3-control/effect.sock" in unit
    assert "runtime-recovery.sock" in unit
    assert "LimitFSIZE=80M" in unit
    assert "ExecStartPre=" not in unit and "ExecStartPost=" not in unit


def test_clock_measurements_are_also_preserved_in_normal_minutes(recorder):
    for t in range(61):
        c = {
            "schema": "stream_v3.clock_observation.v1",
            "observed_at": sample(t)["observed_at"],
            "synchronized": True,
            "offset_seconds": 0.02,
            "uncertainty_seconds": 0.1,
        }
        step(recorder, t, clock=c)
    summary = json.loads(
        recorder.store.db.execute("SELECT payload FROM minutes").fetchone()[0]
    )
    assert summary["counts"]["clock_VALID"] == 60
    assert summary["metrics"]["clock_offset_seconds"]["max"] == 0.02


def test_non_regular_and_deep_json_inputs_are_bounded_failures(tmp_path, recorder):
    p = tmp_path / "fifo"
    os.mkfifo(p)
    assert m.read_source(p) == (None, "INPUT_NOT_REGULAR")
    p = tmp_path / "nested"
    p.write_text("[" * 2000 + "]" * 2000)
    value, error = m.read_source(p)
    frame, _ = step(recorder, 0, value, read_error=error)
    assert frame["quality"] == "UNAVAILABLE"
    assert frame["reason"] in {"READ_JSON_INVALID", "SOURCE_SCHEMA_INVALID"}


def test_collect_cli_rejects_symlink_state_without_creating_database(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            str(SCRIPT),
            "collect",
            "--source",
            str(tmp_path / "missing"),
            "--state",
            str(alias),
            "--steps",
            "1",
        ],
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert result.returncode == 2
    assert not list(target.iterdir())
