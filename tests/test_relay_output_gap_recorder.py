from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("relay_output_gap_recorder", ROOT / "ops/scripts/relay_output_gap_recorder.py")
recorder = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(recorder)

T0 = datetime(2026, 10, 22, 21, 31, 7, tzinfo=timezone.utc)


CYCLE = iter(range(10_000))


def line(captured: datetime, *, ok: bool = True, ocr_only: bool = False, shown_lag: float = 4.0, reason: str = "verified_clock_advancing") -> str:
    target = {
        "captured_at_utc": captured.isoformat().replace("+00:00", "Z"),
        "capture_ok": ok or ocr_only,
        "clock_reading_valid": ok,
        "display_clock_at_utc": (captured - timedelta(seconds=shown_lag)).isoformat().replace("+00:00", "Z") if ok else None,
        "display_age_seconds": shown_lag if ok else None,
        "classification_reason": reason if ok else ("clock_pair_unreliable" if ocr_only else "capture_stream_ended"),
    }
    return json.dumps({"state": {"source_identity": "probe", "cycle": next(CYCLE), "targets": {"oci_relay": target}}})


def write_history(directory: Path, previous: list[str], current: list[str]) -> None:
    (directory / "viewer_clock_history.previous.jsonl").write_text("\n".join(previous) + "\n")
    (directory / "viewer_clock_history.jsonl").write_text("\n".join(current) + "\n")


def morning(directory: Path) -> None:
    # The 2026-10-23 06:31 JST shape: valid every 10 s, three failed captures that repeat the last
    # captured_at, then a 41 s jump.
    before = [line(T0 + timedelta(seconds=10 * i)) for i in range(5)]
    failed = [line(T0 + timedelta(seconds=40), ok=False) for _ in range(3)]
    after = [line(T0 + timedelta(seconds=81 + 10 * i)) for i in range(3)]
    write_history(directory, before + failed, after)


def test_records_the_gap_once_across_rotation_overlap(tmp_path: Path) -> None:
    morning(tmp_path)
    out = tmp_path / "gaps.jsonl"
    assert recorder.record([tmp_path / "viewer_clock_history.previous.jsonl", tmp_path / "viewer_clock_history.jsonl"], out, "oci_relay", 60) == 1
    assert recorder.record([tmp_path / "viewer_clock_history.previous.jsonl", tmp_path / "viewer_clock_history.jsonl"], out, "oci_relay", 60) == 0
    gap = json.loads(out.read_text())
    assert gap["kind"] == "relay_output_interrupted"
    assert gap["unobserved_sec"] == 41.0 and gap["shown_clock_advance_sec"] == 41.0
    assert gap["failed_samples_between"] == {"capture_stream_ended": 3}
    assert gap["last_valid_captured_jst"].startswith("2026-10-23T06:31:47")


def test_steady_ten_second_samples_are_not_gaps(tmp_path: Path) -> None:
    write_history(tmp_path, [line(T0 + timedelta(seconds=10 * i)) for i in range(30)], [])
    out = tmp_path / "gaps.jsonl"
    assert recorder.record([tmp_path / "viewer_clock_history.previous.jsonl"], out, "oci_relay", 60) == 0


def test_single_ocr_misread_is_not_a_gap(tmp_path: Path) -> None:
    rows = [line(T0 + timedelta(seconds=10 * i)) for i in range(3)]
    rows += [line(T0 + timedelta(seconds=30), ok=False, ocr_only=True)]
    rows += [line(T0 + timedelta(seconds=40 + 10 * i)) for i in range(3)]
    write_history(tmp_path, rows, [])
    assert recorder.record([tmp_path / "viewer_clock_history.previous.jsonl"], tmp_path / "gaps.jsonl", "oci_relay", 60) == 0


def test_summary_keeps_the_largest_gap_per_day(tmp_path: Path) -> None:
    morning(tmp_path)
    out = tmp_path / "gaps.jsonl"
    recorder.record([tmp_path / "viewer_clock_history.previous.jsonl", tmp_path / "viewer_clock_history.jsonl"], out, "oci_relay", 60)
    assert [g["shown_clock_advance_sec"] for g in recorder.summary(out)] == [41.0]
