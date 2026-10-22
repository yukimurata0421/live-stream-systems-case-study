#!/usr/bin/env python3
"""Keep a durable record of gaps in the OCI relay output's clock.

The relay clock monitor keeps about an hour of ten-second samples in
`viewer_clock_history{.previous,}.jsonl` and then rotates them away. This reads both files and
appends one record per gap to an append-only JSONL file, so daily events (such as the WAN
renumbering around 06:30 JST) stay measurable after rotation.

A gap is the time between two consecutive verified clock readings when frame capture itself
failed in between (`relay_output_interrupted`), or when nothing at all was sampled for longer
than --missing-sec (`observation_missing`). A clock jump alone is not used: after an
interruption the live clock simply shows the current time again, and a single OCR misread
leaves the same 20 s spacing as a real outage. The record estimates how much relay output was
not seen, not viewer playback: the reader shares the delivery host's WAN, and provider
buffering is not included. Read-only towards the monitor; it writes only its own file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

JST = timezone(timedelta(hours=9))
SCHEMA = "stream_v3.relay_output_gap.v1"


def _time(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")) if value else None
    except ValueError:
        return None


def samples(paths: list[Path], target: str) -> Iterator[dict[str, Any]]:
    """Relay-target samples from the history files, oldest first, without duplicates."""
    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    for path in paths:
        if not path.is_file():
            continue
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                    sample = record["state"]["targets"][target]
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue
                captured = _time(sample.get("captured_at_utc") or sample.get("checked_at_utc"))
                if captured is None:
                    continue
                state = record.get("state") or {}
                # Failed captures repeat the last captured_at; the probe cycle tells them apart.
                key = f"{state.get('source_identity')}:{state.get('cycle')}" if state.get("cycle") is not None else captured.isoformat()
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "captured": captured,
                        "valid": sample.get("capture_ok") is True and sample.get("clock_reading_valid") is True,
                        "capture_ok": sample.get("capture_ok") is True,
                        "checked": _time(sample.get("checked_at_utc")) or captured,
                        "shown": _time(sample.get("display_clock_at_utc")),
                        "age": sample.get("display_age_seconds"),
                        "reason": sample.get("classification_reason"),
                    }
                )
    # A failed capture repeats the previous captured_at, so order by when each sample was checked.
    yield from sorted(rows, key=lambda row: (row["checked"], not row["valid"]))


def gaps(rows: Iterator[dict[str, Any]], missing_sec: float) -> Iterator[dict[str, Any]]:
    last: dict[str, Any] | None = None
    between: Counter[str] = Counter()
    capture_failed = False
    for row in rows:
        if not (row["valid"] and row["shown"] is not None):
            between[str(row["reason"])] += 1
            capture_failed = capture_failed or not row["capture_ok"]
            continue
        if last is not None:
            unobserved = (row["captured"] - last["captured"]).total_seconds()
            advanced = (row["shown"] - last["shown"]).total_seconds()
            kind = "relay_output_interrupted" if capture_failed else ("observation_missing" if unobserved > missing_sec and not between else "")
            if kind:
                ident = hashlib.sha256(f"{last['captured'].isoformat()}|{row['captured'].isoformat()}".encode()).hexdigest()[:16]
                yield {
                    "schema": SCHEMA,
                    "kind": kind,
                    "gap_id": ident,
                    "last_valid_captured_jst": last["captured"].astimezone(JST).isoformat(),
                    "next_valid_captured_jst": row["captured"].astimezone(JST).isoformat(),
                    "unobserved_sec": round(unobserved, 1),
                    "last_shown_clock_jst": last["shown"].astimezone(JST).isoformat(),
                    "next_shown_clock_jst": row["shown"].astimezone(JST).isoformat(),
                    "shown_clock_advance_sec": round(advanced, 1),
                    "display_age_before_sec": last["age"],
                    "display_age_after_sec": row["age"],
                    "failed_samples_between": dict(between),
                }
        last = row
        between = Counter()
        capture_failed = False


def record(paths: list[Path], output: Path, target: str, missing_sec: float) -> int:
    known: set[str] = set()
    if output.is_file():
        with output.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    known.add(json.loads(line)["gap_id"])
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue
    added = 0
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("a", encoding="utf-8") as handle:
        for gap in gaps(samples(paths, target), missing_sec):
            if gap["gap_id"] in known:
                continue
            gap["recorded_at_jst"] = datetime.now(JST).isoformat(timespec="seconds")
            handle.write(json.dumps(gap, sort_keys=True) + "\n")
            known.add(gap["gap_id"])
            added += 1
        handle.flush()
        os.fsync(handle.fileno())
    return added


def summary(output: Path) -> list[dict[str, Any]]:
    """Largest relay output interruption per JST day."""
    best: dict[str, dict[str, Any]] = {}
    if output.is_file():
        with output.open(encoding="utf-8") as handle:
            for line in handle:
                gap = json.loads(line)
                if gap.get("kind") != "relay_output_interrupted":
                    continue
                day = gap["last_valid_captured_jst"][:10]
                if day not in best or gap["shown_clock_advance_sec"] > best[day]["shown_clock_advance_sec"]:
                    best[day] = gap
    return [best[day] for day in sorted(best)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="relay_output_gap_recorder")
    parser.add_argument("--history-dir", type=Path, required=True, help="directory holding viewer_clock_history*.jsonl")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target", default="oci_relay")
    parser.add_argument("--missing-sec", type=float, default=60.0, help="no samples at all for longer than this is observation_missing")
    parser.add_argument("--summary", action="store_true", help="print the largest gap per day instead of recording")
    args = parser.parse_args(argv)
    if args.summary:
        for gap in summary(args.output):
            print(json.dumps(gap, sort_keys=True))
        return 0
    paths = [args.history_dir / "viewer_clock_history.previous.jsonl", args.history_dir / "viewer_clock_history.jsonl"]
    added = record(paths, args.output, args.target, args.missing_sec)
    print(json.dumps({"schema": "stream_v3.relay_output_gap_run.v1", "added": added}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
