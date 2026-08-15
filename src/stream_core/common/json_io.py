from __future__ import annotations

import gzip
import json
import os
from pathlib import Path
from typing import Callable, Iterator


def read_json_file(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def rotated_jsonl_paths(path: Path) -> list[Path]:
    rotations: list[tuple[int, Path]] = []
    prefix = path.name + "."
    for candidate in path.parent.glob(prefix + "*"):
        suffix = candidate.name[len(prefix) :]
        if suffix.endswith(".gz"):
            suffix = suffix[:-3]
        if suffix.isdigit() and candidate.is_file():
            rotations.append((int(suffix), candidate))

    ordered = [candidate for _generation, candidate in sorted(rotations, reverse=True)]
    if path.is_file():
        ordered.append(path)
    return ordered


def iter_jsonl(path: Path) -> Iterator[dict]:
    for candidate in rotated_jsonl_paths(path):
        opener = gzip.open if candidate.suffix == ".gz" else open
        try:
            with opener(candidate, "rt", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(payload, dict):
                        yield payload
        except OSError:
            continue


def _json_object(line: str | bytes) -> dict | None:
    if isinstance(line, bytes):
        try:
            line = line.decode("utf-8")
        except UnicodeDecodeError:
            return None
    line = line.strip()
    if not line:
        return None
    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _reverse_lines(path: Path, *, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
    with path.open("rb") as fh:
        fh.seek(0, os.SEEK_END)
        position = fh.tell()
        remainder = b""
        while position > 0:
            read_size = min(chunk_size, position)
            position -= read_size
            fh.seek(position)
            block = fh.read(read_size) + remainder
            lines = block.split(b"\n")
            remainder = lines[0]
            for line in reversed(lines[1:]):
                if line:
                    yield line
        if remainder:
            yield remainder


def _latest_in_candidate(path: Path, predicate: Callable[[dict], bool]) -> dict:
    if path.suffix != ".gz":
        try:
            for line in _reverse_lines(path):
                payload = _json_object(line)
                if payload is not None and predicate(payload):
                    return payload
        except OSError:
            return {}
        return {}

    latest: dict = {}
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                payload = _json_object(line)
                if payload is not None and predicate(payload):
                    latest = payload
    except OSError:
        return {}
    return latest


def latest_jsonl(path: Path, *, predicate: Callable[[dict], bool] | None = None) -> dict:
    match = predicate or (lambda _payload: True)
    for candidate in reversed(rotated_jsonl_paths(path)):
        payload = _latest_in_candidate(candidate, match)
        if payload:
            return payload
    return {}


def iter_jsonl_recent(
    path: Path,
    *,
    cutoff_ts: int,
    timestamp: Callable[[dict], int],
    project: Callable[[dict], dict] | None = None,
) -> Iterator[dict]:
    """Yield timestamped JSONL rows newest-first until ``cutoff_ts``.

    Current and delay-compressed rotated files are read backwards, so routine
    monitoring does not scan multi-gigabyte histories. Gzip files are only
    reached when the requested time window crosses older rotations.
    """

    cutoff = int(cutoff_ts)
    for candidate in reversed(rotated_jsonl_paths(path)):
        if candidate.suffix != ".gz":
            saw_timestamp = False
            try:
                for line in _reverse_lines(candidate):
                    payload = _json_object(line)
                    if payload is None:
                        continue
                    event_ts = int(timestamp(payload) or 0)
                    if event_ts <= 0:
                        continue
                    saw_timestamp = True
                    if event_ts < cutoff:
                        return
                    yield project(payload) if project is not None else payload
            except OSError:
                continue
            if saw_timestamp:
                continue
            continue

        recent: list[tuple[int, dict]] = []
        oldest_ts: int | None = None
        try:
            with gzip.open(candidate, "rt", encoding="utf-8") as fh:
                for line in fh:
                    payload = _json_object(line)
                    if payload is None:
                        continue
                    event_ts = int(timestamp(payload) or 0)
                    if event_ts <= 0:
                        continue
                    oldest_ts = event_ts if oldest_ts is None else min(oldest_ts, event_ts)
                    if event_ts >= cutoff:
                        retained = project(payload) if project is not None else payload
                        recent.append((event_ts, retained))
        except OSError:
            continue
        for _event_ts, payload in sorted(recent, key=lambda item: item[0], reverse=True):
            yield payload
        if oldest_ts is not None and oldest_ts < cutoff:
            return


def append_jsonl(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def write_jsonl_atomic(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    tmp.replace(path)


def atomic_write_json_file(path: Path, payload: dict, *, indent: int | None = None, sort_keys: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if indent is None:
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=sort_keys)
    else:
        text = json.dumps(payload, ensure_ascii=False, indent=indent, sort_keys=sort_keys)
    tmp.write_text(text + "\n", encoding="utf-8")
    tmp.replace(path)
