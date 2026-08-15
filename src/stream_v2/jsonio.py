from __future__ import annotations

import gzip
import json
import os
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional


def read_json(path: Path) -> Optional[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            value = json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return None
    if isinstance(value, dict):
        return value
    return None


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, sort_keys=False, separators=(",", ":"))
        f.write("\n")
    os.replace(tmp, path)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, sort_keys=False, separators=(",", ":"))
        f.write("\n")


def rotated_jsonl_paths(path: Path) -> list[Path]:
    rotations: list[tuple[int, Path]] = []
    prefix = path.name + "."
    for candidate in path.parent.glob(prefix + "*"):
        suffix = candidate.name[len(prefix) :]
        if suffix.endswith(".gz"):
            suffix = suffix[:-3]
        if suffix.isdigit() and candidate.is_file():
            rotations.append((int(suffix), candidate))

    # Numeric logrotate generations are newest at .1 and oldest at the
    # highest number. Readers consume history chronologically, then current.
    ordered = [candidate for _generation, candidate in sorted(rotations, reverse=True)]
    if path.is_file():
        ordered.append(path)
    return ordered


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    for candidate in rotated_jsonl_paths(path):
        opener = gzip.open if candidate.suffix == ".gz" else open
        try:
            with opener(candidate, "rt", encoding="utf-8") as f:
                for line in f:
                    value = _json_object(line)
                    if value is not None:
                        yield value
        except OSError:
            continue


def _json_object(line: str | bytes) -> Optional[dict[str, Any]]:
    if isinstance(line, bytes):
        try:
            line = line.decode("utf-8")
        except UnicodeDecodeError:
            return None
    line = line.strip()
    if not line:
        return None
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _reverse_lines(path: Path, *, chunk_size: int = 1024 * 1024) -> Iterator[bytes]:
    with path.open("rb") as f:
        f.seek(0, os.SEEK_END)
        position = f.tell()
        remainder = b""
        while position > 0:
            read_size = min(chunk_size, position)
            position -= read_size
            f.seek(position)
            block = f.read(read_size) + remainder
            lines = block.split(b"\n")
            remainder = lines[0]
            for line in reversed(lines[1:]):
                if line:
                    yield line
        if remainder:
            yield remainder


def _latest_in_candidate(
    path: Path,
    predicate: Callable[[dict[str, Any]], bool],
) -> Optional[dict[str, Any]]:
    if path.suffix != ".gz":
        try:
            for line in _reverse_lines(path):
                value = _json_object(line)
                if value is not None and predicate(value):
                    return value
        except OSError:
            return None
        return None

    latest: Optional[dict[str, Any]] = None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                value = _json_object(line)
                if value is not None and predicate(value):
                    latest = value
    except OSError:
        return None
    return latest


def latest_jsonl_where(
    path: Path,
    predicate: Callable[[dict[str, Any]], bool],
) -> Optional[dict[str, Any]]:
    for candidate in reversed(rotated_jsonl_paths(path)):
        value = _latest_in_candidate(candidate, predicate)
        if value is not None:
            return value
    return None


def latest_jsonl(path: Path) -> Optional[dict[str, Any]]:
    return latest_jsonl_where(path, lambda _value: True)
