from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Iterator


def read_json_file(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def rotated_jsonl_paths(path: Path) -> list[Path]:
    candidates = [path]
    candidates.extend(path.parent.glob(path.name + ".*"))

    def sort_key(candidate: Path) -> tuple[int, float, str]:
        if candidate == path:
            return (1, 0.0, candidate.name)
        try:
            return (0, candidate.stat().st_mtime, candidate.name)
        except OSError:
            return (0, 0.0, candidate.name)

    return sorted((p for p in candidates if p.exists() and p.is_file()), key=sort_key)


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
