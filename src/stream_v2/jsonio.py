from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable, Optional


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


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict):
                    yield value
    except FileNotFoundError:
        return


def latest_jsonl(path: Path) -> Optional[dict[str, Any]]:
    last: Optional[dict[str, Any]] = None
    for value in iter_jsonl(path):
        last = value
    return last
