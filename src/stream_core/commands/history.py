from __future__ import annotations

from pathlib import Path
from typing import Iterable


def first_heading(path: Path) -> str:
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if text.startswith("# "):
                return text[2:].strip()
    except OSError:
        return ""
    return ""


def history_entries(history_dirs: Iterable[Path], *, day: str = "", grep_text: str = "") -> list[Path]:
    entries = sorted(
        (path for history_dir in history_dirs for path in history_dir.glob("*.md")),
        key=lambda p: (p.name, str(p)),
        reverse=True,
    )
    if day:
        entries = [p for p in entries if p.name.startswith(day)]
    needle = grep_text.strip().lower()
    if not needle:
        return entries
    matched: list[Path] = []
    for path in entries:
        if needle in path.name.lower():
            matched.append(path)
            continue
        try:
            text = path.read_text(encoding="utf-8").lower()
        except OSError:
            continue
        if needle in text:
            matched.append(path)
    return matched


def ops_history_entries(history_dir: Path, *, day: str = "", grep_text: str = "") -> list[Path]:
    return history_entries((history_dir,), day=day, grep_text=grep_text)


def history(
    base_dir: Path,
    ops_log_dir: Path,
    limit: int,
    *,
    day: str = "",
    grep_text: str = "",
    paths_only: bool = False,
    routine_check_dir: Path | None = None,
    extra_history_dirs: Iterable[Path] = (),
) -> int:
    history_dirs = [ops_log_dir, *extra_history_dirs]
    if routine_check_dir is not None:
        history_dirs.append(routine_check_dir)
    entries = history_entries(history_dirs, day=day, grep_text=grep_text)
    limit = max(1, int(limit))
    shown = entries[:limit]
    if not shown:
        print("[info] no matching history docs")
        return 0
    for path in shown:
        if paths_only:
            print(path)
            continue
        rel = path.relative_to(base_dir).as_posix()
        heading = first_heading(path)
        suffix = f" - {heading}" if heading else ""
        print(f"{rel}{suffix}")
    if len(entries) > len(shown):
        print(f"[info] {len(entries) - len(shown)} more entries hidden; raise --limit to show more")
    return 0
