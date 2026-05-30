from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .classification import (
    build_artist_slot_counts,
    classification_priority,
    classify_track,
    infer_slot_from_artists,
    source_priority,
)
from .constants import SKIP_REASONS, SOURCE_DIRS, TIME_SLOTS
from .duration import is_long_track
from .overrides import apply_confirmed_manual_override
from .text import normalize_track_title


def reset_target_dirs(target_base: Path) -> None:
    target_base.mkdir(parents=True, exist_ok=True)
    for slot in TIME_SLOTS:
        folder = target_base / slot
        folder.mkdir(parents=True, exist_ok=True)
        for path in folder.iterdir():
            if path.is_symlink():
                path.unlink()


def link_track(target_base: Path, source_folder: str, mp3_file: Path, slot: str) -> None:
    target_link = target_base / slot / f"{source_folder}_{mp3_file.name}"
    if target_link.exists() or target_link.is_symlink():
        target_link.unlink()
    os.symlink(os.path.relpath(mp3_file, target_link.parent), target_link)


def initial_source_counts() -> dict[str, dict[str, int]]:
    return {
        source: {slot: 0 for slot in (*TIME_SLOTS, *SKIP_REASONS)}
        for source in SOURCE_DIRS
    }


def build_library_candidates(
    base_dir: Path,
    overrides: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]]]:
    overrides = overrides or {}
    source_counts = initial_source_counts()
    candidates: list[dict[str, Any]] = []

    for source_folder in SOURCE_DIRS:
        source_path = base_dir / source_folder
        if not source_path.exists():
            continue

        for mp3_file in sorted(source_path.glob("*.mp3")):
            if is_long_track(mp3_file):
                source_counts[source_folder]["skipped_long"] += 1
                continue
            slot, genre, source = classify_track(mp3_file)
            slot, source, override_action, override_reason = apply_confirmed_manual_override(
                mp3_file,
                slot,
                source,
                overrides,
            )
            if slot == "exclude":
                source_counts[source_folder]["skipped_excluded"] += 1
                continue
            candidates.append(
                {
                    "source_folder": source_folder,
                    "path": mp3_file,
                    "slot": slot,
                    "genre": genre,
                    "source": source,
                    "title_key": normalize_track_title(mp3_file.name),
                    "override_action": override_action,
                    "override_reason": override_reason,
                }
            )

    artist_slot_counts = build_artist_slot_counts(candidates)
    for item in candidates:
        if item["slot"] != "others":
            continue
        inferred_slot = infer_slot_from_artists(item["path"], artist_slot_counts)
        if inferred_slot != "others":
            item["slot"] = inferred_slot
            item["source"] = "artist"

    return candidates, source_counts


def selected_library_items(candidates: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], set[int]]:
    selected_by_title: dict[str, dict[str, Any]] = {}
    for item in candidates:
        title_key = item["title_key"]
        current = selected_by_title.get(title_key)
        if current is None:
            selected_by_title[title_key] = item
            continue
        item_score = (
            item["slot"] != "others",
            classification_priority(item["source"]),
            source_priority(item["source_folder"]),
            str(item["path"].name).lower(),
        )
        current_score = (
            current["slot"] != "others",
            classification_priority(current["source"]),
            source_priority(current["source_folder"]),
            str(current["path"].name).lower(),
        )
        if item_score > current_score:
            selected_by_title[title_key] = item

    selected = list(selected_by_title.values())
    return selected, {id(item) for item in selected}


def organize_library(
    base_dir: Path,
    target_base: Path,
    overrides: dict[str, Any] | None = None,
) -> dict[str, dict[str, int]]:
    reset_target_dirs(target_base)
    candidates, source_counts = build_library_candidates(base_dir, overrides=overrides)
    _, selected_ids = selected_library_items(candidates)
    for item in candidates:
        source_folder = item["source_folder"]
        if id(item) not in selected_ids:
            source_counts[source_folder]["skipped_duplicate"] += 1
            continue
        link_track(target_base, source_folder, item["path"], item["slot"])
        source_counts[source_folder][item["slot"]] += 1

    return source_counts
