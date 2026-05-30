from __future__ import annotations

import json
import logging
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .io import utc_now_iso
from .time_policy import PATTERN


@dataclass
class FolderState:
    played: set[Path] = field(default_factory=set)
    pattern_index: int = 0


def track_prefix(track: Path | None) -> str:
    if track is None:
        return "unknown"
    lower = track.name.lower()
    if lower.startswith("minor_"):
        return "minor"
    if lower.startswith("major_"):
        return "major"
    return "unknown"


def list_tracks(
    folder: Path,
    *,
    max_track_sec: int,
    duration_lookup: Callable[[Path], float],
) -> list[Path]:
    tracks: list[Path] = []
    for path in folder.iterdir():
        if not path.is_file():
            continue
        lower = path.name.lower()
        if not lower.endswith(".mp3"):
            continue
        if not (lower.startswith("minor_") or lower.startswith("major_")):
            continue
        if max_track_sec > 0 and duration_lookup(path) > float(max_track_sec):
            continue
        tracks.append(path)
    tracks.sort(key=lambda p: p.name.lower())
    return tracks


def load_pattern_state(path: Path, *, library_root: Path) -> dict[str, FolderState]:
    state_by_folder: dict[str, FolderState] = {}
    try:
        if not path.exists():
            return state_by_folder
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return state_by_folder
        by_folder = raw.get("by_folder", {})
        if not isinstance(by_folder, dict):
            return state_by_folder
        for folder_name, payload in by_folder.items():
            if not isinstance(folder_name, str) or not isinstance(payload, dict):
                continue
            idx = payload.get("pattern_index", 0)
            if not isinstance(idx, int):
                continue
            state = state_by_folder.setdefault(folder_name, FolderState())
            state.pattern_index = idx % len(PATTERN)
            played_filenames = payload.get("played_filenames", [])
            if isinstance(played_filenames, list):
                state.played = {
                    library_root / folder_name / name
                    for name in played_filenames
                    if isinstance(name, str) and name
                }
    except Exception:
        logging.exception("Failed to load pattern state: %s", path)
    return state_by_folder


def pattern_state_payload(state_by_folder: dict[str, FolderState]) -> dict:
    return {
        "schema": "pattern_state/v1",
        "updated_at_utc": utc_now_iso(),
        "by_folder": {
            folder: {
                "pattern_index": state.pattern_index,
                "played_filenames": sorted(path.name for path in state.played),
            }
            for folder, state in state_by_folder.items()
        },
    }


def pick_track(
    *,
    library_root: Path,
    folder_name: str,
    state_by_folder: dict[str, FolderState],
    max_track_sec: int,
    duration_lookup: Callable[[Path], float],
) -> Path:
    folder = library_root / folder_name
    if not folder.exists():
        raise FileNotFoundError(f"Time bucket folder does not exist: {folder}")

    tracks = list_tracks(folder, max_track_sec=max_track_sec, duration_lookup=duration_lookup)
    if not tracks:
        raise RuntimeError(f"No playable tracks found in: {folder}")

    state = state_by_folder.setdefault(folder_name, FolderState())
    track_set = set(tracks)
    state.played.intersection_update(track_set)

    if len(state.played) >= len(track_set):
        state.played.clear()

    desired_prefix = PATTERN[state.pattern_index]
    desired_pool = [t for t in tracks if track_prefix(t) == desired_prefix]
    desired_unplayed = [t for t in desired_pool if t not in state.played]
    unplayed_any = [t for t in tracks if t not in state.played]

    if desired_unplayed:
        selected = random.choice(desired_unplayed)
    elif unplayed_any:
        selected = random.choice(unplayed_any)
        logging.info(
            "No unplayed '%s' tracks left in %s. Falling back to any unplayed track.",
            desired_prefix,
            folder,
        )
    elif desired_prefix in {"major", "minor"} and desired_pool:
        state.played = {t for t in state.played if track_prefix(t) != desired_prefix}
        desired_unplayed = [t for t in desired_pool if t not in state.played]
        selected = random.choice(desired_unplayed if desired_unplayed else desired_pool)
        logging.info(
            "No unplayed tracks left in %s. Resetting exhausted '%s' rotation pool.",
            folder,
            desired_prefix,
        )
    else:
        state.played.clear()
        desired_all = [t for t in tracks if track_prefix(t) == desired_prefix]
        selected = random.choice(desired_all if desired_all else tracks)

    state.played.add(selected)
    state.pattern_index = (state.pattern_index + 1) % len(PATTERN)
    return selected
