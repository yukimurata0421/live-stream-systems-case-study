from __future__ import annotations

from pathlib import Path
from typing import Any

from .constants import GENRE_TO_SLOT, TIME_SLOTS
from .duration import read_audio
from .text import artist_tokens, genre_from_filename, genre_from_title_tag, normalize_genre


def classify_genre(genre: str) -> str:
    normalized = normalize_genre(genre)
    if not normalized:
        return "others"
    return GENRE_TO_SLOT.get(normalized, "others")


def classify_track(path: Path) -> tuple[str, str, str]:
    filename_genre = genre_from_filename(path)
    slot = classify_genre(filename_genre)
    if slot != "others":
        return slot, filename_genre, "filename"

    title_genre = genre_from_title_tag(read_audio(path))
    slot = classify_genre(title_genre)
    if slot != "others":
        return slot, title_genre, "title"

    return "others", filename_genre or title_genre, "none"


def source_priority(source_folder: str) -> int:
    return 2 if source_folder == "major" else 1


def classification_priority(source: str) -> int:
    if source == "filename":
        return 3
    if source == "title":
        return 2
    if source == "artist":
        return 1
    return 0


def infer_slot_from_artists(path: Path, artist_slot_counts: dict[str, dict[str, int]]) -> str:
    totals = {slot: 0 for slot in TIME_SLOTS if slot != "others"}
    for artist in artist_tokens(path):
        for slot, count in artist_slot_counts.get(artist, {}).items():
            totals[slot] += count
    best_slot, best_count = max(totals.items(), key=lambda item: item[1])
    return best_slot if best_count > 0 else "others"


def build_artist_slot_counts(candidates: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for item in candidates:
        if item["slot"] == "others":
            continue
        for artist in artist_tokens(item["path"]):
            slot_counts = counts.setdefault(artist, {})
            slot_counts[item["slot"]] = slot_counts.get(item["slot"], 0) + 1
    return counts


def build_others_artist_counts(candidates: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in candidates:
        if item["slot"] != "others":
            continue
        for artist in artist_tokens(item["path"]):
            counts[artist] = counts.get(artist, 0) + 1
    return counts
