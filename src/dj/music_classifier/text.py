from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def normalize_genre(text: str) -> str:
    normalized = text.lower()
    normalized = normalized.replace("｜", "|")
    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def normalize_track_title(text: str) -> str:
    stem = str(text).replace("｜", "|")
    if stem.lower().endswith(".mp3"):
        stem = stem[:-4]
    title = stem.split("|", 1)[0]
    title = re.sub(r"\[(?:ncs|arcade|official|copyright|lyric|music).+?\]", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s+", " ", title).strip()
    return normalize_genre(title)


def normalized_phrase_in_text(phrase: str, text: str) -> bool:
    normalized_phrase = normalize_genre(phrase)
    normalized_text = normalize_genre(text)
    if not normalized_phrase:
        return False
    return re.search(rf"(^|\s){re.escape(normalized_phrase)}($|\s)", normalized_text) is not None


def artist_tokens(path: Path) -> list[str]:
    title = Path(path.stem.replace("｜", "|").split("|", 1)[0]).name
    if " - " not in title:
        return []
    artists = title.split(" - ", 1)[0]
    tokens = re.split(r"\s+(?:x|feat\.?|ft\.?|and)\s+|[,&]", artists, flags=re.IGNORECASE)
    return [normalize_genre(token) for token in tokens if normalize_genre(token)]


def display_track_title(path: Path) -> str:
    return Path(path.stem.replace("｜", "|").split("|", 1)[0]).name.strip()


def genre_from_filename(path: Path) -> str:
    stem = path.stem.replace("｜", "|")
    if "|" not in stem:
        return ""
    parts = [part.strip() for part in stem.split("|")]
    for part in parts[1:]:
        lowered = part.lower()
        if "ncs" in lowered or "copyright" in lowered:
            continue
        if part:
            return part
    return ""


def genre_from_title_tag(audio: Any) -> str:
    if audio is None:
        return ""
    for key in ("TIT2", "title"):
        value = audio.get(key)
        if value is None:
            continue
        text = str(value).replace("｜", "|")
        parts = [p.strip() for p in text.split("|")]
        for part in parts[1:]:
            lowered = part.lower()
            if "ncs" in lowered or "copyright" in lowered:
                continue
            if part:
                return part
    return ""
