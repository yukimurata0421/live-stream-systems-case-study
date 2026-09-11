from __future__ import annotations

import re
import unicodedata

NOW_PLAYING_PREFIX = "▶ Now Playing: "

_MOJIBAKE_MARKERS = ("\uFFFD", "Ã", "Â", "â", "ð", "ï")


def _mojibake_score(value: str) -> int:
    return sum(value.count(marker) for marker in _MOJIBAKE_MARKERS)


def _repair_mojibake_run(value: str) -> str:
    repaired = value
    for _ in range(2):
        current_score = _mojibake_score(repaired)
        if current_score == 0:
            break
        candidates: list[str] = []
        for encoding in ("cp1252", "latin-1"):
            try:
                candidates.append(repaired.encode(encoding).decode("utf-8"))
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
        improved = [candidate for candidate in candidates if _mojibake_score(candidate) < current_score]
        if not improved:
            break
        repaired = min(improved, key=_mojibake_score)
    return repaired


def repair_mojibake(value: str) -> str:
    """Repair damaged Windows-1252 runs while preserving valid Unicode runs."""
    parts: list[str] = []
    run: list[str] = []

    def flush() -> None:
        if run:
            parts.append(_repair_mojibake_run("".join(run)))
            run.clear()

    for character in value:
        try:
            character.encode("cp1252")
        except UnicodeEncodeError:
            flush()
            parts.append(character)
        else:
            run.append(character)
    flush()
    return "".join(parts)


def beautify_title(filename: str) -> str:
    title = re.sub(r"^(major_|minor_)", "", filename, flags=re.IGNORECASE)
    title = re.sub(r"(?:\.mp3)+$", "", title, flags=re.IGNORECASE)
    title = title.replace("_", " ").strip()
    title = unicodedata.normalize("NFKC", title)
    title = repair_mojibake(title)
    title = (
        title.replace("｜", " | ")
        .replace("¦", " | ")
        .replace("‖", " | ")
        .replace("／", "/")
    )
    cleaned: list[str] = []
    for ch in title:
        cat0 = unicodedata.category(ch)[0]
        if cat0 == "C":
            continue
        if ch == "\uFFFD":
            continue
        cleaned.append(ch)
    title = "".join(cleaned)
    return re.sub(r"\s+", " ", title).strip()
