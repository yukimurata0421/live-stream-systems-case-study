from __future__ import annotations

import re
import unicodedata

NOW_PLAYING_PREFIX = "▶ Now Playing: "


def beautify_title(filename: str) -> str:
    title = re.sub(r"^(major_|minor_)", "", filename, flags=re.IGNORECASE)
    title = re.sub(r"\.mp3$", "", title, flags=re.IGNORECASE)
    title = title.replace("_", " ").strip()
    title = unicodedata.normalize("NFKC", title)
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
    title = title.encode("utf-8", errors="replace").decode("utf-8", errors="replace")
    return re.sub(r"\s+", " ", title).strip()
