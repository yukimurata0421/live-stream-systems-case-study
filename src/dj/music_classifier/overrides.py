from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .constants import APPLIED_OVERRIDE_CONFIDENCE, CLASSIFIED_TIME_SLOTS
from .text import artist_tokens, normalize_genre, normalize_track_title, normalized_phrase_in_text


def load_classification_overrides(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError(f"classification overrides must be a JSON object: {path}")
    return payload


def _override_entry(value: Any) -> dict[str, str]:
    if isinstance(value, str):
        return {"slot": value, "confidence": APPLIED_OVERRIDE_CONFIDENCE, "reason": ""}
    if not isinstance(value, dict):
        return {}
    return {
        "slot": str(value.get("slot", "") or "").strip(),
        "action": str(value.get("action", "") or "").strip(),
        "confidence": str(value.get("confidence", "") or "").strip().lower(),
        "reason": str(value.get("reason", "") or "").strip(),
    }


def _iter_exclude_patterns(overrides: dict[str, Any]) -> list[dict[str, str]]:
    patterns: list[dict[str, str]] = []
    for raw in overrides.get("exclude_patterns", []) or []:
        if isinstance(raw, str):
            patterns.append(
                {
                    "pattern": raw,
                    "confidence": APPLIED_OVERRIDE_CONFIDENCE,
                    "reason": "manual exclude pattern",
                }
            )
            continue
        if isinstance(raw, dict):
            pattern = str(raw.get("pattern", "") or "").strip()
            if not pattern:
                continue
            patterns.append(
                {
                    "pattern": pattern,
                    "confidence": str(raw.get("confidence", "") or "").strip().lower(),
                    "reason": str(raw.get("reason", "") or "").strip(),
                }
            )
    return patterns


def _confirmed(entry: dict[str, str]) -> bool:
    return entry.get("confidence") == APPLIED_OVERRIDE_CONFIDENCE


def _valid_slot(slot: str) -> bool:
    return slot in CLASSIFIED_TIME_SLOTS


def manual_override_for_track(path: Path, overrides: dict[str, Any]) -> dict[str, str]:
    title_key = normalize_track_title(path.name)
    for raw_key, raw_entry in (overrides.get("track_overrides", {}) or {}).items():
        if normalize_track_title(str(raw_key)) == title_key:
            return _override_entry(raw_entry)

    artist_map = overrides.get("artist_overrides", {}) or {}
    for artist in artist_tokens(path):
        raw_entry = artist_map.get(artist)
        if raw_entry is None:
            for raw_artist, candidate in artist_map.items():
                if normalize_genre(str(raw_artist)) == artist:
                    raw_entry = candidate
                    break
        if raw_entry is not None:
            return _override_entry(raw_entry)

    normalized_name = normalize_genre(path.stem)
    for item in _iter_exclude_patterns(overrides):
        if normalized_phrase_in_text(item["pattern"], normalized_name):
            return {
                "action": "exclude",
                "confidence": item["confidence"],
                "reason": item["reason"] or f"matched exclude pattern: {item['pattern']}",
            }

    return {}


def apply_confirmed_manual_override(
    path: Path,
    slot: str,
    source: str,
    overrides: dict[str, Any],
) -> tuple[str, str, str, str]:
    entry = manual_override_for_track(path, overrides)
    if not entry or not _confirmed(entry):
        return slot, source, "", ""

    if entry.get("action") == "exclude":
        return "exclude", "override_exclude", "exclude", entry.get("reason", "")

    override_slot = entry.get("slot", "")
    if _valid_slot(override_slot):
        return override_slot, "override", "classify", entry.get("reason", "")

    return slot, source, "", ""
