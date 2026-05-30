from __future__ import annotations

from pathlib import Path
from typing import Any

from .classification import build_others_artist_counts
from .constants import NON_ROTATION_PATTERNS
from .library import build_library_candidates, selected_library_items
from .overrides import _valid_slot, manual_override_for_track
from .text import artist_tokens, display_track_title, normalize_genre, normalized_phrase_in_text


def default_others_suggestion(path: Path, artist_count: int) -> dict[str, str]:
    normalized_name = normalize_genre(path.stem)
    for pattern, reason in NON_ROTATION_PATTERNS:
        if normalized_phrase_in_text(pattern, normalized_name):
            return {
                "action": "exclude_candidate",
                "candidate_slot": "",
                "confidence": "candidate",
                "reason": reason,
            }
    if artist_count >= 2:
        return {
            "action": "review_artist_cluster",
            "candidate_slot": "",
            "confidence": "candidate",
            "reason": f"same artist has {artist_count} unclassified tracks; classify as a group after listening",
        }
    return {
        "action": "review_track",
        "candidate_slot": "",
        "confidence": "candidate",
        "reason": "no genre, no confirmed override, and no classified artist evidence",
    }


def build_others_report(
    base_dir: Path,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    overrides = overrides or {}
    candidates, source_counts = build_library_candidates(base_dir, overrides=overrides)
    selected, selected_ids = selected_library_items(candidates)
    others_artist_counts = build_others_artist_counts(selected)

    duplicate_count = 0
    for item in candidates:
        if id(item) not in selected_ids:
            duplicate_count += 1

    items: list[dict[str, Any]] = []
    for item in selected:
        if item["slot"] != "others":
            continue
        path = item["path"]
        manual = manual_override_for_track(path, overrides)
        artists = artist_tokens(path)
        artist_count = max([others_artist_counts.get(artist, 0) for artist in artists] or [0])
        suggestion = default_others_suggestion(path, artist_count)
        if manual:
            confidence = manual.get("confidence", "")
            if manual.get("action") == "exclude":
                suggestion = {
                    "action": "exclude_pending_confirmation",
                    "candidate_slot": "",
                    "confidence": confidence or "candidate",
                    "reason": manual.get("reason", ""),
                }
            elif _valid_slot(manual.get("slot", "")):
                suggestion = {
                    "action": "classify_pending_confirmation",
                    "candidate_slot": manual["slot"],
                    "confidence": confidence or "candidate",
                    "reason": manual.get("reason", ""),
                }

        items.append(
            {
                "source_folder": item["source_folder"],
                "filename": path.name,
                "title": display_track_title(path),
                "artists": artists,
                "current_slot": "others",
                "classification_source": item["source"],
                "candidate_action": suggestion["action"],
                "candidate_slot": suggestion["candidate_slot"],
                "confidence": suggestion["confidence"],
                "reason": suggestion["reason"],
            }
        )

    by_action: dict[str, int] = {}
    for item in items:
        action = item["candidate_action"]
        by_action[action] = by_action.get(action, 0) + 1

    return {
        "base_dir": str(base_dir),
        "others_count": len(items),
        "skipped_long": sum(counts["skipped_long"] for counts in source_counts.values()),
        "skipped_excluded": sum(counts["skipped_excluded"] for counts in source_counts.values()),
        "duplicate_count": duplicate_count,
        "by_action": by_action,
        "items": items,
    }


def format_others_report(report: dict[str, Any]) -> str:
    lines = [
        f"others_count={report['others_count']}",
        f"skipped_long={report['skipped_long']} skipped_excluded={report['skipped_excluded']} duplicate_count={report['duplicate_count']}",
    ]
    for action, count in sorted(report["by_action"].items()):
        lines.append(f"{action}: {count}")
    lines.append("")
    for item in report["items"]:
        slot = item["candidate_slot"] or "-"
        lines.append(
            f"{item['candidate_action']} slot={slot} confidence={item['confidence']} "
            f"title=\"{item['title']}\" reason=\"{item['reason']}\""
        )
    return "\n".join(lines)
