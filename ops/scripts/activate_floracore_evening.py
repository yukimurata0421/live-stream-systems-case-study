#!/usr/bin/env python3
"""Make the evening Auto DJ bucket exclusive to the reviewed Floracore collection."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MUSIC_ROOT = PROJECT_ROOT / "ncs_music"
EXPECTED_TRACKS = 60
ROTATION_PREFIX = "minor"
NCS_ARCHIVE_RELATIVE = Path("time_tags_ncs_archive") / "evening"


def activation_plan(music_root: Path) -> list[tuple[Path, Path, str]]:
    source_root = music_root / "floracore_evening" / "tracks"
    target_root = music_root / "time_tags" / "evening"
    tracks = sorted(source_root.glob("Floracore - *.mp3"), key=lambda path: path.name.casefold())
    if len(tracks) != EXPECTED_TRACKS:
        raise RuntimeError(f"expected {EXPECTED_TRACKS} Floracore tracks, found {len(tracks)}")

    plan: list[tuple[Path, Path, str]] = []
    for source in tracks:
        target = target_root / f"{ROTATION_PREFIX}_{source.name}"
        relative_source = os.path.relpath(source, target.parent)
        plan.append((source, target, relative_source))
    return plan


def ncs_archive_plan(music_root: Path) -> list[tuple[Path, Path]]:
    target_root = music_root / "time_tags" / "evening"
    archive_root = music_root / NCS_ARCHIVE_RELATIVE
    if not target_root.exists():
        return []

    plan: list[tuple[Path, Path]] = []
    for source in sorted(target_root.iterdir(), key=lambda path: path.name.casefold()):
        if "floracore" in source.name.casefold():
            continue
        if not source.is_symlink():
            raise RuntimeError(f"refusing to archive non-symlink evening entry: {source}")
        relative_target = os.readlink(source)
        if not relative_target.startswith(("../../major/", "../../minor/")):
            raise RuntimeError(f"unexpected NCS symlink target: {source} -> {relative_target}")
        archive = archive_root / source.name
        if archive.exists() or archive.is_symlink():
            raise RuntimeError(f"refusing to replace existing archive path: {archive}")
        plan.append((source, archive))
    return plan


def activate(music_root: Path, *, apply: bool, credit_confirmed: bool) -> dict[str, object]:
    if apply and not credit_confirmed:
        raise RuntimeError("--credit-confirmed is required with --apply")

    plan = activation_plan(music_root)
    archive_plan = ncs_archive_plan(music_root)
    created = 0
    existing = 0
    archived = 0
    for source, target, relative_source in plan:
        if target.is_symlink():
            if os.readlink(target) != relative_source:
                raise RuntimeError(f"unexpected symlink target: {target} -> {os.readlink(target)}")
            existing += 1
            continue
        if target.exists():
            raise RuntimeError(f"refusing to replace existing path: {target}")
        if apply:
            target.parent.mkdir(parents=True, exist_ok=True)
            os.symlink(relative_source, target)
            created += 1

    if apply and archive_plan:
        archive_root = music_root / NCS_ARCHIVE_RELATIVE
        archive_root.mkdir(parents=True, exist_ok=True)
        for source, archive in archive_plan:
            source.replace(archive)
            archived += 1

    return {
        "mode": "apply" if apply else "dry-run",
        "bucket": "evening",
        "exclusive_provider": "floracore",
        "rotation_prefix": ROTATION_PREFIX,
        "planned": len(plan),
        "created": created,
        "existing": existing,
        "ncs_to_archive": len(archive_plan),
        "ncs_archived": archived,
        "ncs_archive": str(music_root / NCS_ARCHIVE_RELATIVE),
        "credit_confirmed": credit_confirmed,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Make the evening Auto DJ bucket exclusive to the reviewed Floracore collection."
    )
    parser.add_argument("--music-root", type=Path, default=DEFAULT_MUSIC_ROOT)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--credit-confirmed",
        action="store_true",
        help="Confirm that the live YouTube description and overlay credit are in place.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = activate(
        args.music_root.resolve(),
        apply=args.apply,
        credit_confirmed=args.credit_confirmed,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
