from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

try:
    from .music_time_classifier import (
        SKIP_REASONS,
        TIME_SLOTS,
        build_others_report,
        format_others_report,
        load_classification_overrides,
        organize_library,
    )
except ImportError:  # pragma: no cover - supports direct script execution.
    from music_time_classifier import (  # type: ignore
        SKIP_REASONS,
        TIME_SLOTS,
        build_others_report,
        format_others_report,
        load_classification_overrides,
        organize_library,
    )

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MUSIC_ROOT = PROJECT_ROOT / "ncs_music"
DEFAULT_OVERRIDES_FILE = PROJECT_ROOT / "configs" / "auto_dj_classification_overrides.json"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Organize NCS tracks into AutoDJ time buckets.")
    parser.add_argument(
        "--music-root",
        type=Path,
        default=DEFAULT_MUSIC_ROOT,
        help="Directory containing major/minor source folders.",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=None,
        help="Output time_tags directory. Defaults to MUSIC_ROOT/time_tags.",
    )
    parser.add_argument(
        "--others-report",
        action="store_true",
        help="Report current others candidates without rewriting time_tags.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print others report as JSON.",
    )
    parser.add_argument(
        "--overrides",
        type=Path,
        default=DEFAULT_OVERRIDES_FILE,
        help="Classification override JSON. Only confidence=confirmed changes time_tags.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    music_root = args.music_root
    target_base = args.target or music_root / "time_tags"
    overrides = load_classification_overrides(args.overrides)

    if args.others_report:
        report = build_others_report(music_root, overrides=overrides)
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(format_others_report(report))
        return

    print("ファイル名/TIT2のジャンル表記を優先して再分類します...")
    source_counts = organize_library(music_root, target_base, overrides=overrides)

    print("完了")
    for slot in TIME_SLOTS:
        major = source_counts["major"][slot]
        minor = source_counts["minor"][slot]
        print(f"{slot}: total={major + minor} major={major} minor={minor}")
    for reason in SKIP_REASONS:
        major = source_counts["major"][reason]
        minor = source_counts["minor"][reason]
        print(f"{reason}: total={major + minor} major={major} minor={minor}")


if __name__ == "__main__":
    main()
