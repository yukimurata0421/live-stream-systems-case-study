#!/usr/bin/env python3
"""Prepare Floracore MP3 files for review without altering audio bytes."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections import Counter
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMP_ROOT = PROJECT_ROOT / ".temp_music"
SOURCE_ROOT = TEMP_ROOT / "floracore_EDM_music"
OUTPUT_ROOT = PROJECT_ROOT / "ncs_music" / "floracore_evening"
TRACKS_ROOT = OUTPUT_ROOT / "tracks"
DUPLICATE_LABELS = {
    ("celosia evermore", "P01"): " [Artcore Mix]",
    ("celosia evermore", "P03"): " [Data Mix]",
}
SOURCE_VIDEO_IDS = {
    "P01": "E7PJshBPeAE",
    "P02": "x8o_qyaU8n0",
    "P03": "aBwjeWdg_zQ",
    "P04": "8L_gkrEbSiM",
    "P05": "yLGDpLfOWCU",
    "P06": "_eOChDp7PY4",
    "P07": "VxgChbVZJig",
}


def natural_track_key(path: Path) -> tuple[int, str]:
    match = re.fullmatch(r"track(\d+)", path.parent.name, flags=re.IGNORECASE)
    track_number = int(match.group(1)) if match else 9999
    return track_number, path.name.casefold()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def probe(path: Path) -> dict[str, str | int | float]:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=codec_name,sample_rate,channels,bit_rate:format=duration,bit_rate",
        "-of",
        "json",
        str(path),
    ]
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    payload = json.loads(completed.stdout)
    stream = payload["streams"][0]
    audio_format = payload["format"]
    return {
        "codec": stream["codec_name"],
        "sample_rate_hz": int(stream["sample_rate"]),
        "channels": int(stream["channels"]),
        "stream_bitrate_bps": int(stream.get("bit_rate") or 0),
        "duration_seconds": round(float(audio_format["duration"]), 3),
        "container_bitrate_bps": int(audio_format.get("bit_rate") or 0),
    }


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            delete=False,
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = Path(handle.name)
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def main() -> None:
    source_paths = sorted(SOURCE_ROOT.rglob("*.mp3"), key=lambda path: str(path).casefold())
    if not source_paths:
        raise SystemExit(f"No MP3 files found under {SOURCE_ROOT}")

    collections = sorted({path.relative_to(SOURCE_ROOT).parts[0] for path in source_paths})
    collection_ids = {name: f"P{index:02d}" for index, name in enumerate(collections, start=1)}
    title_counts = Counter(path.stem.casefold() for path in source_paths)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    TRACKS_ROOT.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, str | int | float]] = []
    for collection in collections:
        playlist_id = collection_ids[collection]
        playlist_paths = sorted(
            (path for path in source_paths if path.relative_to(SOURCE_ROOT).parts[0] == collection),
            key=natural_track_key,
        )
        for source_path in playlist_paths:
            track_match = re.fullmatch(r"track(\d+)", source_path.parent.name, flags=re.IGNORECASE)
            track_number = int(track_match.group(1)) if track_match else 0
            title = source_path.stem
            duplicate_suffix = ""
            if title_counts[title.casefold()] > 1:
                duplicate_suffix = DUPLICATE_LABELS.get(
                    (title.casefold(), playlist_id),
                    f" [{playlist_id}T{track_number:02d}]",
                )
            prepared_name = f"Floracore - {title}{duplicate_suffix}.mp3"
            prepared_path = TRACKS_ROOT / prepared_name

            source_hash = sha256(source_path)
            if prepared_path.exists():
                if sha256(prepared_path) != source_hash:
                    raise RuntimeError(f"Refusing to replace a different file: {prepared_path}")
            else:
                shutil.copy2(source_path, prepared_path)

            prepared_hash = sha256(prepared_path)
            if prepared_hash != source_hash:
                raise RuntimeError(f"Copy verification failed: {prepared_path}")

            relative_source = source_path.relative_to(SOURCE_ROOT)
            rows.append(
                {
                    "prepared_filename": prepared_name,
                    "title": title,
                    "target_bucket": "evening",
                    "rotation_prefix": "minor",
                    "playlist_id": playlist_id,
                    "source_video_url": f"https://www.youtube.com/watch?v={SOURCE_VIDEO_IDS[playlist_id]}",
                    "source_track_number": track_number,
                    "source_collection": collection,
                    "source_relative_path": str(relative_source),
                    "size_bytes": source_path.stat().st_size,
                    "sha256": source_hash,
                    **probe(source_path),
                }
            )

    expected_names = {str(row["prepared_filename"]) for row in rows}
    unexpected = sorted(path.name for path in TRACKS_ROOT.glob("*.mp3") if path.name not in expected_names)
    if unexpected:
        raise RuntimeError(f"Unexpected MP3 files already exist in output: {unexpected}")

    fieldnames = list(rows[0].keys())
    csv_buffer = tempfile.SpooledTemporaryFile(mode="w+", encoding="utf-8", newline="")
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    csv_buffer.seek(0)
    atomic_write_text(OUTPUT_ROOT / "manifest.csv", csv_buffer.read())
    csv_buffer.close()

    checksum_lines = "".join(
        f'{row["sha256"]}  tracks/{row["prepared_filename"]}\n' for row in rows
    )
    atomic_write_text(OUTPUT_ROOT / "SHA256SUMS", checksum_lines)

    total_seconds = sum(float(row["duration_seconds"]) for row in rows)
    total_bytes = sum(int(row["size_bytes"]) for row in rows)
    readme = f"""# Floracore EDM evening collection

This package flattens the source directories for the permanent evening music library used by stream_v3 on Dell.

- tracks: {len(rows)}
- source collections: {len(collections)}
- total duration: {total_seconds / 3600:.2f} hours
- total size: {total_bytes / 1024 / 1024:.1f} MiB
- audio: MP3 / 44,100 Hz / stereo
- exclusive bucket: `evening` (16:00–20:59 JST)

## Audio quality

No re-encoding, gain adjustment, sample-rate conversion, or ID3 changes were performed. Each file in tracks/ is a byte-identical copy of its source MP3, verifiable with SHA256SUMS.

## Permission and credit

Permission for YouTube Live use has been obtained, conditional on attribution during the broadcast.

```text
Music provided by @Floracore_EDM
https://www.youtube.com/@Floracore_EDM
```

## Files

- `tracks/`: flattened MP3 files, normally named `Floracore - <title>.mp3`.
- `manifest.csv`: source playlist, original track order, duration, bitrate, original relative path, and SHA-256.
- `SHA256SUMS`: integrity verification after distribution or transfer.

The two Celosia Evermore files have different durations and are retained as [Artcore Mix] and [Data Mix]. ops/scripts/activate_floracore_evening.py manages the Floracore-only time_tags/evening selection; this preparation step does not duplicate the audio files.
"""
    atomic_write_text(OUTPUT_ROOT / "README.md", readme)

    print(f"prepared={len(rows)} output={OUTPUT_ROOT}")


if __name__ == "__main__":
    main()
