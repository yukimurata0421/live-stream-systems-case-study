#!/usr/bin/env python3
"""Build and push the public static tree to GCS."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


SITE_DIR = Path(__file__).resolve().parents[1]
PUBLIC_DIR = SITE_DIR / "public"
DEST = os.environ.get("YUKIMURATA_GCS_DEST", "").strip()
JSON_CACHE_CONTROL = os.environ.get("YUKIMURATA_JSON_CACHE_CONTROL", "public, max-age=60")
STATIC_CACHE_CONTROL = os.environ.get("YUKIMURATA_STATIC_CACHE_CONTROL", "public, max-age=300")


def run(args: list[str], *, stdout: int | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=True, text=True, stdout=stdout)


def ensure_gcloud() -> None:
    if shutil.which("gcloud") is None:
        raise FileNotFoundError("gcloud command not found")


def ensure_destination() -> None:
    if not DEST.startswith("gs://") or DEST.rstrip("/") == "gs://":
        raise RuntimeError("YUKIMURATA_GCS_DEST must name an explicit gs:// destination")


def main() -> int:
    ensure_destination()
    ensure_gcloud()
    run([sys.executable, str(SITE_DIR / "scripts" / "build_public.py")])

    print(f"Uploading {PUBLIC_DIR} -> {DEST}", flush=True)
    run(["gcloud", "storage", "rsync", str(PUBLIC_DIR), DEST, "--recursive"])
    run(
        [
            "gcloud",
            "storage",
            "objects",
            "update",
            f"{DEST}/**.json",
            f"--cache-control={JSON_CACHE_CONTROL}",
        ],
        stdout=subprocess.DEVNULL,
    )
    run(
        [
            "gcloud",
            "storage",
            "objects",
            "update",
            f"{DEST}/**.html",
            f"{DEST}/assets/**",
            f"--cache-control={STATIC_CACHE_CONTROL}",
        ],
        stdout=subprocess.DEVNULL,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
