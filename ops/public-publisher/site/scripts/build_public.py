#!/usr/bin/env python3
"""Build the public static tree from the local status snapshot."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


SITE_DIR = Path(__file__).resolve().parents[1]
PUBLIC_DIR = SITE_DIR / "public"
STATUS_DIR = Path(os.environ.get("STATUS_DIR", str(SITE_DIR.parent / "status")))


def copy_optional(name: str) -> None:
    src = STATUS_DIR / name
    if src.exists():
        PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, PUBLIC_DIR / name)


def main() -> int:
    subprocess.run([sys.executable, str(SITE_DIR/'scripts/collect_observation_bundle.py')], check=True)
    # JSON generation is owned by arena. The publisher only fetches artifacts.
    copy_optional("stream-v3-prometheus.html")
    copy_optional("stream-v3-loki.html")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
