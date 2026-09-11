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
STATUS_DIR = Path(os.environ.get("STATUS_DIR", str(SITE_DIR.parent / "status"))).expanduser()


def run_collect() -> None:
    collector = STATUS_DIR / "collect_stream_v3_public.py"
    if not collector.exists():
        raise FileNotFoundError(f"collector not found: {collector}")

    subprocess.run([sys.executable, str(collector)], check=True)


def run_reliability_collect() -> None:
    collector = SITE_DIR / "scripts" / "collect_reliability.py"
    if not collector.exists():
        raise FileNotFoundError(f"reliability collector not found: {collector}")

    subprocess.run([sys.executable, str(collector), "--force"], check=True)


def copy_required(name: str) -> None:
    src = STATUS_DIR / name
    dst = PUBLIC_DIR / name
    if not src.exists():
        raise FileNotFoundError(f"required public source missing: {src}")

    PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def copy_optional(name: str) -> None:
    src = STATUS_DIR / name
    if src.exists():
        PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, PUBLIC_DIR / name)


def main() -> int:
    run_collect()
    run_reliability_collect()

    copy_required("stream-v3-prometheus.json")
    copy_required("stream-v3-loki.json")
    copy_optional("stream-v3-prometheus.html")
    copy_optional("stream-v3-loki.html")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
