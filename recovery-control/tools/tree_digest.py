#!/usr/bin/env python3
"""Print a stable relative-path/content digest for an immutable release tree."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    aggregate = hashlib.sha256()
    files = 0
    symlinks = 0
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            payload = f"L\0{relative}\0{path.readlink()}\n".encode()
            symlinks += 1
        elif path.is_file():
            payload = f"F\0{relative}\0{hashlib.sha256(path.read_bytes()).hexdigest()}\n".encode()
            files += 1
        else:
            continue
        aggregate.update(payload)
    print(
        json.dumps(
            {
                "schema_version": "release.tree_digest.v1",
                "root": str(root),
                "file_count": files,
                "symlink_count": symlinks,
                "tree_sha256": aggregate.hexdigest(),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
