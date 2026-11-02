#!/usr/bin/env python3
"""Read-only source provenance for existing producers, not process health proof."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess
import time


def sha(data):
    return hashlib.sha256(data).hexdigest()


def load(path):
    path = Path(path)
    if path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError("registry too large")
    value = json.loads(path.read_bytes())
    if value.get("schema") != "stream.release_registry.v1" or not re.fullmatch("[0-9a-f]{40}", value.get("commit", "")):
        raise ValueError("invalid registry")
    if not 0 < len(value["components"]) <= 32:
        raise ValueError("registry component limit")
    names = set()
    for component in value["components"]:
        if not re.fullmatch("[a-z0-9-]{1,64}", component["name"]) or component["name"] in names:
            raise ValueError("invalid component name")
        names.add(component["name"])
        if not Path(component["root"]).is_absolute() or not 0 < len(component["files"]) <= 4096:
            raise ValueError("invalid source root or file count")
        if component.get("kind") not in ("committed-artifact", "legacy-source-pinned"):
            raise ValueError("invalid provenance kind")
        if component.get("commit") and not re.fullmatch("[0-9a-f]{40}", component["commit"]):
            raise ValueError("invalid component commit")
        for name, expected in component["files"].items():
            if Path(name).is_absolute() or any(x in ("", ".", "..") for x in name.split("/")) or "\\" in name:
                raise ValueError("unsafe source path")
            if not re.fullmatch("[0-9a-f]{64}", expected):
                raise ValueError("invalid source digest")
        unit = component.get("unit")
        if unit and not re.fullmatch("[a-zA-Z0-9_-]+\\.service", unit):
            raise ValueError("invalid unit")
    return value


def audit(registry):
    results = []
    remaining = 64 * 1024 * 1024
    for component in registry["components"]:
        root = Path(component["root"])
        available = matched = True
        try:
            if root.is_symlink() or not root.is_dir():
                raise ValueError("physical source root required")
            if component.get("pointer"):
                pointer = Path(component["pointer"])
                matched = pointer.is_symlink() and pointer.resolve() == root
            for rel, expected in component["files"].items():
                p = root / rel
                if p.is_symlink() or not p.resolve().is_relative_to(root) or not p.is_file():
                    raise ValueError("invalid source file")
                size = p.stat().st_size
                if size > 2 * 1024 * 1024 or size > remaining:
                    raise ValueError("source byte limit")
                remaining -= size
                matched = sha(p.read_bytes()) == expected and matched
            if component["kind"] == "committed-artifact":
                from stream_v3_release import verify
                manifest = verify(root, component["artifact_sha256"], require_bindings=True)
                matched = matched and manifest["commit"] == component.get("commit", registry["commit"])
        except (OSError, ValueError):
            available = matched = False
        entry = None
        if component.get("unit") and component.get("entrypoint"):
            try:
                command = subprocess.check_output(["systemctl", "show", component["unit"],
                    "--property=ExecStart", "--value"], timeout=3).decode()
                entry = bool(re.search(r'(?:^|[\s=\"\'])' + re.escape(component["entrypoint"]) + r'(?=$|[\s;\"\'])', command))
            except (OSError, subprocess.SubprocessError):
                entry = None
        # This is a source-on-disk check. Never label these bytes as loaded memory.
        results.append({"name": component["name"], "kind": component["kind"],
            "commit": component.get("commit", registry["commit"]),
            "source_hash": sha(json.dumps(component["files"], sort_keys=True, separators=(",", ":")).encode()),
            "available": available, "source_match": matched, "configured_entrypoint_match": entry})
    return results


def render(registry, rows):
    lines = ["# HELP stream_v3_observed_release_info Expected committed source inventory; not loaded-memory or health proof.",
             "# TYPE stream_v3_observed_release_info gauge"]
    for row in rows:
        labels = f'component="{row["name"]}",commit="{row["commit"]}",source_hash="{row["source_hash"]}",kind="{row["kind"]}"'
        lines.append(f"stream_v3_observed_release_info{{{labels}}} 1")
        for key in ("available", "source_match", "configured_entrypoint_match"):
            if row[key] is not None:
                lines.append(f'stream_v3_observed_release_{key}{{component="{row["name"]}"}} {int(row[key])}')
    return "\n".join(lines) + "\n"


class CachedAudit:
    def __init__(self, path, interval=60):
        self.registry = load(path)
        self.interval = interval
        self.checked = None
        self.metrics = ""

    def read(self):
        now = time.monotonic()
        if self.checked is None or now - self.checked >= self.interval:
            self.metrics = render(self.registry, audit(self.registry))
            self.checked = now
        return self.metrics
