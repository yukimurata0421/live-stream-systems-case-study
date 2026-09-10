#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any


def run(*args: str) -> str:
    completed = subprocess.run(args, check=True, capture_output=True, text=True, timeout=60)
    return completed.stdout.strip()


def container_hashes(image: str, paths: list[str]) -> dict[str, str]:
    output = run("docker", "run", "--rm", "--network", "none", "--entrypoint", "sha256sum", image, *paths)
    result: dict[str, str] = {}
    for line in output.splitlines():
        digest, path = line.split(maxsplit=1)
        result[path.strip()] = digest
    return result


def image_identity(image: str) -> dict[str, str]:
    raw = run(
        "docker",
        "image",
        "inspect",
        image,
        "--format",
        "{{json .}}",
    )
    value = json.loads(raw)
    labels = value.get("Config", {}).get("Labels", {})
    return {
        "image_id": str(value.get("Id") or ""),
        "runtime_boundary_release": str(labels.get("org.stream-v3.runtime-boundary-release") or ""),
    }


def verify_section(section: dict[str, Any]) -> dict[str, Any]:
    image = str(section["candidate_image"])
    expected = {"/" + relative: str(metadata["sha256"]) for relative, metadata in section["source_files"].items()}
    observed = container_hashes(image, sorted(expected))
    mismatches = {
        path: {"expected": expected[path], "observed": observed.get(path, "MISSING")}
        for path in expected
        if observed.get(path) != expected[path]
    }
    return {
        "image": image,
        "identity": image_identity(image),
        "expected_file_count": len(expected),
        "observed_file_count": len(observed),
        "mismatches": mismatches,
        "pass": not mismatches,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads((args.release / "release_manifest.json").read_text(encoding="utf-8"))
    executor = verify_section(manifest["executor_image"])
    fast = verify_section(manifest["fast_image"])
    base_stream_engine = container_hashes(
        str(manifest["executor_image"]["base_image"]),
        ["/app/src/stream_core/stream_engine.py"],
    )["/app/src/stream_core/stream_engine.py"]
    candidate_stream_engine = container_hashes(
        str(manifest["executor_image"]["candidate_image"]),
        ["/app/src/stream_core/stream_engine.py"],
    )["/app/src/stream_core/stream_engine.py"]
    result = {
        "schema_version": "runtime_boundary.image_verification.v1",
        "release_id": manifest["release_id"],
        "executor": executor,
        "fast": fast,
        "base_stream_engine_sha256": base_stream_engine,
        "candidate_stream_engine_sha256": candidate_stream_engine,
        "base_stream_engine_unchanged": base_stream_engine == candidate_stream_engine,
        "base_stream_engine_change_expected": bool(manifest["executor_image"].get("base_stream_engine_replaced")),
        "enforcement_enabled": manifest["enforcement_enabled"],
        "complete": executor["pass"]
        and fast["pass"]
        and (
            (base_stream_engine != candidate_stream_engine)
            if manifest["executor_image"].get("base_stream_engine_replaced")
            else (base_stream_engine == candidate_stream_engine)
        )
        and manifest["enforcement_enabled"] is False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
