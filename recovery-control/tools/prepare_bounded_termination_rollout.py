#!/usr/bin/env python3
"""Dell v25専用のread-only plan builder。applyは行わず、競合検知付きpatchを保存する。"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

EXPECTED_UID = "c0028d12-12be-414b-a382-5c2b8ea8764b"
EXPECTED_IMAGE = "stream-v3:ack-correlation-repair-20260903-v25"
VALUES = {"FR_FFMPEG_FORCE_KILL_ENABLED": "1", "FR_FFMPEG_TERM_GRACE_SEC": "2", "FR_FFMPEG_KILL_WAIT_SEC": "1"}
PREFIX = "stream-v3.yukimurata.dev/planned-rollout-"


def rollout_annotations(now: dt.datetime, *, rollback: bool) -> dict[str, str]:
    def iso(value: dt.datetime) -> str:
        return value.isoformat(timespec="seconds").replace("+00:00", "Z")

    suffix = "rollback" if rollback else "enable"
    return {
        PREFIX + "id": f"bounded-termination-v26-{suffix}-{now.strftime('%Y%m%dT%H%M%SZ')}",
        PREFIX + "at": iso(now),
        PREFIX + "expires-at": iso(now + dt.timedelta(seconds=3600)),
        PREFIX + "reason": f"bounded exact-child termination {suffix}; unchanged v25 image and controller",
    }


def build_plan(deployment: dict[str, Any], now: dt.datetime) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    meta = deployment["metadata"]
    spec = deployment["spec"]
    if meta["uid"] != EXPECTED_UID or meta["generation"] != 353:
        raise ValueError("DEPLOYMENT_IDENTITY_DRIFT")
    if spec["replicas"] != 1 or spec["strategy"] != {"type": "Recreate"}:
        raise ValueError("DEPLOYMENT_STRATEGY_DRIFT")
    before = copy.deepcopy(spec["template"])
    containers = before["spec"]["containers"]
    indexes = [i for i, item in enumerate(containers) if item["name"] == "stream-engine"]
    if len(indexes) != 1 or len(containers) != 4:
        raise ValueError("CONTAINER_CARDINALITY_DRIFT")
    index = indexes[0]
    engine = containers[index]
    if engine["image"] != EXPECTED_IMAGE:
        raise ValueError("STREAM_IMAGE_DRIFT")
    if not isinstance(engine.get("env"), list):
        raise ValueError("ENV_SHAPE_DRIFT")
    forbidden = {*VALUES, "FFMPEG_RW_TIMEOUT_ENABLED", "FFMPEG_RW_TIMEOUT_USEC"}
    if any(item["name"] in forbidden for item in engine["env"]):
        raise ValueError("EXPECTED_UNSET_GATES_DRIFT")
    after = copy.deepcopy(before)
    after["spec"]["containers"][index]["env"].extend({"name": key, "value": value} for key, value in VALUES.items())
    after.setdefault("metadata", {}).setdefault("annotations", {}).update(rollout_annotations(now, rollback=False))
    restored = copy.deepcopy(before)
    restored.setdefault("metadata", {}).setdefault("annotations", {}).update(rollout_annotations(now, rollback=True))

    def patch(expected: dict[str, Any], replacement: dict[str, Any], generation: int) -> list[dict[str, Any]]:
        return [
            {"op": "test", "path": "/metadata/uid", "value": EXPECTED_UID},
            {"op": "test", "path": "/metadata/generation", "value": generation},
            {"op": "test", "path": "/spec/replicas", "value": 1},
            {"op": "test", "path": "/spec/strategy", "value": {"type": "Recreate"}},
            {"op": "test", "path": "/spec/template", "value": expected},
            {"op": "replace", "path": "/spec/template", "value": replacement},
        ]

    return patch(before, after, 353), patch(after, restored, 354)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    raw = subprocess.run(
        ["sudo", "k3s", "kubectl", "-n", "stream-v3", "get", "deployment", "stream-v3-runtime", "-o", "json"],
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    ).stdout
    now = dt.datetime.now(dt.UTC)
    candidate, rollback = build_plan(json.loads(raw), now)
    # Patchのtest operandに既存templateを含むため、Git除外領域へowner-onlyで保存する。
    os.umask(0o077)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    hashes = {}
    for name, patch in (("candidate.json", candidate), ("rollback.json", rollback)):
        payload = json.dumps(patch, sort_keys=True, indent=2) + "\n"
        with (args.output_dir / name).open("x", encoding="utf-8") as handle:
            handle.write(payload)
        hashes[name] = hashlib.sha256(payload.encode()).hexdigest()
    manifest = {
        "prepared_at": now.isoformat(),
        "host_id": "dell-stream-runtime",
        "deployment_uid": EXPECTED_UID,
        "before_generation": 353,
        "image_unchanged": EXPECTED_IMAGE,
        "values": VALUES,
        "patch_sha256": hashes,
        "action": "PLAN_ONLY",
        "rollback": "restore prior env once; template/generation drift rejects patch",
    }
    with (args.output_dir / "plan_manifest.json").open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, sort_keys=True, indent=2)
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
