"""Prepare one conflict-checked Dell owner-export rollout and exact rollback.

Read-only Kubernetes access. Generated private artifacts do not apply a change.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

DEPLOYMENT_UID = "c0028d12-12be-414b-a382-5c2b8ea8764b"
BASE_IMAGE = "stream-v3:ack-correlation-repair-20260903-v25"


def build_upgrade_plan(
    deployment: dict[str, Any], *, image: str, release_id: str, expected_image: str, expected_generation: int, now: datetime
) -> tuple[list[Any], list[Any]]:
    """Upgrade the existing owner only; preserve all other template fields."""
    if (
        not re.fullmatch(r"recovery-owner-[a-z0-9.-]+", release_id)
        or image != "stream-v3:" + release_id
        or not re.fullmatch(r"stream-v3:recovery-owner-[a-z0-9.-]+", expected_image)
        or image == expected_image
        or type(expected_generation) is not int
        or expected_generation < 1
    ):
        raise ValueError("OWNER_UPGRADE_IDENTITY_INVALID")
    meta, spec = deployment["metadata"], deployment["spec"]
    if (
        meta["uid"] != DEPLOYMENT_UID
        or meta["generation"] != expected_generation
        or spec["replicas"] != 1
        or spec["strategy"] != {"type": "Recreate"}
    ):
        raise ValueError("OWNER_DEPLOYMENT_IDENTITY_DRIFT")
    before = copy.deepcopy(spec["template"])
    after = copy.deepcopy(before)
    containers = after["spec"]["containers"]
    engines = [c for c in containers if c["name"] == "stream-engine"]
    if len(containers) != 4 or len(engines) != 1 or engines[0]["image"] != expected_image:
        raise ValueError("OWNER_RUNTIME_IDENTITY_DRIFT")
    engine = engines[0]
    config_env = [e for e in engine["env"] if e["name"] == "FR_RECOVERY_EVIDENCE_CONFIG_FILE"]
    if config_env != [{"name": "FR_RECOVERY_EVIDENCE_CONFIG_FILE", "value": "/recovery-config/owner.json"}]:
        raise ValueError("OWNER_UPGRADE_CONFIG_BINDING_INVALID")
    old_release = expected_image.removeprefix("stream-v3:")
    roots = {
        "recovery-config": "/etc/stream-recovery-observation/runtime-owner",
        "recovery-evidence": "/var/lib/stream-recovery-observation/runtime-owner",
    }
    for name, root in roots.items():
        volumes = [v for v in after["spec"]["volumes"] if v["name"] == name]
        mounts = [m for m in engine["volumeMounts"] if m["name"] == name]
        if (
            len(volumes) != 1
            or volumes[0].get("hostPath") != {"path": f"{root}/{old_release}", "type": "Directory"}
            or len(mounts) != 1
            or mounts[0].get("mountPath") != "/" + name
            or mounts[0].get("readOnly", False) is not (name == "recovery-config")
        ):
            raise ValueError("OWNER_UPGRADE_VOLUME_BINDING_INVALID")
        volumes[0]["hostPath"]["path"] = f"{root}/{release_id}"
    engine["image"] = image
    rollback = copy.deepcopy(before)
    for template, suffix in ((after, "enable"), (rollback, "rollback")):
        prefix = "stream-v3.yukimurata.dev/planned-rollout-"
        annotations = template.setdefault("metadata", {}).setdefault("annotations", {})
        annotations.update(
            {
                prefix + "id": release_id + "-" + suffix,
                prefix + "at": now.isoformat(),
                prefix + "expires-at": (now + timedelta(minutes=15)).isoformat(),
                prefix + "reason": "independent owner ledger evidence " + suffix,
            }
        )

    def patch(expected: dict[str, Any], replacement: dict[str, Any], generation: int) -> list[Any]:
        return [
            {"op": "test", "path": "/metadata/uid", "value": DEPLOYMENT_UID},
            {"op": "test", "path": "/metadata/generation", "value": generation},
            {"op": "test", "path": "/spec/replicas", "value": 1},
            {"op": "test", "path": "/spec/strategy", "value": {"type": "Recreate"}},
            {"op": "test", "path": "/spec/template", "value": expected},
            {"op": "replace", "path": "/spec/template", "value": replacement},
        ]

    return patch(before, after, expected_generation), patch(after, rollback, expected_generation + 1)


def build_plan(deployment: dict[str, Any], *, image: str, release_id: str, now: datetime) -> tuple[list[Any], list[Any]]:
    if not re.fullmatch(r"stream-v3:recovery-owner-[a-z0-9.-]+", image) or not re.fullmatch(r"recovery-owner-[a-z0-9.-]+", release_id):
        raise ValueError("OWNER_CANDIDATE_ID_INVALID")
    meta, spec = deployment["metadata"], deployment["spec"]
    if meta["uid"] != DEPLOYMENT_UID or meta["generation"] != 354 or spec["replicas"] != 1 or spec["strategy"] != {"type": "Recreate"}:
        raise ValueError("OWNER_DEPLOYMENT_IDENTITY_DRIFT")
    before = copy.deepcopy(spec["template"])
    after = copy.deepcopy(before)
    engines = [c for c in after["spec"]["containers"] if c["name"] == "stream-engine"]
    if len(engines) != 1 or len(after["spec"]["containers"]) != 4 or engines[0]["image"] != BASE_IMAGE:
        raise ValueError("OWNER_RUNTIME_IDENTITY_DRIFT")
    engine = engines[0]
    values = {e["name"]: e.get("value") for e in engine["env"]}
    if any(
        values.get(k) != v
        for k, v in {"FR_FFMPEG_FORCE_KILL_ENABLED": "1", "FR_FFMPEG_TERM_GRACE_SEC": "2", "FR_FFMPEG_KILL_WAIT_SEC": "1"}.items()
    ):
        raise ValueError("OWNER_TERMINATION_PROFILE_DRIFT")
    additions = {
        "FFMPEG_RW_TIMEOUT_ENABLED": "1",
        "FFMPEG_RW_TIMEOUT_USEC": "15000000",
        "FR_RECOVERY_EVIDENCE_CONFIG_FILE": "/recovery-config/owner.json",
    }
    if set(values).intersection(additions):
        raise ValueError("OWNER_CONFIG_ALREADY_PRESENT")
    engine["image"] = image
    engine["env"].extend({"name": key, "value": value} for key, value in additions.items())
    mounts = {"recovery-config": "/recovery-config", "recovery-evidence": "/recovery-evidence"}
    if {v["name"] for v in after["spec"]["volumes"]}.intersection(mounts):
        raise ValueError("OWNER_VOLUME_ALREADY_PRESENT")
    for name, path in mounts.items():
        engine["volumeMounts"].append({"name": name, "mountPath": path, "readOnly": name == "recovery-config"})
        host_path = (
            f"/etc/stream-recovery-observation/runtime-owner/{release_id}"
            if name == "recovery-config"
            else f"/var/lib/stream-recovery-observation/runtime-owner/{release_id}"
        )
        after["spec"]["volumes"].append({"name": name, "hostPath": {"path": host_path, "type": "Directory"}})

    def annotate(template: dict[str, Any], suffix: str) -> None:
        prefix = "stream-v3.yukimurata.dev/planned-rollout-"
        annotations = template.setdefault("metadata", {}).setdefault("annotations", {})
        annotations.update(
            {
                prefix + "id": release_id + "-" + suffix,
                prefix + "at": now.isoformat(),
                prefix + "expires-at": (now + timedelta(minutes=15)).isoformat(),
                prefix + "reason": "recovery soak owner export and bounded network IO " + suffix,
            }
        )

    annotate(after, "enable")
    restored = copy.deepcopy(before)
    annotate(restored, "rollback")

    def patch(expected: dict[str, Any], replacement: dict[str, Any], generation: int) -> list[Any]:
        return [
            {"op": "test", "path": "/metadata/uid", "value": DEPLOYMENT_UID},
            {"op": "test", "path": "/metadata/generation", "value": generation},
            {"op": "test", "path": "/spec/replicas", "value": 1},
            {"op": "test", "path": "/spec/strategy", "value": {"type": "Recreate"}},
            {"op": "test", "path": "/spec/template", "value": expected},
            {"op": "replace", "path": "/spec/template", "value": replacement},
        ]

    return patch(before, after, 354), patch(after, restored, 355)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--upgrade-current-image")
    parser.add_argument("--upgrade-current-generation", type=int)
    args = parser.parse_args()
    raw = subprocess.run(
        ["sudo", "-n", "k3s", "kubectl", "-n", "stream-v3", "get", "deployment", "stream-v3-runtime", "-o", "json"],
        capture_output=True,
        check=True,
        text=True,
        timeout=15,
    ).stdout
    now = datetime.now(UTC)
    if (args.upgrade_current_image is None) != (args.upgrade_current_generation is None):
        raise ValueError("OWNER_UPGRADE_EXPECTED_IDENTITY_REQUIRED")
    if args.upgrade_current_image is not None:
        candidate, rollback = build_upgrade_plan(
            json.loads(raw),
            image=args.image,
            release_id=args.release_id,
            expected_image=args.upgrade_current_image,
            expected_generation=args.upgrade_current_generation,
            now=now,
        )
    else:
        candidate, rollback = build_plan(json.loads(raw), image=args.image, release_id=args.release_id, now=now)
    os.umask(0o077)
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    hashes = {}
    for name, value in (("candidate.json", candidate), ("rollback.json", rollback), ("before.json", json.loads(raw))):
        data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
        with (args.output / name).open("xb") as target:
            target.write(data)
        hashes[name] = hashlib.sha256(data).hexdigest()
    print(json.dumps({"prepared_at": now.isoformat(), "applied": False, "sha256": hashes}))


if __name__ == "__main__":
    main()
