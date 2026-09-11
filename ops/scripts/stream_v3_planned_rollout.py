#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from maintenance_audit import audit_maintenance_decision


ANNOTATION_PREFIX = "stream-v3.yukimurata.dev"


def utc_text(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_images(values: list[str]) -> list[dict[str, str]]:
    images: list[dict[str, str]] = []
    seen: set[str] = set()
    for value in values:
        name, separator, image = value.partition("=")
        name = name.strip()
        image = image.strip()
        if not separator or not name or not image:
            raise ValueError(f"invalid --image value: {value!r}; expected container=image")
        if name in seen:
            raise ValueError(f"duplicate container in --image: {name}")
        seen.add(name)
        images.append({"name": name, "image": image})
    return images


def rollout_patch(*, rollout_id: str, reason: str, started_ts: int, ttl_sec: int, images: list[dict]) -> dict:
    annotations = {
        f"{ANNOTATION_PREFIX}/planned-rollout-id": rollout_id,
        f"{ANNOTATION_PREFIX}/planned-rollout-at": utc_text(started_ts),
        f"{ANNOTATION_PREFIX}/planned-rollout-expires-at": utc_text(started_ts + ttl_sec),
        f"{ANNOTATION_PREFIX}/planned-rollout-reason": reason[:120],
    }
    template: dict = {"metadata": {"annotations": annotations}}
    if images:
        template["spec"] = {"containers": images}
    return {"spec": {"template": template}}


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, check=False)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        description="Apply one annotated stream-v3 Deployment rollout for planned-change classification"
    )
    value.add_argument("--reason", required=True, help="short non-secret rollout reason")
    value.add_argument("--image", action="append", default=[], help="optional container=image update; repeatable")
    value.add_argument("--namespace", default="stream-v3")
    value.add_argument("--deployment", default="stream-v3-runtime")
    value.add_argument("--kubectl", default="kubectl")
    value.add_argument("--ttl-sec", type=int, default=900)
    value.add_argument("--timeout-sec", type=int, default=180)
    value.add_argument("--no-wait", action="store_true")
    value.add_argument("--dry-run", action="store_true")
    value.add_argument("--rollout-id", default="")
    value.add_argument("--maintenance-generation", type=int, default=0, help="audit-only maintenance generation")
    value.add_argument(
        "--maintenance-authorization-id",
        default=os.environ.get("MAINTENANCE_AUDIT_AUTHORIZATION_ID", ""),
        help="audit-only authorization correlation; never changes rollout behavior",
    )
    return value


def main() -> int:
    args = parser().parse_args()
    reason = str(args.reason or "").strip()
    if not reason or any(character in reason for character in "\r\n"):
        print("planned-rollout: --reason must be one non-empty line", flush=True)
        return 2
    if not 300 <= int(args.ttl_sec) <= 3600:
        print("planned-rollout: --ttl-sec must be between 300 and 3600", flush=True)
        return 2
    try:
        images = parse_images(list(args.image))
    except ValueError as exc:
        print(f"planned-rollout: {exc}", flush=True)
        return 2

    started_ts = int(time.time())
    rollout_id = str(args.rollout_id or f"rollout-{started_ts}-{uuid.uuid4().hex[:8]}")[:80]
    patch = rollout_patch(
        rollout_id=rollout_id,
        reason=reason,
        started_ts=started_ts,
        ttl_sec=int(args.ttl_sec),
        images=images,
    )
    summary = {
        "rollout_id": rollout_id,
        "namespace": args.namespace,
        "deployment": args.deployment,
        "reason": reason[:120],
        "planned_at_utc": utc_text(started_ts),
        "expires_at_utc": utc_text(started_ts + int(args.ttl_sec)),
        "images": images,
        "dry_run": bool(args.dry_run),
    }
    if args.dry_run:
        print(json.dumps({**summary, "patch": patch}, ensure_ascii=False, separators=(",", ":")))
        return 0

    command = [
        args.kubectl,
        "-n",
        args.namespace,
        "patch",
        "deployment",
        args.deployment,
        "--type=strategic",
        "-p",
        json.dumps(patch, ensure_ascii=False, separators=(",", ":")),
    ]
    audit_maintenance_decision(
        path_id="MP-01",
        phase="ADMISSION",
        operation="restart_deployment",
        path_role="PLANNED_EXECUTOR",
        process_service="stream_v3_planned_rollout.py",
        resource_identity=f"deployment/{args.namespace}/{args.deployment}",
        correlation_id=rollout_id,
        operation_generation=int(args.maintenance_generation) or None,
        authorization_id=str(args.maintenance_authorization_id or ""),
        in_flight_evidence={"status": "PROPOSED", "count": 0, "source": "operator process before kubectl PATCH"},
        generation_evidence={"status": "CONFIRMED", "rollout_id": rollout_id, "source": "planned rollout annotation"},
    )
    audit_maintenance_decision(
        path_id="MP-01",
        phase="EFFECT_BOUNDARY",
        operation="restart_deployment",
        path_role="PLANNED_EXECUTOR",
        process_service="stream_v3_planned_rollout.py",
        resource_identity=f"deployment/{args.namespace}/{args.deployment}",
        correlation_id=rollout_id,
        operation_generation=int(args.maintenance_generation) or None,
        authorization_id=str(args.maintenance_authorization_id or ""),
        in_flight_evidence={"status": "PROPOSED", "count": 1, "source": "kubectl PATCH call about to begin"},
        generation_evidence={"status": "CONFIRMED", "rollout_id": rollout_id, "source": "planned rollout annotation"},
    )
    completed = run_command(command)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "kubectl patch failed").strip()
        print(f"planned-rollout: patch failed: {detail}", flush=True)
        return 1
    if not args.no_wait:
        status = run_command(
            [
                args.kubectl,
                "-n",
                args.namespace,
                "rollout",
                "status",
                f"deployment/{args.deployment}",
                f"--timeout={max(30, int(args.timeout_sec))}s",
            ]
        )
        if status.returncode != 0:
            detail = (status.stderr or status.stdout or "rollout status failed").strip()
            print(json.dumps({**summary, "status": "rollout_unconfirmed", "detail": detail[:240]}, ensure_ascii=False))
            return 1
    print(json.dumps({**summary, "status": "completed"}, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
