#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[2]
REQUIRED_DOCKERIGNORE_PATTERNS = (
    ".git/",
    ".state/",
    "venv/",
    ".venv/",
    "ncs_music/*",
    "logs/",
    "runtime/",
)


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str
    command: tuple[str, ...] = ()
    blocker: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "ok": self.ok,
            "detail": self.detail,
            "command": list(self.command),
            "blocker": self.blocker,
        }


RunCommand = Callable[[list[str]], subprocess.CompletedProcess[str]]
Which = Callable[[str], str | None]


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, text=True, capture_output=True, check=False, timeout=15)


def command_check(name: str, *, which: Which = shutil.which, blocker: bool = True) -> Check:
    path = which(name)
    return Check(name=f"command:{name}", ok=bool(path), detail=path or "not found", blocker=blocker and not bool(path))


def run_check(name: str, command: list[str], *, runner: RunCommand = run_command, blocker: bool = True) -> Check:
    try:
        cp = runner(command)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return Check(name=name, ok=False, detail=str(exc), command=tuple(command), blocker=blocker)
    detail = (cp.stdout or cp.stderr or "").strip().splitlines()
    return Check(
        name=name,
        ok=cp.returncode == 0,
        detail=detail[0] if detail else f"rc={cp.returncode}",
        command=tuple(command),
        blocker=blocker and cp.returncode != 0,
    )


def ffmpeg_nvenc_check(*, runner: RunCommand = run_command, blocker: bool = True) -> Check:
    command = ["ffmpeg", "-hide_banner", "-encoders"]
    try:
        cp = runner(command)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return Check(name="ffmpeg:h264_nvenc", ok=False, detail=str(exc), command=tuple(command), blocker=blocker)
    output = "\n".join(part for part in (cp.stdout, cp.stderr) if part)
    ok = cp.returncode == 0 and "h264_nvenc" in output
    if ok:
        detail = "h264_nvenc encoder is available"
    elif cp.returncode == 0:
        detail = "h264_nvenc encoder is missing"
    else:
        detail_lines = output.strip().splitlines()
        detail = detail_lines[0] if detail_lines else f"rc={cp.returncode}"
    return Check(name="ffmpeg:h264_nvenc", ok=ok, detail=detail, command=tuple(command), blocker=blocker and not ok)


def dockerignore_check(path: Path = ROOT / ".dockerignore") -> Check:
    try:
        patterns = {
            line.strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
    except OSError as exc:
        return Check(name="build-context:dockerignore", ok=False, detail=str(exc), blocker=True)

    missing = [pattern for pattern in REQUIRED_DOCKERIGNORE_PATTERNS if pattern not in patterns]
    if missing:
        return Check(
            name="build-context:dockerignore",
            ok=False,
            detail="missing " + ",".join(missing),
            blocker=True,
        )
    return Check(
        name="build-context:dockerignore",
        ok=True,
        detail="excludes state, venv, logs, runtime, git, and local music payload",
    )


def preflight(*, overlay: str = "shadow", runner: RunCommand = run_command, which: Which = shutil.which) -> dict[str, object]:
    checks: list[Check] = []
    validate_command = ["python3", "ops/scripts/validate_k3s_manifests.py"]
    if overlay != "shadow":
        validate_command.extend(["--overlay", overlay])
    checks.append(run_check(f"manifest:{overlay}", validate_command, runner=runner))
    checks.append(dockerignore_check())

    nvidia_smi = command_check("nvidia-smi", which=which)
    checks.append(nvidia_smi)
    if nvidia_smi.ok:
        checks.append(
            run_check(
                "nvidia-smi:gpu",
                ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
                runner=runner,
            )
        )

    ffmpeg = command_check("ffmpeg", which=which)
    checks.append(ffmpeg)
    if ffmpeg.ok:
        checks.append(ffmpeg_nvenc_check(runner=runner))

    kubectl = command_check("kubectl", which=which)
    checks.append(kubectl)
    if kubectl.ok:
        checks.append(run_check("kubectl:client", ["kubectl", "version", "--client=true"], runner=runner, blocker=False))
        checks.append(run_check("kubectl:cluster", ["kubectl", "cluster-info"], runner=runner))
        if overlay != "shadow":
            checks.append(
                run_check(
                    "secret:stream-v3-secrets",
                    ["kubectl", "-n", "stream-v3", "get", "secret", "stream-v3-secrets"],
                    runner=runner,
                    blocker=True,
                )
            )

    k3s = command_check("k3s", which=which, blocker=False)
    checks.append(k3s)
    if k3s.ok:
        checks.append(run_check("k3s:version", ["k3s", "--version"], runner=runner, blocker=False))

    image_tools = []
    for tool in ("nerdctl", "podman", "docker"):
        item = command_check(tool, which=which, blocker=False)
        checks.append(item)
        if item.ok:
            image_tools.append(tool)

    if not image_tools:
        checks.append(Check(name="image-build-tool", ok=False, detail="nerdctl, podman, or docker is required", blocker=True))
    elif "docker" in image_tools:
        checks.append(run_check("docker:daemon", ["docker", "info", "--format", "{{.ServerVersion}}"], runner=runner))

    blockers = [check.to_dict() for check in checks if check.blocker]
    return {
        "repo_root": str(ROOT),
        "ok": not blockers,
        "checks": [check.to_dict() for check in checks],
        "blockers": blockers,
        "next_apply_command": (
            f"kubectl kustomize --load-restrictor=LoadRestrictionsNone deploy/k3s/{overlay} | kubectl apply -f -"
        ),
        "next_build_command": next_build_command(image_tools),
    }


def next_build_command(image_tools: list[str]) -> str:
    available = set(image_tools)
    if "nerdctl" in available:
        return "nerdctl -n k8s.io build -f deploy/k3s/Containerfile -t stream-v3:local ."
    if "podman" in available:
        return "podman build -f deploy/k3s/Containerfile -t stream-v3:local ."
    if "docker" in available:
        return "docker build -f deploy/k3s/Containerfile -t stream-v3:local ."
    return "install nerdctl, podman, or docker before building stream-v3:local"


def render_text(report: dict[str, object]) -> str:
    lines = [f"[v3-k3s-preflight] ok={str(report.get('ok')).lower()} repo={report.get('repo_root')}"]
    checks = report.get("checks") if isinstance(report.get("checks"), list) else []
    for item in checks:
        if not isinstance(item, dict):
            continue
        status = "ok" if item.get("ok") else "fail"
        marker = " blocker" if item.get("blocker") else ""
        lines.append(f"[{status}] {item.get('name')}{marker}: {item.get('detail')}")
    if report.get("ok"):
        lines.append(f"[next] build: {report.get('next_build_command')}")
        lines.append(f"[next] apply: {report.get('next_apply_command')}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check whether this host can build/apply stream_v3 k3s shadow stack.")
    parser.add_argument("--json", action="store_true", help="print machine-readable preflight result")
    parser.add_argument(
        "--overlay",
        default="shadow",
        help="kustomize overlay to validate and suggest for apply; default: shadow",
    )
    args = parser.parse_args(argv)
    report = preflight(overlay=args.overlay)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(render_text(report))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
