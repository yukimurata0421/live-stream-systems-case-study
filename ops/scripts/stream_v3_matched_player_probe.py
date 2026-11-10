#!/usr/bin/env python3
"""Observe the public YouTube player with the browser from the live Dell image."""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from zoneinfo import ZoneInfo

from stream_v3_release import verify

KUBECTL = ("sudo", "-n", "/usr/local/bin/kubectl", "-n", "stream-v3")
DOCKER = ("sudo", "-n", "docker")
SELECTOR = "app.kubernetes.io/name=stream-v3,app.kubernetes.io/component=runtime"
ENGINE = "stream-engine"
UNIT = "stream-v3-matched-player-probe"
PROBE = "ops/scripts/stream_v3_player_av_probe.py"
DIGEST = r"sha256:[0-9a-f]{64}"
BROWSER_PATH = r"/opt/stream-browser/(\d+\.\d+\.\d+\.\d+)/chrome"
CONFIG_KEYS = {
    "PLAYER_VIDEO_ID": "video_id",
    "PLAYER_ARTIFACT": "artifact",
    "PLAYER_ARTIFACT_SHA256": "artifact_sha256",
    "PLAYER_PLAYWRIGHT_SITE": "playwright_site",
    "PLAYER_PILLOW_SITE": "pillow_site",
    "PLAYER_OCR_ROOT": "ocr",
    "PLAYER_OUTPUT_ROOT": "output_root",
}

# In the runtime container, inspect the kernel's executable links without
# reading command lines or the environment, and never start a browser there.
READ_BROWSER = r"""
import hashlib, json, os, re
from pathlib import Path

pattern = r"/opt/stream-browser/\d+\.\d+\.\d+\.\d+/chrome"
executables = {}
for proc in Path("/proc").glob("[0-9]*"):
    try:
        if (proc / "comm").read_text().strip() not in {"chrome", "chromium"}:
            continue
        link = proc / "exe"
        path = os.readlink(link)
        if not re.fullmatch(pattern, path):
            raise SystemExit("unexpected running browser path")
        stat = link.stat()
        identity = path, stat.st_dev, stat.st_ino
        executables.setdefault(identity, []).append(link)
    except (FileNotFoundError, ProcessLookupError):
        continue
if len(executables) != 1:
    raise SystemExit("expected one running browser executable")
(path, device, inode), links = next(iter(executables.items()))
for link in links:
    try:
        with link.open("rb") as exe:
            stat = os.fstat(exe.fileno())
            if (stat.st_dev, stat.st_ino) != (device, inode):
                continue
            sha256 = hashlib.file_digest(exe, "sha256").hexdigest()
            print(json.dumps(dict(path=path, sha256=sha256)))
            break
    except (FileNotFoundError, ProcessLookupError):
        continue
else:
    raise SystemExit("running browser disappeared")
"""


def read(*command, timeout=25):
    return subprocess.check_output(command, text=True, timeout=timeout).strip()


def runtime():
    pods = json.loads(read(*KUBECTL, "get", "pods", "-l", SELECTOR, "-o", "json"))["items"]
    pods = [pod for pod in pods if not pod["metadata"].get("deletionTimestamp")]
    if len(pods) != 1:
        raise ValueError("expected a single runtime Pod")
    pod = pods[0]
    for container in pod["status"].get("containerStatuses", []):
        if container["name"] == ENGINE and container["ready"] and "running" in container["state"]:
            return {
                "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                "pod": pod["metadata"]["name"], "uid": pod["metadata"]["uid"],
                "container_id": container["containerID"], "image_id": container["imageID"],
                "restart_count": container["restartCount"],
            }
    raise ValueError("stream-engine is not ready")


def running_browser(target):
    browser = json.loads(read(*KUBECTL, "exec", target["pod"], "-c", ENGINE, "--",
                              "nice", "-n", "19", "python3", "-I", "-B", "-c", READ_BROWSER))
    match = re.fullmatch(BROWSER_PATH, browser["path"])
    if not match or not re.fullmatch(r"[0-9a-f]{64}", browser["sha256"]) or runtime() != target:
        raise ValueError("runtime changed while identifying the browser")
    return dict(browser, version=match[1])


def copy_browser(image, browser, destination):
    # Docker's short library name and containerd's fully qualified name
    # represent the same repository. Do not resolve a tag or pull an image.
    match = re.fullmatch(rf"docker\.io/library/stream-v3@({DIGEST})", image)
    if not match:
        raise ValueError("expected the Dell runtime image digest")
    reference = f"stream-v3@{match[1]}"
    local = json.loads(read(*DOCKER, "image", "inspect", reference, "--format", "{{json .}}"))
    if reference not in local["RepoDigests"] or not re.fullmatch(DIGEST, local["Id"]):
        raise ValueError("matching image is not available locally")
    source = read(*DOCKER, "create", "--pull=never", "--network=none",
                  "--entrypoint", "/bin/false", local["Id"])
    try:
        command = (*DOCKER, "cp", f"{source}:{Path(browser['path']).parent}/.", "-")
        with subprocess.Popen(command, stdout=subprocess.PIPE) as archive:
            try:
                subprocess.run(["tar", "--extract", "--no-same-owner", "--directory", str(destination)],
                               stdin=archive.stdout, check=True, timeout=60)
                archive.stdout.close()
                if archive.wait(timeout=5) != 0:
                    raise ValueError("could not copy browser from the local image")
            finally:
                if archive.poll() is None:
                    archive.kill()
    finally:
        read(*DOCKER, "rm", source)
    return local["Id"]


def isolated_command(args, stage, browser):
    properties = [
        "User=nobody", "Group=nogroup", "CPUQuota=100%", "CPUWeight=1", "MemoryMax=1G",
        "MemorySwapMax=0", "TasksMax=256", "RuntimeMaxSec=180", "TimeoutStopSec=8",
        "KillMode=control-group", "Nice=19", "NoNewPrivileges=yes", "CapabilityBoundingSet=",
        "ProtectSystem=strict", "ProtectHome=yes", "PrivateTmp=yes", "PrivateDevices=yes",
        "PrivateIPC=yes", "ProtectKernelTunables=yes", "ProtectKernelModules=yes",
        "ProtectControlGroups=yes", f"RuntimeDirectory={UNIT}",
        f"Environment=HOME=/run/{UNIT}", f"Environment=TMPDIR=/run/{UNIT}", "UMask=0022",
        "InaccessiblePaths=-/run/stream-v3-control -/var/lib/rancher -/var/lib/stream-recovery-control "
        "-/run/k3s -/etc/stream-v3-fallback-controller -/run/credentials -/run/docker.sock "
        "-/run/containerd -/run/crio -/run/user",
        f"BindReadOnlyPaths={stage / 'browser'}:/opt/player-browser",
        f"BindReadOnlyPaths={stage / 'observation'}:/opt/player-observation",
        f"BindReadOnlyPaths={args.playwright_site}:/opt/player-python",
        f"BindReadOnlyPaths={args.pillow_site}:/opt/player-pillow",
        f"BindReadOnlyPaths={args.ocr}:/opt/player-ocr",
        "Environment=PYTHONPATH=/opt/player-python:/opt/player-pillow",
        "Environment=LD_LIBRARY_PATH=/opt/player-ocr/usr/lib/x86_64-linux-gnu",
        f"BindPaths={args.output / 'receiver'}:/evidence", "ReadWritePaths=/evidence",
    ]
    command = ["sudo", "-n", "systemd-run", "--quiet", "--wait", "--pipe", "--collect", f"--unit={UNIT}"]
    for prop in properties:
        command.extend(("-p", prop))
    return command + [
        "/usr/bin/python3", "-B", f"/opt/player-observation/{PROBE}",
        "--video-id", args.video_id, "--chromium", "/opt/player-browser/chrome",
        "--expected-browser-sha256", browser["sha256"],
        "--expected-browser-version", browser["version"], "--seconds", str(args.seconds),
        "--output", "/evidence/player", "--surface", "embed", "--verify-displayed-clock",
        "--confined-no-sandbox", "--tesseract", "/opt/player-ocr/usr/bin/tesseract",
        "--tessdata-dir", "/opt/player-ocr/usr/share/tesseract-ocr/5/tessdata",
    ]


def run(args):
    manifest = verify(args.artifact, args.artifact_sha256)
    profile = Path(__file__).resolve().parents[2] / "configs/releases/public-player-probe.json"
    allowed = {row["destination"] for row in json.loads(profile.read_text())["files"]}
    if set(manifest["files"]) != allowed or manifest["runtime_bindings"]:
        raise ValueError("expected the closed public-player artifact")
    target = runtime()
    browser = running_browser(target)
    if args.output is None:
        stamp = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y%m%d")
        args.output = Path(tempfile.mkdtemp(prefix=f"matched-player-{stamp}-", dir=args.output_root))
    else:
        args.output.mkdir(mode=0o700)
    print(f"Evidence: {args.output}", flush=True)
    evidence = args.output / "binding.json"
    binding = {"status": "UNKNOWN", "runtime": target, "browser": browser,
               "base_artifact_sha256": manifest["artifact_sha256"]}
    try:
        with tempfile.TemporaryDirectory(prefix="stage-", dir=args.output) as temp:
            stage = Path(temp)
            for name in allowed:
                destination = stage / "observation" / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                source = Path(__file__).with_name(Path(PROBE).name) if name == PROBE else args.artifact / name
                data = source.read_bytes()
                digest = hashlib.sha256(data).hexdigest()
                if name == PROBE:
                    binding["probe_sha256"] = digest
                elif digest != manifest["files"][name]["sha256"]:
                    raise ValueError("artifact changed while staging")
                destination.write_bytes(data)
            (stage / "browser").mkdir()
            binding["local_image_id"] = copy_browser(target["image_id"], browser, stage / "browser")
            if runtime() != target:
                raise ValueError("runtime changed before observation")
            read("sudo", "-n", "install", "-d", "-m", "0755", "-o", "nobody", "-g", "nogroup",
                 str(args.output / "receiver"))
            with (args.output / "unit.log").open("w") as log:
                result = subprocess.run(isolated_command(args, stage, browser), stdout=log, stderr=log)
            binding["observer_exit"] = result.returncode
        binding["runtime_after"] = runtime()
        if binding["runtime_after"] != target or running_browser(target) != browser:
            raise ValueError("runtime or browser changed during observation")
        if result.returncode == 0:
            binding["status"] = "PASS"
    finally:
        evidence.write_text(json.dumps(binding, indent=2) + "\n")
        print(f"Result: {binding['status']} ({evidence})", flush=True)
    return 0 if binding["status"] == "PASS" else 2


def config_values(path):
    values = {}
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or key not in CONFIG_KEYS or not value or key in values:
            raise ValueError(f"{path}:{number}: expected a unique PLAYER_* assignment")
        values[key] = value
    return values


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    default_config = Path.home() / ".config/stream-v3/matched-player.env"
    parser.add_argument("--env-file", type=Path, help=f"local settings (default: {default_config})")
    parser.add_argument("--video-id")
    parser.add_argument("--seconds", type=int, choices=range(65, 121), default=120, metavar="65..120")
    parser.add_argument("--artifact")
    parser.add_argument("--artifact-sha256")
    parser.add_argument("--playwright-site")
    parser.add_argument("--pillow-site")
    parser.add_argument("--ocr")
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--output", type=Path, help="new directory under a trusted parent")
    output.add_argument("--output-root", help="existing trusted parent for a unique run directory")
    args = parser.parse_args(argv)
    required = {key: field for key, field in CONFIG_KEYS.items()
                if field != "output_root" or args.output is None}
    missing = {key: field for key, field in required.items() if getattr(args, field) is None}
    if args.env_file is not None or missing:
        config = args.env_file or default_config
        try:
            values = config_values(config)
        except FileNotFoundError:
            if args.env_file is not None:
                parser.error(f"settings file not found: {config}")
            values = {}
        except (OSError, ValueError) as error:
            parser.error(str(error))
        for key, field in missing.items():
            if key not in values:
                parser.error(f"set {key} in {config} or supply --{field.replace('_', '-')}")
            setattr(args, field, values[key])
    if not re.fullmatch(r"[0-9a-f]{64}", args.artifact_sha256):
        parser.error("artifact SHA256 is required")
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", args.video_id):
        parser.error("invalid YouTube video ID")
    if args.output is not None:
        args.output = args.output.absolute()
    for field in ("artifact", "playwright_site", "pillow_site", "ocr", "output_root"):
        value = getattr(args, field)
        if value is not None:
            setattr(args, field, Path(value))
    for path in (args.output, args.output_root, args.playwright_site, args.pillow_site, args.ocr):
        if path is None:
            continue
        if not path.is_absolute() or re.search(r"[\s:\\%]", str(path)):
            parser.error("systemd paths must be absolute and free of whitespace and specifiers")
    if args.output_root is not None and not args.output_root.is_dir():
        parser.error("output root must be an existing directory")
    return args


def main():
    args = parse_args()
    try:
        return run(args)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(error, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
