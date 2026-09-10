#!/usr/bin/env python3
"""Build and push the public static tree to GCS."""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SITE_DIR = Path(__file__).resolve().parents[1]
PUBLIC_DIR = SITE_DIR / "public"
DEST = os.environ.get("YUKIMURATA_GCS_DEST", "").strip()
JSON_CACHE_CONTROL = os.environ.get("YUKIMURATA_JSON_CACHE_CONTROL", "public, max-age=60")
STATIC_CACHE_CONTROL = os.environ.get("YUKIMURATA_STATIC_CACHE_CONTROL", "public, max-age=300")
ALLOWED_FILES = (
    "index.html",
    "stream-v3-prometheus.html",
    "stream-v3-loki.html",
    "stream-v3-prometheus.json",
    "stream-v3-loki.json",
    "reliability-indicators.json",
    "assets/site.css",
    "assets/site.js",
)
REQUIRED_JSON = ("stream-v3-prometheus.json", "stream-v3-loki.json", "reliability-indicators.json")
MAXIMUM_FILE_BYTES = 8 * 1024 * 1024
MAXIMUM_SOURCE_AGE_SECONDS = 180


def run(args: list[str], *, deadline: float, stdout: int | None = None) -> None:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("PUBLICATION_DEADLINE_EXCEEDED")
    # The builder/gcloud workers belong to this invocation's own process
    # group. A timeout must not leave them uploading after the lock is freed.
    with subprocess.Popen(args, text=True, stdout=stdout, start_new_session=True) as process:
        try:
            process.communicate(timeout=remaining)
        except subprocess.TimeoutExpired as exc:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.communicate(timeout=5)
            raise TimeoutError("PUBLICATION_DEADLINE_EXCEEDED") from exc
        if process.returncode:
            raise subprocess.CalledProcessError(process.returncode, args)


def ensure_gcloud() -> None:
    if shutil.which("gcloud") is None:
        raise FileNotFoundError("gcloud command not found")


def ensure_destination() -> None:
    if re.fullmatch(r"gs://[a-z0-9][a-z0-9._-]+/[A-Za-z0-9/_-]+", DEST) is None or any(
        part in {"", ".", ".."} for part in DEST[5:].split("/")
    ):
        raise RuntimeError("YUKIMURATA_GCS_DEST must name an explicit bucket and prefix without wildcards")


def _object(raw: bytes) -> dict[str, Any]:
    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("PUBLIC_JSON_DUPLICATE_KEY")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise ValueError("PUBLIC_JSON_NONFINITE_NUMBER")

    value = json.loads(raw, object_pairs_hook=unique, parse_constant=reject_constant)
    if not isinstance(value, dict):
        raise ValueError("PUBLIC_JSON_NOT_OBJECT")
    return value


def stage_public_tree(source: Path, destination: Path, *, now: float) -> dict[str, str]:
    """Freeze only explicitly public files. Never follow links or copy backups."""
    hashes: dict[str, str] = {}
    for name in ALLOWED_FILES:
        path = source / name
        if not path.exists() and name not in REQUIRED_JSON and name != "index.html":
            continue
        if any(parent.is_symlink() for parent in (source, *(source / p for p in path.relative_to(source).parents[:-1]))):
            raise ValueError("PUBLIC_SOURCE_SYMLINK")
        if not path.resolve().is_relative_to(source.resolve()):
            raise ValueError("PUBLIC_SOURCE_ESCAPES_ROOT")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= MAXIMUM_FILE_BYTES:
                raise ValueError("PUBLIC_SOURCE_NOT_BOUNDED_REGULAR_FILE")
            raw = stream.read(MAXIMUM_FILE_BYTES + 1)
        if len(raw) > MAXIMUM_FILE_BYTES:
            raise ValueError("PUBLIC_SOURCE_TOO_LARGE")
        if name in REQUIRED_JSON:
            value = _object(raw)
            generated = value.get("generated_at")
            if (
                isinstance(generated, bool)
                or not isinstance(generated, (float, int))
                or not 0 <= generated <= now
                or not math.isfinite(generated)
                or not 0 <= now - generated <= MAXIMUM_SOURCE_AGE_SECONDS
            ):
                raise ValueError("PUBLIC_SOURCE_STALE_OR_FUTURE")
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        hashes[name] = hashlib.sha256(raw).hexdigest()
    return hashes


def write_status(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, separators=(",", ":"), sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def read_status(path: Path) -> dict[str, Any]:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return {}
    with os.fdopen(descriptor, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 64 * 1024:
            raise ValueError("PUBLICATION_STATE_INVALID")
        value = _object(stream.read(64 * 1024 + 1))
    failures = value.get("failure_count")
    if type(failures) is not int or failures < 0:
        raise ValueError("PUBLICATION_FAILURE_COUNTER_INVALID")
    return value


def main() -> int:
    ensure_destination()
    ensure_gcloud()
    budget = float(os.environ.get("YUKIMURATA_PUBLISH_DEADLINE_SECONDS", "240"))
    if not math.isfinite(budget) or not 1 <= budget <= 300:
        raise ValueError("PUBLICATION_DEADLINE_INVALID")
    state_path = Path(os.environ.get("YUKIMURATA_PUBLISH_STATUS_FILE", str(SITE_DIR / ".state" / "public-push.json")))
    if not state_path.is_absolute() or state_path.resolve().is_relative_to(PUBLIC_DIR.resolve()):
        raise ValueError("PUBLICATION_STATUS_MUST_BE_PRIVATE")
    state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = os.open(state_path.with_suffix(".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        prior = read_status(state_path)
        status: dict[str, Any] = {
            "schema": "stream_v3.publication_status.v1",
            "status": "RUNNING",
            "started_at": datetime.now(UTC).isoformat(),
            "completed_at": None,
            "failure_count": int(prior.get("failure_count", 0)),
            "last_failure_at": prior.get("last_failure_at"),
            "public_mirror_verified": False,
            "control_capability_count": 0,
        }
        write_status(state_path, status)
        deadline = time.monotonic() + budget
        try:
            run([sys.executable, str(SITE_DIR / "scripts" / "build_public.py")], deadline=deadline)
            with tempfile.TemporaryDirectory(prefix="public-push-", dir=state_path.parent) as temporary:
                staged = Path(temporary)
                hashes = stage_public_tree(PUBLIC_DIR, staged, now=time.time())
                # Set cache metadata on upload rather than a second wildcard
                # pass over all remote objects. No deletion of remote history.
                run(
                    [
                        "gcloud",
                        "storage",
                        "rsync",
                        str(staged),
                        DEST,
                        "--recursive",
                        r"--exclude=.*\.json$",
                        f"--cache-control={STATIC_CACHE_CONTROL}",
                    ],
                    deadline=deadline,
                )
                run(
                    [
                        "gcloud",
                        "storage",
                        "rsync",
                        str(staged),
                        DEST,
                        "--recursive",
                        r"--exclude=^(?!.*\.json$).*",
                        f"--cache-control={JSON_CACHE_CONTROL}",
                    ],
                    deadline=deadline,
                )
                status["artifact_sha256"] = hashes
            status.update(status="SUCCEEDED", completed_at=datetime.now(UTC).isoformat())
            write_status(state_path, status)
        except (OSError, ValueError, subprocess.SubprocessError):
            failed_at = datetime.now(UTC).isoformat()
            status.update(
                status="FAILED", completed_at=failed_at, last_failure_at=failed_at, failure_count=int(status["failure_count"]) + 1
            )
            write_status(state_path, status)
            raise
        return 0
    finally:
        os.close(lock)


if __name__ == "__main__":
    raise SystemExit(main())
