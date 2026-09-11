#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
import re
import secrets
import shlex
import stat
import subprocess
from pathlib import Path
from typing import Any


NAMESPACE = "stream-monitoring-v4"
SECRET_NAME = "youtube-api-oauth"
DEFAULT_SOURCE = Path("/etc/default/adsb-streamnew-youtube-monitor")
SOURCE_KEYS = {
    "YTW_OAUTH_CLIENT_ID": "client-id",
    "YTW_OAUTH_CLIENT_SECRET": "client-secret",
    "YTW_OAUTH_REFRESH_TOKEN": "refresh-token",
}
SECRET_KEYS = frozenset(SOURCE_KEYS.values())
MAX_SOURCE_BYTES = 1024 * 1024
_ENV_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Create the isolated read-only YouTube API OAuth Secret without "
            "printing or replacing credential values"
        )
    )
    result.add_argument("--kubectl", default="/usr/local/bin/k3s")
    result.add_argument("--source-env", type=Path, default=DEFAULT_SOURCE)
    result.add_argument("--source-owner-uid", type=int, default=1000)
    return result


def _read_stable_private_file(path: Path, *, allowed_owner_uid: int) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise RuntimeError("OAuth source env is unavailable or is a symlink") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeError("OAuth source env must be a regular file")
        if before.st_nlink != 1:
            raise RuntimeError("OAuth source env must have exactly one hard link")
        if before.st_uid not in {0, allowed_owner_uid}:
            raise RuntimeError("OAuth source env owner is not allowlisted")
        if stat.S_IMODE(before.st_mode) & 0o077:
            raise RuntimeError("OAuth source env must not be group/world accessible")
        if before.st_size < 0 or before.st_size > MAX_SOURCE_BYTES:
            raise RuntimeError("OAuth source env size is outside the allowed bound")
        chunks: list[bytes] = []
        remaining = MAX_SOURCE_BYTES + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
            before.st_mode,
            before.st_uid,
            before.st_gid,
            before.st_nlink,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
            after.st_mode,
            after.st_uid,
            after.st_gid,
            after.st_nlink,
        )
        if len(raw) > MAX_SOURCE_BYTES or len(raw) != before.st_size:
            raise RuntimeError("OAuth source env changed or exceeded its size bound")
        if identity_before != identity_after:
            raise RuntimeError("OAuth source env changed while it was read")
    finally:
        os.close(descriptor)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError("OAuth source env is not UTF-8") from exc


def _decode_env_value(raw: str, *, key: str) -> str:
    if raw != raw.strip():
        raise RuntimeError(
            f"OAuth source value contains whitespace padding or a control character: {key}"
        )
    try:
        values = shlex.split(raw, comments=False, posix=True)
    except ValueError as exc:
        raise RuntimeError(f"OAuth source value has invalid quoting: {key}") from exc
    if len(values) > 1:
        raise RuntimeError(f"OAuth source value contains unsupported whitespace: {key}")
    value = values[0] if values else ""
    if value != value.strip() or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        raise RuntimeError(
            f"OAuth source value contains whitespace padding or a control character: {key}"
        )
    return value


def _credentials(text: str) -> dict[str, str]:
    selected: dict[str, str] = {}
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        key = key.strip()
        if not _ENV_KEY.fullmatch(key) or key not in SOURCE_KEYS:
            continue
        if key in selected:
            raise RuntimeError(f"OAuth source env contains duplicate key: {key}")
        selected[key] = _decode_env_value(raw_value, key=key)
    missing = sorted(key for key in SOURCE_KEYS if key not in selected)
    if missing:
        raise RuntimeError(f"OAuth source env is missing required keys: {missing}")
    if not selected["YTW_OAUTH_CLIENT_ID"]:
        raise RuntimeError("OAuth client ID is empty")
    if not selected["YTW_OAUTH_REFRESH_TOKEN"]:
        raise RuntimeError("OAuth refresh token is empty")
    limits = {
        "YTW_OAUTH_CLIENT_ID": 4096,
        "YTW_OAUTH_CLIENT_SECRET": 4096,
        "YTW_OAUTH_REFRESH_TOKEN": 8192,
    }
    for key, limit in limits.items():
        if len(selected[key]) > limit:
            raise RuntimeError(f"OAuth source value is too large: {key}")
    return {target: selected[source] for source, target in SOURCE_KEYS.items()}


def _encoded(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def _existing(kubectl: str) -> dict[str, Any] | None:
    result = subprocess.run(
        [
            kubectl,
            "kubectl",
            "-n",
            NAMESPACE,
            "get",
            "secret",
            SECRET_NAME,
            "-o",
            "json",
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).lower()
        if "notfound" in detail or "not found" in detail:
            return None
        raise RuntimeError("failed to read the YouTube API Secret")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("existing YouTube API Secret is not valid JSON") from exc
    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    data = payload.get("data") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or payload.get("kind") != "Secret"
        or payload.get("type") != "Opaque"
        or payload.get("immutable") is not True
        or not isinstance(metadata, dict)
        or metadata.get("name") != SECRET_NAME
        or metadata.get("namespace") != NAMESPACE
        or not isinstance(metadata.get("resourceVersion"), str)
        or not metadata["resourceVersion"]
        or not isinstance(data, dict)
        or set(data) != SECRET_KEYS
        or not all(isinstance(value, str) for value in data.values())
    ):
        raise RuntimeError("existing YouTube API Secret has an invalid exact structure")
    return payload


def _matches(payload: dict[str, Any], credentials: dict[str, str]) -> bool:
    data = payload["data"]
    for key in sorted(SECRET_KEYS):
        try:
            decoded = base64.b64decode(data[key], validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            return False
        if not secrets.compare_digest(decoded, credentials[key]):
            return False
    return True


def _secret(credentials: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": SECRET_NAME, "namespace": NAMESPACE},
        "type": "Opaque",
        "immutable": True,
        "data": {key: _encoded(credentials[key]) for key in sorted(SECRET_KEYS)},
    }


def _create(kubectl: str, payload: dict[str, Any]) -> bool:
    result = subprocess.run(
        [kubectl, "kubectl", "create", "-f", "-"],
        input=json.dumps(payload, separators=(",", ":")),
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=15,
        check=False,
    )
    if result.returncode == 0:
        return True
    detail = result.stderr.lower()
    if "alreadyexists" in detail or "already exists" in detail:
        return False
    raise RuntimeError("failed to create the YouTube API Secret")


def ensure_secret(kubectl: str, credentials: dict[str, str]) -> str:
    current = _existing(kubectl)
    if current is not None:
        if not _matches(current, credentials):
            raise RuntimeError(
                "existing YouTube API Secret differs; explicit credential rotation is required"
            )
        return "unchanged"
    if _create(kubectl, _secret(credentials)):
        return "created"
    raced = _existing(kubectl)
    if raced is None or not _matches(raced, credentials):
        raise RuntimeError(
            "YouTube API Secret create race produced a different or missing value"
        )
    return "unchanged_after_create_race"


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.source_owner_uid < 0:
        raise ValueError("--source-owner-uid must not be negative")
    source = _read_stable_private_file(
        args.source_env,
        allowed_owner_uid=args.source_owner_uid,
    )
    credentials = _credentials(source)
    result = ensure_secret(args.kubectl, credentials)
    print(
        json.dumps(
            {
                "secret": SECRET_NAME,
                "status": result,
                "exact_keys": sorted(SECRET_KEYS),
                "values_printed": False,
                "source_token_url_copied": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
