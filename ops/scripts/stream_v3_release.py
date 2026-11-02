#!/usr/bin/env python3
"""Build bounded releases from committed blobs, verify them, and switch a pointer.

No service management, network, credentials, or durable runtime state is owned here.
The caller must separately verify loaded entrypoints and natural consumer execution.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import tempfile

MANIFEST = "release.json"
MAX_FILE = 2 * 1024 * 1024
MAX_TOTAL = 64 * 1024 * 1024
MAX_FILES = 4096


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def relative(value):
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError("invalid relative path")
    p = PurePosixPath(value)
    if p.is_absolute() or any(x in ("", ".", "..") for x in value.split("/")):
        raise ValueError("unsafe relative path")
    if value == MANIFEST or "__pycache__" in p.parts:
        raise ValueError("reserved path")
    return value


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], timeout=30)


def artifact_id(manifest):
    return digest(canonical({k: v for k, v in manifest.items() if k != "artifact_sha256"}))


def build(repo, commit, profile_path, output):
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("full commit SHA required")
    if git(repo, "cat-file", "-t", commit).strip() != b"commit":
        raise ValueError("commit required")
    tree = {}
    for row in git(repo, "ls-tree", "-rz", commit).split(b"\0"):
        if row:
            meta, name = row.split(b"\t", 1)
            mode, kind, oid = meta.decode().split()
            tree[name.decode()] = (mode, kind, oid)

    def blob(name):
        relative(name)
        mode, kind, oid = tree[name]
        if kind != "blob" or mode not in ("100644", "100755"):
            raise ValueError("only regular committed files are allowed")
        size = int(git(repo, "cat-file", "-s", oid))
        if size > MAX_FILE:
            raise ValueError("file size limit")
        return git(repo, "cat-file", "blob", oid), mode, oid

    raw_profile, _, profile_oid = blob(profile_path)
    profile = json.loads(raw_profile)
    if profile.get("schema") != "stream.release_profile.v1":
        raise ValueError("profile schema")
    entries = profile.get("files", [])
    bindings = profile.get("runtime_bindings", [])
    if not 0 < len(entries) <= MAX_FILES or len(bindings) > 32:
        raise ValueError("profile size")
    names = []
    for item in entries + bindings:
        name = relative(item["destination"])
        if any(name == p or name.startswith(p + "/") or p.startswith(name + "/") for p in names):
            raise ValueError("overlapping destinations")
        names.append(name)
    keys = [b["key"] for b in bindings]
    if len(set(keys)) != len(keys) or any(not re.fullmatch(r"[a-zA-Z0-9_-]+", k) for k in keys):
        raise ValueError("invalid binding keys")
    output = Path(output).absolute()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() or output.is_symlink():
        raise ValueError("output already exists")
    staging = Path(tempfile.mkdtemp(prefix=".release-build-", dir=output.parent))
    try:
        manifest = {"schema": "stream.release.v1", "commit": commit,
                    "profile": profile_path, "profile_blob": profile_oid,
                    "profile_sha256": digest(raw_profile), "name": profile["name"],
                    "files": {}, "runtime_bindings": bindings}
        total = 0
        for item in entries:
            data, mode, oid = blob(item["source"])
            total += len(data)
            if total > MAX_TOTAL:
                raise ValueError("release size limit")
            target = staging / item["destination"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            file_mode = 0o555 if mode == "100755" else 0o444
            target.chmod(file_mode)
            manifest["files"][item["destination"]] = {
                "source": item["source"], "blob": oid, "sha256": digest(data), "mode": file_mode}
        manifest["artifact_sha256"] = artifact_id(manifest)
        (staging / MANIFEST).write_bytes(canonical(manifest) + b"\n")
        (staging / MANIFEST).chmod(0o444)
        os.rename(staging, output)
        return manifest
    except BaseException:
        shutil.rmtree(staging)
        raise


def verify(root, expected=None, require_bindings=False):
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("physical release directory required")
    manifest_path = root / MANIFEST
    if manifest_path.is_symlink() or manifest_path.stat().st_size > MAX_FILE:
        raise ValueError("invalid manifest")
    m = json.loads(manifest_path.read_bytes())
    if m.get("schema") != "stream.release.v1" or artifact_id(m) != m.get("artifact_sha256"):
        raise ValueError("manifest digest mismatch")
    if expected is not None and m["artifact_sha256"] != expected:
        raise ValueError("unexpected artifact")
    if not 0 < len(m["files"]) <= MAX_FILES:
        raise ValueError("manifest size")
    bindings = {relative(x["destination"]) for x in m["runtime_bindings"]}
    allowed_dirs = set()
    for name in list(m["files"]) + list(bindings):
        relative(name)
        allowed_dirs.update(str(x) for x in PurePosixPath(name).parents if str(x) != ".")
    seen = set()
    total = 0
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in list(dirs) + files:
            p = Path(directory) / name
            rel = p.relative_to(root).as_posix()
            if rel in bindings:
                if not p.is_symlink() or not p.exists():
                    raise ValueError("invalid runtime binding")
                seen.add(rel)
                if name in dirs:
                    dirs.remove(name)
                continue
            if p.is_symlink():
                raise ValueError("unexpected symlink")
            if p.is_dir():
                if rel not in allowed_dirs:
                    # CPython cache files are runtime products, never release sources.
                    if name == "__pycache__" and str(PurePosixPath(rel).parent) in allowed_dirs:
                        if any(x.is_symlink() or not x.is_file() or x.suffix != ".pyc" for x in p.iterdir()):
                            raise ValueError("unexpected cache content")
                        dirs.remove(name)
                        continue
                    raise ValueError("unexpected directory")
                continue
            if not stat.S_ISREG(p.stat().st_mode):
                raise ValueError("non-regular artifact")
            if rel == MANIFEST:
                continue
            entry = m["files"].get(rel)
            if entry is None or p.stat().st_size > MAX_FILE:
                raise ValueError("unexpected or oversized file")
            data = p.read_bytes()
            total += len(data)
            if total > MAX_TOTAL or digest(data) != entry["sha256"] or stat.S_IMODE(p.stat().st_mode) != entry["mode"]:
                raise ValueError("source or mode mismatch")
            seen.add(rel)
    if not set(m["files"]).issubset(seen) or (require_bindings and not bindings.issubset(seen)):
        raise ValueError("missing artifact files or bindings")
    return m


def bind(root, targets, expected):
    m = verify(root, expected)
    targets = dict(targets)
    if set(targets) != {b["key"] for b in m["runtime_bindings"]}:
        raise ValueError("exact runtime binding keys required")
    for b in m["runtime_bindings"]:
        target = Path(targets[b["key"]])
        if not target.is_absolute() or not target.exists():
            raise ValueError("existing absolute binding target required")
        p = Path(root) / b["destination"]
        if p.is_symlink() and os.readlink(p) == str(target):
            continue
        if p.exists() or p.is_symlink():
            raise ValueError("refusing binding replacement")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.symlink_to(target)
    return verify(root, expected, require_bindings=True)


def activate(root, pointer, expected_current, expected_artifact):
    root = Path(root).absolute()
    pointer = Path(pointer).absolute()
    with (pointer.parent / ("." + pointer.name + ".release.lock")).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        m = verify(root, expected_artifact, require_bindings=True)
        if not pointer.is_symlink() or str(pointer.resolve()) != str(Path(expected_current).absolute()):
            raise ValueError("current release changed")
        tmp = pointer.with_name("." + pointer.name + "." + str(os.getpid()))
        try:
            tmp.symlink_to(root)
            os.replace(tmp, pointer)
            fd = os.open(pointer.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            tmp.unlink(missing_ok=True)
        return {"schema": "stream.release_activation.v1", "previous": expected_current,
                "current": str(root), "commit": m["commit"], "artifact_sha256": m["artifact_sha256"],
                "service_restarted": False, "natural_execution_verified": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("build")
    p.add_argument("--repo", type=Path, required=True)
    p.add_argument("--commit", required=True)
    p.add_argument("--profile", required=True)
    p.add_argument("--output", type=Path, required=True)
    for command in ("verify", "bind", "activate"):
        p = sub.add_parser(command)
        p.add_argument("--root", type=Path, required=True)
        p.add_argument("--expect-artifact", required=True)
        if command == "verify":
            p.add_argument("--require-bindings", action="store_true")
        elif command == "bind":
            p.add_argument("--binding", action="append", default=[])
        else:
            p.add_argument("--pointer", type=Path, required=True)
            p.add_argument("--expect-current", required=True)
    a = parser.parse_args()
    if a.command == "build":
        result = build(a.repo, a.commit, a.profile, a.output)
    elif a.command == "verify":
        result = verify(a.root, a.expect_artifact, a.require_bindings)
    elif a.command == "bind":
        result = bind(a.root, [x.split("=", 1) for x in a.binding], a.expect_artifact)
    else:
        result = activate(a.root, a.pointer, a.expect_current, a.expect_artifact)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
