#!/usr/bin/env python3
"""Publish the approved public snapshot to a private R2 bucket."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import re
import shlex
import signal
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import suppress
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable
from publication_receipts import Receipts, LIVE_FILES, OBSERVATION_FILES, failure, health_observation
from r2_network import KeepAliveHTTPS

SITE_DIR = Path(__file__).resolve().parents[1]
MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_TOTAL_BYTES = 16 * 1024 * 1024
MAX_SOURCE_AGE = 180
PREFIX = os.environ.get("YUKIMURATA_R2_PREFIX", "public/")
API_BASE = "https://api.cloudflare.com/client/v4"


class PublicationError(RuntimeError):
    pass


class ProviderError(PublicationError):
    def __init__(self, status: int):
        self.status = status
        super().__init__(f"R2_HTTP_{status}")


def validate_prefix():
    if not re.fullmatch(r"(?:[A-Za-z0-9][A-Za-z0-9_-]*/)+", PREFIX):
        raise PublicationError("INVALID_PUBLIC_PREFIX")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        raise PublicationError("R2_REDIRECT_REFUSED")


@dataclass(frozen=True)
class Credentials:
    account_id: str
    token: str = field(repr=False)


def strict_object(raw: bytes) -> dict[str, Any]:
    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise PublicationError("DUPLICATE_JSON_KEY")
            result[key] = value
        return result

    def reject(value: str) -> None:
        raise PublicationError("NONFINITE_JSON_VALUE")

    value = json.loads(raw, object_pairs_hook=unique, parse_constant=reject)
    if not isinstance(value, dict):
        raise PublicationError("JSON_OBJECT_REQUIRED")
    return value


def read_regular(path: Path, maximum: int) -> bytes:
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise PublicationError("SOURCE_SYMLINK")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= maximum:
            raise PublicationError("SOURCE_NOT_BOUNDED_REGULAR_FILE")
        raw = stream.read(maximum + 1)
        after = os.fstat(stream.fileno())
    if len(raw) != before.st_size or (before.st_mtime_ns, before.st_size) != (
        after.st_mtime_ns, after.st_size
    ):
        raise PublicationError("SOURCE_CHANGED_DURING_READ")
    return raw


def freeze_public_tree(
    public_dir: Path, manifest: dict[str, Any], *, now: float
) -> dict[str, bytes]:
    names = manifest.get("files")
    required = manifest.get("required")
    if (
        not isinstance(names, list) or not isinstance(required, list)
        or not 1 <= len(names) <= 100 or not all(isinstance(n, str) for n in names)
        or len(names) != len(set(names)) or not set(required) <= set(names)
    ):
        raise PublicationError("INVALID_PUBLIC_MANIFEST")
    frozen: dict[str, bytes] = {}
    total = 0
    for name in names:
        relative = PurePosixPath(name)
        if (
            relative.is_absolute() or str(relative) != name or "\\" in name
            or any(part.startswith(".") for part in relative.parts)
            or relative.suffix not in {".html", ".css", ".js", ".json"}
        ):
            raise PublicationError("INVALID_PUBLIC_FILE_NAME")
        path = public_dir / name
        if not path.exists() and not path.is_symlink() and name not in required:
            continue
        raw = read_regular(path, MAX_FILE_BYTES)
        total += len(raw)
        if total > MAX_TOTAL_BYTES:
            raise PublicationError("PUBLIC_TREE_TOO_LARGE")
        if name.endswith(".json"):
            value = strict_object(raw)
            if name in required:
                generated = value.get("generated_at")
                if (
                    type(generated) not in {int, float}
                    or not math.isfinite(generated)
                    or not 0 <= now - generated <= MAX_SOURCE_AGE
                ):
                    raise PublicationError("PUBLIC_SOURCE_STALE_OR_FUTURE")
        frozen[name] = raw
    return frozen

RELEASE_FILE = "site-release.json"
REVISION_META = re.compile(r'  <meta name="site-revision" content="[a-f0-9]{64}" />\n')
VERSIONED_ASSET = re.compile(r'((?:src|href)="\./)(assets/[a-zA-Z0-9_.-]+\.(?:js|css))(?:\?v=[a-f0-9]{64})?(")')


def prepare_site_release(frozen: dict[str, bytes]) -> dict[str, bytes]:
    """Bind the page and static assets without including changing observations."""
    html = REVISION_META.sub("", frozen["index.html"].decode("utf-8"))
    html = VERSIONED_ASSET.sub(lambda match: "".join(match.groups()), html)
    if html.count("</head>") != 1 or 'name="site-revision"' in html:
        raise PublicationError("INVALID_SITE_REVISION_MARKER")
    static = {name: raw for name, raw in frozen.items() if name.endswith((".js", ".css"))}
    if not static:
        raise PublicationError("SITE_RELEASE_HAS_NO_SCRIPT")
    for match in VERSIONED_ASSET.finditer(html):
        if match[2] not in static:
            raise PublicationError("SITE_RELEASE_ASSET_MISSING")
    canonical = {name: hashlib.sha256(raw).hexdigest() for name, raw in static.items()}
    canonical["index.html"] = hashlib.sha256(html.encode()).hexdigest()
    revision = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    html = VERSIONED_ASSET.sub(lambda match: f'{match[1]}{match[2]}?v={revision}{match[3]}', html)
    html = html.replace("</head>", f'  <meta name="site-revision" content="{revision}" />\n</head>')
    result = {**frozen, "index.html": html.encode()}
    files = {name: hashlib.sha256(raw).hexdigest() for name, raw in static.items()}
    files["index.html"] = hashlib.sha256(result["index.html"]).hexdigest()
    result[RELEASE_FILE] = (json.dumps({"schema": "public.site_release.v1", "revision": revision,
        "files": files}, sort_keys=True, separators=(",", ":")) + "\n").encode()
    return result


def write_public_release(public: Path, frozen: dict[str, bytes]) -> None:
    for name in ("index.html", RELEASE_FILE):
        path = public / name
        raw = frozen[name]
        if path.exists() and read_regular(path, MAX_FILE_BYTES) == raw:
            continue
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o644)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def load_credentials(path: Path, public_dir: Path) -> Credentials:
    if path.resolve().is_relative_to(public_dir.resolve()):
        raise PublicationError("CREDENTIALS_INSIDE_PUBLIC_TREE")
    metadata = path.lstat()
    if (
        stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or metadata.st_uid not in {0, os.getuid()}
    ):
        raise PublicationError("CREDENTIAL_FILE_PERMISSIONS")
    values: dict[str, str] = {}
    for line in read_regular(path, 16384).decode().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:]
        key, separator, value = line.partition("=")
        if separator:
            parsed = shlex.split(value, comments=True)
            if len(parsed) != 1:
                raise PublicationError("INVALID_CREDENTIAL_FILE")
            values[key.strip()] = parsed[0]
    account = values.get("CLOUDFLARE_ACCOUNT_ID", "")
    token = values.get("CLOUDFLARE_API_TOKEN", "")
    if not re.fullmatch(r"[a-f0-9]{32}", account) or not token or re.search(r"\s", token):
        raise PublicationError("INVALID_CREDENTIAL_FILE")
    return Credentials(account, token)


class R2Client:
    def __init__(
        self, credentials: Credentials, bucket: str, *, deadline: float,
        opener: Callable[..., Any] | None = None,
    ):
        validate_prefix()
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", bucket):
            raise PublicationError("INVALID_BUCKET_NAME")
        self.credentials = credentials
        self.bucket = bucket
        self.deadline = deadline
        self.transport = KeepAliveHTTPS() if opener is None else None
        self.opener = opener or self.transport.open

    def network_stats(self):
        return self.transport.snapshot() if self.transport else {}

    def close(self):
        if self.transport:
            self.transport.close()

    def request(self, method: str, key: str, body: bytes | None = None) -> bytes:
        if not key.startswith(PREFIX) or any(p.startswith(".") for p in key.split("/")):
            raise PublicationError("OBJECT_OUTSIDE_SITE_PREFIX")
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise PublicationError("PUBLICATION_DEADLINE_EXCEEDED")
        url = (
            f"{API_BASE}/accounts/{self.credentials.account_id}/r2/buckets/{self.bucket}"
            f"/objects/{urllib.parse.quote(key, safe='/')}"
        )
        headers = {"Authorization": f"Bearer {self.credentials.token}"}
        if body is not None:
            headers.update({"Content-Type": "application/octet-stream", "cf-r2-storage-class": "Standard"})
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        started = time.monotonic()
        try:
            with self.opener(request, timeout=min(15, remaining)) as response:
                limit = MAX_FILE_BYTES if method == "GET" else 65536
                raw = response.read(limit + 1)
                if len(raw) > limit:
                    raise PublicationError("R2_RESPONSE_TOO_LARGE")
                return raw
        except urllib.error.HTTPError as exc:
            status = exc.code
            exc.close()
            error = ProviderError(status); error.elapsed_seconds = time.monotonic()-started
            raise error from None
        except (OSError, ValueError, PublicationError) as exc:
            exc.elapsed_seconds = time.monotonic()-started
            raise

    def get(self, name: str) -> bytes | None:
        try:
            return self.request("GET", PREFIX + name)
        except ProviderError as exc:
            if exc.status == 404:
                return None
            raise

    def put(self, name: str, raw: bytes) -> None:
        result = strict_object(self.request("PUT", PREFIX + name, raw))
        if result.get("success") is not True:
            raise PublicationError("R2_UPLOAD_NOT_ACKNOWLEDGED")
        metadata = result.get("result") or {}
        if metadata.get("key", PREFIX + name) != PREFIX + name:
            raise PublicationError("R2_UPLOAD_KEY_MISMATCH")


def publish(
    client: R2Client, frozen: dict[str, bytes], prior_hashes: dict[str, str], *, workers: int = 2,
    receipts=None, continue_on_error=False,
) -> dict[str, Any]:
    if type(workers) is not int or not 1 <= workers <= 2:
        raise PublicationError("INVALID_PUBLICATION_CONCURRENCY")

    def describe(exc, *, stage, name):
        info = failure(exc, stage=stage, name=name)
        if name in OBSERVATION_FILES and name in frozen:
            try:
                stamp = source_timestamp(strict_object(frozen[name]).get('generated_at'))
                info.update(source_generated_at=stamp, source_age_seconds=round(time.time()-stamp, 3))
            except (OSError, ValueError, PublicationError):
                pass
        return info

    def read(name, stage):
        for attempt in range(3):
            try: return client.get(name)
            except (OSError, PublicationError) as exc:
                if receipts: receipts.event({**describe(exc, stage=stage, name=name), "method": "GET", "attempt": attempt+1})
                transient = isinstance(exc, OSError) or isinstance(exc, ProviderError) and exc.status in {408, 429, 500, 502, 503, 504}
                if not receipts or not transient or attempt == 2: raise
                delay = (.25, .75)[attempt]
                if time.monotonic()+delay+1 >= getattr(client, 'deadline', 0): raise
                time.sleep(delay)

    def verified(name, digest):
        if receipts: receipts.set(name, "VERIFIED", digest)

    def publish_one(item):
        name, raw = item
        digest = hashlib.sha256(raw).hexdigest()
        row = receipts.get(name) if receipts else {}
        if row.get("state") == "OUTCOME_UNKNOWN":
            observed = read(name, "resolve_unknown")
            if observed is None or hashlib.sha256(observed).hexdigest() != row["sha256"]:
                raise PublicationError("R2_UPLOAD_OUTCOME_UNCONFIRMED")
            verified(name, row["sha256"])
            row = receipts.get(name)
        known = row.get("sha256") if receipts and row.get("state") == "VERIFIED" else prior_hashes.get(name) if not receipts else None
        if known == digest:
            return name, digest, "unchanged"
        # A prior verified object whose content changed can be replaced directly.
        # With no successful prior record, reconcile before considering a PUT.
        if known is None and read(name, "initial_reconcile") == raw:
            verified(name, digest)
            return name, digest, "reconciled"
        if receipts: receipts.set(name, "OUTCOME_UNKNOWN", digest)
        try:
            client.put(name, raw)
        except (OSError, ValueError, PublicationError) as exc:
            detail = describe(exc, stage="put", name=name)
            if receipts and (detail['code'] == 'DNS_ERROR' or detail.get('request_not_sent') is True):
                # DNS, or a transport-proven failure before HTTP headers/body.
                # A new cycle may safely try its new snapshot; no PUT is retried here.
                receipts.set(name, 'NOT_SENT', digest)
                receipts.event({**detail, 'method':'PUT', 'outcome':'NOT_SENT', 'attempt_sha256':digest})
                raise PublicationError('R2_REQUEST_NOT_SENT') from None
            if receipts: receipts.event({**detail, "method": "PUT", "outcome": "OUTCOME_UNKNOWN", "attempt_sha256":digest})
            # Reconcile once; never repeat an uncertain PUT in this cycle.
            if read(name, "put_reconcile") != raw:
                raise PublicationError("R2_UPLOAD_OUTCOME_UNCONFIRMED") from None
            verified(name, digest)
            return name, digest, "reconciled"
        else:
            if read(name, "readback") != raw:
                raise PublicationError("R2_UPLOAD_VERIFICATION_FAILED")
            verified(name, digest)
            return name, digest, "uploaded"

    hashes: dict[str, str] = {}
    counts = {"uploaded": 0, "reconciled": 0, "unchanged": 0}
    errors = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="public-r2") as executor:
        items = [item for item in frozen.items() if item[0] != RELEASE_FILE]
        futures = [executor.submit(publish_one, item) for item in items]
        try:
            for (name, _), future in zip(items, futures):
                try:
                    name, digest, outcome = future.result()
                except (OSError, ValueError, PublicationError) as exc:
                    info = describe(exc, stage="object", name=name); errors.append(info)
                    if receipts: receipts.event(info)
                    if not continue_on_error: raise
                    continue
                hashes[name] = digest
                counts[outcome] += 1
        except BaseException:
            # Do not leave background uploads alive after releasing the publication lock.
            for future in futures:
                future.cancel()
            raise
    # Announce a release only after every preceding object has been acknowledged and read back.
    if RELEASE_FILE in frozen and not errors:
        try:
            name, digest, outcome = publish_one((RELEASE_FILE, frozen[RELEASE_FILE]))
            hashes[name] = digest; counts[outcome] += 1
        except (OSError, ValueError, PublicationError) as exc:
            if not continue_on_error: raise
            info = describe(exc, stage="release", name=RELEASE_FILE); errors.append(info)
            if receipts: receipts.event(info)
    return {"artifact_sha256": hashes, **counts, **({"errors": errors} if continue_on_error else {})}


def source_timestamp(value):
    if type(value) in (int, float) and math.isfinite(value):
        return value
    if isinstance(value, str) and len(value) <= 40 and re.match(r"\d{4}-\d{2}-\d{2}T", value):
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is not None:
                return parsed.timestamp()
        except ValueError:
            pass
    raise PublicationError("PUBLIC_SOURCE_TIMESTAMP_INVALID")


def freeze_independently(public, manifest, *, now):
    """A stale ancillary report must not prevent fresh live objects from advancing."""
    names = manifest.get("files"); required = manifest.get("required")
    if (not isinstance(names, list) or not isinstance(required, list) or not 1 <= len(names) <= 100
            or not all(isinstance(n, str) for n in names) or len(set(names)) != len(names)
            or not set(required) <= set(names)):
        raise PublicationError("INVALID_PUBLIC_MANIFEST")
    frozen, errors, total = {}, [], 0
    for name in names:
        try:
            value = freeze_public_tree(public, {"files": [name], "required": [name] if name in required and name not in OBSERVATION_FILES else []}, now=now)
            if name in OBSERVATION_FILES:
                if name not in value:
                    raise PublicationError("PUBLIC_SOURCE_TIMESTAMP_INVALID")
                generated = source_timestamp(strict_object(value[name]).get('generated_at'))
                if not 0 <= now-generated <= MAX_SOURCE_AGE:
                    raise PublicationError("PUBLIC_SOURCE_STALE_OR_FUTURE")
            total += sum(len(raw) for raw in value.values())
            if total > MAX_TOTAL_BYTES: raise PublicationError("PUBLIC_TREE_TOO_LARGE")
            frozen.update(value)
        except (OSError, ValueError, PublicationError) as exc:
            if str(exc) == "PUBLIC_TREE_TOO_LARGE": raise
            info = failure(exc, stage="freeze", name=name)
            if name in OBSERVATION_FILES:
                try:
                    stamp = source_timestamp(strict_object(read_regular(public/name, MAX_FILE_BYTES)).get('generated_at'))
                    info.update(source_generated_at=stamp, source_age_seconds=round(now-stamp,3))
                except (OSError, ValueError, PublicationError): pass
            errors.append(info)
    configured_live = manifest.get('live', list(LIVE_FILES) if any(n in names for n in LIVE_FILES) else [])
    if configured_live and (set(configured_live) != set(LIVE_FILES) or not set(configured_live) <= set(names)):
        raise PublicationError("INVALID_PUBLIC_MANIFEST")
    return frozen, errors


def write_status(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, sort_keys=True)
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


def build_public_tree(deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise PublicationError("PUBLICATION_DEADLINE_EXCEEDED")
    with subprocess.Popen(
        [sys.executable, str(SITE_DIR / "scripts/build_public.py")], start_new_session=True
    ) as process:
        try:
            result = process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
            raise PublicationError("PUBLICATION_DEADLINE_EXCEEDED") from None
        if result:
            raise PublicationError("PUBLIC_BUILD_FAILED")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", help="Inspect the current public tree without building or using credentials")
    args = parser.parse_args()
    validate_prefix()
    public = SITE_DIR / "public"
    manifest = strict_object(read_regular(SITE_DIR / "scripts/r2-public-files.json", 32768))
    if args.plan:
        frozen = freeze_public_tree(public, manifest, now=time.time())
        print(json.dumps({"files": sorted(frozen), "bytes": sum(map(len, frozen.values())), "network_requests": 0}))
        return 0

    bucket = os.environ.get("YUKIMURATA_R2_BUCKET", "").strip()
    credentials_path = Path(os.environ.get("YUKIMURATA_R2_CREDENTIALS", str(Path.home() / ".cloudflare-r2.env")))
    credentials = load_credentials(credentials_path, public)
    status_path = Path(os.environ.get("YUKIMURATA_R2_STATUS_FILE", str(SITE_DIR / ".state/public-push-r2.json")))
    if not status_path.is_absolute() or status_path.resolve().is_relative_to(public.resolve()):
        raise PublicationError("STATUS_MUST_BE_PRIVATE")
    status_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = os.open(status_path.with_suffix(".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        prior = strict_object(read_regular(status_path, 65536)) if status_path.exists() else {}
        failure_count = prior.get("failure_count", 0)
        if type(failure_count) is not int or failure_count < 0:
            raise PublicationError("INVALID_FAILURE_COUNTER")
        binding = f"{credentials.account_id}/{bucket}/{PREFIX}"
        last_hashes = (
            prior.get("verified_sha256", {})
            if prior.get("destination") == binding and prior.get("status") == "SUCCEEDED"
            else {}
        )
        status = {
            "schema": "public.r2_publication.v1", "status": "RUNNING", "destination": binding,
            "started_at": datetime.now(UTC).isoformat(), "completed_at": None,
            "failure_count": failure_count, "last_failure_at": prior.get("last_failure_at"),
            "verified_sha256": last_hashes, "public_mirror_verified": False,
        }
        deadline = time.monotonic() + 240
        receipts = Receipts(status_path.with_suffix('.objects.json'), status_path.with_suffix('.events.jsonl'),
            binding=binding, allowed=manifest['files'], write=write_status, seed=last_hashes)
        write_status(status_path, status)
        client = None
        try:
            client = R2Client(credentials, bucket, deadline=deadline)
            build_started = time.monotonic()
            build_public_tree(deadline)
            frozen, errors = freeze_independently(public, manifest, now=time.time())
            for info in errors: receipts.event(info)
            dynamic = {name: frozen[name] for name in (*LIVE_FILES, *OBSERVATION_FILES) if name in frozen}
            # Generated history HTML is dynamic and does not bind a static revision.
            dynamic.update({name: raw for name, raw in frozen.items() if name in {'stream-v3-loki.html', 'stream-v3-prometheus.html'}})
            static = {name: raw for name, raw in frozen.items() if name not in dynamic}
            static_errors = [e for e in errors if e.get('object') not in OBSERVATION_FILES]
            upload_started = time.monotonic()
            status["build_seconds"] = round(upload_started - build_started, 3)
            result = publish(client, dynamic, {}, receipts=receipts, continue_on_error=True)
            errors.extend(result.pop('errors'))
            if not static_errors:
                try:
                    if RELEASE_FILE in manifest['files']:
                        static = prepare_site_release(static)
                        write_public_release(public, static)
                    static_result = publish(client, static, {}, receipts=receipts, continue_on_error=True)
                    errors.extend(static_result.pop('errors'))
                    for key in ('uploaded', 'reconciled', 'unchanged'): result[key] += static_result[key]
                    result['artifact_sha256'].update(static_result['artifact_sha256'])
                except (OSError, ValueError, PublicationError) as exc:
                    info = failure(exc, stage='static_release'); errors.append(info); receipts.event(info)
            status["publish_seconds"] = round(time.monotonic() - upload_started, 3)
            status["duration_seconds"] = round(time.monotonic() - build_started, 3)
            status["max_parallel_requests"] = 2
            status.update(result)
            status.update(status="PARTIAL" if errors else "SUCCEEDED", errors=errors,
                verified_sha256=result["artifact_sha256"], completed_at=datetime.now(UTC).isoformat())
            if errors:
                status.update(failure_count=failure_count+1, last_failure_at=status['completed_at'])
        except (OSError, ValueError, PublicationError) as exc:
            failed_at = datetime.now(UTC).isoformat()
            detail = failure(exc, stage='cycle'); receipts.event(detail)
            status.update(status="FAILED", completed_at=failed_at, last_failure_at=failed_at,
                failure_count=failure_count + 1, error_type=type(exc).__name__, errors=[detail])
        finally:
            if client is not None:
                status['network'] = client.network_stats()
                client.close()
        status['publication_health'] = health_observation(deadline=deadline,
            opener=urllib.request.build_opener(NoRedirect()).open)
        if status['publication_health'].get('code') != 'HEALTH_NOT_CONFIGURED' and status['publication_health'].get('http_status') != 200:
            receipts.event({'stage':'publication_health', **status['publication_health']})
        write_status(status_path, status)
        print(json.dumps({k: status.get(k) for k in ["status", "uploaded", "reconciled", "unchanged", "completed_at", "errors", "publication_health", "network"]}))
        return 0 if status['status'] == 'SUCCEEDED' else 1
    finally:
        os.close(lock)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, PublicationError) as exc:
        print(f"R2 publication failed ({type(exc).__name__})", file=sys.stderr)
        raise SystemExit(1) from None
