"""Independent, sequential viewer clock worker. No control-plane capabilities."""

from __future__ import annotations

import argparse
import fcntl
import io
import json
import math
import os
import resource
import signal
import time
import uuid
import zipfile
from pathlib import Path
from typing import Any, Sequence

from .viewer_clock import AnalysisConfig, ClockAnalyzer, GrayImage, PLATFORMS, SCHEMA, utc_timestamp
from .viewer_clock_capture import FixtureAdapter, ProbeError, ProcessRunner, TesseractOCR, TwitchAdapter, YouTubeAdapter, capture_frame


def atomic_bytes(path: Path, content: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as handle:
            os.chmod(temporary, 0o600)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(",", ":")) + "\n").encode()


class EvidenceStore:
    """Atomic, bounded ROI bundles. The newest active bundle per target is protected."""

    def __init__(self, root: Path, *, max_bytes: int = 256 * 1024 * 1024, retention_seconds: float = 7 * 86400, platforms: tuple[str, ...] = PLATFORMS) -> None:
        self.root, self.max_bytes, self.retention_seconds = root, max_bytes, retention_seconds
        if not platforms or any(not value or not all(c.isalnum() or c == "_" for c in value) for value in platforms):
            raise ValueError("invalid_evidence_targets")
        self.platforms = platforms
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        # A SIGKILL may leave an incomplete bundle. Only our own temporary names
        # in this dedicated directory are reclaimed, while the worker lock is held.
        for platform in self.platforms:
            for temporary in self.root.glob(f".*-{platform}-*.zip.*.tmp"):
                temporary.unlink()
        self.active: dict[str, Path] = {}
        for platform in self.platforms:
            matches = sorted(self.root.glob(f"*-{platform}-*.zip"), key=lambda p: p.stat().st_mtime)
            if matches:
                self.active[platform] = matches[-1]

    def allocation_bytes(self, size: int) -> int:
        block = os.statvfs(self.root).f_frsize or 4096
        return math.ceil(size / block) * block

    @staticmethod
    def disk_bytes(path: Path) -> int:
        stat = path.stat()
        return max(stat.st_size, stat.st_blocks * 512)

    def prune(self, *, now: float, incoming_bytes: int = 0) -> bool:
        files = sorted(self.root.glob("*.zip"), key=lambda p: p.stat().st_mtime)
        for path in list(files):
            # Seven days is a hard upper age; an active incident normally refreshes
            # its minimal pair every cycle. Never retain stale images indefinitely.
            if now - path.stat().st_mtime > self.retention_seconds:
                path.unlink()
                files.remove(path)
        total = sum(self.disk_bytes(p) for p in files)
        incoming_bytes = self.allocation_bytes(incoming_bytes)
        protected = set(self.active.values())
        for path in files:
            if total + incoming_bytes <= self.max_bytes:
                break
            if path not in protected:
                size = self.disk_bytes(path)
                path.unlink()
                total -= size
        return total + incoming_bytes <= self.max_bytes

    def save(self, row: dict[str, Any], images: tuple[GrayImage, GrayImage], *, now: float, representative: GrayImage | None = None) -> str | None:
        platform = row["platform"]
        if platform not in self.platforms:
            raise ValueError("invalid_platform")
        bundle = io.BytesIO()
        with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("observation.json", json_bytes(row))
            archive.writestr("previous.pgm", images[0].pgm())
            archive.writestr("current.pgm", images[1].pgm())
            if representative is not None:
                archive.writestr("representative.pgm", representative.pgm())
        content = bundle.getvalue()
        return self._commit(platform, content, now=now)

    def save_jpeg_pair(self, row: dict[str, Any], previous: bytes | None, current: bytes, *, now: float) -> str | None:
        """Retain authenticated display JPEGs only for anomalies; normal images are not passed here."""
        platform = row["platform"]
        if platform not in self.platforms:
            raise ValueError("invalid_platform")
        for value in (previous, current):
            if value is not None and (not value.startswith(b"\xff\xd8") or len(value) > 1024 * 1024):
                raise ValueError("invalid_evidence_jpeg")
        bundle = io.BytesIO()
        with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_STORED) as archive:
            archive.writestr("observation.json", json_bytes({**row, "evidence_image_kind": "720p_display_snapshot", "ocr_uses_native_resolution": True}))
            archive.writestr("current.jpg", current)
            if previous is not None:
                archive.writestr("previous.jpg", previous)
        return self._commit(platform, bundle.getvalue(), now=now)

    def _commit(self, platform: str, content: bytes, *, now: float) -> str | None:
        if not self.prune(now=now, incoming_bytes=len(content)):
            return None
        path = self.root / f"{int(now * 1000):013d}-{platform}-{uuid.uuid4().hex[:12]}.zip"
        atomic_bytes(path, content)
        self.active[platform] = path
        return path.name

    def recovered(self, platform: str) -> None:
        self.active.pop(platform, None)


def append_history(root: Path, payload: dict[str, Any], *, max_bytes: int = 1024 * 1024) -> None:
    path = root / "viewer_clock_history.jsonl"
    content = json_bytes(payload)
    if len(content) > max_bytes:
        raise ValueError("history_row_too_large")
    if path.exists() and path.stat().st_size + len(content) > max_bytes:
        os.replace(path, root / "viewer_clock_history.previous.jsonl")
    with path.open("ab") as handle:
        os.chmod(path, 0o600)
        handle.write(content)


def load_config(path: Path) -> dict[str, Any]:
    try:
        if path.stat().st_size > 65536:
            raise ValueError("config_too_large")
        config = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        raise ValueError("config_unreadable") from None
    if not isinstance(config, dict):
        raise ValueError("config_not_object")
    allowed = {"interval_seconds", "cycle_budget_seconds", "target_budget_seconds", "capture_height", "ffmpeg", "tesseract", "tessdata_dir", "audit_interval", "minimum_ocr_confidence", "stall_confirmations", "noise_floor", "change_threshold", "evidence_max_bytes", "evidence_retention_seconds", "history_max_bytes", "targets"}
    if set(config) - allowed:
        raise ValueError("unknown_config_key")
    for key, default, low, high in (
        ("interval_seconds", 60, 60, 86400), ("cycle_budget_seconds", 50, 2, 50),
        ("target_budget_seconds", 24, 1, 24), ("audit_interval", 10, 1, 100),
        ("minimum_ocr_confidence", 70, 0, 100), ("stall_confirmations", 2, 2, 100),
        ("noise_floor", 12, 0, 254), ("change_threshold", 0.015, 0.00001, 1),
        ("evidence_max_bytes", 256 * 1024 * 1024, 4096, 256 * 1024 * 1024),
        ("evidence_retention_seconds", 7 * 86400, 1, 7 * 86400),
        ("history_max_bytes", 1024 * 1024, 16384, 16 * 1024 * 1024),
    ):
        value = config.setdefault(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not low <= value <= high:
            raise ValueError("config_number_out_of_range")
    for key in ("audit_interval", "stall_confirmations", "noise_floor", "evidence_max_bytes", "history_max_bytes"):
        if not isinstance(config[key], int):
            raise ValueError("config_integer_required")
    if config["cycle_budget_seconds"] < 2 * config["target_budget_seconds"]:
        raise ValueError("insufficient_per_target_budget")
    if config.setdefault("capture_height", 720) not in (360, 720):
        raise ValueError("invalid_capture_height")
    targets = config.get("targets")
    if not isinstance(targets, dict) or set(targets) - set(PLATFORMS):
        raise ValueError("invalid_targets")
    for platform, target in targets.items():
        if not isinstance(target, dict) or set(target) - {"roi", "public_capture_enabled", "video_id", "resolver_state_file", "yt_dlp", "streamlink", "api_state_file", "url_state_file", "url_max_age_seconds", "fixture_file"}:
            raise ValueError("invalid_target_config")
        roi = target.get("roi")
        if not isinstance(roi, list) or len(roi) != 4 or any(isinstance(v, bool) or not isinstance(v, (float, int)) or not math.isfinite(v) for v in roi):
            raise ValueError("invalid_roi")
        x, y, w, h = roi
        if not (0 <= x < 1 and 0 <= y < 1 and 0 < w <= 0.25 and 0 < h <= 0.1 and x + w <= 1 and y + h <= 1):
            raise ValueError("invalid_roi")
        if not isinstance(target.get("public_capture_enabled", False), bool):
            raise ValueError("invalid_capture_flag")
        if target.get("fixture_file") and target.get("public_capture_enabled"):
            raise ValueError("ambiguous_capture_mode")
        if target.get("api_state_file") and target.get("url_state_file"):
            raise ValueError("ambiguous_twitch_url_source")
        if "url_max_age_seconds" in target:
            age = target["url_max_age_seconds"]
            if isinstance(age, bool) or not isinstance(age, (int, float)) or not math.isfinite(age) or not 1 <= age <= 900:
                raise ValueError("invalid_url_max_age")
        for key in ("video_id", "resolver_state_file", "yt_dlp", "streamlink", "api_state_file", "url_state_file", "fixture_file"):
            if key in target and (not isinstance(target[key], str) or not target[key] or len(target[key]) > 4096):
                raise ValueError("invalid_target_string")
    for key in ("ffmpeg", "tesseract", "tessdata_dir"):
        if key in config and (not isinstance(config[key], str) or not config[key] or len(config[key]) > 4096):
            raise ValueError("invalid_executable_config")
    return config


class ClockWorker:
    def __init__(self, config: dict[str, Any], state_root: Path, *, runner: ProcessRunner | None = None) -> None:
        self.config, self.state_root = config, Path(state_root)
        self.runner = runner or ProcessRunner()
        self.state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.evidence = EvidenceStore(self.state_root / "evidence", max_bytes=config["evidence_max_bytes"], retention_seconds=config["evidence_retention_seconds"])
        self.ocr = TesseractOCR(self.runner, binary=config.get("tesseract", "tesseract"), tessdata_dir=config.get("tessdata_dir"))
        self.analyzers: dict[str, ClockAnalyzer] = {}
        self.adapters: dict[str, Any] = {}
        for platform, target in config["targets"].items():
            self.analyzers[platform] = ClockAnalyzer(platform, AnalysisConfig(
                roi=tuple(target["roi"]), audit_interval=config["audit_interval"], minimum_confidence=config["minimum_ocr_confidence"],
                stall_confirmations=config["stall_confirmations"], noise_floor=config["noise_floor"], change_threshold=config["change_threshold"],
            ))
            adapter = YouTubeAdapter if platform == "youtube" else TwitchAdapter
            self.adapters[platform] = FixtureAdapter(Path(target["fixture_file"])) if target.get("fixture_file") else adapter(target, self.runner, height=config["capture_height"])

    def run_cycle(self) -> dict[str, Any]:
        started, started_epoch = time.monotonic(), time.time()
        cycle_deadline = started + self.config["cycle_budget_seconds"]
        usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        cpu_before = time.process_time()
        targets: dict[str, Any] = {}
        for platform in PLATFORMS:
            analyzer = self.analyzers.get(platform)
            if analyzer is None:
                analyzer = ClockAnalyzer(platform, AnalysisConfig((0, 0, 0.1, 0.05)))
                targets[platform] = analyzer.unknown("target_not_configured", now=time.time())
                continue
            deadline = min(cycle_deadline, time.monotonic() + self.config["target_budget_seconds"])
            target_started = time.monotonic()
            stage = "resolve"
            try:
                source = self.adapters[platform].resolve(deadline=min(deadline, time.monotonic() + 8))
                resolve_seconds = time.monotonic() - target_started
                stage = "capture"
                capture = capture_frame(source, self.runner, ffmpeg=self.config.get("ffmpeg", "ffmpeg"), height=self.config["capture_height"], deadline=min(deadline, time.monotonic() + 12))
                ocr_started = time.monotonic()
                ocr_seconds = 0.0
                stage = "analysis"

                def read_roi(image: GrayImage):
                    nonlocal ocr_seconds
                    before = time.monotonic()
                    reading = self.ocr.read(image, deadline=min(deadline, before + 2))
                    ocr_seconds += time.monotonic() - before
                    return reading

                row, images = analyzer.analyze(capture, read_roi, now=time.time())
                row["analysis_duration_seconds"] = round(time.monotonic() - ocr_started, 3)
                row["ocr_duration_seconds"] = round(ocr_seconds, 3)
                row["resolve_duration_seconds"] = round(resolve_seconds, 3)
                row["checked_at_utc"] = utc_timestamp(time.time())
                row["sample_age_seconds"] = max(0, time.time() - capture.captured_epoch)
                if time.monotonic() > deadline:
                    row, images = analyzer.unknown("target_deadline_exceeded", now=time.time()), None
                if images:
                    try:
                        evidence_id = self.evidence.save(row, images, now=time.time())
                        row["evidence_status"] = "saved" if evidence_id else "capacity_exhausted"
                        row["evidence_id"] = evidence_id
                    except OSError:
                        row["evidence_status"] = "storage_error"
                elif row["clock_state"].startswith("ADVANCING"):
                    self.evidence.recovered(platform)
                # Do not keep full frames alive while processing the other target.
                del capture
                targets[platform] = row
            except ProbeError as exc:
                targets[platform] = analyzer.unknown(str(exc), now=time.time())
            except (OSError, ValueError, MemoryError):
                targets[platform] = analyzer.unknown("worker_resource_or_io_error", now=time.time())
            except Exception:
                # A target boundary must never suppress the other target's result.
                targets[platform] = analyzer.unknown("worker_internal_error", now=time.time())
            targets[platform]["target_duration_seconds"] = round(time.monotonic() - target_started, 3)
            if targets[platform]["clock_state"] == "UNKNOWN":
                targets[platform]["error_stage"] = stage
        try:
            self.evidence.prune(now=time.time())
        except OSError:
            pass  # Reported on the next evidence write; acquisition remains independent.
        children = resource.getrusage(resource.RUSAGE_CHILDREN)
        payload = {
            "schema": SCHEMA, "checked_at_utc": utc_timestamp(time.time()),
            "cycle_started_at_utc": utc_timestamp(started_epoch), "targets": targets,
            "worker": {
                "duration_seconds": round(time.monotonic() - started, 3),
                "cpu_seconds": round(time.process_time() - cpu_before, 4),
                "child_cpu_seconds": round(children.ru_utime + children.ru_stime - usage_before.ru_utime - usage_before.ru_stime, 4),
                "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                "child_peak_rss_bytes": children.ru_maxrss * 1024,
            },
        }
        atomic_bytes(self.state_root / "viewer_clock_status.json", json_bytes(payload))
        append_history(self.state_root, payload, max_bytes=self.config["history_max_bytes"])
        return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True, help="Dedicated writable directory; never the legacy state file.")
    parser.add_argument("--once", action="store_true", help="One WARMUP cycle, useful for acquisition diagnostics only.")
    parser.add_argument("--max-cycles", type=int, help="Stop after a finite number of scheduled cycles for evaluation.")
    args = parser.parse_args(argv)
    if args.max_cycles is not None and (args.max_cycles <= 0 or args.once):
        parser.error("max-cycles must be positive and cannot be combined with once")
    try:
        config = load_config(args.config)
    except ValueError as exc:
        parser.error(str(exc))
    os.umask(0o077)
    args.state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (args.state_root / ".worker.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.exit(2, "viewer_clock_worker_already_running\n")
        worker = ClockWorker(config, args.state_root)
        def stop(signum: int, frame: Any) -> None:
            worker.runner.stopping = True
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        cycles = 0
        while not worker.runner.stopping:
            started = time.monotonic()
            try:
                payload = worker.run_cycle()
            except (OSError, ValueError, MemoryError):
                parser.exit(1, "viewer_clock_state_write_failed\n")
            print(json.dumps({"schema": SCHEMA, "checked_at_utc": payload["checked_at_utc"], "targets": {k: v["clock_state"] for k, v in payload["targets"].items()}}, sort_keys=True), flush=True)
            cycles += 1
            if args.once or (args.max_cycles is not None and cycles >= args.max_cycles):
                break
            # No catch-up burst after a slow cycle and no overlapping decoders.
            next_cycle = started + config["interval_seconds"]
            while not worker.runner.stopping and time.monotonic() < next_cycle:
                time.sleep(min(0.2, max(0, next_cycle - time.monotonic())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
