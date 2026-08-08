#!/usr/bin/env python3
"""Capture and classify a lightweight frame from the public YouTube viewer path."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence


def default_repo_root() -> Path:
    return Path(os.environ.get("STREAM_V3_REPO_DIR", Path(__file__).resolve().parents[2])).expanduser()


def default_state_root(repo_root: Path) -> Path:
    configured = os.environ.get("STREAM_V3_OBSERVABILITY_STATE_ROOT") or os.environ.get("STREAM_RUNTIME_STATE_DIR")
    return Path(configured).expanduser() if configured else repo_root / ".state" / "observability-monitor"


DEFAULT_REPO_ROOT = default_repo_root()
DEFAULT_STATE_ROOT = default_state_root(DEFAULT_REPO_ROOT)
VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{6,32}$")
URL_RE = re.compile(r"https?://\S+")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        handle.write("\n")


def safe_detail(value: str) -> str:
    return URL_RE.sub("<redacted-url>", str(value or "").replace("\n", " "))[-240:]


def run(args: Sequence[str], *, timeout_sec: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(args), capture_output=True, text=True, timeout=timeout_sec, check=False)


def frame_fingerprint(path: Path, *, ffmpeg: str, timeout_sec: float = 5.0) -> str:
    """Return a compact perceptual fingerprint without an optional image library dependency."""
    try:
        completed = subprocess.run(
            [
                ffmpeg,
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "error",
                "-i",
                str(path),
                "-vf",
                "scale=16:9,format=gray",
                "-frames:v",
                "1",
                "-f",
                "rawvideo",
                "-",
            ],
            capture_output=True,
            timeout=timeout_sec,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    pixels = completed.stdout
    if completed.returncode != 0 or len(pixels) != 16 * 9:
        return ""
    mean = sum(pixels) / len(pixels)
    bits = "".join("1" if value >= mean else "0" for value in pixels)
    return f"{int(bits, 2):036x}"


def fingerprint_distance(left: str, right: str) -> int | None:
    if len(left) != 36 or len(right) != 36:
        return None
    try:
        return (int(left, 16) ^ int(right, 16)).bit_count()
    except ValueError:
        return None


def resolve_viewer_url(video_id: str, *, yt_dlp: str, timeout_sec: float) -> tuple[str, str]:
    watch_url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        completed = run(
            [
                yt_dlp,
                "--no-warnings",
                "--no-playlist",
                "--format",
                "worst[height<=360][vcodec!=none]/worst[height<=360]/worst",
                "--get-url",
                watch_url,
            ],
            timeout_sec=timeout_sec,
        )
    except subprocess.TimeoutExpired:
        return "", "yt_dlp_timeout"
    url = next((line.strip() for line in completed.stdout.splitlines() if line.strip().startswith("http")), "")
    if completed.returncode != 0 or not url:
        return "", f"yt_dlp_failed:{completed.returncode}:{safe_detail(completed.stderr)}"
    return url, ""


def capture_frame(
    viewer_url: str,
    output_path: Path,
    *,
    ffmpeg: str,
    timeout_sec: float,
) -> tuple[dict[str, Any], str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(f".{output_path.stem}.{os.getpid()}.jpg")
    try:
        completed = run(
            [
                ffmpeg,
                "-hide_banner",
                "-nostdin",
                "-loglevel",
                "info",
                "-i",
                viewer_url,
                "-an",
                "-vf",
                "fps=5,scale=320:-2,blackdetect=d=2:pix_th=0.10,thumbnail=40",
                "-frames:v",
                "1",
                "-q:v",
                "5",
                "-y",
                str(temporary),
            ],
            timeout_sec=timeout_sec,
        )
    except subprocess.TimeoutExpired:
        temporary.unlink(missing_ok=True)
        return {}, "ffmpeg_timeout"

    stderr = completed.stderr
    black = "black_start:" in stderr
    if completed.returncode != 0 or not temporary.exists() or temporary.stat().st_size <= 0:
        temporary.unlink(missing_ok=True)
        return {}, f"ffmpeg_failed:{completed.returncode}:{safe_detail(stderr)}"
    os.replace(temporary, output_path)
    content = output_path.read_bytes()
    fingerprint = frame_fingerprint(output_path, ffmpeg=ffmpeg)
    return {
        "frame_ok": True,
        "black_detected": black,
        "freeze_detected": False,
        "capture_bytes": len(content),
        "capture_sha256": hashlib.sha256(content).hexdigest(),
        "capture_fingerprint": fingerprint,
        "capture_file": str(output_path),
    }, ""


def evaluate_result(
    previous: dict[str, Any],
    *,
    checked_at_utc: str,
    video_id: str,
    api_live_state: str,
    capture: dict[str, Any],
    error: str,
    duration_sec: float,
) -> dict[str, Any]:
    frame_ok = capture.get("frame_ok") is True
    fingerprint_delta = fingerprint_distance(
        str(previous.get("capture_fingerprint") or ""),
        str(capture.get("capture_fingerprint") or ""),
    )
    previous_sha = str(previous.get("capture_sha256") or "")
    current_sha = str(capture.get("capture_sha256") or "")
    freeze_detected = frame_ok and bool(previous_sha) and previous_sha == current_sha
    visual_bad = frame_ok and (capture.get("black_detected") is True or freeze_detected)
    probe_bad = bool(error) or not frame_ok
    previous_probe_failures = int(previous.get("consecutive_probe_failures") or 0)
    previous_visual_failures = int(previous.get("consecutive_visual_failures") or 0)
    probe_failures = previous_probe_failures + 1 if probe_bad else 0
    visual_failures = previous_visual_failures + 1 if visual_bad else 0
    status = "failed" if visual_bad else ("degraded" if probe_bad else "healthy")
    payload: dict[str, Any] = {
        "schema": "stream_v3.viewer_synthetic.v1",
        "checked_at_utc": checked_at_utc,
        "status": status,
        "video_id": video_id,
        "api_live_state": api_live_state,
        "frame_ok": frame_ok,
        "black_detected": bool(capture.get("black_detected")),
        "freeze_detected": freeze_detected,
        "fingerprint_delta": fingerprint_delta,
        "consecutive_probe_failures": probe_failures,
        "consecutive_visual_failures": visual_failures,
        "duration_sec": round(max(0.0, duration_sec), 3),
        "reason": safe_detail(error),
    }
    for key in ("capture_bytes", "capture_sha256", "capture_fingerprint", "capture_file"):
        if key in capture:
            payload[key] = capture[key]
    if not probe_bad:
        payload["last_success_at_utc"] = checked_at_utc
    elif previous.get("last_success_at_utc"):
        payload["last_success_at_utc"] = previous["last_success_at_utc"]
    if not visual_bad and frame_ok:
        payload["last_visual_ok_at_utc"] = checked_at_utc
    elif previous.get("last_visual_ok_at_utc"):
        payload["last_visual_ok_at_utc"] = previous["last_visual_ok_at_utc"]
    return payload


def history_row(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: payload.get(key)
        for key in (
            "schema",
            "checked_at_utc",
            "status",
            "video_id",
            "api_live_state",
            "frame_ok",
            "black_detected",
            "freeze_detected",
            "consecutive_probe_failures",
            "consecutive_visual_failures",
            "capture_bytes",
            "capture_sha256",
            "capture_fingerprint",
            "fingerprint_delta",
            "duration_sec",
            "reason",
        )
    }


def main(argv: Sequence[str] | None = None) -> int:
    state_root = Path(os.environ.get("STREAM_RUNTIME_STATE_DIR", str(DEFAULT_STATE_ROOT)))
    log_root = Path(os.environ.get("STREAM_RUNTIME_LOG_DIR", str(state_root / "logs")))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolver-state-file", type=Path, default=state_root / "youtube_video_id_resolver_state.json")
    parser.add_argument("--state-file", type=Path, default=state_root / "viewer_synthetic_status.json")
    parser.add_argument("--history-file", type=Path, default=log_root / "viewer_synthetic_status.jsonl")
    parser.add_argument("--capture-file", type=Path, default=state_root / "capture" / "viewer_synthetic" / "latest.jpg")
    parser.add_argument("--yt-dlp", default=os.environ.get("STREAM_V3_VIEWER_YT_DLP_BIN", "yt-dlp"))
    parser.add_argument("--ffmpeg", default=os.environ.get("STREAM_V3_VIEWER_FFMPEG_BIN", "ffmpeg"))
    parser.add_argument("--resolve-timeout-sec", type=float, default=float(os.environ.get("STREAM_V3_VIEWER_RESOLVE_TIMEOUT_SEC", "25")))
    parser.add_argument("--capture-timeout-sec", type=float, default=float(os.environ.get("STREAM_V3_VIEWER_CAPTURE_TIMEOUT_SEC", "35")))
    args = parser.parse_args(argv)

    started = time.monotonic()
    previous = read_json(args.state_file)
    resolver = read_json(args.resolver_state_file)
    video_id = str(resolver.get("video_id") or "").strip()
    api_live_state = str(resolver.get("api_live_state") or "unknown").lower()
    capture: dict[str, Any] = {}
    error = ""
    if not VIDEO_ID_RE.fullmatch(video_id):
        error = "resolver_video_id_missing_or_invalid"
    elif api_live_state not in {"live", "testing", "unknown"}:
        error = f"resolver_state_not_live:{api_live_state}"
    else:
        viewer_url, error = resolve_viewer_url(video_id, yt_dlp=args.yt_dlp, timeout_sec=max(5.0, args.resolve_timeout_sec))
        if viewer_url:
            capture, error = capture_frame(
                viewer_url,
                args.capture_file,
                ffmpeg=args.ffmpeg,
                timeout_sec=max(10.0, args.capture_timeout_sec),
            )

    payload = evaluate_result(
        previous,
        checked_at_utc=utc_now(),
        video_id=video_id,
        api_live_state=api_live_state,
        capture=capture,
        error=error,
        duration_sec=time.monotonic() - started,
    )
    atomic_json(args.state_file, payload)
    append_jsonl(args.history_file, history_row(payload))
    print(json.dumps(history_row(payload), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
