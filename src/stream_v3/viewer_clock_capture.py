"""Read-only public playback adapters, bounded child processes, and local OCR."""

from __future__ import annotations

import csv
import io
import json
import math
import os
import re
import selectors
import signal
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .viewer_clock import Capture, ClockReading, GrayImage, TIME_RE


class ProbeError(Exception):
    """Only fixed error codes cross the observation/log boundary."""


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: bytes
    stderr: bytes


class ProcessRunner:
    def __init__(self) -> None:
        self.stopping = False

    def run(self, args: list[str], *, deadline: float, input_bytes: bytes | None = None, max_stdout: int = 2 * 1024 * 1024) -> CommandResult:
        if self.stopping:
            raise ProbeError("worker_stopping")
        if time.monotonic() >= deadline:
            raise ProbeError("deadline_exceeded")
        env = {**os.environ, "OMP_THREAD_LIMIT": "1", "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"}
        try:
            process = subprocess.Popen(args, stdin=subprocess.PIPE if input_bytes is not None else subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True, env=env)
        except OSError:
            raise ProbeError("executable_unavailable") from None
        output, errors = bytearray(), bytearray()
        pending = memoryview(input_bytes or b"")
        selector = selectors.DefaultSelector()
        try:
            for stream, kind in ((process.stdout, "stdout"), (process.stderr, "stderr")):
                assert stream is not None
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, kind)
            if process.stdin is not None:
                os.set_blocking(process.stdin.fileno(), False)
                if pending:
                    selector.register(process.stdin, selectors.EVENT_WRITE, "stdin")
                else:
                    process.stdin.close()
            while selector.get_map() or process.poll() is None:
                if self.stopping:
                    raise ProbeError("worker_stopping")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ProbeError("deadline_exceeded")
                for key, _ in selector.select(min(0.05, remaining)):
                    if key.data == "stdin":
                        try:
                            count = os.write(key.fd, pending[:65536])
                            pending = pending[count:]
                        except BrokenPipeError:
                            pending = pending[:0]
                        if not pending:
                            selector.unregister(key.fileobj)
                            key.fileobj.close()
                    else:
                        content = os.read(key.fd, 65536)
                        if not content:
                            selector.unregister(key.fileobj)
                            continue
                        target = output if key.data == "stdout" else errors
                        limit = max_stdout if key.data == "stdout" else 65536
                        if len(target) + len(content) > limit:
                            raise ProbeError("subprocess_output_limit")
                        target.extend(content)
            return CommandResult(process.wait(), bytes(output), bytes(errors))
        finally:
            # Kill the whole private session even if its parent has already exited.
            # This also covers descendants retaining a pipe after parent termination.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            selector.close()
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()


@dataclass(frozen=True)
class PlaybackSource:
    url: str = field(repr=False)
    identity: str
    codec: str = "unknown"
    public: bool = True


def public_url(value: str) -> bool:
    try:
        parsed = urlsplit(value)
        return parsed.scheme in ("http", "https") and bool(parsed.hostname) and parsed.username is None and parsed.password is None
    except ValueError:
        return False


class YouTubeAdapter:
    """Reuse the existing yt-dlp resolver mechanism, without API-live gating."""

    def __init__(self, config: dict[str, Any], runner: ProcessRunner, *, height: int) -> None:
        self.config, self.runner, self.height = config, runner, height
        self.last_video_id = config.get("video_id", "")

    def resolve(self, *, deadline: float) -> PlaybackSource:
        if not self.config.get("public_capture_enabled", False):
            raise ProbeError("public_capture_not_enabled")
        video_id = self.last_video_id
        if self.config.get("resolver_state_file"):
            try:
                path = Path(self.config["resolver_state_file"])
                if path.stat().st_size > 65536:
                    raise ValueError("resolver_too_large")
                candidate = json.loads(path.read_text())["video_id"]
                if isinstance(candidate, str) and re.fullmatch(r"[A-Za-z0-9_-]{6,32}", candidate):
                    video_id = candidate
            except (OSError, ValueError, KeyError, TypeError):
                # The API resolver supplies a public identity hint. An API outage
                # must not veto capture of the configured/last valid public ID.
                if not video_id:
                    raise ProbeError("resolver_identity_unavailable") from None
        if not isinstance(video_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{6,32}", video_id):
            raise ProbeError("invalid_video_id")
        self.last_video_id = video_id
        result = self.runner.run([
            self.config.get("yt_dlp", "yt-dlp"), "--ignore-config", "--no-cache-dir", "--no-warnings", "--no-playlist", "--skip-download",
            "--socket-timeout", "5", "--retries", "0", "--extractor-retries", "0", "--fragment-retries", "0",
            "--format", f"best[height<={self.height}][vcodec!=none]/bestvideo[height<={self.height}]",
            "--print", "%(vcodec)s\t%(url)s", f"https://www.youtube.com/watch?v={video_id}",
        ], deadline=deadline, max_stdout=65536)
        if result.returncode != 0:
            raise ProbeError("youtube_playback_resolution_failed")
        parts = result.stdout.decode("utf-8", errors="replace").strip().split("\t", 1)
        if len(parts) != 2 or not public_url(parts[1]):
            raise ProbeError("youtube_playback_url_invalid")
        codec = parts[0] if re.fullmatch(r"[A-Za-z0-9_.-]{1,40}", parts[0]) else "unknown"
        return PlaybackSource(parts[1], f"youtube:{video_id}", codec)


class TwitchAdapter:
    def __init__(self, config: dict[str, Any], runner: ProcessRunner, *, height: int) -> None:
        self.config, self.runner, self.height = config, runner, height

    def resolve(self, *, deadline: float) -> PlaybackSource:
        if not self.config.get("public_capture_enabled", False):
            raise ProbeError("public_capture_not_enabled")
        # The GET producer selects the current target. Never reuse a previous URL
        # or fall back to a channel stored in the worker configuration.
        try:
            path = Path(self.config.get("api_state_file") or self.config["url_state_file"])
            if path.stat().st_size > 65536:
                raise ValueError("url_state_too_large")
            hint = json.loads(path.read_text())
            if not isinstance(hint, dict):
                raise ValueError("invalid_url_state")
            if self.config.get("api_state_file"):
                # Reuse the existing credential-free Helix GET evidence. The
                # collector has already bound user_login through GET /users.
                # A successful OFFLINE response still supplies a viewer target;
                # its API verdict must not be mistaken for video unavailability.
                login = hint.get("user_login")
                if (hint.get("schema") != "monitoring_v4.twitch_api_evidence.v1"
                        or hint.get("probe_status") != "ok" or hint.get("http_status") != 200
                        or hint.get("error_reason") != "" or hint.get("provider_state") not in ("LIVE", "OFFLINE")
                        or not isinstance(login, str) or not re.fullmatch(r"[A-Za-z0-9_]{1,25}", login)):
                    raise ValueError("api_identity_unavailable")
                hint = {"platform": "twitch", "checked_at_utc": hint.get("collected_at"),
                        "expires_at_utc": hint.get("valid_until"), "stream_id": hint.get("stream_id"),
                        "url": f"https://www.twitch.tv/{login}"}
            if hint.get("platform") != "twitch":
                raise ValueError("invalid_url_state")
        except (KeyError, OSError, ValueError, TypeError):
            raise ProbeError("twitch_url_unavailable") from None
        url = hint.get("url")
        if not isinstance(url, str) or not url:
            raise ProbeError("twitch_url_unavailable")
        now = time.time()
        try:
            stamp = datetime.fromisoformat(hint["checked_at_utc"].replace("Z", "+00:00"))
            if stamp.tzinfo is None:
                raise ValueError("naive_timestamp")
            age = now - stamp.timestamp()
            if not 0 <= age <= self.config.get("url_max_age_seconds", 180):
                raise ProbeError("twitch_url_stale")
            if hint.get("expires_at_utc") is not None:
                expires = datetime.fromisoformat(hint["expires_at_utc"].replace("Z", "+00:00"))
                if expires.tzinfo is None or expires.timestamp() <= now:
                    raise ProbeError("twitch_url_expired")
        except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
            raise ProbeError("twitch_url_timestamp_invalid") from None
        if not public_url(url):
            raise ProbeError("twitch_url_invalid")
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.port not in (None, 443):
            raise ProbeError("twitch_url_invalid")
        host = parsed.hostname or ""
        watch = host in ("twitch.tv", "www.twitch.tv", "m.twitch.tv")
        match = re.fullmatch(r"/([A-Za-z0-9_]{1,25})/?", parsed.path) if watch else None
        channel = match[1] if match else hint.get("channel", "")
        if not isinstance(channel, str) or not re.fullmatch(r"[A-Za-z0-9_]{1,25}", channel):
            raise ProbeError("twitch_url_target_missing")
        identity = f"twitch:{channel.lower()}"
        stream_id = hint.get("stream_id")
        if stream_id is not None:
            if not isinstance(stream_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", stream_id):
                raise ProbeError("twitch_url_target_invalid")
            identity += f":{stream_id}"
        if not watch:
            media_host = any(host.endswith('.' + domain) for domain in ("ttvnw.net", "live-video.net"))
            if not media_host or not parsed.path.endswith(".m3u8"):
                raise ProbeError("twitch_url_host_or_path_invalid")
            return PlaybackSource(url, identity)
        if not match:
            raise ProbeError("twitch_url_invalid")
        # Some channels expose only their source rendition. Bound that fallback
        # to 1080p (never unrestricted "best"), then scale in the capped decoder.
        qualities = "360p,360p30,360p60,480p,720p,720p30,720p60,1080p,1080p30,1080p60" if self.height == 360 else "720p,720p30,720p60,480p,360p,360p30,360p60,1080p,1080p30,1080p60"
        result = self.runner.run([
            self.config.get("streamlink", "streamlink"), "--no-config", "--loglevel", "none", "--http-timeout", "5", "--stream-url",
            f"https://www.twitch.tv/{channel}", qualities,
        ], deadline=deadline, max_stdout=65536)
        if result.returncode != 0:
            raise ProbeError("twitch_playback_resolution_failed")
        url = result.stdout.decode("utf-8", errors="replace").strip()
        if not public_url(url):
            raise ProbeError("twitch_playback_url_invalid")
        return PlaybackSource(url, identity)


class FixtureAdapter:
    """Explicit local-file mode for development; never resolves a network URL."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path).resolve()

    def resolve(self, *, deadline: float) -> PlaybackSource:
        if not self.path.is_file():
            raise ProbeError("fixture_unavailable")
        return PlaybackSource(str(self.path), "local_fixture", public=False)


def capture_frame(source: PlaybackSource, runner: ProcessRunner, *, ffmpeg: str, height: int, deadline: float) -> Capture:
    started = time.monotonic()
    width = height * 16 // 9
    protocols = "http,https,tcp,tls,crypto" if source.public else "file,pipe"
    result = runner.run([
        ffmpeg, "-hide_banner", "-nostdin", "-loglevel", "info", "-threads", "1", "-filter_threads", "1",
        "-protocol_whitelist", protocols, "-rw_timeout", "5000000", "-i", source.url,
        "-map", "0:v:0", "-an", "-sn", "-dn", "-vf", f"scale=w=min({width}\\,iw):h=min({height}\\,ih):force_original_aspect_ratio=decrease,format=gray",
        "-frames:v", "1", "-threads", "1", "-c:v", "pgm", "-f", "image2pipe", "pipe:1",
    ], deadline=deadline)
    if result.returncode != 0:
        raise ProbeError("frame_capture_failed")
    try:
        frame = GrayImage.from_pgm(result.stdout)
    except ValueError:
        raise ProbeError("invalid_captured_frame") from None
    codec_match = re.search(rb"Video:\s*([a-zA-Z0-9_.-]+)", result.stderr)
    dimensions = re.search(rb"Video:[^\r\n]*?\b(\d{2,5})x(\d{2,5})\b", result.stderr)
    codec = codec_match[1].decode("ascii") if codec_match else source.codec
    return Capture(frame, source.identity, time.time(), time.monotonic(), codec, time.monotonic() - started,
                   int(dimensions[1]) if dimensions else None, int(dimensions[2]) if dimensions else None)


class TesseractOCR:
    def __init__(self, runner: ProcessRunner, *, binary: str = "tesseract", tessdata_dir: str | None = None) -> None:
        self.runner, self.binary, self.tessdata_dir = runner, binary, tessdata_dir

    def read(self, image: GrayImage, *, deadline: float) -> ClockReading:
        args = [self.binary, "stdin", "stdout", "-l", "eng", "--psm", "7", "-c", "tessedit_char_whitelist=0123456789:", "-c", "tessedit_create_tsv=1"]
        if self.tessdata_dir:
            args[3:3] = ["--tessdata-dir", self.tessdata_dir]
        try:
            result = self.runner.run(args, deadline=deadline, input_bytes=image.ocr_input().pgm(), max_stdout=65536)
        except (ProbeError, ValueError) as exc:
            return ClockReading(error=str(exc) if isinstance(exc, ProbeError) else "ocr_roi_too_large")
        if result.returncode != 0:
            return ClockReading(error="ocr_failed")
        words: list[tuple[str, float]] = []
        try:
            for row in csv.DictReader(io.StringIO(result.stdout.decode("utf-8")), delimiter="\t"):
                if row.get("level") == "5" and row.get("text", "").strip():
                    value = float(row["conf"])
                    if math.isfinite(value):
                        words.append((re.sub(r"[^0-9:]", "", row["text"])[:32], value))
        except (ValueError, KeyError, UnicodeError, TypeError):
            return ClockReading(error="ocr_output_invalid")
        text = "".join(w[0] for w in words)[:32]
        matches = list(TIME_RE.finditer(text))
        if len(matches) != 1 or text != matches[0][0]:
            return ClockReading(text=text, error="ocr_time_unreadable")
        return ClockReading(text, min((w[1] for w in words), default=0.0))
