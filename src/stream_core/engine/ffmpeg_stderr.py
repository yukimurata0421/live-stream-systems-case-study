from __future__ import annotations

import io
import json
import re
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO


_RTMP_STREAM_KEY_RE = re.compile(r"(rtmps?://[^\s]+?/live2/)[^\s]+", re.IGNORECASE)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def redact_stderr_line(line: str) -> str:
    return _RTMP_STREAM_KEY_RE.sub(r"\1***", line)


class FFmpegStderrCapture:
    """Mirror redacted FFmpeg stderr and persist timestamped JSONL with rotation."""

    def __init__(
        self,
        *,
        path: Path,
        max_bytes: int,
        backup_count: int,
        run_id: str,
        ffmpeg_pid: int,
        stream_generation: str,
    ) -> None:
        self.path = path
        self.max_bytes = max(4096, int(max_bytes))
        self.backup_count = max(0, int(backup_count))
        self.run_id = run_id
        self.ffmpeg_pid = int(ffmpeg_pid)
        self.stream_generation = stream_generation
        self.line_count = 0
        self.last_line = ""
        self.rotated_count = 0
        self.capture_error = ""
        self._stream: BinaryIO | None = None
        self._thread: threading.Thread | None = None

    def start(self, stream: object) -> None:
        # unittest mocks and callers without PIPE remain valid no-op captures.
        if not isinstance(stream, io.IOBase):
            return
        self._stream = stream  # type: ignore[assignment]
        self._thread = threading.Thread(
            target=self._run,
            name=f"ffmpeg-stderr-{self.ffmpeg_pid}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout_sec: float = 2.0) -> dict[str, object]:
        thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0.0, timeout_sec))
            if thread.is_alive() and not self.capture_error:
                self.capture_error = "stderr capture thread did not reach EOF"
        return self.summary()

    def summary(self) -> dict[str, object]:
        return {
            "path": str(self.path),
            "format": "jsonl",
            "redacted": True,
            "line_count": self.line_count,
            "last_line": self.last_line[-1024:],
            "rotated_count": self.rotated_count,
            "capture_error": self.capture_error,
        }

    def _rotate(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.backup_count <= 0:
            self.path.write_text("", encoding="utf-8")
            self.rotated_count += 1
            return
        oldest = self.path.with_name(f"{self.path.name}.{self.backup_count}")
        try:
            oldest.unlink()
        except FileNotFoundError:
            pass
        for index in range(self.backup_count - 1, 0, -1):
            source = self.path.with_name(f"{self.path.name}.{index}")
            target = self.path.with_name(f"{self.path.name}.{index + 1}")
            if source.exists():
                source.replace(target)
        if self.path.exists():
            self.path.replace(self.path.with_name(f"{self.path.name}.1"))
        self.rotated_count += 1

    def _run(self) -> None:
        stream = self._stream
        if stream is None:
            return
        handle = None
        try:
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                if self.path.exists() and self.path.stat().st_size >= self.max_bytes:
                    self._rotate()
                handle = self.path.open("a", encoding="utf-8")
            except Exception as exc:
                self.capture_error = f"persistence disabled: {type(exc).__name__}: {exc}"
            while True:
                raw = stream.readline()
                if not raw:
                    break
                if isinstance(raw, bytes):
                    text = raw.decode("utf-8", errors="replace")
                else:
                    text = str(raw)
                line = redact_stderr_line(text.rstrip("\r\n"))
                self.line_count += 1
                self.last_line = line
                payload = {
                    "ts_utc": utc_now(),
                    "run_id": self.run_id,
                    "stream_generation": self.stream_generation,
                    "ffmpeg_pid": self.ffmpeg_pid,
                    "line": line,
                }
                encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
                try:
                    sys.stderr.write(line + "\n")
                    sys.stderr.flush()
                except Exception:
                    pass
                if handle is not None:
                    try:
                        if (
                            handle.tell() > 0
                            and handle.tell() + len(encoded.encode("utf-8")) > self.max_bytes
                        ):
                            handle.close()
                            handle = None
                            self._rotate()
                            handle = self.path.open("a", encoding="utf-8")
                        handle.write(encoded)
                        handle.flush()
                    except Exception as exc:
                        self.capture_error = f"persistence disabled: {type(exc).__name__}: {exc}"
                        if handle is not None:
                            try:
                                handle.close()
                            except Exception:
                                pass
                            handle = None
        except Exception as exc:
            self.capture_error = f"{type(exc).__name__}: {exc}"
        finally:
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
