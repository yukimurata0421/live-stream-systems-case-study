from __future__ import annotations

from pathlib import Path


DIAGNOSTIC_PATTERNS = (
    ("webgl2_blocklisted", "WebGL2 blocklisted"),
    ("webgl_context_fatal", "ContextResult::kFatalFailure"),
    ("gpu_process_crashed", "GPU process crashed"),
    ("renderer_process_crashed", "renderer process crashed"),
)


def read_new_diagnostics(path: Path, offset: int, *, max_bytes: int = 262_144) -> tuple[int, list[dict[str, str]]]:
    """Read newly appended browser diagnostics without retaining URLs or arbitrary log text."""
    try:
        size = path.stat().st_size
    except OSError:
        return 0, []
    start = 0 if size < int(offset) else max(0, int(offset))
    if size - start > max_bytes:
        start = size - max_bytes
    try:
        with path.open("rb") as handle:
            handle.seek(start)
            raw = handle.read(max_bytes)
            next_offset = handle.tell()
    except OSError:
        return start, []
    text = raw.decode("utf-8", "replace")
    found: list[dict[str, str]] = []
    for kind, marker in DIAGNOSTIC_PATTERNS:
        count = text.count(marker)
        if count:
            found.append({"diagnostic": kind, "count": str(count)})
    return next_offset, found
