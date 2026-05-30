from __future__ import annotations

from pathlib import Path

from .model import CheckResult


REQUIRED_RELATIVE_FILES = (
    "src/stream_core/stream_engine.py",
    "src/dj/auto_dj.py",
    "src/watchers/stream_watchdog.py",
    "src/watchers/youtube_watchdog.py",
    "src/watchers/youtube_video_id_resolver.py",
    "src/watchers/fast_recovery.py",
    "ui/overlay/index.html",
)


def required_file_results(base_dir: Path, relative_files: tuple[str, ...] = REQUIRED_RELATIVE_FILES) -> list[CheckResult]:
    results: list[CheckResult] = []
    for rel in relative_files:
        path = base_dir / rel
        exists = path.exists()
        results.append(
            CheckResult(
                name=f"file:{rel}",
                category="file_contract",
                severity="ok" if exists else "fail",
                ok=exists,
                fatal=not exists,
                summary=f"file: {path}" if exists else f"file missing: {path}",
                path=str(path),
            )
        )
    return results
