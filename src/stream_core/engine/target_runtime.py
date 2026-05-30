from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from . import runtime_state


@dataclass(frozen=True)
class TargetRuntime:
    stream_key_hash: str
    stream_lock_file: Path
    takeover_coord_file: Path
    runtime_state_file: Path


def resolve_target_runtime(cfg) -> TargetRuntime:
    key_src = f"test:{cfg.test_output}:{cfg.test_output_file}" if cfg.test_mode else cfg.rtmp_url
    stream_key_hash = hashlib.sha256(key_src.encode("utf-8")).hexdigest()
    stream_lock_file = cfg.stream_lock_dir / f"adsb-stream-new-stream-{stream_key_hash}.lock"
    takeover_coord_file = cfg.stream_lock_dir / f"adsb-stream-new-stream-{stream_key_hash}.takeover.lock"
    runtime_state_file = cfg.runtime_state_file
    if runtime_state_file.name == "stream_runtime_state.json":
        runtime_state_file = runtime_state.hashed_runtime_state_file(runtime_state_file, stream_key_hash)
    return TargetRuntime(
        stream_key_hash=stream_key_hash,
        stream_lock_file=stream_lock_file,
        takeover_coord_file=takeover_coord_file,
        runtime_state_file=runtime_state_file,
    )
