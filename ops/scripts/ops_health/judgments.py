from __future__ import annotations

import sys
from pathlib import Path

try:
    from stream_core.ops_health.judgments import *  # noqa: F401,F403
except ModuleNotFoundError:
    base_dir = Path(__file__).resolve().parents[3]
    src_dir = base_dir / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    from stream_core.ops_health.judgments import *  # noqa: F401,F403
