from __future__ import annotations

import subprocess
from typing import Sequence

from stream_core.common.systemd import run_systemctl as _run_systemctl
from stream_core.common.systemd import systemctl_prefix


def run_systemctl(
    args: Sequence[str],
    *,
    require_privilege: bool,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    return _run_systemctl(args, require_privilege=require_privilege, check=check)
