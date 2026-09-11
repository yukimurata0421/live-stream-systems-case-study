from __future__ import annotations

import os
import subprocess
from pathlib import Path


def validate_cluster_unchanged(rendered: Path, *, kubectl: str, sudo: str) -> None:
    prefix = [sudo, "-n", kubectl, "kubectl"] if sudo else [kubectl, "kubectl"]
    dry_run = subprocess.run(
        [*prefix, "apply", "--dry-run=server", "-k", os.fspath(rendered)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=180,
        check=False,
    )
    if dry_run.returncode != 0 or dry_run.stderr:
        raise RuntimeError("server-side manifest dry-run failed or emitted diagnostics")
    difference = subprocess.run(
        [*prefix, "diff", "-k", os.fspath(rendered)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=180,
        check=False,
    )
    if difference.returncode != 0 or difference.stdout or difference.stderr:
        raise RuntimeError("live manifests differ from the prepared release")
