#!/usr/bin/env python3
"""Derive the narrow runtime-boundary lifecycle hook from an exact live base."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
from pathlib import Path


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_once(source: str, old: str, new: str, label: str) -> str:
    count = source.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one anchor, got {count}")
    return source.replace(old, new, 1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--expected-base-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--diff", type=Path, required=True)
    args = parser.parse_args()
    if sha256(args.base) != args.expected_base_sha256:
        raise SystemExit("LIVE_BASE_IDENTITY_MISMATCH")

    base = args.base.read_text(encoding="utf-8")
    candidate = replace_once(
        base,
        "        self.ffmpeg_proc: Optional[subprocess.Popen] = None\n",
        '        self.ffmpeg_proc: Optional[subprocess.Popen] = None\n        self.ffmpeg_lifecycle_state = "INITIALIZING"\n',
        "lifecycle initialization",
    )
    candidate = replace_once(
        candidate,
        "    def start_overlay_server(self) -> None:\n",
        "    def wait_for_ffmpeg_restart_delay(self, delay_sec: float) -> None:\n"
        '        """Behavior-preserving hook used by the fenced runtime-boundary subclass."""\n\n'
        "        time.sleep(delay_sec)\n\n"
        "    def start_overlay_server(self) -> None:\n",
        "restart-delay hook",
    )
    candidate = replace_once(
        candidate,
        '            self.append_event("ffmpeg_starting", encoder_profile=encoder_profile)\n',
        '            self.ffmpeg_lifecycle_state = "STARTING_FFMPEG"\n'
        '            self.append_event("ffmpeg_starting", encoder_profile=encoder_profile)\n',
        "starting lifecycle",
    )
    candidate = replace_once(
        candidate,
        "            self.ffmpeg_proc = subprocess.Popen(args, stderr=subprocess.PIPE)\n",
        "            self.ffmpeg_proc = subprocess.Popen(args, stderr=subprocess.PIPE)\n"
        '            self.ffmpeg_lifecycle_state = "FFMPEG_RUNNING"\n',
        "running lifecycle",
    )
    candidate = replace_once(
        candidate,
        "            self.ffmpeg_proc = None\n            self.ffmpeg_stderr_capture = None\n",
        '            self.ffmpeg_proc = None\n            self.ffmpeg_lifecycle_state = "FFMPEG_EXITED"\n'
        "            self.ffmpeg_stderr_capture = None\n",
        "exited lifecycle",
    )
    candidate = replace_once(
        candidate,
        "            time.sleep(self.cfg.restart_delay_sec)\n            if not self.wait_for_ingest_connectivity():\n",
        '            self.ffmpeg_lifecycle_state = "RESTART_DELAY"\n'
        "            self.wait_for_ffmpeg_restart_delay(self.cfg.restart_delay_sec)\n"
        '            self.ffmpeg_lifecycle_state = "CONNECTIVITY_WAIT"\n'
        "            if not self.wait_for_ingest_connectivity():\n",
        "restart delay",
    )
    candidate = replace_once(
        candidate,
        '        self.log("stream engine stopped.")\n',
        '        self.ffmpeg_lifecycle_state = "STOPPING"\n        self.log("stream engine stopped.")\n',
        "stopping lifecycle",
    )

    diff_lines = list(
        difflib.unified_diff(
            base.splitlines(),
            candidate.splitlines(),
            fromfile="live-base/stream_engine.py",
            tofile="current-task-overlay/stream_engine.py",
            lineterm="",
        )
    )
    added = [line[1:] for line in diff_lines if line.startswith("+") and not line.startswith("+++")]
    forbidden_added_tokens = (
        "audit_maintenance_decision",
        "os.kill(",
        ".terminate(",
        ".kill(",
        "systemctl",
        "kubectl",
        "subprocess.Popen(",
    )
    forbidden_added = [token for token in forbidden_added_tokens if any(token in line for line in added)]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(candidate, encoding="utf-8")
    args.diff.parent.mkdir(parents=True, exist_ok=True)
    args.diff.write_text("\n".join(diff_lines) + "\n", encoding="utf-8")
    manifest = {
        "schema_version": "stream_engine.current_task_overlay.v1",
        "base_path": str(args.base),
        "base_sha256": sha256(args.base),
        "expected_base_sha256": args.expected_base_sha256,
        "output_path": str(args.output),
        "output_sha256": sha256(args.output),
        "added_line_count": len(added),
        "forbidden_added_tokens": forbidden_added,
        "scope": [
            "lifecycle observation state",
            "behavior-preserving restart-delay hook",
        ],
        "mp10_audit_instrumentation_included": False,
        "ffmpeg_stderr_semantics_changed": False,
        "physical_mutation_call_added": False,
        "production_behavior_modified": False,
        "complete": not forbidden_added and candidate != base,
    }
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(manifest, sort_keys=True))
    return 0 if manifest["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
