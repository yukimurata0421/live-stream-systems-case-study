#!/usr/bin/env python3
"""guardが停止しても、今回所有するrelay process groupを有限時間で停止する。"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
from pathlib import Path


def elapsed():
    return time.clock_gettime(time.CLOCK_BOOTTIME)


def identity(pid):
    raw = Path(f"/proc/{pid}/stat").read_text()
    fields = raw[raw.rfind(")") + 2:].split()
    return {"pid": pid, "parent_pid": int(fields[1]), "start_ticks": int(fields[19]),
            "process_group": int(fields[2]), "session": int(fields[3])}


def atomic(path, body):
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as out:
        json.dump(body, out, sort_keys=True)
        out.flush()
        os.fsync(out.fileno())
    os.replace(temporary, path)


def supervise(command, runtime, state=None):
    proc = subprocess.Popen(command, start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    anchor = identity(proc.pid)
    if anchor["parent_pid"] != os.getpid() or anchor["process_group"] != proc.pid or anchor["session"] != proc.pid:
        raise RuntimeError("RELAY_SUPERVISOR_CHILD_UNPROVEN")
    stopped = False
    def stop_requested(*_):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop_requested)
    signal.signal(signal.SIGINT, stop_requested)
    started = elapsed()
    reason = "GUARD_START_TIMEOUT"
    latest = {}
    try:
        while not stopped:
            # wait/reap前のowned session leaderをanchorにする。
            current = identity(proc.pid)
            if current != anchor:
                reason = "GUARD_IDENTITY_CHANGED"
                break
            status_path = runtime / "guard-status.json"
            if status_path.is_file():
                raw = status_path.read_bytes()
                if len(raw) > 8192:
                    reason = "GUARD_STATUS_TOO_LARGE"
                    break
                latest = json.loads(raw)
                now = elapsed()
                if latest.get("halted") is True:
                    reason = latest.get("reason", "GUARD_HALTED")
                    break
                if not 0 <= now - latest["heartbeat_elapsed"] <= 5:
                    reason = "GUARD_HEARTBEAT_EXPIRED"
                    break
                expiry = latest.get("lease_elapsed_deadline")
                if expiry is not None and now >= expiry:
                    reason = "LEASE_ELAPSED_EXPIRED"
                    break
            elif elapsed() - started > 6:
                break
            time.sleep(.1)
        else:
            reason = "OPERATOR_STOP"
    finally:
        # PIDをreapせず同じsessionを確認してから、所有groupだけを停止する。
        if identity(proc.pid) != anchor:
            raise RuntimeError("RELAY_SUPERVISOR_STOP_IDENTITY_UNKNOWN")
        stop_started = elapsed()
        try:
            os.killpg(proc.pid, signal.SIGCONT)
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass  # The anchored, unreaped leader may already be a zombie.
        deadline = elapsed() + 3
        while elapsed() < deadline:
            members = []
            for path in Path("/proc").iterdir():
                if path.name.isdecimal():
                    try:
                        member = identity(int(path.name))
                        raw = (path / "stat").read_text().rsplit(")", 1)[1].split()
                        if member["session"] == proc.pid and raw[0] != "Z":
                            members.append(member)
                    except (FileNotFoundError, ProcessLookupError):
                        pass
            if not members:
                break
            time.sleep(.05)
        else:
            os.killpg(proc.pid, signal.SIGKILL)
            time.sleep(.1)
        proc.wait(timeout=1)
        remaining = []
        for path in Path("/proc").iterdir():
            if path.name.isdecimal():
                try:
                    member = identity(int(path.name))
                    raw = (path / "stat").read_text().rsplit(")", 1)[1].split()
                    if member["session"] == anchor["session"] and raw[0] != "Z":
                        remaining.append(member)
                except (FileNotFoundError, ProcessLookupError):
                    pass
        receipt = {"schema": "stream.relay_supervisor_stop.v1",
               "instance": latest.get("instance"), "anchor": anchor, "reason": reason,
               "process_exit_observed": not remaining, "remaining_count": len(remaining),
               "stop_elapsed": elapsed() - stop_started}
        atomic(runtime / "stop-receipt.json", receipt)
        if state is not None:
            atomic(state / ('supervisor-stop-' + str(latest.get('instance', proc.pid)) + '.json'), receipt)
        if remaining:
            raise RuntimeError("RELAY_WRITER_STOP_UNKNOWN")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--state", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command[:1] == ["--"]:
        args.command = args.command[1:]
    supervise(args.command, args.runtime, args.state)


if __name__ == "__main__":
    main()
