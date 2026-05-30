from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class HeartbeatAction:
    stop_reason: str = ""
    exit_code_if_still_running: int = 0

    @property
    def should_stop(self) -> bool:
        return bool(self.stop_reason)


def stop_for_shutdown(
    proc: subprocess.Popen,
    *,
    reason: str,
    signum: int = 0,
    grace_sec: float,
    append_event: Callable[..., str],
    log: Callable[[str], None],
) -> bool:
    if proc.poll() is not None:
        return True
    pid = int(proc.pid or 0)
    append_event(
        "ffmpeg_stop_requested",
        ffmpeg_pid=pid,
        reason=reason,
        signal=signum,
        grace_sec=grace_sec,
    )
    try:
        proc.terminate()
    except Exception as e:
        append_event("ffmpeg_stop_terminate_error", ffmpeg_pid=pid, reason=reason, error=str(e))
        return proc.poll() is not None
    try:
        rc = proc.wait(timeout=grace_sec)
        append_event("ffmpeg_stop_exited", ffmpeg_pid=pid, reason=reason, exit_code=rc)
        return True
    except subprocess.TimeoutExpired:
        log(f"FFmpeg did not stop within {grace_sec:.1f}s after {reason}; killing pid={pid}.")
        append_event("ffmpeg_stop_timeout_kill", ffmpeg_pid=pid, reason=reason, grace_sec=grace_sec)
        try:
            proc.kill()
        except Exception as e:
            append_event("ffmpeg_stop_kill_error", ffmpeg_pid=pid, reason=reason, error=str(e))
            return proc.poll() is not None
    try:
        rc = proc.wait(timeout=1.0)
        append_event("ffmpeg_stop_killed", ffmpeg_pid=pid, reason=reason, exit_code=rc)
        return True
    except subprocess.TimeoutExpired:
        append_event("ffmpeg_stop_kill_wait_timeout", ffmpeg_pid=pid, reason=reason)
        return False


def stop_quietly(proc: subprocess.Popen, *, wait_timeout: float = 1.0) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
    except Exception:
        pass
    try:
        proc.wait(timeout=wait_timeout)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def wait_until_exit_or_action(
    proc: subprocess.Popen,
    *,
    heartbeat_sec: float,
    heartbeat_action: Callable[[], HeartbeatAction],
    stop_process: Callable[[str], bool | None],
) -> int:
    while True:
        try:
            return int(proc.wait(timeout=heartbeat_sec))
        except subprocess.TimeoutExpired:
            action = heartbeat_action()
            if not action.should_stop:
                continue
            stopped = stop_process(action.stop_reason)
            rc = proc.poll()
            if rc is None:
                if stopped is False:
                    raise RuntimeError(f"ffmpeg still running after stop_process: {action.stop_reason}")
                return action.exit_code_if_still_running
            return int(rc)
