#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import json
import os
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    from stream_core.engine import audio_boot, ffmpeg_lifecycle, ingest, locks as engine_locks, preflight, process_discovery, rendering_boot, restart_context, runtime_state, target_runtime
    from stream_core.engine.config import Config, load_config, to_bool, to_float, to_int
    from stream_core.engine.encoder_profile import (
        effective_encoder_profile as choose_effective_encoder_profile,
        emergency_low_upload_profile as choose_emergency_low_upload_profile,
        encoder_profile_expired as is_encoder_profile_expired,
    )
    from stream_core.engine.events import StreamEventWriter
    from stream_core.engine.ffmpeg_args import build_ffmpeg_args, build_filter as build_video_filter, build_output_args as build_ffmpeg_output_args
except ModuleNotFoundError:
    from engine import audio_boot, ffmpeg_lifecycle, ingest, locks as engine_locks, preflight, process_discovery, rendering_boot, restart_context, runtime_state, target_runtime
    from engine.config import Config, load_config, to_bool, to_float, to_int
    from engine.encoder_profile import (
        effective_encoder_profile as choose_effective_encoder_profile,
        emergency_low_upload_profile as choose_emergency_low_upload_profile,
        encoder_profile_expired as is_encoder_profile_expired,
    )
    from engine.events import StreamEventWriter
    from engine.ffmpeg_args import build_ffmpeg_args, build_filter as build_video_filter, build_output_args as build_ffmpeg_output_args

def run(cmd: list[str], check: bool = True, timeout: Optional[float] = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=check, timeout=timeout)


class StreamEngine:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.stop_requested = False
        self.restart_count = 0
        self.last_health_ok = True
        self.run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{os.getpid()}"
        self.stream_key_hash = ""
        self.stream_lock_file = Path()
        self.takeover_coord_file = Path()
        self.capture_lock_file = Path()
        self.ffmpeg_proc: Optional[subprocess.Popen] = None
        self.xvfb_proc: Optional[subprocess.Popen] = None
        self.overlay_proc: Optional[subprocess.Popen] = None
        self.browser_proc: Optional[subprocess.Popen] = None
        self.loopback_module_id = ""
        self.lock_fp = None
        self.capture_lock_fp = None
        self.font_file = self.pick_font_file()
        self.event_seq = 0
        self.last_event_id = ""
        self.event_writer = StreamEventWriter(event_log_file=self.cfg.event_log_file, run_id=self.run_id)
        self.active_encoder_profile: dict[str, object] = {}
        self.capture_helpers_force_restart_reason = ""

    def log(self, msg: str) -> None:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

    def next_event_id(self) -> str:
        event_id = self.event_writer.next_event_id()
        self.event_seq = self.event_writer.event_seq
        return event_id

    def append_event(self, event_type: str, **fields: object) -> str:
        self.event_writer.event_log_file = self.cfg.event_log_file
        self.event_writer.stream_key_hash = self.stream_key_hash
        self.event_writer.restart_count = self.restart_count
        self.event_writer.rtmp_url_masked = self.mask_rtmp_url()
        event_id = self.event_writer.append(event_type, **fields)
        self.event_seq = self.event_writer.event_seq
        self.last_event_id = self.event_writer.last_event_id
        return event_id

    def mask_rtmp_url(self) -> str:
        return ingest.mask_rtmp_url(self.cfg.rtmp_url)

    def write_runtime_snapshot(self, status: str, ffmpeg_pid: str = "", note: str = "") -> None:
        runtime_state.write_runtime_snapshot(
            self.cfg.runtime_state_file,
            run_id=self.run_id,
            stream_key_hash=self.stream_key_hash,
            rtmp_url_masked=self.mask_rtmp_url(),
            restart_count=self.restart_count,
            last_health_ok=self.last_health_ok,
            last_event_id=self.last_event_id,
            status=status,
            ffmpeg_pid=ffmpeg_pid,
            note=note,
        )

    def signal_handler(self, signum: int, _frame: object) -> None:
        self.stop_requested = True
        self.log(f"Stop signal received ({signum}).")
        self.append_event("signal", signal=signum, note="stop requested")
        self.stop_ffmpeg_for_shutdown(reason="stop signal", signum=signum)

    def stop_ffmpeg_for_shutdown(self, reason: str, signum: int = 0) -> bool:
        proc = self.ffmpeg_proc
        if not proc or proc.poll() is not None:
            return True
        return ffmpeg_lifecycle.stop_for_shutdown(
            proc,
            reason=reason,
            signum=signum,
            grace_sec=float(self.cfg.stop_ffmpeg_term_grace_sec),
            append_event=self.append_event,
            log=self.log,
        )

    def stop_ffmpeg(self) -> None:
        if self.ffmpeg_proc and self.ffmpeg_proc.poll() is None:
            ffmpeg_lifecycle.stop_quietly(self.ffmpeg_proc)

    def ensure_commands(self) -> None:
        preflight.ensure_commands()

    def assert_systemd_launch(self) -> None:
        preflight.assert_systemd_launch(self.cfg)

    def resolve_rtmp_url(self) -> None:
        self.cfg.rtmp_url = ingest.resolve_rtmp_url(self.cfg.rtmp_url, self.cfg.stream_key)

    def validate_rtmp_url(self) -> None:
        ingest.validate_rtmp_url(self.cfg.rtmp_url, self.cfg.stream_key)

    def configure_target_runtime_paths(self) -> None:
        target = target_runtime.resolve_target_runtime(self.cfg)
        self.stream_key_hash = target.stream_key_hash
        self.stream_lock_file = target.stream_lock_file
        self.takeover_coord_file = target.takeover_coord_file
        self.cfg.runtime_state_file = target.runtime_state_file

    def lock_holder_pid(self) -> Optional[int]:
        return engine_locks.lock_holder_pid(self.cfg.runtime_state_file)

    def try_acquire_lock(self, lock_path: Path) -> Optional[object]:
        return engine_locks.try_acquire_lock(lock_path)

    def acquire_takeover_coord_lock(self) -> object:
        coord_fp = self.try_acquire_lock(self.takeover_coord_file)
        if coord_fp is not None:
            return coord_fp

        coord_fp = self.takeover_coord_file.open("a+")
        deadline = time.monotonic() + max(1.0, float(self.cfg.takeover_grace_sec))
        try:
            while True:
                if self.stop_requested:
                    raise RuntimeError("Stop requested while waiting for takeover coordination lock")
                try:
                    fcntl.flock(coord_fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return coord_fp
                except BlockingIOError:
                    pass
                except InterruptedError:
                    if self.stop_requested:
                        raise RuntimeError("Stop requested while waiting for takeover coordination lock") from None
                now = time.monotonic()
                if now >= deadline:
                    raise RuntimeError(f"Takeover coordination lock wait timeout: {self.takeover_coord_file}")
                time.sleep(min(0.5, max(0.0, deadline - now)))
        except Exception:
            try:
                coord_fp.close()
            except Exception:
                pass
            raise

    def acquire_single_instance_lock(self) -> None:
        self.lock_fp = self.try_acquire_lock(self.stream_lock_file)
        if self.lock_fp:
            return
        if not self.cfg.takeover_enabled:
            raise RuntimeError(f"Another instance holds lock: {self.stream_lock_file}")

        coord_fp = self.acquire_takeover_coord_lock()
        try:
            if self.stop_requested:
                raise RuntimeError("Stop requested during takeover")
            self.lock_fp = self.try_acquire_lock(self.stream_lock_file)
            if self.lock_fp:
                return
            incumbent = self.lock_holder_pid()
            self.write_runtime_snapshot("takeover", "", f"request-stop incumbent={incumbent or '-'}")
            self.append_event("takeover_request", incumbent_pid=incumbent or 0, phase="request-stop")
            if incumbent and self.pid_alive(incumbent):
                self.log(f"Takeover requested. Signaling incumbent pid={incumbent}")
                os.kill(incumbent, signal.SIGTERM)
            deadline = time.monotonic() + self.cfg.takeover_grace_sec
            while time.monotonic() < deadline and not self.stop_requested:
                self.lock_fp = self.try_acquire_lock(self.stream_lock_file)
                if self.lock_fp:
                    self.write_runtime_snapshot("starting", "", "takeover lock acquired")
                    return
                time.sleep(1.0)
            if self.stop_requested:
                raise RuntimeError("Stop requested during takeover")
            if self.cfg.takeover_force_kill and incumbent and self.pid_alive(incumbent):
                self.write_runtime_snapshot("takeover", "", f"force-kill incumbent={incumbent}")
                self.append_event("takeover_force_kill", incumbent_pid=incumbent, phase="force-kill")
                self.log(f"Takeover grace expired. Force-killing incumbent pid={incumbent}")
                os.kill(incumbent, signal.SIGKILL)
                time.sleep(1.0)
                self.lock_fp = self.try_acquire_lock(self.stream_lock_file)
                if self.lock_fp:
                    self.write_runtime_snapshot("starting", "", "takeover lock acquired after force-kill")
                    return
            raise RuntimeError(f"Takeover timeout. Existing stream did not release lock: {self.stream_lock_file}")
        finally:
            try:
                coord_fp.close()
            except Exception:
                pass

    def acquire_capture_lock(self) -> None:
        self.capture_lock_file = engine_locks.display_capture_lock_path(self.cfg.stream_lock_dir, self.cfg.display_name)
        self.capture_lock_fp = self.try_acquire_lock(self.capture_lock_file)
        if self.capture_lock_fp is None:
            raise RuntimeError(f"Capture resources already in use: {self.capture_lock_file}")

    @staticmethod
    def pid_alive(pid: int) -> bool:
        return engine_locks.pid_alive(pid)

    def foreign_rtmp_pids(self) -> list[int]:
        return process_discovery.foreign_rtmp_pids(
            rtmp_url=self.cfg.rtmp_url,
            test_mode=self.cfg.test_mode,
            current_pid=os.getpid(),
            run_cmd=lambda cmd: run(cmd, check=False),
        )

    def cleanup_stale_rtmp_ffmpeg(self) -> None:
        for pid in self.foreign_rtmp_pids():
            self.log(f"Killing stale ffmpeg publisher pid={pid}")
            self.append_event("stale_ffmpeg_kill", pid=pid, signal="TERM")
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
        time.sleep(1.0)
        for pid in self.foreign_rtmp_pids():
            self.log(f"Force-killing stale ffmpeg publisher pid={pid}")
            self.append_event("stale_ffmpeg_kill", pid=pid, signal="KILL")
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass

    @staticmethod
    def parse_pgrep_output(stdout: str) -> list[tuple[int, str]]:
        return process_discovery.parse_pgrep_output(stdout)

    def pgrep_cmds(self, pattern: str) -> list[tuple[int, str]]:
        return process_discovery.pgrep_cmds(pattern, run_cmd=lambda cmd: run(cmd, check=False))

    def stale_capture_helper_pids(self) -> dict[str, list[int]]:
        return process_discovery.stale_capture_helper_pids(
            base_dir=self.cfg.base_dir,
            overlay_dir=self.cfg.overlay_dir,
            browser_profile_dir=self.cfg.browser_profile_dir,
            display_name=self.cfg.display_name,
            overlay_port=self.cfg.overlay_port,
            current_pid=os.getpid(),
            run_cmd=lambda cmd: run(cmd, check=False),
        )

    def terminate_stale_pids(self, label: str, pids: list[int]) -> None:
        process_discovery.terminate_stale_pids(
            label,
            pids,
            current_pid=os.getpid(),
            pid_alive=self.pid_alive,
            append_event=self.append_event,
            log=self.log,
            kill=os.kill,
            sleep=time.sleep,
        )

    def cleanup_stale_capture_helpers(self) -> None:
        # If a foreign RTMP publisher is still alive, the helpers may be serving
        # an active stream. Do not tear down capture resources in that case.
        active_publishers = self.foreign_rtmp_pids()
        if active_publishers:
            self.log(f"Skipping stale capture helper cleanup: active RTMP publisher(s) {active_publishers}")
            self.append_event("stale_capture_helper_cleanup_skipped", active_publishers=active_publishers)
            return

        stale = self.stale_capture_helper_pids()
        for label in ("browser", "overlay", "xvfb"):
            self.terminate_stale_pids(label, stale.get(label, []))

    def assert_rtmp_health_gate(self) -> None:
        if not self.cfg.health_gate_abort_on_foreign:
            return
        count = len(self.foreign_rtmp_pids())
        if count > 0:
            raise RuntimeError(f"Health gate blocked start: {count} foreign RTMP publisher(s) active.")

    def ensure_pulse_server(self) -> None:
        audio_boot.ensure_pulse_server(base_dir=self.cfg.base_dir, run_cmd=lambda cmd: run(cmd, check=False))

    def pick_font_file(self) -> str:
        return preflight.pick_font_file(self.cfg.font_file)

    def display_ready(self) -> bool:
        return rendering_boot.display_ready(self.cfg.display_name, run_cmd=lambda cmd: run(cmd, check=False))

    def ensure_x_display(self) -> None:
        proc = rendering_boot.start_x_display(self.cfg, run_cmd=lambda cmd: run(cmd, check=False))
        if proc is not None:
            self.xvfb_proc = proc

    def is_port_listening(self, host: str, port: int) -> bool:
        return rendering_boot.is_port_listening(host, port)

    def http_get_text(self, url: str, timeout_sec: float = 2.0) -> str:
        return rendering_boot.http_get_text(url, timeout_sec=timeout_sec)

    def overlay_http_ready_probe(self) -> tuple[bool, str]:
        return rendering_boot.overlay_http_ready_probe(self.cfg)

    def start_overlay_server(self) -> None:
        if not self.cfg.use_overlay_wrapper:
            return
        if self.is_port_listening("127.0.0.1", self.cfg.overlay_port):
            self.log(f"Overlay port already in use: {self.cfg.overlay_port}")
            return
        self.overlay_proc = rendering_boot.start_overlay_server(self.cfg)

    def process_alive(self, proc: Optional[subprocess.Popen]) -> bool:
        return proc is not None and proc.poll() is None

    @staticmethod
    def proc_status_memory_mib(pid: int) -> dict[str, float]:
        status: dict[str, float] = {}
        try:
            text = Path(f"/proc/{pid}/status").read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return status
        for line in text.splitlines():
            if ":" not in line:
                continue
            key, raw = line.split(":", 1)
            if key not in {"VmRSS", "RssAnon", "RssFile", "RssShmem"}:
                continue
            parts = raw.strip().split()
            if not parts:
                continue
            try:
                kib = float(parts[0])
            except ValueError:
                continue
            status[key] = kib / 1024.0
        return status

    def helper_memory_guard_action(self) -> ffmpeg_lifecycle.HeartbeatAction:
        if not self.cfg.capture_helper_memory_guard_enabled:
            return ffmpeg_lifecycle.HeartbeatAction()
        if not self.process_alive(self.xvfb_proc):
            return ffmpeg_lifecycle.HeartbeatAction()

        pid = int(self.xvfb_proc.pid)
        memory = self.proc_status_memory_mib(pid)
        rss_mib = float(memory.get("VmRSS", 0.0))
        shmem_mib = float(memory.get("RssShmem", 0.0))
        rss_limit = int(self.cfg.xvfb_memory_guard_rss_mib)
        shmem_limit = int(self.cfg.xvfb_memory_guard_shmem_mib)
        rss_exceeded = rss_limit > 0 and rss_mib >= rss_limit
        shmem_exceeded = shmem_limit > 0 and shmem_mib >= shmem_limit
        if not (rss_exceeded or shmem_exceeded):
            return ffmpeg_lifecycle.HeartbeatAction()

        reason = "xvfb memory guard"
        self.capture_helpers_force_restart_reason = reason
        self.log(
            "Xvfb memory guard triggered: "
            f"pid={pid} rss={rss_mib:.1f}MiB shmem={shmem_mib:.1f}MiB "
            f"limits rss={rss_limit}MiB shmem={shmem_limit}MiB"
        )
        self.append_event(
            "capture_helper_memory_guard_triggered",
            helper="xvfb",
            pid=pid,
            rss_mib=round(rss_mib, 3),
            shmem_mib=round(shmem_mib, 3),
            rss_limit_mib=rss_limit,
            shmem_limit_mib=shmem_limit,
        )
        return ffmpeg_lifecycle.HeartbeatAction(stop_reason=reason)

    def terminate_helper_proc(self, label: str, proc: Optional[subprocess.Popen], *, reason: str) -> None:
        if not self.process_alive(proc):
            return
        pid = int(proc.pid)
        self.append_event("capture_helper_stop_requested", helper=label, pid=pid, reason=reason)
        try:
            proc.terminate()
        except Exception as e:
            self.append_event("capture_helper_stop_error", helper=label, pid=pid, reason=reason, error=str(e))
            return
        try:
            rc = proc.wait(timeout=2.0)
            self.append_event("capture_helper_stopped", helper=label, pid=pid, reason=reason, exit_code=rc)
        except subprocess.TimeoutExpired:
            self.append_event("capture_helper_stop_timeout_kill", helper=label, pid=pid, reason=reason)
            try:
                proc.kill()
            except Exception as e:
                self.append_event("capture_helper_kill_error", helper=label, pid=pid, reason=reason, error=str(e))
                return
            try:
                rc = proc.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                rc = 0
            self.append_event("capture_helper_killed", helper=label, pid=pid, reason=reason, exit_code=rc)

    def restart_capture_stack_after_ffmpeg_stop(self, reason: str) -> bool:
        self.log(f"Restarting capture helpers after {reason}.")
        self.append_event("capture_stack_restart_requested", reason=reason)
        self.terminate_helper_proc("browser", self.browser_proc, reason=reason)
        self.browser_proc = None
        self.terminate_helper_proc("xvfb", self.xvfb_proc, reason=reason)
        self.xvfb_proc = None
        self.ensure_x_display()
        self.ensure_browser_running(force=True, reason=reason)
        self.append_event(
            "capture_stack_restarted",
            reason=reason,
            xvfb_pid=self.xvfb_proc.pid if self.xvfb_proc else None,
            browser_pid=self.browser_proc.pid if self.browser_proc else None,
        )
        return True

    def ensure_x_display_running(self) -> bool:
        if self.display_ready():
            return False
        self.log(f"X display disappeared; restarting helper display {self.cfg.display_name}.")
        self.append_event("capture_helper_restart_requested", helper="xvfb", reason="display_unavailable")
        self.ensure_x_display()
        self.append_event("capture_helper_restarted", helper="xvfb", pid=self.xvfb_proc.pid if self.xvfb_proc else None)
        return True

    def ensure_overlay_server_running(self) -> bool:
        if not self.cfg.use_overlay_wrapper:
            return False
        if self.process_alive(self.overlay_proc) and self.is_port_listening("127.0.0.1", self.cfg.overlay_port):
            return False
        if self.is_port_listening("127.0.0.1", self.cfg.overlay_port):
            self.append_event("capture_helper_adopted", helper="overlay", port=self.cfg.overlay_port)
            return False
        self.log(f"Overlay server disappeared; restarting port {self.cfg.overlay_port}.")
        self.append_event("capture_helper_restart_requested", helper="overlay", reason="port_unavailable", port=self.cfg.overlay_port)
        self.start_overlay_server()
        self.append_event("capture_helper_restarted", helper="overlay", pid=self.overlay_proc.pid if self.overlay_proc else None, port=self.cfg.overlay_port)
        return True

    def ensure_browser_running(self, *, force: bool = False, reason: str = "process_not_running") -> bool:
        if not self.cfg.auto_start_browser:
            return False
        if self.process_alive(self.browser_proc) and not force:
            return False
        if self.process_alive(self.browser_proc):
            self.log(f"Browser renderer will be restarted after {reason}.")
            self.browser_proc.terminate()
            try:
                self.browser_proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                self.browser_proc.kill()
        else:
            self.log("Browser renderer disappeared; restarting browser.")
        self.append_event("capture_helper_restart_requested", helper="browser", reason=reason)
        self.start_browser()
        self.append_event("capture_helper_restarted", helper="browser", pid=self.browser_proc.pid if self.browser_proc else None)
        return True

    def ensure_capture_helpers_running(self) -> list[str]:
        restarted: list[str] = []
        if self.capture_helpers_force_restart_reason:
            reason = self.capture_helpers_force_restart_reason
            self.capture_helpers_force_restart_reason = ""
            if self.restart_capture_stack_after_ffmpeg_stop(reason):
                restarted.extend(["xvfb", "browser"])
        x_display_restarted = self.ensure_x_display_running()
        if x_display_restarted:
            restarted.append("xvfb")
        if self.ensure_overlay_server_running():
            restarted.append("overlay")
        if self.ensure_browser_running(force=x_display_restarted, reason="display_restarted" if x_display_restarted else "process_not_running"):
            restarted.append("browser")
        if restarted:
            self.append_event("capture_helpers_recovered", helpers=restarted)
        return restarted

    def ensure_virtual_sink(self) -> None:
        audio_boot.ensure_virtual_sink(pulse_sink=self.cfg.pulse_sink, run_cmd=lambda cmd: run(cmd, check=False))

    def detect_pulse_monitor(self) -> str:
        return audio_boot.detect_pulse_monitor(
            pulse_source=self.cfg.pulse_source,
            pulse_sink=self.cfg.pulse_sink,
            run_cmd=lambda cmd: run(cmd, check=False),
        )

    def build_browser_url(self) -> str:
        return rendering_boot.build_browser_url(self.cfg)

    def resolve_browser_bin(self) -> Optional[str]:
        return rendering_boot.resolve_browser_bin(self.cfg.browser_bin)

    def start_browser(self) -> None:
        if not self.cfg.auto_start_browser:
            return
        settle_sec, settle_mode = self.effective_browser_settle_sec()
        self.browser_proc = rendering_boot.start_browser(
            self.cfg,
            settle_sec=0,
            url=self.build_browser_url(),
        )
        self.append_event("browser_started", settle_sec=settle_sec, settle_mode=settle_mode)
        if settle_sec > 0:
            time.sleep(settle_sec)

    def has_recent_restart_context(self) -> bool:
        return restart_context.has_recent_restart_context(self.cfg)

    def _restart_reason_payload(self) -> dict | None:
        return restart_context.restart_reason_payload(self.cfg.restart_reason_file)

    def _restart_reason_age_sec(self, payload: dict) -> float | None:
        return restart_context.restart_reason_age_sec(payload)

    def _restart_reason_is_recent(self, deadline: float) -> bool:
        return restart_context.restart_reason_is_recent(self.cfg.restart_reason_file, deadline=deadline)

    def emit_startup_restart_context(self) -> None:
        restart_context.emit_startup_restart_context(
            self.cfg,
            run_id=self.run_id,
            stream_pid=os.getpid(),
            append_event=self.append_event,
        )

    def effective_pre_ffmpeg_min_wait_sec(self) -> tuple[float, str]:
        if self.cfg.test_mode:
            return self.cfg.pre_ffmpeg_min_wait_sec_test, "test"
        if self.has_recent_restart_context():
            return self.cfg.pre_ffmpeg_min_wait_sec_restart, "restart"
        return self.cfg.pre_ffmpeg_min_wait_sec, "normal"

    def effective_browser_settle_sec(self) -> tuple[float, str]:
        if self.cfg.test_mode:
            return self.cfg.browser_start_settle_sec_test, "test"
        if self.has_recent_restart_context():
            return self.cfg.browser_start_settle_sec_restart, "restart"
        return self.cfg.browser_start_settle_sec, "normal"

    def emergency_low_upload_profile(self) -> dict[str, object] | None:
        return choose_emergency_low_upload_profile(self.cfg, self._restart_reason_payload())

    def effective_encoder_profile(self) -> dict[str, object]:
        return choose_effective_encoder_profile(self.cfg, self._restart_reason_payload())

    def encoder_profile_expired(self, profile: dict[str, object]) -> bool:
        return is_encoder_profile_expired(profile)

    def wait_for_render_ready(self) -> None:
        min_wait_sec, wait_mode = self.effective_pre_ffmpeg_min_wait_sec()
        min_wait_deadline = time.monotonic() + min_wait_sec
        overlay_deadline = time.monotonic() + self.cfg.pre_ffmpeg_overlay_ready_timeout_sec
        overlay_ready = False
        last_probe_reason = "probe not run"
        self.append_event(
            "pre_ffmpeg_wait_start",
            wait_mode=wait_mode,
            min_wait_sec=min_wait_sec,
            overlay_timeout_sec=self.cfg.pre_ffmpeg_overlay_ready_timeout_sec,
            require_overlay_ready=self.cfg.pre_ffmpeg_require_overlay_ready,
        )

        while not self.stop_requested:
            now = time.monotonic()
            if not overlay_ready and now <= overlay_deadline:
                overlay_ready, last_probe_reason = self.overlay_http_ready_probe()
                if overlay_ready:
                    self.log(f"Overlay ready probe passed: {last_probe_reason}")

            if now >= min_wait_deadline:
                break
            time.sleep(self.cfg.pre_ffmpeg_overlay_ready_poll_sec)

        if self.cfg.pre_ffmpeg_require_overlay_ready and not overlay_ready:
            raise RuntimeError(f"Overlay did not become ready before ffmpeg start: {last_probe_reason}")
        if not overlay_ready:
            self.log(f"Overlay ready probe did not pass before ffmpeg start (fail-open): {last_probe_reason}")

    def ensure_local_audio_monitor(self) -> None:
        self.loopback_module_id = audio_boot.ensure_local_audio_monitor(
            enabled=self.cfg.local_monitor_audio,
            monitor_sink=self.cfg.monitor_sink,
            pulse_sink=self.cfg.pulse_sink,
            latency_msec=self.cfg.monitor_loopback_latency_msec,
            run_cmd=lambda cmd: run(cmd, check=False),
            log=self.log,
        )

    def build_display_input(self) -> str:
        if self.cfg.display_input:
            return self.cfg.display_input
        if "+" in self.cfg.display_name:
            return self.cfg.display_name
        if "." in self.cfg.display_name:
            return f"{self.cfg.display_name}{self.cfg.display_offset}"
        return f"{self.cfg.display_name}.0{self.cfg.display_offset}"

    def build_output_args(self) -> list[str]:
        return build_ffmpeg_output_args(self.cfg)

    def build_filter(self) -> str:
        return build_video_filter(self.cfg.output_size)

    def ffmpeg_args(
        self,
        x11_input: str,
        pulse_source: str,
        encoder_profile: dict[str, object] | None = None,
    ) -> list[str]:
        profile = encoder_profile or self.effective_encoder_profile()
        return build_ffmpeg_args(self.cfg, x11_input=x11_input, pulse_source=pulse_source, encoder_profile=profile)

    def ffmpeg_heartbeat_action(self, encoder_profile: dict[str, object]) -> ffmpeg_lifecycle.HeartbeatAction:
        ffmpeg_pid = str(self.ffmpeg_proc.pid) if self.ffmpeg_proc else ""
        self.last_health_ok = True
        self.write_runtime_snapshot("running", ffmpeg_pid, "ffmpeg heartbeat")
        memory_action = self.helper_memory_guard_action()
        if memory_action.should_stop:
            return memory_action
        restarted_helpers = self.ensure_capture_helpers_running()
        if "xvfb" in restarted_helpers:
            return ffmpeg_lifecycle.HeartbeatAction(stop_reason="capture display restarted")
        if self.encoder_profile_expired(encoder_profile):
            self.log("Emergency low-upload profile expired; restarting ffmpeg with normal encoder profile.")
            self.append_event(
                "encoder_profile_restore_requested",
                ffmpeg_pid=self.ffmpeg_proc.pid if self.ffmpeg_proc else 0,
                encoder_profile=encoder_profile,
            )
            return ffmpeg_lifecycle.HeartbeatAction(stop_reason="emergency low-upload profile expired")
        return ffmpeg_lifecycle.HeartbeatAction()

    def cleanup(self) -> None:
        self.write_runtime_snapshot("stopped", "", "cleanup")
        self.append_event("engine_cleanup", note="cleanup called")
        if self.loopback_module_id:
            run(["pactl", "unload-module", self.loopback_module_id], check=False)
        for proc in (self.ffmpeg_proc, self.browser_proc, self.overlay_proc, self.xvfb_proc):
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
        for fp in (self.capture_lock_fp, self.lock_fp):
            try:
                if fp:
                    fp.close()
            except Exception:
                pass

    def run(self) -> int:
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        os.environ["PULSE_SHM"] = self.cfg.pulse_shm

        self.ensure_commands()
        self.assert_systemd_launch()
        if not self.cfg.test_mode:
            self.resolve_rtmp_url()
            self.validate_rtmp_url()
        self.configure_target_runtime_paths()
        self.append_event("engine_start", test_mode=self.cfg.test_mode)
        preflight.prepare_runtime_paths(self.cfg)

        self.ensure_pulse_server()
        self.acquire_single_instance_lock()
        self.acquire_capture_lock()
        self.cleanup_stale_rtmp_ffmpeg()
        self.cleanup_stale_capture_helpers()
        self.assert_rtmp_health_gate()
        self.write_runtime_snapshot("starting", "", "preflight passed")
        self.ensure_x_display()
        self.start_overlay_server()
        self.ensure_virtual_sink()
        pulse_source = self.detect_pulse_monitor()
        self.ensure_local_audio_monitor()
        self.start_browser()
        self.wait_for_render_ready()

        self.emit_startup_restart_context()

        x11_input = self.build_display_input()
        self.log(f"Display      : {self.cfg.display_name}")
        self.log(f"X11 input    : {x11_input}")
        self.log(f"Capture size : {self.cfg.video_size} @ {self.cfg.frame_rate}fps")
        self.log(f"Output size  : {self.cfg.output_size}")
        self.log(f"Pulse sink   : {self.cfg.pulse_sink or '<default>'}")
        self.log(f"Audio source : {pulse_source}")
        self.log(f"Overlay mode : {1 if self.cfg.use_overlay_wrapper else 0}")
        self.log(f"Map URL      : {self.cfg.stream1090_url}")
        self.log(f"Map center   : lat={self.cfg.map_lat} lon={self.cfg.map_lon} zoom={self.cfg.map_zoom}")
        self.log(f"Map scales   : scale={self.cfg.map_scale} iconScale={self.cfg.map_icon_scale} labelScale={self.cfg.map_label_scale} largeMode={self.cfg.map_large_mode}")
        self.log(f"Browser URL  : {self.build_browser_url()}")
        min_wait_sec, min_wait_mode = self.effective_pre_ffmpeg_min_wait_sec()
        self.log(
            "Pre-FFmpeg   : "
            f"min_wait={min_wait_sec:.1f}s "
            f"(mode={min_wait_mode}) "
            f"overlay_timeout={self.cfg.pre_ffmpeg_overlay_ready_timeout_sec:.1f}s "
            f"require_overlay_ready={int(self.cfg.pre_ffmpeg_require_overlay_ready)}"
        )
        self.log(f"Font file    : {self.font_file}")
        if self.cfg.test_mode:
            self.log("Mode         : TEST")
        else:
            self.log(f"RTMP URL     : {self.mask_rtmp_url()}")

        while not self.stop_requested:
            self.assert_rtmp_health_gate()
            self.ensure_capture_helpers_running()
            self.write_runtime_snapshot("running", "", "starting ffmpeg process")
            encoder_profile = self.effective_encoder_profile()
            self.active_encoder_profile = encoder_profile
            self.log(
                "Encoder      : "
                f"profile={encoder_profile.get('name')} "
                f"video={encoder_profile.get('video_bitrate')} "
                f"maxrate={encoder_profile.get('video_maxrate')} "
                f"bufsize={encoder_profile.get('video_bufsize')} "
                f"audio={encoder_profile.get('audio_bitrate')}"
            )
            self.log("Starting ffmpeg stream process...")
            self.append_event("ffmpeg_starting", encoder_profile=encoder_profile)
            args = self.ffmpeg_args(x11_input, pulse_source, encoder_profile=encoder_profile)
            self.ffmpeg_proc = subprocess.Popen(args)
            self.write_runtime_snapshot("running", str(self.ffmpeg_proc.pid), "ffmpeg started")
            self.append_event("ffmpeg_started", ffmpeg_pid=self.ffmpeg_proc.pid, encoder_profile=encoder_profile)
            rc = ffmpeg_lifecycle.wait_until_exit_or_action(
                self.ffmpeg_proc,
                heartbeat_sec=self.cfg.runtime_heartbeat_sec,
                heartbeat_action=lambda: self.ffmpeg_heartbeat_action(encoder_profile),
                stop_process=self.stop_ffmpeg_for_shutdown,
            )
            self.ffmpeg_proc = None
            self.write_runtime_snapshot("running", "", "ffmpeg exited")
            self.append_event("ffmpeg_exited", exit_code=rc)
            if self.stop_requested:
                self.write_runtime_snapshot("stopping", "", "stop requested")
                self.append_event("engine_stopping", note="stop requested")
                break
            self.restart_count += 1
            self.last_health_ok = False
            self.write_runtime_snapshot("restarting", "", f"ffmpeg exited code={rc}")
            self.log(f"ffmpeg exited with code {rc}. Restarting in {self.cfg.restart_delay_sec}s...")
            self.append_event("ffmpeg_restart_scheduled", exit_code=rc, delay_sec=self.cfg.restart_delay_sec)
            time.sleep(self.cfg.restart_delay_sec)

        self.log("stream engine stopped.")
        return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Python stream engine for stream-new.")
    parser.add_argument("--print-config", action="store_true", help="Print resolved config and exit.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config()
    if args.print_config:
        print(json.dumps(cfg.__dict__, ensure_ascii=False, indent=2, default=str))
        return 0
    engine = StreamEngine(cfg)
    try:
        return engine.run()
    finally:
        engine.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
