from __future__ import annotations

import select
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

RECOVERY_CONTROL_ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_CONTAINER = RECOVERY_CONTROL_ROOT.parent
STREAM_V3_ROOT = REPOSITORY_CONTAINER if (REPOSITORY_CONTAINER / "src" / "stream_v3").is_dir() else REPOSITORY_CONTAINER / "stream_v3"
STREAM_V3_SRC = STREAM_V3_ROOT / "src"
if str(STREAM_V3_SRC) not in sys.path:
    sys.path.insert(0, str(STREAM_V3_SRC))

import stream_core.runtime_boundary_entrypoint as runtime_entrypoint  # noqa: E402
from stream_core.runtime_boundary_entrypoint import RuntimeBoundaryStreamEngine  # noqa: E402

from fast_recovery_controller import ack_delivery  # noqa: E402
from runtime_boundary import OutcomeUnknown  # noqa: E402


class StubbornProcess:
    def __init__(self, pid: int = 4200) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self.terminate_count = 0
        self.kill_count = 0

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminate_count += 1

    def kill(self) -> None:
        self.kill_count += 1
        self.returncode = -signal.SIGKILL

    def wait(self, timeout: float | None = None) -> int:
        if self.returncode is None:
            raise subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=timeout)
        return self.returncode


def request(*, generation: str = "run-1:3:4200") -> SimpleNamespace:
    return SimpleNamespace(
        intent_type="RESTART_FFMPEG",
        producer_id="independent-controller",
        reason="confirmed tcp stall",
        request_id="request-1",
        idempotency_key="request-1",
        expected_ffmpeg_generation=generation,
        target_identity={
            "host_id": "dell-yuki",
            "host_boot_id": "boot-1",
            "namespace": "stream-v3",
            "pod_uid": "pod-1",
            "container_name": "stream-engine",
            "container_id": "containerd://container-1",
            "ffmpeg_generation": "protocol-generation-1",
            "ffmpeg_pid": 437698,
        },
    )


def engine_with(proc: StubbornProcess | subprocess.Popen[str]) -> RuntimeBoundaryStreamEngine:
    engine = object.__new__(RuntimeBoundaryStreamEngine)
    engine.ffmpeg_proc = proc
    engine.run_id = "run-1"
    engine.restart_count = 3
    engine.ffmpeg_stop_context = {}
    return engine


@contextmanager
def owned_idle_child(*, ignore_term: bool) -> Iterator[subprocess.Popen[str]]:
    """Only test-created idle children: no production PID, socket, or media workload."""
    source = (
        "import signal, time\n"
        + ("signal.signal(signal.SIGTERM, signal.SIG_IGN)\n" if ignore_term else "")
        + "print('ready', flush=True)\ntime.sleep(60)\n"
    )
    proc = subprocess.Popen([sys.executable, "-c", source], stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout is not None
        assert select.select([proc.stdout], [], [], 5)[0], "owned child readiness timeout"
        assert proc.stdout.readline().strip() == "ready"
        yield proc
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=5)
        if proc.stdout is not None:
            proc.stdout.close()


@pytest.mark.parametrize("ignore_term", [False, True], ids=["term-exit", "bounded-kill"])
def test_real_pidfd_termination_only_affects_exact_owned_child(monkeypatch: pytest.MonkeyPatch, ignore_term: bool) -> None:
    monkeypatch.setenv("FR_FFMPEG_FORCE_KILL_ENABLED", "1")
    monkeypatch.setenv("FR_FFMPEG_TERM_GRACE_SEC", "2")
    monkeypatch.setenv("FR_FFMPEG_KILL_WAIT_SEC", "1")
    with owned_idle_child(ignore_term=ignore_term) as proc, owned_idle_child(ignore_term=True) as sentinel:
        engine = engine_with(proc)
        started = time.monotonic()
        result = engine.perform_fast_recovery_effect(request(generation=f"run-1:3:{proc.pid}"))
        elapsed = time.monotonic() - started
        expected_signal = signal.SIGKILL if ignore_term else signal.SIGTERM
        assert proc.poll() == -expected_signal
        assert result["exit_observed"] is True
        assert result["exit_code"] == -expected_signal
        assert result["pidfd_used"] is True
        assert result["physical_effect_count"] == 1
        assert result["signal_attempt_count"] == (2 if ignore_term else 1)
        assert result["effect"] == ("SIGTERM_THEN_SIGKILL" if ignore_term else "SIGTERM")
        assert sentinel.poll() is None, "unrelated owned sentinel must remain alive"
        assert elapsed < 3.5
        if ignore_term:
            assert elapsed >= 2
        print(f"real_pidfd ignore_term={ignore_term} elapsed_sec={elapsed:.6f} unrelated_child_alive=True")


def test_stubborn_exact_child_is_killed_after_term_deadline_with_one_logical_effect() -> None:
    proc = StubbornProcess()
    engine = engine_with(proc)
    sent: list[tuple[int, int]] = []

    def pidfd_send_signal(pidfd: int, signum: int, _siginfo=None, _flags: int = 0) -> None:
        sent.append((pidfd, signum))
        if signum == signal.SIGKILL:
            proc.returncode = -signal.SIGKILL

    with (
        mock.patch.dict(
            "os.environ",
            {
                "FR_FFMPEG_FORCE_KILL_ENABLED": "1",
                "FR_FFMPEG_TERM_GRACE_SEC": "0.01",
                "FR_FFMPEG_KILL_WAIT_SEC": "0.01",
            },
            clear=False,
        ),
        mock.patch.object(runtime_entrypoint.os, "pidfd_open", return_value=17),
        mock.patch.object(runtime_entrypoint.os, "close"),
        mock.patch.object(
            runtime_entrypoint,
            "signal",
            SimpleNamespace(
                SIGTERM=signal.SIGTERM,
                SIGKILL=signal.SIGKILL,
                pidfd_send_signal=mock.Mock(side_effect=pidfd_send_signal),
            ),
            create=True,
        ),
        mock.patch("stream_core.runtime_boundary_entrypoint.time.sleep", return_value=None),
        mock.patch(
            "stream_core.runtime_boundary_entrypoint.time.monotonic",
            side_effect=[0.0, 0.0, 2.1, 2.2, 2.3],
        ),
    ):
        result = engine.perform_fast_recovery_effect(request())

    assert sent == [(17, signal.SIGTERM), (17, signal.SIGKILL)]
    assert result["physical_effect_count"] == 1
    assert result["effect"] == "SIGTERM_THEN_SIGKILL"
    assert result["signal_attempt_count"] == 2
    assert result["exit_observed"] is True
    assert result["exit_code"] == -signal.SIGKILL
    assert result["pidfd_used"] is True
    assert engine.ffmpeg_stop_context["recovery_action_id"] == "request-1"
    assert engine.ffmpeg_stop_context["signal_attempt_count"] == 2


def test_generation_drift_before_kill_fails_closed_without_second_signal() -> None:
    proc = StubbornProcess()
    engine = engine_with(proc)
    generations = iter(["run-1:3:4200", "run-1:4:4200"])
    engine.ffmpeg_generation = lambda _pid: next(generations)  # type: ignore[method-assign]
    sent: list[int] = []

    with (
        mock.patch.dict(
            "os.environ",
            {
                "FR_FFMPEG_FORCE_KILL_ENABLED": "1",
                "FR_FFMPEG_TERM_GRACE_SEC": "0.01",
                "FR_FFMPEG_KILL_WAIT_SEC": "0.01",
            },
            clear=False,
        ),
        mock.patch.object(runtime_entrypoint.os, "pidfd_open", return_value=19),
        mock.patch.object(runtime_entrypoint.os, "close"),
        mock.patch.object(
            runtime_entrypoint,
            "signal",
            SimpleNamespace(
                SIGTERM=signal.SIGTERM,
                SIGKILL=signal.SIGKILL,
                pidfd_send_signal=mock.Mock(side_effect=lambda _fd, sig, *_args: sent.append(sig)),
            ),
            create=True,
        ),
        mock.patch("stream_core.runtime_boundary_entrypoint.time.sleep", return_value=None),
        mock.patch(
            "stream_core.runtime_boundary_entrypoint.time.monotonic",
            side_effect=[0.0, 0.0, 2.1, 2.2],
        ),
        pytest.raises(OutcomeUnknown, match="FFMPEG_GENERATION_DRIFT_BEFORE_SIGKILL"),
    ):
        engine.perform_fast_recovery_effect(request())

    assert sent == [signal.SIGTERM]


def test_force_kill_requires_explicit_runtime_gate() -> None:
    proc = StubbornProcess()
    engine = engine_with(proc)
    with (
        mock.patch.dict(
            "os.environ",
            {
                "FR_FFMPEG_FORCE_KILL_ENABLED": "0",
                "FR_FFMPEG_TERM_GRACE_SEC": "0.01",
            },
            clear=False,
        ),
        mock.patch.object(runtime_entrypoint.os, "pidfd_open", None),
        pytest.raises(OutcomeUnknown, match="SIGTERM_SENT_EXIT_NOT_OBSERVED"),
    ):
        engine.perform_fast_recovery_effect(request())

    assert proc.kill_count == 0


def test_force_kill_fails_closed_when_pidfd_is_unavailable() -> None:
    proc = StubbornProcess()
    engine = engine_with(proc)
    with (
        mock.patch.dict(
            "os.environ",
            {
                "FR_FFMPEG_FORCE_KILL_ENABLED": "1",
                "FR_FFMPEG_TERM_GRACE_SEC": "0.01",
            },
            clear=False,
        ),
        mock.patch.object(runtime_entrypoint.os, "pidfd_open", None),
        pytest.raises(OutcomeUnknown, match="SIGKILL_REQUIRES_PIDFD") as captured,
    ):
        engine.perform_fast_recovery_effect(request())

    assert captured.value.result["signal_attempt_count"] == 1
    assert captured.value.result["pidfd_used"] is False
    assert proc.terminate_count == 1
    assert proc.kill_count == 0


def test_measurement_derived_candidate_reaches_one_bounded_exact_child_effect() -> None:
    start = datetime(2026, 9, 3, tzinfo=UTC)
    state: dict[str, Any] = {}

    def observation(seconds: int, sequence: int, bytes_acked: int, *, pressure: bool = False) -> dict[str, Any]:
        return {
            "schema_version": "runtime.ffmpeg_observation.v1",
            "observed_at": (start + timedelta(seconds=seconds)).isoformat(),
            "producer_instance_id": "isolated-producer",
            "sequence": sequence,
            "protocol_ffmpeg_pid": 4200,
            "ffmpeg_generation": "native-generation",
            "target_identity": {
                "host_id": "dell-yuki",
                "host_boot_id": "boot-1",
                "namespace": "stream-v3",
                "pod_uid": "pod-1",
                "container_name": "stream-engine",
                "container_id": "containerd://container-1",
                "ffmpeg_generation": "protocol-generation-1",
                "ffmpeg_pid": 4200,
            },
            "tcp_metrics": {
                "bytes_acked": bytes_acked,
                "send_q": 600_000 if pressure else 0,
                "notsent": 600_000 if pressure else 0,
                "unacked": 100,
                "lastsnd_ms": 2_000 if pressure else 5,
                "rto_ms": 250,
            },
        }

    profile = {
        "video_bitrate": "3400k",
        "video_maxrate": "3400k",
        "video_bufsize": "6800k",
        "audio_bitrate": "192k",
    }
    normal_step = round(4.8 * 60 * 1_000_000 / 8)
    acked = 1_000_000
    report: dict[str, Any] = {}
    for minute in range(6):
        seconds = minute * 60
        report = ack_delivery.observe_ack_delivery(
            state,
            now_ts=int(start.timestamp()) + seconds,
            runtime_observation=observation(seconds, minute + 1, acked),
            stream_profile=profile,
            measurement_enabled=True,
            action_enabled=False,
            restart_threshold_mbps=None,
            pending_effect=False,
            history_sec=300,
            history_sample_sec=60,
            minimum_healthy_samples=4,
        )
        acked += normal_step
    assert report["statistics"]["baseline_ready"] is True
    measured_threshold = float(report["statistics"]["healthy"]["median_mbps"]) * 0.1
    acked -= normal_step

    low_step = round(0.1 * 10 * 1_000_000 / 8)
    for offset in range(10, 71, 10):
        acked += low_step
        report = ack_delivery.observe_ack_delivery(
            state,
            now_ts=int(start.timestamp()) + 300 + offset,
            runtime_observation=observation(300 + offset, 6 + offset // 10, acked, pressure=True),
            stream_profile=profile,
            measurement_enabled=True,
            action_enabled=True,
            restart_threshold_mbps=measured_threshold,
            pending_effect=False,
            history_sec=300,
            history_sample_sec=60,
            minimum_healthy_samples=4,
        )
    assert report["restart_candidate_confirmed"] is True

    proc = StubbornProcess()
    engine = engine_with(proc)
    sent: list[int] = []

    def pidfd_send_signal(_pidfd: int, signum: int, _siginfo=None, _flags: int = 0) -> None:
        sent.append(signum)
        if signum == signal.SIGKILL:
            proc.returncode = -signal.SIGKILL

    with (
        mock.patch.dict(
            "os.environ",
            {
                "FR_FFMPEG_FORCE_KILL_ENABLED": "1",
                "FR_FFMPEG_TERM_GRACE_SEC": "0.01",
                "FR_FFMPEG_KILL_WAIT_SEC": "0.01",
            },
            clear=False,
        ),
        mock.patch.object(runtime_entrypoint.os, "pidfd_open", return_value=23),
        mock.patch.object(runtime_entrypoint.os, "close"),
        mock.patch.object(
            runtime_entrypoint,
            "signal",
            SimpleNamespace(
                SIGTERM=signal.SIGTERM,
                SIGKILL=signal.SIGKILL,
                pidfd_send_signal=mock.Mock(side_effect=pidfd_send_signal),
            ),
            create=True,
        ),
        mock.patch("stream_core.runtime_boundary_entrypoint.time.sleep", return_value=None),
        mock.patch(
            "stream_core.runtime_boundary_entrypoint.time.monotonic",
            side_effect=[0.0, 0.0, 2.1, 2.2, 2.3],
        ),
    ):
        result = engine.perform_fast_recovery_effect(request())

    assert sent == [signal.SIGTERM, signal.SIGKILL]
    assert result["physical_effect_count"] == 1
    assert result["exit_observed"] is True
