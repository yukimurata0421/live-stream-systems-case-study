#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import queue
import signal
import statistics
import subprocess
import threading
import time
from dataclasses import dataclass, asdict
from pathlib import Path

ROOT = Path('/home/yuki/projects/stream_v2')
ENGINE = ROOT / 'src' / 'stream_core' / 'stream_engine.py'
PYTHON = ROOT / 'venv' / 'bin' / 'python3'
OUT_DIR = ROOT / 'runtime' / 'bench_pre_ffmpeg_wait'


@dataclass
class Case:
    name: str
    env: dict[str, str]


@dataclass
class RunResult:
    case: str
    iteration: int
    ok: bool
    start_to_overlay_ready_sec: float | None
    start_to_ffmpeg_start_sec: float | None
    ffmpeg_survived_sec: float | None
    exit_code: int | None
    note: str


def reader_thread(pipe, q: queue.Queue[tuple[float, str]]) -> None:
    for line in pipe:
        q.put((time.monotonic(), line.rstrip('\n')))


def run_case(case: Case, iteration: int, base_port: int) -> RunResult:
    run_dir = OUT_DIR / case.name / f'run_{iteration:02d}'
    run_dir.mkdir(parents=True, exist_ok=True)

    port = base_port + iteration
    env = os.environ.copy()
    env.update(
        {
            'BASE_DIR': str(ROOT),
            'REQUIRE_SYSTEMD_LAUNCH': '0',
            'ALLOW_DIRECT_STREAM_SH': '1',
            'TEST_MODE': '1',
            'TEST_OUTPUT': 'null',
            'DISPLAY': ':101',
            'DISPLAY_NAME': ':101',
            'AUTO_START_XVFB': '1',
            'AUTO_START_BROWSER': '1',
            'USE_OVERLAY_WRAPPER': '1',
            'OVERLAY_PORT': str(port),
            'STREAM_RUNTIME_STATE_DIR': str(run_dir / 'state'),
            'RUNTIME_STATE_FILE': str(run_dir / 'state' / 'stream_runtime_state.json'),
            'EVENT_LOG_FILE': str(run_dir / 'logs' / 'stream_engine_events.jsonl'),
            'RESTART_REASON_FILE': str(run_dir / 'state' / 'restart_reason.json'),
            'OVERLAY_SERVER_LOG_FILE': str(run_dir / 'logs' / 'overlay_server.log'),
            'BROWSER_LOG_FILE': str(run_dir / 'logs' / 'browser.log'),
            'XVFB_LOG_FILE': str(run_dir / 'logs' / 'xvfb.log'),
        }
    )
    env.update(case.env)

    cmd = [str(PYTHON), str(ENGINE)]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )
    assert proc.stdout is not None
    q: queue.Queue[tuple[float, str]] = queue.Queue()
    t = threading.Thread(target=reader_thread, args=(proc.stdout, q), daemon=True)
    t.start()

    started_at = time.monotonic()
    overlay_ready_at: float | None = None
    ffmpeg_start_at: float | None = None
    ffmpeg_survive_to: float | None = None
    note = ''

    try:
        deadline = started_at + 90
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                note = f'exited_before_ffmpeg rc={proc.returncode}'
                break
            try:
                ts, line = q.get(timeout=0.2)
            except queue.Empty:
                continue
            if 'Overlay ready probe passed:' in line and overlay_ready_at is None:
                overlay_ready_at = ts
            if 'Starting ffmpeg stream process...' in line:
                ffmpeg_start_at = ts
                ffmpeg_survive_to = ts + 6.0
                break

        if ffmpeg_start_at is None:
            # graceful stop and fail
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    proc.kill()
            return RunResult(
                case=case.name,
                iteration=iteration,
                ok=False,
                start_to_overlay_ready_sec=(overlay_ready_at - started_at) if overlay_ready_at else None,
                start_to_ffmpeg_start_sec=None,
                ffmpeg_survived_sec=None,
                exit_code=proc.returncode,
                note=note or 'ffmpeg_not_started_within_timeout',
            )

        # hold a bit to detect immediate crash
        while time.monotonic() < (ffmpeg_survive_to or 0):
            if proc.poll() is not None:
                note = f'ffmpeg_did_not_survive rc={proc.returncode}'
                break
            time.sleep(0.2)

        ok = proc.poll() is None

        # stop after observation
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()

        return RunResult(
            case=case.name,
            iteration=iteration,
            ok=ok,
            start_to_overlay_ready_sec=(overlay_ready_at - started_at) if overlay_ready_at else None,
            start_to_ffmpeg_start_sec=(ffmpeg_start_at - started_at) if ffmpeg_start_at else None,
            ffmpeg_survived_sec=6.0 if ok else None,
            exit_code=proc.returncode,
            note=note or ('ok' if ok else 'unknown_failure'),
        )
    finally:
        log_path = run_dir / 'logs' / 'engine_stdout.log'
        log_path.parent.mkdir(parents=True, exist_ok=True)
        # drain queue snapshot
        lines: list[str] = []
        while True:
            try:
                _ts, line = q.get_nowait()
                lines.append(line)
            except queue.Empty:
                break
        if lines:
            log_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def summarize(results: list[RunResult]) -> dict:
    by_case: dict[str, dict] = {}
    for r in results:
        by_case.setdefault(r.case, {'runs': []})['runs'].append(asdict(r))
    for case, payload in by_case.items():
        runs = payload['runs']
        ok_runs = [x for x in runs if x['ok']]
        ff = [x['start_to_ffmpeg_start_sec'] for x in ok_runs if x['start_to_ffmpeg_start_sec'] is not None]
        ov = [x['start_to_overlay_ready_sec'] for x in ok_runs if x['start_to_overlay_ready_sec'] is not None]
        payload['ok_count'] = len(ok_runs)
        payload['total'] = len(runs)
        if ff:
            payload['ffmpeg_start_sec'] = {
                'min': min(ff),
                'p50': statistics.median(ff),
                'max': max(ff),
                'mean': statistics.fmean(ff),
            }
        if ov:
            payload['overlay_ready_sec'] = {
                'min': min(ov),
                'p50': statistics.median(ov),
                'max': max(ov),
                'mean': statistics.fmean(ov),
            }
    return by_case


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cases = [
        Case(
            name='strict_ready_zero_wait',
            env={
                'RESET_BROWSER_PROFILE': '1',
                'BROWSER_START_SETTLE_SEC': '0',
                'BROWSER_START_SETTLE_SEC_TEST': '0',
                'PRE_FFMPEG_MIN_WAIT_SEC': '0',
                'PRE_FFMPEG_MIN_WAIT_SEC_TEST': '0',
                'PRE_FFMPEG_REQUIRE_OVERLAY_READY': '1',
                'PRE_FFMPEG_OVERLAY_READY_TIMEOUT_SEC': '20',
            },
        ),
        Case(
            name='fail_open_zero_wait',
            env={
                'RESET_BROWSER_PROFILE': '1',
                'BROWSER_START_SETTLE_SEC': '0',
                'BROWSER_START_SETTLE_SEC_TEST': '0',
                'PRE_FFMPEG_MIN_WAIT_SEC': '0',
                'PRE_FFMPEG_MIN_WAIT_SEC_TEST': '0',
                'PRE_FFMPEG_REQUIRE_OVERLAY_READY': '0',
                'PRE_FFMPEG_OVERLAY_READY_TIMEOUT_SEC': '20',
            },
        ),
        Case(
            name='fail_open_wait4',
            env={
                'RESET_BROWSER_PROFILE': '1',
                'BROWSER_START_SETTLE_SEC': '0',
                'BROWSER_START_SETTLE_SEC_TEST': '0',
                'PRE_FFMPEG_MIN_WAIT_SEC': '4',
                'PRE_FFMPEG_MIN_WAIT_SEC_TEST': '4',
                'PRE_FFMPEG_REQUIRE_OVERLAY_READY': '0',
                'PRE_FFMPEG_OVERLAY_READY_TIMEOUT_SEC': '20',
            },
        ),
    ]

    all_results: list[RunResult] = []
    for ci, case in enumerate(cases):
        for i in range(1, 7):
            res = run_case(case, i, base_port=19000 + ci * 100)
            print(f"[{case.name} #{i}] ok={res.ok} ffmpeg_start={res.start_to_ffmpeg_start_sec} overlay={res.start_to_overlay_ready_sec} note={res.note}")
            all_results.append(res)
            time.sleep(0.5)

    summary = summarize(all_results)
    ts = time.strftime('%Y%m%d_%H%M%S', time.gmtime())
    out = OUT_DIR / f'summary_{ts}.json'
    out.write_text(json.dumps({'generated_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'summary': summary}, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'Wrote: {out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
