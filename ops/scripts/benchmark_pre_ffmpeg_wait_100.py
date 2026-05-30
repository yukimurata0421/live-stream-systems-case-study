#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import queue
import signal
import statistics
import subprocess
import threading
import time
from pathlib import Path

ROOT = Path('/home/yuki/projects/stream_v2')
ENGINE = ROOT / 'src' / 'stream_core' / 'stream_engine.py'
PYTHON = ROOT / 'venv' / 'bin' / 'python3'
OUT_DIR = ROOT / 'runtime' / 'bench_pre_ffmpeg_wait'


def reader_thread(pipe, q: queue.Queue[tuple[float, str]]) -> None:
    for line in pipe:
        q.put((time.monotonic(), line.rstrip('\n')))


def quantile(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    xs = sorted(xs)
    if len(xs) == 1:
        return xs[0]
    pos = (len(xs) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return xs[lo]
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def run_once(i: int, base_port: int) -> dict:
    run_dir = OUT_DIR / 'p99_100' / f'run_{i:03d}'
    run_dir.mkdir(parents=True, exist_ok=True)
    port = base_port + i

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
            # Current operational intent for restart/test: no fixed min wait.
            'BROWSER_START_SETTLE_SEC': '0',
            'BROWSER_START_SETTLE_SEC_TEST': '0',
            'PRE_FFMPEG_MIN_WAIT_SEC': '0',
            'PRE_FFMPEG_MIN_WAIT_SEC_TEST': '0',
            'PRE_FFMPEG_REQUIRE_OVERLAY_READY': '0',
            'PRE_FFMPEG_OVERLAY_READY_TIMEOUT_SEC': '20',
            'STREAM_RUNTIME_STATE_DIR': str(run_dir / 'state'),
            'RUNTIME_STATE_FILE': str(run_dir / 'state' / 'stream_runtime_state.json'),
            'EVENT_LOG_FILE': str(run_dir / 'logs' / 'stream_engine_events.jsonl'),
            'RESTART_REASON_FILE': str(run_dir / 'state' / 'restart_reason.json'),
            'OVERLAY_SERVER_LOG_FILE': str(run_dir / 'logs' / 'overlay_server.log'),
            'BROWSER_LOG_FILE': str(run_dir / 'logs' / 'browser.log'),
            'XVFB_LOG_FILE': str(run_dir / 'logs' / 'xvfb.log'),
            'RESET_BROWSER_PROFILE': '1',
        }
    )

    proc = subprocess.Popen(
        [str(PYTHON), str(ENGINE)],
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

    started = time.monotonic()
    overlay_ready_at = None
    ffmpeg_start_at = None
    note = 'timeout'

    try:
        deadline = started + 90
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                note = f'exited rc={proc.returncode}'
                break
            try:
                ts, line = q.get(timeout=0.2)
            except queue.Empty:
                continue
            if overlay_ready_at is None and 'Overlay ready probe passed:' in line:
                overlay_ready_at = ts
            if 'Starting ffmpeg stream process...' in line:
                ffmpeg_start_at = ts
                note = 'ffmpeg_started'
                break

        ok = ffmpeg_start_at is not None

        # observe immediate crash for 4s
        survived_4s = False
        if ok:
            target = time.monotonic() + 4.0
            survived_4s = True
            while time.monotonic() < target:
                if proc.poll() is not None:
                    survived_4s = False
                    note = f'ffmpeg_early_exit rc={proc.returncode}'
                    break
                time.sleep(0.2)

        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()

        return {
            'iteration': i,
            'ok': ok,
            'survived_4s': survived_4s,
            'start_to_overlay_ready_sec': (overlay_ready_at - started) if overlay_ready_at else None,
            'start_to_ffmpeg_start_sec': (ffmpeg_start_at - started) if ffmpeg_start_at else None,
            'exit_code': proc.returncode,
            'note': note,
        }
    finally:
        # best-effort drain
        lines = []
        while True:
            try:
                _ts, line = q.get_nowait()
                lines.append(line)
            except queue.Empty:
                break
        if lines:
            p = run_dir / 'logs' / 'engine_stdout.log'
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('\n'.join(lines) + '\n', encoding='utf-8')


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    runs = []
    base_port = 20000
    n = 100
    for i in range(1, n + 1):
        r = run_once(i, base_port)
        runs.append(r)
        if i % 10 == 0:
            print(f"progress: {i}/{n} ok={sum(1 for x in runs if x['ok'])} survived_4s={sum(1 for x in runs if x['survived_4s'])}", flush=True)
        time.sleep(0.2)

    vals = [x['start_to_ffmpeg_start_sec'] for x in runs if x['ok'] and x['start_to_ffmpeg_start_sec'] is not None]
    ov = [x['start_to_overlay_ready_sec'] for x in runs if x['ok'] and x['start_to_overlay_ready_sec'] is not None]
    survived = [x for x in runs if x['survived_4s']]

    summary = {
        'generated_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'count_total': len(runs),
        'count_ok': len(vals),
        'count_survived_4s': len(survived),
        'ffmpeg_start_sec': {
            'min': min(vals) if vals else None,
            'p50': quantile(vals, 0.50),
            'p90': quantile(vals, 0.90),
            'p95': quantile(vals, 0.95),
            'p99': quantile(vals, 0.99),
            'max': max(vals) if vals else None,
            'mean': statistics.fmean(vals) if vals else None,
        },
        'overlay_ready_sec': {
            'min': min(ov) if ov else None,
            'p50': quantile(ov, 0.50),
            'p90': quantile(ov, 0.90),
            'p95': quantile(ov, 0.95),
            'p99': quantile(ov, 0.99),
            'max': max(ov) if ov else None,
            'mean': statistics.fmean(ov) if ov else None,
        },
        'runs': runs,
    }

    ts = time.strftime('%Y%m%d_%H%M%S', time.gmtime())
    out = OUT_DIR / f'p99_100_summary_{ts}.json'
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'Wrote: {out}', flush=True)
    print(json.dumps(summary['ffmpeg_start_sec'], ensure_ascii=False), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
