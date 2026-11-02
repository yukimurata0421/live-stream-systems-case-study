#!/usr/bin/env python3
"""Bounded loopback test: fixture journal -> HTTP -> Prometheus -> JSON.

Uses only owned temporary files and child processes. No production data,
credentials, service manager, notifications or recovery entrypoints are used.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import resource
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.request import ProxyHandler, build_opener
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from stream_core.common import upload_samples, observation_projection


def module(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def limits():
    os.setsid()
    os.nice(10)
    resource.setrlimit(resource.RLIMIT_CPU, (45, 45))
    resource.setrlimit(resource.RLIMIT_AS, (4 * 1024**3, 4 * 1024**3))
    resource.setrlimit(resource.RLIMIT_NOFILE, (256, 256))
    os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:2])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prometheus", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=False)
    sampler = module("stream_v3_upload_measurement")
    projector = module("stream_v3_observation_snapshot")
    children, logs, results = [], [], []
    started = time.monotonic()
    identity = upload_samples.clock.identity()
    env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "PYTHONDONTWRITEBYTECODE": "1"}

    def spawn(command, name, *, stdout=None, cwd=None):
        log = (args.evidence / (name + ".log")).open("w");logs.append(log)
        child = subprocess.Popen(command, cwd=cwd, env=env, preexec_fn=limits,
                                 stdout=stdout or log, stderr=log, text=True)
        children.append(child)
        return child

    def stop(child):
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL);child.wait(timeout=5)

    temporary = tempfile.mkdtemp(prefix="stream-observation-shadow-")
    try:
        folder = Path(temporary);source = folder / "source";source.mkdir()
        sequence = 0

        def write_sample(*, age=0., zero=False, utc_step=0.):
            nonlocal sequence
            sequence += 1
            now = upload_samples.clock.capture()
            generation = f"fixture-{sequence}"
            worker = sampler.Sampler({}, identity=identity, host="fixture-encoder")
            for delta in [-60, 0]:
                wall = now["wall"] + delta + utc_step
                value = {"schema_version": "runtime.ffmpeg_observation.v1", "observed_at": upload_samples.utc(wall - .1),
                    "producer_instance_id": "fixture", "ffmpeg_generation": generation,
                    "sequence": delta + 61, "local_ffmpeg_pid": 123, "ffmpeg_running": True,
                    "managed_child_cardinality": "1", "protocol_ffmpeg_pid": 0, "runtime_identity": None,
                    "tcp_metrics": {"bytes_sent": 1000 + (delta+60)*(0 if zero else 500000),
                        "bytes_acked": 1000 + (delta+60)*500000, "socket_cookie": generation}}
                row = worker.step(value, {"wall": wall, "mono": now["mono"] + delta})
            row["source_mono"] -= age
            upload_samples.append_sample(source / "samples.jsonl", row)
            upload_samples.write_json(source / "status.json", {"schema": "stream.upload_sampler_status.v1",
                **identity, "source_host": "fixture-encoder", "source_mono": now["mono"],
                "measurement_epoch": row["measurement_epoch"], "status": "READY"})

        def start_exporter(port=0):
            child = spawn([sys.executable, "-I", "-B", str(Path(__file__).with_name("stream_v3_observation_exporter.py")),
                "--directory", str(source), "--source-host", "fixture-encoder",
                "--machine", identity["machine_id_sha256"], "--port", str(port)],
                f"exporter-{len(children)}", stdout=subprocess.PIPE, cwd=folder)
            if not select.select([child.stdout], [], [], 5)[0]:
                raise RuntimeError("exporter_start_timeout")
            value = json.loads(child.stdout.readline())
            return child, value["listen"][1]

        write_sample(zero=True)
        exporter, port = start_exporter()
        with socket.socket() as available:
            available.bind(("127.0.0.1", 0));prom_port = available.getsockname()[1]
        rules = folder / "rules.yml"
        rules.write_text((ROOT / "ops/monitoring/observation-shadow/rules.yml").read_text().replace("interval: 15s", "interval: 2s"))
        config = (ROOT / "ops/monitoring/observation-shadow/prometheus.yml.in").read_text()
        config = config.replace("@RULES_FILE@", str(rules)).replace("@OBSERVER_ADDRESS@", f"127.0.0.1:{port}")
        config = config.replace("interval: 15s", "interval: 2s").replace("timeout: 3s", "timeout: 1s")
        (folder / "prometheus.yml").write_text(config)
        prometheus = spawn([str(args.prometheus.resolve()), "--config.file=" + str(folder / "prometheus.yml"),
            "--storage.tsdb.path=" + str(folder / "tsdb"), "--storage.tsdb.retention.time=1h",
            f"--web.listen-address=127.0.0.1:{prom_port}", "--query.max-concurrency=2", "--query.timeout=3s"],
            "prometheus", cwd=folder)
        url = f"http://127.0.0.1:{prom_port}"
        opener = build_opener(ProxyHandler({}))

        def check(name, *, valid, rate=None, validity="VALID", exporter_up=1):
            deadline = min(started + 90, time.monotonic() + 15)
            phase_started = time.time()
            latest = None
            while time.monotonic() < deadline:
                if prometheus.poll() is not None:
                    raise RuntimeError("isolated_prometheus_exited")
                latest = projector.snapshot(url=url, instance=f"127.0.0.1:{port}", host="fixture-encoder")
                confirmed = observation_projection.still_valid(latest)
                if confirmed == valid and (rate is None or latest["upload_mbps"] == rate):
                    # For unavailable phases, also require the fresh recording
                    # rule to have observed the change, not a startup query error.
                    if not valid and latest["observation"]["reason"] in {"query_invalid", "query_unavailable"}:
                        time.sleep(.25);continue
                    query = (f'up{{job="stream_v3_observation_shadow",instance="127.0.0.1:{port}"}}'
                             if exporter_up == 0 else
                             f'stream_v3_observation_validity{{job="stream_v3_observation_shadow",instance="127.0.0.1:{port}",validity="{validity}"}}')
                    with opener.open(url + "/api/v1/query?" + urlencode({"query": query}), timeout=2) as response:
                        proof = json.load(response)["data"]["result"]
                    if (len(proof) != 1 or proof[0]["value"][0] < phase_started
                            or float(proof[0]["value"][1]) != (0 if exporter_up == 0 else 1)):
                        time.sleep(.25);continue
                    results.append({"scenario": name, "passed": True, "projection": latest, "scrape_proof": proof})
                    return
                time.sleep(.25)
            raise RuntimeError(f"scenario_timeout:{name}:{latest}")

        check("valid_zero_and_control_identity_missing", valid=True, rate=0.)
        write_sample(utc_step=7200.)
        check("source_utc_offset_and_new_generation", valid=True, rate=4.)
        write_sample(age=200.)
        check("expired_source_with_live_exporter", valid=False, validity="STALE")
        with opener.open(f"http://127.0.0.1:{port}/metrics", timeout=2) as response:
            assert response.status == 200
        (source / "samples.jsonl").unlink()
        check("missing_source_with_live_exporter", valid=False, validity="UNAVAILABLE")
        write_sample()
        check("source_recovers_in_new_generation", valid=True, rate=4.)
        stop(exporter)
        check("exporter_down_with_retained_prometheus_samples", valid=False, exporter_up=0)
        exporter, _ = start_exporter(port)
        check("exporter_restart", valid=True, rate=4.)
        summary = {"status": "PASS", "scenarios": results, "production_modified": False,
                   "elapsed_seconds": time.monotonic() - started}
    except Exception as error:
        summary = {"status": "FAILED", "scenarios": results, "error": str(error),
                   "production_modified": False, "elapsed_seconds": time.monotonic() - started}
    finally:
        for child in reversed(children):
            stop(child)
        for log in logs:
            log.close()
        shutil.rmtree(temporary)
    summary["owned_children_exited"] = all(child.poll() is not None for child in children)
    (args.evidence / "result.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({key: value for key, value in summary.items() if key != "scenarios"}))
    return 0 if summary["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
