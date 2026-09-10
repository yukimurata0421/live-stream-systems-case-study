from __future__ import annotations

import argparse
import collections
import json
import ssl
import statistics
import time
import urllib.request
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only Phase 4 Agent status burst")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--requests", type=int, default=600)
    parser.add_argument("--sleep-seconds", type=float, default=0.005)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    tls = ssl.create_default_context(cafile=str(config["server_ca_certificate"]))
    tls.minimum_version = ssl.TLSVersion.TLSv1_3
    tls.load_cert_chain(str(config["tls_certificate"]), str(config["tls_private_key"]))
    url = f"{config['agent_base_url']}/v1/shadow/status"
    latencies: list[float] = []
    states: collections.Counter[str] = collections.Counter()
    errors: collections.Counter[str] = collections.Counter()
    physical_attempts: list[int] = []
    started = time.time()
    for _ in range(args.requests):
        request_started = time.perf_counter_ns()
        try:
            with urllib.request.urlopen(url, context=tls, timeout=2.0) as response:
                value = json.load(response)
            latencies.append((time.perf_counter_ns() - request_started) / 1_000_000)
            states[str(value.get("authority_state"))] += 1
            physical_attempts.append(int(value.get("physical_attempt_count", -1)))
        except Exception as error:  # read-only probe records the transport/error class only
            errors[type(error).__name__] += 1
        time.sleep(args.sleep_seconds)
    summary = {
        "profile": "LIVE_AGENT_STATUS_BURST_V1",
        "requests": args.requests,
        "success": len(latencies),
        "errors": dict(errors),
        "states": dict(states),
        "physical_attempt_max": max(physical_attempts) if physical_attempts else None,
        "latency_ms": {
            "median": statistics.median(latencies) if latencies else None,
            "maximum": max(latencies) if latencies else None,
        },
        "wall_seconds": time.time() - started,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0 if len(latencies) == args.requests and not errors and summary["physical_attempt_max"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
