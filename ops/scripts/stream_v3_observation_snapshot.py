#!/usr/bin/env python3
"""Build a bounded shadow JSON view from local Prometheus recording rules."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from stream_core.common import observation_projection as projection, upload_samples, observation_time
from stream_core.common.observation_contract import identifier


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def snapshot(*, url, instance, host, job=projection.JOB):
    parsed = urlsplit(url)
    if (parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
            or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in {"", "/"}):
        raise ValueError("prometheus_must_be_local")
    identifier(instance);identifier(host);identifier(job)
    query = ('{__name__=~"stream_v3:observation_fresh|stream_v3:observation_age_upper_seconds|stream_v3:upload_observed_mbps",'
             f'job={json.dumps(job)},instance={json.dumps(instance)},host={json.dumps(host)}}}')
    started = observation_time.capture()
    now = started.wall
    try:
        with build_opener(ProxyHandler({}), NoRedirect()).open(
                url.rstrip("/") + "/api/v1/query?" + urlencode({"query": query, "time": now}), timeout=3) as response:
            raw = response.read(65537)
        if len(raw) > 65536:
            raise ValueError("query_too_large")
        payload = json.loads(raw)
    except (OSError, ValueError):
        payload = {}
    finished = observation_time.capture()
    elapsed = finished.mono - started.mono
    result = projection.project(payload, now_wall=now, elapsed_seconds=elapsed, instance=instance, host=host, job=job)
    return observation_time.attach(result, "observation_projection", now=finished)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:9090")
    parser.add_argument("--instance", required=True)
    parser.add_argument("--source-host", required=True)
    parser.add_argument("--job", default=projection.JOB)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = snapshot(url=args.prometheus_url, instance=args.instance, host=args.source_host, job=args.job)
    if args.output:
        upload_samples.write_json(args.output, result)
    else:
        print(json.dumps(result, allow_nan=False))


if __name__ == "__main__":
    main()
