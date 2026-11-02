#!/usr/bin/env python3
"""Read-only host observation endpoint. Upload is the first adapter."""
from __future__ import annotations

import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import ipaddress
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from stream_core.common import observation_contract as contract
from stream_core.common import upload_observation, upload_samples


def labels(values):
    return "{" + ",".join(f"{key}={json.dumps(str(value))}" for key, value in sorted(values.items())) + "}"


def render(observation, *, host, source_hash, config_hash):
    fact = observation.observation
    base = {"source": fact.source, "scope": fact.scope, "host": host}
    lines = []

    def metric(name, help_text, entries):
        lines.extend([f"# HELP {name} {help_text}", f"# TYPE {name} gauge"])
        for extra, value in entries:
            lines.append(f"{name}{labels({**base, **extra})} {float(value):.9g}")

    metric("stream_v3_observer_build_info", "Startup source and configuration digests; not deployment authorization.",
           [({"source_hash": source_hash, "config_hash": config_hash}, 1)])
    metric("stream_v3_observation_validity", "One-hot validity of the source observation, not delivery health.",
           [({"validity": item.value}, fact.validity == item) for item in contract.Validity])
    metric("stream_v3_observation_target_state", "One-hot state for this scope; measurements alone leave delivery unknown.",
           [({"state": item.value}, fact.target_state == item) for item in contract.TargetState])
    metric("stream_v3_observation_expected_activity", "One-hot expectation; this adapter does not infer active or standby.",
           [({"activity": item.value}, fact.expected_activity == item) for item in contract.Activity])
    metric("stream_v3_observation_reason", "Bounded reason code for the current observation.", [({"reason": fact.reason}, 1)])
    metric("stream_v3_observation_collector_available", "Sampler heartbeat is current and collection has not reported failure.",
           [({}, observation.collector_available)])
    metric("stream_v3_observation_max_age_seconds", "Unchanged source validity limit.", [({}, fact.max_age_seconds)])
    if fact.age_upper_seconds is not None:
        metric("stream_v3_observation_source_age_seconds", "Original sample age from same-host boottime, never scrape time.",
               [({}, fact.age_upper_seconds)])
    if observation.mbps is not None:
        metric("stream_v3_observation_upload_mbps", "Measured TCP send rate; not proof of provider or player reception.",
               [({}, observation.mbps)])
        metric("stream_v3_observation_upload_interval_seconds", "Duration of the original counter measurement.",
               [({}, observation.interval_seconds)])
    return "\n".join(lines) + "\n"


def source_digest():
    # Hash the actual loaded package layout, including the private clock adapter
    # or the public canonical copy. The release must remain immutable after start.
    modules = [contract, upload_observation, upload_samples, upload_samples.clock]
    canonical = getattr(upload_samples.clock, "_module", None)
    paths = {Path(__file__).resolve(), *(Path(item.__file__).resolve() for item in modules)}
    if canonical is not None:
        paths.add(Path(canonical.__file__).resolve())
    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda path: str(path.relative_to(ROOT))):
        digest.update(str(path.relative_to(ROOT)).encode() + b"\0")
        digest.update(path.read_bytes() + b"\0")
    return digest.hexdigest()


def make_handler(read, *, host, source_hash, config_hash):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            payload = render(read(), host=host, source_hash=source_hash, config_hash=config_hash).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass
    return Handler


class Server(HTTPServer):
    # Serial, bounded requests. A stalled client cannot monopolize the socket
    # indefinitely or create an unbounded thread per connection.
    request_queue_size = 4

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(2)
        return connection, address


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--source-host", required=True)
    parser.add_argument("--machine", required=True)
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9120)
    parser.add_argument("--once", action="store_true", help="Print one scrape without opening a listener.")
    args = parser.parse_args()
    host = contract.identifier(args.source_host)
    address = ipaddress.IPv4Address(args.listen)
    if address.is_unspecified or address.is_multicast or not 0 <= args.port <= 65535:
        parser.error("bind an explicit loopback or private host address")
    if not (address.is_loopback or address.is_private):
        parser.error("public listeners are not supported")
    identity = upload_samples.clock.identity()
    if args.machine != identity["machine_id_sha256"]:
        parser.error("configured machine does not match this host")
    config = {"directory": str(args.directory.resolve()), "host": host,
              "machine": args.machine, "listen": str(address), "port": args.port}
    config_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    digest = source_digest()
    read = lambda: upload_observation.collect(args.directory, host=host, identity=identity)
    if args.once:
        print(render(read(), host=host, source_hash=digest, config_hash=config_hash), end="")
        return
    with Server((str(address), args.port), make_handler(read, host=host, source_hash=digest,
                                                       config_hash=config_hash)) as server:
        print(json.dumps({"listen": server.server_address, "source_hash": digest,
                          "config_hash": config_hash}), flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
