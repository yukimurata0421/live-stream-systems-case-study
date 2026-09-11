#!/usr/bin/env python3
"""Create a narrow, sanitized observation-boundary replay fixture from a dump.

This utility deliberately restores only into a disposable, network-disabled
PostgreSQL container.  It never accepts a DSN and it omits raw identifiers,
notification bodies, and string payload values from its output.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import secrets
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


EXPECTED_DUMP_SHA256 = "0771dfe5c573d562d9a2bbbcb92a4edcca8d9271408e1a76dd4525f0cbc38ff4"
HISTORICAL_BUILD = "ef2f0b0d5ed9e0f5fefac6d4ef1f72d508ae2afa"
HISTORICAL_SOURCE = "stream-v3-input-quality-raw-authority-v1.19-20260811@392d4af3970303ed394b6eaa067c22e897886ab5"
POSTGRES_IMAGE = "postgres@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193"
WINDOW_ID = "HR-20260813-RUNTIME-ROLLOUT"
WINDOW_START = "2026-08-12T16:48:32Z"
WINDOW_END = "2026-08-12T16:51:20Z"
SANITIZATION_REVISION = "monitoring-v4-historical-sanitization-r1"

_SENSITIVE_KEY = re.compile(
    r"(?:id|identifier|token|secret|password|credential|authorization|url|host|path|argv|command|pid|generation|subject|content)",
    re.IGNORECASE,
)


class ExtractionError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run(args: list[str], *, timeout: int = 60, input_text: str | None = None) -> str:
    completed = subprocess.run(
        args,
        check=False,
        text=True,
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if completed.returncode:
        raise ExtractionError(
            f"command failed ({completed.returncode}): {' '.join(args[:3])}: {completed.stderr.strip()}"
        )
    return completed.stdout


def _postgres(container: str, sql: str) -> list[dict[str, Any]]:
    raw = _run(["docker", "exec", container, "psql", "-h", "127.0.0.1", "-U", "postgres", "-d", "historical", "--csv", "-tq", "-c", sql])
    return [json.loads(row[0]) for row in csv.reader(io.StringIO(raw)) if row]


def _safe_payload(value: Any) -> Any:
    """Preserve only non-identifying boolean/numeric payload evidence."""
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, list):
        return [item for item in (_safe_payload(item) for item in value) if item is not _DROP]
    if isinstance(value, dict):
        return {
            key: cleaned
            for key, item in sorted(value.items())
            if not _SENSITIVE_KEY.search(str(key))
            for cleaned in (_safe_payload(item),)
            if cleaned is not _DROP
        }
    return _DROP


class _Drop:
    pass


_DROP = _Drop()


def _alias(prefix: str, row: dict[str, Any]) -> str:
    semantic = "-".join(
        str(row[key]).lower().replace("_", "-")
        for key in ("domain", "source", "observed_at", "state")
    )
    semantic = re.sub(r"[^a-z0-9-]+", "-", semantic).strip("-")
    return f"{prefix}-{semantic}"


def _fixture_from_rows(
    rows: list[dict[str, Any]], *, dump_sha256: str, transitions: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    if len(rows) != 6:
        raise ExtractionError(f"expected six delivery/rendering observations, found {len(rows)}")
    observations: list[dict[str, Any]] = []
    for row in rows:
        payload = json.loads(row.pop("payload_json")) if row.get("payload_json") else {}
        observations.append(
            {
                "provenance": "historical_observed",
                "alias": _alias("obs", row),
                "domain": row["domain"],
                "source": row["source"],
                "evidence_role": row["evidence_role"],
                "state": row["state"],
                "reason_code": row["reason_code"],
                "observed_at": row["observed_at"],
                "received_at": row["received_at"],
                "freshness_limit_sec": int(row["freshness_limit_sec"]),
                "producer_revision": row["producer_revision"],
                "source_event_alias": _alias("event", row),
                "source_generation_alias": f"generation-{row['domain']}-{row['source']}",
                "payload": _safe_payload(payload),
            }
        )
    observations.sort(key=lambda item: (item["observed_at"], item["domain"]))
    return {
        "schema": "monitoring_v4.historical_replay_window.v1",
        "fixture_id": "2026-08-13_runtime_rollout_observation_baseline",
        "replay_boundary": "persisted_observation",
        "provenance": {
            "historical_window_id": WINDOW_ID,
            "historical_start": WINDOW_START,
            "historical_end": WINDOW_END,
            "dump_sha256": dump_sha256,
            "historical_revision": {
                "schema_revision": 3,
                "build": HISTORICAL_BUILD,
                "source": HISTORICAL_SOURCE,
                "source_policy": "monitoring-v4-source-policy-r3.3",
                "reducer": "monitoring-v4-current-reducer-r3.2",
                "incident": "monitoring-v4-incident-policy-r4.2",
                "route": "monitoring-v4-route-policy-r5.1",
                "template": "monitoring-v4-ja-r5.1",
            },
            "sanitization_revision": SANITIZATION_REVISION,
        },
        "observations": observations,
        "historical_expected": {
            "provenance": "historical_observed",
            "episodes": 2,
            "transitions": transitions or [
                {"domain": "rendering", "phase": "detected", "severity": "warning", "occurred_at": "2026-08-12T16:50:20Z"},
                {"domain": "delivery", "phase": "detected", "severity": "critical", "occurred_at": "2026-08-12T16:50:20Z"},
                {"domain": "delivery", "phase": "recovered", "severity": "info", "occurred_at": "2026-08-12T16:51:20Z"},
                {"domain": "rendering", "phase": "recovered", "severity": "info", "occurred_at": "2026-08-12T16:51:20Z"}
            ],
            "logical_intents": 6,
            "external_delivery_attempts": 0,
            "external_delivery_results": 0,
            "persisted_parity": {
                "16:50:20Z": {"equivalent": False, "delivery_classification": "unclassified_contract_difference", "skew_sec": -19},
                "16:51:20Z": {"equivalent": True}
            }
        },
        "synthetic_actions": [],
        "unknown": [
            "raw_source_body", "exact_pod_uid", "ffmpeg_exit_direct_cause",
            "rollout_initiator", "historical_cri_digest", "exact_pod_process_lifecycle"
        ]
    }


def extract_fixture(dump: Path, output: Path) -> dict[str, Any]:
    if sha256_file(dump) != EXPECTED_DUMP_SHA256:
        raise ExtractionError("dump SHA-256 mismatch; refusing extraction")
    if not dump.is_file():
        raise ExtractionError("dump path is not a regular file")
    container = f"stream-v4-historical-extract-{secrets.token_hex(6)}"
    mounted_dump = f"{dump.resolve()}:/input/historical.dump:ro"
    try:
        _run([
            "docker", "run", "-d", "--name", container, "--network", "none",
            "--cpus", "1", "--memory", "768m", "--pids-limit", "256",
            "--tmpfs", "/var/lib/postgresql/data:rw,size=512m",
            "-e", "POSTGRES_PASSWORD=historical-audit-only", "-v", mounted_dump,
            POSTGRES_IMAGE,
        ])
        deadline = time.monotonic() + 30
        while True:
            ready = subprocess.run(
                ["docker", "exec", container, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
            ).returncode == 0
            if ready:
                break
            if time.monotonic() >= deadline:
                raise ExtractionError("disposable PostgreSQL did not become ready")
            time.sleep(0.25)
        _run(["docker", "exec", container, "createdb", "-h", "127.0.0.1", "-U", "postgres", "historical"])
        _run(["docker", "exec", container, "pg_restore", "--no-owner", "-h", "127.0.0.1", "-U", "postgres", "-d", "historical", "/input/historical.dump"], timeout=120)
        schema = _postgres(container, "SELECT row_to_json(x)::text FROM (SELECT max(version)::int AS schema_revision FROM schema_migrations) x")
        if schema != [{"schema_revision": 3}]:
            raise ExtractionError(f"historical schema mismatch: {schema!r}")
        releases = _postgres(container, "SELECT row_to_json(x)::text FROM (SELECT build_revision, source_revision FROM shadow_cycles ORDER BY completed_ts DESC LIMIT 1) x")
        if not releases or releases[0].get("build_revision") != HISTORICAL_BUILD or releases[0].get("source_revision") != HISTORICAL_SOURCE:
            raise ExtractionError("historical build/source revision mismatch")
        rows = _postgres(container, """
            SELECT row_to_json(x)::text FROM (
              SELECT domain, source, evidence_role, status AS state, reason_code,
                     observed_at, received_at, freshness_limit_sec, producer_revision,
                     payload_json
              FROM observations
              WHERE domain IN ('delivery', 'rendering')
                AND observed_at IN (
                  '2026-08-12T16:48:32Z', '2026-08-12T16:49:51Z',
                  '2026-08-12T16:50:12Z', '2026-08-12T16:50:58Z'
                )
              ORDER BY observed_at, domain
            ) x
        """)
        transitions = _postgres(container, """
            SELECT row_to_json(x)::text FROM (
              SELECT domain, phase, severity, occurred_at
              FROM incident_transitions
              WHERE domain IN ('delivery', 'rendering')
                AND occurred_at IN ('2026-08-12T16:50:20Z', '2026-08-12T16:51:20Z')
              ORDER BY occurred_at, domain
            ) x
        """)
        if len(transitions) != 4:
            raise ExtractionError(f"expected four historical transitions, found {len(transitions)}")
        fixture = _fixture_from_rows(rows, dump_sha256=EXPECTED_DUMP_SHA256, transitions=transitions)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(fixture, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        return fixture
    finally:
        subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dump", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        extract_fixture(args.dump, args.output)
    except (ExtractionError, OSError, subprocess.SubprocessError) as exc:
        print(f"historical dump extraction failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
