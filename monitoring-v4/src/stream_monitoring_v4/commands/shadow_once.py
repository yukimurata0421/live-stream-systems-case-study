from __future__ import annotations

import argparse
import json
import os
import socket
import time
from contextlib import nullcontext
from pathlib import Path

from stream_contracts.monitoring_v4.time import utc_text

from stream_monitoring_v4.runtime.isolation import validate_isolated_database_path
from stream_monitoring_v4.runtime.shadow_cycle import (
    ShadowCycleRequest,
    execute_shadow_cycle,
)
from stream_monitoring_v4.runtime.source_revision import (
    normalize_source_revision,
    read_source_revision,
)
from stream_monitoring_v4.runtime.projection_lock import projection_lock
from stream_monitoring_v4.runtime.safe_input import validate_safe_input_generation
from stream_monitoring_v4.storage.factory import (
    add_repository_arguments,
    repository_from_args,
    wait_for_integrity,
)
from stream_monitoring_v4.storage.postgres import PostgresMonitoringRepository


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Run the Monitoring v4 R0-R5 isolated decision pipeline once; "
            "any read-only API credential remains confined to its collector"
        )
    )
    add_repository_arguments(result)
    result.add_argument("--state-root", type=Path, required=True, help="read-only existing source state root")
    result.add_argument(
        "--source-repo",
        type=Path,
        default=Path(os.environ.get("STREAM_V3_SOURCE_REPO", "/opt/stream_v3")),
        help="stream_v3 migration source repository; used only for isolation enforcement",
    )
    result.add_argument("--now-ts", type=int, default=None, help="fixed test clock; current time by default")
    result.add_argument(
        "--public-shadow-output",
        type=Path,
        default=None,
        help="isolated non-public public-safe projection; defaults beside the SQLite database",
    )
    result.add_argument(
        "--build-revision",
        default="unversioned-local-worktree",
        help="immutable revision used to segment coverage/parity evidence",
    )
    result.add_argument(
        "--source-revision",
        default="",
        help="explicit secret-free source deployment identity; file discovery by default",
    )
    result.add_argument(
        "--source-revision-file",
        type=Path,
        default=None,
        help="allowlisted stream_v3 deployed revision env; defaults under --state-root",
    )
    result.add_argument(
        "--summary-only",
        action="store_true",
        help="emit a bounded journal summary instead of all contract payloads",
    )
    result.add_argument(
        "--lease-name",
        default="",
        help="optional database lease enforcing a single scheduled writer",
    )
    result.add_argument("--lease-ttl-sec", type=int, default=180)
    result.add_argument(
        "--input-projection-lock",
        type=Path,
        default=None,
        help="optional shared lock pinning one complete sanitized input generation",
    )
    result.add_argument("--input-projection-lock-timeout-sec", type=float, default=10.0)
    result.add_argument(
        "--youtube-api-state-file",
        type=Path,
        default=None,
        help="sanitized v4-owned direct YouTube API evidence; disabled by default",
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    repository, database = initialize_runtime(args)
    try:
        return run_parsed_once(args, repository, database)
    finally:
        close = getattr(repository, "close", None)
        if callable(close):
            close()


def initialize_runtime(args, *, postgres_startup_timeout_sec: int = 0):
    """Initialize one backend without granting any delivery/runtime authority."""

    applied_ts = int(time.time() if args.now_ts is None else args.now_ts)
    applied_at = utc_text(applied_ts)
    database: Path | None = None
    if not args.postgres:
        database = validate_isolated_database_path(
            args.db,
            source_state_root=args.state_root,
            migration_source_repository=args.source_repo,
        )
        args.db = database
    repository = repository_from_args(args, application_name="stream-monitoring-v4-core")
    try:
        if isinstance(repository, PostgresMonitoringRepository):
            wait_for_integrity(
                repository,
                timeout_sec=max(0, int(postgres_startup_timeout_sec)),
            )
        else:
            repository.initialize(applied_at=applied_at)
    except BaseException:
        close = getattr(repository, "close", None)
        if callable(close):
            close()
        raise
    return repository, database


def run_parsed_once(args, repository, database: Path | None) -> int:
    fixed_now_ts = int(args.now_ts) if args.now_ts is not None else None
    if args.postgres and fixed_now_ts is not None:
        raise ValueError(
            "live PostgreSQL shadow cycles do not accept --now-ts; replay into isolated SQLite"
        )
    if args.postgres and not args.lease_name:
        raise ValueError("live PostgreSQL shadow cycles require --lease-name")
    guard = (
        repository.cycle_guard(args.lease_name)
        if isinstance(repository, PostgresMonitoringRepository)
        else nullcontext(True)
    )
    with guard as guard_acquired:
        if not guard_acquired:
            _print_cycle_skip(args.lease_name, "postgres_advisory_lock_held")
            return 0
        input_guard = (
            projection_lock(
                args.input_projection_lock,
                exclusive=False,
                timeout_sec=args.input_projection_lock_timeout_sec,
            )
            if args.input_projection_lock is not None
            else nullcontext()
        )
        with input_guard:
            if args.input_projection_lock is not None:
                validate_safe_input_generation(args.state_root)
            return _run_with_writer_lease(
                args,
                repository,
                database,
                fixed_now_ts,
            )


def _run_with_writer_lease(args, repository, database, fixed_now_ts: int | None) -> int:
    lease_now_ts = fixed_now_ts if fixed_now_ts is not None else int(time.time())
    source_revision = (
        normalize_source_revision(args.source_revision)
        if args.source_revision
        else read_source_revision(
            args.source_revision_file or args.state_root / "deployed-revision.env"
        )
    )
    lease_owner = f"{socket.gethostname()}:{os.getpid()}"
    lease_acquired = False
    if args.lease_name:
        lease_acquired = repository.acquire_lease(
            args.lease_name,
            lease_owner,
            now_ts=lease_now_ts,
            ttl_sec=max(60, int(args.lease_ttl_sec)),
        )
        if not lease_acquired:
            _print_cycle_skip(
                args.lease_name,
                "single_writer_lease_held",
                at_ts=lease_now_ts,
            )
            return 0
    try:
        return _run(args, repository, database, fixed_now_ts, source_revision)
    finally:
        if lease_acquired:
            repository.release_lease(args.lease_name, lease_owner)


def _print_cycle_skip(lease_name: str, reason: str, *, at_ts: int | None = None) -> None:
    print(
        json.dumps(
            {
                "schema": "monitoring_v4.shadow_cycle_skip.v1",
                "reason": reason,
                "lease_name": lease_name,
                "at": utc_text(int(time.time()) if at_ts is None else int(at_ts)),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _run(
    args,
    repository,
    database: Path | None,
    fixed_now_ts: int | None,
    source_revision: str,
) -> int:
    payload = execute_shadow_cycle(
        repository,
        ShadowCycleRequest(
            state_root=args.state_root,
            source_repository=args.source_repo,
            database=database,
            public_shadow_output=args.public_shadow_output,
            build_revision=args.build_revision,
            source_revision=source_revision,
            fixed_now_ts=fixed_now_ts,
            summary_only=args.summary_only,
            youtube_api_state_file=args.youtube_api_state_file,
        ),
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
