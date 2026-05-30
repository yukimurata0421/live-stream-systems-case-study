from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

try:
    from stream_core.diagnostics import needrestart_contract, pipewire_canary, suite
except ModuleNotFoundError:
    from diagnostics import needrestart_contract, pipewire_canary, suite


@dataclass(frozen=True)
class DoctorContext:
    base_dir: Path
    read_env_file: Callable[[Path], dict[str, str]]
    parse_bool: Callable[[object], bool | None]
    run: Callable[..., object]
    needrestart_status: Callable[[], dict]
    ingest_status: Callable[[], dict]
    pipewire_status: Callable[[], dict]


def needrestart_contract_status(path: Path = Path("/etc/needrestart/conf.d/stream-24x7.conf")) -> dict:
    return needrestart_contract.needrestart_contract_status(path)


def pipewire_canary_status(ctx: DoctorContext) -> dict:
    return pipewire_canary.pipewire_canary_status(
        read_env_file=ctx.read_env_file,
        parse_bool=ctx.parse_bool,
        run=ctx.run,
    )


def _diagnostic_context(ctx: DoctorContext) -> suite.DiagnosticContext:
    return suite.DiagnosticContext(
        base_dir=ctx.base_dir,
        read_env_file=ctx.read_env_file,
        parse_bool=ctx.parse_bool,
        run=ctx.run,
    )


def doctor(ctx: DoctorContext) -> int:
    results = suite.collect_contract_results(
        _diagnostic_context(ctx),
        needrestart_status_func=ctx.needrestart_status,
        ingest_status_func=ctx.ingest_status,
        pipewire_status_func=ctx.pipewire_status,
    )
    for result in results:
        print(suite.format_result_line(result))
    return 0 if all(item.ok for item in results) else 1


def contract_check(ctx: DoctorContext, *, json_output: bool = False) -> int:
    results = suite.collect_contract_results(
        _diagnostic_context(ctx),
        needrestart_status_func=ctx.needrestart_status,
        ingest_status_func=ctx.ingest_status,
        pipewire_status_func=ctx.pipewire_status,
    )
    payload = suite.contract_payload(results)
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        for result in results:
            print(suite.format_result_line(result))
        print(
            "[summary] "
            f"ok={payload['ok']} warn_count={payload['warn_count']} "
            f"fail_count={payload['fail_count']} fatal_count={payload['fatal_count']}"
        )
    return 0 if payload["ok"] else 1
