from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .dependencies import command_results
from .file_contracts import required_file_results
from .ingest_contract import ingest_result, stream_ingest_endpoint_status
from .model import CheckResult, payload_from_results, status_prefix
from .needrestart_contract import needrestart_contract_status, needrestart_result_from_status
from .pipewire_canary import pipewire_canary_status, pipewire_result


@dataclass(frozen=True)
class DiagnosticContext:
    base_dir: Path
    read_env_file: Callable[[Path], dict[str, str]]
    parse_bool: Callable[[object], bool | None]
    run: Callable[..., object]


def collect_contract_results(
    ctx: DiagnosticContext,
    *,
    needrestart_status_func: Callable[[], dict] | None = None,
    ingest_status_func: Callable[[], dict] | None = None,
    pipewire_status_func: Callable[[], dict] | None = None,
) -> list[CheckResult]:
    needrestart_status = needrestart_status_func() if needrestart_status_func else needrestart_contract_status()
    ingest_status = ingest_status_func() if ingest_status_func else stream_ingest_endpoint_status(ctx.read_env_file)
    pipewire_status = pipewire_status_func() if pipewire_status_func else pipewire_canary_status(
        read_env_file=ctx.read_env_file,
        parse_bool=ctx.parse_bool,
        run=ctx.run,
    )
    return [
        *command_results(),
        *required_file_results(ctx.base_dir),
        needrestart_result_from_status(needrestart_status),
        ingest_result(ingest_status),
        pipewire_result(pipewire_status),
    ]


def contract_payload(results: list[CheckResult]) -> dict:
    payload = payload_from_results(results)
    payload["mode"] = "contract_check"
    payload["fatal_checks"] = [item.name for item in results if item.fatal]
    return payload


def format_result_line(result: CheckResult) -> str:
    prefix = status_prefix(result)
    if result.category == "dependency":
        return f"[{prefix}] {result.summary}"
    if result.category == "file_contract":
        return f"[{prefix}] {result.summary}"
    if result.name == "needrestart:stream_override":
        if result.ok:
            return f"[ok] needrestart contract: {result.path}"
        return f"[ng] needrestart contract: {result.data.get('reason', result.summary)} ({result.path})"
    if result.name == "ingest:youtube_endpoint":
        data = result.data
        reason = data.get("reason", "")
        preferred = data.get("preferred_url", "")
        if data.get("judgment") == "rtmps_preferred":
            return f"[ok] ingest endpoint: {data.get('scheme')}://{data.get('host')}:{data.get('port')}/live2 ({reason})"
        if data.get("judgment") in {"rtmps_preferred_implicit_443", "rtmp_legacy"}:
            return f"[warn] ingest endpoint: {result.detail} ({reason}); preferred={preferred}"
        return f"[ng] ingest endpoint: {reason} ({result.path})"
    if result.name == "audio:pipewire_canary":
        data = result.data
        return (
            "[info] pipewire canary "
            f"prefer={data.get('prefer_pipewire_pulse')} pipewire_active={data.get('pipewire_active')} "
            f"server_is_pipewire={data.get('server_is_pipewire')} recommendation={data.get('recommendation')}"
        )
    return f"[{prefix}] {result.summary}"
