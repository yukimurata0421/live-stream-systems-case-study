from __future__ import annotations

import json
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


@dataclass(frozen=True)
class ShadowCommandContext:
    state_base_dir: Path
    subsystems_status_file: Path
    subsystems_status_events_file: Path
    recovery_orchestrator_events_file: Path
    recovery_action_plan_file: Path
    stream_id: str = "adsb-streamnew"


def _ensure_src_on_path() -> None:
    src_dir = Path(__file__).resolve().parents[2]
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


def runtime_config(ctx: ShadowCommandContext, *, state_root: Path | None = None):
    try:
        from stream_v2.config import DEFAULT_MAX_CONSISTENCY_WINDOW_SEC, DEFAULT_SOURCE_STATE_ROOT, RuntimeConfig
    except ImportError:
        _ensure_src_on_path()
        from stream_v2.config import DEFAULT_MAX_CONSISTENCY_WINDOW_SEC, DEFAULT_SOURCE_STATE_ROOT, RuntimeConfig

    source_state_root = Path(os.environ.get("STREAM_V2_SOURCE_STATE_ROOT", str(DEFAULT_SOURCE_STATE_ROOT))).expanduser()
    return RuntimeConfig(
        source_state_root=source_state_root,
        state_root=(state_root or ctx.state_base_dir),
        stream_id=ctx.stream_id,
        max_consistency_window_sec=DEFAULT_MAX_CONSISTENCY_WINDOW_SEC,
        mode="shadow",
    )


def run_shadow_pipeline(ctx: ShadowCommandContext, *, record: bool = True):
    try:
        from stream_v2.pipeline import ShadowPipeline
    except ImportError:
        _ensure_src_on_path()
        from stream_v2.pipeline import ShadowPipeline

    if record:
        return ShadowPipeline(runtime_config(ctx)).run_once()
    with tempfile.TemporaryDirectory(prefix="stream_v2_shadow_no_record.") as td:
        return ShadowPipeline(runtime_config(ctx, state_root=Path(td))).run_once()


def run_subsystems_status_pipeline(ctx: ShadowCommandContext, *, record: bool = True):
    try:
        from stream_v2.pipeline import ShadowPipeline
    except ImportError:
        _ensure_src_on_path()
        from stream_v2.pipeline import ShadowPipeline

    if record:
        return ShadowPipeline(runtime_config(ctx)).run_subsystems_status_once()
    with tempfile.TemporaryDirectory(prefix="stream_v2_subsystems_no_record.") as td:
        return ShadowPipeline(runtime_config(ctx, state_root=Path(td))).run_subsystems_status_once()


def run_recovery_orchestrator_pipeline(ctx: ShadowCommandContext, *, record: bool = True):
    try:
        from stream_v2.pipeline import ShadowPipeline
    except ImportError:
        _ensure_src_on_path()
        from stream_v2.pipeline import ShadowPipeline

    if record:
        return ShadowPipeline(runtime_config(ctx)).run_recovery_orchestrator_once()
    with tempfile.TemporaryDirectory(prefix="stream_v2_orchestrator_no_record.") as td:
        return ShadowPipeline(runtime_config(ctx, state_root=Path(td))).run_recovery_orchestrator_once()


def all_gates_passed(gates: object) -> bool:
    if not isinstance(gates, dict):
        return True
    for value in gates.values():
        if isinstance(value, dict) and value.get("passed") is False:
            return False
    return True


def subsystems_status(
    ctx: ShadowCommandContext,
    *,
    json_output: bool = False,
    record: bool = True,
    shadow_runner: Callable[[bool], Any] | None = None,
) -> int:
    result = shadow_runner(record) if shadow_runner is not None else run_subsystems_status_pipeline(ctx, record=record)
    payload = result.snapshot
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0
    overall = payload.get("overall") if isinstance(payload.get("overall"), dict) else {}
    degraded = overall.get("degraded_subsystems", [])
    degraded_text = ",".join(degraded) if isinstance(degraded, list) and degraded else "-"
    print(
        "[subsystems-status] "
        f"state={overall.get('state', 'unknown')} "
        f"public={overall.get('stream_public_state', 'unknown')} "
        f"video_id={overall.get('expected_video_id', '')} "
        f"action={overall.get('recommended_action', 'none')} "
        f"scope={overall.get('action_scope', 'none')} "
        f"degraded={degraded_text}"
    )
    if record:
        print(f"[subsystems-status] snapshot={ctx.subsystems_status_file} history={ctx.subsystems_status_events_file}")
    return 0


def recovery_orchestrator(
    ctx: ShadowCommandContext,
    *,
    json_output: bool = False,
    record: bool = True,
    shadow_runner: Callable[[bool], Any] | None = None,
) -> int:
    result = shadow_runner(record) if shadow_runner is not None else run_recovery_orchestrator_pipeline(ctx, record=record)
    event = result.orchestrator_event
    if json_output:
        print(json.dumps(event, ensure_ascii=False, separators=(",", ":")))
        return 0
    selected = event.get("selected_action") if isinstance(event.get("selected_action"), dict) else {}
    print(
        "[recovery-orchestrator] "
        f"mode=shadow execute={selected.get('execute', False)} "
        f"action={selected.get('action', 'none')} "
        f"scope={selected.get('scope', 'none')} "
        f"gates_passed={all_gates_passed(event.get('gates'))}"
    )
    if record:
        print(
            f"[recovery-orchestrator] history={ctx.recovery_orchestrator_events_file} "
            f"action_plan={ctx.recovery_action_plan_file}"
        )
    return 0


def shadow_once(
    ctx: ShadowCommandContext,
    *,
    json_output: bool = False,
    record: bool = True,
    shadow_runner: Callable[[bool], Any] | None = None,
) -> int:
    result = shadow_runner(record) if shadow_runner is not None else run_shadow_pipeline(ctx, record=record)
    payload = {
        "subsystems_status": result.snapshot,
        "recovery_orchestrator": result.orchestrator_event,
        "recovery_action_plan": result.recovery_action_plan,
        "objective_sli": result.objective_sli,
        "stream_components": result.stream_components,
    }
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0
    overall = result.snapshot.get("overall") if isinstance(result.snapshot.get("overall"), dict) else {}
    selected = result.orchestrator_event.get("selected_action")
    selected = selected if isinstance(selected, dict) else {}
    print(
        "[shadow-once] "
        f"state={overall.get('state', 'unknown')} "
        f"public={overall.get('stream_public_state', 'unknown')} "
        f"action={selected.get('action', 'none')} "
        f"execute={selected.get('execute', False)}"
    )
    if record:
        print(
            "[shadow-once] "
            f"snapshot={ctx.subsystems_status_file} status_history={ctx.subsystems_status_events_file} "
            f"orchestrator_history={ctx.recovery_orchestrator_events_file}"
        )
    return 0


def shadow_sli(ctx: ShadowCommandContext, *, json_output: bool = False) -> int:
    try:
        from stream_v2.sli import ObjectiveSliCalculator
        from stream_v2.timeutil import now_utc
    except ImportError:
        _ensure_src_on_path()
        from stream_v2.sli import ObjectiveSliCalculator
        from stream_v2.timeutil import now_utc

    config = runtime_config(ctx)
    payload = ObjectiveSliCalculator(config).calculate(now=now_utc())
    if json_output:
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        return 0

    timer_policy = payload.get("shadow_timer_policy") if isinstance(payload.get("shadow_timer_policy"), dict) else {}
    window_policy = payload.get("window_policy") if isinstance(payload.get("window_policy"), dict) else {}
    print(
        "[shadow-sli] "
        f"generated_at={payload.get('ts_utc', '')} "
        f"interval_sec={timer_policy.get('expected_interval_sec', '')} "
        f"coverage_ratio={window_policy.get('window_complete_coverage_ratio', '')}"
    )
    windows = payload.get("windows") if isinstance(payload.get("windows"), dict) else {}
    for name in ("last_24h", "last_7d", "last_30d"):
        window = windows.get(name) if isinstance(windows.get(name), dict) else {}
        shadow = window.get("subsystems_shadow") if isinstance(window.get("subsystems_shadow"), dict) else {}
        print(
            "[shadow-sli] "
            f"window={name} complete={window.get('window_complete', False)} "
            f"coverage_sec={window.get('data_coverage_sec', 0)} "
            f"status_samples={shadow.get('status_sample_count', 0)} "
            f"orchestrator_samples={shadow.get('orchestrator_sample_count', 0)} "
            f"selected_actions={shadow.get('selected_action_counts', {})} "
            f"production_actions={shadow.get('production_action_counts', {})} "
            f"disagreements={shadow.get('shadow_vs_production_disagreement_count', 0)}"
        )
    return 0
