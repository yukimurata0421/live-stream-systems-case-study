from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

from ..model import ActionCandidate, SubsystemsSnapshot
from .types import GateResult
from stream_core.supervisor.factory import STREAM_V3_K8S_TARGET_MAP

STREAM_SERVICE = "adsb-streamnew-youtube-stream.service"
DJ_SERVICE = "adsb-streamnew-auto-dj.service"
VIDEO_RESOLVER_SERVICE = "adsb-streamnew-youtube-video-resolver.service"
YOUTUBE_MONITOR_SERVICE = "adsb-streamnew-youtube-monitor.service"


@dataclass(frozen=True)
class ExecutionStep:
    step_id: str
    kind: str
    description: str
    command: tuple[str, ...] = ()
    service_unit: str = ""
    require_privilege: bool = False
    url_risk: str = "none"
    reads: tuple[str, ...] = ()
    writes: tuple[str, ...] = ()
    blocked_by: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "kind": self.kind,
            "description": self.description,
            "command": list(self.command),
            "service_unit": self.service_unit,
            "require_privilege": self.require_privilege,
            "url_risk": self.url_risk,
            "reads": list(self.reads),
            "writes": list(self.writes),
            "blocked_by": list(self.blocked_by),
        }


@dataclass(frozen=True)
class ExecutionPlan:
    action: str
    scope: str
    mode: str
    executable: bool
    execute: bool
    reason: str
    blocked_by: tuple[str, ...] = ()
    steps: tuple[ExecutionStep, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "scope": self.scope,
            "mode": self.mode,
            "executable": self.executable,
            "execute": self.execute,
            "reason": self.reason,
            "blocked_by": list(self.blocked_by),
            "steps": [step.to_dict() for step in self.steps],
        }


class ActionPlanBuilder:
    """Build the concrete execution plan for an approved recovery action.

    Phase 1/2 remains shadow-only. This builder deliberately separates the
    action decision from a runnable command so the refactor does not collapse
    engine-local recovery, systemd restarts, and YouTube API mutation into the
    same legacy restart path.
    """

    def build(
        self,
        snapshot: SubsystemsSnapshot,
        candidate: ActionCandidate,
        gate_result: GateResult,
        *,
        mode: str,
        supervisor_mode: str = "systemd",
    ) -> ExecutionPlan:
        del snapshot
        steps, executor_blockers = self._steps_for(candidate, supervisor_mode=supervisor_mode)
        blocked_by = [*gate_result.blocked_by, *executor_blockers]
        if mode == "shadow":
            blocked_by.append("shadow_mode")
        executable = bool(steps) and not gate_result.blocked_by and not executor_blockers
        execute = executable and mode != "shadow"
        reason = self._reason(candidate, gate_result, executor_blockers, mode)
        return ExecutionPlan(
            action=candidate.action,
            scope=candidate.scope,
            mode=mode,
            executable=executable,
            execute=execute,
            reason=reason,
            blocked_by=tuple(dict.fromkeys(blocked_by)),
            steps=tuple(steps),
        )

    def _steps_for(self, candidate: ActionCandidate, *, supervisor_mode: str = "systemd") -> tuple[list[ExecutionStep], list[str]]:
        action = candidate.action
        if action == "none":
            return [
                ExecutionStep(
                    step_id="no_action",
                    kind="noop",
                    description="No recovery action is required.",
                )
            ], []
        if action == "alert":
            return [
                ExecutionStep(
                    step_id="record_alert",
                    kind="v2_audit_log",
                    description="Record monitoring alert in v2 recovery audit outputs.",
                    writes=("recovery_orchestrator.jsonl", "recovery_action_plan.jsonl"),
                )
            ], []
        if action == "restart_dj":
            if _is_k8s(supervisor_mode):
                target = STREAM_V3_K8S_TARGET_MAP[DJ_SERVICE]
                return [
                    ExecutionStep(
                        step_id="restart_auto_dj_runtime_pod",
                        kind="k8s_rollout_restart",
                        description="Restart the v3 runtime workload that owns Auto DJ; container-level restart is not a separate owner in v3.",
                        command=("kubectl", "-n", "stream-v3", "rollout", "restart", target),
                        service_unit=DJ_SERVICE,
                        require_privilege=False,
                        url_risk="none",
                        writes=("k8s:" + target,),
                    )
                ], []
            return [
                ExecutionStep(
                    step_id="restart_auto_dj",
                    kind="systemd_restart",
                    description="Restart Auto DJ only; does not change YouTube URL or stream binding.",
                    command=("systemctl", "restart", DJ_SERVICE),
                    service_unit=DJ_SERVICE,
                    require_privilege=True,
                    url_risk="none",
                    writes=("systemd:" + DJ_SERVICE,),
                )
            ], []
        if action == "restart_stream":
            if _is_k8s(supervisor_mode):
                target = STREAM_V3_K8S_TARGET_MAP[STREAM_SERVICE]
                return [
                    ExecutionStep(
                        step_id="restart_stream_runtime_pod",
                        kind="k8s_rollout_restart",
                        description="Restart the v3 runtime workload through k8s; intended to preserve the current YouTube stream URL after v3 cutover.",
                        command=("kubectl", "-n", "stream-v3", "rollout", "restart", target),
                        service_unit=STREAM_SERVICE,
                        require_privilege=False,
                        url_risk="same_url_preserving",
                        writes=("k8s:" + target,),
                    )
                ], []
            return [
                ExecutionStep(
                    step_id="restart_stream_service",
                    kind="systemd_restart",
                    description="Restart the stream service through systemd; intended to preserve the current YouTube stream URL.",
                    command=("systemctl", "restart", STREAM_SERVICE),
                    service_unit=STREAM_SERVICE,
                    require_privilege=True,
                    url_risk="same_url_preserving",
                    writes=("systemd:" + STREAM_SERVICE,),
                )
            ], []
        if action == "resync_resolver":
            if _is_k8s(supervisor_mode):
                return [
                    ExecutionStep(
                        step_id="run_v3_control_shadow_once",
                        kind="v3_control_task",
                        description="Refresh v3 shadow state through the v3 control loop; resolver-specific ownership is not split yet.",
                        command=("python3", "-m", "stream_v3.control_loop", "--once", "--only", "shadow_once"),
                        url_risk="none",
                        reads=("youtube_watchdog_stats.json", "youtube_video_id_resolver_state.json"),
                        writes=("v3_control_state.json", "subsystems_status.json", "recovery_action_plan.json"),
                        blocked_by=("resolver_specific_k8s_task_not_yet_split",),
                    )
                ], ["resolver_specific_k8s_task_not_yet_split"]
            return [
                ExecutionStep(
                    step_id="start_video_resolver",
                    kind="systemd_start_oneshot",
                    description="Refresh the expected/candidate YouTube video ID resolver without promoting a candidate URL.",
                    command=("systemctl", "start", VIDEO_RESOLVER_SERVICE),
                    service_unit=VIDEO_RESOLVER_SERVICE,
                    require_privilege=True,
                    url_risk="none",
                    reads=("youtube_watchdog_stats.json", "youtube_video_id_resolver_state.json"),
                    writes=("youtube_video_id_resolver_state.json",),
                )
            ], []
        if action == "force_current_broadcast_live":
            return [
                ExecutionStep(
                    step_id="force_current_broadcast_live",
                    kind="youtube_api_mutation",
                    description="Attempt one-time liveBroadcasts.transition for the current broadcast only.",
                    command=("python3", "src/watchers/youtube_watchdog.py", "--force-live-once"),
                    service_unit=YOUTUBE_MONITOR_SERVICE,
                    require_privilege=False,
                    url_risk="can_change_youtube_lifecycle",
                    reads=("youtube_watchdog_stats.json", "oauth token state"),
                    writes=("YouTube liveBroadcasts.transition",),
                    blocked_by=("youtube_mutation_not_enabled_in_stream_v2",),
                )
            ], ["youtube_mutation_not_enabled_in_stream_v2"]
        if action == "create_replacement_broadcast":
            return [
                ExecutionStep(
                    step_id="replacement_broadcast",
                    kind="youtube_api_mutation",
                    description="Create/bind a replacement broadcast. This is intentionally not executor-owned in shadow refactor.",
                    url_risk="can_change_youtube_url",
                    writes=("YouTube liveBroadcasts.insert", "YouTube liveStreams.bind"),
                    blocked_by=("replacement_broadcast_not_executor_owned",),
                )
            ], ["replacement_broadcast_not_executor_owned"]
        if action == "restart_ffmpeg":
            return [
                ExecutionStep(
                    step_id="restart_ffmpeg_inside_engine",
                    kind="engine_control",
                    description="Restart FFmpeg inside the stream engine without restarting the full service.",
                    service_unit=STREAM_SERVICE,
                    url_risk="same_url_preserving",
                    writes=("stream_engine ffmpeg child process",),
                    blocked_by=("native_ffmpeg_control_api_not_yet_available",),
                )
            ], ["native_ffmpeg_control_api_not_yet_available"]
        if action in {"reload_overlay", "restart_browser"}:
            return [
                ExecutionStep(
                    step_id="refresh_rendering_inside_engine",
                    kind="engine_control",
                    description="Refresh Chromium/overlay rendering inside the stream engine without replacing the YouTube broadcast.",
                    service_unit=STREAM_SERVICE,
                    url_risk="same_url_preserving",
                    writes=("stream_engine browser/overlay helpers",),
                    blocked_by=("native_rendering_control_api_not_yet_available",),
                )
            ], ["native_rendering_control_api_not_yet_available"]
        if action == "repair_pulse":
            return [
                ExecutionStep(
                    step_id="validate_pulse_repair",
                    kind="script_dry_run",
                    description="Validate Pulse/PipeWire repair path before any user audio stack restart.",
                    command=("python3", "ops/scripts/ensure_pulse.py", "--dry-run", "--no-write-config"),
                    url_risk="same_url_preserving",
                    reads=("PulseAudio/PipeWire user session",),
                    blocked_by=("pulse_repair_requires_operator_window",),
                )
            ], ["pulse_repair_requires_operator_window"]
        if action == "retry_probe":
            return [
                ExecutionStep(
                    step_id="rerun_shadow_probe",
                    kind="v2_read_only_probe",
                    description="Re-read production runtime state and refresh v2 shadow outputs.",
                    command=("python3", "-m", "stream_v2", "shadow-once"),
                    reads=("production runtime state",),
                    writes=("stream_v2 shadow state",),
                )
            ], []
        return [], [f"unknown_action:{action}"]

    def _reason(
        self,
        candidate: ActionCandidate,
        gate_result: GateResult,
        executor_blockers: Sequence[str],
        mode: str,
    ) -> str:
        if gate_result.blocked_by:
            return "gates_block_execution_plan"
        if executor_blockers:
            return "executor_missing_native_control_path"
        if mode == "shadow":
            return "shadow_mode_plan_only"
        if candidate.action == "none":
            return "no_action_required"
        return "ready_to_execute"


def _is_k8s(mode: str) -> bool:
    return mode.strip().lower() in {"k8s", "k3s", "kubernetes"}
