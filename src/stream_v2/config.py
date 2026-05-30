from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


DEFAULT_SOURCE_STATE_ROOT = Path("/home/yuki/.local/state/adsb-streamnew")
DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REPO_STATE_ROOT = DEFAULT_REPO_ROOT / ".state" / "adsb-streamnew-v2"
DEFAULT_V2_STATE_ROOT = Path(
    os.environ.get(
        "STREAM_V2_STATE_ROOT",
        os.environ.get("STREAM_RUNTIME_STATE_DIR", str(DEFAULT_REPO_STATE_ROOT)),
    )
).expanduser()
DEFAULT_STREAM_ID = "adsb-streamnew"
DEFAULT_MAX_CONSISTENCY_WINDOW_SEC = 120.0
DEFAULT_SUPERVISOR_MODE = os.environ.get("STREAM_RUNTIME_SUPERVISOR", "systemd").strip().lower() or "systemd"


@dataclass(frozen=True)
class RuntimeConfig:
    source_state_root: Path = DEFAULT_SOURCE_STATE_ROOT
    state_root: Path = DEFAULT_V2_STATE_ROOT
    stream_id: str = DEFAULT_STREAM_ID
    max_consistency_window_sec: float = DEFAULT_MAX_CONSISTENCY_WINDOW_SEC
    mode: str = "shadow"
    supervisor_mode: str = DEFAULT_SUPERVISOR_MODE

    @property
    def logs_dir(self) -> Path:
        return self.state_root / "logs"

    @property
    def subsystems_status_path(self) -> Path:
        return self.state_root / "subsystems_status.json"

    @property
    def subsystems_status_log_path(self) -> Path:
        return self.logs_dir / "subsystems_status.jsonl"

    @property
    def orchestrator_log_path(self) -> Path:
        return self.logs_dir / "recovery_orchestrator.jsonl"

    @property
    def recovery_action_plan_path(self) -> Path:
        return self.state_root / "recovery_action_plan.json"

    @property
    def recovery_action_plan_log_path(self) -> Path:
        return self.logs_dir / "recovery_action_plan.jsonl"

    @property
    def objective_sli_path(self) -> Path:
        return self.state_root / "objective_sli.json"

    @property
    def objective_sli_log_path(self) -> Path:
        return self.logs_dir / "objective_sli.jsonl"

    @property
    def stream_components_path(self) -> Path:
        return self.state_root / "stream_components.json"

    @property
    def stream_components_log_path(self) -> Path:
        return self.logs_dir / "stream_components.jsonl"

    @property
    def action_lock_path(self) -> Path:
        return self.state_root / "recovery_action.lock.json"
