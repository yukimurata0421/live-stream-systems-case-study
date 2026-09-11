"""Backward-compatible facade for the split safe-input projection subsystem."""

from stream_monitoring_v4.runtime.safe_inputs.constants import (
    LIFECYCLE_MAX_LINE_BYTES,
    LIFECYCLE_MAX_LINES,
    LIFECYCLE_TAIL_BYTES,
    RAW_LIFECYCLE_FILES,
    RAW_ROLLOUT_FILE,
    SAFE_JSON_FILES,
    SAFE_OUTBOX_FILE,
    SAFE_LIFECYCLE_FILE,
    SAFE_REVISION_FILE,
    SAFE_ROLLOUT_FILE,
)
from stream_monitoring_v4.runtime.safe_inputs.lifecycle import (
    runtime_lifecycle_projection as _runtime_lifecycle_projection,
)
from stream_monitoring_v4.runtime.safe_inputs.publisher import (
    ProjectionResult,
    atomic_projected_write as _atomic_write,
    project_safe_inputs,
    project_safe_inputs_unlocked as _project_safe_inputs_unlocked,
    revision_content as _revision_content,
)
from stream_monitoring_v4.runtime.safe_inputs.generation import (
    SAFE_GENERATION_MANIFEST,
    validate_safe_input_generation,
)
from stream_monitoring_v4.runtime.safe_inputs.rollout import (
    runtime_rollout_projection as _runtime_rollout_projection,
)
from stream_monitoring_v4.runtime.safe_inputs.sanitize import TRANSFORMS
from stream_monitoring_v4.runtime.safe_inputs.stable_io import (
    stable_text_lines as _stable_text_lines,
)

__all__ = [
    "LIFECYCLE_MAX_LINE_BYTES",
    "LIFECYCLE_MAX_LINES",
    "LIFECYCLE_TAIL_BYTES",
    "ProjectionResult",
    "RAW_LIFECYCLE_FILES",
    "RAW_ROLLOUT_FILE",
    "SAFE_JSON_FILES",
    "SAFE_OUTBOX_FILE",
    "SAFE_GENERATION_MANIFEST",
    "SAFE_LIFECYCLE_FILE",
    "SAFE_REVISION_FILE",
    "SAFE_ROLLOUT_FILE",
    "TRANSFORMS",
    "project_safe_inputs",
    "validate_safe_input_generation",
]
