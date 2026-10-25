from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

import rfc8785

from cra_dell_recovery.models import TargetIdentity


def logical_generation_scope_id(action: str, target: TargetIdentity | Mapping[str, Any]) -> str:
    """Return the PID-independent identity for one logical FFmpeg generation.

    PID remains part of the exact execution snapshot, but it cannot open a new
    physical-effect fence for the same declared generation.
    """

    normalized_action = action.strip().lower()
    if normalized_action != "restart_ffmpeg":
        raise ValueError("effect scope action must be restart_ffmpeg")
    identity = target if isinstance(target, TargetIdentity) else TargetIdentity.from_dict(dict(target))
    logical_identity = identity.to_dict()
    logical_identity.pop("ffmpeg_pid")
    canonical = rfc8785.dumps(
        {
            "action": normalized_action,
            "logical_generation_identity": logical_identity,
        }
    )
    return hashlib.sha256(canonical).hexdigest()


def effect_scope_id(action: str, target: TargetIdentity | Mapping[str, Any]) -> str:
    """Return the durable physical-effect fence identity.

    Trace identifiers such as command_id and correlation_id are deliberately not
    part of this value. This legacy exact-snapshot identifier is retained for
    evidence compatibility; physical admission additionally uses
    :func:`logical_generation_scope_id`, which excludes PID.
    """

    normalized_action = action.strip().lower()
    if normalized_action != "restart_ffmpeg":
        raise ValueError("effect scope action must be restart_ffmpeg")
    identity = target if isinstance(target, TargetIdentity) else TargetIdentity.from_dict(dict(target))
    canonical = rfc8785.dumps(
        {
            "action": normalized_action,
            "exact_target_identity": identity.to_dict(),
        }
    )
    return hashlib.sha256(canonical).hexdigest()
