"""Owner-side recovery evidence API, NOT an observer daemon or DB opener.

The existing EffectLedger owner supplies a consistent, read-only connection
and current target. No CLI, service, socket, signing key, or automatic hook is
added here. The owner must publish the returned object in its own cycle.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import rfc8785

from cra_dell_recovery.recovery_history import history_hash, validate_policy
from cra_dell_recovery.time import isoformat_utc

from .model import validate_target

SCHEMA = "runtime.recovery_evidence.v1"
INDEPENDENT_SCHEMA = "runtime.recovery_evidence.v2"
LIFECYCLE_SCHEMA = "runtime.recovery_evidence.v3"


def source_hash(value: dict[str, Any]) -> str:
    # This owner exports unsigned JSON. Importing the signing module here
    # pulled cryptography/schema dependencies into the stream runtime image.
    return hashlib.sha256(rfc8785.dumps({"raw": value})).hexdigest()


def effect_safety(
    db: sqlite3.Connection,
    *,
    now: datetime,
    allowed_producers: list[str],
    target_identity: dict[str, Any] | None = None,
    history_policy: dict[str, Any] | None = None,
    independent_effects: bool = False,
) -> dict[str, Any]:
    if not db.in_transaction or db.execute("PRAGMA query_only").fetchone()[0] != 1:
        raise ValueError("RUNTIME_EVIDENCE_REQUIRES_QUERY_ONLY_SNAPSHOT")
    if not allowed_producers or any(not isinstance(p, str) or not p for p in allowed_producers):
        raise ValueError("RUNTIME_EVIDENCE_EFFECT_OWNER_REQUIRED")
    integrity = db.execute("PRAGMA quick_check").fetchone()[0]
    rows = db.execute("SELECT state, physical_attempt_count, effect_scope_id, identity_json FROM effect_scope_fences").fetchall()
    policy = validate_policy(history_policy, now=now)
    current_target = source_hash(target_identity) if target_identity is not None else None
    historical = []
    for entry in policy["retired_unknown"]:
        row = db.execute(
            "SELECT f.state, f.physical_attempt_count, f.identity_json, f.created_at, r.evidence_json, r.evidence_digest, r.recorded_at, "
            "f.action, f.result_json "
            "FROM effect_scope_fences f JOIN effect_scope_retirements r USING(effect_scope_id) WHERE f.effect_scope_id=?",
            (entry["effect_scope_id"],),
        ).fetchone()
        if row is None:
            raise ValueError("EFFECT_HISTORY_FROZEN_RECORD_MISSING")
        identity, evidence = json.loads(row[2]), json.loads(row[4])
        if (
            row[0] != "RETIRED_TARGET_OUTCOME_UNKNOWN"
            or row[1] != 1
            or row[3] != entry["created_at"]
            or row[6] != entry["retired_at"]
            or not isinstance(identity, dict)
            or not isinstance(evidence, dict)
            or source_hash(identity) != entry["target_sha256"]
            or source_hash(evidence) != entry["retirement_sha256"]
            or hashlib.sha256(row[4].encode()).hexdigest() != row[5]
            or row[7] != "restart_ffmpeg"
            or row[8] != row[4]
            or evidence.get("schema_version") != "runtime.retired_target_reconciliation_evidence.v1"
            or evidence.get("oracle") != "EXACT_TARGET_RETIRED_AND_HEALTHY_REPLACEMENT"
            or evidence.get("physical_effect_outcome") != "UNKNOWN"
            or type(evidence.get("automatic_retry_count")) is not int
            or evidence["automatic_retry_count"] != 0
            or type(evidence.get("physical_attempt_count")) is not int
            or evidence["physical_attempt_count"] != 1
            or evidence.get("before_target") != identity
            or not isinstance(evidence.get("observed_target"), dict)
            or evidence["observed_target"].get("pod_uid") == identity.get("pod_uid")
        ):
            raise ValueError("EFFECT_HISTORY_FROZEN_RECORD_CHANGED")
        if (current_target is None and not independent_effects) or current_target == entry["target_sha256"]:
            raise ValueError("EFFECT_HISTORY_RETIRED_TARGET_REAPPEARED_OR_CURRENT_UNKNOWN")
        historical.append(dict(entry))
    # Both generations of admitted requests matter. Counting only typed
    # requests hid legacy producers from this audit.
    producers = db.execute(
        "SELECT producer_id, operation FROM typed_effect_requests UNION ALL SELECT producer_id, operation FROM effect_requests"
    ).fetchall()
    proof_row = db.execute(
        "SELECT r.resolution, r.evidence_json, f.owner_request_id FROM effect_reconciliations r "
        "JOIN effect_scope_fences f USING(effect_scope_id) ORDER BY r.recorded_at DESC, r.reconciliation_id DESC LIMIT 1"
    ).fetchone()
    proof: dict[str, Any] = {}
    if proof_row is not None:
        evidence = json.loads(proof_row[1])
        if (
            isinstance(evidence, dict)
            and isinstance(evidence.get("before_target"), dict)
            and isinstance(evidence.get("observed_target"), dict)
        ):
            proof = {
                "resolution": proof_row[0],
                "action_id": proof_row[2],
                "before_target": source_hash(evidence["before_target"]),
                "after_target": source_hash(evidence["observed_target"]),
            }
    # Retirement never proves success. Only the exact, pre-epoch frozen set
    # is separated from actionable/new-epoch unresolved scopes.
    terminal = {"RECONCILED_EFFECT_OBSERVED", "RELEASED_NO_EFFECT", "EFFECT_FAILED"}
    unresolved = sum(row[0] not in terminal for row in rows)
    current_unresolved = sum(
        row[0] not in terminal and current_target is not None and source_hash(json.loads(row[3])) == current_target for row in rows
    )
    return {
        "observed_at": isoformat_utc(now),
        "integrity": integrity,
        "physical_attempt_count": sum(int(row[1]) for row in rows),
        "duplicate_attempt_count": sum(max(0, int(row[1]) - 1) for row in rows),
        "unresolved_scope_count": unresolved - len(historical),
        "unresolved_scope_count_total": unresolved,
        "current_target_unresolved_scope_count": None if independent_effects and current_target is None else current_unresolved,
        "current_target_sha256": current_target,
        "history_policy_sha256": history_hash(policy),
        "historical_retired_unknown": historical,
        "unauthorized_effect_count": sum(
            p not in allowed_producers or op not in {"restart_ffmpeg", "reconcile_ffmpeg"} for p, op in producers
        ),
        "last_reconciliation": proof,
    }


def runtime_activation(target: dict[str, Any], *, proc_root: Path = Path("/proc"), local_pid: int | None = None) -> dict[str, Any]:
    """Only the runtime owner reads target process data; never export it."""
    pid = local_pid if local_pid is not None else target.get("target_identity", {}).get("ffmpeg_pid")
    if type(pid) is not int or pid < 1:
        return {"bounded_termination_enabled": None, "rw_timeout_enabled": None}
    with (proc_root / str(pid) / "environ").open("rb") as source:
        raw = source.read(128 * 1024 + 1)
    if len(raw) > 128 * 1024:
        raise ValueError("RECOVERY_ACTIVATION_SOURCE_TOO_LARGE")
    selected: dict[bytes, bytes] = {}
    for entry in raw.split(b"\0"):
        key, _, value = entry.partition(b"=")
        if key in (b"FR_FFMPEG_FORCE_KILL_ENABLED", b"FFMPEG_RW_TIMEOUT_ENABLED", b"FR_FFMPEG_TERM_GRACE_SEC", b"FR_FFMPEG_KILL_WAIT_SEC"):
            selected[key] = value
    enabled = {b"1", b"true", b"yes", b"on"}
    try:
        term = float(selected.get(b"FR_FFMPEG_TERM_GRACE_SEC", b"2"))
        kill = float(selected.get(b"FR_FFMPEG_KILL_WAIT_SEC", b"1"))
        bounded = math.isfinite(term) and math.isfinite(kill) and 0 < term <= 2 and 0 < kill <= 1
    except ValueError:
        bounded = False
    rw_active = False
    if selected.get(b"FFMPEG_RW_TIMEOUT_ENABLED", b"").lower() in enabled:
        with (proc_root / str(pid) / "cmdline").open("rb") as source:
            command = source.read(128 * 1024 + 1)
        if len(command) > 128 * 1024:
            raise ValueError("RECOVERY_ACTIVATION_SOURCE_TOO_LARGE")
        arguments = command.split(b"\0")
        values = [arguments[i + 1] for i, arg in enumerate(arguments[:-1]) if arg == b"-rw_timeout"]
        rw_active = bool(values) and all(value.isdigit() and 0 < int(value) <= 15_000_000 for value in values)
    return {
        "bounded_termination_enabled": selected.get(b"FR_FFMPEG_FORCE_KILL_ENABLED", b"").lower() in enabled and bounded,
        "rw_timeout_enabled": rw_active,
    }


def owner_evidence(
    db: sqlite3.Connection,
    *,
    target_identity: dict[str, Any] | None,
    release_id: str,
    source_commit: str,
    stream_id: str,
    allowed_producers: list[str],
    now: datetime,
    proc_root: Path = Path("/proc"),
    local_pid: int | None = None,
    host_id: str | None = None,
    history_policy: dict[str, Any] | None = None,
    independent_effects: bool = False,
    host_boot_id: str | None = None,
) -> dict[str, Any]:
    """Call only inside the owner's bounded snapshot; caller pins identity.

    Missing activation data remains UNKNOWN. Database failure raises; the
    caller must NOT advance a prior export's timestamp on failure.
    """
    target = None if independent_effects and target_identity is None else validate_target(target_identity)
    if target is None and (not host_id or not host_boot_id):
        raise ValueError("RUNTIME_EVIDENCE_PHYSICAL_IDENTITY_REQUIRED")
    effects = effect_safety(
        db,
        now=now,
        allowed_producers=allowed_producers,
        target_identity=target,
        history_policy=history_policy,
        independent_effects=independent_effects,
    )
    if independent_effects:
        effects["target_status"] = "VALID" if target is not None else "UNKNOWN"
    try:
        activation = runtime_activation({"target_identity": target or {}}, proc_root=proc_root, local_pid=local_pid)
    except (OSError, ValueError):
        activation = {"bounded_termination_enabled": None, "rw_timeout_enabled": None}
    return {
        "schema": INDEPENDENT_SCHEMA if independent_effects else SCHEMA,
        "host_id": host_id if host_id is not None else (target or {})["host_id"],
        "host_boot_id": target["host_boot_id"] if target is not None else host_boot_id,
        "release_id": release_id,
        "source_commit": source_commit,
        "stream_id": stream_id,
        "target_sha256": source_hash(target) if target is not None else None,
        "observed_at": isoformat_utc(now),
        "valid_until": isoformat_utc(now + timedelta(seconds=45)),
        "effects": effects,
        "activation": activation,
    }
