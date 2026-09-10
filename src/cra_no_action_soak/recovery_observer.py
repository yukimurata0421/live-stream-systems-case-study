"""Host-local observation adapter for the recovery-aware soak.

Dell/arena/Pi read fixed JSON files only: no DB or target-process access.
CRA alone delegates query-only Central DB reads to its own authority package.
Output/state paths are observation-owned; no network, signal, or shell calls.
The operator transports signed packets to CRA's admitted inbox separately.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cra_dell_recovery.canonical import Signer
from cra_dell_recovery.recovery_history import validate_policy
from cra_dell_recovery.time import isoformat_utc

from .host_status import atomic_write_json, read_regular_bytes
from .recovery_facts import (
    HostBinding,
    network_fact,
    owner_target_snapshot,
    platform_fact,
    publication_fact,
    runtime_evidence_fact,
    sign_facts,
    source_hash,
    strict_json_object,
    strict_object,
    transport_fact,
)

CONFIG_SCHEMA = "cra.recovery_observer_config.v1"
STATE_SCHEMA = "cra.recovery_observer_state.v1"
SOURCE_NAMES = {
    "dell": {"network", "target", "transport", "runtime_evidence"},
    "arena": {"network", "platform", "control_path"},
    "cra": {"runtime"},
    "raspi": {"local_artifact", "remote_artifact", "publisher", "network"},
}


def _private_key(path: Path) -> Ed25519PrivateKey:
    key = serialization.load_pem_private_key(read_regular_bytes(path, maximum_bytes=64 * 1024, secret=True), password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("RECOVERY_OBSERVER_KEY_NOT_ED25519")
    return key


def validate_config(config: dict[str, Any]) -> HostBinding:
    if config.get("schema") != CONFIG_SCHEMA or not isinstance(config.get("binding"), dict):
        raise ValueError("RECOVERY_OBSERVER_CONFIG_INVALID")
    binding = HostBinding(**config["binding"])
    fields = {"schema", "binding", "sources", "state_file", "output_file", "signing_private_key_file", "host_contract_file"}
    fields.update(
        {"dell": {"runtime_binding", "effect_history_policy"}, "arena": {"expected_video_id"}, "cra": {"database_file"}, "raspi": set()}[
            binding.role
        ]
    )
    source_names = SOURCE_NAMES[binding.role]
    runtime = config.get("runtime_binding")
    if binding.role == "dell" and isinstance(runtime, dict) and runtime.get("evidence_schema") == "runtime.recovery_evidence.v3":
        source_names = source_names - {"target"}
    if set(config) != fields or not isinstance(config.get("sources"), dict) or set(config["sources"]) != source_names:
        raise ValueError("RECOVERY_OBSERVER_CONFIG_FIELDS_INVALID")
    paths = [config[name] for name in fields if name.endswith("_file")] + list(config["sources"].values())
    if any(not isinstance(path, str) or not Path(path).is_absolute() for path in paths):
        raise ValueError("RECOVERY_OBSERVER_PATH_INVALID")
    if binding.role == "arena" and (not isinstance(config["expected_video_id"], str) or not config["expected_video_id"]):
        raise ValueError("RECOVERY_OBSERVER_BROADCAST_BINDING_REQUIRED")
    if binding.role == "dell":
        validate_policy(config["effect_history_policy"])
        runtime = config["runtime_binding"]
        if (
            not isinstance(runtime, dict)
            or not {"release_id", "source_commit", "target_host_id"}
            <= set(runtime)
            <= {"release_id", "source_commit", "target_host_id", "evidence_schema"}
            or runtime.get("evidence_schema", "runtime.recovery_evidence.v1")
            not in ("runtime.recovery_evidence.v1", "runtime.recovery_evidence.v2", "runtime.recovery_evidence.v3")
            or not isinstance(runtime["release_id"], str)
            or not runtime["release_id"]
            or not isinstance(runtime["source_commit"], str)
            or re.fullmatch(r"[0-9a-f]{40}", runtime["source_commit"]) is None
            or not isinstance(runtime["target_host_id"], str)
            or not runtime["target_host_id"]
        ):
            raise ValueError("RECOVERY_OBSERVER_RUNTIME_BINDING_REQUIRED")
    return binding


def build_facts(
    config: dict[str, Any],
    *,
    now: datetime | None = None,
    host_boot_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    role = config["binding"]["role"]
    sources: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    for name, path in config["sources"].items():
        try:
            sources[name] = (
                strict_json_object(read_regular_bytes(Path(path), maximum_bytes=64 * 1024, expected_owner_uid=0))
                if role == "dell" and name == "runtime_evidence"
                else strict_object(Path(path))
            )
        except (OSError, ValueError):
            sources[name] = {}
            errors.append(name)
    now = now or datetime.now(UTC)
    if role == "dell":
        target, transport = sources.get("target", {}), sources.get("transport", {})
        try:
            network = network_fact(sources.get("network", {}), host_boot_id=host_boot_id)
        except (TypeError, ValueError):
            network = {"state": "UNKNOWN", "observed_at": sources.get("network", {}).get("ts_utc"), "episode": None}
            errors.append("network_episode")
        lifecycle = None
        try:
            if host_boot_id is not None and sources.get("runtime_evidence", {}).get("host_boot_id") != host_boot_id:
                raise ValueError("RUNTIME_EVIDENCE_PHYSICAL_BOOT_MISMATCH")
            effects, activation = runtime_evidence_fact(
                sources.get("runtime_evidence", {}),
                target=target,
                binding=HostBinding(**config["binding"]),
                runtime_binding=config["runtime_binding"],
                now=now,
                history_policy=config["effect_history_policy"],
                physical_boot_id=host_boot_id,
            )
            if config["runtime_binding"].get("evidence_schema") == "runtime.recovery_evidence.v3":
                owner = sources["runtime_evidence"]
                target = owner_target_snapshot(owner)
                lifecycle = owner["lifecycle"]
        except (TypeError, ValueError):
            effects = {"observed_at": None, "integrity": "UNKNOWN"}
            activation = {"bounded_termination_enabled": None, "rw_timeout_enabled": None}
            errors.append("runtime_evidence")
        facts = {
            "network": network,
            "transport": transport_fact(target, transport, now=now),
            "effects": effects,
            "activation": activation,
        }
        if lifecycle is not None:
            facts["transport"]["lifecycle"] = lifecycle
    elif role == "arena":
        status = sources.get("control_path", {})
        facts = {
            "network": network_fact(sources.get("network", {})),
            "platform": platform_fact(
                sources.get("platform", {}), stream_id=config["binding"]["stream_id"], expected_video_id=config["expected_video_id"]
            ),
            "control_path": {
                "state": "UP" if status.get("status") == "READY" else "DOWN" if status else "UNKNOWN",
                "observed_at": status.get("observed_at"),
            },
        }
    elif role == "cra":
        # Deliberately absent from Dell/arena immutable artifacts.
        from cra_authority.recovery_safety import central_safety

        try:
            safety = central_safety(Path(config["database_file"]), sources.get("runtime", {}), now=now)
        except (OSError, ValueError):
            safety = {"observed_at": isoformat_utc(now), "integrity": "UNKNOWN"}
            errors.append("central_database")
        facts = {"safety": safety}
    elif role == "raspi":
        publisher = sources.get("publisher", {})
        facts = {
            "publication": publication_fact(
                local_generated_at=sources.get("local_artifact", {}).get("generated_at"),
                remote_generated_at=sources.get("remote_artifact", {}).get("generated_at"),
                upload_completed_at=publisher.get("completed_at"),
                upload_succeeded=publisher.get("status") == "SUCCEEDED",
                network_state=str(sources.get("network", {}).get("state", "UNKNOWN")),
                now=now,
            )
        }
    else:
        raise ValueError("RECOVERY_OBSERVER_ROLE_INVALID")
    hashes = {name: source_hash(value) for name, value in sources.items()}
    hashes["read_errors"] = source_hash({"names": errors})
    return facts, hashes


def observe(config: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    binding = validate_config(config)
    state_path, output_path = Path(config["state_file"]), Path(config["output_file"])
    inputs = [Path(p).resolve() for p in config["sources"].values()]
    inputs.append(Path(config["signing_private_key_file"]).resolve())
    inputs.append(Path(config["host_contract_file"]).resolve())
    if config.get("database_file"):
        inputs.append(Path(config["database_file"]).resolve())
    outputs = [state_path.resolve(), output_path.resolve(), state_path.with_suffix(".lock").resolve()]
    if (
        not state_path.is_absolute()
        or not output_path.is_absolute()
        or len(set(outputs)) != len(outputs)
        or any(path in inputs for path in outputs)
    ):
        raise ValueError("RECOVERY_OBSERVER_PATH_COLLISION")
    contract = strict_object(Path(config["host_contract_file"]))
    metadata = Path(config["host_contract_file"]).stat()
    if metadata.st_uid != 0 or metadata.st_mode & 0o022:
        raise ValueError("RECOVERY_OBSERVER_HOST_CONTRACT_NOT_ROOT_OWNED")
    if contract.get("host_id") != binding.host_id:
        raise ValueError("RECOVERY_OBSERVER_PHYSICAL_HOST_MISMATCH")
    state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = os.open(state_path.with_suffix(".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        if output_path.exists() and not state_path.exists():
            raise ValueError("RECOVERY_OBSERVER_STATE_MISSING")
        state: dict[str, Any] = (
            strict_object(state_path)
            if state_path.exists()
            else {
                "schema": STATE_SCHEMA,
                "host_boot_id": boot,
                "producer_id": str(uuid.uuid4()),
                "sequence": 0,
                "config_sha256": source_hash(config),
                "source_failure_count": 0,
            }
        )
        if state.get("schema") != STATE_SCHEMA or state.get("host_boot_id") != boot or state.get("config_sha256") != source_hash(config):
            raise ValueError("RECOVERY_OBSERVER_BOOT_ROTATION_REQUIRES_FRESH_EPOCH")
        # All file reads precede the wrapper timestamp. Per-source timestamps
        # remain untouched, including source failures and stale cache entries.
        facts, hashes = build_facts(config, now=now, host_boot_id=boot)
        current = now or datetime.now(UTC)
        failures = state["source_failure_count"] + int(hashes["read_errors"] != source_hash({"names": []}))
        packet = sign_facts(
            binding=binding,
            signer=Signer(binding.key_id, _private_key(Path(config["signing_private_key_file"]))),
            host_boot_id=boot,
            producer_id=state["producer_id"],
            sequence=state["sequence"] + 1,
            now=current,
            valid_until=current + timedelta(seconds=45),
            facts=facts,
            source_hashes=hashes,
            source_failure_count=failures,
        )
        # Reserve sequence first: crash may leave a gap but never reuse a
        # sequence for a different signed packet.
        updated = {**state, "sequence": packet["sequence"], "source_failure_count": failures}
        atomic_write_json(state_path, updated)
        try:
            atomic_write_json(output_path, packet)
        except OSError:
            atomic_write_json(state_path, {**updated, "source_failure_count": failures + 1})
            raise
        return packet
    finally:
        os.close(lock)


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish local signed recovery observations without control authority")
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    result = observe(strict_object(args.config, maximum_bytes=128 * 1024))
    print(json.dumps({k: result[k] for k in ("role", "sequence", "observed_at", "payload_sha256")}))


if __name__ == "__main__":
    main()
