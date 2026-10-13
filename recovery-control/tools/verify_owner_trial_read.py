"""Emit a deterministic whole-file read proof for an owner workflow trial."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import stat
from pathlib import Path
from typing import Any

CATALOG = "harness/scenarios/owner_observation_chaos_v1.json"
MAXIMUM_INPUT_BYTES = 2 * 1024 * 1024


def _object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("OWNER_TRIAL_JSON_DUPLICATE_KEY")
        value[key] = item
    return value


def read_object(path: Path) -> tuple[bytes, dict[str, Any]]:
    metadata = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= MAXIMUM_INPUT_BYTES:
        raise ValueError("OWNER_TRIAL_INPUT_INVALID")
    raw = path.read_bytes()
    value = json.loads(raw, object_pairs_hook=_object_pairs)
    if not isinstance(value, dict):
        raise ValueError("OWNER_TRIAL_INPUT_NOT_OBJECT")
    return raw, value


def _digest_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()).hexdigest()


def _bounded_identifier(value: Any) -> str | None:
    return value if isinstance(value, str) and len(value) <= 256 else None


def _binding_summary(binding: Any) -> dict[str, Any]:
    value = binding if isinstance(binding, dict) else {}
    probes = value.get("probes")
    nodeids = [probe.get("nodeid") for probe in probes if isinstance(probe, dict)] if isinstance(probes, list) else []
    return {
        "verification_case_id": _bounded_identifier(value.get("verification_case_id")),
        "invariant_id": _bounded_identifier(value.get("invariant_id")),
        "mechanism": _bounded_identifier(value.get("mechanism")),
        "probe_count": len(probes) if isinstance(probes, list) else None,
        "binding_sha256": _digest_json(binding),
        "nodeids_sha256": _digest_json(nodeids),
    }


def build_proof(
    trial_raw: bytes,
    trial: dict[str, Any],
    canonical_raw: bytes,
    canonical: dict[str, Any],
    observations_raw: bytes,
    observations: dict[str, Any],
) -> dict[str, Any]:
    trial_bindings = trial.get("assurance", {}).get("bindings")
    canonical_bindings = canonical.get("assurance", {}).get("bindings")
    if not isinstance(trial_bindings, list) or not isinstance(canonical_bindings, list):
        raise ValueError("OWNER_TRIAL_BINDINGS_INVALID")
    missing = [binding for binding in canonical_bindings if binding not in trial_bindings]
    extra = [binding for binding in trial_bindings if binding not in canonical_bindings]
    reconstructed = copy.deepcopy(canonical)
    reconstructed["assurance"]["bindings"] = [binding for binding in canonical_bindings if binding not in missing]
    whole_equal = reconstructed == trial
    exactly_one = len(missing) == 1 and not extra and whole_equal
    proof = {
        "schema": "cra.owner_trial_read_proof.v1",
        "trial_sha256": hashlib.sha256(trial_raw).hexdigest(),
        "canonical_sha256": hashlib.sha256(canonical_raw).hexdigest(),
        "observations_sha256": hashlib.sha256(observations_raw).hexdigest(),
        "trial_binding_count": len(trial_bindings),
        "canonical_binding_count": len(canonical_bindings),
        # Do not emit whole bindings here. The owner identity binding currently
        # carries dozens of probes; forwarding it through an agent event can
        # exceed the transport's retained command-output boundary. Hashes keep
        # the complete documents and binding contents source-bound while the
        # human-meaningful IDs and counts remain inspectable.
        "missing_bindings": [_binding_summary(binding) for binding in missing],
        "extra_binding_count": len(extra),
        "extra_bindings_sha256": _digest_json(extra),
        "whole_document_equal_after_removing_missing_bindings": whole_equal,
        "exactly_one_missing_and_no_other_difference": exactly_one,
        "observations": observations,
    }
    proof["read_proof_sha256"] = _digest_json(proof)
    return proof


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trial", type=Path, required=True)
    parser.add_argument("--canonical", type=Path, required=True)
    parser.add_argument("--observations", type=Path, required=True)
    args = parser.parse_args()
    trial_raw, trial = read_object(args.trial)
    canonical_raw, canonical = read_object(args.canonical)
    observations_raw, observations = read_object(args.observations)
    proof = build_proof(trial_raw, trial, canonical_raw, canonical, observations_raw, observations)
    print(json.dumps(proof, ensure_ascii=False, sort_keys=True))
    return 0 if proof["exactly_one_missing_and_no_other_difference"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
