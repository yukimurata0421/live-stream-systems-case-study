"""cra-central: keys, policy publication, audit endpoint and verification."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cra import policy as policy_mod
from cra import signing
from cra.central import server
from cra.central.store import CentralStore
from cra.central.verify import verify


def _config(path: Path) -> dict[str, Any]:
    raw: dict[str, Any] = json.loads(path.read_text())
    for name in ("db_path", "policy_envelope_path", "policy_key_path"):
        raw[name] = Path(raw[name])
    return raw


def cmd_keygen(args: argparse.Namespace) -> int:
    print(signing.generate_key(args.out))
    return 0


def cmd_publish(args: argparse.Namespace) -> int:
    """Validate, sign and publish a policy. The version must increase (I8)."""
    config = _config(args.config)
    store = CentralStore(config["db_path"])
    candidate = policy_mod.parse(json.loads(args.policy.read_text()))
    latest = store.latest_policy_version()
    if candidate.version <= latest:
        print(f"version must be greater than {latest}", file=sys.stderr)
        return 2
    envelope = policy_mod.sign_envelope(signing.load_private_key(config["policy_key_path"]), candidate)
    store.record_policy(envelope["policy"])
    target = config["policy_envelope_path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp")
    tmp.write_text(json.dumps(envelope, sort_keys=True, indent=1))
    os.replace(tmp, target)
    print(json.dumps(envelope["policy"], sort_keys=True))
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    config = _config(args.config)
    server.serve(
        CentralStore(config["db_path"]),
        config["policy_envelope_path"],
        config["agents"],
        config.get("listen", "0.0.0.0"),
        int(config.get("port", 8790)),
    )
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    config = _config(args.config)
    store = CentralStore(config["db_path"])
    report = verify(store.audit_records(args.agent), store.policies(), now=datetime.now(UTC))
    report["conflicting_records"] = store.conflict_count()
    print(json.dumps(report, indent=1, ensure_ascii=False))
    return 1 if report["violations"] or report["conflicting_records"] else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cra-central")
    sub = parser.add_subparsers(dest="command", required=True)
    keygen = sub.add_parser("keygen", help="create an Ed25519 key; prints the public key")
    keygen.add_argument("--out", type=Path, required=True)
    keygen.set_defaults(func=cmd_keygen)
    publish = sub.add_parser("publish-policy", help="sign and publish a new policy version")
    publish.add_argument("--config", type=Path, required=True)
    publish.add_argument("--policy", type=Path, required=True)
    publish.set_defaults(func=cmd_publish)
    serve = sub.add_parser("serve")
    serve.add_argument("--config", type=Path, required=True)
    serve.set_defaults(func=cmd_serve)
    check = sub.add_parser("verify", help="check audit logs against invariants; exit 1 on violation")
    check.add_argument("--config", type=Path, required=True)
    check.add_argument("--agent")
    check.set_defaults(func=cmd_verify)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
