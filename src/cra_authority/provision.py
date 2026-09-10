from __future__ import annotations

import argparse
import json
from pathlib import Path

from cra_authority.no_action_store import NoActionCentralStore
from cra_authority.runtime import RuntimeConfig, SingletonLock, _secure_read


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize the local CRA intent ledger; performs no network or effect action")
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = RuntimeConfig.load(args.config)
    value = config.value
    identity_path = args.config.with_name("cra-provisioning.json")
    identity = json.loads(_secure_read(identity_path, maximum_bytes=64 * 1024))
    required = {"host_id", "agent_id", "controller_instance_id", "agent_installation_id"}
    if not isinstance(identity, dict) or set(identity) != required:
        raise ValueError(f"provisioning fields must be exact: {sorted(required)}")
    if any(not isinstance(identity[name], str) or not identity[name].strip() for name in required):
        raise ValueError("provisioning identity values must be non-empty strings")
    with SingletonLock(Path(value["lock_file"])):
        store = NoActionCentralStore(Path(value["database"]), Path(value["migration"]))
        try:
            existing = store.read_one("SELECT count(*) FROM control_plane_identity")
            if existing is not None and int(existing[0]) == 0:
                store.bootstrap(
                    target_id=str(value["target_id"]),
                    host_id=str(identity["host_id"]),
                    agent_id=str(identity["agent_id"]),
                    controller_instance_id=str(identity["controller_instance_id"]),
                    agent_installation_id=str(identity["agent_installation_id"]),
                )
                result = "INITIALIZED"
            else:
                target = store.read_one(
                    "SELECT target_id,host_id,agent_id,authority_state FROM targets WHERE target_id=?",
                    (str(value["target_id"]),),
                )
                if (
                    target is None
                    or str(target["host_id"]) != str(identity["host_id"])
                    or str(target["agent_id"]) != str(identity["agent_id"])
                    or str(target["authority_state"]) != "SAFE_BLOCKED"
                ):
                    raise ValueError("existing CRA ledger identity does not match provisioning request")
                result = "ALREADY_INITIALIZED"
            print(
                json.dumps(
                    {
                        "result": result,
                        "database": str(value["database"]),
                        "authority_state": "SAFE_BLOCKED",
                        "formal_reconciliation_required": True,
                        "external_action_count": 0,
                    },
                    sort_keys=True,
                )
            )
        finally:
            store.close()


if __name__ == "__main__":
    main()
