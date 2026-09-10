from __future__ import annotations

import argparse
import json
from pathlib import Path

from maintenance_shadow.snapshot import read_json
from maintenance_shadow.store import MaintenanceShadowStore, ShadowState


def main() -> None:
    parser = argparse.ArgumentParser(description="No-effect Maintenance Coordinator Shadow control")
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--migration", type=Path, required=True)
    parser.add_argument("--producer-id", default="arena-maintenance-shadow")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status")
    start = subparsers.add_parser("start-rehearsal")
    start.add_argument("--target-snapshot", type=Path, required=True)
    start.add_argument("--maintenance-id")
    transition = subparsers.add_parser("transition")
    transition.add_argument("--maintenance-id", required=True)
    transition.add_argument("--generation", type=int, required=True)
    transition.add_argument("--to", choices=[state.value for state in ShadowState], required=True)
    transition.add_argument("--reason", required=True)
    args = parser.parse_args()
    store = MaintenanceShadowStore(args.database, args.migration, producer_id=args.producer_id, startup=False)
    try:
        if args.command == "status":
            proof = store.startup_proof()
            print(json.dumps({"state": dict(store.state()), "proof": proof.to_dict(), "physical_effect_count": 0}, sort_keys=True))
        elif args.command == "start-rehearsal":
            raw = read_json(args.target_snapshot)
            target = raw.get("target_identity") if raw else None
            identifier, generation = store.start_rehearsal(target, maintenance_id=args.maintenance_id)
            print(json.dumps({"maintenance_id": identifier, "maintenance_generation": generation, "physical_effect_count": 0}))
        else:
            store.transition(args.maintenance_id, args.generation, ShadowState(args.to), reason=args.reason)
            print(
                json.dumps(
                    {
                        "maintenance_id": args.maintenance_id,
                        "maintenance_generation": args.generation,
                        "state": args.to,
                        "physical_effect_count": 0,
                    }
                )
            )
    finally:
        store.close()


if __name__ == "__main__":
    main()
