#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from runtime_boundary import EffectLedger


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--initial-producer-id", default="legacy-in-pod")
    parser.add_argument("--initial-generation", type=int, default=1)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init")
    subparsers.add_parser("status")
    switch = subparsers.add_parser("switch")
    switch.add_argument("--expected-producer-id", required=True)
    switch.add_argument("--expected-generation", type=int, required=True)
    switch.add_argument("--new-producer-id", required=True)
    switch.add_argument("--new-generation", type=int, required=True)
    args = parser.parse_args()
    ledger = EffectLedger(
        args.ledger,
        initial_producer_id=args.initial_producer_id,
        initial_producer_generation=args.initial_generation,
        allow_initialize=args.command == "init",
    )
    try:
        if args.command in {"init", "status"}:
            result = {
                "initialized": args.command == "init",
                "authority": ledger.authority(),
                "in_flight_count": ledger.unresolved_count(),
            }
            code = 0
        else:
            decision = ledger.switch_authority(
                expected_producer_id=args.expected_producer_id,
                expected_generation=args.expected_generation,
                new_producer_id=args.new_producer_id,
                new_generation=args.new_generation,
            )
            result = {
                "accepted": decision.accepted,
                "reason": decision.reason,
                "producer_id": decision.producer_id,
                "producer_generation": decision.producer_generation,
                "authority_version": decision.authority_version,
                "in_flight_count": ledger.unresolved_count(),
            }
            code = 0 if decision.accepted else 2
        print(json.dumps(result, sort_keys=True))
        return code
    finally:
        ledger.close()


if __name__ == "__main__":
    raise SystemExit(main())
