from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from cra_dell_recovery.time import isoformat_utc

from .host_status import atomic_write_json

SCHEMA = "cra.clock_tracking_fact.v1"


def collect(output: Path, *, now: datetime | None = None) -> dict[str, object]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    synchronized = subprocess.run(
        ["timedatectl", "show", "--value", "-p", "NTPSynchronized"],
        check=True,
        capture_output=True,
        text=True,
        timeout=2,
    )
    tracking = subprocess.run(
        ["chronyc", "-c", "tracking"],
        check=True,
        capture_output=True,
        text=True,
        timeout=2,
    )
    value: dict[str, object] = {
        "schema": SCHEMA,
        "observed_at": isoformat_utc(current),
        "ntp_synchronized": synchronized.stdout.strip().lower() == "yes",
        "tracking_csv": tracking.stdout.strip(),
    }
    atomic_write_json(output, value, mode=0o640)
    os.chmod(output, 0o640, follow_symlinks=False)
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish a non-privileged local chrony tracking fact")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    value = collect(args.output)
    print(
        json.dumps(
            {
                "schema": value["schema"],
                "observed_at": value["observed_at"],
                "ntp_synchronized": value["ntp_synchronized"],
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
