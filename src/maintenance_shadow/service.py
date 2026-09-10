from __future__ import annotations

import argparse
import json
import signal
import threading
from pathlib import Path
from typing import Any

from maintenance_shadow.snapshot import ShadowSnapshotProducer
from maintenance_shadow.store import MaintenanceShadowStore


def load_config(path: Path) -> dict[str, Any]:
    return dict(json.loads(path.read_text(encoding="utf-8")))


def main() -> None:
    parser = argparse.ArgumentParser(description="Maintenance Protocol v2 audit-only coordinator shadow")
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = load_config(args.config)
    store = MaintenanceShadowStore(
        Path(config["database"]),
        Path(config["migration"]),
        producer_id=str(config["producer_id"]),
    )
    store.reconcile_startup()
    producer = ShadowSnapshotProducer(
        store,
        target_snapshot_path=Path(config["target_snapshot"]) if config.get("target_snapshot") else None,
        authority_projection_path=Path(config["authority_projection"]) if config.get("authority_projection") else None,
        output_path=Path(config["output_snapshot"]),
        ttl_seconds=float(config.get("ttl_seconds", 10.0)),
    )
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    interval = max(0.1, float(config.get("interval_seconds", 1.0)))
    try:
        while not stopped.is_set():
            producer.publish()
            stopped.wait(interval)
    finally:
        store.close()


if __name__ == "__main__":
    main()
