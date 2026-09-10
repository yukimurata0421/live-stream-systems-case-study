from __future__ import annotations

import argparse
import json
import signal
import ssl
import threading
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from cra_dell_recovery.canonical import KeyRing, SignedMessageCodec, Signer
from cra_dell_recovery.schema import SchemaRegistry
from dell_recovery_agent.authority import AgentAuthorityLease
from dell_recovery_agent.execution import AgentService, FakePhysicalAdapter
from dell_recovery_agent.maintenance_snapshot import ShadowMaintenanceSnapshotCache
from dell_recovery_agent.server import ShadowAgentHTTPServer
from dell_recovery_agent.storage import DellStore
from dell_recovery_agent.target import FileTargetObserver


def load_config(path: Path) -> dict[str, Any]:
    return dict(json.loads(path.read_text(encoding="utf-8")))


def load_private(path: Path) -> Ed25519PrivateKey:
    key = serialization.load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("agent signing key must be Ed25519")
    return key


def load_public(path: Path) -> Ed25519PublicKey:
    key = serialization.load_pem_public_key(path.read_bytes())
    if not isinstance(key, Ed25519PublicKey):
        raise ValueError("CRA verification key must be Ed25519")
    return key


def main() -> None:
    parser = argparse.ArgumentParser(description="Dell Recovery Agent no-action shadow endpoint")
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = load_config(args.config)
    store = DellStore(Path(config["database"]), Path(config["migration"]))
    new_identity = not store.has_identity()
    if new_identity:
        store.bootstrap(
            agent_id=str(config["agent_id"]),
            installation_id=str(config["agent_installation_id"]),
            host_id=str(config["host_id"]),
            host_boot_id=str(config["host_boot_id"]),
            target_id=str(config["target_id"]),
        )
    store.assert_managed_target(str(config["target_id"]))
    if not new_identity:
        store.startup_recover(str(config["target_id"]))
    codec = SignedMessageCodec(
        SchemaRegistry(Path(config["contract_dir"])),
        Signer(str(config["agent_key_id"]), load_private(Path(config["agent_private_key"]))),
        KeyRing({str(config["cra_key_id"]): load_public(Path(config["cra_public_key"]))}),
    )
    service = AgentService(
        store,
        codec,
        FileTargetObserver(Path(config["target_snapshot"])),
        FakePhysicalAdapter(),
        critical_db_deadline_seconds=float(config.get("critical_db_deadline_seconds", 3.0)),
    )
    authority_lease = AgentAuthorityLease(
        store,
        codec,
        suspect_after_seconds=float(config.get("suspect_after_seconds", 4.0)),
        lease_ttl_seconds=float(config.get("lease_ttl_seconds", 15.0)),
        critical_db_deadline_seconds=float(config.get("critical_db_deadline_seconds", 3.0)),
    )
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.minimum_version = ssl.TLSVersion.TLSv1_3
    tls.load_cert_chain(str(config["tls_certificate"]), str(config["tls_private_key"]))
    tls.load_verify_locations(cafile=str(config["client_ca_certificate"]))
    tls.verify_mode = ssl.CERT_REQUIRED
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    with ShadowAgentHTTPServer(
        service,
        tls,
        host=str(config.get("listen_host", "127.0.0.1")),
        port=int(config.get("listen_port", 9443)),
        shadow_only=True,
        authority_lease=authority_lease,
        maintenance_snapshot_cache=(
            ShadowMaintenanceSnapshotCache(
                Path(config["maintenance_snapshot_cache"]),
                Path(config["maintenance_snapshot_schema"]) if config.get("maintenance_snapshot_schema") else None,
            )
            if config.get("maintenance_snapshot_cache")
            else None
        ),
    ):
        tick_interval = max(0.1, float(config.get("lease_tick_interval_seconds", 1.0)))
        while not stopped.wait(tick_interval):
            authority_lease.tick(str(config["target_id"]))


if __name__ == "__main__":
    main()
