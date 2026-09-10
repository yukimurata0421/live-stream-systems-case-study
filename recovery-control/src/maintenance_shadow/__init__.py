"""Audit-only Maintenance Protocol v2 coordinator shadow.

This package deliberately has no physical mutation adapter and no Kubernetes client.
"""

from maintenance_shadow.snapshot import ShadowSnapshotProducer
from maintenance_shadow.store import MaintenanceShadowStore, ShadowState

__all__ = ["MaintenanceShadowStore", "ShadowSnapshotProducer", "ShadowState"]
