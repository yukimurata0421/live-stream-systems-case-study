from __future__ import annotations

from typing import Protocol

from cra_authority.monitoring_evidence import MonitoringEvidenceProjection


class MonitoringV4Adapter(Protocol):
    """Read-only evidence boundary; no authorization or verdict methods exist."""

    def latest(self) -> MonitoringEvidenceProjection: ...


class FixtureMonitoringAdapter:
    def __init__(self, *projections: MonitoringEvidenceProjection) -> None:
        if not projections:
            raise ValueError("at least one Monitoring Evidence Projection is required")
        self._projections = list(projections)
        self._last = projections[-1]

    def latest(self) -> MonitoringEvidenceProjection:
        if self._projections:
            self._last = self._projections.pop(0)
        return self._last
