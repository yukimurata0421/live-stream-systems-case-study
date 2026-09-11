"""Backward-compatible facade for the observation process subsystem."""

from stream_monitoring_v4.runtime.observation.models import ObserverRun
from stream_monitoring_v4.runtime.observation.service import Observer
from stream_monitoring_v4.runtime.observation.worker import (
    ActiveAdapter as _ActiveAdapter,
    adapter_process as _adapter_process,
)

__all__ = ["Observer", "ObserverRun"]
