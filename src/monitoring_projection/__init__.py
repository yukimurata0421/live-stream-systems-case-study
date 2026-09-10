"""Capability-free Monitoring v4 facts to CRA evidence projection boundary."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .producer import MonitoringProjectionProducer, ProjectionProducerConfig

__all__ = ["MonitoringProjectionProducer", "ProjectionProducerConfig"]


def __getattr__(name: str) -> Any:
    """Preserve the package API without pre-importing executable modules."""
    if name in __all__:
        from .producer import MonitoringProjectionProducer, ProjectionProducerConfig

        exports = {
            "MonitoringProjectionProducer": MonitoringProjectionProducer,
            "ProjectionProducerConfig": ProjectionProducerConfig,
        }
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
