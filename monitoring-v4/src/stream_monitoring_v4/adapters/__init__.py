"""Read-only source adapters for Monitoring v4."""

from .base import AdapterBatch, ReadOnlyAdapter
from .audio import LegacySubsystemAudioAdapter
from .external import ExternalBlackboxAdapter
from .factory import SHADOW_DOMAINS, state_file_adapters
from .monitoring import MonitoringSelfAdapter
from .reliability import InputQualityProjectionAdapter
from .rendering import MapRuntimeAdapter
from .viewer import ViewerSyntheticAdapter
from .youtube import YouTubeStateAdapter

__all__ = [
    "AdapterBatch",
    "ExternalBlackboxAdapter",
    "InputQualityProjectionAdapter",
    "LegacySubsystemAudioAdapter",
    "MapRuntimeAdapter",
    "MonitoringSelfAdapter",
    "ReadOnlyAdapter",
    "SHADOW_DOMAINS",
    "ViewerSyntheticAdapter",
    "YouTubeStateAdapter",
    "state_file_adapters",
]
