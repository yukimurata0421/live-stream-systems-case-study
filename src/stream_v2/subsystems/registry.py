from __future__ import annotations

from ..legacy_component import LegacyComponent
from .local_delivery.legacy import components as local_delivery_components
from .monitoring.legacy import components as monitoring_components
from .music.legacy import components as music_components
from .rendering.legacy import components as rendering_components
from .youtube_lifecycle.legacy import components as youtube_lifecycle_components


def stream_components_by_subsystem() -> dict[str, list[LegacyComponent]]:
    return {
        "rendering": rendering_components(),
        "music": music_components(),
        "local_delivery": local_delivery_components(),
        "youtube_lifecycle": youtube_lifecycle_components(),
        "monitoring": monitoring_components(),
    }


def stream_components_flat() -> list[LegacyComponent]:
    out: list[LegacyComponent] = []
    for components in stream_components_by_subsystem().values():
        out.extend(components)
    return out


def stream_components_payload() -> dict[str, object]:
    by_subsystem = stream_components_by_subsystem()
    return {
        "schema_version": 1,
        "subsystems": {
            name: [component.to_dict() for component in components]
            for name, components in by_subsystem.items()
        },
        "missing": [component.to_dict() for component in stream_components_flat() if not component.exists],
    }
