"""Versioned semantic contract for browser precipitation render heartbeat."""

from __future__ import annotations

import re


PRECIPITATION_RENDER_STATES = frozenset(
    {
        "warming_up",
        "unavailable",
        "stale",
        "no_rain",
        "layer_missing",
        "layer_mismatch",
        "layer_loaded",
        "layer_loaded_lkg",
    }
)


def empty_precipitation_render_report() -> dict[str, object]:
    return {
        "evaluated": False,
        "available": False,
        "fresh": False,
        "has_precipitation": None,
        "layer_loaded": False,
        "validtime": "",
        "layer_validtime": "",
        "state": "warming_up",
    }


def normalize_precipitation_render_report(value: object) -> dict[str, object]:
    """Validate and bound a browser precipitation-render report.

    A missing report is accepted as the v1 rolling-upgrade state. Once a
    browser sends the v2 object, contradictory combinations are rejected rather
    than converted into a healthy render signal.
    """

    if value is None:
        return empty_precipitation_render_report()
    if not isinstance(value, dict):
        raise ValueError("precipitation render report must be an object")

    bool_fields = ("evaluated", "available", "fresh", "layer_loaded")
    if any(not isinstance(value.get(name), bool) for name in bool_fields):
        raise ValueError("precipitation render booleans are invalid")
    has_precipitation = value.get("has_precipitation")
    if has_precipitation is not None and not isinstance(has_precipitation, bool):
        raise ValueError("precipitation presence is invalid")
    validtime = value.get("validtime")
    if not isinstance(validtime, str) or (validtime and re.fullmatch(r"\d{14}", validtime) is None):
        raise ValueError("precipitation validtime is invalid")
    layer_validtime = value.get("layer_validtime")
    if not isinstance(layer_validtime, str) or (
        layer_validtime and re.fullmatch(r"\d{14}", layer_validtime) is None
    ):
        raise ValueError("precipitation layer validtime is invalid")
    state = value.get("state")
    if state not in PRECIPITATION_RENDER_STATES:
        raise ValueError("precipitation render state is invalid")

    evaluated = value["evaluated"]
    available = value["available"]
    fresh = value["fresh"]
    layer_loaded = value["layer_loaded"]
    valid_generation = bool(validtime)
    valid_layer_generation = bool(layer_validtime)
    generation_matches = valid_generation and validtime == layer_validtime
    semantic_ok = {
        "warming_up": (
            not evaluated
            and has_precipitation is None
            and not fresh
            and not layer_loaded
            and not valid_layer_generation
        ),
        "unavailable": (
            evaluated and not available and not fresh and not layer_loaded and not valid_layer_generation
        ),
        "stale": (
            evaluated and not fresh and not layer_loaded and valid_generation and not valid_layer_generation
        ),
        "no_rain": (
            evaluated
            and available
            and fresh
            and has_precipitation is False
            and not layer_loaded
            and valid_generation
            and not valid_layer_generation
        ),
        "layer_missing": (
            evaluated
            and available
            and fresh
            and has_precipitation is True
            and not layer_loaded
            and valid_generation
            and not valid_layer_generation
        ),
        "layer_mismatch": (
            evaluated
            and available
            and fresh
            and has_precipitation is True
            and layer_loaded
            and valid_generation
            and valid_layer_generation
            and not generation_matches
        ),
        "layer_loaded": (
            evaluated
            and available
            and fresh
            and has_precipitation is True
            and layer_loaded
            and generation_matches
        ),
        "layer_loaded_lkg": (
            evaluated
            and not available
            and fresh
            and has_precipitation is True
            and layer_loaded
            and generation_matches
        ),
    }[str(state)]
    if not semantic_ok:
        raise ValueError("precipitation render report is contradictory")

    return {
        "evaluated": evaluated,
        "available": available,
        "fresh": fresh,
        "has_precipitation": has_precipitation,
        "layer_loaded": layer_loaded,
        "validtime": validtime,
        "layer_validtime": layer_validtime,
        "state": state,
    }
