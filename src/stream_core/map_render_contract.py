"""Bounded semantic contract reported by the live ADS-B MapLibre renderer."""

from __future__ import annotations

from typing import Mapping


SCHEMA = "stream_v3.map_semantic_render.v1"
REQUIRED_SOURCES = (
    "openmaptiles",
    "terrain-dem",
    "coverage",
    "range-rings",
    "range-labels",
    "aircraft",
)
REQUIRED_LAYERS = (
    "water",
    "coastline",
    "coverage-shadow",
    "coverage-line",
    "range-ring-shadow",
    "range-rings",
    "range-labels",
    "aircraft-icon",
)
REQUIRED_UI_ELEMENTS = (
    "mapLegends",
    "altitudeLegend",
    "precipitationStatus",
    "mapAttribution",
)


def empty_semantic_render_report() -> dict[str, object]:
    return {
        "schema": SCHEMA,
        "ok": False,
        "map_style_loaded": False,
        "render_context_healthy": False,
        "required_sources": {name: False for name in REQUIRED_SOURCES},
        "required_layers": {name: False for name in REQUIRED_LAYERS},
        "ui_elements": {name: False for name in REQUIRED_UI_ELEMENTS},
        "aircraft_sample_count": 0,
        "aircraft_source_feature_count": 0,
        "aircraft_rendered_feature_count": 0,
        "coverage_point_count": 0,
        "coverage_source_feature_count": 0,
        "range_ring_feature_count": 0,
        "range_label_feature_count": 0,
        "map_error_count": 0,
        "failed_checks": ["semantic_report_missing"],
    }


def _exact_booleans(value: object, names: tuple[str, ...], field: str) -> dict[str, bool]:
    if not isinstance(value, dict) or set(value) != set(names):
        raise ValueError(f"{field} must contain the exact required names")
    if any(not isinstance(value.get(name), bool) for name in names):
        raise ValueError(f"{field} values must be booleans")
    return {name: value[name] for name in names}


def _count(value: Mapping[str, object], name: str, *, maximum: int) -> int:
    result = value.get(name)
    if not isinstance(result, int) or isinstance(result, bool) or not 0 <= result <= maximum:
        raise ValueError(f"{name} is outside its bounded integer contract")
    return result


def normalize_semantic_render_report(value: object) -> dict[str, object]:
    if value is None:
        return empty_semantic_render_report()
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        raise ValueError("semantic render report schema is invalid")
    if not isinstance(value.get("map_style_loaded"), bool) or not isinstance(
        value.get("render_context_healthy"), bool
    ):
        raise ValueError("semantic render readiness values are invalid")
    sources = _exact_booleans(value.get("required_sources"), REQUIRED_SOURCES, "required_sources")
    layers = _exact_booleans(value.get("required_layers"), REQUIRED_LAYERS, "required_layers")
    ui_elements = _exact_booleans(value.get("ui_elements"), REQUIRED_UI_ELEMENTS, "ui_elements")
    aircraft_sample = _count(value, "aircraft_sample_count", maximum=100_000)
    aircraft_source = _count(value, "aircraft_source_feature_count", maximum=100_000)
    aircraft_rendered = _count(value, "aircraft_rendered_feature_count", maximum=100_000)
    coverage_points = _count(value, "coverage_point_count", maximum=1_000_000)
    coverage_features = _count(value, "coverage_source_feature_count", maximum=1)
    ring_features = _count(value, "range_ring_feature_count", maximum=3)
    label_features = _count(value, "range_label_feature_count", maximum=3)
    map_errors = _count(value, "map_error_count", maximum=10_000)
    failed: list[str] = []
    if value["map_style_loaded"] is not True:
        failed.append("map_style_loaded")
    if value["render_context_healthy"] is not True:
        failed.append("render_context_healthy")
    failed.extend(f"source:{name}" for name, present in sources.items() if not present)
    failed.extend(f"layer:{name}" for name, present in layers.items() if not present)
    failed.extend(f"ui:{name}" for name, visible in ui_elements.items() if not visible)
    if aircraft_sample != aircraft_source:
        failed.append("aircraft_source_count")
    if aircraft_rendered > aircraft_source:
        failed.append("aircraft_rendered_count")
    if coverage_features not in ({0} if coverage_points == 0 else {1}):
        failed.append("coverage_source_count")
    if ring_features != 3:
        failed.append("range_ring_count")
    if label_features != 3:
        failed.append("range_label_count")
    return {
        "schema": SCHEMA,
        "ok": not failed,
        "map_style_loaded": value["map_style_loaded"],
        "render_context_healthy": value["render_context_healthy"],
        "required_sources": sources,
        "required_layers": layers,
        "ui_elements": ui_elements,
        "aircraft_sample_count": aircraft_sample,
        "aircraft_source_feature_count": aircraft_source,
        "aircraft_rendered_feature_count": aircraft_rendered,
        "coverage_point_count": coverage_points,
        "coverage_source_feature_count": coverage_features,
        "range_ring_feature_count": ring_features,
        "range_label_feature_count": label_features,
        "map_error_count": map_errors,
        "failed_checks": failed,
    }
