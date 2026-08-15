"""Pinned file identity for browser-visible ADS-B map runtime assets."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path


SCHEMA = "stream_v3.map_asset_manifest.v1"
MANIFEST_NAME = "adsb-map/asset-manifest.json"
MAX_ASSET_BYTES = 16 * 1024 * 1024
ASSET_FILES = (
    "index.html",
    "adsb-map/index.html",
    "adsb-map/map.css",
    "adsb-map/map.js",
    "adsb-map/precipitation_render.mjs",
    "adsb-map/solar_theme.mjs",
    "adsb-map/style.json",
    "adsb-map/airports.geojson",
    "adsb-map/neutral-terrain.webp",
    "adsb-map/ATTRIBUTION.md",
    "adsb-map/vendor/maplibre-gl.mjs",
    "adsb-map/vendor/maplibre-gl-shared.mjs",
    "adsb-map/vendor/maplibre-gl-worker.mjs",
    "adsb-map/vendor/maplibre-gl.css",
    "adsb-map/vendor/LICENSE-maplibre-gl.txt",
)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def build_asset_manifest(root: Path) -> dict[str, object]:
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("overlay asset root must be a real directory")
    files: list[dict[str, object]] = []
    for relative in ASSET_FILES:
        path = root.joinpath(*relative.split("/"))
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"map asset is missing or linked: {relative}")
        content = path.read_bytes()
        if not content or len(content) > MAX_ASSET_BYTES:
            raise ValueError(f"map asset size is invalid: {relative}")
        files.append(
            {
                "path": relative,
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )
    signed = {"schema": SCHEMA, "files": files}
    return {**signed, "revision": hashlib.sha256(_canonical(signed)).hexdigest()}


def verify_asset_manifest(root: Path) -> dict[str, object]:
    root = Path(root)
    manifest_path = root / MANIFEST_NAME
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("map asset manifest is missing or linked")
    raw = manifest_path.read_bytes()
    if not raw or len(raw) > 1024 * 1024:
        raise ValueError("map asset manifest size is invalid")
    try:
        declared = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("map asset manifest is invalid JSON") from exc
    expected = build_asset_manifest(root)
    if declared != expected:
        raise ValueError("map asset manifest does not match runtime files")
    revision = expected.get("revision")
    if not isinstance(revision, str) or re.fullmatch(r"[0-9a-f]{64}", revision) is None:
        raise ValueError("map asset manifest revision is invalid")
    return expected
