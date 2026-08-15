#!/usr/bin/env python3
"""Serve the stream overlay, ADS-B data, map tiles, and processed weather."""

from __future__ import annotations

import argparse
import http.server
import json
import math
import os
import re
import socketserver
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from functools import partial
from pathlib import Path

try:  # package import in tests and tooling
    from .map_asset_manifest import verify_asset_manifest
    from .map_render_contract import (
        empty_semantic_render_report,
        normalize_semantic_render_report,
    )
    from .precipitation_render_contract import (
        empty_precipitation_render_report,
        normalize_precipitation_render_report,
    )
except ImportError:  # direct script execution in the runtime container
    from map_asset_manifest import verify_asset_manifest
    from map_render_contract import (
        empty_semantic_render_report,
        normalize_semantic_render_report,
    )
    from precipitation_render_contract import (
        empty_precipitation_render_report,
        normalize_precipitation_render_report,
    )

SCRIPT_PATH = Path(__file__).resolve()
BASE_DIR = SCRIPT_PATH.parents[2]
ActualRangeRecord = dict[str, object]


class OverlayHandler(http.server.SimpleHTTPRequestHandler):
    stream1090_url = "http://stream1090.lan/stream1090/"
    now_playing_file = Path("now_playing.txt")
    now_playing_json_file = Path("now_playing.json")
    actual_range_ledger_file = Path("/state/overlay/coverage/actual_range_ledger.sqlite3")
    actual_range_supplement_file = Path("/dev/shm/adsb-streamnew/overlay_actual_range_supplement.json")
    actual_range_supplement_hours = 24.0
    actual_range_bucket_sec = 300.0
    actual_range_max_nmi = 500.0
    actual_range_receiver_height_ft = 0.0
    # Keep one bogus decoded position from turning into a day-long range spike.
    actual_range_los_accept_ratio = 1.20
    actual_range_los_quarantine_ratio = 1.25
    actual_range_los_quarantine_repeat_count = 2
    actual_range_neighbor_support_degrees = 2
    actual_range_neighbor_support_margin_nmi = 20.0
    actual_range_aircraft_max_seen_pos_sec = 120.0
    actual_range_ledger_lock = threading.RLock()
    actual_range_ledger_status: dict[str, object] = {
        "schema": "stream_v3.actual_range_ledger_status.v1",
        "state": "not_initialized",
        "persisted": False,
    }
    openfreemap_tilejson_url = "https://tiles.openfreemap.org/planet"
    mapterhorn_tile_template = "https://tiles.mapterhorn.com/{z}/{x}/{y}.webp"
    map_tile_cache_max_entries = 256
    map_tile_cache: OrderedDict[str, tuple[bytes, str]] = OrderedDict()
    map_tile_cache_lock = threading.RLock()
    openfreemap_tile_template = ""
    openfreemap_tile_template_expires_at = 0.0
    precipitation_root = Path("/state/overlay/precipitation")
    render_status_lock = threading.RLock()
    render_ready_payload: dict[str, object] = {}
    render_ready_received_at = 0.0
    render_ready_max_age_sec = 30.0
    render_server_started_at = time.time()

    def do_GET(self) -> None:
        if self.handle_overlay_request(send_body=True):
            return
        super().do_GET()

    def do_HEAD(self) -> None:
        if self.handle_overlay_request(send_body=False):
            return
        super().do_HEAD()

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/render/ready":
            self.accept_render_ready()
            return
        self.send_error(405, "method not allowed")

    def handle_overlay_request(self, *, send_body: bool) -> bool:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/now_playing.txt":
            self.serve_now_playing_text(send_body=send_body)
            return True
        if parsed.path == "/now_playing.json":
            self.serve_now_playing_json(send_body=send_body)
            return True
        if parsed.path.startswith("/map-tiles/"):
            self.proxy_map_tile(parsed, send_body=send_body)
            return True
        if parsed.path.startswith("/weather/"):
            self.serve_precipitation_asset(parsed, send_body=send_body)
            return True
        if parsed.path == "/render/status.json":
            self.serve_render_status(send_body=send_body)
            return True
        if parsed.path == "/asset/manifest.json":
            self.serve_asset_manifest(send_body=send_body)
            return True
        if parsed.path == "/coverage/status.json":
            self.send_json_payload(type(self).actual_range_ledger_status_snapshot(), send_body=send_body)
            return True
        if parsed.path == "/stream1090" or parsed.path.startswith("/stream1090/"):
            self.proxy_stream1090(parsed, send_body=send_body)
            return True
        if parsed.path in {"/adsb/aircraft.json", "/adsb/receiver.json"}:
            self.proxy_adsb_json(parsed.path.rsplit("/", 1)[-1], send_body=send_body)
            return True
        return False

    def send_json_payload(self, payload: dict[str, object], *, send_body: bool = True) -> None:
        body = (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def accept_render_ready(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0 or length > 16_384:
            self.send_error(400, "invalid render-ready payload")
            return
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self.send_error(400, "invalid render-ready payload")
            return
        required = (
            isinstance(payload, dict)
            and payload.get("ready") is True
            and payload.get("map_tiles_ready") is True
            and payload.get("aircraft_sample_ready") is True
        )
        if not required:
            self.send_error(400, "render is not ready")
            return
        try:
            precipitation = normalize_precipitation_render_report(payload.get("precipitation"))
            semantic = normalize_semantic_render_report(payload.get("semantic"))
        except ValueError as exc:
            self.send_error(400, str(exc))
            return
        asset_identity = self.asset_identity_snapshot()
        asset_revision = payload.get("asset_revision")
        modern_report = "semantic" in payload or "asset_revision" in payload
        if modern_report and (
            semantic.get("ok") is not True
            or asset_identity.get("ok") is not True
            or not isinstance(asset_revision, str)
            or asset_revision != asset_identity.get("revision")
        ):
            self.send_error(400, "render semantic or asset identity contract failed")
            return
        accepted = {
            "ready": True,
            "map_tiles_ready": True,
            "aircraft_sample_ready": True,
            "reported_at_ms": payload.get("reported_at_ms"),
            "precipitation": precipitation,
            "semantic": semantic,
            "asset_revision": asset_revision if isinstance(asset_revision, str) else "",
        }
        with self.render_status_lock:
            type(self).render_ready_payload = accepted
            type(self).render_ready_received_at = time.time()
        self.send_json_payload({"accepted": True})

    def serve_render_status(self, *, send_body: bool = True) -> None:
        now = time.time()
        with self.render_status_lock:
            report = dict(type(self).render_ready_payload)
            received_at = type(self).render_ready_received_at
            started_at = type(self).render_server_started_at
        age_sec = now - received_at if received_at > 0 else None
        ready = bool(report.get("ready")) and age_sec is not None and age_sec <= self.render_ready_max_age_sec
        try:
            precipitation = normalize_precipitation_render_report(report.get("precipitation"))
        except ValueError:
            precipitation = empty_precipitation_render_report()
        try:
            semantic = normalize_semantic_render_report(report.get("semantic"))
        except ValueError:
            semantic = empty_semantic_render_report()
        asset_identity = self.asset_identity_snapshot()
        browser_asset_revision = report.get("asset_revision")
        asset_identity["browser_revision_match"] = bool(
            asset_identity.get("ok") is True
            and isinstance(browser_asset_revision, str)
            and browser_asset_revision == asset_identity.get("revision")
        )
        self.send_json_payload(
            {
                "schema": "stream_v3.render_ready.v2",
                "ready": ready,
                "state": "ready" if ready else "warming_up",
                "age_sec": round(age_sec, 3) if age_sec is not None else None,
                "server_uptime_sec": round(max(0.0, now - started_at), 3),
                "map_tiles_ready": report.get("map_tiles_ready") is True,
                "aircraft_sample_ready": report.get("aircraft_sample_ready") is True,
                "reported_at_ms": report.get("reported_at_ms"),
                "precipitation": precipitation,
                "semantic": semantic,
                "asset_identity": asset_identity,
            },
            send_body=send_body,
        )

    def asset_identity_snapshot(self) -> dict[str, object]:
        root = Path(self.directory)
        try:
            manifest = verify_asset_manifest(root)
        except (OSError, ValueError) as exc:
            return {
                "schema": "stream_v3.map_asset_identity.v1",
                "ok": False,
                "revision": "",
                "file_count": 0,
                "reason": type(exc).__name__,
            }
        files = manifest.get("files")
        return {
            "schema": "stream_v3.map_asset_identity.v1",
            "ok": True,
            "revision": manifest["revision"],
            "file_count": len(files) if isinstance(files, list) else 0,
            "reason": "",
        }

    def serve_asset_manifest(self, *, send_body: bool = True) -> None:
        identity = self.asset_identity_snapshot()
        if identity.get("ok") is not True:
            body = (json.dumps(identity, sort_keys=True, separators=(",", ":")) + "\n").encode(
                "utf-8"
            )
            self.send_response(503)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if send_body:
                self.wfile.write(body)
            return
        self.send_json_payload(identity, send_body=send_body)

    def serve_precipitation_asset(
        self,
        parsed: urllib.parse.ParseResult,
        *,
        send_body: bool = True,
    ) -> None:
        if parsed.path in {"/weather/status.json", "/weather/health.json"}:
            path = self.precipitation_root / parsed.path.rsplit("/", 1)[-1]
            content_type = "application/json"
        else:
            manifest_match = re.fullmatch(
                r"/weather/tiles/(\d{14})/manifest\.json",
                parsed.path,
            )
            match = re.fullmatch(
                r"/weather/tiles/(\d{14})/(\d{1,2})/(\d+)/(\d+)\.png",
                parsed.path,
            )
            if manifest_match:
                validtime = manifest_match.group(1)
                path = self.precipitation_root / "generations" / validtime / "manifest.json"
                content_type = "application/json"
            elif match:
                validtime, zoom_text, x_text, y_text = match.groups()
                zoom = int(zoom_text)
                x = int(x_text)
                y = int(y_text)
                if zoom > 10 or x >= 1 << zoom or y >= 1 << zoom:
                    self.send_error(404, "invalid precipitation tile")
                    return
                path = self.precipitation_root / "generations" / validtime / zoom_text / x_text / f"{y_text}.png"
                content_type = "image/png"
            else:
                self.send_error(404, "unknown precipitation asset")
                return

        try:
            body = path.read_bytes()
        except FileNotFoundError:
            self.send_error(404, "precipitation asset unavailable")
            return
        except OSError:
            self.send_error(503, "precipitation asset unavailable")
            return

        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def proxy_map_tile(self, parsed: urllib.parse.ParseResult, *, send_body: bool = True) -> None:
        vector_match = re.fullmatch(r"/map-tiles/openfreemap/(\d+)/(\d+)/(\d+)\.pbf", parsed.path)
        terrain_match = re.fullmatch(r"/map-tiles/terrain/(\d+)/(\d+)/(\d+)\.webp", parsed.path)
        if not vector_match and not terrain_match:
            self.send_error(404, "unknown map tile")
            return

        try:
            if vector_match:
                z, x, y = vector_match.groups()
                template = self.resolve_openfreemap_tile_template()
                upstream_url = template.replace("{z}", z).replace("{x}", x).replace("{y}", y)
                body, content_type = self.fetch_cached_map_asset(
                    upstream_url,
                    fallback_content_type="application/vnd.mapbox-vector-tile",
                )
            else:
                z, x, y = terrain_match.groups()
                upstream_url = self.mapterhorn_tile_template.format(z=z, x=x, y=y)
                try:
                    body, content_type = self.fetch_cached_map_asset(
                        upstream_url,
                        fallback_content_type="image/webp",
                    )
                except urllib.error.HTTPError as exc:
                    if exc.code != 404:
                        raise
                    neutral_path = Path(self.directory) / "adsb-map" / "neutral-terrain.webp"
                    body = neutral_path.read_bytes()
                    content_type = "image/webp"
                    self.store_cached_map_asset(upstream_url, body, content_type)
        except (urllib.error.URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
            body = (json.dumps({"error": type(exc).__name__}, separators=(",", ":")) + "\n").encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if send_body:
                self.wfile.write(body)
            return

        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    @classmethod
    def resolve_openfreemap_tile_template(cls) -> str:
        now = time.monotonic()
        with cls.map_tile_cache_lock:
            if cls.openfreemap_tile_template and now < cls.openfreemap_tile_template_expires_at:
                return cls.openfreemap_tile_template

        req = urllib.request.Request(
            cls.openfreemap_tilejson_url,
            headers={"User-Agent": "stream-overlay-map/1.0", "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as res:
            tilejson = json.loads(res.read().decode("utf-8"))
        tiles = tilejson.get("tiles") if isinstance(tilejson, dict) else None
        if not isinstance(tiles, list) or not tiles or not isinstance(tiles[0], str):
            raise ValueError("OpenFreeMap TileJSON has no tile template")
        template = tiles[0]
        parsed = urllib.parse.urlparse(template)
        if parsed.scheme != "https" or parsed.hostname != "tiles.openfreemap.org":
            raise ValueError("unexpected OpenFreeMap tile host")
        if not all(token in template for token in ("{z}", "{x}", "{y}")):
            raise ValueError("OpenFreeMap tile template is incomplete")
        with cls.map_tile_cache_lock:
            cls.openfreemap_tile_template = template
            cls.openfreemap_tile_template_expires_at = now + 900.0
        return template

    @classmethod
    def fetch_cached_map_asset(cls, url: str, *, fallback_content_type: str) -> tuple[bytes, str]:
        with cls.map_tile_cache_lock:
            cached = cls.map_tile_cache.get(url)
            if cached is not None:
                cls.map_tile_cache.move_to_end(url)
                return cached
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "stream-overlay-map/1.0", "Accept-Encoding": "identity"},
        )
        with urllib.request.urlopen(req, timeout=20) as res:
            body = res.read()
            content_type = res.headers.get("Content-Type", fallback_content_type).split(";", 1)[0]
        cls.store_cached_map_asset(url, body, content_type)
        return body, content_type

    @classmethod
    def store_cached_map_asset(cls, url: str, body: bytes, content_type: str) -> None:
        with cls.map_tile_cache_lock:
            cls.map_tile_cache[url] = (body, content_type)
            cls.map_tile_cache.move_to_end(url)
            while len(cls.map_tile_cache) > cls.map_tile_cache_max_entries:
                cls.map_tile_cache.popitem(last=False)

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def serve_now_playing_text(self, *, send_body: bool = True) -> None:
        try:
            body = self.now_playing_file.read_bytes()
            status = 200
        except OSError:
            body = b""
            status = 404

        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def serve_now_playing_json(self, *, send_body: bool = True) -> None:
        try:
            body = self.now_playing_json_file.read_bytes()
            status = 200
        except OSError:
            fallback = self.fallback_now_playing_json()
            if fallback is None:
                body = b""
                status = 404
            else:
                body = fallback
                status = 200

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def fallback_now_playing_json(self) -> bytes | None:
        try:
            title_line = self.now_playing_file.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        if not title_line:
            return None
        payload = {
            "schema": "now_playing_overlay_fallback/v1",
            "status": "playing",
            "now_playing": {
                "title": title_line,
                "title_line": title_line,
            },
        }
        return (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")

    def proxy_adsb_json(self, filename: str, *, send_body: bool = True) -> None:
        base = self.stream1090_url.rstrip("/") + "/"
        url = urllib.parse.urljoin(base, "data/" + filename)
        try:
            with urllib.request.urlopen(url, timeout=3) as res:
                body = res.read()
                status = res.status
                content_type = res.headers.get("Content-Type", "application/json")
        except (urllib.error.URLError, TimeoutError) as exc:
            body = ('{"error":"%s"}' % str(exc).replace('"', "'")).encode("utf-8")
            status = 502
            content_type = "application/json"

        if status == 200 and filename == "receiver.json":
            body = self.sanitize_receiver_json(body)
            content_type = "application/json"

        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def proxy_stream1090(self, parsed: urllib.parse.ParseResult, *, send_body: bool = True) -> None:
        base = self.stream1090_url.rstrip("/") + "/"
        rel_path = parsed.path.removeprefix("/stream1090").lstrip("/")
        url = urllib.parse.urljoin(base, rel_path)
        if parsed.query:
            url += "?" + parsed.query

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "stream-overlay-proxy/1.0"})
            with urllib.request.urlopen(req, timeout=8) as res:
                body = res.read()
                status = res.status
                content_type = res.headers.get("Content-Type", "application/octet-stream")
        except (urllib.error.URLError, TimeoutError) as exc:
            body = ('{"error":"%s"}' % str(exc).replace('"', "'")).encode("utf-8")
            status = 502
            content_type = "application/json"

        if "text/html" in content_type:
            body = self.inject_stream1090_css(body)
            content_type = "text/html; charset=utf-8"
        elif status == 200 and rel_path == "config.js":
            body = self.inject_stream1090_privacy_config(body)
            content_type = "application/javascript; charset=utf-8"
        elif status == 200 and rel_path == "data/outline.json":
            body = self.augment_actual_range_outline(body, base)
            content_type = "application/json"

        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    @staticmethod
    def inject_stream1090_css(body: bytes) -> bytes:
        text = body.decode("utf-8", errors="replace")
        injected = """
<style id="stream_overlay_proxy_css">
html,body,#map_container,#map_canvas,.ol-viewport{cursor:none !important;}
.ol-attribution,.ol-scale-line,#stream_plane_counter{display:none !important;visibility:hidden !important;opacity:0 !important;pointer-events:none !important;}
#update_error,#update_error_detail,#generic_error,#generic_error_detail,#js_error,#js_error_detail,#timers_paused,.error_box{display:none !important;visibility:hidden !important;opacity:0 !important;pointer-events:none !important;}
#stream_footer_notice{right:14px !important;bottom:10px !important;top:auto !important;}
</style>
"""
        if "</head>" in text:
            text = text.replace("</head>", injected + "</head>", 1)
        else:
            text = injected + text
        return text.encode("utf-8")

    @staticmethod
    def sanitize_receiver_json(body: bytes) -> bytes:
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return body
        if not isinstance(payload, dict):
            return body
        payload.pop("lat", None)
        payload.pop("lon", None)
        payload["receiver_location_hidden"] = True
        return (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")

    @staticmethod
    def inject_stream1090_privacy_config(body: bytes) -> bytes:
        text = body.decode("utf-8", errors="replace")
        injected = """

// stream-overlay privacy: hide the receiver position marker while preserving solid range guides.
SiteShow = false;
SiteCirclesLineDash = [];
"""
        if "stream-overlay privacy" in text:
            return text.encode("utf-8")
        return (text.rstrip() + injected + "\n").encode("utf-8")

    @classmethod
    def augment_actual_range_outline(cls, body: bytes, stream1090_base_url: str) -> bytes:
        try:
            outline = json.loads(body.decode("utf-8"))
            aircraft = cls.fetch_json(urllib.parse.urljoin(stream1090_base_url, "data/aircraft.json"), timeout=2)
            receiver = cls.fetch_json(urllib.parse.urljoin(stream1090_base_url, "data/receiver.json"), timeout=2)
            merged = cls.merge_actual_range_outline(outline, aircraft, receiver, time.time())
            return (json.dumps(merged, separators=(",", ":")) + "\n").encode("utf-8")
        except Exception:
            return body

    @staticmethod
    def fetch_json(url: str, timeout: float) -> object:
        req = urllib.request.Request(url, headers={"User-Agent": "stream-overlay-proxy/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as res:
            return json.loads(res.read().decode("utf-8"))

    @classmethod
    def merge_actual_range_outline(
        cls,
        outline: object,
        aircraft: object,
        receiver: object,
        now_ts: float,
    ) -> object:
        if not isinstance(outline, dict) or not isinstance(aircraft, dict) or not isinstance(receiver, dict):
            return outline
        site_lat = receiver.get("lat")
        site_lon = receiver.get("lon")
        if not isinstance(site_lat, (int, float)) or not isinstance(site_lon, (int, float)):
            return outline

        receiver_height_ft = cls.receiver_height_ft(receiver)
        base_records = cls.outline_records_by_bearing(outline, float(site_lat), float(site_lon))
        base_bearing_count = len(base_records)
        try:
            mergeable_supplement = cls.update_actual_range_ledger(
                aircraft,
                float(site_lat),
                float(site_lon),
                now_ts,
                receiver_height_ft,
                base_records,
            )
        except Exception as exc:
            # The upstream outline remains usable when the persistent ledger is unavailable.
            cls.record_actual_range_ledger_error(exc)
            mergeable_supplement = {}

        changed = False
        for key, record in mergeable_supplement.items():
            base = base_records.get(key)
            record_distance_m = float(record["distance_m"])
            if not base or record_distance_m > float(base["distance_m"]):
                base_records[key] = {
                    "lat": float(record["lat"]),
                    "lon": float(record["lon"]),
                    "alt": record.get("alt", 0),
                    "distance_m": record_distance_m,
                }
                changed = True

        last24h = outline.setdefault("actualRange", {}).setdefault("last24h", {})
        if changed:
            last24h["points"] = [
                [round(float(rec["lat"]), 4), round(float(rec["lon"]), 4), int(rec.get("alt") or 0)]
                for _key, rec in sorted(base_records.items(), key=lambda item: int(item[0]))
            ]
        status = cls.actual_range_ledger_status_snapshot()
        status["base_bearing_count"] = base_bearing_count
        status["merged_bearing_count"] = len(base_records)
        last24h["streamV3CoverageLedger"] = status
        return outline

    @classmethod
    def actual_range_ledger_status_snapshot(cls) -> dict[str, object]:
        with cls.actual_range_ledger_lock:
            return dict(cls.actual_range_ledger_status)

    @classmethod
    def record_actual_range_ledger_error(cls, exc: Exception) -> None:
        with cls.actual_range_ledger_lock:
            status = dict(cls.actual_range_ledger_status)
            status.update(
                {
                    "schema": "stream_v3.actual_range_ledger_status.v1",
                    "state": "degraded",
                    "persisted": cls.actual_range_ledger_file.exists(),
                    "last_error": type(exc).__name__,
                    "last_error_at_utc": cls.utc_timestamp(time.time()),
                }
            )
            cls.actual_range_ledger_status = status

    @classmethod
    def update_actual_range_ledger(
        cls,
        aircraft: dict,
        site_lat: float,
        site_lon: float,
        now_ts: float,
        receiver_height_ft: float,
        base_records: dict[str, ActualRangeRecord],
    ) -> dict[str, ActualRangeRecord]:
        """Persist time-bucketed observations and return the rolling maximum per bearing."""

        window_sec = max(60.0, cls.actual_range_supplement_hours * 3600.0)
        bucket_sec = max(60.0, cls.actual_range_bucket_sec)
        cutoff_ts = now_ts - window_sec
        with cls.actual_range_ledger_lock:
            cls.actual_range_ledger_file.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(str(cls.actual_range_ledger_file), timeout=3.0)
            connection.row_factory = sqlite3.Row
            try:
                connection.execute("PRAGMA busy_timeout = 3000")
                cls.initialize_actual_range_ledger(connection)
                metadata = cls.actual_range_ledger_metadata(connection)
                receiver_key = f"{site_lat:.6f},{site_lon:.6f},{receiver_height_ft:.1f}"
                receiver_reset = metadata.get("receiver_key") not in {None, receiver_key}
                if receiver_reset:
                    connection.execute("DELETE FROM actual_range_samples")
                    reset_count = cls.safe_int(metadata.get("receiver_reset_count", "0")) + 1
                    cls.set_actual_range_ledger_metadata(connection, "receiver_reset_count", str(reset_count))
                    cls.set_actual_range_ledger_metadata(connection, "last_receiver_reset_at", str(now_ts))
                    cls.set_actual_range_ledger_metadata(connection, "first_collected_at", str(now_ts))
                cls.set_actual_range_ledger_metadata(connection, "receiver_key", receiver_key)

                imported_count, imported_earliest = cls.import_legacy_actual_range_supplement(
                    connection,
                    cutoff_ts,
                    bucket_sec,
                    receiver_height_ft,
                )
                metadata = cls.actual_range_ledger_metadata(connection)
                if "first_collected_at" not in metadata:
                    first_collected_at = imported_earliest if imported_earliest is not None else now_ts
                    cls.set_actual_range_ledger_metadata(connection, "first_collected_at", str(first_collected_at))

                source_now = cls.coerce_source_epoch(aircraft.get("now"), now_ts)
                source_messages = aircraft.get("messages")
                previous_messages = cls.safe_int(metadata.get("last_source_messages", "0"))
                source_reset_count = cls.safe_int(metadata.get("source_reset_count", "0"))
                if isinstance(source_messages, (int, float)):
                    current_messages = max(0, int(source_messages))
                    if previous_messages > 0 and current_messages < previous_messages:
                        source_reset_count += 1
                        cls.set_actual_range_ledger_metadata(connection, "source_reset_count", str(source_reset_count))
                        cls.set_actual_range_ledger_metadata(connection, "last_source_reset_at", str(now_ts))
                    cls.set_actual_range_ledger_metadata(connection, "last_source_messages", str(current_messages))
                cls.set_actual_range_ledger_metadata(connection, "last_collected_at", str(now_ts))
                cls.store_actual_range_aircraft_samples(
                    connection,
                    aircraft,
                    site_lat,
                    site_lon,
                    source_now,
                    now_ts,
                    receiver_height_ft,
                    cutoff_ts,
                    bucket_sec,
                )
                connection.execute("DELETE FROM actual_range_samples WHERE observed_ts < ?", (cutoff_ts,))

                rows = list(
                    connection.execute(
                        """
                        SELECT bucket_ts, bearing, quality, lat, lon, alt, distance_m,
                               observed_ts, radio_los_nmi, los_ratio, repeat_count
                          FROM actual_range_samples
                         WHERE observed_ts >= ?
                         ORDER BY bearing ASC, distance_m DESC, observed_ts DESC
                        """,
                        (cutoff_ts,),
                    )
                )
                selected: dict[str, ActualRangeRecord] = {}
                valid_rows: list[sqlite3.Row] = []
                rejected_keys: list[tuple[int, int, str]] = []
                for row in rows:
                    key = str(int(row["bearing"]))
                    record: ActualRangeRecord = {
                        "lat": float(row["lat"]),
                        "lon": float(row["lon"]),
                        "alt": int(row["alt"]),
                        "distance_m": float(row["distance_m"]),
                        "updated_ts": float(row["observed_ts"]),
                        "radio_los_nmi": float(row["radio_los_nmi"]),
                        "los_ratio": float(row["los_ratio"]),
                        "los_status": str(row["quality"]),
                        "los_repeat_count": int(row["repeat_count"]),
                    }
                    if not cls.is_actual_range_supplement_plausible(
                        key,
                        float(record["distance_m"]),
                        record.get("alt", 0),
                        receiver_height_ft,
                    ):
                        rejected_keys.append((int(row["bucket_ts"]), int(row["bearing"]), str(row["quality"])))
                        continue
                    valid_rows.append(row)
                    if key not in selected and cls.is_actual_range_supplement_mergeable(
                        key,
                        record,
                        base_records,
                        receiver_height_ft,
                    ):
                        selected[key] = record
                if rejected_keys:
                    connection.executemany(
                        "DELETE FROM actual_range_samples WHERE bucket_ts = ? AND bearing = ? AND quality = ?",
                        rejected_keys,
                    )

                metadata = cls.actual_range_ledger_metadata(connection)
                first_collected_at = cls.coerce_float(metadata.get("first_collected_at"), now_ts)
                maturity_sec = max(0.0, now_ts - first_collected_at)
                maturity_ratio = min(1.0, maturity_sec / window_sec)
                observed_times = [float(row["observed_ts"]) for row in valid_rows]
                source_age_sec = max(0.0, now_ts - source_now)
                state = "ready" if maturity_ratio >= 0.999 else "warming_up"
                if not valid_rows:
                    state = "empty"
                if source_age_sec > cls.actual_range_aircraft_max_seen_pos_sec:
                    state = "source_stale"
                status: dict[str, object] = {
                    "schema": "stream_v3.actual_range_ledger_status.v1",
                    "state": state,
                    "persisted": True,
                    "storage_schema": "stream_v3.actual_range_ledger.sqlite.v1",
                    "window_sec": int(window_sec),
                    "bucket_sec": int(bucket_sec),
                    "maturity_ratio": round(maturity_ratio, 6),
                    "maturity_age_sec": round(maturity_sec, 3),
                    "sample_count": len(valid_rows),
                    "bucket_count": len({int(row["bucket_ts"]) for row in valid_rows}),
                    "bearing_count": len({int(row["bearing"]) for row in valid_rows}),
                    "selected_bearing_count": len(selected),
                    "source_sample_age_sec": round(source_age_sec, 3),
                    "source_reset_count": cls.safe_int(metadata.get("source_reset_count", "0")),
                    "receiver_reset_count": cls.safe_int(metadata.get("receiver_reset_count", "0")),
                    "legacy_records_imported": cls.safe_int(metadata.get("legacy_records_imported", "0")),
                    "oldest_sample_age_sec": round(max(0.0, now_ts - min(observed_times)), 3)
                    if observed_times
                    else None,
                    "newest_sample_age_sec": round(max(0.0, now_ts - max(observed_times)), 3)
                    if observed_times
                    else None,
                    "last_source_reset_at_utc": cls.optional_utc_timestamp(metadata.get("last_source_reset_at")),
                    "last_receiver_reset_at_utc": cls.optional_utc_timestamp(metadata.get("last_receiver_reset_at")),
                    "updated_at_utc": cls.utc_timestamp(now_ts),
                    "last_error": None,
                }
                connection.commit()
                cls.actual_range_ledger_status = status
                return selected
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    @staticmethod
    def initialize_actual_range_ledger(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS actual_range_samples (
                bucket_ts INTEGER NOT NULL,
                bearing INTEGER NOT NULL CHECK (bearing >= 0 AND bearing < 360),
                quality TEXT NOT NULL CHECK (quality IN ('accept', 'quarantine')),
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                alt INTEGER NOT NULL,
                distance_m REAL NOT NULL,
                observed_ts REAL NOT NULL,
                radio_los_nmi REAL NOT NULL,
                los_ratio REAL NOT NULL,
                repeat_count INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (bucket_ts, bearing, quality)
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS actual_range_samples_observed_idx ON actual_range_samples(observed_ts)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS actual_range_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        connection.execute("PRAGMA user_version = 1")

    @staticmethod
    def actual_range_ledger_metadata(connection: sqlite3.Connection) -> dict[str, str]:
        return {str(row[0]): str(row[1]) for row in connection.execute("SELECT key, value FROM actual_range_metadata")}

    @staticmethod
    def set_actual_range_ledger_metadata(connection: sqlite3.Connection, key: str, value: str) -> None:
        connection.execute(
            "INSERT INTO actual_range_metadata(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    @classmethod
    def import_legacy_actual_range_supplement(
        cls,
        connection: sqlite3.Connection,
        cutoff_ts: float,
        bucket_sec: float,
        receiver_height_ft: float,
    ) -> tuple[int, float | None]:
        metadata = cls.actual_range_ledger_metadata(connection)
        if metadata.get("legacy_import_complete") == "1":
            return 0, None
        try:
            raw = json.loads(cls.actual_range_supplement_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return 0, None
        if not isinstance(raw, dict) or raw.get("schema") != "overlay_actual_range_supplement/v1":
            return 0, None
        records = raw.get("records")
        if not isinstance(records, dict):
            return 0, None

        imported = 0
        earliest: float | None = None
        for key, value in records.items():
            if not isinstance(key, str) or not key.isdigit() or not isinstance(value, dict):
                continue
            if not all(isinstance(value.get(name), (int, float)) for name in ("lat", "lon", "distance_m", "updated_ts")):
                continue
            observed_ts = float(value["updated_ts"])
            if observed_ts < cutoff_ts:
                continue
            bearing = int(key) % 360
            distance_m = float(value["distance_m"])
            alt = value.get("alt", 0)
            if not cls.is_actual_range_supplement_plausible(key, distance_m, alt, receiver_height_ft):
                continue
            quality = cls.actual_range_los_status(distance_m, alt, receiver_height_ft)
            radio_los_nmi = cls.radio_los_nmi(cls.coerce_non_negative_float(alt), receiver_height_ft)
            repeat_count = max(1, cls.safe_int(value.get("los_repeat_count", 1)))
            bucket_ts = int(math.floor(observed_ts / bucket_sec) * bucket_sec)
            cls.upsert_actual_range_ledger_candidate(
                connection,
                bucket_ts=bucket_ts,
                bearing=bearing,
                quality=quality,
                lat=float(value["lat"]),
                lon=float(value["lon"]),
                alt=int(alt) if isinstance(alt, (int, float)) else 0,
                distance_m=distance_m,
                observed_ts=observed_ts,
                radio_los_nmi=radio_los_nmi,
                los_ratio=distance_m / 1852.0 / radio_los_nmi if radio_los_nmi > 0 else 0.0,
                repeat_count=repeat_count,
                increment_quarantine=False,
            )
            imported += 1
            earliest = observed_ts if earliest is None else min(earliest, observed_ts)
        cls.set_actual_range_ledger_metadata(connection, "legacy_import_complete", "1")
        cls.set_actual_range_ledger_metadata(connection, "legacy_records_imported", str(imported))
        return imported, earliest

    @classmethod
    def store_actual_range_aircraft_samples(
        cls,
        connection: sqlite3.Connection,
        aircraft: dict,
        site_lat: float,
        site_lon: float,
        source_now: float,
        now_ts: float,
        receiver_height_ft: float,
        cutoff_ts: float,
        bucket_sec: float,
    ) -> None:
        max_distance_m = cls.actual_range_max_nmi * 1852.0
        for ac in aircraft.get("aircraft", []):
            if not isinstance(ac, dict):
                continue
            lat = ac.get("lat")
            lon = ac.get("lon")
            seen_pos = ac.get("seen_pos", 0)
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                continue
            if not isinstance(seen_pos, (int, float)) or seen_pos < 0 or seen_pos > cls.actual_range_aircraft_max_seen_pos_sec:
                continue
            observed_ts = source_now - float(seen_pos)
            if observed_ts < cutoff_ts or observed_ts > now_ts + 5.0:
                continue
            distance_m, bearing_deg = range_distance_bearing_m(site_lat, site_lon, float(lat), float(lon))
            if distance_m <= 0 or distance_m > max_distance_m:
                continue
            bearing = int(round(bearing_deg)) % 360
            alt = ac.get("alt_baro")
            if not isinstance(alt, (int, float)):
                alt = ac.get("alt_geom", 0)
            quality = cls.actual_range_los_status(distance_m, alt, receiver_height_ft)
            if quality == "reject":
                continue
            radio_los_nmi = cls.radio_los_nmi(cls.coerce_non_negative_float(alt), receiver_height_ft)
            bucket_ts = int(math.floor(observed_ts / bucket_sec) * bucket_sec)
            cls.upsert_actual_range_ledger_candidate(
                connection,
                bucket_ts=bucket_ts,
                bearing=bearing,
                quality=quality,
                lat=float(lat),
                lon=float(lon),
                alt=int(alt) if isinstance(alt, (int, float)) else 0,
                distance_m=distance_m,
                observed_ts=observed_ts,
                radio_los_nmi=radio_los_nmi,
                los_ratio=distance_m / 1852.0 / radio_los_nmi if radio_los_nmi > 0 else 0.0,
                repeat_count=1,
                increment_quarantine=True,
            )

    @staticmethod
    def upsert_actual_range_ledger_candidate(
        connection: sqlite3.Connection,
        *,
        bucket_ts: int,
        bearing: int,
        quality: str,
        lat: float,
        lon: float,
        alt: int,
        distance_m: float,
        observed_ts: float,
        radio_los_nmi: float,
        los_ratio: float,
        repeat_count: int,
        increment_quarantine: bool,
    ) -> None:
        current = connection.execute(
            "SELECT distance_m, repeat_count FROM actual_range_samples "
            "WHERE bucket_ts = ? AND bearing = ? AND quality = ?",
            (bucket_ts, bearing, quality),
        ).fetchone()
        confirmations = repeat_count
        if current is not None and quality == "quarantine" and increment_quarantine:
            confirmations = max(confirmations, int(current["repeat_count"]) + 1)
        if current is not None and distance_m <= float(current["distance_m"]):
            if quality == "quarantine" and confirmations > int(current["repeat_count"]):
                connection.execute(
                    "UPDATE actual_range_samples SET repeat_count = ? "
                    "WHERE bucket_ts = ? AND bearing = ? AND quality = ?",
                    (confirmations, bucket_ts, bearing, quality),
                )
            return
        connection.execute(
            """
            INSERT INTO actual_range_samples(
                bucket_ts, bearing, quality, lat, lon, alt, distance_m,
                observed_ts, radio_los_nmi, los_ratio, repeat_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bucket_ts, bearing, quality) DO UPDATE SET
                lat = excluded.lat,
                lon = excluded.lon,
                alt = excluded.alt,
                distance_m = excluded.distance_m,
                observed_ts = excluded.observed_ts,
                radio_los_nmi = excluded.radio_los_nmi,
                los_ratio = excluded.los_ratio,
                repeat_count = excluded.repeat_count
            """,
            (
                bucket_ts,
                bearing,
                quality,
                lat,
                lon,
                alt,
                distance_m,
                observed_ts,
                radio_los_nmi,
                los_ratio,
                confirmations,
            ),
        )

    @staticmethod
    def coerce_source_epoch(value: object, fallback: float) -> float:
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            epoch = float(value)
            if epoch > 0 and epoch <= fallback + 300.0:
                return epoch
        return fallback

    @staticmethod
    def coerce_float(value: object, fallback: float) -> float:
        try:
            result = float(value)
        except (TypeError, ValueError):
            return fallback
        return result if math.isfinite(result) else fallback

    @staticmethod
    def utc_timestamp(value: float) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(value))

    @classmethod
    def optional_utc_timestamp(cls, value: object) -> str | None:
        timestamp = cls.coerce_float(value, -1.0)
        return cls.utc_timestamp(timestamp) if timestamp >= 0 else None

    @staticmethod
    def outline_records_by_bearing(outline: dict, site_lat: float, site_lon: float) -> dict[str, ActualRangeRecord]:
        points = outline.get("actualRange", {}).get("last24h", {}).get("points", [])
        records: dict[str, ActualRangeRecord] = {}
        if not isinstance(points, list):
            return records
        for point in points:
            if not isinstance(point, list) or len(point) < 2:
                continue
            lat, lon = point[0], point[1]
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                continue
            distance_m, bearing_deg = range_distance_bearing_m(site_lat, site_lon, float(lat), float(lon))
            key = str(int(round(bearing_deg)) % 360)
            alt = point[2] if len(point) > 2 and isinstance(point[2], (int, float)) else 0
            if key not in records or distance_m > float(records[key]["distance_m"]):
                records[key] = {"lat": float(lat), "lon": float(lon), "alt": alt, "distance_m": distance_m}
        return records

    @classmethod
    def load_actual_range_supplement(cls, now_ts: float) -> dict[str, ActualRangeRecord]:
        try:
            raw = json.loads(cls.actual_range_supplement_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(raw, dict) or raw.get("schema") != "overlay_actual_range_supplement/v1":
            return {}
        records = raw.get("records", {})
        if not isinstance(records, dict):
            return {}
        valid: dict[str, ActualRangeRecord] = {}
        for key, rec in records.items():
            if not isinstance(key, str) or not isinstance(rec, dict):
                continue
            if not all(isinstance(rec.get(k), (int, float)) for k in ("lat", "lon", "distance_m", "updated_ts")):
                continue
            valid[key] = rec
        return cls.prune_actual_range_supplement(valid, now_ts)

    @classmethod
    def save_actual_range_supplement(cls, records: dict[str, ActualRangeRecord]) -> None:
        try:
            cls.actual_range_supplement_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = cls.actual_range_supplement_file.with_suffix(".tmp")
            payload = {
                "schema": "overlay_actual_range_supplement/v1",
                "updated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "records": records,
            }
            tmp.write_text(json.dumps(payload, separators=(",", ":")) + "\n", encoding="utf-8")
            tmp.replace(cls.actual_range_supplement_file)
        except OSError:
            return

    @classmethod
    def prune_actual_range_supplement(
        cls,
        records: dict[str, ActualRangeRecord],
        now_ts: float,
    ) -> dict[str, ActualRangeRecord]:
        max_age = max(60.0, cls.actual_range_supplement_hours * 3600.0)
        return {
            key: rec
            for key, rec in records.items()
            if now_ts - float(rec.get("updated_ts", 0.0)) <= max_age
        }

    @classmethod
    def filter_actual_range_supplement(
        cls,
        records: dict[str, ActualRangeRecord],
        base_records: dict[str, ActualRangeRecord],
        receiver_height_ft: float | None = None,
    ) -> dict[str, ActualRangeRecord]:
        if receiver_height_ft is None:
            receiver_height_ft = cls.actual_range_receiver_height_ft
        return {
            key: rec
            for key, rec in records.items()
            if cls.is_actual_range_supplement_mergeable(key, rec, base_records, receiver_height_ft)
        }

    @classmethod
    def prune_rejected_actual_range_supplement(
        cls,
        records: dict[str, ActualRangeRecord],
        receiver_height_ft: float,
    ) -> dict[str, ActualRangeRecord]:
        return {
            key: rec
            for key, rec in records.items()
            if cls.is_actual_range_supplement_plausible(key, float(rec["distance_m"]), rec.get("alt", 0), receiver_height_ft)
        }

    @classmethod
    def is_actual_range_supplement_mergeable(
        cls,
        key: str,
        rec: ActualRangeRecord,
        base_records: dict[str, ActualRangeRecord],
        receiver_height_ft: float,
    ) -> bool:
        distance_m = float(rec["distance_m"])
        los_status = cls.actual_range_los_status(distance_m, rec.get("alt", 0), receiver_height_ft)
        if los_status == "accept":
            return True
        if los_status != "quarantine":
            return False

        repeat_count = cls.safe_int(rec.get("los_repeat_count", 0))
        if repeat_count >= cls.actual_range_los_quarantine_repeat_count:
            return True
        return cls.has_actual_range_neighbor_support(key, distance_m, base_records)

    @classmethod
    def is_actual_range_supplement_plausible(
        cls,
        key: str,
        distance_m: float,
        alt: object,
        receiver_height_ft: float | None = None,
    ) -> bool:
        if distance_m <= 0:
            return False
        max_distance_m = cls.actual_range_max_nmi * 1852.0
        if distance_m > max_distance_m:
            return False

        try:
            bearing = int(key) % 360
        except ValueError:
            return False

        if receiver_height_ft is None:
            receiver_height_ft = cls.actual_range_receiver_height_ft
        return cls.actual_range_los_status(distance_m, alt, receiver_height_ft) != "reject"

    @classmethod
    def has_actual_range_neighbor_support(
        cls,
        key: str,
        distance_m: float,
        base_records: dict[str, ActualRangeRecord],
    ) -> bool:
        try:
            bearing = int(key) % 360
        except ValueError:
            return False
        neighbor_distances = [
            float(base_records[str((bearing + offset) % 360)]["distance_m"])
            for offset in range(-cls.actual_range_neighbor_support_degrees, cls.actual_range_neighbor_support_degrees + 1)
            if str((bearing + offset) % 360) in base_records
        ]
        if not neighbor_distances:
            return False
        supported_distance_m = max(neighbor_distances) + cls.actual_range_neighbor_support_margin_nmi * 1852.0
        return distance_m <= supported_distance_m

    @classmethod
    def actual_range_los_status(cls, distance_m: float, alt: object, receiver_height_ft: float) -> str:
        aircraft_alt_ft = cls.coerce_non_negative_float(alt)
        receiver_height_ft = max(0.0, receiver_height_ft)
        radio_los_nmi = cls.radio_los_nmi(aircraft_alt_ft, receiver_height_ft)
        if radio_los_nmi <= 0:
            return "reject"
        distance_nmi = distance_m / 1852.0
        los_ratio = distance_nmi / radio_los_nmi
        if los_ratio <= cls.actual_range_los_accept_ratio:
            return "accept"
        if los_ratio <= cls.actual_range_los_quarantine_ratio:
            return "quarantine"
        return "reject"

    @staticmethod
    def radio_los_nmi(aircraft_alt_ft: float, receiver_height_ft: float) -> float:
        return 1.23 * (math.sqrt(max(0.0, aircraft_alt_ft)) + math.sqrt(max(0.0, receiver_height_ft)))

    @classmethod
    def receiver_height_ft(cls, receiver: dict) -> float:
        for key in ("receiver_height_ft", "antenna_height_ft", "site_alt_ft", "alt_ft", "height_ft"):
            value = receiver.get(key)
            if isinstance(value, (int, float)) and value >= 0:
                return float(value)
        for key in ("receiver_height_m", "antenna_height_m", "site_alt_m", "alt_m", "height_m"):
            value = receiver.get(key)
            if isinstance(value, (int, float)) and value >= 0:
                return float(value) * 3.28084
        return max(0.0, float(cls.actual_range_receiver_height_ft))

    @staticmethod
    def coerce_non_negative_float(value: object) -> float:
        if isinstance(value, (int, float)) and value > 0:
            return float(value)
        return 0.0

    @staticmethod
    def safe_int(value: object) -> int:
        if isinstance(value, bool):
            return 0
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value.strip())
            except ValueError:
                return 0
        return 0

    @classmethod
    def update_actual_range_supplement(
        cls,
        records: dict[str, ActualRangeRecord],
        aircraft: dict,
        site_lat: float,
        site_lon: float,
        now_ts: float,
        receiver_height_ft: float,
    ) -> None:
        max_distance_m = cls.actual_range_max_nmi * 1852.0
        for ac in aircraft.get("aircraft", []):
            if not isinstance(ac, dict):
                continue
            lat = ac.get("lat")
            lon = ac.get("lon")
            seen_pos = ac.get("seen_pos", 0)
            if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                continue
            if isinstance(seen_pos, (int, float)) and seen_pos > cls.actual_range_aircraft_max_seen_pos_sec:
                continue
            distance_m, bearing_deg = range_distance_bearing_m(site_lat, site_lon, float(lat), float(lon))
            if distance_m <= 0 or distance_m > max_distance_m:
                continue
            key = str(int(round(bearing_deg)) % 360)
            alt = ac.get("alt_baro")
            if not isinstance(alt, (int, float)):
                alt = ac.get("alt_geom", 0)
            los_status = cls.actual_range_los_status(distance_m, alt, receiver_height_ft)
            if los_status == "reject":
                continue
            alt_ft = int(alt) if isinstance(alt, (int, float)) else 0
            current = records.get(key)
            repeat_count = 1
            if current and los_status == "quarantine":
                repeat_count = cls.safe_int(current.get("los_repeat_count", 0)) + 1
            if not current or distance_m > float(current.get("distance_m", 0.0)):
                radio_los_nmi = cls.radio_los_nmi(cls.coerce_non_negative_float(alt), receiver_height_ft)
                records[key] = {
                    "lat": float(lat),
                    "lon": float(lon),
                    "alt": alt_ft,
                    "distance_m": distance_m,
                    "updated_ts": now_ts,
                    "radio_los_nmi": radio_los_nmi,
                    "los_ratio": distance_m / 1852.0 / radio_los_nmi if radio_los_nmi > 0 else 0.0,
                    "los_status": los_status,
                    "los_repeat_count": repeat_count,
                }
            elif los_status == "quarantine":
                current["updated_ts"] = now_ts
                current["los_status"] = los_status
                current["los_repeat_count"] = repeat_count


def range_distance_bearing_m(lat1: float, lon1: float, lat2: float, lon2: float) -> tuple[float, float]:
    to_rad = math.pi / 180.0
    phi1 = lat1 * to_rad
    phi2 = lat2 * to_rad
    d_lat = (lat2 - lat1) * to_rad
    d_lon = (lon2 - lon1) * to_rad
    a = math.sin(d_lat / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lon / 2.0) ** 2
    distance_m = 6_371_000.0 * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    y = math.sin(d_lon) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(d_lon)
    bearing_deg = (math.degrees(math.atan2(y, x)) + 360.0) % 360.0
    return distance_m, bearing_deg


def default_now_playing_json_file(now_playing_file: Path) -> Path:
    explicit = os.environ.get("NOW_PLAYING_JSON") or os.environ.get("NOW_PLAYING_SNAPSHOT_FILE")
    if explicit:
        return Path(explicit)
    if now_playing_file.name == "now_playing.txt":
        return now_playing_file.parent / "overlay" / "now_playing.json"
    return BASE_DIR / "ui" / "overlay" / "now_playing.json"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.environ.get("OVERLAY_BIND_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("OVERLAY_PORT", "18080")))
    parser.add_argument("--directory", default=os.environ.get("OVERLAY_DIR", str(BASE_DIR / "ui" / "overlay")))
    parser.add_argument("--stream1090-url", default=os.environ.get("STREAM1090_URL", OverlayHandler.stream1090_url))
    parser.add_argument(
        "--now-playing-file",
        default=os.environ.get("NOW_PLAYING_FILE", str(BASE_DIR / "now_playing.txt")),
    )
    parser.add_argument(
        "--now-playing-json-file",
        default=os.environ.get("NOW_PLAYING_JSON") or os.environ.get("NOW_PLAYING_SNAPSHOT_FILE"),
    )
    parser.add_argument(
        "--actual-range-ledger-file",
        default=os.environ.get(
            "OVERLAY_ACTUAL_RANGE_LEDGER_FILE",
            "/state/overlay/coverage/actual_range_ledger.sqlite3",
        ),
    )
    parser.add_argument(
        "--actual-range-supplement-file",
        default=os.environ.get(
            "OVERLAY_ACTUAL_RANGE_SUPPLEMENT_FILE",
            "/dev/shm/adsb-streamnew/overlay_actual_range_supplement.json",
        ),
    )
    parser.add_argument(
        "--actual-range-supplement-hours",
        type=float,
        default=float(os.environ.get("OVERLAY_ACTUAL_RANGE_SUPPLEMENT_HOURS", "24")),
    )
    parser.add_argument(
        "--actual-range-bucket-sec",
        type=float,
        default=float(os.environ.get("OVERLAY_ACTUAL_RANGE_BUCKET_SEC", "300")),
    )
    parser.add_argument(
        "--actual-range-receiver-height-ft",
        type=float,
        default=float(os.environ.get("OVERLAY_ACTUAL_RANGE_RECEIVER_HEIGHT_FT", "0")),
    )
    parser.add_argument(
        "--precipitation-root",
        default=os.environ.get("PRECIPITATION_ROOT", "/state/overlay/precipitation"),
    )
    args = parser.parse_args()

    OverlayHandler.stream1090_url = args.stream1090_url
    overlay_dir = Path(args.directory).resolve()
    OverlayHandler.now_playing_file = Path(args.now_playing_file).resolve()
    OverlayHandler.now_playing_json_file = (
        Path(args.now_playing_json_file).resolve()
        if args.now_playing_json_file
        else default_now_playing_json_file(OverlayHandler.now_playing_file).resolve()
    )
    OverlayHandler.actual_range_ledger_file = Path(args.actual_range_ledger_file).resolve()
    OverlayHandler.actual_range_supplement_file = Path(args.actual_range_supplement_file).resolve()
    OverlayHandler.actual_range_supplement_hours = args.actual_range_supplement_hours
    OverlayHandler.actual_range_bucket_sec = args.actual_range_bucket_sec
    OverlayHandler.actual_range_receiver_height_ft = args.actual_range_receiver_height_ft
    OverlayHandler.precipitation_root = Path(args.precipitation_root).resolve()
    OverlayHandler.render_ready_payload = {}
    OverlayHandler.render_ready_received_at = 0.0
    OverlayHandler.render_server_started_at = time.time()
    OverlayHandler.actual_range_ledger_status = {
        "schema": "stream_v3.actual_range_ledger_status.v1",
        "state": "not_initialized",
        "persisted": OverlayHandler.actual_range_ledger_file.exists(),
    }
    handler = partial(OverlayHandler, directory=str(overlay_dir))
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer((args.host, args.port), handler) as httpd:
        httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
