from __future__ import annotations

import http.server
import json
import shutil
import socketserver
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from functools import partial
from pathlib import Path
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "stream_core"))

import overlay_server  # type: ignore


class _ReusableTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def _serve(handler_cls: type[http.server.BaseHTTPRequestHandler]) -> tuple[_ReusableTCPServer, str]:
    server = _ReusableTCPServer(("127.0.0.1", 0), handler_cls)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    return server, f"http://{host}:{port}/"


def _semantic_report() -> dict[str, object]:
    return {
        "schema": "stream_v3.map_semantic_render.v1",
        "map_style_loaded": True,
        "render_context_healthy": True,
        "required_sources": {
            name: True
            for name in (
                "openmaptiles",
                "terrain-dem",
                "coverage",
                "range-rings",
                "range-labels",
                "aircraft",
            )
        },
        "required_layers": {
            name: True
            for name in (
                "water",
                "coastline",
                "coverage-shadow",
                "coverage-line",
                "range-ring-shadow",
                "range-rings",
                "range-labels",
                "aircraft-icon",
            )
        },
        "ui_elements": {
            name: True
            for name in (
                "mapLegends",
                "altitudeLegend",
                "precipitationStatus",
                "mapAttribution",
            )
        },
        "aircraft_sample_count": 3,
        "aircraft_source_feature_count": 3,
        "aircraft_rendered_feature_count": 2,
        "coverage_point_count": 12,
        "coverage_source_feature_count": 1,
        "range_ring_feature_count": 3,
        "range_label_feature_count": 3,
        "map_error_count": 0,
    }


class _Stream1090FixtureHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        body = b"<html><head></head><body>tar1090</body></html>"
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class OverlayActualRangeOutlineTests(unittest.TestCase):
    def setUp(self) -> None:
        self._coverage_state_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._coverage_state_dir.cleanup)
        ledger_patch = mock.patch.object(
            overlay_server.OverlayHandler,
            "actual_range_ledger_file",
            Path(self._coverage_state_dir.name) / "actual_range_ledger.sqlite3",
        )
        supplement_patch = mock.patch.object(
            overlay_server.OverlayHandler,
            "actual_range_supplement_file",
            Path(self._coverage_state_dir.name) / "missing_legacy_supplement.json",
        )
        status_patch = mock.patch.object(
            overlay_server.OverlayHandler,
            "actual_range_ledger_status",
            {
                "schema": "stream_v3.actual_range_ledger_status.v1",
                "state": "not_initialized",
                "persisted": False,
            },
        )
        ledger_patch.start()
        supplement_patch.start()
        status_patch.start()
        self.addCleanup(ledger_patch.stop)
        self.addCleanup(supplement_patch.stop)
        self.addCleanup(status_patch.stop)

    def test_render_ready_rejects_partial_report_and_expires_old_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            previous_payload = overlay_server.OverlayHandler.render_ready_payload
            previous_received_at = overlay_server.OverlayHandler.render_ready_received_at
            previous_started_at = overlay_server.OverlayHandler.render_server_started_at
            try:
                overlay_server.OverlayHandler.render_ready_payload = {}
                overlay_server.OverlayHandler.render_ready_received_at = 0.0
                overlay_server.OverlayHandler.render_server_started_at = time.time()
                handler = partial(overlay_server.OverlayHandler, directory=td)
                overlay, overlay_url = _serve(handler)
                try:
                    partial_body = json.dumps(
                        {
                            "ready": True,
                            "map_tiles_ready": True,
                            "aircraft_sample_ready": False,
                            "reported_at_ms": 123456,
                        }
                    ).encode("utf-8")
                    request = urllib.request.Request(
                        overlay_url + "render/ready",
                        data=partial_body,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with self.assertRaises(urllib.error.HTTPError) as rejected:
                        urllib.request.urlopen(request, timeout=3)
                    self.assertEqual(rejected.exception.code, 400)
                    rejected.exception.close()

                    with urllib.request.urlopen(overlay_url + "render/status.json", timeout=3) as res:
                        after_rejection = json.loads(res.read().decode("utf-8"))
                    self.assertFalse(after_rejection["ready"])
                    self.assertEqual(after_rejection["state"], "warming_up")

                    overlay_server.OverlayHandler.render_ready_payload = {
                        "ready": True,
                        "map_tiles_ready": True,
                        "aircraft_sample_ready": True,
                        "reported_at_ms": 654321,
                    }
                    overlay_server.OverlayHandler.render_ready_received_at = time.time() - 31.0
                    with urllib.request.urlopen(overlay_url + "render/status.json", timeout=3) as res:
                        expired = json.loads(res.read().decode("utf-8"))
                    self.assertFalse(expired["ready"])
                    self.assertEqual(expired["state"], "warming_up")
                    self.assertGreaterEqual(expired["age_sec"], 30.9)
                    self.assertEqual(expired["reported_at_ms"], 654321)
                finally:
                    overlay.shutdown()
                    overlay.server_close()
            finally:
                overlay_server.OverlayHandler.render_ready_payload = previous_payload
                overlay_server.OverlayHandler.render_ready_received_at = previous_received_at
                overlay_server.OverlayHandler.render_server_started_at = previous_started_at

    def test_render_ready_endpoint_requires_real_map_and_adsb_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            previous_payload = overlay_server.OverlayHandler.render_ready_payload
            previous_received_at = overlay_server.OverlayHandler.render_ready_received_at
            previous_started_at = overlay_server.OverlayHandler.render_server_started_at
            try:
                overlay_server.OverlayHandler.render_ready_payload = {}
                overlay_server.OverlayHandler.render_ready_received_at = 0.0
                overlay_server.OverlayHandler.render_server_started_at = 1.0
                handler = partial(overlay_server.OverlayHandler, directory=td)
                overlay, overlay_url = _serve(handler)
                try:
                    with urllib.request.urlopen(overlay_url + "render/status.json", timeout=3) as res:
                        before = json.loads(res.read().decode("utf-8"))
                    self.assertFalse(before["ready"])
                    self.assertEqual(before["state"], "warming_up")
                    self.assertFalse(before["map_tiles_ready"])
                    self.assertFalse(before["aircraft_sample_ready"])

                    body = json.dumps(
                        {
                            "ready": True,
                            "map_tiles_ready": True,
                            "aircraft_sample_ready": True,
                            "reported_at_ms": 123456,
                        }
                    ).encode("utf-8")
                    request = urllib.request.Request(
                        overlay_url + "render/ready",
                        data=body,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(request, timeout=3) as res:
                        accepted = json.loads(res.read().decode("utf-8"))
                    self.assertTrue(accepted["accepted"])

                    with urllib.request.urlopen(overlay_url + "render/status.json", timeout=3) as res:
                        after = json.loads(res.read().decode("utf-8"))
                    self.assertEqual(after["schema"], "stream_v3.render_ready.v2")
                    self.assertTrue(after["ready"])
                    self.assertEqual(after["state"], "ready")
                    self.assertTrue(after["map_tiles_ready"])
                    self.assertTrue(after["aircraft_sample_ready"])
                    self.assertEqual(after["reported_at_ms"], 123456)
                    self.assertEqual(after["precipitation"]["state"], "warming_up")
                    self.assertFalse(after["precipitation"]["evaluated"])
                    self.assertLessEqual(after["age_sec"], 1.0)
                finally:
                    overlay.shutdown()
                    overlay.server_close()
            finally:
                overlay_server.OverlayHandler.render_ready_payload = previous_payload
                overlay_server.OverlayHandler.render_ready_received_at = previous_received_at
                overlay_server.OverlayHandler.render_server_started_at = previous_started_at

    def test_modern_render_report_is_pinned_to_verified_assets_and_semantics(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            shutil.copy2(ROOT / "ui" / "overlay" / "index.html", root / "index.html")
            shutil.copytree(ROOT / "ui" / "overlay" / "adsb-map", root / "adsb-map")
            previous_payload = overlay_server.OverlayHandler.render_ready_payload
            previous_received_at = overlay_server.OverlayHandler.render_ready_received_at
            try:
                overlay_server.OverlayHandler.render_ready_payload = {}
                overlay_server.OverlayHandler.render_ready_received_at = 0.0
                handler = partial(overlay_server.OverlayHandler, directory=td)
                overlay, overlay_url = _serve(handler)
                try:
                    with urllib.request.urlopen(overlay_url + "asset/manifest.json", timeout=3) as res:
                        identity = json.loads(res.read())
                    self.assertTrue(identity["ok"])
                    self.assertRegex(identity["revision"], r"^[0-9a-f]{64}$")

                    body = json.dumps(
                        {
                            "ready": True,
                            "map_tiles_ready": True,
                            "aircraft_sample_ready": True,
                            "reported_at_ms": 123456,
                            "semantic": _semantic_report(),
                            "asset_revision": identity["revision"],
                        }
                    ).encode("utf-8")
                    request = urllib.request.Request(
                        overlay_url + "render/ready",
                        data=body,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(request, timeout=3) as res:
                        self.assertTrue(json.loads(res.read())["accepted"])
                    with urllib.request.urlopen(overlay_url + "render/status.json", timeout=3) as res:
                        status = json.loads(res.read())
                    self.assertTrue(status["semantic"]["ok"])
                    self.assertTrue(status["asset_identity"]["ok"])
                    self.assertTrue(status["asset_identity"]["browser_revision_match"])

                    wrong = json.loads(body)
                    wrong["asset_revision"] = "0" * 64
                    rejected = urllib.request.Request(
                        overlay_url + "render/ready",
                        data=json.dumps(wrong).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with self.assertRaises(urllib.error.HTTPError) as failure:
                        urllib.request.urlopen(rejected, timeout=3)
                    self.assertEqual(failure.exception.code, 400)
                    failure.exception.close()
                finally:
                    overlay.shutdown()
                    overlay.server_close()
            finally:
                overlay_server.OverlayHandler.render_ready_payload = previous_payload
                overlay_server.OverlayHandler.render_ready_received_at = previous_received_at

    def test_render_ready_preserves_validated_precipitation_generation_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            previous_payload = overlay_server.OverlayHandler.render_ready_payload
            previous_received_at = overlay_server.OverlayHandler.render_ready_received_at
            try:
                overlay_server.OverlayHandler.render_ready_payload = {}
                overlay_server.OverlayHandler.render_ready_received_at = 0.0
                handler = partial(overlay_server.OverlayHandler, directory=td)
                overlay, overlay_url = _serve(handler)
                try:
                    precipitation = {
                        "evaluated": True,
                        "available": True,
                        "fresh": True,
                        "has_precipitation": True,
                        "layer_loaded": True,
                        "validtime": "20260815013500",
                        "layer_validtime": "20260815013500",
                        "state": "layer_loaded",
                    }
                    body = json.dumps(
                        {
                            "ready": True,
                            "map_tiles_ready": True,
                            "aircraft_sample_ready": True,
                            "reported_at_ms": 123456,
                            "precipitation": precipitation,
                        }
                    ).encode("utf-8")
                    request = urllib.request.Request(
                        overlay_url + "render/ready",
                        data=body,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(request, timeout=3) as res:
                        accepted = json.loads(res.read().decode("utf-8"))
                    self.assertTrue(accepted["accepted"])

                    with urllib.request.urlopen(overlay_url + "render/status.json", timeout=3) as res:
                        status = json.loads(res.read().decode("utf-8"))
                    self.assertEqual(status["precipitation"], precipitation)
                finally:
                    overlay.shutdown()
                    overlay.server_close()
            finally:
                overlay_server.OverlayHandler.render_ready_payload = previous_payload
                overlay_server.OverlayHandler.render_ready_received_at = previous_received_at

    def test_render_ready_rejects_contradictory_precipitation_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            previous_payload = overlay_server.OverlayHandler.render_ready_payload
            previous_received_at = overlay_server.OverlayHandler.render_ready_received_at
            try:
                overlay_server.OverlayHandler.render_ready_payload = {}
                overlay_server.OverlayHandler.render_ready_received_at = 0.0
                handler = partial(overlay_server.OverlayHandler, directory=td)
                overlay, overlay_url = _serve(handler)
                try:
                    body = json.dumps(
                        {
                            "ready": True,
                            "map_tiles_ready": True,
                            "aircraft_sample_ready": True,
                            "reported_at_ms": 123456,
                            "precipitation": {
                                "evaluated": True,
                                "available": True,
                                "fresh": True,
                                "has_precipitation": True,
                                "layer_loaded": False,
                                "validtime": "20260815013500",
                                "layer_validtime": "",
                                "state": "layer_loaded",
                            },
                        }
                    ).encode("utf-8")
                    request = urllib.request.Request(
                        overlay_url + "render/ready",
                        data=body,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with self.assertRaises(urllib.error.HTTPError) as rejected:
                        urllib.request.urlopen(request, timeout=3)
                    self.assertEqual(rejected.exception.code, 400)
                    rejected.exception.close()

                    with urllib.request.urlopen(overlay_url + "render/status.json", timeout=3) as res:
                        status = json.loads(res.read().decode("utf-8"))
                    self.assertFalse(status["ready"])
                finally:
                    overlay.shutdown()
                    overlay.server_close()
            finally:
                overlay_server.OverlayHandler.render_ready_payload = previous_payload
                overlay_server.OverlayHandler.render_ready_received_at = previous_received_at

    def test_processed_precipitation_assets_are_served_from_local_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            weather = root / "weather"
            tile = weather / "generations" / "20260802080000" / "7" / "114" / "50.png"
            tile.parent.mkdir(parents=True)
            tile.write_bytes(b"processed-precipitation")
            manifest = tile.parents[2] / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema": "stream_v3.precipitation_generation_manifest.v1",
                        "validtime": "20260802080000",
                    }
                ),
                encoding="utf-8",
            )
            (weather / "status.json").write_text(
                json.dumps({"analysis_only": True, "validtime": "20260802080000"}),
                encoding="utf-8",
            )
            previous_root = overlay_server.OverlayHandler.precipitation_root
            overlay_server.OverlayHandler.precipitation_root = weather
            handler = partial(overlay_server.OverlayHandler, directory=td)
            overlay, overlay_url = _serve(handler)
            try:
                with urllib.request.urlopen(overlay_url + "weather/status.json", timeout=3) as res:
                    self.assertTrue(json.loads(res.read())["analysis_only"])
                    self.assertEqual(res.headers.get_content_type(), "application/json")
                with urllib.request.urlopen(
                    overlay_url + "weather/tiles/20260802080000/7/114/50.png",
                    timeout=3,
                ) as res:
                    self.assertEqual(res.read(), b"processed-precipitation")
                    self.assertEqual(res.headers.get_content_type(), "image/png")
                with urllib.request.urlopen(
                    overlay_url + "weather/tiles/20260802080000/manifest.json",
                    timeout=3,
                ) as res:
                    self.assertEqual(
                        json.loads(res.read())["schema"],
                        "stream_v3.precipitation_generation_manifest.v1",
                    )
                    self.assertEqual(res.headers.get_content_type(), "application/json")
                with self.assertRaises(urllib.error.HTTPError) as invalid:
                    urllib.request.urlopen(
                        overlay_url + "weather/tiles/20260802080000/7/999/50.png",
                        timeout=3,
                    )
                self.assertEqual(invalid.exception.code, 404)
                invalid.exception.close()
            finally:
                overlay.shutdown()
                overlay.server_close()
                overlay_server.OverlayHandler.precipitation_root = previous_root

    def test_openfreemap_tilejson_template_is_validated_and_cached(self) -> None:
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(
            {"tiles": ["https://tiles.openfreemap.org/planet/version/{z}/{x}/{y}.pbf"]}
        ).encode("utf-8")
        previous_template = overlay_server.OverlayHandler.openfreemap_tile_template
        previous_expiry = overlay_server.OverlayHandler.openfreemap_tile_template_expires_at
        try:
            overlay_server.OverlayHandler.openfreemap_tile_template = ""
            overlay_server.OverlayHandler.openfreemap_tile_template_expires_at = 0.0
            with mock.patch.object(urllib.request, "urlopen", return_value=response) as urlopen:
                first = overlay_server.OverlayHandler.resolve_openfreemap_tile_template()
                second = overlay_server.OverlayHandler.resolve_openfreemap_tile_template()

            self.assertEqual(first, "https://tiles.openfreemap.org/planet/version/{z}/{x}/{y}.pbf")
            self.assertEqual(second, first)
            self.assertEqual(urlopen.call_count, 1)
        finally:
            overlay_server.OverlayHandler.openfreemap_tile_template = previous_template
            overlay_server.OverlayHandler.openfreemap_tile_template_expires_at = previous_expiry

    def test_map_vector_tile_route_proxies_same_origin_content(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            handler = partial(overlay_server.OverlayHandler, directory=td)
            with (
                mock.patch.object(
                    overlay_server.OverlayHandler,
                    "resolve_openfreemap_tile_template",
                    return_value="https://tiles.openfreemap.org/planet/version/{z}/{x}/{y}.pbf",
                ),
                mock.patch.object(
                    overlay_server.OverlayHandler,
                    "fetch_cached_map_asset",
                    return_value=(b"vector-tile", "application/vnd.mapbox-vector-tile"),
                ),
            ):
                overlay, overlay_url = _serve(handler)
                try:
                    with urllib.request.urlopen(overlay_url + "map-tiles/openfreemap/6/57/25.pbf", timeout=3) as res:
                        self.assertEqual(res.read(), b"vector-tile")
                        self.assertEqual(res.headers.get_content_type(), "application/vnd.mapbox-vector-tile")
                finally:
                    overlay.shutdown()
                    overlay.server_close()

    def test_missing_terrain_tile_uses_neutral_terrarium_asset(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            neutral = root / "adsb-map" / "neutral-terrain.webp"
            neutral.parent.mkdir(parents=True)
            neutral.write_bytes(b"neutral-terrain")
            error = urllib.error.HTTPError("https://tiles.mapterhorn.com/6/57/25.webp", 404, "missing", {}, None)
            handler = partial(overlay_server.OverlayHandler, directory=td)
            with mock.patch.object(
                overlay_server.OverlayHandler,
                "fetch_cached_map_asset",
                side_effect=error,
            ), mock.patch.object(overlay_server.OverlayHandler, "store_cached_map_asset"):
                overlay, overlay_url = _serve(handler)
                try:
                    with urllib.request.urlopen(overlay_url + "map-tiles/terrain/6/57/25.webp", timeout=3) as res:
                        self.assertEqual(res.read(), b"neutral-terrain")
                        self.assertEqual(res.headers.get_content_type(), "image/webp")
                finally:
                    overlay.shutdown()
                    overlay.server_close()

    def test_receiver_json_sanitizer_removes_site_coordinates(self) -> None:
        body = b'{"lat":35.0,"lon":139.0,"readsb":true}\n'

        sanitized = overlay_server.OverlayHandler.sanitize_receiver_json(body)

        payload = sanitized.decode("utf-8")
        self.assertNotIn('"lat"', payload)
        self.assertNotIn('"lon"', payload)
        self.assertIn('"receiver_location_hidden":true', payload)

    def test_privacy_config_hides_site_marker_only(self) -> None:
        body = b"SiteShow = true;\nSiteCircles = true;\nactual_range_show = true;\n"

        injected = overlay_server.OverlayHandler.inject_stream1090_privacy_config(body).decode("utf-8")

        self.assertIn("SiteShow = false;", injected)
        self.assertIn("SiteCirclesLineDash = [];", injected)
        self.assertNotIn("SiteCircles = false;", injected)
        self.assertNotIn("actual_range_show = false;", injected)

    def test_stream1090_css_hides_native_error_boxes(self) -> None:
        body = b"<html><head></head><body>tar1090</body></html>"

        injected = overlay_server.OverlayHandler.inject_stream1090_css(body).decode("utf-8")

        self.assertIn("#update_error", injected)
        self.assertIn("#generic_error", injected)
        self.assertIn(".error_box", injected)
        self.assertIn("display:none !important", injected)

    def test_now_playing_json_is_served_from_runtime_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            snapshot = root / "state" / "overlay" / "now_playing.json"
            snapshot.parent.mkdir(parents=True)
            snapshot.write_text(
                json.dumps({"now_playing": {"title": "Runtime Track"}}) + "\n",
                encoding="utf-8",
            )
            previous_json = overlay_server.OverlayHandler.now_playing_json_file
            try:
                overlay_server.OverlayHandler.now_playing_json_file = snapshot
                handler = partial(overlay_server.OverlayHandler, directory=td)
                overlay, overlay_url = _serve(handler)
                try:
                    with urllib.request.urlopen(overlay_url + "now_playing.json", timeout=3) as res:
                        payload = json.loads(res.read().decode("utf-8"))
                    self.assertEqual(payload["now_playing"]["title"], "Runtime Track")
                finally:
                    overlay.shutdown()
                    overlay.server_close()
            finally:
                overlay_server.OverlayHandler.now_playing_json_file = previous_json

    def test_now_playing_json_falls_back_to_text_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            now_playing = root / "now_playing.txt"
            missing_snapshot = root / "missing" / "now_playing.json"
            now_playing.write_text("Now Playing: Text Track\n", encoding="utf-8")
            previous_text = overlay_server.OverlayHandler.now_playing_file
            previous_json = overlay_server.OverlayHandler.now_playing_json_file
            try:
                overlay_server.OverlayHandler.now_playing_file = now_playing
                overlay_server.OverlayHandler.now_playing_json_file = missing_snapshot
                handler = partial(overlay_server.OverlayHandler, directory=td)
                overlay, overlay_url = _serve(handler)
                try:
                    with urllib.request.urlopen(overlay_url + "now_playing.json", timeout=3) as res:
                        payload = json.loads(res.read().decode("utf-8"))
                    self.assertEqual(payload["now_playing"]["title_line"], "Now Playing: Text Track")
                finally:
                    overlay.shutdown()
                    overlay.server_close()
            finally:
                overlay_server.OverlayHandler.now_playing_file = previous_text
                overlay_server.OverlayHandler.now_playing_json_file = previous_json

    def test_stream1090_head_proxy_returns_headers_without_body(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            upstream, upstream_url = _serve(_Stream1090FixtureHandler)
            try:
                previous_url = overlay_server.OverlayHandler.stream1090_url
                overlay_server.OverlayHandler.stream1090_url = upstream_url
                handler = partial(overlay_server.OverlayHandler, directory=td)
                overlay, overlay_url = _serve(handler)
                try:
                    req = urllib.request.Request(overlay_url + "stream1090/", method="HEAD")
                    with urllib.request.urlopen(req, timeout=3) as res:
                        body = res.read()
                        self.assertEqual(res.status, 200)
                        self.assertIn("text/html", res.headers.get("Content-Type", ""))
                        self.assertGreater(int(res.headers.get("Content-Length", "0")), 0)
                        self.assertEqual(body, b"")
                finally:
                    overlay.shutdown()
                    overlay.server_close()
                    overlay_server.OverlayHandler.stream1090_url = previous_url
            finally:
                upstream.shutdown()
                upstream.server_close()

    def test_fresh_aircraft_extends_outline_bucket(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.object(
                overlay_server.OverlayHandler,
                "actual_range_supplement_file",
                Path(td) / "supplement.json",
            ):
                outline = {"actualRange": {"last24h": {"points": [[0.0, 1.0, 30000]]}}}
                aircraft = {
                    "aircraft": [
                        {
                            "lat": 0.0,
                            "lon": 2.0,
                            "alt_baro": 36000,
                            "seen_pos": 1.0,
                            "type": "adsb_icao",
                        }
                    ]
                }
                receiver = {"lat": 0.0, "lon": 0.0}

                merged = overlay_server.OverlayHandler.merge_actual_range_outline(
                    outline,
                    aircraft,
                    receiver,
                    1000.0,
                )

                points = merged["actualRange"]["last24h"]["points"]
                self.assertEqual(len(points), 1)
                self.assertAlmostEqual(points[0][1], 2.0, places=4)
                self.assertEqual(points[0][2], 36000)

    def test_stale_aircraft_does_not_extend_outline(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.object(
                overlay_server.OverlayHandler,
                "actual_range_supplement_file",
                Path(td) / "supplement.json",
            ):
                outline = {"actualRange": {"last24h": {"points": [[0.0, 1.0, 30000]]}}}
                aircraft = {
                    "aircraft": [
                        {
                            "lat": 0.0,
                            "lon": 2.0,
                            "alt_baro": 36000,
                            "seen_pos": 300.0,
                            "type": "adsb_icao",
                        }
                    ]
                }
                receiver = {"lat": 0.0, "lon": 0.0}

                merged = overlay_server.OverlayHandler.merge_actual_range_outline(
                    outline,
                    aircraft,
                    receiver,
                    1000.0,
                )

                points = merged["actualRange"]["last24h"]["points"]
                self.assertEqual(points, [[0.0, 1.0, 30000]])

    def test_implausible_far_aircraft_does_not_spike_outline(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.object(
                overlay_server.OverlayHandler,
                "actual_range_supplement_file",
                Path(td) / "supplement.json",
            ):
                outline = {"actualRange": {"last24h": {"points": [[0.0, 1.0, 30000]]}}}
                aircraft = {
                    "aircraft": [
                        {
                            "lat": 0.0,
                            "lon": 4.0,
                            "alt_baro": 4000,
                            "seen_pos": 1.0,
                            "type": "adsb_icao",
                        }
                    ]
                }
                receiver = {"lat": 0.0, "lon": 0.0}

                merged = overlay_server.OverlayHandler.merge_actual_range_outline(
                    outline,
                    aircraft,
                    receiver,
                    1000.0,
                )

                points = merged["actualRange"]["last24h"]["points"]
                self.assertEqual(points, [[0.0, 1.0, 30000]])

    def test_implausible_persisted_supplement_record_is_pruned(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "supplement.json"
            path.write_text(
                (
                    '{"schema":"overlay_actual_range_supplement/v1","records":{'
                    '"90":{"lat":0.0,"lon":4.0,"alt":4000,'
                    '"distance_m":444000.0,"updated_ts":1000.0}}}\n'
                ),
                encoding="utf-8",
            )
            with mock.patch.object(overlay_server.OverlayHandler, "actual_range_supplement_file", path):
                outline = {"actualRange": {"last24h": {"points": [[0.0, 1.0, 30000]]}}}
                merged = overlay_server.OverlayHandler.merge_actual_range_outline(
                    outline,
                    {"aircraft": []},
                    {"lat": 0.0, "lon": 0.0},
                    1010.0,
                )

                self.assertEqual(merged["actualRange"]["last24h"]["points"], [[0.0, 1.0, 30000]])
                status = overlay_server.OverlayHandler.actual_range_ledger_status_snapshot()
                self.assertEqual(status["legacy_records_imported"], 0)
                self.assertEqual(status["sample_count"], 0)

    def test_radio_los_status_thresholds(self) -> None:
        los_nmi = overlay_server.OverlayHandler.radio_los_nmi(10000, 0)

        self.assertEqual(
            overlay_server.OverlayHandler.actual_range_los_status(los_nmi * 1.20 * 1852.0, 10000, 0),
            "accept",
        )
        self.assertEqual(
            overlay_server.OverlayHandler.actual_range_los_status(los_nmi * 1.22 * 1852.0, 10000, 0),
            "quarantine",
        )
        self.assertEqual(
            overlay_server.OverlayHandler.actual_range_los_status(los_nmi * 1.26 * 1852.0, 10000, 0),
            "reject",
        )

    def test_radio_los_quarantine_requires_repeat_before_merge(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "supplement.json"
            with mock.patch.object(overlay_server.OverlayHandler, "actual_range_supplement_file", path):
                receiver = {"lat": 0.0, "lon": 0.0}
                aircraft = {"aircraft": [{"lat": 0.0, "lon": 2.5, "seen_pos": 1.0, "alt_baro": 10000}]}

                first = overlay_server.OverlayHandler.merge_actual_range_outline(
                    {"actualRange": {"last24h": {"points": []}}},
                    aircraft,
                    receiver,
                    1000.0,
                )
                self.assertEqual(first["actualRange"]["last24h"]["points"], [])

                second = overlay_server.OverlayHandler.merge_actual_range_outline(
                    {"actualRange": {"last24h": {"points": []}}},
                    aircraft,
                    receiver,
                    1010.0,
                )
                self.assertEqual(second["actualRange"]["last24h"]["points"][0][2], 10000)
                self.assertAlmostEqual(second["actualRange"]["last24h"]["points"][0][1], 2.5, places=4)

    def test_radio_los_quarantine_allows_neighbor_supported_point(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.object(
                overlay_server.OverlayHandler,
                "actual_range_supplement_file",
                Path(td) / "supplement.json",
            ):
                outline = {"actualRange": {"last24h": {"points": [[0.0, 2.35, 30000]]}}}
                aircraft = {"aircraft": [{"lat": 0.0, "lon": 2.5, "seen_pos": 1.0, "alt_baro": 10000}]}

                merged = overlay_server.OverlayHandler.merge_actual_range_outline(
                    outline,
                    aircraft,
                    {"lat": 0.0, "lon": 0.0},
                    1000.0,
                )

                self.assertEqual(len(merged["actualRange"]["last24h"]["points"]), 1)
                self.assertAlmostEqual(merged["actualRange"]["last24h"]["points"][0][1], 2.5, places=4)

    def test_radio_los_uses_receiver_height_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with mock.patch.object(
                overlay_server.OverlayHandler,
                "actual_range_supplement_file",
                Path(td) / "supplement.json",
            ):
                outline = {"actualRange": {"last24h": {"points": []}}}
                aircraft = {"aircraft": [{"lat": 0.0, "lon": 2.58, "seen_pos": 1.0, "alt_baro": 10000}]}

                merged = overlay_server.OverlayHandler.merge_actual_range_outline(
                    outline,
                    aircraft,
                    {"lat": 0.0, "lon": 0.0, "receiver_height_ft": 100},
                    1000.0,
                )

                self.assertAlmostEqual(merged["actualRange"]["last24h"]["points"][0][1], 2.58, places=4)

    def test_supplement_record_survives_next_outline_request(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "supplement.json"
            with mock.patch.object(overlay_server.OverlayHandler, "actual_range_supplement_file", path):
                receiver = {"lat": 0.0, "lon": 0.0}
                first = overlay_server.OverlayHandler.merge_actual_range_outline(
                    {"actualRange": {"last24h": {"points": []}}},
                    {"aircraft": [{"lat": 0.0, "lon": 2.0, "seen_pos": 1.0, "alt_baro": 36000}]},
                    receiver,
                    1000.0,
                )
                self.assertEqual(first["actualRange"]["last24h"]["points"][0][2], 36000)

                second = overlay_server.OverlayHandler.merge_actual_range_outline(
                    {"actualRange": {"last24h": {"points": []}}},
                    {"aircraft": []},
                    receiver,
                    1010.0,
                )
                self.assertEqual(second["actualRange"]["last24h"]["points"], first["actualRange"]["last24h"]["points"])

    def test_persistent_ledger_uses_next_bucket_after_old_maximum_expires(self) -> None:
        receiver = {"lat": 0.0, "lon": 0.0}
        far = overlay_server.OverlayHandler.merge_actual_range_outline(
            {"actualRange": {"last24h": {"points": []}}},
            {
                "now": 1000.0,
                "messages": 100,
                "aircraft": [{"lat": 0.0, "lon": 2.0, "seen_pos": 0.0, "alt_baro": 36000}],
            },
            receiver,
            1000.0,
        )
        self.assertAlmostEqual(far["actualRange"]["last24h"]["points"][0][1], 2.0, places=4)

        overlay_server.OverlayHandler.merge_actual_range_outline(
            {"actualRange": {"last24h": {"points": []}}},
            {
                "now": 1301.0,
                "messages": 200,
                "aircraft": [{"lat": 0.0, "lon": 1.5, "seen_pos": 0.0, "alt_baro": 36000}],
            },
            receiver,
            1301.0,
        )
        after_expiry = overlay_server.OverlayHandler.merge_actual_range_outline(
            {"actualRange": {"last24h": {"points": []}}},
            {"now": 87401.0, "messages": 300, "aircraft": []},
            receiver,
            87401.0,
        )

        points = after_expiry["actualRange"]["last24h"]["points"]
        self.assertEqual(len(points), 1)
        self.assertAlmostEqual(points[0][1], 1.5, places=4)
        status = after_expiry["actualRange"]["last24h"]["streamV3CoverageLedger"]
        self.assertEqual(status["state"], "ready")
        self.assertEqual(status["sample_count"], 1)

    def test_source_counter_reset_keeps_persisted_coverage(self) -> None:
        receiver = {"lat": 0.0, "lon": 0.0}
        first = overlay_server.OverlayHandler.merge_actual_range_outline(
            {"actualRange": {"last24h": {"points": []}}},
            {
                "now": 1000.0,
                "messages": 1000,
                "aircraft": [{"lat": 0.0, "lon": 2.0, "seen_pos": 0.0, "alt_baro": 36000}],
            },
            receiver,
            1000.0,
        )
        after_reset = overlay_server.OverlayHandler.merge_actual_range_outline(
            {"actualRange": {"last24h": {"points": []}}},
            {"now": 1100.0, "messages": 10, "aircraft": []},
            receiver,
            1100.0,
        )

        self.assertEqual(after_reset["actualRange"]["last24h"]["points"], first["actualRange"]["last24h"]["points"])
        status = after_reset["actualRange"]["last24h"]["streamV3CoverageLedger"]
        self.assertEqual(status["source_reset_count"], 1)
        self.assertEqual(status["last_source_reset_at_utc"], "1970-01-01T00:18:20Z")

    def test_stale_source_epoch_is_reported_and_not_retimestamped(self) -> None:
        merged = overlay_server.OverlayHandler.merge_actual_range_outline(
            {"actualRange": {"last24h": {"points": []}}},
            {
                "now": 1000.0,
                "messages": 10,
                "aircraft": [{"lat": 0.0, "lon": 2.0, "seen_pos": 0.0, "alt_baro": 36000}],
            },
            {"lat": 0.0, "lon": 0.0},
            1400.0,
        )

        status = merged["actualRange"]["last24h"]["streamV3CoverageLedger"]
        self.assertEqual(status["state"], "source_stale")
        self.assertEqual(status["source_sample_age_sec"], 400.0)
        self.assertEqual(status["oldest_sample_age_sec"], 400.0)

    def test_legacy_supplement_is_imported_once_into_persistent_ledger(self) -> None:
        legacy = Path(self._coverage_state_dir.name) / "legacy.json"
        legacy.write_text(
            json.dumps(
                {
                    "schema": "overlay_actual_range_supplement/v1",
                    "records": {
                        "90": {
                            "lat": 0.0,
                            "lon": 2.0,
                            "alt": 36000,
                            "distance_m": 222389.853,
                            "updated_ts": 950.0,
                            "los_repeat_count": 1,
                        }
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        with mock.patch.object(overlay_server.OverlayHandler, "actual_range_supplement_file", legacy):
            merged = overlay_server.OverlayHandler.merge_actual_range_outline(
                {"actualRange": {"last24h": {"points": []}}},
                {"now": 1000.0, "messages": 1, "aircraft": []},
                {"lat": 0.0, "lon": 0.0},
                1000.0,
            )

        self.assertAlmostEqual(merged["actualRange"]["last24h"]["points"][0][1], 2.0, places=4)
        status = merged["actualRange"]["last24h"]["streamV3CoverageLedger"]
        self.assertEqual(status["legacy_records_imported"], 1)
        self.assertTrue(overlay_server.OverlayHandler.actual_range_ledger_file.exists())

    def test_receiver_change_invalidates_old_ledger_coordinates(self) -> None:
        overlay_server.OverlayHandler.merge_actual_range_outline(
            {"actualRange": {"last24h": {"points": []}}},
            {
                "now": 1000.0,
                "messages": 100,
                "aircraft": [{"lat": 0.0, "lon": 2.0, "seen_pos": 0.0, "alt_baro": 36000}],
            },
            {"lat": 0.0, "lon": 0.0},
            1000.0,
        )
        changed = overlay_server.OverlayHandler.merge_actual_range_outline(
            {"actualRange": {"last24h": {"points": []}}},
            {"now": 1100.0, "messages": 200, "aircraft": []},
            {"lat": 10.0, "lon": 10.0},
            1100.0,
        )

        self.assertEqual(changed["actualRange"]["last24h"]["points"], [])
        status = changed["actualRange"]["last24h"]["streamV3CoverageLedger"]
        self.assertEqual(status["receiver_reset_count"], 1)
        self.assertEqual(status["state"], "empty")

    def test_ledger_failure_falls_back_to_upstream_outline(self) -> None:
        invalid_database_path = Path(self._coverage_state_dir.name) / "database-is-a-directory"
        invalid_database_path.mkdir()
        with mock.patch.object(
            overlay_server.OverlayHandler,
            "actual_range_ledger_file",
            invalid_database_path,
        ):
            merged = overlay_server.OverlayHandler.merge_actual_range_outline(
                {"actualRange": {"last24h": {"points": [[0.0, 1.0, 30000]]}}},
                {"now": 1000.0, "messages": 1, "aircraft": []},
                {"lat": 0.0, "lon": 0.0},
                1000.0,
            )

        self.assertEqual(merged["actualRange"]["last24h"]["points"], [[0.0, 1.0, 30000]])
        status = merged["actualRange"]["last24h"]["streamV3CoverageLedger"]
        self.assertEqual(status["state"], "degraded")
        self.assertEqual(status["last_error"], "OperationalError")

    def test_coverage_status_endpoint_exposes_ledger_health_without_coordinates(self) -> None:
        overlay_server.OverlayHandler.actual_range_ledger_status = {
            "schema": "stream_v3.actual_range_ledger_status.v1",
            "state": "ready",
            "persisted": True,
            "bearing_count": 180,
        }
        with tempfile.TemporaryDirectory() as td:
            handler = partial(overlay_server.OverlayHandler, directory=td)
            overlay, overlay_url = _serve(handler)
            try:
                with urllib.request.urlopen(overlay_url + "coverage/status.json", timeout=3) as res:
                    body = res.read().decode("utf-8")
                    payload = json.loads(body)
                self.assertEqual(payload["state"], "ready")
                self.assertEqual(payload["bearing_count"], 180)
                self.assertNotIn("lat", body)
                self.assertNotIn("lon", body)
            finally:
                overlay.shutdown()
                overlay.server_close()


if __name__ == "__main__":
    unittest.main()
