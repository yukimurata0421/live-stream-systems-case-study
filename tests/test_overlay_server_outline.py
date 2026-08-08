from __future__ import annotations

import http.server
import json
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
                    self.assertTrue(after["ready"])
                    self.assertEqual(after["state"], "ready")
                    self.assertTrue(after["map_tiles_ready"])
                    self.assertTrue(after["aircraft_sample_ready"])
                    self.assertEqual(after["reported_at_ms"], 123456)
                    self.assertLessEqual(after["age_sec"], 1.0)
                finally:
                    overlay.shutdown()
                    overlay.server_close()
            finally:
                overlay_server.OverlayHandler.render_ready_payload = previous_payload
                overlay_server.OverlayHandler.render_ready_received_at = previous_received_at
                overlay_server.OverlayHandler.render_server_started_at = previous_started_at

    def test_processed_precipitation_assets_are_served_from_local_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            weather = root / "weather"
            tile = weather / "generations" / "20260802080000" / "7" / "114" / "50.png"
            tile.parent.mkdir(parents=True)
            tile.write_bytes(b"processed-precipitation")
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
                self.assertEqual(overlay_server.OverlayHandler.load_actual_range_supplement(1010.0), {})

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


if __name__ == "__main__":
    unittest.main()
