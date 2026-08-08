from __future__ import annotations

import html as html_lib
import json
import re
import shutil
import subprocess
import tempfile
import threading
import unittest
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAP_DIR = ROOT / "ui" / "overlay" / "adsb-map"


class _QuietStaticHandler(SimpleHTTPRequestHandler):
    def log_message(self, _format: str, *_args: object) -> None:
        return


class AdsbMapContractTests(unittest.TestCase):
    def test_runtime_assets_and_vendored_licence_are_present(self) -> None:
        required = (
            "index.html",
            "map.css",
            "map.js",
            "solar_theme.mjs",
            "style.json",
            "airports.geojson",
            "neutral-terrain.webp",
            "ATTRIBUTION.md",
            "vendor/maplibre-gl.mjs",
            "vendor/maplibre-gl-shared.mjs",
            "vendor/maplibre-gl-worker.mjs",
            "vendor/maplibre-gl.css",
            "vendor/LICENSE-maplibre-gl.txt",
        )
        for relative in required:
            path = MAP_DIR / relative
            self.assertTrue(path.is_file(), relative)
            self.assertGreater(path.stat().st_size, 0, relative)

    def test_map_is_aircraft_first_without_flight_labels_or_tracks(self) -> None:
        script = (MAP_DIR / "map.js").read_text(encoding="utf-8")

        self.assertIn('id: "aircraft-icon"', script)
        self.assertNotIn('id: "tracks"', script)
        self.assertNotIn('id: "track-shadow"', script)
        self.assertNotIn('addSource("tracks"', script)
        self.assertNotIn("TRACK_RETENTION_MS", script)
        self.assertNotIn("TRACK_MAX_POINTS", script)
        self.assertNotIn('id: "aircraft-label"', script)
        self.assertNotRegex(script, re.compile(r"\bflight\b", re.IGNORECASE))
        self.assertIn("aircraftLabels: false", script)
        self.assertEqual(script.count("aircraftTracks: false"), 2)

    def test_solar_theme_changes_only_base_map_layers(self) -> None:
        script = (MAP_DIR / "map.js").read_text(encoding="utf-8")
        theme = (MAP_DIR / "solar_theme.mjs").read_text(encoding="utf-8")

        self.assertIn('import {solarTheme} from "./solar_theme.mjs";', script)
        self.assertIn("SOLAR_THEME_REFRESH_MS = 60_000", script)
        self.assertIn("SOLAR_THEME_TRANSITION_MS = 120_000", script)
        self.assertIn('params.get("solarTime")', script)
        self.assertIn('phase = "NIGHT"', theme)
        self.assertIn('phase = "TWILIGHT"', theme)
        self.assertIn('phase = "GOLDEN_HOUR"', theme)
        self.assertIn('phase = "DAY"', theme)
        self.assertIn("solarAircraftColorsFixed: true", script)
        self.assertIn("solarPrecipitationColorsFixed: true", script)
        apply_block = script.split("function applySolarTheme", 1)[1].split(
            "function addPlaneImage",
            1,
        )[0]
        self.assertNotIn('"aircraft-position"', apply_block)
        self.assertNotIn('"aircraft-icon"', apply_block)
        self.assertNotIn("precipitation-layer-", apply_block)
        self.assertNotIn("document.body.style.filter", script)
        self.assertNotIn("map.getCanvas().style.filter", script)

    def test_solar_theme_calculation_tracks_tokyo_daylight_without_an_api(self) -> None:
        chromium = next(
            (
                path
                for name in ("chromium", "chromium-browser", "google-chrome")
                if (path := shutil.which(name))
            ),
            None,
        )
        if chromium is None:
            self.skipTest("Chromium is not installed")

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            shutil.copy2(MAP_DIR / "solar_theme.mjs", root / "solar_theme.mjs")
            (root / "index.html").write_text(
                """<!doctype html><html><body><pre id="result">pending</pre>
<script type="module">
import {solarTheme} from "./solar_theme.mjs";
const point = [36.12028, 140.23215];
const samples = Object.fromEntries([
  ["dawn", "2026-08-04T19:30:00Z"],
  ["sunrise", "2026-08-04T20:05:00Z"],
  ["day", "2026-08-05T03:00:00Z"],
  ["sunset", "2026-08-05T09:25:00Z"],
  ["twilight", "2026-08-05T10:00:00Z"],
  ["night", "2026-08-05T14:00:00Z"],
].map(([name, timestamp]) => [name, solarTheme(new Date(timestamp), ...point)]));
document.getElementById("result").textContent = JSON.stringify(samples);
</script></body></html>""",
                encoding="utf-8",
            )
            handler = partial(_QuietStaticHandler, directory=str(root))
            server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            host, port = server.server_address
            try:
                command = [
                    chromium,
                    "--headless",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--virtual-time-budget=1000",
                    "--dump-dom",
                    f"http://{host}:{port}/",
                ]
                try:
                    completed = subprocess.run(
                        command,
                        text=True,
                        capture_output=True,
                        timeout=20,
                        check=False,
                    )
                except subprocess.TimeoutExpired as exc:
                    stdout = (
                        exc.stdout.decode("utf-8", "replace")
                        if isinstance(exc.stdout, bytes)
                        else (exc.stdout or "")
                    )
                    stderr = (
                        exc.stderr.decode("utf-8", "replace")
                        if isinstance(exc.stderr, bytes)
                        else (exc.stderr or "")
                    )
                    if '<pre id="result">' not in stdout or "pending</pre>" in stdout:
                        self.skipTest("Chromium did not produce a completed DOM before timeout")
                    completed = subprocess.CompletedProcess(command, 0, stdout, stderr)
            finally:
                server.shutdown()
                server.server_close()

        self.assertEqual(completed.returncode, 0, completed.stderr[-1000:])
        match = re.search(r'<pre id="result">(.*?)</pre>', completed.stdout, re.S)
        self.assertIsNotNone(match, completed.stdout[-2000:])
        assert match is not None
        samples = json.loads(html_lib.unescape(match.group(1)))
        self.assertEqual(
            {name: sample["phase"] for name, sample in samples.items()},
            {
                "dawn": "TWILIGHT",
                "sunrise": "GOLDEN_HOUR",
                "day": "DAY",
                "sunset": "GOLDEN_HOUR",
                "twilight": "TWILIGHT",
                "night": "NIGHT",
            },
        )
        self.assertTrue(samples["sunrise"]["rising"])
        self.assertFalse(samples["sunset"]["rising"])
        self.assertGreater(samples["sunset"]["warmth"], samples["sunrise"]["warmth"])
        self.assertEqual(samples["day"]["brightness"], 0.10)
        self.assertEqual(samples["day"]["warmth"], 0)
        self.assertEqual(samples["night"]["brightness"], 0)
        self.assertEqual(samples["night"]["warmth"], 0)

    def test_coverage_and_range_rings_are_solid(self) -> None:
        script = (MAP_DIR / "map.js").read_text(encoding="utf-8")
        css = (MAP_DIR / "map.css").read_text(encoding="utf-8")
        coverage_shadow_block = script.split('id: "coverage-shadow"', 1)[1].split('id: "coverage-line"', 1)[0]
        coverage_block = script.split('id: "coverage-line"', 1)[1].split("});", 1)[0]
        ring_block = script.split('id: "range-rings"', 1)[1].split("});", 1)[0]

        self.assertIn('const COVERAGE_COLOR = "#A29BBA";', script)
        self.assertIn('const COVERAGE_OPACITY = 0.62;', script)
        self.assertIn('const COVERAGE_WIDTH = 1.5;', script)
        self.assertIn('const COVERAGE_HALO_COLOR = "#030A0F";', script)
        self.assertIn('const COVERAGE_HALO_OPACITY = 0.58;', script)
        self.assertIn('const COVERAGE_HALO_WIDTH = 2.8;', script)
        self.assertIn('const RANGE_RING_COLOR = "#B0CFD4";', script)
        self.assertIn('const RANGE_RING_OPACITY = 0.55;', script)
        self.assertIn('const RANGE_RING_WIDTH = 1.0;', script)
        self.assertIn('"line-color": COVERAGE_COLOR', coverage_block)
        self.assertIn('"line-width": COVERAGE_WIDTH', coverage_block)
        self.assertIn('"line-opacity": COVERAGE_OPACITY', coverage_block)
        self.assertIn('"line-color": COVERAGE_HALO_COLOR', coverage_shadow_block)
        self.assertIn('"line-width": COVERAGE_HALO_WIDTH', coverage_shadow_block)
        self.assertIn('"line-opacity": COVERAGE_HALO_OPACITY', coverage_shadow_block)
        self.assertNotIn("line-blur", coverage_shadow_block)
        self.assertIn('"line-cap": "round"', coverage_block)
        self.assertIn('"line-join": "round"', coverage_block)
        self.assertIn('"line-miter-limit": 2', coverage_block)
        self.assertNotIn("line-dasharray", coverage_block)
        self.assertNotIn("line-dasharray", ring_block)
        self.assertIn(".line.ring { border-top: 1px solid rgba(176, 207, 212, 0.55); }", css)
        self.assertIn('rangeRingLine: "solid"', script)
        self.assertIn("integratedSidebar: embedded", script)
        self.assertEqual(script.count('}, "weather-sea-veil");'), 1)
        self.assertEqual(script.count('}, "coastline-glow");'), 2)
        self.assertIn("coverageAboveTerrain:", script)
        self.assertIn("coverageAbovePrecipitation:", script)
        self.assertIn("coverageBelowBoundaries:", script)
        self.assertIn("rangeRingOpacity: RANGE_RING_OPACITY", script)
        self.assertIn("rangeRingWidth: RANGE_RING_WIDTH", script)

    def test_precipitation_is_local_analysis_only_and_below_boundaries(self) -> None:
        script = (MAP_DIR / "map.js").read_text(encoding="utf-8")
        html = (MAP_DIR / "index.html").read_text(encoding="utf-8")

        self.assertIn('fetchJson("/weather/status.json")', script)
        self.assertIn("payload?.analysis_only !== true", script)
        self.assertIn("payload?.forecast_minutes !== 0", script)
        self.assertIn('}, "weather-sea-veil");', script)
        self.assertIn("PRECIPITATION_FADE_MS = 1_000", script)
        self.assertIn("PRECIPITATION_LAYER_OPACITY = 0.82", script)
        self.assertIn("PRECIPITATION_DEFAULT_STALE_MS = 15 * 60 * 1_000", script)
        self.assertNotIn("www.jma.go.jp", script)
        self.assertIn("Precipitation: JMA, processed (PDL1.0)", html)

        style = json.loads((MAP_DIR / "style.json").read_text(encoding="utf-8"))
        layer_ids = [layer["id"] for layer in style["layers"]]
        self.assertLess(layer_ids.index("water"), layer_ids.index("weather-sea-veil"))
        self.assertLess(layer_ids.index("weather-sea-veil"), layer_ids.index("coastline-glow"))
        self.assertLess(layer_ids.index("coastline-glow"), layer_ids.index("coastline"))
        sea_veil = next(layer for layer in style["layers"] if layer["id"] == "weather-sea-veil")
        coastline_glow = next(layer for layer in style["layers"] if layer["id"] == "coastline-glow")
        coastline = next(layer for layer in style["layers"] if layer["id"] == "coastline")
        self.assertEqual(sea_veil["paint"]["fill-opacity"], 0.24)
        self.assertEqual(coastline_glow["paint"]["line-width"], 3.6)
        self.assertEqual(coastline["paint"]["line-width"], 1.0)

    def test_precipitation_layer_keeps_fresh_lkg_but_hides_expired_data(self) -> None:
        script = (MAP_DIR / "map.js").read_text(encoding="utf-8")
        refresh_block = script.split("async function refreshPrecipitation()", 1)[1].split(
            "function addLiveLayers()",
            1,
        )[0]

        self.assertIn("if (activePrecipitation)", refresh_block)
        self.assertIn(
            "Date.now() - activePrecipitation.observedMs > activePrecipitation.staleAfterMs",
            refresh_block,
        )
        self.assertIn('setPrecipitationStatus("fresh", activePrecipitation.observedMs)', refresh_block)
        self.assertIn('setPrecipitationStatus("stale", activePrecipitation.observedMs)', refresh_block)
        self.assertIn("fadeOutActivePrecipitation()", refresh_block)
        self.assertIn("diagnostics.precipitationAvailable = false", refresh_block)
        self.assertIn("diagnostics.precipitationLayerLoaded = true", refresh_block)
        self.assertIn("diagnostics.precipitationLayerLoaded = false", refresh_block)
        self.assertIn("invalid local precipitation tile template", script)

    def test_map_reports_real_render_readiness_after_tiles_and_adsb_sample(self) -> None:
        script = (MAP_DIR / "map.js").read_text(encoding="utf-8")

        self.assertIn('fetch("/render/ready"', script)
        self.assertIn("map.areTilesLoaded()", script)
        self.assertIn('map.isSourceLoaded("openmaptiles")', script)
        self.assertIn('map.isSourceLoaded("terrain-dem")', script)
        self.assertIn("diagnostics.lastAircraftEpoch > 0", script)
        self.assertIn("map_tiles_ready: true", script)
        self.assertIn("aircraft_sample_ready: true", script)
        self.assertIn("const payload = await response.json();", script)
        self.assertIn("payload?.accepted !== true", script)
        self.assertLess(
            script.index("const payload = await response.json();"),
            script.index("renderReadyLastPostedAt = now;"),
        )
        self.assertIn("renderReadyReported: false", script)
        self.assertIn('map.on("idle"', script)

    def test_render_readiness_heartbeat_does_not_depend_on_map_idle(self) -> None:
        script = (MAP_DIR / "map.js").read_text(encoding="utf-8")

        self.assertIn("const RENDER_READY_REPUBLISH_MS = 10_000", script)
        self.assertIn("const RENDER_READY_HEARTBEAT_MS = 5_000", script)
        self.assertIn("const RENDER_READY_AIRCRAFT_MAX_AGE_MS = 15_000", script)
        self.assertIn("let renderReadyEstablished = false", script)
        self.assertIn("let renderContextHealthy = true", script)
        self.assertIn("aircraftLastReceivedAt = Date.now()", script)
        self.assertIn("now - aircraftLastReceivedAt <= RENDER_READY_AIRCRAFT_MAX_AGE_MS", script)
        self.assertIn("setInterval(publishRenderReady, RENDER_READY_HEARTBEAT_MS)", script)
        self.assertIn('addEventListener("webglcontextlost"', script)
        self.assertIn('addEventListener("webglcontextrestored"', script)
        self.assertIn("renderReadyEstablished = true", script)

    def test_altitude_and_precipitation_legends_are_readable_in_left_stack(self) -> None:
        html = (MAP_DIR / "index.html").read_text(encoding="utf-8")
        css = (MAP_DIR / "map.css").read_text(encoding="utf-8")
        stack = css.split("#mapLegends {", 1)[1].split("}", 1)[0]
        legend_panels = css.split("#altitudeLegend,\n#precipitationStatus {", 1)[1].split("}", 1)[0]
        precipitation = css.split("#precipitationStatus {", 1)[1].split("}", 1)[0]

        self.assertIn('id="mapLegends"', html)
        self.assertLess(html.index('id="altitudeLegend"'), html.index('id="precipitationStatus"'))
        self.assertIn("left: 12px", stack)
        self.assertIn("top: 230px", stack)
        self.assertIn("flex-direction: column", stack)
        self.assertIn("align-items: flex-start", stack)
        self.assertIn("font-size: 13px", legend_panels)
        self.assertIn("width: min(440px", legend_panels)
        self.assertIn("AIRCRAFT ALTITUDE", html)
        self.assertIn("<span class=\"legend-unit\">FEET</span>", html)
        self.assertIn("PRECIPITATION <span class=\"legend-unit\">mm/h</span>", html)
        self.assertEqual(html.count('class="altitude-scale-row"'), 2)
        self.assertIn("grid-template-columns: repeat(3, minmax(0, 1fr))", css)
        self.assertIn('class="legend-row guide-scale"', html)
        self.assertIn('class="legend-row precipitation-scale"', html)
        self.assertIn("rangeRingLegendOverlap: false", (MAP_DIR / "map.js").read_text(encoding="utf-8"))
        self.assertNotIn("position: absolute", legend_panels)
        self.assertNotIn("position: absolute", precipitation)

    def test_coverage_label_matches_last24h_los_aware_reception_source(self) -> None:
        html = (MAP_DIR / "index.html").read_text(encoding="utf-8")
        script = (MAP_DIR / "map.js").read_text(encoding="utf-8")

        self.assertIn("24H LOS-AWARE COVERAGE", html)
        self.assertNotIn("24H MAX RECEPTION", html)
        self.assertIn('coverageWindow: "last24h"', script)
        self.assertIn('coverageSourceField: "actualRange.last24h.points"', script)
        self.assertIn('representation: "maximum-reception-by-bearing"', script)

    def test_attribution_is_a_bottom_right_card(self) -> None:
        css = (MAP_DIR / "map.css").read_text(encoding="utf-8")
        attribution = css.split("#mapAttribution {", 1)[1].split("}", 1)[0]

        self.assertIn("right: 12px", attribution)
        self.assertIn("bottom: 10px", attribution)
        self.assertIn("width: max-content", attribution)
        self.assertIn("max-width: calc(100vw - 24px)", attribution)
        self.assertIn("border-radius: 6px", attribution)
        self.assertIn("background: rgba(5, 14, 18, 0.88)", attribution)
        self.assertIn("font-size: 11px", attribution)
        self.assertNotIn("transform:", attribution)
        self.assertIn("attributionUnusedWidth", (MAP_DIR / "map.js").read_text(encoding="utf-8"))

    def test_labels_are_latin_or_english_and_roads_are_not_rendered(self) -> None:
        style = json.loads((MAP_DIR / "style.json").read_text(encoding="utf-8"))
        style_text = json.dumps(style, ensure_ascii=False)
        source_layers = {layer.get("source-layer") for layer in style["layers"]}

        self.assertIn("name:latin", style_text)
        self.assertIn("name_en", style_text)
        self.assertNotIn("name:nonlatin", style_text)
        self.assertNotIn("transportation", source_layers)
        self.assertNotIn("transportation_name", source_layers)

    def test_tile_sources_are_local_proxy_routes_with_visible_attribution(self) -> None:
        style = json.loads((MAP_DIR / "style.json").read_text(encoding="utf-8"))
        html = (MAP_DIR / "index.html").read_text(encoding="utf-8")

        self.assertEqual(
            style["sources"]["openmaptiles"]["tiles"],
            ["/map-tiles/openfreemap/{z}/{x}/{y}.pbf"],
        )
        self.assertEqual(
            style["sources"]["terrain-dem"]["tiles"],
            ["/map-tiles/terrain/{z}/{x}/{y}.webp"],
        )
        self.assertIn("OpenStreetMap contributors (ODbL)", html)
        self.assertIn("GSI Japan (Approval R 7JHs 542)", html)

    def test_airports_use_iata_codes_and_public_domain_source_is_recorded(self) -> None:
        airports = json.loads((MAP_DIR / "airports.geojson").read_text(encoding="utf-8"))
        attribution = (MAP_DIR / "ATTRIBUTION.md").read_text(encoding="utf-8")
        codes = {feature["properties"]["code"] for feature in airports["features"]}

        self.assertEqual(len(airports["features"]), 14)
        self.assertTrue({"HND", "NRT", "IBR", "SDJ", "NGO"}.issubset(codes))
        self.assertIn("Public Domain", attribution)


if __name__ == "__main__":
    unittest.main()
