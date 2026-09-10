import * as maplibregl from "./vendor/maplibre-gl.mjs";
import {precipitationRenderSnapshot} from "./precipitation_render.mjs";
import {waitForMapSourceReady} from "./precipitation_source_ready.mjs";
import {solarTheme} from "./solar_theme.mjs";

const params = new URLSearchParams(window.location.search);
const embedded = params.get("embedded") === "1";
document.documentElement.classList.toggle("embedded", embedded);
const numberParam = (name, fallback) => {
  const raw = params.get(name);
  if (raw === null || raw.trim() === "") return fallback;
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
};

const center = [numberParam("lon", 140.75), numberParam("lat", 36.35)];
const legacyZoom = numberParam("zoom", 7.6);
// tar1090/OpenLayers uses 256 px tiles; MapLibre uses a 512 px zoom convention.
const zoom = numberParam("maplibreZoom", legacyZoom - 1);
const AIRCRAFT_REFRESH_MS = 1_000;
const RANGE_REFRESH_MS = 60_000;
const PRECIPITATION_REFRESH_MS = 60_000;
const PRECIPITATION_DEFAULT_STALE_MS = 15 * 60 * 1_000;
const PRECIPITATION_FADE_MS = 1_000;
const PRECIPITATION_LAYER_OPACITY = 0.82;
const PRECIPITATION_SOURCE_READY_TIMEOUT_MS = 120_000;
const PRECIPITATION_SOURCE_READY_POLL_MS = 250;
const RENDER_READY_REPUBLISH_MS = 10_000;
const RENDER_READY_HEARTBEAT_MS = 5_000;
const RENDER_READY_AIRCRAFT_MAX_AGE_MS = 15_000;
const SOLAR_THEME_REFRESH_MS = 60_000;
const SOLAR_THEME_TRANSITION_MS = 120_000;
const MAX_POSITION_AGE_SEC = 20;
const COVERAGE_COLOR = "#A29BBA";
const COVERAGE_OPACITY = 0.68;
const COVERAGE_WIDTH = 1.8;
const COVERAGE_HALO_COLOR = "#030A0F";
const COVERAGE_HALO_OPACITY = 0.58;
const COVERAGE_HALO_WIDTH = 2.8;
const RANGE_RING_COLOR = "#B0CFD4";
const RANGE_RING_OPACITY = 0.65;
const RANGE_RING_WIDTH = 1.35;
const RANGE_LABEL_SIZE = 14;
const AIRCRAFT_ICON_SIZE = 0.84;
const REQUIRED_SEMANTIC_SOURCES = [
  "openmaptiles", "terrain-dem", "coverage", "range-rings", "range-labels", "aircraft",
];
const REQUIRED_SEMANTIC_LAYERS = [
  "water", "coastline", "coverage-shadow", "coverage-line", "range-ring-shadow",
  "range-rings", "range-labels", "aircraft-icon",
];
const REQUIRED_SEMANTIC_UI = [
  "mapLegends", "altitudeLegend", "precipitationStatus", "mapAttribution",
];
const ALTITUDE_COLORS = {
  ground: "#f3f4f6",
  low: "#ffd166",
  mid: "#39e6c3",
  high: "#67a7ff",
  very_high: "#f278ff",
  unknown: "#9aa6ae",
};
const TERRAIN_RELIEF_COLORS = [
  [-20, "#1c2929"],
  [20, "#20302c"],
  [100, "#26352e"],
  [300, "#303b31"],
  [600, "#3a4134"],
  [1000, "#47463a"],
  [1500, "#554b3f"],
  [2200, "#645345"],
  [3200, "#735f4e"],
  [4500, "#806b59"],
];
const SOLAR_THEME_LAYERS = [
  "background",
  "terrain-relief",
  "terrain-hillshade",
  "wood-muted",
  "park-muted",
  "water",
  "weather-sea-veil",
  "coastline",
  "country-border",
  "prefecture-border",
  "major-city",
  "major-city-large",
];
const solarTimeOverride = (() => {
  const raw = params.get("solarTime");
  if (!raw) return null;
  const epochMs = Date.parse(raw);
  return Number.isFinite(epochMs) ? epochMs : null;
})();

const emptyCollection = () => ({type: "FeatureCollection", features: []});
const errors = [];
let mapLoaded = false;
let aircraftFailures = 0;
let precipitationRefreshInFlight = false;
let precipitationSequence = 0;
let activePrecipitation = null;
let rangeRing150Coordinates = [];
let renderReadyPostInFlight = false;
let renderReadyLastPostedAt = 0;
let renderReadyEstablished = false;
let renderContextHealthy = true;
let aircraftLastReceivedAt = 0;
let assetRevision = "";
let solarObserver = {lat: center[1], lon: center[0], source: "map-center"};
let solarThemeInitialized = false;

const diagnostics = {
  aircraftCount: 0,
  aircraftTracks: false,
  coveragePoints: 0,
  coverageSourceFeatureCount: 0,
  aircraftSourceFeatureCount: 0,
  rangeRingFeatureCount: 0,
  rangeLabelFeatureCount: 0,
  lastAircraftEpoch: 0,
  coverageLine: "solid",
  coverageColor: COVERAGE_COLOR,
  coverageOpacity: COVERAGE_OPACITY,
  coverageWidth: COVERAGE_WIDTH,
  coverageHaloColor: COVERAGE_HALO_COLOR,
  coverageHaloOpacity: COVERAGE_HALO_OPACITY,
  coverageHaloWidth: COVERAGE_HALO_WIDTH,
  coverageWindow: "last24h",
  coverageSourceField: "actualRange.last24h.points",
  integratedSidebar: embedded,
  rangeRingLine: "solid",
  rangeRingLegendOverlap: false,
  aircraftLabels: false,
  labelLanguage: "latin-or-english",
  precipitationAvailable: false,
  precipitationFresh: false,
  precipitationHasRain: false,
  precipitationValidtime: null,
  precipitationLayerValidtime: null,
  precipitationLayerLoaded: false,
  precipitationEvaluated: false,
  precipitationLayerOpacity: PRECIPITATION_LAYER_OPACITY,
  precipitationAnalysisOnly: true,
  precipitationLoadState: "idle",
  precipitationPendingValidtime: null,
  precipitationLastLoadReason: null,
  precipitationLastLoadDurationMs: null,
  renderReadyReported: false,
  solarTheme: "PENDING",
  solarAltitudeDegrees: null,
  solarAzimuthDegrees: null,
  solarRising: null,
  solarBrightness: 0,
  solarWarmth: 0,
  solarSampleTimeUtc: null,
  solarObserver: {...solarObserver},
  solarTimeOverride: solarTimeOverride !== null,
  solarThemedLayers: [...SOLAR_THEME_LAYERS],
  solarAircraftColorsFixed: true,
  solarPrecipitationColorsFixed: true,
  errors,
};
window.adsbMapDiagnostics = diagnostics;

const map = new maplibregl.Map({
  container: "map",
  style: "./style.json",
  center,
  zoom,
  bearing: 0,
  pitch: 0,
  maxPitch: 0,
  attributionControl: false,
  interactive: false,
  preserveDrawingBuffer: true,
  fadeDuration: 0,
  crossSourceCollisions: false,
  renderWorldCopies: false,
  pixelRatio: 1,
});
window.adsbMap = map;
map.getCanvas().addEventListener("webglcontextlost", () => {
  renderContextHealthy = false;
  renderReadyEstablished = false;
  diagnostics.renderReadyReported = false;
});
map.getCanvas().addEventListener("webglcontextrestored", () => {
  renderContextHealthy = true;
});

const status = document.getElementById("mapStatus");
const precipitationStatus = document.getElementById("precipitationStatus");
const precipitationTitle = precipitationStatus.querySelector(".precipitation-title");
const precipitationTime = document.getElementById("precipitationTime");
function showStatus(message) {
  status.textContent = message;
  status.classList.toggle("visible", Boolean(message));
}

async function refreshAssetIdentity() {
  try {
    const identity = await fetchJson("/asset/manifest.json");
    if (
      identity?.schema !== "stream_v3.map_asset_identity.v1"
      || identity?.ok !== true
      || !/^[0-9a-f]{64}$/.test(String(identity?.revision || ""))
    ) {
      throw new Error("asset identity contract failed");
    }
    assetRevision = identity.revision;
  } catch (_error) {
    assetRevision = "";
    diagnostics.renderReadyReported = false;
  }
}

function semanticRenderSnapshot() {
  let aircraftRenderedFeatureCount = 0;
  try {
    aircraftRenderedFeatureCount = map.queryRenderedFeatures(
      undefined,
      {layers: ["aircraft-icon"]},
    ).length;
  } catch (_error) {
    aircraftRenderedFeatureCount = 0;
  }
  return {
    schema: "stream_v3.map_semantic_render.v1",
    map_style_loaded: mapLoaded && Boolean(map.getStyle()),
    render_context_healthy: renderContextHealthy,
    required_sources: Object.fromEntries(
      REQUIRED_SEMANTIC_SOURCES.map((name) => [name, Boolean(map.getSource(name))]),
    ),
    required_layers: Object.fromEntries(
      REQUIRED_SEMANTIC_LAYERS.map((name) => [name, Boolean(map.getLayer(name))]),
    ),
    ui_elements: Object.fromEntries(
      REQUIRED_SEMANTIC_UI.map((name) => [name, Boolean(document.getElementById(name))]),
    ),
    aircraft_sample_count: diagnostics.aircraftCount,
    aircraft_source_feature_count: diagnostics.aircraftSourceFeatureCount,
    aircraft_rendered_feature_count: aircraftRenderedFeatureCount,
    coverage_point_count: diagnostics.coveragePoints,
    coverage_source_feature_count: diagnostics.coverageSourceFeatureCount,
    range_ring_feature_count: diagnostics.rangeRingFeatureCount,
    range_label_feature_count: diagnostics.rangeLabelFeatureCount,
    map_error_count: errors.length,
  };
}

async function publishRenderReady() {
  const now = Date.now();
  if (renderReadyPostInFlight || now - renderReadyLastPostedAt < RENDER_READY_REPUBLISH_MS) return;
  const initialMapTilesReady = mapLoaded
    && map.areTilesLoaded()
    && map.isSourceLoaded("openmaptiles")
    && map.isSourceLoaded("terrain-dem");
  const mapTilesReady = renderContextHealthy && (
    initialMapTilesReady
    || (renderReadyEstablished
      && mapLoaded
      && Boolean(map.getSource("openmaptiles"))
      && Boolean(map.getSource("terrain-dem")))
  );
  const aircraftSampleReady = diagnostics.lastAircraftEpoch > 0
    && aircraftLastReceivedAt > 0
    && now - aircraftLastReceivedAt <= RENDER_READY_AIRCRAFT_MAX_AGE_MS;
  if (!mapTilesReady || !aircraftSampleReady || !assetRevision) return;
  renderReadyPostInFlight = true;
  try {
    const response = await fetch("/render/ready", {
      method: "POST",
      cache: "no-store",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({
        ready: true,
        map_tiles_ready: true,
        aircraft_sample_ready: true,
        reported_at_ms: now,
        precipitation: precipitationRenderSnapshot(diagnostics),
        semantic: semanticRenderSnapshot(),
        asset_revision: assetRevision,
      }),
    });
    if (!response.ok) throw new Error(`render-ready HTTP ${response.status}`);
    const payload = await response.json();
    if (payload?.accepted !== true) throw new Error("render-ready response was not accepted");
    renderReadyLastPostedAt = now;
    renderReadyEstablished = true;
    diagnostics.renderReadyReported = true;
  } catch (_error) {
    diagnostics.renderReadyReported = false;
  } finally {
    renderReadyPostInFlight = false;
  }
}

map.on("error", (event) => {
  const message = String(event?.error?.message || event?.error || "unknown map error");
  if (!errors.includes(message)) errors.push(message);
});

function altitudeBand(altitude) {
  if (typeof altitude === "string" && altitude.toLowerCase() === "ground") return "ground";
  if (!Number.isFinite(altitude)) return "unknown";
  if (altitude < 10_000) return "low";
  if (altitude < 25_000) return "mid";
  if (altitude < 35_000) return "high";
  return "very_high";
}

function parseHexColor(color) {
  const match = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(color);
  if (!match) throw new TypeError(`unsupported theme color: ${color}`);
  return match.slice(1).map((part) => Number.parseInt(part, 16));
}

function mixedColor(base, target, amount) {
  const baseRgb = parseHexColor(base);
  const targetRgb = parseHexColor(target);
  const progress = Math.min(1, Math.max(0, amount));
  const mixed = baseRgb.map((channel, index) => Math.round(
    channel + (targetRgb[index] - channel) * progress,
  ));
  return `#${mixed.map((channel) => channel.toString(16).padStart(2, "0")).join("")}`;
}

function themedColor(base, theme, brightnessScale = 1, warmthScale = 1) {
  const lifted = mixedColor(base, "#eef2e7", theme.brightness * brightnessScale);
  return mixedColor(lifted, "#f08a45", theme.warmth * warmthScale);
}

function themedRgba(base, alpha, theme, brightnessScale = 1, warmthScale = 1) {
  const [red, green, blue] = parseHexColor(themedColor(base, theme, brightnessScale, warmthScale));
  return `rgba(${red},${green},${blue},${alpha})`;
}

function terrainReliefExpression(theme) {
  const expression = ["interpolate", ["linear"], ["elevation"]];
  for (const [elevation, color] of TERRAIN_RELIEF_COLORS) {
    expression.push(elevation, themedColor(color, theme, 1, 1));
  }
  return expression;
}

function setThemedPaint(layerId, property, value, transitionMs, transitionable = true) {
  if (!map.getLayer(layerId)) return;
  if (transitionable) {
    map.setPaintProperty(layerId, `${property}-transition`, {duration: transitionMs, delay: 0});
  }
  map.setPaintProperty(layerId, property, value);
}

function applySolarTheme({immediate = false} = {}) {
  if (!mapLoaded) return;
  const sampleDate = new Date(solarTimeOverride ?? Date.now());
  const theme = solarTheme(sampleDate, solarObserver.lat, solarObserver.lon);
  const transitionMs = immediate || !solarThemeInitialized ? 0 : SOLAR_THEME_TRANSITION_MS;

  setThemedPaint("background", "background-color", "#04090f", transitionMs);
  // MapLibre supports the color-relief color expression, but not its transition property.
  // Setting the unsupported sibling emits a map error even though the color itself renders.
  setThemedPaint(
    "terrain-relief",
    "color-relief-color",
    terrainReliefExpression(theme),
    transitionMs,
    false,
  );
  setThemedPaint(
    "terrain-hillshade",
    "hillshade-shadow-color",
    themedRgba("#030a0d", 0.78, theme, 0.45, 0.10),
    transitionMs,
  );
  setThemedPaint(
    "terrain-hillshade",
    "hillshade-highlight-color",
    themedRgba("#66776a", 0.48, theme, 1.15, 1.00),
    transitionMs,
  );
  setThemedPaint(
    "terrain-hillshade",
    "hillshade-accent-color",
    themedRgba("#0f1a1a", 0.55, theme, 0.75, 0.50),
    transitionMs,
  );
  setThemedPaint("wood-muted", "fill-color", themedColor("#395046", theme, 1.00, 0.65), transitionMs);
  setThemedPaint("park-muted", "fill-color", themedColor("#425446", theme, 1.00, 0.55), transitionMs);
  setThemedPaint("water", "fill-color", "#04090f", transitionMs);
  setThemedPaint("weather-sea-veil", "fill-color", themedColor("#071923", theme, 0.50, 0.05), transitionMs);
  setThemedPaint(
    "coastline",
    "line-color",
    "#70a8b6",
    transitionMs,
  );
  setThemedPaint(
    "country-border",
    "line-color",
    themedRgba("#88a6a0", 0.88, theme, 0.50, 0.12),
    transitionMs,
  );
  setThemedPaint(
    "prefecture-border",
    "line-color",
    themedRgba("#7fa49b", 0.90, theme, 0.50, 0.12),
    transitionMs,
  );
  setThemedPaint(
    "major-city",
    "text-color",
    themedRgba("#c4d3d5", 0.84, theme, 0.50, 0.10),
    transitionMs,
  );
  setThemedPaint(
    "major-city-large",
    "text-color",
    themedRgba("#d3e0e1", 0.94, theme, 0.45, 0.10),
    transitionMs,
  );

  diagnostics.solarTheme = theme.phase;
  diagnostics.solarAltitudeDegrees = Number(theme.altitudeDegrees.toFixed(2));
  diagnostics.solarAzimuthDegrees = Number(theme.azimuthDegrees.toFixed(2));
  diagnostics.solarRising = theme.rising;
  diagnostics.solarBrightness = Number(theme.brightness.toFixed(4));
  diagnostics.solarWarmth = Number(theme.warmth.toFixed(4));
  diagnostics.solarSampleTimeUtc = theme.sampleTimeUtc;
  diagnostics.solarObserver = {...solarObserver};
  document.documentElement.dataset.solarTheme = theme.phase;
  solarThemeInitialized = true;
}

function addPlaneImage(name, color) {
  const canvas = document.createElement("canvas");
  canvas.width = 48;
  canvas.height = 48;
  const context = canvas.getContext("2d");
  context.translate(24, 24);
  context.beginPath();
  context.moveTo(0, -21);
  context.bezierCurveTo(3, -18, 4, -10, 4, -4);
  context.lineTo(18, 3);
  context.lineTo(18, 7);
  context.lineTo(4, 5);
  context.lineTo(3, 14);
  context.lineTo(9, 18);
  context.lineTo(9, 21);
  context.lineTo(0, 18);
  context.lineTo(-9, 21);
  context.lineTo(-9, 18);
  context.lineTo(-3, 14);
  context.lineTo(-4, 5);
  context.lineTo(-18, 7);
  context.lineTo(-18, 3);
  context.lineTo(-4, -4);
  context.bezierCurveTo(-4, -10, -3, -18, 0, -21);
  context.closePath();
  context.lineJoin = "round";
  context.lineWidth = 1;
  context.strokeStyle = "#080b10";
  context.fillStyle = color;
  context.fill();
  context.stroke();
  map.addImage(`plane-${name}`, context.getImageData(0, 0, 48, 48), {pixelRatio: 1});
}

function destination(lon, lat, distanceNmi, directionDegrees) {
  const angular = distanceNmi * 1.852 / 6371.0088;
  const lat1 = lat * Math.PI / 180;
  const lon1 = lon * Math.PI / 180;
  const bearing = directionDegrees * Math.PI / 180;
  const lat2 = Math.asin(
    Math.sin(lat1) * Math.cos(angular)
      + Math.cos(lat1) * Math.sin(angular) * Math.cos(bearing),
  );
  const lon2 = lon1 + Math.atan2(
    Math.sin(bearing) * Math.sin(angular) * Math.cos(lat1),
    Math.cos(angular) - Math.sin(lat1) * Math.sin(lat2),
  );
  return [lon2 * 180 / Math.PI, lat2 * 180 / Math.PI];
}

async function fetchJson(path) {
  const separator = path.includes("?") ? "&" : "?";
  const response = await fetch(`${path}${separator}ts=${Date.now()}`, {cache: "no-store"});
  if (!response.ok) throw new Error(`${path}: HTTP ${response.status}`);
  return response.json();
}

function jstClock(epochMs) {
  const value = new Date(epochMs + 9 * 60 * 60 * 1_000);
  return `${String(value.getUTCHours()).padStart(2, "0")}:${String(value.getUTCMinutes()).padStart(2, "0")}`;
}

function setPrecipitationStatus(mode, observedMs = 0) {
  precipitationStatus.classList.toggle("visible", mode !== "hidden");
  precipitationStatus.classList.toggle("stale", mode === "stale");
  precipitationTitle.textContent = mode === "stale" ? "PRECIPITATION STALE" : "PRECIPITATION";
  precipitationTime.textContent = observedMs ? `${jstClock(observedMs)} JST` : "--:-- JST";
  updateRangeRingLegendOverlap();
}

function updateRangeRingLegendOverlap() {
  if (!mapLoaded || !rangeRing150Coordinates.length) return;
  const legendBounds = document.getElementById("mapLegends").getBoundingClientRect();
  const margin = 4;
  diagnostics.rangeRingLegendOverlap = rangeRing150Coordinates.some((coordinate) => {
    const point = map.project(coordinate);
    return point.x >= legendBounds.left - margin
      && point.x <= legendBounds.right + margin
      && point.y >= legendBounds.top - margin
      && point.y <= legendBounds.bottom + margin;
  });
}

function removePrecipitationLayer(layer) {
  if (!layer) return;
  if (map.getLayer(layer.layerId)) map.removeLayer(layer.layerId);
  if (map.getSource(layer.sourceId)) map.removeSource(layer.sourceId);
}

function fadeOutActivePrecipitation() {
  const previous = activePrecipitation;
  activePrecipitation = null;
  diagnostics.precipitationLayerLoaded = false;
  diagnostics.precipitationLayerValidtime = null;
  if (!previous) return;
  if (map.getLayer(previous.layerId)) map.setPaintProperty(previous.layerId, "raster-opacity", 0);
  setTimeout(() => removePrecipitationLayer(previous), PRECIPITATION_FADE_MS + 150);
}

async function installPrecipitationLayer(payload, observedMs, staleAfterMs) {
  const validtime = String(payload.validtime || "");
  const tileTemplate = String(payload.tile_template || "");
  if (!/^\d{14}$/.test(validtime)) throw new Error("invalid precipitation validtime");
  if (!/^\/weather\/tiles\/\d{14}\/\{z\}\/\{x\}\/\{y\}\.png$/.test(tileTemplate)) {
    throw new Error("invalid local precipitation tile template");
  }
  if (activePrecipitation?.validtime === validtime) {
    activePrecipitation.observedMs = observedMs;
    activePrecipitation.staleAfterMs = staleAfterMs;
    diagnostics.precipitationLayerLoaded = true;
    diagnostics.precipitationLayerValidtime = validtime;
    diagnostics.precipitationLoadState = "loaded";
    diagnostics.precipitationPendingValidtime = null;
    return;
  }

  precipitationSequence += 1;
  const sequence = precipitationSequence;
  const sourceId = `precipitation-source-${sequence}`;
  const layerId = `precipitation-layer-${sequence}`;
  const minzoom = Number.isFinite(payload.minzoom) ? payload.minzoom : 6;
  const maxzoom = Number.isFinite(payload.maxzoom) ? payload.maxzoom : 7;
  map.addSource(sourceId, {
    type: "raster",
    tiles: [tileTemplate],
    // JMA publishes the active precipitation image on even zoom levels.
    // A 512 px source makes MapLibre request z6 at the fixed z6.6 stream view.
    tileSize: 512,
    minzoom,
    maxzoom,
    attribution: "Precipitation: Japan Meteorological Agency, processed (PDL1.0)",
  });
  map.addLayer({
    id: layerId,
    type: "raster",
    source: sourceId,
    paint: {
      "raster-opacity": 0.001,
      "raster-opacity-transition": {duration: PRECIPITATION_FADE_MS, delay: 0},
      "raster-fade-duration": 0,
      "raster-resampling": "linear",
    },
  }, "weather-sea-veil");

  diagnostics.precipitationLoadState = "loading";
  diagnostics.precipitationPendingValidtime = validtime;
  const sourceReady = await waitForMapSourceReady(map, sourceId, {
    timeoutMs: PRECIPITATION_SOURCE_READY_TIMEOUT_MS,
    pollMs: PRECIPITATION_SOURCE_READY_POLL_MS,
  });
  diagnostics.precipitationLastLoadReason = sourceReady.reason;
  diagnostics.precipitationLastLoadDurationMs = sourceReady.elapsed_ms;
  diagnostics.precipitationPendingValidtime = null;
  if (!sourceReady.ready || sequence !== precipitationSequence) {
    diagnostics.precipitationLoadState = "failed";
    removePrecipitationLayer({sourceId, layerId});
    if (!sourceReady.ready) {
      throw new Error(`local precipitation source was not ready: ${sourceReady.reason}`);
    }
    return;
  }

  const previous = activePrecipitation;
  activePrecipitation = {
    sourceId,
    layerId,
    validtime,
    observedMs,
    staleAfterMs,
  };
  map.setPaintProperty(layerId, "raster-opacity", PRECIPITATION_LAYER_OPACITY);
  if (previous && map.getLayer(previous.layerId)) {
    map.setPaintProperty(previous.layerId, "raster-opacity", 0);
    setTimeout(() => removePrecipitationLayer(previous), PRECIPITATION_FADE_MS + 150);
  }
  diagnostics.precipitationLayerLoaded = true;
  diagnostics.precipitationLayerValidtime = validtime;
  diagnostics.precipitationLoadState = "loaded";
}

async function refreshPrecipitation() {
  if (!mapLoaded || precipitationRefreshInFlight) return;
  precipitationRefreshInFlight = true;
  try {
    const payload = await fetchJson("/weather/status.json");
    if (payload?.available !== true || payload?.analysis_only !== true || payload?.forecast_minutes !== 0) {
      throw new Error("precipitation status is not analysis-only");
    }
    const observedMs = Date.parse(String(payload.observed_at_utc || ""));
    if (!Number.isFinite(observedMs)) throw new Error("invalid precipitation observation time");
    const configuredStaleMs = Number(payload.stale_after_sec) * 1_000;
    const staleAfterMs = Number.isFinite(configuredStaleMs) && configuredStaleMs >= 60_000
      ? configuredStaleMs
      : PRECIPITATION_DEFAULT_STALE_MS;
    const hasRain = payload.has_precipitation === true;
    const stale = Date.now() - observedMs > staleAfterMs;

    diagnostics.precipitationAvailable = true;
    diagnostics.precipitationFresh = !stale;
    diagnostics.precipitationHasRain = hasRain;
    diagnostics.precipitationValidtime = String(payload.validtime || "");
    if (stale) {
      setPrecipitationStatus(hasRain ? "stale" : "hidden", observedMs);
      fadeOutActivePrecipitation();
    } else if (!hasRain) {
      setPrecipitationStatus("hidden");
      fadeOutActivePrecipitation();
    } else {
      setPrecipitationStatus("fresh", observedMs);
      await installPrecipitationLayer(payload, observedMs, staleAfterMs);
    }
  } catch (_error) {
    diagnostics.precipitationAvailable = false;
    if (activePrecipitation) {
      if (Date.now() - activePrecipitation.observedMs > activePrecipitation.staleAfterMs) {
        setPrecipitationStatus("stale", activePrecipitation.observedMs);
        diagnostics.precipitationFresh = false;
        fadeOutActivePrecipitation();
      } else {
        setPrecipitationStatus("fresh", activePrecipitation.observedMs);
        diagnostics.precipitationFresh = true;
        diagnostics.precipitationValidtime = activePrecipitation.validtime;
        diagnostics.precipitationLayerLoaded = true;
        diagnostics.precipitationLayerValidtime = activePrecipitation.validtime;
      }
    } else {
      setPrecipitationStatus("hidden");
      diagnostics.precipitationFresh = false;
      diagnostics.precipitationLayerLoaded = false;
    }
  } finally {
    diagnostics.precipitationEvaluated = true;
    precipitationRefreshInFlight = false;
  }
}

function addLiveLayers() {
  map.addSource("coverage", {type: "geojson", data: emptyCollection()});
  map.addSource("range-rings", {type: "geojson", data: emptyCollection()});
  map.addSource("range-labels", {type: "geojson", data: emptyCollection()});
  map.addSource("aircraft", {type: "geojson", data: emptyCollection()});

  map.addLayer({
    id: "coverage-shadow",
    type: "line",
    source: "coverage",
    layout: {"line-cap": "round", "line-join": "round", "line-miter-limit": 2},
    paint: {
      "line-color": COVERAGE_HALO_COLOR,
      "line-width": COVERAGE_HALO_WIDTH,
      "line-opacity": COVERAGE_HALO_OPACITY,
    },
  }, "coastline-glow");
  map.addLayer({
    id: "coverage-line",
    type: "line",
    source: "coverage",
    layout: {"line-cap": "round", "line-join": "round", "line-miter-limit": 2},
    paint: {
      "line-color": COVERAGE_COLOR,
      "line-width": COVERAGE_WIDTH,
      "line-opacity": COVERAGE_OPACITY,
    },
  }, "coastline-glow");

  map.addLayer({
    id: "range-ring-shadow",
    type: "line",
    source: "range-rings",
    paint: {"line-color": "rgba(3,10,13,0.86)", "line-width": 3.7, "line-blur": 0.8},
  });
  map.addLayer({
    id: "range-rings",
    type: "line",
    source: "range-rings",
    paint: {
      "line-color": RANGE_RING_COLOR,
      "line-opacity": RANGE_RING_OPACITY,
      "line-width": RANGE_RING_WIDTH,
    },
  });
  map.addLayer({
    id: "range-labels",
    type: "symbol",
    source: "range-labels",
    layout: {
      "text-field": ["get", "label"],
      "text-font": ["Noto Sans Regular"],
      "text-size": RANGE_LABEL_SIZE,
      "text-letter-spacing": 0.1,
      "text-allow-overlap": true,
    },
    paint: {
      "text-color": "rgba(196,219,222,0.90)",
      "text-halo-color": "rgba(5,13,16,0.95)",
      "text-halo-width": 1.4,
      "text-halo-blur": 0.7,
    },
  });
  map.addLayer({
    id: "aircraft-icon",
    type: "symbol",
    source: "aircraft",
    layout: {
      "icon-image": ["get", "icon"],
      "icon-size": AIRCRAFT_ICON_SIZE,
      "icon-rotate": ["get", "heading"],
      "icon-rotation-alignment": "map",
      "icon-allow-overlap": true,
      "icon-ignore-placement": true,
    },
  });
}

async function refreshReceiverAndRings() {
  try {
    const receiver = await fetchJson("../stream1090/data/receiver.json");
    if (!Number.isFinite(receiver.lat) || !Number.isFinite(receiver.lon)) return;
    solarObserver = {lat: receiver.lat, lon: receiver.lon, source: "receiver.json"};
    applySolarTheme({immediate: solarTimeOverride !== null});
    const rings = [];
    const labels = [];
    for (const radius of [50, 100, 150]) {
      const coordinates = [];
      for (let direction = 0; direction <= 360; direction += 2) {
        coordinates.push(destination(receiver.lon, receiver.lat, radius, direction));
      }
      rings.push({
        type: "Feature",
        properties: {label: `${radius} NM`},
        geometry: {type: "LineString", coordinates},
      });
      labels.push({
        type: "Feature",
        properties: {label: `${radius} NM`},
        geometry: {type: "Point", coordinates: destination(receiver.lon, receiver.lat, radius, 48)},
      });
      if (radius === 150) {
        rangeRing150Coordinates = coordinates;
      }
    }
    map.getSource("range-rings").setData({type: "FeatureCollection", features: rings});
    map.getSource("range-labels").setData({type: "FeatureCollection", features: labels});
    diagnostics.rangeRingFeatureCount = rings.length;
    diagnostics.rangeLabelFeatureCount = labels.length;
    updateRangeRingLegendOverlap();
  } catch (_error) {
    // The receiver marker is intentionally absent; stale rings are safer than visual churn.
  }
}

async function refreshCoverage() {
  try {
    const outline = await fetchJson("../stream1090/data/outline.json");
    const points = outline?.actualRange?.last24h?.points;
    if (!Array.isArray(points)) return;
    const coordinates = points
      .filter((point) => Array.isArray(point) && Number.isFinite(point[0]) && Number.isFinite(point[1]))
      .map((point) => [point[1], point[0]]);
    if (coordinates.length < 3) return;
    coordinates.push(coordinates[0]);
    map.getSource("coverage").setData({
      type: "FeatureCollection",
      features: [{
        type: "Feature",
        properties: {window: "last24h", representation: "maximum-reception-by-bearing"},
        geometry: {type: "LineString", coordinates},
      }],
    });
    diagnostics.coveragePoints = coordinates.length - 1;
    diagnostics.coverageSourceFeatureCount = 1;
  } catch (_error) {
    // Preserve the last valid 24-hour outline during a short upstream outage.
  }
}

async function refreshAircraft() {
  try {
    const payload = await fetchJson("../adsb/aircraft.json");
    const sampleEpoch = Number.isFinite(payload.now) ? payload.now : Date.now() / 1000;
    const aircraft = Array.isArray(payload.aircraft) ? payload.aircraft : [];
    const currentFeatures = [];

    for (const item of aircraft) {
      if (!Number.isFinite(item.lat) || !Number.isFinite(item.lon)) continue;
      if (Number.isFinite(item.seen_pos) && item.seen_pos > MAX_POSITION_AGE_SEC) continue;
      const identifier = String(item.hex || "").trim();
      if (!identifier) continue;
      const altitude = item.alt_baro ?? item.alt_geom;
      const band = altitudeBand(altitude);
      const color = ALTITUDE_COLORS[band];
      const coordinate = [item.lon, item.lat];
      const heading = Number.isFinite(item.track) ? item.track : 0;
      const properties = {color, icon: `plane-${band}`, heading};
      currentFeatures.push({
        type: "Feature",
        properties,
        geometry: {type: "Point", coordinates: coordinate},
      });
    }

    map.getSource("aircraft").setData({type: "FeatureCollection", features: currentFeatures});
    diagnostics.aircraftCount = currentFeatures.length;
    diagnostics.aircraftSourceFeatureCount = currentFeatures.length;
    diagnostics.lastAircraftEpoch = sampleEpoch;
    aircraftLastReceivedAt = Date.now();
    aircraftFailures = 0;
    showStatus("");
  } catch (_error) {
    aircraftFailures += 1;
    if (aircraftFailures >= 4) showStatus("ADS-B DATA RETRYING");
  }
}

map.on("load", () => {
  mapLoaded = true;
  applySolarTheme({immediate: true});
  for (const [name, color] of Object.entries(ALTITUDE_COLORS)) addPlaneImage(name, color);
  addLiveLayers();
  refreshReceiverAndRings();
  refreshCoverage();
  refreshAircraft();
  refreshPrecipitation();
  refreshAssetIdentity();
  setInterval(refreshAircraft, AIRCRAFT_REFRESH_MS);
  setInterval(refreshReceiverAndRings, RANGE_REFRESH_MS);
  setInterval(refreshCoverage, RANGE_REFRESH_MS);
  setInterval(refreshPrecipitation, PRECIPITATION_REFRESH_MS);
  setInterval(refreshAssetIdentity, RANGE_REFRESH_MS);
  setInterval(applySolarTheme, SOLAR_THEME_REFRESH_MS);
  setInterval(publishRenderReady, RENDER_READY_HEARTBEAT_MS);
});

map.on("idle", () => {
  if (!mapLoaded || !map.areTilesLoaded()) return;
  const bounds = map.getBounds();
  const layerIds = map.getStyle().layers.map((layer) => layer.id);
  const coverageLayerIndex = layerIds.indexOf("coverage-line");
  const precipitationLayerIndex = activePrecipitation
    ? layerIds.indexOf(activePrecipitation.layerId)
    : layerIds.indexOf("weather-sea-veil");
  const attribution = document.getElementById("mapAttribution");
  const attributionStyle = getComputedStyle(attribution);
  const attributionInnerWidth = attribution.clientWidth
    - parseFloat(attributionStyle.paddingLeft)
    - parseFloat(attributionStyle.paddingRight);
  const attributionContentWidth = Math.max(
    ...Array.from(attribution.querySelectorAll("span"), (item) => item.scrollWidth),
  );
  window.adsbMapRenderReport = {
    viewport: [document.documentElement.clientWidth, document.documentElement.clientHeight],
    center: [map.getCenter().lng, map.getCenter().lat],
    zoom: map.getZoom(),
    bounds: [bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()],
    openFreeMapLoaded: map.isSourceLoaded("openmaptiles"),
    terrainLoaded: map.isSourceLoaded("terrain-dem"),
    prefectureSegments: map.queryRenderedFeatures(undefined, {layers: ["prefecture-border"]}).length,
    aircraftLabels: false,
    aircraftTracks: false,
    labelLanguage: "latin-or-english",
    coverageLine: "solid",
    coverageColor: COVERAGE_COLOR,
    coverageOpacity: COVERAGE_OPACITY,
    coverageWidth: COVERAGE_WIDTH,
    coverageHaloColor: COVERAGE_HALO_COLOR,
    coverageHaloOpacity: COVERAGE_HALO_OPACITY,
    coverageHaloWidth: COVERAGE_HALO_WIDTH,
    coverageMiterLimit: 2,
    coverageAboveTerrain: coverageLayerIndex > layerIds.indexOf("water"),
    coverageAbovePrecipitation: coverageLayerIndex > precipitationLayerIndex,
    coverageBelowBoundaries: coverageLayerIndex < layerIds.indexOf("coastline-glow"),
    coverageWindow: diagnostics.coverageWindow,
    coverageSourceField: diagnostics.coverageSourceField,
    integratedSidebar: embedded,
    rangeRingLine: "solid",
    rangeRingColor: RANGE_RING_COLOR,
    rangeRingOpacity: RANGE_RING_OPACITY,
    rangeRingWidth: RANGE_RING_WIDTH,
    rangeRingLegendOverlap: diagnostics.rangeRingLegendOverlap,
    attributionCardWidth: attribution.getBoundingClientRect().width,
    attributionUnusedWidth: Math.max(0, attributionInnerWidth - attributionContentWidth),
    precipitationAvailable: diagnostics.precipitationAvailable,
    precipitationFresh: diagnostics.precipitationFresh,
    precipitationHasRain: diagnostics.precipitationHasRain,
    precipitationValidtime: diagnostics.precipitationValidtime,
    precipitationLayerValidtime: diagnostics.precipitationLayerValidtime,
    precipitationLayerLoaded: diagnostics.precipitationLayerLoaded,
    precipitationEvaluated: diagnostics.precipitationEvaluated,
    precipitationLayerOpacity: diagnostics.precipitationLayerOpacity,
    precipitationAnalysisOnly: diagnostics.precipitationAnalysisOnly,
    precipitationLoadState: diagnostics.precipitationLoadState,
    precipitationPendingValidtime: diagnostics.precipitationPendingValidtime,
    precipitationLastLoadReason: diagnostics.precipitationLastLoadReason,
    precipitationLastLoadDurationMs: diagnostics.precipitationLastLoadDurationMs,
    solarTheme: diagnostics.solarTheme,
    solarAltitudeDegrees: diagnostics.solarAltitudeDegrees,
    solarAzimuthDegrees: diagnostics.solarAzimuthDegrees,
    solarRising: diagnostics.solarRising,
    solarBrightness: diagnostics.solarBrightness,
    solarWarmth: diagnostics.solarWarmth,
    solarSampleTimeUtc: diagnostics.solarSampleTimeUtc,
    solarObserver: diagnostics.solarObserver,
    solarTimeOverride: diagnostics.solarTimeOverride,
    solarThemedLayers: diagnostics.solarThemedLayers,
    solarAircraftColorsFixed: diagnostics.solarAircraftColorsFixed,
    solarPrecipitationColorsFixed: diagnostics.solarPrecipitationColorsFixed,
    renderReadyReported: diagnostics.renderReadyReported,
    errors,
  };
  window.adsbMapReady = true;
  document.documentElement.dataset.mapReady = "true";
  document.documentElement.dataset.mapReport = JSON.stringify(window.adsbMapRenderReport);
  publishRenderReady();
});

setTimeout(() => {
  if (!mapLoaded) showStatus("MAP DATA RETRYING");
}, 20_000);
